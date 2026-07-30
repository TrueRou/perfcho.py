import hashlib
import json
from collections.abc import Iterator
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import cast

import bcrypt
import pytest
from sqlalchemy import select, text

from perfcho.infra.db.enums import BeatmapStatus, Ruleset, ScoreboardVariant
from perfcho.infra.db.models.scoring import CalculationFormula, CalculationRelease
from perfcho.modules.common import ObjectStorage, StoredObject
from tools.bancho_migration.config import MigrationConfig, MigrationOverrides
from tools.bancho_migration.domains.community import migrate_community
from tools.bancho_migration.domains.content import migrate_content
from tools.bancho_migration.domains.identity import migrate_identity
from tools.bancho_migration.domains.multiplayer import migrate_multiplayer
from tools.bancho_migration.domains.scoring import migrate_scoring
from tools.bancho_migration.domains.social import migrate_social
from tools.bancho_migration.models import DiagnosticSeverity, MigrationRuntime, SourceSchema
from tools.bancho_migration.report import MigrationReport
from tools.bancho_migration.schema import EXCLUDED_TABLES, REQUIRED_COLUMNS, validate_source_schema
from tools.bancho_migration.source import BanchoSource
from tools.bancho_migration.state import MigrationStateStore
from tools.bancho_migration.storage import (
    BeatmapChecksumMismatch,
    SourceFileInvalid,
    read_beatmap_file,
    read_replay_file,
    upload_beatmap_file,
    upload_replay_file,
)
from tools.bancho_migration.target import create_target_engine, prepare_target
from tools.bancho_migration.transforms import (
    beatmap_status,
    mod_set,
    normalized_accuracy,
    scoreboard,
    unix_datetime,
)


class FakeObjectStorage:
    def __init__(self) -> None:
        self.writes: list[tuple[str, bytes, str, bytes | None]] = []
        self.deletes: list[str] = []

    async def put(
        self,
        storage_key: str,
        content: bytes,
        *,
        media_type: str,
        expected_sha256: bytes | None = None,
    ) -> StoredObject:
        self.writes.append((storage_key, content, media_type, expected_sha256))
        return StoredObject(storage_key, len(content), media_type, hashlib.sha256(content).digest(), "etag")

    async def delete(self, storage_key: str) -> None:
        self.deletes.append(storage_key)


class EmptyBanchoSource:
    def iter_batches(self, *args: object, **kwargs: object) -> Iterator[list[dict[str, object]]]:
        del args, kwargs
        yield from ()

    def fetch_all(self, *args: object, **kwargs: object) -> list[dict[str, object]]:
        del args, kwargs
        return []


class FixtureBanchoSource:
    def __init__(self, tables: dict[str, list[dict[str, object]]]) -> None:
        self.tables = tables

    def iter_batches(
        self,
        table: str,
        *,
        key: str,
        batch_size: int,
        start_after: int = 0,
        columns: tuple[str, ...] = ("*",),
    ) -> Iterator[list[dict[str, object]]]:
        rows = sorted(
            (row for row in self.tables[table] if self._integer(row[key]) > start_after),
            key=lambda row: self._integer(row[key]),
        )
        for offset in range(0, len(rows), batch_size):
            batch = rows[offset : offset + batch_size]
            yield [self._project(row, columns) for row in batch]

    def fetch_all(
        self,
        table: str,
        *,
        columns: tuple[str, ...] = ("*",),
        order_by: tuple[str, ...] = (),
        where: str | None = None,
        parameters: tuple[object, ...] | list[object] = (),
    ) -> list[dict[str, object]]:
        rows = list(self.tables[table])
        if where == "`server` = %s AND `set_id` = %s":
            rows = [row for row in rows if row["server"] == parameters[0] and row["set_id"] == parameters[1]]
        elif where is not None and where.startswith("`userid` IN"):
            rows = [row for row in rows if row["userid"] in parameters]
        elif where is not None and where.startswith("`user1` IN"):
            rows = [row for row in rows if row["user1"] in parameters]
        elif where is not None and where.startswith("`id` IN"):
            rows = [row for row in rows if row["id"] in parameters]
        elif where == "`pool_id` = %s":
            rows = [row for row in rows if row["pool_id"] == parameters[0]]
        elif where == "`id` = %s":
            rows = [row for row in rows if row["id"] == parameters[0]]
        elif where is not None:
            raise AssertionError(f"unsupported fixture WHERE clause: {where}")
        for order in reversed(order_by):
            name, _, direction = order.partition(" ")
            rows.sort(key=lambda row: str(row[name]), reverse=direction.upper() == "DESC")
        return [self._project(row, columns) for row in rows]

    def maximum(self, table: str, column: str) -> int:
        return max((self._integer(row[column]) for row in self.tables[table]), default=0)

    @staticmethod
    def _project(row: dict[str, object], columns: tuple[str, ...]) -> dict[str, object]:
        return dict(row) if columns == ("*",) else {column: row[column] for column in columns}

    @staticmethod
    def _integer(value: object) -> int:
        assert isinstance(value, int) and not isinstance(value, bool)
        return value


def _osu_file() -> bytes:
    return b"""osu file format v14

[Events]
0,0,"bg.jpg",0,0
2,1500,2000
Video,0,"video.mp4"
Sprite,Foreground,Centre,"sb.png",320,240

[HitObjects]
64,64,1000,1,0,0:0:0:0:
128,64,2500,2,0,B|192:64,1,100
256,64,4000,8,0,4500
"""


def _config(tmp_path: Path, overrides: Path | None = None) -> MigrationConfig:
    return MigrationConfig.from_values(
        source_url="mysql+pymysql://bancho:secret@127.0.0.1/bancho",
        target_url="postgresql+asyncpg://perfcho:secret@127.0.0.1/perfcho",
        data_directory=tmp_path,
        migration_id="legacy-2026-07",
        source_timezone="UTC",
        batch_size=500,
        report_path=tmp_path / "report.json",
        overrides_path=overrides,
        confirm_offline=True,
    )


def test_config_digest_is_secret_free_stable_and_sensitive_to_overrides(tmp_path: Path) -> None:
    first = _config(tmp_path)
    second = _config(tmp_path)
    assert first.digest == second.digest
    assert "secret" not in first.digest

    overrides = tmp_path / "overrides.json"
    overrides.write_text(json.dumps({"accounts": {"3": {"target_account_id": 33}}}))
    changed = _config(tmp_path, overrides)
    assert changed.digest != first.digest
    assert changed.overrides == MigrationOverrides.load(overrides)
    assert changed.overrides.accounts[3].target_account_id == 33


def test_config_rejects_non_async_target_and_unsafe_migration_id(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match=r"postgresql\+asyncpg"):
        MigrationConfig.from_values(
            source_url="mysql://localhost/bancho",
            target_url="postgresql://localhost/perfcho",
            data_directory=tmp_path,
            migration_id="valid",
            source_timezone="UTC",
            batch_size=1,
            report_path=tmp_path / "report.json",
            overrides_path=None,
            confirm_offline=False,
        )
    with pytest.raises(ValueError, match="migration ID"):
        _config(tmp_path).__class__(
            source_url="mysql://localhost/bancho",
            target_url="postgresql+asyncpg://localhost/perfcho",
            data_directory=tmp_path,
            migration_id="unsafe/id",
            source_timezone=_config(tmp_path).source_timezone,
        )


def test_schema_contract_requires_core_tables_and_explicitly_excludes_sb() -> None:
    tables = {name: columns for name, columns in REQUIRED_COLUMNS.items()}
    for excluded in EXCLUDED_TABLES:
        tables[excluded] = frozenset({"id"})
    schema = SourceSchema(tables, "5.2.2", {}, "fingerprint")
    report = MigrationReport("test")
    validate_source_schema(schema, report)
    assert not report.has_errors
    assert {item.entity for item in report.diagnostics} == EXCLUDED_TABLES

    broken = SourceSchema({"users": frozenset({"id"})}, "5.2.2", {}, "broken")
    broken_report = MigrationReport("test")
    validate_source_schema(broken, broken_report)
    assert broken_report.has_errors
    assert any(item.code == "source_columns_missing" for item in broken_report.diagnostics)
    assert any(item.code == "source_table_missing" for item in broken_report.diagnostics)


def test_legacy_score_and_content_scalar_transforms_are_explicit() -> None:
    assert scoreboard(0) == (1, Ruleset.OSU, ScoreboardVariant.VANILLA)
    assert scoreboard(4) == (5, Ruleset.OSU, ScoreboardVariant.RELAX)
    assert scoreboard(8) == (8, Ruleset.OSU, ScoreboardVariant.AUTOPILOT)
    assert beatmap_status(1) is BeatmapStatus.PENDING
    assert beatmap_status(5) is BeatmapStatus.LOVED
    assert normalized_accuracy("98.765") == Decimal("0.987650000")
    board_id, canonical, digest, bits = mod_set(4, 1 << 7)
    assert board_id == 5
    assert canonical == []
    assert len(digest) == 32
    assert bits == 1 << 7
    with pytest.raises(ValueError, match="disagree"):
        mod_set(0, 1 << 7)


def test_unix_timestamp_uses_deterministic_fallback() -> None:
    fallback = datetime(2007, 9, 16, tzinfo=UTC)
    assert unix_datetime(0, fallback=fallback) == fallback
    assert unix_datetime(1, fallback=fallback) == datetime(1970, 1, 1, 0, 0, 1, tzinfo=UTC)


def test_beatmap_file_is_bounded_verified_and_structurally_parsed(tmp_path: Path) -> None:
    osu_directory = tmp_path / ".data" / "osu"
    osu_directory.mkdir(parents=True)
    content = _osu_file()
    path = osu_directory / "123.osu"
    path.write_bytes(content)
    md5 = hashlib.md5(content, usedforsecurity=False).hexdigest()

    metadata = read_beatmap_file(tmp_path, 123, md5)
    assert metadata.path == path
    assert metadata.object_count == 3
    assert (metadata.circle_count, metadata.slider_count, metadata.spinner_count) == (1, 1, 1)
    assert metadata.drain_length_ms == 2500
    assert metadata.has_storyboard
    assert metadata.has_video
    assert metadata.sha256 == hashlib.sha256(content).digest()

    with pytest.raises(BeatmapChecksumMismatch):
        read_beatmap_file(tmp_path, 123, "0" * 32)
    with pytest.raises(SourceFileInvalid, match="exceeds"):
        read_beatmap_file(tmp_path, 123, md5, maximum_bytes=10)


@pytest.mark.asyncio
async def test_content_addressed_upload_keys_match_runtime_conventions(tmp_path: Path) -> None:
    osu_directory = tmp_path / ".data" / "osu"
    replay_directory = tmp_path / ".data" / "osr"
    osu_directory.mkdir(parents=True)
    replay_directory.mkdir(parents=True)
    content = _osu_file()
    (osu_directory / "123.osu").write_bytes(content)
    replay_content = b"stable replay frames"
    (replay_directory / "456.osr").write_bytes(replay_content)
    storage = FakeObjectStorage()
    object_storage = cast(ObjectStorage, storage)

    beatmap = read_beatmap_file(tmp_path, 123, hashlib.md5(content, usedforsecurity=False).hexdigest())
    replay = read_replay_file(tmp_path, 456)
    stored_map = await upload_beatmap_file(
        object_storage,
        beatmap,
        source_code="osu",
        beatmapset_id=12,
        beatmap_id=123,
    )
    stored_replay = await upload_replay_file(object_storage, replay, account_id=42)

    assert stored_map.storage_key == f"beatmaps/osu/12/123/{beatmap.sha256.hex()}.osu"
    assert stored_replay.storage_key == f"replays/stable/42/{replay.sha256.hex()}.osr"
    assert all(write[3] == hashlib.sha256(write[1]).digest() for write in storage.writes)


def test_report_bounds_diagnostics_and_writes_structured_json(tmp_path: Path) -> None:
    report = MigrationReport("test", diagnostic_limit=1)
    report.add(DiagnosticSeverity.WARNING, "first", "first warning")
    report.add(DiagnosticSeverity.ERROR, "dropped", "dropped error")
    report.increment("phase", "inserted", 2)
    report.finish()
    path = tmp_path / "nested" / "report.json"
    report.write(path)
    payload = json.loads(path.read_text())
    assert payload["migration_id"] == "test"
    assert payload["counters"]["phase"]["inserted"] == 2
    assert payload["counters"]["report"]["diagnostics_dropped"] == 1
    assert payload["diagnostics"][0]["severity"] == "warning"


@pytest.mark.asyncio
@pytest.mark.postgres
async def test_target_preparation_installs_legacy_credential_contract_and_checkpoint(
    postgres_database_url: str,
) -> None:
    engine = create_target_engine(postgres_database_url)
    try:
        session_factory = await prepare_target(engine)
        state = MigrationStateStore("integration", session_factory)
        started_at = datetime(2026, 7, 29, tzinfo=UTC)
        await state.initialize(source_fingerprint="source", config_digest="config", started_at=started_at)
        checkpoint = await state.load()
        assert checkpoint is not None
        assert checkpoint.source_fingerprint == "source"
        assert checkpoint.phase == "pending"

        async with session_factory() as session:
            constraint = await session.scalar(
                text(
                    "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
                    "WHERE conname = 'ck_password_credentials_algorithm_pepper_consistency'"
                )
            )
            nullable = await session.scalar(
                text(
                    "SELECT is_nullable FROM information_schema.columns "
                    "WHERE table_schema = 'iam' AND table_name = 'password_credentials' "
                    "AND column_name = 'pepper_version'"
                )
            )
        assert constraint is not None
        assert "bcrypt_md5" in constraint
        assert nullable == "YES"

        with pytest.raises(RuntimeError, match="does not match"):
            await state.initialize(source_fingerprint="other", config_digest="config", started_at=started_at)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.postgres
async def test_empty_scoring_migration_installs_current_calculation_provenance(
    postgres_database_url: str,
    tmp_path: Path,
) -> None:
    engine = create_target_engine(postgres_database_url)
    try:
        session_factory = await prepare_target(engine)
        config = MigrationConfig.from_values(
            source_url="mysql+pymysql://bancho:secret@127.0.0.1/bancho",
            target_url=postgres_database_url,
            data_directory=tmp_path,
            migration_id="scoring-integration",
            source_timezone="UTC",
            batch_size=100,
            report_path=tmp_path / "scoring-report.json",
            overrides_path=None,
            confirm_offline=True,
        )
        state = MigrationStateStore(config.migration_id, session_factory)
        await state.initialize(
            source_fingerprint="empty-source", config_digest=config.digest, started_at=datetime.now(UTC)
        )
        runtime = MigrationRuntime(
            config=config,
            overrides=MigrationOverrides(),
            source=cast(BanchoSource, EmptyBanchoSource()),
            session_factory=session_factory,
            state=state,
            report=MigrationReport(config.migration_id),
            source_schema=SourceSchema({}, "5.2.2", {}, "empty-source"),
            object_storage=cast(ObjectStorage, FakeObjectStorage()),
        )

        await migrate_scoring(runtime)

        async with session_factory() as session:
            formulas = list(await session.scalars(select(CalculationFormula)))
            releases = list(await session.scalars(select(CalculationRelease)))
        assert {formula.code for formula in formulas} == {
            "legacy-bancho-difficulty",
            "legacy-bancho-performance",
        }
        assert len(releases) == 8
        performance_releases = [
            release
            for release in releases
            if release.formula_id
            == next(formula.id for formula in formulas if formula.code == "legacy-bancho-performance")
        ]
        assert all(release.difficulty_release_id is not None for release in performance_releases)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.postgres
async def test_complete_fixture_migrates_all_supported_domains(
    postgres_database_url: str,
    tmp_path: Path,
) -> None:
    instant = datetime(2020, 1, 2, 3, 4, 5)
    osu_content = _osu_file()
    map_md5 = hashlib.md5(osu_content, usedforsecurity=False).hexdigest()
    password = bcrypt.hashpw(b"a" * 32, bcrypt.gensalt(rounds=4)).decode()
    common_user = {
        "safe_name": "unused",
        "country": "US",
        "silence_end": 0,
        "donor_end": 0,
        "creation_time": instant,
        "latest_activity": instant,
        "clan_id": 0,
        "clan_priv": 0,
        "preferred_mode": 0,
        "play_style": 3,
        "custom_badge_name": None,
        "custom_badge_icon": None,
        "userpage_content": None,
        "api_key": None,
    }
    tables: dict[str, list[dict[str, object]]] = {
        "users": [
            {
                **common_user,
                "id": 1,
                "name": "BanchoBot",
                "safe_name": "banchobot",
                "email": "bot@example.invalid",
                "priv": 1,
                "pw_bcrypt": None,
            },
            {
                **common_user,
                "id": 2,
                "name": "Alice",
                "safe_name": "alice",
                "email": "alice@example.test",
                "priv": 17,
                "pw_bcrypt": password,
                "clan_id": 1,
                "clan_priv": 3,
                "donor_end": 2_000_000_000,
                "api_key": "must-not-be-imported",
            },
        ],
        "client_hashes": [
            {
                "userid": 2,
                "osupath": "path-hash",
                "adapters": "adapter-hash",
                "uninstall_id": "uninstall-hash",
                "disk_serial": "disk-hash",
                "latest_time": instant,
                "occurrences": 2,
            }
        ],
        "ingame_logins": [
            {
                "id": 1,
                "userid": 2,
                "ip": "127.0.0.2",
                "osu_ver": "b20200102",
                "osu_stream": "stable40",
                "datetime": instant,
            }
        ],
        "relationships": [{"user1": 2, "user2": 1, "type": "friend"}],
        "clans": [{"id": 1, "name": "Migrated Team", "tag": "MT", "owner": 2, "created_at": instant}],
        "channels": [
            {
                "id": 1,
                "name": "#legacy",
                "topic": "Legacy topic",
                "read_priv": 1,
                "write_priv": 1,
                "auto_join": 1,
            }
        ],
        "mail": [{"id": 1, "from_id": 1, "to_id": 2, "msg": "hello", "time": 1_600_000_000, "read": 1}],
        "mapsets": [{"server": "osu!", "id": 10, "last_osuapi_check": instant}],
        "maps": [
            {
                "server": "osu!",
                "id": 100,
                "set_id": 10,
                "status": 2,
                "md5": map_md5,
                "artist": "Artist",
                "title": "Title",
                "version": "Hard",
                "creator": "Alice",
                "filename": "Artist - Title (Alice) [Hard].osu",
                "last_update": instant,
                "total_length": 5,
                "max_combo": 3,
                "frozen": 0,
                "plays": 1,
                "passes": 1,
                "mode": 0,
                "bpm": 180.0,
                "cs": 4.0,
                "ar": 9.0,
                "od": 8.0,
                "hp": 6.0,
                "diff": 4.5,
            }
        ],
        "map_requests": [{"id": 1, "map_id": 100, "player_id": 2, "datetime": instant, "active": 1}],
        "favourites": [{"userid": 2, "setid": 10, "created_at": 1_600_000_000}],
        "ratings": [{"userid": 2, "map_md5": map_md5, "rating": 9}],
        "comments": [
            {"id": 1, "target_id": 100, "target_type": "map", "userid": 2, "time": 1.0, "comment": "map", "colour": ""},
            {
                "id": 2,
                "target_id": 200,
                "target_type": "replay",
                "userid": 2,
                "time": 2.0,
                "comment": "replay",
                "colour": "ABCDEF",
            },
        ],
        "scores": [
            {
                "id": 200,
                "map_md5": map_md5,
                "score": 1_000_000,
                "pp": 123.45,
                "acc": 98.5,
                "max_combo": 3,
                "mods": 0,
                "n300": 3,
                "n100": 0,
                "n50": 0,
                "nmiss": 0,
                "ngeki": 0,
                "nkatu": 0,
                "grade": "A",
                "status": 2,
                "mode": 0,
                "play_time": instant,
                "time_elapsed": 3000,
                "client_flags": 0,
                "userid": 2,
                "perfect": 1,
                "online_checksum": "b" * 32,
            }
        ],
        "stats": [
            {
                "id": 2,
                "mode": 0,
                "tscore": 1_000_000,
                "rscore": 1_000_000,
                "pp": 123.45,
                "plays": 1,
                "playtime": 3,
                "acc": 98.5,
                "max_combo": 3,
                "total_hits": 3,
                "replay_views": 1,
                "xh_count": 0,
                "x_count": 0,
                "sh_count": 0,
                "s_count": 0,
                "a_count": 1,
            }
        ],
        "achievements": [{"id": 1, "file": "osu-test", "name": "Test", "desc": "Test unlock", "cond": "score >= 1"}],
        "user_achievements": [{"userid": 2, "achid": 1}],
        "logs": [{"id": 1, "from": 1, "to": 2, "action": "note", "msg": "legacy", "time": instant}],
        "tourney_pools": [{"id": 1, "name": "Test Pool", "created_at": instant, "created_by": 2}],
        "tourney_pool_maps": [{"map_id": 100, "pool_id": 1, "mods": 0, "slot": 1}],
    }
    osu_directory = tmp_path / ".data" / "osu"
    replay_directory = tmp_path / ".data" / "osr"
    osu_directory.mkdir(parents=True)
    replay_directory.mkdir(parents=True)
    (osu_directory / "100.osu").write_bytes(osu_content)
    (replay_directory / "200.osr").write_bytes(b"fixture replay")

    engine = create_target_engine(postgres_database_url)
    try:
        session_factory = await prepare_target(engine)
        config = MigrationConfig.from_values(
            source_url="mysql+pymysql://bancho:secret@127.0.0.1/bancho",
            target_url=postgres_database_url,
            data_directory=tmp_path,
            migration_id="full-fixture",
            source_timezone="UTC",
            batch_size=1,
            report_path=tmp_path / "full-report.json",
            overrides_path=None,
            confirm_offline=True,
        )
        state = MigrationStateStore(config.migration_id, session_factory)
        await state.initialize(source_fingerprint="fixture", config_digest=config.digest, started_at=datetime.now(UTC))
        runtime = MigrationRuntime(
            config=config,
            overrides=MigrationOverrides(),
            source=cast(BanchoSource, FixtureBanchoSource(tables)),
            session_factory=session_factory,
            state=state,
            report=MigrationReport(config.migration_id),
            source_schema=SourceSchema({}, "5.2.2", {"users": 2}, "fixture"),
            object_storage=cast(ObjectStorage, FakeObjectStorage()),
        )

        await migrate_identity(runtime)
        await migrate_social(runtime)
        await migrate_community(runtime)
        await migrate_content(runtime)
        await migrate_scoring(runtime)
        await migrate_multiplayer(runtime)

        assert not runtime.report.has_errors, runtime.report.diagnostics
        async with session_factory() as session:
            counts = {
                name: await session.scalar(text(f"SELECT count(*) FROM {table}"))
                for name, table in {
                    "account": "core.accounts",
                    "team": "social.teams",
                    "mail": "community.messages",
                    "map": "content.beatmaps",
                    "comment": "content.comments",
                    "score": "scoring.scores",
                    "performance": "scoring.score_performances",
                    "replay": "scoring.replays",
                    "unlock": "social.achievement_unlocks",
                    "pool_item": "multiplayer.tournament_pool_items",
                }.items()
            }
            credential = (
                await session.execute(
                    text("SELECT algorithm, pepper_version FROM iam.password_credentials WHERE account_id = 2")
                )
            ).one()
        assert counts == {
            "account": 2,
            "team": 1,
            "mail": 1,
            "map": 1,
            "comment": 2,
            "score": 1,
            "performance": 1,
            "replay": 1,
            "unlock": 1,
            "pool_item": 1,
        }
        assert credential == ("bcrypt_md5", None)
    finally:
        await engine.dispose()
