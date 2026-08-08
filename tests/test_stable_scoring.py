import hashlib
import uuid
from base64 import b64encode
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import cast

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.api.cho import router
from perfcho.api.cho.dependencies import get_stable_services
from perfcho.infra.compose import StableServices
from perfcho.infra.db.models.scoring import RankingPolicy, Score
from perfcho.infra.db.projectors.ranking import _metric_value, _tie_break_value
from perfcho.infra.security.rijndael import Rijndael256Cbc
from perfcho.infra.settings import Settings
from perfcho.modules.authorization import AuthorizationQueryService
from perfcho.modules.common import Clock, IdGenerator, ObjectStorage, StoredObject
from perfcho.modules.content import (
    BeatmapNotFound,
    BeatmapRevisionView,
    ContentQueryService,
    ContentSyncService,
    RatingSummary,
    UpstreamContentUnavailable,
)
from perfcho.modules.identity import IdentityService, InvalidCredentials, StableWebPrincipal
from perfcho.modules.realtime import RealtimeRepository, RealtimeSession, RealtimeSessionNotFound
from perfcho.modules.scoring import (
    AcceptedScoreResult,
    AcceptScore,
    AccountStatsView,
    LeaderboardPage,
    LeaderboardScoreView,
    RankingQueryService,
    ReplayQueryService,
    ReplayReference,
    ReplayService,
    Ruleset,
    ScoreboardVariant,
    ScoreOutcome,
    ScoreRejected,
    ScoringService,
)
from perfcho.modules.social import AchievementUnlockView, FollowView, SocialService

NOW = datetime(2026, 7, 29, 12, 30, tzinfo=UTC)
PASSWORD_MD5 = "c" * 32
BEATMAP_MD5 = "a" * 32
OSU_VERSION = "20260711"
REPLAY_CONTENT = b"stable replay frame payload"
SESSION_ID = uuid.uuid5(uuid.NAMESPACE_URL, "perfcho:test:stable-scoring-session")


class FixedClock:
    def now(self) -> datetime:
        return NOW


class FakeIds:
    def __init__(self) -> None:
        self.values: list[uuid.UUID] = []

    def new(self) -> uuid.UUID:
        value = uuid.uuid7()
        self.values.append(value)
        return value


class FakeIdentity:
    async def verify_stable_web(self, identifier: str, password_token: str) -> StableWebPrincipal:
        if identifier != "player" or password_token != PASSWORD_MD5:
            raise InvalidCredentials()
        return StableWebPrincipal(3, "player", SESSION_ID, NOW + timedelta(hours=1))


class FakeRealtime:
    def __init__(self) -> None:
        self.online = True

    async def resolve_session(self, session_id: uuid.UUID, *, at: datetime) -> RealtimeSession:
        assert session_id == SESSION_ID and at == NOW
        if not self.online:
            raise RealtimeSessionNotFound()
        return RealtimeSession(SESSION_ID, 3, 1, NOW + timedelta(minutes=5))


class FakeContentQuery:
    def __init__(self, beatmap: BeatmapRevisionView) -> None:
        self.beatmap = beatmap
        self.missing = False

    async def lookup_md5(self, md5: str | bytes) -> BeatmapRevisionView:
        assert md5 in {BEATMAP_MD5, bytes.fromhex(BEATMAP_MD5)}
        if self.missing:
            raise BeatmapNotFound()
        return self.beatmap

    async def get_rating(self, beatmap_id: int, account_id: int | None = None) -> RatingSummary:
        assert beatmap_id == 10 and account_id == 3
        return RatingSummary(10, None, 0, None)


class FakeContentSync:
    def __init__(self, resolved: BeatmapRevisionView) -> None:
        self.resolved = resolved
        self.error: Exception | None = None
        self.resolve_calls: list[tuple[str | bytes, str, int | None]] = []
        self.refreshes: list[int] = []

    async def resolve_revision(
        self,
        md5: str | bytes,
        file_name: str,
        external_beatmapset_id: int | None,
    ) -> BeatmapRevisionView:
        self.resolve_calls.append((md5, file_name, external_beatmapset_id))
        if self.error is not None:
            raise self.error
        return self.resolved

    async def refresh_if_due(self, external_beatmapset_id: int) -> None:
        self.refreshes.append(external_beatmapset_id)


class FakeScoring:
    def __init__(self) -> None:
        self.commands: list[AcceptScore] = []
        self.error: Exception | None = None
        self.new_unlocks: tuple[AchievementUnlockView, ...] = ()
        self._returned_unlocks = False

    async def accept(self, command: AcceptScore) -> AcceptedScoreResult:
        self.commands.append(command)
        if self.error is not None:
            raise self.error
        unlocks = () if self._returned_unlocks else self.new_unlocks
        self._returned_unlocks = True
        return AcceptedScoreResult(
            attempt_id=uuid.uuid7(),
            score_id=40,
            beatmap_id=10,
            beatmap_revision_id=20,
            scoreboard_id=1,
            mod_set_id=30,
            outcome=command.score.outcome,
            new_achievement_unlocks=unlocks,
        )


class FakeObjectStream:
    metadata = StoredObject("replays/stable/3/replay.osr", len(REPLAY_CONTENT), "application/octet-stream", None)

    async def iter_chunks(self) -> AsyncIterator[bytes]:
        yield REPLAY_CONTENT[:8]
        yield REPLAY_CONTENT[8:]


class FakeStorage:
    def __init__(self) -> None:
        self.puts: list[tuple[str, bytes]] = []
        self.opens: list[str] = []

    async def put(
        self,
        storage_key: str,
        content: bytes,
        *,
        media_type: str,
        expected_sha256: bytes | None = None,
    ) -> StoredObject:
        self.puts.append((storage_key, content))
        return StoredObject(storage_key, len(content), media_type, expected_sha256)

    @asynccontextmanager
    async def open(self, storage_key: str) -> AsyncIterator[FakeObjectStream]:
        self.opens.append(storage_key)
        yield FakeObjectStream()


class FakeReplayQuery:
    async def get(self, score_id: int) -> ReplayReference:
        assert score_id == 40
        return ReplayReference(40, 7, 1, Ruleset.OSU, "replays/stable/3/replay.osr", len(REPLAY_CONTENT), "stable")


class FakeReplayService:
    def __init__(self) -> None:
        self.views: list[tuple[uuid.UUID, ReplayReference, int | None]] = []

    async def record_view(
        self,
        *,
        request_id: uuid.UUID,
        replay: ReplayReference,
        viewer_account_id: int | None,
    ) -> None:
        self.views.append((request_id, replay, viewer_account_id))


class FakeRankingQuery:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []
        self.stats = [
            AccountStatsView(100, Decimal("0.95"), 4, 1_000, 8, 120),
            AccountStatsView(200, Decimal("0.96"), 5, 2_000, 6, 130),
        ]

    async def get_stable_leaderboard(self, **kwargs: object) -> LeaderboardPage:
        self.calls.append(kwargs)
        score = LeaderboardScoreView(
            score_id=40,
            account_id=7,
            display_name="friend",
            metric_value=Decimal(1_000_000),
            max_combo=10,
            n50=0,
            n100=0,
            n300=10,
            nmiss=0,
            nkatu=0,
            ngeki=0,
            perfect=True,
            legacy_mod_bits=0,
            rank=1,
            ended_at=NOW,
            has_replay=True,
        )
        return LeaderboardPage((score,), score)

    async def get_account_stats(
        self,
        account_id: int,
        ruleset: Ruleset,
        variant: ScoreboardVariant,
    ) -> AccountStatsView:
        del account_id, ruleset, variant
        return self.stats.pop(0)


class FakeSocial:
    async def list_friends(self, account_id: int) -> tuple[FollowView, ...]:
        assert account_id == 3
        return (FollowView(7, "friend", None, NOW, True),)

    async def list_incoming_follower_account_ids(
        self,
        target_account_id: int,
        candidate_actor_account_ids: tuple[int, ...],
    ) -> frozenset[int]:
        assert target_account_id == 3
        del candidate_actor_account_ids
        return frozenset()


def beatmap() -> BeatmapRevisionView:
    return BeatmapRevisionView(
        beatmap_id=10,
        external_beatmap_id=100,
        beatmapset_id=20,
        external_beatmapset_id=200,
        revision_id=20,
        md5=bytes.fromhex(BEATMAP_MD5),
        sha256=b"s" * 32,
        file_name="Artist - Title (Creator) [Test].osu",
        artist="Artist",
        title="Title",
        creator="Creator",
        difficulty_name="Test",
        ruleset="osu",
        status="ranked",
        source_updated_at=NOW,
        total_length_ms=60_000,
        drain_length_ms=50_000,
        bpm=Decimal(180),
        circle_size=Decimal(4),
        overall_difficulty=Decimal(8),
        approach_rate=Decimal(9),
        health_drain=Decimal(6),
        object_count=10,
        max_combo=10,
        star_rating=Decimal(1),
        has_video=False,
        is_current=True,
    )


def stable_services() -> tuple[StableServices, FakeScoring, FakeStorage, FakeReplayService, FakeRankingQuery]:
    scoring = FakeScoring()
    storage = FakeStorage()
    replay = FakeReplayService()
    ranking = FakeRankingQuery()
    services = StableServices(
        identity=cast(IdentityService, FakeIdentity()),
        authorization=cast(AuthorizationQueryService, object()),
        realtime=cast(RealtimeRepository, FakeRealtime()),
        clock=cast(Clock, FixedClock()),
        id_generator=cast(IdGenerator, FakeIds()),
        settings=Settings(),
        content_query=cast(ContentQueryService, FakeContentQuery(beatmap())),
        content_sync=cast(ContentSyncService, FakeContentSync(beatmap())),
        object_storage=cast(ObjectStorage, storage),
        scoring=cast(ScoringService, scoring),
        replay_query=cast(ReplayQueryService, FakeReplayQuery()),
        replay=cast(ReplayService, replay),
        ranking_query=cast(RankingQueryService, ranking),
        social=cast(SocialService, FakeSocial()),
    )
    return services, scoring, storage, replay, ranking


def stable_app(services: StableServices) -> FastAPI:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_stable_services] = lambda: services
    return app


def encrypted_score(
    *,
    checksum: str | None = None,
    play_time: str = "260729123000",
    passed: bool = True,
) -> tuple[str, str, str]:
    fields = [
        BEATMAP_MD5,
        "player ",
        "",
        "10",
        "0",
        "0",
        "0",
        "0",
        "0",
        "1000000",
        "10",
        "True" if passed else "False",
        "X" if passed else "F",
        "0",
        "True" if passed else "False",
        "0",
        play_time,
        OSU_VERSION,
        "0",
    ]
    checksum_payload = (
        f"chickenmcnuggets10o1500smustard00uu{BEATMAP_MD5}10Trueplayer1000000X0QTrue0"
        f"{OSU_VERSION}{play_time}client-hash"
    )
    fields[2] = checksum or hashlib.md5(checksum_payload.encode(), usedforsecurity=False).hexdigest()
    iv = b"i" * 32
    cipher = Rijndael256Cbc(
        key=f"osu!-scoreburgr---------{OSU_VERSION}".encode(),
        iv=iv,
    )
    score = b64encode(cipher.encrypt(":".join(fields).encode())).decode()
    client_hash = b64encode(cipher.encrypt(b"client-hash")).decode()
    return score, client_hash, b64encode(iv).decode()


def submission_files(
    password: str = PASSWORD_MD5,
    *,
    checksum: str | None = None,
    storyboard_hash: str = "",
    replay: bytes = REPLAY_CONTENT,
    unique_ids: str = "uninstall-id|disk-id",
    play_time: str = "260729123000",
    passed: bool = True,
    fail_time_ms: int = 0,
) -> list[tuple[str, tuple]]:
    score, client_hash, iv = encrypted_score(checksum=checksum, play_time=play_time, passed=passed)
    return [
        ("score", (None, score)),
        ("score", ("replay.osr", replay, "application/octet-stream")),
        ("x", (None, "0")),
        ("ft", (None, str(fail_time_ms))),
        ("st", (None, "60000")),
        ("pass", (None, password)),
        ("osuver", (None, OSU_VERSION)),
        ("s", (None, client_hash)),
        ("iv", (None, iv)),
        ("bmk", (None, BEATMAP_MD5)),
        ("sbk", (None, storyboard_hash)),
        ("c1", (None, unique_ids)),
    ]


def replace_form_field(files: list[tuple[str, tuple]], name: str, value: str) -> list[tuple[str, tuple]]:
    return [(key, (None, value) if key == name else item) for key, item in files]


@pytest.mark.asyncio
async def test_stable_score_submission_stages_replay_and_calls_canonical_service() -> None:
    services, scoring, storage, _, _ = stable_services()
    app = stable_app(services)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://c.test") as client:
        response = await client.post("/web/osu-submit-modular-selector.php", files=submission_files())

    assert response.status_code == 200
    assert "beatmapId:100" in response.text
    assert "onlineScoreId:40" in response.text
    assert storage.puts[0][1] == REPLAY_CONTENT
    command = scoring.commands[0]
    assert command.meta.actor is not None and command.meta.actor.account_id == 3
    assert command.replay is not None
    assert command.replay.storage_key.startswith("replays/stable/3/")
    assert command.attestation.client_integrity_digest is not None
    assert command.attestation.checksum == command.score.online_checksum
    assert command.attestation.checksum != command.meta.request_digest
    assert command.attestation.verification_state == "pending"
    assert "rankedScoreBefore:100" in response.text
    assert "rankedScoreAfter:200" in response.text
    assert "totalScoreBefore:1000" in response.text
    assert "totalScoreAfter:2000" in response.text
    assert "ppBefore:120" in response.text
    assert "ppAfter:130" in response.text


@pytest.mark.asyncio
async def test_stable_score_chart_renders_only_unlocks_created_by_this_submission() -> None:
    services, scoring, _, _, _ = stable_services()
    scoring.new_unlocks = (
        AchievementUnlockView(7, "million-score", "Million Score", "Reach one million", NOW),
        AchievementUnlockView(8, "full-combo", "Full Combo", "Complete a full combo", NOW),
    )
    app = stable_app(services)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://c.test") as client:
        first = await client.post("/web/osu-submit-modular-selector.php", files=submission_files())
        second = await client.post("/web/osu-submit-modular-selector.php", files=submission_files())

    assert (
        "achievements-new:million-score+Million Score+Reach one million/full-combo+Full Combo+Complete a full combo"
    ) in first.text
    assert "achievements-new:" in second.text
    assert "million-score+Million Score+Reach one million" not in second.text
    assert "full-combo+Full Combo+Complete a full combo" not in second.text


@pytest.mark.asyncio
async def test_score_submission_logs_stages_domain_code_and_no_submission_secrets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import importlib

    web_module = importlib.import_module("perfcho.api.cho.router.web")
    events: list[tuple[str, str, dict[str, object]]] = []

    def capture(level: str, event: str, **fields: object) -> None:
        events.append((level, event, fields))

    monkeypatch.setattr(web_module, "log_event", capture)
    services, scoring, storage, _, _ = stable_services()
    app = stable_app(services)
    accepted_files = submission_files()

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://c.test") as client:
        accepted = await client.post("/web/osu-submit-modular-selector.php", files=accepted_files)
        scoring.error = ScoreRejected("rejected")
        rejected = await client.post("/web/osu-submit-modular-selector.php", files=submission_files())

    assert accepted.status_code == rejected.status_code == 200
    assert any(
        event == "stable.score_submission.stage" and fields["stage"] == "object_staged" for _, event, fields in events
    )
    assert any(event == "stable.score_submission.completed" and fields["score_id"] == 40 for _, event, fields in events)
    assert any(
        event == "stable.score_submission.rejected"
        and fields["error_code"] == "score_rejected"
        and fields["error_type"] == "ScoreRejected"
        for _, event, fields in events
    )
    rendered = repr(events)
    encrypted_value = cast(str, accepted_files[0][1][1])
    for secret in (
        PASSWORD_MD5,
        BEATMAP_MD5,
        encrypted_value,
        "client-hash",
        storage.puts[0][0],
        REPLAY_CONTENT.decode(),
    ):
        assert secret not in rendered


@pytest.mark.asyncio
async def test_stable_score_submission_rejects_invalid_password_before_object_write() -> None:
    services, scoring, storage, _, _ = stable_services()
    app = stable_app(services)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://c.test") as client:
        response = await client.post(
            "/web/osu-submit-modular-selector.php",
            files=submission_files(password="0" * 32),
        )

    assert response.text == "error: pass"
    assert storage.puts == []
    assert scoring.commands == []


@pytest.mark.asyncio
async def test_stable_score_submission_rejects_checksum_timing_and_short_replay_as_text() -> None:
    services, scoring, storage, _, _ = stable_services()
    app = stable_app(services)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://c.test") as client:
        checksum = await client.post(
            "/web/osu-submit-modular-selector.php",
            files=submission_files(checksum="b" * 32, storyboard_hash="c" * 32),
        )
        timing = await client.post(
            "/web/osu-submit-modular-selector.php",
            files=replace_form_field(submission_files(), "st", str(10**20)),
        )
        fail_timing = await client.post(
            "/web/osu-submit-modular-selector.php",
            files=replace_form_field(submission_files(), "ft", str(10**20)),
        )
        storyboard = await client.post(
            "/web/osu-submit-modular-selector.php",
            files=replace_form_field(submission_files(), "sbk", "not-an-md5"),
        )
        client_ids = await client.post(
            "/web/osu-submit-modular-selector.php",
            files=replace_form_field(submission_files(), "c1", "one-component"),
        )
        stale_time = await client.post(
            "/web/osu-submit-modular-selector.php",
            files=submission_files(play_time="690101000000"),
        )
        replay = await client.post(
            "/web/osu-submit-modular-selector.php",
            files=submission_files(replay=b"too short"),
        )

    assert {
        checksum.text,
        timing.text,
        fail_timing.text,
        storyboard.text,
        client_ids.text,
        stale_time.text,
        replay.text,
    } == {"error: no"}
    assert storage.puts == []
    assert scoring.commands == []


@pytest.mark.asyncio
@pytest.mark.parametrize("replay", [b"", b"failed replay"])
async def test_stable_failed_score_allows_an_incomplete_replay_payload(replay: bytes) -> None:
    services, scoring, storage, _, _ = stable_services()
    app = stable_app(services)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://c.test") as client:
        response = await client.post(
            "/web/osu-submit-modular-selector.php",
            files=submission_files(passed=False, fail_time_ms=30_000, replay=replay),
        )

    assert response.status_code == 200
    assert response.text == "error: no"
    assert storage.puts == []
    assert scoring.commands[0].replay is None
    assert scoring.commands[0].score.outcome is ScoreOutcome.FAILED


@pytest.mark.asyncio
async def test_stable_score_request_digest_covers_unique_client_fields() -> None:
    services, scoring, _, _, _ = stable_services()
    app = stable_app(services)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://c.test") as client:
        first = await client.post(
            "/web/osu-submit-modular-selector.php",
            files=submission_files(unique_ids="uninstall-a|disk-a"),
        )
        second = await client.post(
            "/web/osu-submit-modular-selector.php",
            files=submission_files(unique_ids="uninstall-b|disk-b"),
        )

    assert first.status_code == second.status_code == 200
    assert scoring.commands[0].meta.request_digest != scoring.commands[1].meta.request_digest


@pytest.mark.asyncio
async def test_stable_score_chunked_multipart_is_bounded_before_form_parsing() -> None:
    services, scoring, storage, _, _ = stable_services()
    services = replace(services, settings=Settings(stable_score_submission_max_bytes=1024))
    app = stable_app(services)
    encoded = httpx.Request(
        "POST",
        "http://c.test/web/osu-submit-modular-selector.php",
        files=submission_files(replay=b"r" * 2048),
    )
    body = encoded.read()

    async def chunks() -> AsyncIterator[bytes]:
        for offset in range(0, len(body), 127):
            yield body[offset : offset + 127]

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://c.test") as client:
        response = await client.post(
            "/web/osu-submit-modular-selector.php",
            content=chunks(),
            headers={"Content-Type": encoded.headers["Content-Type"]},
        )

    assert response.text == "error: no"
    assert "content-length" not in encoded.headers or int(encoded.headers["content-length"]) > 1024
    assert storage.puts == []
    assert scoring.commands == []


@pytest.mark.asyncio
async def test_stable_score_expected_application_error_uses_stable_text() -> None:
    services, scoring, storage, _, _ = stable_services()
    scoring.error = ScoreRejected("rejected")
    app = stable_app(services)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://c.test") as client:
        response = await client.post("/web/osu-submit-modular-selector.php", files=submission_files())

    assert response.status_code == 200
    assert response.text == "error: no"
    assert storage.puts


@pytest.mark.asyncio
async def test_stable_replay_download_streams_object_and_records_non_owner_view() -> None:
    services, _, storage, replay_service, _ = stable_services()
    app = stable_app(services)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://c.test") as client:
        response = await client.get(
            "/web/osu-getreplay.php",
            params={"u": "player", "h": PASSWORD_MD5, "m": "0", "c": "40"},
        )

    assert response.content == REPLAY_CONTENT
    assert storage.opens == ["replays/stable/3/replay.osr"]
    assert replay_service.views[0][2] == 3


@pytest.mark.asyncio
async def test_stable_leaderboard_serializes_projection_and_friend_filter() -> None:
    services, _, _, _, ranking = stable_services()
    app = stable_app(services)
    params = {
        "us": "player",
        "ha": PASSWORD_MD5,
        "s": "false",
        "vv": "4",
        "v": "3",
        "c": BEATMAP_MD5,
        "f": "Artist - Title (Creator) [Test].osu",
        "m": "0",
        "i": "200",
        "mods": "0",
        "h": "package-hash",
        "a": "false",
    }
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://c.test") as client:
        response = await client.get("/web/osu-osz2-getscores.php", params=params)

    lines = response.text.splitlines()
    assert lines[0] == "2|false|100|200|1|0|"
    assert lines[1:4] == ["0", "Artist - Title [Test]", "0"]
    personal_fields = lines[4].split("|")
    score_fields = lines[5].split("|")
    assert len(personal_fields) == len(score_fields) == 16
    assert personal_fields[:3] == ["40", "friend", "1000000"]
    assert score_fields[-3:] == ["1", str(int(NOW.timestamp())), "1"]
    assert ranking.calls[0]["leaderboard_type"] == 3
    assert "friend_account_ids" not in ranking.calls[0]
    assert cast(FakeContentSync, services.content_sync).refreshes == [200]


@pytest.mark.asyncio
async def test_stable_leaderboard_blocks_to_complete_unknown_current_beatmap() -> None:
    services, _, _, _, ranking = stable_services()
    cast(FakeContentQuery, services.content_query).missing = True
    app = stable_app(services)
    params = {
        "us": "player",
        "ha": PASSWORD_MD5,
        "s": "false",
        "vv": "4",
        "v": "1",
        "c": BEATMAP_MD5,
        "f": "Artist - Title (Creator) [Test].osu",
        "m": "0",
        "i": "200",
        "mods": "0",
        "h": "package-hash",
        "a": "false",
    }
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://c.test") as client:
        response = await client.get("/web/osu-osz2-getscores.php", params=params)

    assert response.text.startswith("2|false|100|200|1|0|")
    assert ranking.calls
    sync = cast(FakeContentSync, services.content_sync)
    assert sync.resolve_calls == [(BEATMAP_MD5, params["f"], 200)]
    assert sync.refreshes == [200]


@pytest.mark.asyncio
async def test_stable_leaderboard_reports_update_only_for_confirmed_replacement() -> None:
    services, _, _, _, ranking = stable_services()
    cast(FakeContentQuery, services.content_query).missing = True
    cast(FakeContentSync, services.content_sync).resolved = replace(
        beatmap(),
        md5=bytes.fromhex("b" * 32),
    )
    app = stable_app(services)
    params = {
        "us": "player",
        "ha": PASSWORD_MD5,
        "s": "false",
        "vv": "4",
        "v": "1",
        "c": BEATMAP_MD5,
        "f": "Artist - Title (Creator) [Test].osu",
        "m": "0",
        "i": "200",
        "mods": "0",
        "h": "package-hash",
        "a": "false",
    }
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://c.test") as client:
        response = await client.get("/web/osu-osz2-getscores.php", params=params)

    assert response.text == "1|false"
    assert ranking.calls == []


@pytest.mark.asyncio
async def test_stable_leaderboard_reports_known_historical_revision_as_client_update() -> None:
    services, _, _, _, ranking = stable_services()
    cast(FakeContentQuery, services.content_query).beatmap = replace(beatmap(), is_current=False)
    app = stable_app(services)
    params = {
        "us": "player",
        "ha": PASSWORD_MD5,
        "s": "false",
        "vv": "4",
        "v": "1",
        "c": BEATMAP_MD5,
        "f": "Artist - Title (Creator) [Test].osu",
        "m": "0",
        "i": "200",
        "mods": "0",
        "h": "package-hash",
        "a": "false",
    }
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://c.test") as client:
        response = await client.get("/web/osu-osz2-getscores.php", params=params)

    assert response.text == "1|false"
    assert ranking.calls == []
    sync = cast(FakeContentSync, services.content_sync)
    assert sync.resolve_calls == []
    assert sync.refreshes == [200]


@pytest.mark.asyncio
async def test_stable_leaderboard_does_not_misreport_upstream_failure_as_missing() -> None:
    services, _, _, _, _ = stable_services()
    cast(FakeContentQuery, services.content_query).missing = True
    cast(FakeContentSync, services.content_sync).error = UpstreamContentUnavailable()
    app = stable_app(services)
    params = {
        "us": "player",
        "ha": PASSWORD_MD5,
        "s": "false",
        "vv": "4",
        "v": "1",
        "c": BEATMAP_MD5,
        "f": "Artist - Title (Creator) [Test].osu",
        "m": "0",
        "i": "200",
        "mods": "0",
        "h": "package-hash",
        "a": "false",
    }
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://c.test") as client:
        response = await client.get("/web/osu-osz2-getscores.php", params=params)

    assert response.status_code == 503
    assert response.content == b""


@pytest.mark.asyncio
async def test_ranking_uses_exact_microseconds_and_defers_unconfigured_pp() -> None:
    ended_at = datetime(2026, 7, 29, 12, 30, 0, 123456, tzinfo=UTC)
    score = cast(Score, SimpleNamespace(ended_at=ended_at))
    expected = int((ended_at - datetime(1970, 1, 1, tzinfo=UTC)).total_seconds()) * 1_000_000 + 123456

    assert _tie_break_value(score, "ended_at") == Decimal(expected)
    assert (
        await _metric_value(
            cast(AsyncSession, None),
            score,
            cast(RankingPolicy, SimpleNamespace(metric="pp", calculation_release_id=None)),
        )
        is None
    )
