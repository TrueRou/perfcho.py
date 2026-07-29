import uuid
from base64 import b64encode
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import cast

import httpx
import pytest
from fastapi import FastAPI
from py3rijndael import Pkcs7Padding, RijndaelCbc

from perfcho.api.stable import router
from perfcho.api.stable.dependencies import get_stable_services
from perfcho.composition import StableServices
from perfcho.infra.settings import Settings
from perfcho.modules.authorization import AuthorizationQueryService
from perfcho.modules.common import Clock, IdGenerator, ObjectStorage, StoredObject
from perfcho.modules.content import BeatmapRevisionView, ContentQueryService, RatingSummary
from perfcho.modules.identity import IdentityService, InvalidCredentials, StableWebPrincipal
from perfcho.modules.realtime import RealtimeRepository
from perfcho.modules.scoring import (
    AcceptedScoreResult,
    AcceptScore,
    LeaderboardPage,
    LeaderboardScoreView,
    RankingQueryService,
    ReplayQueryService,
    ReplayReference,
    ReplayService,
    Ruleset,
    ScoringService,
)
from perfcho.modules.social import FollowView, SocialService

NOW = datetime(2026, 7, 29, 12, 30, tzinfo=UTC)
PASSWORD_MD5 = "c" * 32
BEATMAP_MD5 = "a" * 32
OSU_VERSION = "20260711"
REPLAY_CONTENT = b"stable replay frames"


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
        return StableWebPrincipal(3, "player", uuid.uuid7(), NOW + timedelta(hours=1))


class FakeContentQuery:
    def __init__(self, beatmap: BeatmapRevisionView) -> None:
        self.beatmap = beatmap

    async def lookup_md5(self, md5: str | bytes) -> BeatmapRevisionView:
        assert md5 in {BEATMAP_MD5, bytes.fromhex(BEATMAP_MD5)}
        return self.beatmap

    async def get_rating(self, beatmapset_id: int, account_id: int | None = None) -> RatingSummary:
        assert beatmapset_id == 200 and account_id == 3
        return RatingSummary(200, None, 0, None)


class FakeScoring:
    def __init__(self) -> None:
        self.commands: list[AcceptScore] = []

    async def accept(self, command: AcceptScore) -> AcceptedScoreResult:
        self.commands.append(command)
        return AcceptedScoreResult(
            attempt_id=uuid.uuid7(),
            score_id=40,
            beatmap_id=10,
            beatmap_revision_id=20,
            scoreboard_id=1,
            mod_set_id=30,
            outcome=command.score.outcome,
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
        return ReplayReference(40, 7, Ruleset.OSU, "replays/stable/3/replay.osr", len(REPLAY_CONTENT), "stable")


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


class FakeSocial:
    async def list_friends(self, account_id: int) -> tuple[FollowView, ...]:
        assert account_id == 3
        return (FollowView(7, "friend", None, NOW, True),)


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
        realtime=cast(RealtimeRepository, object()),
        clock=cast(Clock, FixedClock()),
        id_generator=cast(IdGenerator, FakeIds()),
        settings=Settings(),
        content_query=cast(ContentQueryService, FakeContentQuery(beatmap())),
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


def encrypted_score() -> tuple[str, str, str]:
    fields = [
        BEATMAP_MD5,
        "player ",
        "b" * 32,
        "10",
        "0",
        "0",
        "0",
        "0",
        "0",
        "1000000",
        "10",
        "True",
        "X",
        "0",
        "True",
        "0",
        "260729123000",
        "b20260711.1",
    ]
    iv = b"i" * 32
    cipher = RijndaelCbc(
        key=f"osu!-scoreburgr---------{OSU_VERSION}".encode(),
        iv=iv,
        padding=Pkcs7Padding(32),
        block_size=32,
    )
    score = b64encode(cipher.encrypt(":".join(fields).encode())).decode()
    client_hash = b64encode(cipher.encrypt(b"client-hash")).decode()
    return score, client_hash, b64encode(iv).decode()


def submission_files(password: str = PASSWORD_MD5) -> list[tuple[str, tuple]]:
    score, client_hash, iv = encrypted_score()
    return [
        ("score", (None, score)),
        ("score", ("replay.osr", REPLAY_CONTENT, "application/octet-stream")),
        ("x", (None, "0")),
        ("ft", (None, "0")),
        ("st", (None, "60000")),
        ("pass", (None, password)),
        ("osuver", (None, OSU_VERSION)),
        ("s", (None, client_hash)),
        ("iv", (None, iv)),
        ("bmk", (None, BEATMAP_MD5)),
        ("sbk", (None, "")),
        ("c1", (None, "device-identifiers")),
    ]


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
    assert command.replay.storage_key.startswith("replays/stable/3/")
    assert command.attestation.client_integrity_digest is not None


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
    assert ranking.calls[0]["friend_account_ids"] == (7,)
