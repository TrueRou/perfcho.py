import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import cast

import httpx
import pytest
from fastapi import FastAPI

from perfcho.api.stable import router
from perfcho.api.stable.dependencies import get_stable_services
from perfcho.infra.composition import StableServices
from perfcho.infra.settings import Settings
from perfcho.modules.authorization import AuthorizationQueryService
from perfcho.modules.common import Clock, IdGenerator, ObjectStorage, StoredObject
from perfcho.modules.community import CommunityService
from perfcho.modules.content import (
    BeatmapRevisionView,
    BeatmapsetView,
    ContentQueryService,
    ContentSearch,
    ContentSearchPage,
    ContentService,
    FavouriteResult,
    RatingSummary,
)
from perfcho.modules.identity import IdentityService, InvalidCredentials, StableWebPrincipal
from perfcho.modules.realtime import RealtimeRepository, RealtimeSession, RealtimeSessionNotFound
from perfcho.modules.scoring import BeatmapGradeView, RankingQueryService, Ruleset, ScoreGrade
from perfcho.modules.social import AccountIdentityView, FollowView, SocialService

NOW = datetime(2026, 7, 29, 12, 30, tzinfo=UTC)
PASSWORD_MD5 = "a" * 32
BEATMAP_MD5 = "b" * 32
SESSION_ID = uuid.uuid5(uuid.NAMESPACE_URL, "perfcho:test:stable-web-session")


class FixedClock:
    def now(self) -> datetime:
        return NOW


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
    def __init__(self, beatmap: BeatmapRevisionView, beatmapset: BeatmapsetView) -> None:
        self.beatmap = beatmap
        self.beatmapset = beatmapset
        self.search_query: ContentSearch | None = None
        self.account_rating: int | None = None

    async def batch_lookup(
        self,
        file_names: tuple[str, ...],
        external_beatmap_ids: tuple[int, ...],
    ) -> tuple[BeatmapRevisionView, ...]:
        assert file_names or external_beatmap_ids
        return (self.beatmap,)

    async def search(self, query: ContentSearch) -> ContentSearchPage:
        self.search_query = query
        return ContentSearchPage((self.beatmapset,), True)

    async def get_beatmapset(self, beatmapset_id: int, *, external: bool = True) -> BeatmapsetView:
        assert external and beatmapset_id == self.beatmapset.external_beatmapset_id
        return self.beatmapset

    async def lookup_beatmap(self, beatmap_id: int, *, external: bool = True) -> BeatmapRevisionView:
        assert external and beatmap_id == self.beatmap.external_beatmap_id
        return self.beatmap

    async def lookup_md5(self, md5: str | bytes) -> BeatmapRevisionView:
        assert md5 == BEATMAP_MD5
        return self.beatmap

    async def lookup_filename(self, file_name: str) -> BeatmapRevisionView:
        assert file_name == self.beatmap.file_name
        return self.beatmap

    async def list_favourites(self, account_id: int) -> tuple[int, ...]:
        assert account_id == 3
        return (200, 300)

    async def get_rating(self, beatmap_id: int, account_id: int | None = None) -> RatingSummary:
        assert beatmap_id == 10 and account_id == 3
        return RatingSummary(10, Decimal("8.25"), 4, self.account_rating)


class FakeContent:
    def __init__(self) -> None:
        self.favourite_calls: list[tuple[int, int]] = []
        self.rating_calls: list[tuple[int, int, int]] = []

    async def set_favourite(
        self,
        account_id: int,
        beatmapset_id: int,
        favourited: bool = True,
    ) -> FavouriteResult:
        assert favourited
        self.favourite_calls.append((account_id, beatmapset_id))
        return FavouriteResult(account_id, beatmapset_id, True, True)

    async def rate(self, account_id: int, beatmap_id: int, rating: int) -> RatingSummary:
        self.rating_calls.append((account_id, beatmap_id, rating))
        return RatingSummary(beatmap_id, Decimal("8.50"), 5, rating)


class FakeRankingQuery:
    async def get_beatmap_grades(
        self,
        account_id: int,
        beatmap_ids: tuple[int, ...],
    ) -> tuple[BeatmapGradeView, ...]:
        assert account_id == 3 and beatmap_ids == (10,)
        return (BeatmapGradeView(10, Ruleset.OSU, ScoreGrade.S),)


class FakeSocial:
    async def list_friends(self, account_id: int) -> tuple[FollowView, ...]:
        assert account_id == 3
        return (
            FollowView(7, "friend-one", None, NOW, True),
            FollowView(9, "friend-two", "mapper", NOW, False),
        )

    async def resolve_account_by_name(self, display_name: str) -> AccountIdentityView:
        assert display_name == "friend-two"
        return AccountIdentityView(9, "friend-two")


class FakeCommunity:
    def __init__(self) -> None:
        self.mark_read_calls: list[tuple[int, int]] = []

    async def mark_direct_conversation_read(self, account_id: int, other_account_id: int) -> None:
        self.mark_read_calls.append((account_id, other_account_id))


class FakeObjectStream:
    metadata = StoredObject(
        "beatmaps/200/insane.osu",
        11,
        "application/x-osu-beatmap",
        None,
    )

    async def iter_chunks(self) -> AsyncIterator[bytes]:
        yield b"osu file "
        yield b"v1"


class FakeObjectStorage:
    def __init__(self) -> None:
        self.opened: list[str] = []

    @asynccontextmanager
    async def open(self, storage_key: str) -> AsyncIterator[FakeObjectStream]:
        self.opened.append(storage_key)
        yield FakeObjectStream()


def beatmap() -> BeatmapRevisionView:
    return BeatmapRevisionView(
        beatmap_id=10,
        external_beatmap_id=100,
        beatmapset_id=20,
        external_beatmapset_id=200,
        revision_id=30,
        md5=bytes.fromhex(BEATMAP_MD5),
        sha256=b"c" * 32,
        file_name="Artist - Title (Creator) [Insane].osu",
        artist="Artist|Name",
        title="Title",
        creator="Creator",
        difficulty_name="Insane|Diff",
        ruleset="osu",
        status="ranked",
        source_updated_at=NOW,
        total_length_ms=120_000,
        drain_length_ms=100_000,
        bpm=Decimal(180),
        circle_size=Decimal(4),
        overall_difficulty=Decimal(8),
        approach_rate=Decimal(9),
        health_drain=Decimal(6),
        object_count=500,
        max_combo=750,
        star_rating=Decimal("5.25"),
        has_video=True,
        is_current=True,
        file_storage_key="beatmaps/200/insane.osu",
        file_media_type="application/x-osu-beatmap",
        file_size_bytes=11,
    )


def stable_services() -> tuple[StableServices, FakeContentQuery, FakeContent, FakeCommunity]:
    map_view = beatmap()
    set_view = BeatmapsetView(
        beatmapset_id=20,
        external_beatmapset_id=200,
        artist=map_view.artist,
        title=map_view.title,
        creator=map_view.creator,
        status=map_view.status,
        last_updated_at=NOW,
        available=True,
        has_video=True,
        beatmaps=(map_view,),
    )
    content_query = FakeContentQuery(map_view, set_view)
    content = FakeContent()
    community = FakeCommunity()
    services = StableServices(
        identity=cast(IdentityService, FakeIdentity()),
        authorization=cast(AuthorizationQueryService, object()),
        realtime=cast(RealtimeRepository, FakeRealtime()),
        clock=cast(Clock, FixedClock()),
        id_generator=cast(IdGenerator, object()),
        settings=Settings(),
        content_query=cast(ContentQueryService, content_query),
        content=cast(ContentService, content),
        social=cast(SocialService, FakeSocial()),
        community=cast(CommunityService, community),
        object_storage=cast(ObjectStorage, FakeObjectStorage()),
        ranking_query=cast(RankingQueryService, FakeRankingQuery()),
    )
    return services, content_query, content, community


def stable_app(services: StableServices) -> FastAPI:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_stable_services] = lambda: services
    return app


@pytest.mark.asyncio
async def test_web_credentials_require_the_matching_realtime_epoch() -> None:
    services, _, _, _ = stable_services()
    realtime = cast(FakeRealtime, services.realtime)
    realtime.online = False
    app = stable_app(services)

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://c.test") as client:
        response = await client.get("/web/osu-getfriends.php", params=auth_params())

    assert response.status_code == 401


@pytest.mark.asyncio
async def test_web_auth_rejection_log_excludes_identifier_and_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    import importlib

    web_module = importlib.import_module("perfcho.api.stable.router.web")
    events: list[tuple[str, dict[str, object]]] = []

    def capture(level: str, event: str, **fields: object) -> None:
        del level
        events.append((event, fields))

    monkeypatch.setattr(web_module, "log_event", capture)
    monkeypatch.setattr(web_module, "rate_limit", lambda key, **kwargs: True)
    services, _, _, _ = stable_services()
    app = stable_app(services)
    rejected_token = "0" * 32

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://c.test") as client:
        response = await client.get(
            "/web/osu-getfriends.php",
            params={"u": "player", "h": rejected_token},
        )

    assert response.status_code == 401
    assert events == [
        (
            "stable.web.auth_rejected",
            {
                "outcome": "rejected",
                "error_code": "invalid_credentials",
                "error_type": "InvalidCredentials",
            },
        )
    ]
    assert "player" not in repr(events)
    assert rejected_token not in repr(events)


def auth_params(*, password_name: str = "h") -> dict[str, str]:
    return {"u": "player", password_name: PASSWORD_MD5}


@pytest.mark.asyncio
async def test_friends_and_beatmap_info_use_online_web_credentials() -> None:
    services, _, _, _ = stable_services()
    app = stable_app(services)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://c.test") as client:
        friends = await client.get("/web/osu-getfriends.php", params=auth_params())
        info = await client.post(
            "/web/osu-getbeatmapinfo.php",
            params=auth_params(),
            json={
                "Filenames": ["Artist - Title (Creator) [Insane].osu"],
                "Ids": [100],
            },
        )
        invalid = await client.get(
            "/web/osu-getfriends.php",
            params={"u": "player", "h": "0" * 32},
        )

    assert friends.text == "1\n7\n9"
    assert info.text.splitlines() == [
        f"0|100|200|{BEATMAP_MD5}|1|S|N|N|N",
        f"1|100|200|{BEATMAP_MD5}|1|S|N|N|N",
    ]
    assert invalid.status_code == 401
    assert invalid.content == b""


@pytest.mark.asyncio
async def test_mark_as_read_resolves_target_and_advances_authoritative_conversation_cursor() -> None:
    services, _, _, community = stable_services()
    app = stable_app(services)

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://c.test") as client:
        response = await client.get(
            "/web/osu-markasread.php",
            params={**auth_params(), "channel": "friend-two"},
        )
        invalid = await client.get(
            "/web/osu-markasread.php",
            params={"u": "player", "h": "0" * 32, "channel": "friend-two"},
        )

    assert response.status_code == 200
    assert response.content == b""
    assert community.mark_read_calls == [(3, 9)]
    assert invalid.status_code == 401


@pytest.mark.asyncio
async def test_direct_search_serializes_stable_fourteen_field_contract() -> None:
    services, content_query, _, _ = stable_services()
    app = stable_app(services)
    params = {**auth_params(), "r": "0", "q": "test", "m": "0", "p": "2"}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://c.test") as client:
        response = await client.get("/web/osu-search.php", params=params)

    lines = response.text.splitlines()
    assert lines[0] == "101"
    fields = lines[1].split("|")
    assert len(fields) == 14
    assert fields[:5] == ["200.osz", "ArtistIName", "Title", "Creator", "2"]
    assert fields[7:13] == ["200", "0", "1", "0", "0", "0"]
    assert "InsaneIDiff" in fields[13]
    assert content_query.search_query == ContentSearch(
        query="test",
        ruleset="osu",
        statuses=("ranked", "approved"),
        page=2,
    )


@pytest.mark.asyncio
async def test_direct_set_favourites_and_rating_handshake() -> None:
    services, _, content, _ = stable_services()
    app = stable_app(services)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://c.test") as client:
        direct_set = await client.get(
            "/web/osu-search-set.php",
            params={**auth_params(), "c": BEATMAP_MD5},
        )
        favourites = await client.get("/web/osu-getfavourites.php", params=auth_params())
        added = await client.get(
            "/web/osu-addfavourite.php",
            params={**auth_params(), "a": "200"},
        )
        can_rate = await client.get(
            "/web/osu-rate.php",
            params={**auth_params(password_name="p"), "c": BEATMAP_MD5},
        )
        rated = await client.get(
            "/web/osu-rate.php",
            params={**auth_params(password_name="p"), "c": BEATMAP_MD5, "v": "9"},
        )

    assert direct_set.text.startswith("200.osz|ArtistIName|Title|Creator|2|")
    assert favourites.text == "200\n300"
    assert added.text == "Added favourite!"
    assert can_rate.text == "ok"
    assert rated.text == "alreadyvoted\n8.50"
    assert content.favourite_calls == [(3, 200)]
    assert content.rating_calls == [(3, 10, 9)]


@pytest.mark.asyncio
async def test_probes_and_direct_download_redirect() -> None:
    services, _, _, _ = stable_services()
    app = stable_app(services)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://c.test",
        follow_redirects=False,
    ) as client:
        connect = await client.get("/web/bancho_connect.php", params={"v": "b20260711.1"})
        update = await client.get(
            "/web/check-updates.php",
            params={"action": "check", "stream": "stable40"},
        )
        download = await client.get("/d/200")
        no_video_download = await client.get("/d/200n")
        map_file = await client.get("/web/maps/Artist%20-%20Title%20(Creator)%20%5BInsane%5D.osu")

    assert connect.content == b""
    assert update.content == b""
    assert download.status_code == 302
    assert download.headers["location"] == "https://api.nerinyan.moe/d/200"
    assert no_video_download.headers["location"] == "https://api.nerinyan.moe/d/200?nv=1"
    assert map_file.content == b"osu file v1"
    assert map_file.headers["content-type"] == "application/x-osu-beatmap"
