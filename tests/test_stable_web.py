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
from perfcho.composition import StableServices
from perfcho.infra.settings import Settings
from perfcho.modules.authorization import AuthorizationQueryService
from perfcho.modules.common import Clock, IdGenerator, ObjectStorage, StoredObject
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
from perfcho.modules.realtime import RealtimeRepository
from perfcho.modules.social import FollowView, SocialService

NOW = datetime(2026, 7, 29, 12, 30, tzinfo=UTC)
PASSWORD_MD5 = "a" * 32
BEATMAP_MD5 = "b" * 32


class FakeIdentity:
    async def verify_stable_web(self, identifier: str, password_token: str) -> StableWebPrincipal:
        if identifier != "player" or password_token != PASSWORD_MD5:
            raise InvalidCredentials()
        return StableWebPrincipal(3, "player", uuid.uuid7(), NOW + timedelta(hours=1))


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

    async def get_rating(self, beatmapset_id: int, account_id: int | None = None) -> RatingSummary:
        assert beatmapset_id == 200 and account_id == 3
        return RatingSummary(200, Decimal("8.25"), 4, self.account_rating)


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

    async def rate(self, account_id: int, beatmapset_id: int, rating: int) -> RatingSummary:
        self.rating_calls.append((account_id, beatmapset_id, rating))
        return RatingSummary(beatmapset_id, Decimal("8.50"), 5, rating)


class FakeSocial:
    async def list_friends(self, account_id: int) -> tuple[FollowView, ...]:
        assert account_id == 3
        return (
            FollowView(7, "friend-one", None, NOW, True),
            FollowView(9, "friend-two", "mapper", NOW, False),
        )


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


def stable_services() -> tuple[StableServices, FakeContentQuery, FakeContent]:
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
    services = StableServices(
        identity=cast(IdentityService, FakeIdentity()),
        authorization=cast(AuthorizationQueryService, object()),
        realtime=cast(RealtimeRepository, object()),
        clock=cast(Clock, object()),
        id_generator=cast(IdGenerator, object()),
        settings=Settings(),
        content_query=cast(ContentQueryService, content_query),
        content=cast(ContentService, content),
        social=cast(SocialService, FakeSocial()),
        object_storage=cast(ObjectStorage, FakeObjectStorage()),
    )
    return services, content_query, content


def stable_app(services: StableServices) -> FastAPI:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_stable_services] = lambda: services
    return app


def auth_params(*, password_name: str = "h") -> dict[str, str]:
    return {"u": "player", password_name: PASSWORD_MD5}


@pytest.mark.asyncio
async def test_friends_and_beatmap_info_use_online_web_credentials() -> None:
    services, _, _ = stable_services()
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

    assert friends.text == "7\n9"
    assert info.text.splitlines() == [
        f"0|100|200|{BEATMAP_MD5}|1|N|N|N|N",
        f"1|100|200|{BEATMAP_MD5}|1|N|N|N|N",
    ]
    assert invalid.status_code == 401
    assert invalid.content == b""


@pytest.mark.asyncio
async def test_direct_search_serializes_stable_fourteen_field_contract() -> None:
    services, content_query, _ = stable_services()
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
    services, _, content = stable_services()
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
    assert content.rating_calls == [(3, 200, 9)]


@pytest.mark.asyncio
async def test_probes_and_direct_download_redirect() -> None:
    services, _, _ = stable_services()
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
    assert download.headers["location"] == "https://osu.ppy.sh/beatmapsets/200/download"
    assert no_video_download.headers["location"] == "https://osu.ppy.sh/beatmapsets/200/download?noVideo=1"
    assert map_file.content == b"osu file v1"
    assert map_file.headers["content-type"] == "application/x-osu-beatmap"
