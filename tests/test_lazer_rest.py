"""Tests for the osu!lazer REST adapters over canonical services."""

import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import cast

import httpx
import pytest
from fastapi import FastAPI

from perfcho.api import router
from perfcho.api.canonical.dependencies import get_canonical_services
from perfcho.infra.compose import StableServices
from perfcho.infra.settings import Settings
from perfcho.modules.account import PublicAccountView
from perfcho.modules.authorization import AuthorizationQueryService
from perfcho.modules.common import Clock, IdGenerator
from perfcho.modules.content import (
    BeatmapRevisionView,
    BeatmapsetView,
    CommentView,
    ContentSearch,
    ContentSearchPage,
    RatingSummary,
)
from perfcho.modules.identity import AuthenticatedAccount, IdentityService
from perfcho.modules.realtime import RealtimeStateRepository
from perfcho.modules.scoring import Ruleset
from perfcho.modules.social import BlockView, FollowView, SocialService

NOW = datetime(2026, 8, 10, 12, tzinfo=UTC)


class FakeIdentity:
    async def authenticate_access_token(self, token: str) -> AuthenticatedAccount:
        return AuthenticatedAccount(
            account_id=42,
            current_name="Alice",
            account_type="user",
            country_code="JP",
            registered_at=NOW,
            last_seen_at=NOW,
            session_id=uuid.uuid7(),
            scope_codes=("public", "identify", "lazer"),
        )


class FakeAccount:
    def __init__(self) -> None:
        self.lookups: list[tuple[str, str]] = []

    async def get_public(self, lookup: str | int, *, key: str = "id") -> PublicAccountView | None:
        self.lookups.append((str(lookup), key))
        return PublicAccountView(
            account_id=int(lookup),
            current_name="Bob",
            account_type="user",
            country_code="US",
            registered_at=NOW,
            last_seen_at=NOW,
            default_ruleset=Ruleset.OSU,
        )


class FakeSocial:
    def __init__(self) -> None:
        self.follows: list[tuple[int, int]] = []
        self.unfollows: list[tuple[int, int]] = []
        self.blocks: list[tuple[int, int]] = []
        self.unblocks: list[tuple[int, int]] = []

    async def list_friends(self, account_id: int) -> tuple[FollowView, ...]:
        return (FollowView(7, "Friend", None, NOW, True),)

    async def list_blocks(self, account_id: int) -> tuple[BlockView, ...]:
        return (BlockView(9, "Blocked", None, NOW),)

    async def follow(self, actor: int, target: int, *, remark: str | None = None) -> None:
        self.follows.append((actor, target))

    async def unfollow(self, actor: int, target: int) -> bool:
        self.unfollows.append((actor, target))
        return True

    async def block(self, actor: int, target: int, *, reason: str | None = None) -> None:
        self.blocks.append((actor, target))

    async def unblock(self, actor: int, target: int) -> bool:
        self.unblocks.append((actor, target))
        return True


class FakeContentQuery:
    async def lookup_beatmap(self, beatmap_id: int, *, external: bool = True) -> BeatmapRevisionView:
        return _beatmap_view(beatmap_id)

    async def lookup_md5(self, md5: str | bytes) -> BeatmapRevisionView:
        return _beatmap_view(100)

    async def lookup_filename(self, file_name: str) -> BeatmapRevisionView:
        return _beatmap_view(100)

    async def batch_lookup(self, names: tuple[str, ...], ids: tuple[int, ...]) -> tuple[BeatmapRevisionView, ...]:
        return tuple(_beatmap_view(i) for i in ids)

    async def get_beatmapset(self, beatmapset_id: int, *, external: bool = True) -> BeatmapsetView:
        return _beatmapset_view(beatmapset_id)

    async def search(self, query: ContentSearch) -> ContentSearchPage:
        return ContentSearchPage((_beatmapset_view(1),), has_more=False)

    async def list_favourites(self, account_id: int) -> tuple[int, ...]:
        return (1, 2, 3)

    async def get_rating(self, beatmap_id: int, account_id: int | None = None) -> RatingSummary:
        return RatingSummary(beatmap_id, Decimal("8.0"), 2, 8)

    async def list_comments(self, target: str, external_target_id: int) -> tuple[CommentView, ...]:
        return (CommentView(1, 42, target, 0, "nice", None, NOW),)


class FakeContent:
    def __init__(self) -> None:
        self.favourites: list[tuple[int, int, bool]] = []

    async def set_favourite(self, account_id: int, beatmapset_id: int, favourited: bool = True):
        self.favourites.append((account_id, beatmapset_id, favourited))
        return type("F", (), {"account_id": account_id, "beatmapset_id": beatmapset_id, "favourited": favourited, "changed": True})()

    async def create_comment(
        self, account_id: int, target: str, external_target_id: int, position_ms: int, body: str
    ) -> CommentView:
        return CommentView(2, account_id, target, position_ms, body, None, NOW)


class FakeCommunity:
    def __init__(self) -> None:
        self.sent_public: list[tuple[int, int, str]] = []
        self.sent_direct: list[tuple[int, int, str]] = []

    async def list_public_channels(self, account_id: int):
        return (type("C", (), {"channel_id": 5, "name": "#osu", "topic": "t", "auto_join": False, "message_length_limit": 1000, "can_write": True, "can_manage": False})(),)

    async def get_public_channel(self, account_id: int, selector):
        return type("C", (), {"channel_id": 5, "name": "#osu", "topic": "t", "auto_join": False, "message_length_limit": 1000, "can_write": True, "can_manage": False})()

    async def send_public_message(self, sender, selector, client_message_id, content, *, is_action=False, reply_to_id=None):
        self.sent_public.append((sender, selector.channel_id, content))
        return _msg(sender, selector.channel_id, content, is_action)

    async def send_direct_message(self, sender, recipient, client_message_id, content, *, is_action=False, reply_to_id=None):
        self.sent_direct.append((sender, recipient, content))
        return _msg(sender, 999, content, is_action, recipient)

    async def join_channel(self, account_id: int, channel_id: int):
        return type("M", (), {"channel_id": channel_id, "account_id": account_id, "joined": True, "durable": True, "changed": True})()

    async def leave_channel(self, account_id: int, channel_id: int):
        return type("M", (), {"channel_id": channel_id, "account_id": account_id, "joined": False, "durable": True, "changed": True})()

    async def mark_read(self, account_id: int, channel_id: int, message_id: int):
        return type("R", (), {"channel_id": channel_id, "account_id": account_id, "last_read_message_id": message_id, "advanced": True})()


def _beatmap_view(beatmap_id: int) -> BeatmapRevisionView:
    return BeatmapRevisionView(
        beatmap_id=beatmap_id,
        external_beatmap_id=beatmap_id,
        beatmapset_id=1,
        external_beatmapset_id=1,
        revision_id=1,
        md5=b"0" * 16,
        sha256=b"0" * 32,
        file_name="artist - title [diff].osu",
        artist="artist",
        title="title",
        creator="creator",
        difficulty_name="diff",
        ruleset="osu",
        status="ranked",
        source_updated_at=NOW,
        total_length_ms=120000,
        drain_length_ms=100000,
        bpm=Decimal("180"),
        circle_size=Decimal("4"),
        overall_difficulty=Decimal("8"),
        approach_rate=Decimal("9"),
        health_drain=Decimal("6"),
        object_count=500,
        max_combo=600,
        star_rating=Decimal("5.2"),
        has_video=False,
        is_current=True,
    )


def _beatmapset_view(beatmapset_id: int) -> BeatmapsetView:
    return BeatmapsetView(
        beatmapset_id=beatmapset_id,
        external_beatmapset_id=beatmapset_id,
        artist="artist",
        title="title",
        creator="creator",
        status="ranked",
        last_updated_at=NOW,
        available=True,
        has_video=False,
        beatmaps=(_beatmap_view(100 + beatmapset_id),),
    )


def _msg(sender: int, channel_id: int, content: str, is_action: bool, recipient: int | None = None):
    from perfcho.modules.community import MessageResult

    return MessageResult(
        message_id=10,
        channel_id=channel_id,
        sender_account_id=sender,
        client_message_id=uuid.uuid7(),
        content=content,
        is_action=is_action,
        reply_to_id=None,
        created_at=NOW,
        direct_recipient_account_id=recipient,
    )


class FakeStats:
    async def get_for_display(self, account_id: int, ruleset: Ruleset):
        from perfcho.modules.scoring import AccountStatsView

        return AccountStatsView(0, Decimal(0), 0, 0, None)


def build_app(
    *,
    account: FakeAccount | None = None,
    social: FakeSocial | None = None,
    content_query: FakeContentQuery | None = None,
    content: FakeContent | None = None,
    community: FakeCommunity | None = None,
) -> FastAPI:
    services = StableServices(
        identity=cast(IdentityService, FakeIdentity()),
        authorization=cast(AuthorizationQueryService, object()),
        realtime=cast(RealtimeStateRepository, object()),
        clock=cast(Clock, object()),
        id_generator=cast(IdGenerator, object()),
        settings=Settings(argon2_time_cost=1, argon2_memory_cost_kib=8, argon2_parallelism=1),
        account=cast(object, account),
        social=cast(SocialService, social),
        content_query=cast(object, content_query),
        content=cast(object, content),
        community=cast(object, community),
        account_statistics=cast(object, FakeStats()),
    )
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_canonical_services] = lambda: services
    return app


@pytest.mark.asyncio
async def test_get_user_returns_lazer_shape() -> None:
    app = build_app(account=FakeAccount())
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://api.test") as client:
        response = await client.get("/api/v2/users/7/osu")
    assert response.status_code == 200
    body = response.json()
    assert body["id"] == 7
    assert body["username"] == "Bob"
    assert body["country_code"] == "US"
    assert body["statistics_rulesets"]["osu"]["pp"] == 0


@pytest.mark.asyncio
async def test_friends_and_blocks_lists() -> None:
    social = FakeSocial()
    app = build_app(social=social)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://api.test") as client:
        headers = {"Authorization": "Bearer access-value"}
        friends = await client.get("/api/v2/friends", headers=headers)
        blocks = await client.get("/api/v2/blocks", headers=headers)
    assert friends.json()[0]["target_id"] == 7
    assert friends.json()[0]["mutual"] is True
    assert blocks.json()[0]["target_id"] == 9


@pytest.mark.asyncio
async def test_add_remove_friend_and_block() -> None:
    social = FakeSocial()
    app = build_app(social=social)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://api.test") as client:
        headers = {"Authorization": "Bearer access-value"}
        await client.post("/api/v2/friends?target=55", headers=headers)
        await client.delete("/api/v2/friends/55", headers=headers)
        await client.post("/api/v2/blocks?target=66", headers=headers)
        await client.delete("/api/v2/blocks/66", headers=headers)
    assert social.follows == [(42, 55)]
    assert social.unfollows == [(42, 55)]
    assert social.blocks == [(42, 66)]
    assert social.unblocks == [(42, 66)]


@pytest.mark.asyncio
async def test_beatmap_lookup_by_id() -> None:
    app = build_app(content_query=FakeContentQuery())
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://api.test") as client:
        response = await client.get("/api/v2/beatmaps/lookup?id=100")
    assert response.status_code == 200
    assert response.json()["id"] == 100
    assert response.json()["status"] == 1  # ranked


@pytest.mark.asyncio
async def test_beatmapset_search_and_favourites() -> None:
    content = FakeContent()
    app = build_app(content_query=FakeContentQuery(), content=content)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://api.test") as client:
        headers = {"Authorization": "Bearer access-value"}
        search = await client.get("/api/v2/beatmapsets/search?query=title", headers=headers)
        fav = await client.get("/api/v2/me/beatmapset-favourites", headers=headers)
        await client.post("/api/v2/beatmapsets/1/favourites?action=favourite", headers=headers)
    assert search.json()["beatmapsets"][0]["id"] == 1
    assert fav.json()["beatmapsets"] == [1, 2, 3]
    assert content.favourites == [(42, 1, True)]


@pytest.mark.asyncio
async def test_chat_list_and_send() -> None:
    community = FakeCommunity()
    app = build_app(community=community)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://api.test") as client:
        headers = {"Authorization": "Bearer access-value"}
        channels = await client.get("/api/v2/chat/channels", headers=headers)
        sent = await client.post(
            "/api/v2/chat/channels/5/messages", headers=headers, data={"message": "hello", "uuid": str(uuid.uuid7())}
        )
    assert channels.json()[0]["channel_id"] == 5
    assert sent.json()["message_id"] == 10
    assert community.sent_public == [(42, 5, "hello")]


class FakeRankingQuery:
    async def list_rankings(self, *, ruleset, sort="performance", country_code=None, page=0, page_size=50):
        from perfcho.modules.scoring import UserRankingPage, UserRankingView

        return UserRankingPage(
            (UserRankingView(7, "Top", "JP", 1, 1000, 5000000, __import__("decimal").Decimal("0.99")),),
            has_more=False,
            total_count=1,
        )


class FakeScoreQuery:
    async def list_user_scores(self, *, account_id, ruleset=None, score_type="best", limit=50):
        from decimal import Decimal

        from perfcho.modules.scoring import Ruleset, ScoreDetailView, ScoreGrade, ScoreOutcome

        return (
            ScoreDetailView(
                score_id=900,
                account_id=account_id,
                display_name="Bob",
                country_code="US",
                beatmap_id=123,
                ruleset=Ruleset.OSU,
                total_score=100,
                classic_score=90,
                accuracy=Decimal("0.9"),
                max_combo=10,
                grade=ScoreGrade.A,
                outcome=ScoreOutcome.PASSED,
                mods=(),
                statistics={},
                maximum_statistics={},
                started_at=NOW,
                ended_at=NOW,
                has_replay=False,
                ranked=True,
                pp=None,
                position=None,
            ),
        )


def build_ranking_app(ranking_query=None, score_query=None) -> FastAPI:
    services = StableServices(
        identity=cast(IdentityService, FakeIdentity()),
        authorization=cast(AuthorizationQueryService, object()),
        realtime=cast(RealtimeStateRepository, object()),
        clock=cast(Clock, object()),
        id_generator=cast(IdGenerator, object()),
        settings=Settings(argon2_time_cost=1, argon2_memory_cost_kib=8, argon2_parallelism=1),
        ranking_query=cast(object, ranking_query),
        score_query=cast(object, score_query),
    )
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_canonical_services] = lambda: services
    return app


@pytest.mark.asyncio
async def test_rankings_endpoint() -> None:
    app = build_ranking_app(ranking_query=FakeRankingQuery())
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://api.test") as client:
        response = await client.get("/api/v2/rankings/osu/performance")
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert body["ranking"][0]["user"]["id"] == 7
    assert body["ranking"][0]["pp"] == 1000


@pytest.mark.asyncio
async def test_user_scores_endpoint() -> None:
    app = build_ranking_app(score_query=FakeScoreQuery())
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://api.test") as client:
        response = await client.get("/api/v2/users/7/scores/best")
    assert response.status_code == 200
    assert response.json()[0]["id"] == 900


class FakeMultiplayer:
    async def list_public_rooms(self, *, limit=100):
        return ()

    async def get_room(self, public_id):
        return _room_state(public_id)

    async def create_room(self, command):
        return _room_state(1)

    async def join_room(self, command):
        return _room_state(command.public_id)

    async def leave_room(self, command):
        return None

    async def find_room_for_account(self, account_id):
        return None


def _room_state(public_id: int):
    from datetime import UTC, datetime, timedelta

    from perfcho.modules.multiplayer import (
        RoomRecord,
        RoomSlot,
        RoomSettings,
        RoomState,
        SlotStatus,
        TeamMode,
        WinCondition,
    )
    from perfcho.modules.scoring import Ruleset, ScoreboardVariant

    room = RoomRecord(
        room_id=uuid.uuid4(),
        public_id=public_id,
        session_id=uuid.uuid4(),
        version=1,
        creator_account_id=42,
        host_account_id=42,
        capacity=16,
        settings=RoomSettings(
            name="test room",
            beatmap_name="map",
            external_beatmap_id=100,
            beatmap_md5=None,
            ruleset=Ruleset.OSU,
            variant=ScoreboardVariant.VANILLA,
            team_mode=TeamMode.HEAD_TO_HEAD,
            win_condition=WinCondition.SCORE,
        ),
    )
    slots = tuple(
        RoomSlot(i, SlotStatus.NOT_READY if i == 0 else SlotStatus.OPEN, 42 if i == 0 else None) for i in range(16)
    )
    return RoomState(room, 1, slots, False, datetime.now(UTC) + timedelta(hours=1))


def build_mp_app(multiplayer=None) -> FastAPI:
    services = StableServices(
        identity=cast(IdentityService, FakeIdentity()),
        authorization=cast(AuthorizationQueryService, object()),
        realtime=cast(RealtimeStateRepository, object()),
        clock=cast(Clock, object()),
        id_generator=cast(IdGenerator, object()),
        settings=Settings(argon2_time_cost=1, argon2_memory_cost_kib=8, argon2_parallelism=1),
        multiplayer=cast(object, multiplayer),
    )
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_canonical_services] = lambda: services
    return app


@pytest.mark.asyncio
async def test_rooms_list_and_get() -> None:
    app = build_mp_app(multiplayer=FakeMultiplayer())
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://api.test") as client:
        headers = {"Authorization": "Bearer access-value"}
        room = await client.get("/api/v2/rooms/1", headers=headers)
        rooms = await client.get("/api/v2/rooms", headers=headers)
    assert room.status_code == 200
    body = room.json()
    assert body["roomID"] == 1
    assert body["users"][0]["role"] == "Host"
    assert rooms.json()["rooms"] == []
