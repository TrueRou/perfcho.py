import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import cast

import pytest

from perfcho.api.stable.bubbles import StableBubbleRenderer, canonicalize_presence
from perfcho.api.stable.canonize.scoring import LEGACY_MOD_BITS
from perfcho.api.stable.channels import parse_stable_channel_selector, stable_channel_name
from perfcho.api.stable.dispatcher import StableRuntimeContext
from perfcho.api.stable.dispatcher import dispatch_packets as dispatch_bubbles
from perfcho.api.stable.realtime import (
    ClientPacket,
    ClientStatus,
    Message,
    PacketReader,
    PacketWriter,
    ServerPacket,
    UserPresence,
    UserStats,
    build_packet,
    user_stats,
)
from perfcho.api.stable.realtime.countries import stable_country_id
from perfcho.infra.compose import StableServices
from perfcho.infra.settings import Settings
from perfcho.modules.authorization import AuthorizationQueryService
from perfcho.modules.bot import BotCommandService, BotInvocation, CommandResult
from perfcho.modules.common import Clock, IdGenerator
from perfcho.modules.community import (
    ChannelMembershipRequired,
    ChannelSelector,
    ChannelView,
    CommunityInputRejected,
    CommunityService,
    DirectMessageBlocked,
    MessageResult,
    TargetAccountSilenced,
)
from perfcho.modules.identity import IdentityService, ResolvedClientSession
from perfcho.modules.multiplayer import CleanupPresence, MultiplayerService
from perfcho.modules.realtime import (
    PresenceSnapshot,
    PresenceSubscription,
    RealtimeBubble,
    RealtimeBubbleBus,
    RealtimeSession,
    RealtimeStateRepository,
    SessionFence,
)
from perfcho.modules.scoring import AccountStatsView, RankingQueryService
from perfcho.modules.social import AccountIdentityView, SocialInteractionBlocked, SocialService

NOW = datetime(2026, 7, 29, 12, tzinfo=UTC)


async def dispatch_packets(body: bytes, context: StableRuntimeContext, services: StableServices) -> bytes:
    bubbles = await dispatch_bubbles(body, context, services)
    rendered = StableBubbleRenderer().render_many(bubbles, max_bytes=services.settings.stable_max_response_bytes)
    return rendered + bytes(context.stable_output)


EXPIRY = NOW + timedelta(minutes=5)


def test_stable_channel_names_are_parsed_and_rendered_at_the_adapter_boundary() -> None:
    assert parse_stable_channel_selector(" #OsU ") == ChannelSelector(name="osu")
    assert stable_channel_name(ChannelView(7, "osu", "general", False, 256, True, False)) == "#osu"


def test_stable_country_ids_match_legacy_table() -> None:
    assert stable_country_id("JP") == 111
    assert stable_country_id("US") == 225
    assert stable_country_id("xx") == 244
    assert stable_country_id(None) == 244


class FixedClock:
    def now(self) -> datetime:
        return NOW


class FakeIds:
    def new(self) -> uuid.UUID:
        return uuid.uuid7()


class FakeIdentity:
    def __init__(self) -> None:
        self.closed: tuple[str, str] | None = None

    async def close_client_session(self, raw_token: str, *, reason: str) -> None:
        self.closed = (raw_token, reason)


class FakeMultiplayer:
    def __init__(self) -> None:
        self.cleanup_commands: list[object] = []

    async def find_room_for_account(self, account_id: int) -> None:
        assert account_id == 10
        return None

    async def list_public_rooms(self, *, limit: int) -> tuple[object, ...]:
        assert limit == 100
        return ()

    async def cleanup_presence(self, command: object) -> None:
        self.cleanup_commands.append(command)
        return None


class FakeSocial:
    def __init__(self) -> None:
        self.blocked_recipients: frozenset[int] = frozenset()
        self.follow_error: Exception | None = None
        self.filtered: tuple[int, ...] | None = None
        self.incoming_followers = frozenset({20})
        self.incoming_follower_queries: list[tuple[int, tuple[int, ...]]] = []

    async def resolve_account_by_name(self, display_name: str) -> AccountIdentityView:
        if display_name.casefold() == "banchobot":
            return AccountIdentityView(1, "BanchoBot")
        assert display_name == "target"
        return AccountIdentityView(20, "target")

    async def list_incoming_follower_account_ids(
        self,
        target_account_id: int,
        candidate_actor_account_ids: tuple[int, ...],
    ) -> frozenset[int]:
        self.incoming_follower_queries.append((target_account_id, candidate_actor_account_ids))
        return self.incoming_followers.intersection(candidate_actor_account_ids)

    async def filter_message_recipients(
        self,
        sender_account_id: int,
        recipient_account_ids: tuple[int, ...],
    ) -> tuple[int, ...]:
        assert sender_account_id == 10
        self.filtered = recipient_account_ids
        return tuple(account_id for account_id in recipient_account_ids if account_id not in self.blocked_recipients)

    async def follow(self, actor_account_id: int, target_account_id: int) -> object:
        del actor_account_id, target_account_id
        if self.follow_error is not None:
            raise self.follow_error
        return object()

    async def unfollow(self, actor_account_id: int, target_account_id: int) -> bool:
        del actor_account_id, target_account_id
        return True

    async def list_friends(self, account_id: int) -> tuple[object, ...]:
        assert account_id == 10
        return (SimpleNamespace(account_id=20),)


class FakeCommunity:
    def __init__(self, *, blocked: bool = False, error: Exception | None = None) -> None:
        self.error = DirectMessageBlocked() if blocked else error
        self.sent: tuple[int, int, str] | None = None
        self.client_message_ids: list[uuid.UUID] = []
        self.public_client_message_ids: list[uuid.UUID] = []
        self.channel = ChannelView(7, "osu", "general", False, 256, True, False)
        self.member_count = 0

    async def send_direct_message(
        self,
        sender_account_id: int,
        recipient_account_id: int,
        client_message_id: uuid.UUID,
        content: str,
    ) -> MessageResult:
        if self.error is not None:
            raise self.error
        self.sent = (sender_account_id, recipient_account_id, content)
        created = client_message_id not in self.client_message_ids
        if created:
            self.client_message_ids.append(client_message_id)
        return MessageResult(
            1,
            2,
            sender_account_id,
            client_message_id,
            content,
            False,
            None,
            NOW,
            recipient_account_id,
            created,
        )

    async def get_public_channel(self, account_id: int, selector: ChannelSelector) -> ChannelView:
        assert account_id == 10
        assert selector.name is not None
        assert selector.name.casefold() == self.channel.name.casefold()
        return self.channel

    async def get_channel_member_count(
        self,
        account_id: int,
        channel_id: int,
        *,
        already_authorized: bool = False,
    ) -> int:
        del already_authorized
        assert account_id == 10 and channel_id == self.channel.channel_id
        return self.member_count

    async def send_public_message(
        self,
        sender_account_id: int,
        channel_selector: ChannelSelector,
        client_message_id: uuid.UUID,
        content: str,
    ) -> MessageResult:
        assert sender_account_id == 10 and channel_selector == ChannelSelector(name="osu")
        if self.error is not None:
            raise self.error
        created = client_message_id not in self.public_client_message_ids
        if created:
            self.public_client_message_ids.append(client_message_id)
        return MessageResult(
            2, self.channel.channel_id, 10, client_message_id, content, False, None, NOW, created=created
        )


class FakeBot:
    bot_account_id = 1
    bot_name = "BanchoBot"

    def __init__(self, response: str = "pong") -> None:
        self.response = response
        self.invocations: list[BotInvocation] = []

    async def try_execute(self, invocation: BotInvocation) -> CommandResult | None:
        self.invocations.append(invocation)
        if not invocation.content.startswith("!"):
            return None
        return CommandResult(self.response, False, 1.0)


class FakeRankingQuery:
    async def get_account_stats(self, account_id: int, ruleset: object) -> AccountStatsView:
        assert account_id == 10
        del ruleset
        return AccountStatsView(123_456, Decimal("0.987654"), 12, 2_000_000, 4, 321)


class FakeRealtime:
    def __init__(self, presences: tuple[PresenceSnapshot, ...]) -> None:
        self.presences = {snapshot.account_id: snapshot for snapshot in presences}
        self.published: list[tuple[int, RealtimeBubble]] = []
        self.filter_value: PresenceSubscription | None = None
        self.fenced: tuple[uuid.UUID, int] | None = None
        self.away = "Away for lunch"
        self.get_presence_calls: list[int] = []
        self.failed_publish_accounts: frozenset[int] = frozenset()
        self.channel_members: set[int] = set()
        self.stored_presence: PresenceSnapshot | None = None

    async def get_presence(self, account_id: int, *, at: datetime) -> PresenceSnapshot | None:
        del at
        self.get_presence_calls.append(account_id)
        return self.presences.get(account_id)

    async def list_presences(self, *, at: datetime, limit: int) -> tuple[PresenceSnapshot, ...]:
        del at
        return tuple(self.presences.values())[:limit]

    async def set_presence(
        self,
        snapshot: PresenceSnapshot,
        *,
        session_id: uuid.UUID,
        capacity: int | None = None,
    ) -> None:
        assert snapshot.session_id == session_id
        del capacity
        self.stored_presence = snapshot
        self.presences[snapshot.account_id] = snapshot

    async def join_channel(
        self,
        channel_id: int,
        *,
        session_id: uuid.UUID,
        expected_revision: int,
    ) -> None:
        assert channel_id == 7
        assert SessionFence(session_id, expected_revision) == self.presences[10].fence
        self.channel_members.add(10)

    async def leave_channel(
        self,
        channel_id: int,
        *,
        session_id: uuid.UUID,
        expected_revision: int,
    ) -> None:
        assert channel_id == 7
        assert SessionFence(session_id, expected_revision) == self.presences[10].fence
        self.channel_members.discard(10)

    async def list_channel_members(self, channel_id: int) -> frozenset[int]:
        assert channel_id == 7
        return frozenset(self.channel_members)

    async def set_presence_subscription(
        self,
        account_id: int,
        *,
        session_id: uuid.UUID,
        expected_revision: int,
        subscription: PresenceSubscription,
    ) -> None:
        del account_id, session_id, expected_revision
        self.filter_value = subscription

    async def get_presence_subscription(self, account_id: int) -> PresenceSubscription:
        return PresenceSubscription.FOLLOWED if account_id == 20 else PresenceSubscription.NONE

    async def get_away_message(self, account_id: int) -> str:
        assert account_id == 20
        return self.away

    async def get_spectator_relation(
        self,
        account_id: int,
        *,
        spectator_fence: SessionFence,
        at: datetime,
    ) -> None:
        assert spectator_fence == self.presences[account_id].fence
        del at
        return None

    async def fence_session(self, session_id: uuid.UUID, *, expected_revision: int) -> None:
        self.fenced = (session_id, expected_revision)


class FakeBubbleBus:
    def __init__(self, realtime: FakeRealtime) -> None:
        self.realtime = realtime

    async def publish(self, recipient_fence: SessionFence, bubble: RealtimeBubble) -> int:
        account_id = next(
            account_id for account_id, presence in self.realtime.presences.items() if presence.fence == recipient_fence
        )
        if account_id in self.realtime.failed_publish_accounts:
            raise RuntimeError("publish failed")
        self.realtime.published.append((account_id, bubble))
        return 1


def snapshot(account_id: int, name: str, *, action: int = 0) -> PresenceSnapshot:
    presence = UserPresence(account_id, name, 0, 0, 1, 0, 0.0, 0.0, 0)
    stats = UserStats(account_id, action, "", "", 0, 0, 0, 0, 0.0, 0, 0, 0, 0)
    identity, activity, statistics = canonicalize_presence(presence, stats, country_code=None)
    return PresenceSnapshot(account_id, 1, identity, activity, statistics, EXPIRY, uuid.uuid7())


def context(realtime: FakeRealtime, *, opened_at: datetime | None = None) -> StableRuntimeContext:
    fence = realtime.presences[10].fence
    return StableRuntimeContext(
        ResolvedClientSession(
            10,
            "sender",
            1,
            fence.session_id,
            None,
            "b20260711.1",
            None,
            EXPIRY,
            opened_at=opened_at,
        ),
        RealtimeSession(fence.session_id, 10, fence.revision, EXPIRY),
        UserPresence(10, "sender", 0, 0, 1, 0, 0.0, 0.0, 0),
        UserStats(10, 0, "", "", 0, 0, 0, 0, 0.0, 0, 0, 0, 0),
        raw_token="raw-token",
    )


def services(
    realtime: FakeRealtime,
    *,
    identity: FakeIdentity | None = None,
    community: FakeCommunity | None = None,
    social: FakeSocial | None = None,
    settings: Settings | None = None,
    multiplayer: FakeMultiplayer | None = None,
    bot: FakeBot | None = None,
    ranking_query: FakeRankingQuery | None = None,
) -> StableServices:
    return StableServices(
        identity=cast(IdentityService, identity or FakeIdentity()),
        authorization=cast(AuthorizationQueryService, object()),
        realtime=cast(RealtimeStateRepository, realtime),
        clock=cast(Clock, FixedClock()),
        id_generator=cast(IdGenerator, FakeIds()),
        settings=settings or Settings(),
        bubbles=cast(RealtimeBubbleBus, FakeBubbleBus(realtime)),
        social=cast(SocialService, social or FakeSocial()),
        community=cast(CommunityService, community or FakeCommunity()),
        multiplayer=cast(MultiplayerService, multiplayer),
        bot=cast(BotCommandService, bot) if bot is not None else None,
        ranking_query=cast(RankingQueryService, ranking_query),
    )


def message_packet(packet_type: ClientPacket, message: Message) -> bytes:
    writer = PacketWriter()
    with writer.packet(packet_type):
        writer.write_message(message)
    return writer.to_bytes()


def status_packet(status: ClientStatus) -> bytes:
    writer = PacketWriter()
    with writer.packet(ClientPacket.CHANGE_ACTION):
        writer.write_client_status(status)
    return writer.to_bytes()


def id_request_packet(packet_type: ClientPacket, account_ids: tuple[int, ...]) -> bytes:
    writer = PacketWriter()
    with writer.packet(packet_type):
        writer.write_i32_list_u16(account_ids)
    return writer.to_bytes()


@pytest.mark.asyncio
async def test_private_message_is_persisted_published_and_returns_away_reply(monkeypatch: pytest.MonkeyPatch) -> None:
    import importlib

    dispatcher_module = importlib.import_module("perfcho.api.stable.dispatcher.packets")
    events: list[tuple[str, dict[str, object]]] = []

    def capture(level: str, event: str, **fields: object) -> None:
        del level
        events.append((event, fields))

    monkeypatch.setattr(dispatcher_module, "log_event", capture)
    monkeypatch.setattr(dispatcher_module, "sampled", lambda key, rate: True)
    realtime = FakeRealtime((snapshot(10, "sender"), snapshot(20, "target", action=1)))
    community = FakeCommunity()

    response = await dispatch_packets(
        message_packet(ClientPacket.SEND_PRIVATE_MESSAGE, Message("", "hello", "target", 0)),
        context(realtime),
        services(realtime, community=community),
    )

    assert community.sent == (10, 20, "hello")
    assert realtime.published[0][0] == 20
    delivered = next(PacketReader(StableBubbleRenderer().render(realtime.published[0][1]), packet_enum=ServerPacket))
    assert delivered.payload.read_message() == Message("sender", "hello", "target", 10)
    away = next(PacketReader(response, packet_enum=ServerPacket))
    assert away.payload.read_message().text == "Away for lunch"
    message_event = next(fields for event, fields in events if event == "stable.message.state")
    assert message_event == {
        "message_kind": "direct",
        "outcome": "delivered_with_away_reply",
        "account_id": 10,
        "message_length": 5,
        "recipient_count": 1,
        "delivered_count": 1,
        "away_message_length": 14,
        "error_code": None,
        "error_type": None,
    }
    for secret in ("hello", "sender", "target", "Away for lunch"):
        assert secret not in repr(events)


@pytest.mark.asyncio
async def test_public_bot_command_returns_bot_message_and_fans_out_to_channel_members() -> None:
    realtime = FakeRealtime((snapshot(10, "sender"), snapshot(20, "target")))
    realtime.channel_members.update((10, 20))
    bot = FakeBot()

    response = await dispatch_packets(
        message_packet(ClientPacket.SEND_PUBLIC_MESSAGE, Message("", "!ping", "#osu", 0)),
        context(realtime),
        services(realtime, bot=bot),
    )

    packet = next(PacketReader(response, packet_enum=ServerPacket))
    assert packet.packet_type is ServerPacket.SEND_MESSAGE
    assert packet.payload.read_message() == Message("BanchoBot", "pong", "#osu", 1)
    assert bot.invocations[0].sender_name == "sender"
    assert [account_id for account_id, _ in realtime.published] == [20, 20]
    delivered = next(PacketReader(StableBubbleRenderer().render(realtime.published[-1][1]), packet_enum=ServerPacket))
    assert delivered.payload.read_message() == Message("BanchoBot", "pong", "#osu", 1)


@pytest.mark.asyncio
async def test_private_message_to_banchobot_executes_without_bot_presence_lookup() -> None:
    realtime = FakeRealtime((snapshot(10, "sender"),))
    bot = FakeBot()
    community = FakeCommunity()

    response = await dispatch_packets(
        message_packet(ClientPacket.SEND_PRIVATE_MESSAGE, Message("", "!ping", "BanchoBot", 0)),
        context(realtime),
        services(realtime, bot=bot, community=community),
    )

    packet = next(PacketReader(response, packet_enum=ServerPacket))
    assert packet.packet_type is ServerPacket.SEND_MESSAGE
    assert packet.payload.read_message() == Message("BanchoBot", "pong", "sender", 1)
    assert community.sent == (10, 1, "!ping")
    assert realtime.get_presence_calls == []


@pytest.mark.asyncio
async def test_blocked_private_message_maps_to_stable_packet() -> None:
    realtime = FakeRealtime((snapshot(10, "sender"), snapshot(20, "target")))

    response = await dispatch_packets(
        message_packet(ClientPacket.SEND_PRIVATE_MESSAGE, Message("", "hello", "target", 0)),
        context(realtime),
        services(realtime, community=FakeCommunity(blocked=True)),
    )

    packet = next(PacketReader(response, packet_enum=ServerPacket))
    assert packet.packet_type is ServerPacket.USER_DM_BLOCKED
    assert not realtime.published


@pytest.mark.asyncio
async def test_presence_all_and_filter_use_bounded_realtime_state() -> None:
    realtime = FakeRealtime((snapshot(10, "sender"), snapshot(20, "target")))
    request_all = build_packet(ClientPacket.USER_PRESENCE_REQUEST_ALL, (0).to_bytes(4, "little", signed=True))
    filter_packet = build_packet(ClientPacket.RECEIVE_UPDATES, (2).to_bytes(4, "little", signed=True))

    response = await dispatch_packets(request_all + filter_packet, context(realtime), services(realtime))

    packets = list(PacketReader(response, packet_enum=ServerPacket))
    assert [packet.packet_type for packet in packets] == [ServerPacket.USER_PRESENCE, ServerPacket.USER_PRESENCE]
    assert realtime.filter_value is PresenceSubscription.FOLLOWED


@pytest.mark.asyncio
async def test_logout_closes_identity_fences_session_and_broadcasts() -> None:
    realtime = FakeRealtime((snapshot(10, "sender"), snapshot(20, "target")))
    identity = FakeIdentity()
    multiplayer = FakeMultiplayer()
    current = context(realtime)

    response = await dispatch_packets(
        build_packet(ClientPacket.LOGOUT, (0).to_bytes(4, "little", signed=True)),
        current,
        services(realtime, identity=identity, multiplayer=multiplayer),
    )

    assert response == b""
    assert identity.closed == ("raw-token", "client_logout")
    assert realtime.fenced == (current.identity.session_id, 1)
    assert len(multiplayer.cleanup_commands) == 1
    cleanup = multiplayer.cleanup_commands[0]
    assert isinstance(cleanup, CleanupPresence)
    assert cleanup.connection_session_id == current.identity.session_id
    packet = next(PacketReader(StableBubbleRenderer().render(realtime.published[0][1]), packet_enum=ServerPacket))
    assert packet.packet_type is ServerPacket.USER_LOGOUT


@pytest.mark.asyncio
async def test_presence_requests_deduplicate_exclude_self_and_share_response_budget() -> None:
    realtime = FakeRealtime((snapshot(10, "sender"), snapshot(20, "first"), snapshot(30, "second")))
    one_stats_packet_size = len(user_stats(UserStats(20, 0, "", "", 0, 0, 0, 0, 0.0, 0, 0, 0, 0)))
    config = Settings(stable_max_response_bytes=one_stats_packet_size, stable_presence_batch_size=4)

    response = await dispatch_packets(
        id_request_packet(ClientPacket.USER_STATS_REQUEST, (10, 20, 20, 30)),
        context(realtime),
        services(realtime, settings=config),
    )

    packets = list(PacketReader(response, packet_enum=ServerPacket))
    assert [packet.payload.read_user_stats().user_id for packet in packets] == [20]
    assert realtime.get_presence_calls == [20]
    assert len(response) <= config.stable_max_response_bytes


@pytest.mark.asyncio
async def test_dispatcher_client_keepalives_do_not_trigger_server_ping() -> None:
    realtime = FakeRealtime((snapshot(10, "sender"),))

    response = await dispatch_packets(
        build_packet(ClientPacket.PING) * 2,
        context(realtime),
        services(realtime, settings=Settings(stable_max_response_bytes=7)),
    )

    assert response == b""


@pytest.mark.asyncio
async def test_dispatcher_logs_expected_application_code_and_propagates_unexpected_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import importlib

    dispatcher_module = importlib.import_module("perfcho.api.stable.dispatcher.packets")
    realtime = FakeRealtime((snapshot(10, "sender"),))
    current = context(realtime)
    stable_services = services(realtime)
    events: list[tuple[str, dict[str, object]]] = []

    def capture(level: str, event: str, **fields: object) -> None:
        del level
        events.append((event, fields))

    async def expected_error(*args: object) -> bytes:
        del args
        raise CommunityInputRejected("private detail")

    monkeypatch.setattr(dispatcher_module, "log_event", capture)
    monkeypatch.setattr(dispatcher_module, "_dispatch_packets", expected_error)

    response = await dispatch_packets(b"sensitive packet bytes", current, stable_services)

    assert next(PacketReader(response, packet_enum=ServerPacket)).packet_type is ServerPacket.NOTIFICATION
    rejection = next(fields for event, fields in events if event == "stable.packet.application_rejected")
    assert rejection["error_code"] == "community_input_rejected"
    assert rejection["error_type"] == "CommunityInputRejected"
    rejection_exception = rejection["exception"]
    assert isinstance(rejection_exception, BaseException)
    assert rejection_exception.args == ("private detail",)
    assert "private detail" in repr(events)
    assert "sensitive packet bytes" not in repr(events)

    async def unexpected_error(*args: object) -> bytes:
        del args
        raise RuntimeError("unexpected")

    monkeypatch.setattr(dispatcher_module, "_dispatch_packets", unexpected_error)
    with pytest.raises(RuntimeError, match="unexpected"):
        await dispatch_packets(b"", current, stable_services)


@pytest.mark.asyncio
async def test_change_action_normalizes_assistance_and_stores_consistent_presence() -> None:
    realtime = FakeRealtime((snapshot(10, "sender"), snapshot(20, "target")))
    social = FakeSocial()
    status = ClientStatus(
        13,
        "playing",
        "a" * 32,
        LEGACY_MOD_BITS["AP"] | LEGACY_MOD_BITS["RX"],
        3,
        55,
    )

    response = await dispatch_packets(status_packet(status), context(realtime), services(realtime, social=social))

    response_stats = next(PacketReader(response, packet_enum=ServerPacket)).payload.read_user_stats()
    assert response_stats.mode == 3
    assert response_stats.mods & (LEGACY_MOD_BITS["AP"] | LEGACY_MOD_BITS["RX"]) == 0
    assert realtime.stored_presence is not None
    assert realtime.stored_presence.session_id == realtime.presences[10].session_id
    assert realtime.stored_presence.activity.ruleset == "mania"
    assert realtime.stored_presence.statistics.global_rank is None
    assert social.incoming_follower_queries == [(10, (20,))]
    assert [recipient_account_id for recipient_account_id, _ in realtime.published] == [20]


@pytest.mark.asyncio
async def test_presence_broadcast_bounds_follower_candidates_to_online_snapshot_batch() -> None:
    realtime = FakeRealtime((snapshot(10, "sender"), snapshot(20, "first"), snapshot(30, "outside-batch")))
    social = FakeSocial()
    config = Settings(stable_presence_batch_size=2)
    status = ClientStatus(1, "playing", "a" * 32, 0, 0, 0)

    await dispatch_packets(
        status_packet(status),
        context(realtime),
        services(realtime, social=social, settings=config),
    )

    assert social.incoming_follower_queries == [(10, (20,))]
    assert [recipient_account_id for recipient_account_id, _ in realtime.published] == [20]


@pytest.mark.asyncio
async def test_status_request_refreshes_fenced_authoritative_snapshot() -> None:
    realtime = FakeRealtime((snapshot(10, "sender"),))

    await dispatch_packets(build_packet(ClientPacket.REQUEST_STATUS_UPDATE), context(realtime), services(realtime))

    assert realtime.stored_presence is not None
    assert realtime.stored_presence.fence == realtime.presences[10].fence


@pytest.mark.asyncio
async def test_status_request_refreshes_mode_specific_authoritative_stats() -> None:
    realtime = FakeRealtime((snapshot(10, "sender"),))
    current = replace(
        context(realtime),
        presence=UserPresence(10, "sender", 0, 0, 1, 3, 0.0, 0.0, 0),
        stats=UserStats(10, 0, "", "", 0, 3, 0, 0, 0.0, 0, 0, 0, 0),
    )

    response = await dispatch_packets(
        build_packet(ClientPacket.REQUEST_STATUS_UPDATE),
        current,
        services(realtime, ranking_query=FakeRankingQuery()),
    )

    stats = next(PacketReader(response, packet_enum=ServerPacket)).payload.read_user_stats()
    assert (stats.mode, stats.play_count, stats.total_score, stats.performance) == (3, 12, 2_000_000, 321)
    assert realtime.stored_presence is not None
    assert realtime.stored_presence.activity.ruleset == "mania"
    assert (
        realtime.stored_presence.statistics.play_count,
        realtime.stored_presence.statistics.performance,
    ) == (12, 321)


@pytest.mark.asyncio
async def test_public_message_replay_block_filter_and_publish_failure_are_recipient_isolated() -> None:
    realtime = FakeRealtime(
        (snapshot(10, "sender"), snapshot(20, "full"), snapshot(30, "blocked"), snapshot(40, "delivered"))
    )
    realtime.channel_members.update((10, 20, 30, 40))
    realtime.failed_publish_accounts = frozenset({20})
    community = FakeCommunity()
    social = FakeSocial()
    social.blocked_recipients = frozenset({30})
    packet = message_packet(ClientPacket.SEND_PUBLIC_MESSAGE, Message("", "hello", "#osu", 0))
    current = context(realtime)
    stable_services = services(realtime, community=community, social=social)

    first = await dispatch_packets(packet, current, stable_services)
    replay = await dispatch_packets(packet, current, stable_services)

    assert first == replay == b""
    assert social.filtered == (20, 30, 40)
    assert [account_id for account_id, _ in realtime.published] == [40]
    assert len(community.public_client_message_ids) == 1


@pytest.mark.asyncio
async def test_public_message_without_active_membership_returns_actionable_notification() -> None:
    realtime = FakeRealtime((snapshot(10, "sender"),))
    packet = message_packet(ClientPacket.SEND_PUBLIC_MESSAGE, Message("", "hello", "#osu", 0))

    response = await dispatch_packets(
        packet,
        context(realtime),
        services(
            realtime,
            community=FakeCommunity(error=ChannelMembershipRequired("sender is not an active channel member")),
        ),
    )

    notification_packet = next(PacketReader(response, packet_enum=ServerPacket))
    assert notification_packet.packet_type is ServerPacket.NOTIFICATION
    assert notification_packet.payload.read_string() == "Join the channel before sending messages."


@pytest.mark.asyncio
async def test_direct_message_replay_does_not_publish_twice() -> None:
    realtime = FakeRealtime((snapshot(10, "sender"), snapshot(20, "target")))
    community = FakeCommunity()
    packet = message_packet(ClientPacket.SEND_PRIVATE_MESSAGE, Message("", "hello", "target", 0))
    current = context(realtime)
    stable_services = services(realtime, community=community)

    await dispatch_packets(packet, current, stable_services)
    replay = await dispatch_packets(packet, current, stable_services)

    assert replay == b""
    assert len(community.client_message_ids) == 1
    assert [account_id for account_id, _ in realtime.published] == [20]


@pytest.mark.asyncio
async def test_target_silence_and_other_dm_application_errors_map_to_stable_packets() -> None:
    realtime = FakeRealtime((snapshot(10, "sender"), snapshot(20, "target")))
    target_silenced = TargetAccountSilenced(
        "silenced",
        account_id=20,
        ends_at=None,
        remaining_seconds=None,
        channel_id=None,
    )
    packet = message_packet(ClientPacket.SEND_PRIVATE_MESSAGE, Message("", "hello", "target", 0))

    silenced = await dispatch_packets(
        packet,
        context(realtime),
        services(realtime, community=FakeCommunity(error=target_silenced)),
    )
    rejected = await dispatch_packets(
        packet,
        context(realtime),
        services(realtime, community=FakeCommunity(error=CommunityInputRejected("invalid"))),
    )

    assert next(PacketReader(silenced, packet_enum=ServerPacket)).packet_type is ServerPacket.TARGET_IS_SILENCED
    assert next(PacketReader(rejected, packet_enum=ServerPacket)).packet_type is ServerPacket.NOTIFICATION


@pytest.mark.asyncio
async def test_lobby_channel_join_requires_lobby_membership_and_confirms_client_sequence() -> None:
    realtime = FakeRealtime((snapshot(10, "sender"),))
    community = FakeCommunity()
    community.channel = ChannelView(7, "lobby", "Lobby", False, 2000, True, False)
    community.member_count = 1
    stable_services = services(realtime, community=community, multiplayer=FakeMultiplayer())

    rejected = await dispatch_packets(
        build_packet(ClientPacket.CHANNEL_JOIN, b"\x0b\x06#lobby"),
        context(realtime),
        stable_services,
    )
    joined = await dispatch_packets(
        build_packet(ClientPacket.JOIN_LOBBY) + build_packet(ClientPacket.CHANNEL_JOIN, b"\x0b\x06#lobby"),
        context(realtime),
        stable_services,
    )

    assert next(PacketReader(rejected, packet_enum=ServerPacket)).packet_type is ServerPacket.NOTIFICATION
    joined_packets = list(PacketReader(joined, packet_enum=ServerPacket))
    assert [packet.packet_type for packet in joined_packets] == [
        ServerPacket.CHANNEL_JOIN_SUCCESS,
        ServerPacket.CHANNEL_INFO,
    ]
    assert joined_packets[0].payload.read_string() == "#lobby"
    assert 10 in realtime.channel_members


@pytest.mark.asyncio
async def test_channel_join_broadcasts_authoritative_counts() -> None:
    realtime = FakeRealtime((snapshot(10, "sender"), snapshot(20, "target")))
    realtime.channel_members.add(20)
    community = FakeCommunity()
    community.member_count = 2
    stable_services = services(realtime, community=community)

    joined = await dispatch_packets(
        build_packet(ClientPacket.CHANNEL_JOIN, b"\x0b\x04#osu"),
        context(realtime),
        stable_services,
    )

    joined_packets = list(PacketReader(joined, packet_enum=ServerPacket))
    assert [packet.packet_type for packet in joined_packets] == [
        ServerPacket.CHANNEL_JOIN_SUCCESS,
        ServerPacket.CHANNEL_INFO,
    ]
    assert joined_packets[1].payload.read_channel().player_count == 2
    broadcast = next(PacketReader(StableBubbleRenderer().render(realtime.published[0][1]), packet_enum=ServerPacket))
    assert broadcast.payload.read_channel().player_count == 2


@pytest.mark.asyncio
async def test_invalid_or_blocked_friend_target_returns_current_list() -> None:
    realtime = FakeRealtime((snapshot(10, "sender"),))
    social = FakeSocial()
    social.follow_error = SocialInteractionBlocked("blocked")

    response = await dispatch_packets(
        build_packet(ClientPacket.FRIEND_ADD, (20).to_bytes(4, "little", signed=True)),
        context(realtime),
        services(realtime, social=social),
    )

    packet = next(PacketReader(response, packet_enum=ServerPacket))
    assert packet.packet_type is ServerPacket.FRIENDS_LIST
    assert packet.payload.read_i32_list_u16() == (1, 20)


@pytest.mark.asyncio
async def test_logout_during_first_second_is_ignored_without_cleanup_or_fencing() -> None:
    realtime = FakeRealtime((snapshot(10, "sender"), snapshot(20, "target")))
    identity = FakeIdentity()
    multiplayer = FakeMultiplayer()

    response = await dispatch_packets(
        build_packet(ClientPacket.LOGOUT, (0).to_bytes(4, "little", signed=True)),
        context(realtime, opened_at=NOW),
        services(realtime, identity=identity, multiplayer=multiplayer),
    )

    assert response == b""
    assert identity.closed is None
    assert not multiplayer.cleanup_commands
    assert realtime.fenced is None
    assert not realtime.published
