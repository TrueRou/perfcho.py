import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import cast

import pytest

from perfcho.api.stable.dispatcher import StableRuntimeContext, dispatch_packets
from perfcho.infra.composition import StableServices
from perfcho.infra.settings import Settings
from perfcho.modules.authorization import AuthorizationQueryService
from perfcho.modules.common import Clock, IdGenerator
from perfcho.modules.community import (
    CommunityInputRejected,
    CommunityService,
    DirectMessageBlocked,
    MessageResult,
    StableChannel,
    TargetAccountSilenced,
)
from perfcho.modules.identity import IdentityService, ResolvedStableSession
from perfcho.modules.multiplayer import CleanupPresence
from perfcho.modules.realtime import MailboxOverflow, MailboxPacket, PresenceSnapshot, RealtimeSession, SessionFence
from perfcho.modules.realtime.stable import (
    ClientPacket,
    ClientStatus,
    Message,
    PacketReader,
    PacketWriter,
    ServerPacket,
    UserPresence,
    UserStats,
    build_packet,
    user_presence,
    user_stats,
)
from perfcho.modules.realtime.stable.countries import stable_country_id
from perfcho.modules.scoring.mods import LEGACY_MOD_BITS
from perfcho.modules.social import AccountIdentityView, SocialInteractionBlocked, SocialService

NOW = datetime(2026, 7, 29, 12, tzinfo=UTC)
EXPIRY = NOW + timedelta(minutes=5)


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

    async def close_stable_session(self, raw_token: str, *, reason: str) -> None:
        self.closed = (raw_token, reason)


class FakeMultiplayer:
    def __init__(self) -> None:
        self.cleanup_commands: list[object] = []

    async def find_room_for_account(self, account_id: int) -> None:
        assert account_id == 10
        return None

    async def cleanup_presence(self, command: object) -> None:
        self.cleanup_commands.append(command)
        return None


class FakeSocial:
    def __init__(self) -> None:
        self.blocked_recipients: frozenset[int] = frozenset()
        self.follow_error: Exception | None = None
        self.filtered: tuple[int, ...] | None = None

    async def resolve_account_by_name(self, display_name: str) -> AccountIdentityView:
        assert display_name == "target"
        return AccountIdentityView(20, "target")

    async def list_follower_account_ids(self, account_id: int) -> frozenset[int]:
        assert account_id == 10
        return frozenset({20})

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
        self.channel = StableChannel(7, "#osu", "general", False, 256, True, False)
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

    async def get_public_channel_by_stable_name(self, account_id: int, name: str) -> StableChannel:
        assert account_id == 10
        assert name.casefold() in {"#osu", "osu"}
        return self.channel

    async def get_channel_member_count(self, account_id: int, channel_id: int) -> int:
        assert account_id == 10 and channel_id == self.channel.channel_id
        return self.member_count

    async def send_public_message(
        self,
        sender_account_id: int,
        channel_name: str,
        client_message_id: uuid.UUID,
        content: str,
    ) -> MessageResult:
        assert sender_account_id == 10 and channel_name == "#osu"
        if self.error is not None:
            raise self.error
        created = client_message_id not in self.public_client_message_ids
        if created:
            self.public_client_message_ids.append(client_message_id)
        return MessageResult(
            2, self.channel.channel_id, 10, client_message_id, content, False, None, NOW, created=created
        )


class FakeRealtime:
    def __init__(self, presences: tuple[PresenceSnapshot, ...]) -> None:
        self.presences = {snapshot.account_id: snapshot for snapshot in presences}
        self.enqueued: list[tuple[int, bytes]] = []
        self.filter_value: int | None = None
        self.fenced: tuple[uuid.UUID, int] | None = None
        self.away = "Away for lunch"
        self.get_presence_calls: list[int] = []
        self.overflow_accounts: frozenset[int] = frozenset()
        self.channel_members: set[int] = set()
        self.stored_presence: PresenceSnapshot | None = None

    async def get_presence(self, account_id: int, *, at: datetime) -> PresenceSnapshot | None:
        del at
        self.get_presence_calls.append(account_id)
        return self.presences.get(account_id)

    async def list_presences(self, *, at: datetime, limit: int) -> tuple[PresenceSnapshot, ...]:
        del at
        return tuple(self.presences.values())[:limit]

    async def enqueue_mailbox(
        self,
        account_id: int,
        payload: bytes,
        *,
        recipient_fence: SessionFence,
        expires_at: datetime,
    ) -> MailboxPacket:
        assert expires_at == self.presences[account_id].expires_at
        assert recipient_fence == self.presences[account_id].fence
        if account_id in self.overflow_accounts:
            raise MailboxOverflow()
        self.enqueued.append((account_id, payload))
        return MailboxPacket(len(self.enqueued), payload)

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

    async def set_presence_filter(
        self,
        account_id: int,
        *,
        session_id: uuid.UUID,
        expected_revision: int,
        value: int,
    ) -> None:
        del account_id, session_id, expected_revision
        self.filter_value = value

    async def get_presence_filter(self, account_id: int) -> int:
        return 2 if account_id == 20 else 0

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


def snapshot(account_id: int, name: str, *, action: int = 0) -> PresenceSnapshot:
    payload = user_presence(UserPresence(account_id, name, 0, 0, 1, 0, 0.0, 0.0, 0))
    payload += user_stats(UserStats(account_id, action, "", "", 0, 0, 0, 0, 0.0, 0, 0, 0, 0))
    return PresenceSnapshot(account_id, 1, payload, EXPIRY, uuid.uuid7())


def context(realtime: FakeRealtime, *, opened_at: datetime | None = None) -> StableRuntimeContext:
    fence = realtime.presences[10].fence
    return StableRuntimeContext(
        ResolvedStableSession(
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
) -> StableServices:
    return StableServices(
        identity=cast(IdentityService, identity or FakeIdentity()),
        authorization=cast(AuthorizationQueryService, object()),
        realtime=realtime,
        clock=cast(Clock, FixedClock()),
        id_generator=cast(IdGenerator, FakeIds()),
        settings=settings or Settings(),
        social=cast(SocialService, social or FakeSocial()),
        community=cast(CommunityService, community or FakeCommunity()),
        multiplayer=multiplayer,
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
async def test_private_message_is_persisted_enqueued_and_returns_away_reply() -> None:
    realtime = FakeRealtime((snapshot(10, "sender"), snapshot(20, "target", action=1)))
    community = FakeCommunity()

    response = await dispatch_packets(
        message_packet(ClientPacket.SEND_PRIVATE_MESSAGE, Message("", "hello", "target", 0)),
        context(realtime),
        services(realtime, community=community),
    )

    assert community.sent == (10, 20, "hello")
    assert realtime.enqueued[0][0] == 20
    delivered = next(PacketReader(realtime.enqueued[0][1], packet_enum=ServerPacket))
    assert delivered.payload.read_message() == Message("sender", "hello", "target", 10)
    away = next(PacketReader(response, packet_enum=ServerPacket))
    assert away.payload.read_message().text == "Away for lunch"


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
    assert not realtime.enqueued


@pytest.mark.asyncio
async def test_presence_all_and_filter_use_bounded_realtime_state() -> None:
    realtime = FakeRealtime((snapshot(10, "sender"), snapshot(20, "target")))
    request_all = build_packet(ClientPacket.USER_PRESENCE_REQUEST_ALL, (0).to_bytes(4, "little", signed=True))
    filter_packet = build_packet(ClientPacket.RECEIVE_UPDATES, (2).to_bytes(4, "little", signed=True))

    response = await dispatch_packets(request_all + filter_packet, context(realtime), services(realtime))

    packets = list(PacketReader(response, packet_enum=ServerPacket))
    assert [packet.packet_type for packet in packets] == [ServerPacket.USER_PRESENCE, ServerPacket.USER_PRESENCE]
    assert realtime.filter_value == 2


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
    packet = next(PacketReader(realtime.enqueued[0][1], packet_enum=ServerPacket))
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
async def test_dispatcher_cumulative_response_budget_keeps_complete_packets() -> None:
    realtime = FakeRealtime((snapshot(10, "sender"),))

    response = await dispatch_packets(
        build_packet(ClientPacket.PING) * 2,
        context(realtime),
        services(realtime, settings=Settings(stable_max_response_bytes=7)),
    )

    assert [packet.packet_type for packet in PacketReader(response, packet_enum=ServerPacket)] == [ServerPacket.PONG]
    assert len(response) == 7


@pytest.mark.asyncio
async def test_change_action_normalizes_assistance_and_stores_consistent_presence() -> None:
    realtime = FakeRealtime((snapshot(10, "sender"), snapshot(20, "target")))
    status = ClientStatus(
        13,
        "playing",
        "a" * 32,
        LEGACY_MOD_BITS["AP"] | LEGACY_MOD_BITS["RX"],
        3,
        55,
    )

    response = await dispatch_packets(status_packet(status), context(realtime), services(realtime))

    response_stats = next(PacketReader(response, packet_enum=ServerPacket)).payload.read_user_stats()
    assert response_stats.mode == 3
    assert response_stats.mods & (LEGACY_MOD_BITS["AP"] | LEGACY_MOD_BITS["RX"]) == 0
    assert realtime.stored_presence is not None
    assert realtime.stored_presence.session_id == realtime.presences[10].session_id
    stored_packets = list(PacketReader(realtime.stored_presence.payload, packet_enum=ServerPacket))
    stored_presence = stored_packets[0].payload.read_user_presence()
    stored_stats = stored_packets[1].payload.read_user_stats()
    assert (stored_presence.mode, stored_presence.global_rank) == (stored_stats.mode, stored_stats.global_rank)


@pytest.mark.asyncio
async def test_status_request_refreshes_fenced_authoritative_snapshot() -> None:
    realtime = FakeRealtime((snapshot(10, "sender"),))

    await dispatch_packets(build_packet(ClientPacket.REQUEST_STATUS_UPDATE), context(realtime), services(realtime))

    assert realtime.stored_presence is not None
    assert realtime.stored_presence.fence == realtime.presences[10].fence


@pytest.mark.asyncio
async def test_public_message_replay_block_filter_and_overflow_are_recipient_isolated() -> None:
    realtime = FakeRealtime(
        (snapshot(10, "sender"), snapshot(20, "full"), snapshot(30, "blocked"), snapshot(40, "delivered"))
    )
    realtime.channel_members.update((10, 20, 30, 40))
    realtime.overflow_accounts = frozenset({20})
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
    assert [account_id for account_id, _ in realtime.enqueued] == [40]
    assert len(community.public_client_message_ids) == 1


@pytest.mark.asyncio
async def test_direct_message_replay_does_not_enqueue_twice() -> None:
    realtime = FakeRealtime((snapshot(10, "sender"), snapshot(20, "target")))
    community = FakeCommunity()
    packet = message_packet(ClientPacket.SEND_PRIVATE_MESSAGE, Message("", "hello", "target", 0))
    current = context(realtime)
    stable_services = services(realtime, community=community)

    await dispatch_packets(packet, current, stable_services)
    replay = await dispatch_packets(packet, current, stable_services)

    assert replay == b""
    assert len(community.client_message_ids) == 1
    assert [account_id for account_id, _ in realtime.enqueued] == [20]


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
async def test_channel_join_rejects_lobby_and_broadcasts_authoritative_counts() -> None:
    realtime = FakeRealtime((snapshot(10, "sender"), snapshot(20, "target")))
    realtime.channel_members.add(20)
    community = FakeCommunity()
    community.member_count = 2
    stable_services = services(realtime, community=community)

    lobby = await dispatch_packets(
        build_packet(ClientPacket.CHANNEL_JOIN, b"\x0b\x06#lobby"),
        context(realtime),
        stable_services,
    )
    joined = await dispatch_packets(
        build_packet(ClientPacket.CHANNEL_JOIN, b"\x0b\x04#osu"),
        context(realtime),
        stable_services,
    )

    assert next(PacketReader(lobby, packet_enum=ServerPacket)).packet_type is ServerPacket.NOTIFICATION
    joined_packets = list(PacketReader(joined, packet_enum=ServerPacket))
    assert [packet.packet_type for packet in joined_packets] == [
        ServerPacket.CHANNEL_JOIN_SUCCESS,
        ServerPacket.CHANNEL_INFO,
    ]
    assert joined_packets[1].payload.read_channel().player_count == 2
    broadcast = next(PacketReader(realtime.enqueued[0][1], packet_enum=ServerPacket))
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
    assert not realtime.enqueued
