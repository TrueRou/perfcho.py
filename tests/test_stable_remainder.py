import uuid
from datetime import UTC, datetime, timedelta
from typing import cast

import pytest

from perfcho.composition import StableServices
from perfcho.infra.settings import Settings
from perfcho.modules.authorization import AuthorizationQueryService
from perfcho.modules.common import Clock, IdGenerator
from perfcho.modules.community import CommunityService, DirectMessageBlocked, MessageResult
from perfcho.modules.identity import IdentityService, ResolvedStableSession
from perfcho.modules.realtime import MailboxPacket, PresenceSnapshot, RealtimeRepository, RealtimeSession
from perfcho.modules.social import AccountIdentityView, SocialService
from perfcho.realtime.stable import (
    ClientPacket,
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
from perfcho.realtime.stable.countries import stable_country_id
from perfcho.realtime.stable.dispatcher import StableRuntimeContext, dispatch_packets

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


class FakeSocial:
    async def resolve_account_by_name(self, display_name: str) -> AccountIdentityView:
        assert display_name == "target"
        return AccountIdentityView(20, "target")

    async def list_follower_account_ids(self, account_id: int) -> frozenset[int]:
        assert account_id == 10
        return frozenset({20})


class FakeCommunity:
    def __init__(self, *, blocked: bool = False) -> None:
        self.blocked = blocked
        self.sent: tuple[int, int, str] | None = None

    async def send_direct_message(
        self,
        sender_account_id: int,
        recipient_account_id: int,
        client_message_id: uuid.UUID,
        content: str,
    ) -> MessageResult:
        del client_message_id
        if self.blocked:
            raise DirectMessageBlocked()
        self.sent = (sender_account_id, recipient_account_id, content)
        return MessageResult(1, 2, sender_account_id, uuid.uuid7(), content, False, None, NOW, recipient_account_id)


class FakeRealtime:
    def __init__(self, presences: tuple[PresenceSnapshot, ...]) -> None:
        self.presences = {snapshot.account_id: snapshot for snapshot in presences}
        self.enqueued: list[tuple[int, bytes]] = []
        self.filter_value: int | None = None
        self.fenced: tuple[uuid.UUID, int] | None = None
        self.away = "Away for lunch"

    async def get_presence(self, account_id: int, *, at: datetime) -> PresenceSnapshot | None:
        del at
        return self.presences.get(account_id)

    async def list_presences(self, *, at: datetime, limit: int) -> tuple[PresenceSnapshot, ...]:
        del at
        return tuple(self.presences.values())[:limit]

    async def enqueue_mailbox(self, account_id: int, payload: bytes, *, expires_at: datetime) -> MailboxPacket:
        assert expires_at == self.presences[account_id].expires_at
        self.enqueued.append((account_id, payload))
        return MailboxPacket(len(self.enqueued), payload)

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

    async def get_spectator_relation(self, account_id: int, *, at: datetime) -> None:
        del account_id, at
        return None

    async def fence_session(self, session_id: uuid.UUID, *, expected_revision: int) -> None:
        self.fenced = (session_id, expected_revision)


def snapshot(account_id: int, name: str, *, action: int = 0) -> PresenceSnapshot:
    payload = user_presence(UserPresence(account_id, name, 0, 0, 1, 0, 0.0, 0.0, 0))
    payload += user_stats(UserStats(account_id, action, "", "", 0, 0, 0, 0, 0.0, 0, 0, 0, 0))
    return PresenceSnapshot(account_id, 1, payload, EXPIRY)


def context() -> StableRuntimeContext:
    session_id = uuid.uuid7()
    return StableRuntimeContext(
        ResolvedStableSession(10, "sender", 1, session_id, None, "b20260711.1", None, EXPIRY),
        RealtimeSession(session_id, 10, 1, EXPIRY),
        UserPresence(10, "sender", 0, 0, 1, 0, 0.0, 0.0, 0),
        UserStats(10, 0, "", "", 0, 0, 0, 0, 0.0, 0, 0, 0, 0),
        raw_token="raw-token",
    )


def services(
    realtime: FakeRealtime,
    *,
    identity: FakeIdentity | None = None,
    community: FakeCommunity | None = None,
) -> StableServices:
    return StableServices(
        identity=cast(IdentityService, identity or FakeIdentity()),
        authorization=cast(AuthorizationQueryService, object()),
        realtime=cast(RealtimeRepository, realtime),
        clock=cast(Clock, FixedClock()),
        id_generator=cast(IdGenerator, FakeIds()),
        settings=Settings(),
        social=cast(SocialService, FakeSocial()),
        community=cast(CommunityService, community or FakeCommunity()),
    )


def message_packet(packet_type: ClientPacket, message: Message) -> bytes:
    writer = PacketWriter()
    with writer.packet(packet_type):
        writer.write_message(message)
    return writer.to_bytes()


@pytest.mark.asyncio
async def test_private_message_is_persisted_enqueued_and_returns_away_reply() -> None:
    realtime = FakeRealtime((snapshot(10, "sender"), snapshot(20, "target", action=1)))
    community = FakeCommunity()

    response = await dispatch_packets(
        message_packet(ClientPacket.SEND_PRIVATE_MESSAGE, Message("", "hello", "target", 0)),
        context(),
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
        context(),
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

    response = await dispatch_packets(request_all + filter_packet, context(), services(realtime))

    packets = list(PacketReader(response, packet_enum=ServerPacket))
    assert [packet.packet_type for packet in packets] == [ServerPacket.USER_PRESENCE, ServerPacket.USER_PRESENCE]
    assert realtime.filter_value == 2


@pytest.mark.asyncio
async def test_logout_closes_identity_fences_session_and_broadcasts() -> None:
    realtime = FakeRealtime((snapshot(10, "sender"), snapshot(20, "target")))
    identity = FakeIdentity()
    current = context()

    response = await dispatch_packets(
        build_packet(ClientPacket.LOGOUT),
        current,
        services(realtime, identity=identity),
    )

    assert response == b""
    assert identity.closed == ("raw-token", "client_logout")
    assert realtime.fenced == (current.identity.session_id, 1)
    packet = next(PacketReader(realtime.enqueued[0][1], packet_enum=ServerPacket))
    assert packet.packet_type is ServerPacket.USER_LOGOUT
