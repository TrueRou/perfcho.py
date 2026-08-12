import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import cast

import httpx
import pytest
from fastapi import FastAPI
from pydantic import ValidationError

from perfcho.api.stable import router
from perfcho.api.stable.authorization import StablePrivilege
from perfcho.api.stable.bubbles import StableBubbleRenderer, canonicalize_presence
from perfcho.api.stable.canonize.login import StableLoginParseError, parse_stable_login
from perfcho.api.stable.dependencies import get_stable_services
from perfcho.api.stable.dispatcher import StableRuntimeContext
from perfcho.api.stable.realtime import (
    ClientPacket,
    LoginFailureReason,
    PacketReader,
    ServerPacket,
    UserPresence,
    UserStats,
    build_packet,
)
from perfcho.infra.compose import StableServices
from perfcho.infra.settings import Settings
from perfcho.modules.authorization import AuthorizationQueryService, EffectiveAuthorization
from perfcho.modules.common import Clock, IdGenerator
from perfcho.modules.community import ChannelView, CommunityService, OfflineDirectMessage
from perfcho.modules.identity import (
    AuthenticateClientSession,
    ClientSessionResult,
    IdentityService,
    ResolvedClientSession,
)
from perfcho.modules.realtime import (
    NotificationBubble,
    PresenceCapacityReached,
    PresenceSnapshot,
    RealtimeBubble,
    RealtimeBubbleBus,
    RealtimeBubbleSubscription,
    RealtimePollGate,
    RealtimeSession,
    RealtimeStateRepository,
    SessionFence,
    presence_updated_bubble,
)
from perfcho.modules.social import SocialService

NOW = datetime(2026, 7, 29, tzinfo=UTC)


def presence_snapshot(
    presence: UserPresence,
    stats: UserStats,
    revision: int,
    expires_at: datetime,
    session_id: uuid.UUID,
    *,
    country_code: str | None = "OC",
) -> PresenceSnapshot:
    identity, activity, statistics = canonicalize_presence(presence, stats, country_code=country_code)
    return PresenceSnapshot(
        presence.user_id,
        revision,
        identity,
        activity,
        statistics,
        expires_at,
        session_id,
    )


class FakeClock:
    def now(self) -> datetime:
        return NOW


class FakeIds:
    def new(self) -> uuid.UUID:
        return uuid.uuid7()


class FakeBubbleSubscription:
    def __init__(self, bus: FakeBubbleBus) -> None:
        self.bus = bus

    async def receive(self, *, timeout: float) -> RealtimeBubble | None:
        del timeout
        self.bus.receive_calls += 1
        if self.bus.pending:
            return self.bus.pending.pop(0)
        bubble = self.bus.wait_bubble
        self.bus.wait_bubble = None
        return bubble

    async def drain(self, *, limit: int) -> tuple[RealtimeBubble, ...]:
        self.bus.drain_calls += 1
        if self.bus.drain_error is not None:
            raise self.bus.drain_error
        drained = tuple(self.bus.pending[:limit])
        del self.bus.pending[:limit]
        return drained

    async def acknowledge(self) -> None:
        self.bus.acknowledge_calls += 1
        if self.bus.acknowledge_error is not None:
            raise self.bus.acknowledge_error

    async def aclose(self) -> None:
        self.bus.subscribed = False


class FakeBubbleBus:
    def __init__(self) -> None:
        self.pending: list[RealtimeBubble] = []
        self.wait_bubble: RealtimeBubble | None = None
        self.published: list[tuple[SessionFence, RealtimeBubble]] = []
        self.subscribed = False
        self.receive_calls = 0
        self.drain_calls = 0
        self.drain_error: Exception | None = None
        self.acknowledge_calls = 0
        self.acknowledge_error: Exception | None = None
        self.subscribe_calls: list[SessionFence] = []

    async def publish(self, recipient_fence: SessionFence, bubble: RealtimeBubble) -> int:
        self.published.append((recipient_fence, bubble))
        return int(self.subscribed and self.subscribe_calls[-1] == recipient_fence)

    @asynccontextmanager
    async def subscribe(
        self,
        recipient_fence: SessionFence,
    ) -> AsyncIterator[RealtimeBubbleSubscription]:
        self.subscribe_calls.append(recipient_fence)
        self.subscribed = True
        subscription = FakeBubbleSubscription(self)
        try:
            yield subscription
        finally:
            await subscription.aclose()


class FakePollGate:
    def __init__(self) -> None:
        self.active = False
        self.conflict = False
        self.acquired: list[tuple[int, SessionFence, uuid.UUID]] = []
        self.released: list[tuple[int, SessionFence, uuid.UUID]] = []

    async def acquire(
        self,
        account_id: int,
        recipient_fence: SessionFence,
        gate_id: uuid.UUID,
        *,
        expires_at: datetime,
    ) -> bool:
        del expires_at
        if self.conflict or self.active:
            return False
        self.active = True
        self.acquired.append((account_id, recipient_fence, gate_id))
        return True

    async def release(self, account_id: int, recipient_fence: SessionFence, gate_id: uuid.UUID) -> None:
        self.released.append((account_id, recipient_fence, gate_id))
        self.active = False


class FakeIdentity:
    def __init__(self) -> None:
        self.login_command: AuthenticateClientSession | None = None
        self.session_id = uuid.uuid7()
        self.device_id = uuid.uuid7()
        self.touch_calls = 0
        self.close_calls: list[tuple[str, str]] = []
        self.close_error: Exception | None = None

    async def authenticate_client_session(self, command: AuthenticateClientSession) -> ClientSessionResult:
        self.login_command = command
        return ClientSessionResult(
            account_id=3,
            current_name="player",
            session_id=self.session_id,
            device_id=self.device_id,
            raw_token="stable-token-value",
            expires_at=NOW + timedelta(hours=1),
        )

    async def resolve_client_session(self, raw_token: str) -> ResolvedClientSession:
        if raw_token != "stable-token-value":
            from perfcho.modules.identity import InvalidSession

            raise InvalidSession()
        return ResolvedClientSession(
            account_id=3,
            current_name="player",
            auth_version=1,
            session_id=self.session_id,
            device_id=self.device_id,
            client_version="b20260711.1",
            client_variant=None,
            expires_at=NOW + timedelta(hours=1),
        )

    async def touch_client_session(self, raw_token: str) -> ResolvedClientSession:
        self.touch_calls += 1
        return await self.resolve_client_session(raw_token)

    async def close_client_session(self, raw_token: str, *, reason: str = "client_closed") -> None:
        self.close_calls.append((raw_token, reason))
        if self.close_error is not None:
            raise self.close_error


class FakeAuthorization:
    async def get_effective(self, account_id: int) -> EffectiveAuthorization:
        assert account_id == 3
        return EffectiveAuthorization(
            account_id=3,
            evaluated_at=NOW,
            permission_codes=frozenset({"account.login"}),
            role_codes=frozenset(),
            entitlement_codes=frozenset(),
        )


class FakeRealtime:
    def __init__(self) -> None:
        self.session: RealtimeSession | None = None
        self.presence: PresenceSnapshot | None = None
        self.online_presences: dict[int, PresenceSnapshot] = {}
        self.channel_members: dict[int, set[int]] = {}
        self.fenced: list[SessionFence] = []
        self.fail_set_presence = False
        self.fail_presence_capacity = False
        self.open_calls = 0
        self.durable_expires_at: datetime | None = None

    async def open_session(
        self,
        *,
        session_id: uuid.UUID,
        account_id: int,
        expires_at: datetime,
        durable_expires_at: datetime,
    ) -> RealtimeSession:
        self.open_calls += 1
        self.durable_expires_at = durable_expires_at
        self.session = RealtimeSession(session_id, account_id, 1, expires_at)
        return self.session

    async def resolve_session(self, session_id: uuid.UUID, *, at: datetime) -> RealtimeSession:
        del at
        if self.session is None or self.session.session_id != session_id:
            from perfcho.modules.realtime import RealtimeSessionNotFound

            raise RealtimeSessionNotFound()
        return self.session

    async def heartbeat_session(
        self,
        session_id: uuid.UUID,
        *,
        expected_revision: int,
        expires_at: datetime,
    ) -> RealtimeSession:
        assert self.session is not None
        assert session_id == self.session.session_id and expected_revision == self.session.revision
        self.session = RealtimeSession(session_id, self.session.account_id, expected_revision, expires_at)
        return self.session

    async def set_presence(
        self,
        snapshot: PresenceSnapshot,
        *,
        session_id: uuid.UUID,
        capacity: int | None = None,
    ) -> None:
        assert self.session is not None and session_id == self.session.session_id
        if self.fail_set_presence:
            raise RuntimeError("presence bootstrap failed")
        if self.fail_presence_capacity and capacity is not None:
            raise PresenceCapacityReached
        self.presence = snapshot
        self.online_presences[snapshot.account_id] = snapshot

    async def get_presence(self, account_id: int, *, at: datetime) -> PresenceSnapshot | None:
        del at
        return self.online_presences.get(account_id)

    async def list_presences(self, *, at: datetime, limit: int) -> tuple[PresenceSnapshot, ...]:
        del at
        return tuple(self.online_presences[key] for key in sorted(self.online_presences))[:limit]

    async def join_channel(
        self,
        channel_id: int,
        *,
        session_id: uuid.UUID,
        expected_revision: int,
    ) -> None:
        assert self.session is not None
        assert (session_id, expected_revision) == (self.session.session_id, self.session.revision)
        self.channel_members.setdefault(channel_id, set()).add(self.session.account_id)

    async def list_channel_members(self, channel_id: int) -> frozenset[int]:
        return frozenset(self.channel_members.get(channel_id, set()))

    async def get_spectator_relation(
        self,
        account_id: int,
        *,
        spectator_fence: SessionFence,
        at: datetime,
    ) -> None:
        del account_id, spectator_fence, at
        return None

    async def fence_session(self, session_id: uuid.UUID, *, expected_revision: int) -> None:
        assert self.session is not None
        assert (session_id, expected_revision) == (self.session.session_id, self.session.revision)
        self.fenced.append(self.session.fence)
        self.online_presences.pop(self.session.account_id, None)
        self.session = None


class FakeSocial:
    async def list_friends(self, account_id: int) -> tuple[object, ...]:
        assert account_id == 3
        return ()


class FakeCommunity:
    def __init__(self) -> None:
        self.policy: str | None = None
        self.silence_seconds = 91
        self.channels = (
            ChannelView(7, "general", "General", True, 2000, True, False),
            ChannelView(8, "announcements", "News", False, 2000, False, False),
            ChannelView(9, "lobby", "Lobby", True, 2000, True, False),
        )
        self.offline_messages = (OfflineDirectMessage(10, 20, 8, "online", uuid.uuid7(), "older message", False, NOW),)
        self.realtime: FakeRealtime | None = None

    async def set_private_message_policy(self, account_id: int, policy: str) -> str:
        assert account_id == 3
        self.policy = policy
        return policy

    async def list_public_channels(self, account_id: int) -> tuple[ChannelView, ...]:
        assert account_id == 3
        return self.channels

    async def list_unread_offline_direct_messages(self, account_id: int) -> tuple[OfflineDirectMessage, ...]:
        assert account_id == 3
        return self.offline_messages

    async def get_global_silence_remaining_seconds(self, account_id: int) -> int:
        assert account_id == 3
        return self.silence_seconds

    async def get_channel_member_count(
        self,
        account_id: int,
        channel_id: int,
        *,
        already_authorized: bool = False,
    ) -> int:
        del already_authorized
        assert account_id == 3 and self.realtime is not None
        return len(await self.realtime.list_channel_members(channel_id)) + 3


def stable_services() -> tuple[StableServices, FakeIdentity, FakeRealtime]:
    identity = FakeIdentity()
    realtime = FakeRealtime()
    config = Settings()
    services = StableServices(
        identity=cast(IdentityService, identity),
        authorization=cast(AuthorizationQueryService, FakeAuthorization()),
        realtime=cast(RealtimeStateRepository, realtime),
        clock=cast(Clock, FakeClock()),
        id_generator=cast(IdGenerator, FakeIds()),
        settings=config,
        bubbles=cast(RealtimeBubbleBus, FakeBubbleBus()),
        poll_gate=cast(RealtimePollGate, FakePollGate()),
    )
    return services, identity, realtime


def fake_bubbles(services: StableServices) -> FakeBubbleBus:
    return cast(FakeBubbleBus, services.bubbles)


def fake_poll_gate(services: StableServices) -> FakePollGate:
    return cast(FakePollGate, services.poll_gate)


def configure_community(services: StableServices, realtime: FakeRealtime) -> FakeCommunity:
    community = FakeCommunity()
    community.realtime = realtime
    object.__setattr__(services, "community", cast(CommunityService, community))
    object.__setattr__(services, "social", cast(SocialService, FakeSocial()))
    return community


def login_body(*, build: str = "b20260711.1") -> bytes:
    return f"player\n{'a' * 32}\n{build}|0|1|path:adapters:adapterhash:uninstall:disk:|1\n".encode()


def test_login_parser_is_strict_and_preserves_device_components() -> None:
    parsed = parse_stable_login(login_body(), expected_build="b20260711.1")
    assert parsed.identifier == "player"
    assert parsed.utc_offset == 0
    assert parsed.private_messages_from_friends_only
    assert dict(parsed.device_components)["disk"] == "disk"
    with pytest.raises(StableLoginParseError, match="unsupported"):
        parse_stable_login(login_body(build="b20200101"), expected_build="b20260711.1")


@pytest.mark.asyncio
async def test_successful_login_has_ordered_binary_bootstrap() -> None:
    services, identity, realtime = stable_services()
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_stable_services] = lambda: services

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://c.test") as client:
        response = await client.post("/", content=login_body(), headers={"User-Agent": "osu!"})

    assert response.status_code == 200
    assert response.headers["cho-token"] == "stable-token-value"
    assert response.headers["content-type"].startswith("application/octet-stream")
    packets = list(PacketReader(response.content, packet_enum=ServerPacket))
    assert [packet.packet_type for packet in packets] == [
        ServerPacket.PROTOCOL_VERSION,
        ServerPacket.USER_ID,
        ServerPacket.PRIVILEGES,
        ServerPacket.NOTIFICATION,
        ServerPacket.CHANNEL_INFO_END,
        ServerPacket.FRIENDS_LIST,
        ServerPacket.SILENCE_END,
        ServerPacket.USER_PRESENCE,
        ServerPacket.USER_STATS,
    ]
    assert identity.login_command is not None
    assert realtime.presence is not None
    assert realtime.presence.session_id == identity.session_id
    assert realtime.durable_expires_at == NOW + timedelta(hours=1)
    privilege_packet = next(packet for packet in packets if packet.packet_type is ServerPacket.PRIVILEGES)
    own_presence = next(packet for packet in packets if packet.packet_type is ServerPacket.USER_PRESENCE)
    assert privilege_packet.payload.read_i32() == StablePrivilege.PLAYER
    assert own_presence.payload.read_user_presence().privileges == StablePrivilege.PLAYER


@pytest.mark.asyncio
async def test_old_build_and_non_osu_user_agent_fail_in_protocol() -> None:
    services, _, _ = stable_services()
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_stable_services] = lambda: services

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://c.test") as client:
        old = await client.post("/", content=login_body(build="b20200101"), headers={"User-Agent": "osu!"})
        wrong_agent = await client.post("/", content=login_body(), headers={"User-Agent": "browser"})

    old_packets = list(PacketReader(old.content, packet_enum=ServerPacket))
    assert old_packets[-1].payload.read_i32() == LoginFailureReason.OLD_CLIENT
    wrong_packets = list(PacketReader(wrong_agent.content, packet_enum=ServerPacket))
    assert wrong_packets[0].payload.read_i32() == LoginFailureReason.ERROR


@pytest.mark.asyncio
async def test_authenticated_client_keepalive_drains_buffered_bubble() -> None:
    services, identity, realtime = stable_services()
    await realtime.open_session(
        session_id=identity.session_id,
        account_id=3,
        expires_at=NOW + timedelta(minutes=5),
        durable_expires_at=NOW + timedelta(hours=1),
    )
    fake_bubbles(services).pending.append(NotificationBubble("ready"))
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_stable_services] = lambda: services

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://c.test") as client:
        response = await client.post(
            "/",
            content=build_packet(ClientPacket.PING),
            headers={"User-Agent": "osu!", "osu-token": "stable-token-value"},
        )

    assert [packet.packet_type for packet in PacketReader(response.content, packet_enum=ServerPacket)] == [
        ServerPacket.NOTIFICATION,
    ]
    assert identity.touch_calls == 1
    assert fake_bubbles(services).drain_calls == 1


@pytest.mark.asyncio
async def test_idle_ping_waits_for_bubble_and_returns_it_immediately() -> None:
    services, identity, realtime = stable_services()
    await realtime.open_session(
        session_id=identity.session_id,
        account_id=3,
        expires_at=NOW + timedelta(minutes=5),
        durable_expires_at=NOW + timedelta(hours=1),
    )
    fake_bubbles(services).wait_bubble = NotificationBubble("ready")
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_stable_services] = lambda: services

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://c.test") as client:
        response = await client.post(
            "/",
            content=build_packet(ClientPacket.PING),
            headers={"User-Agent": "osu!", "osu-token": "stable-token-value"},
        )

    assert [packet.packet_type for packet in PacketReader(response.content, packet_enum=ServerPacket)] == [
        ServerPacket.NOTIFICATION,
    ]
    assert fake_bubbles(services).receive_calls == 1


@pytest.mark.asyncio
async def test_authenticated_client_keepalive_returns_empty_success() -> None:
    services, identity, realtime = stable_services()
    await realtime.open_session(
        session_id=identity.session_id,
        account_id=3,
        expires_at=NOW + timedelta(minutes=5),
        durable_expires_at=NOW + timedelta(hours=1),
    )
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_stable_services] = lambda: services

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://c.test") as client:
        response = await client.post(
            "/",
            content=build_packet(ClientPacket.PING),
            headers={"User-Agent": "osu!", "osu-token": "stable-token-value"},
        )

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/octet-stream")
    assert response.content == b""
    assert identity.touch_calls == 1
    assert fake_bubbles(services).receive_calls == 1


@pytest.mark.asyncio
async def test_poll_renders_local_and_remote_bubbles_without_publishing_local(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import importlib

    cho_module = importlib.import_module("perfcho.api.stable.router.cho")
    services, identity, realtime = stable_services()
    await realtime.open_session(
        session_id=identity.session_id,
        account_id=3,
        expires_at=NOW + timedelta(minutes=5),
        durable_expires_at=NOW + timedelta(hours=1),
    )
    bus = fake_bubbles(services)
    bus.pending.append(NotificationBubble("remote"))

    async def local_dispatch(
        body: bytes,
        context: StableRuntimeContext,
        dispatched_services: StableServices,
    ) -> tuple[RealtimeBubble, ...]:
        del body, context
        assert dispatched_services is services
        assert bus.subscribed
        return (NotificationBubble("local"),)

    monkeypatch.setattr(cho_module, "dispatch_packets", local_dispatch)
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_stable_services] = lambda: services

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://c.test") as client:
        response = await client.post(
            "/",
            content=build_packet(ClientPacket.PING),
            headers={"User-Agent": "osu!", "osu-token": "stable-token-value"},
        )

    messages = [packet.payload.read_string() for packet in PacketReader(response.content, packet_enum=ServerPacket)]
    assert messages == ["local", "remote"]
    assert bus.published == []


@pytest.mark.asyncio
async def test_poll_drops_over_budget_bubble_without_caching_it() -> None:
    services, identity, realtime = stable_services()
    maximum = len(StableBubbleRenderer().render(NotificationBubble("fits")))
    object.__setattr__(services, "settings", Settings(stable_max_response_bytes=maximum))
    await realtime.open_session(
        session_id=identity.session_id,
        account_id=3,
        expires_at=NOW + timedelta(minutes=5),
        durable_expires_at=NOW + timedelta(hours=1),
    )
    fake_bubbles(services).pending.append(NotificationBubble("x" * 200))
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_stable_services] = lambda: services

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://c.test") as client:
        first = await client.post(
            "/",
            content=build_packet(ClientPacket.PING),
            headers={"User-Agent": "osu!", "osu-token": "stable-token-value"},
        )
        second = await client.post(
            "/",
            content=b"",
            headers={"User-Agent": "osu!", "osu-token": "stable-token-value"},
        )

    assert first.content == second.content == b""
    assert fake_bubbles(services).pending == []


@pytest.mark.asyncio
async def test_poll_subscription_failure_returns_existing_local_bubbles(monkeypatch: pytest.MonkeyPatch) -> None:
    import importlib

    cho_module = importlib.import_module("perfcho.api.stable.router.cho")
    services, identity, realtime = stable_services()
    await realtime.open_session(
        session_id=identity.session_id,
        account_id=3,
        expires_at=NOW + timedelta(minutes=5),
        durable_expires_at=NOW + timedelta(hours=1),
    )
    fake_bubbles(services).drain_error = RuntimeError("subscription failed")

    async def local_dispatch(
        body: bytes,
        context: StableRuntimeContext,
        dispatched_services: StableServices,
    ) -> tuple[RealtimeBubble, ...]:
        del body, context, dispatched_services
        return (NotificationBubble("local"),)

    monkeypatch.setattr(cho_module, "dispatch_packets", local_dispatch)
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_stable_services] = lambda: services

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://c.test") as client:
        response = await client.post(
            "/",
            content=build_packet(ClientPacket.PING),
            headers={"User-Agent": "osu!", "osu-token": "stable-token-value"},
        )

    packet = next(PacketReader(response.content, packet_enum=ServerPacket))
    assert packet.payload.read_string() == "local"


@pytest.mark.asyncio
async def test_multiple_keepalives_do_not_enter_the_short_wait_window() -> None:
    services, identity, realtime = stable_services()
    await realtime.open_session(
        session_id=identity.session_id,
        account_id=3,
        expires_at=NOW + timedelta(minutes=5),
        durable_expires_at=NOW + timedelta(hours=1),
    )
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_stable_services] = lambda: services

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://c.test") as client:
        response = await client.post(
            "/",
            content=build_packet(ClientPacket.PING) * 2,
            headers={"User-Agent": "osu!", "osu-token": "stable-token-value"},
        )

    assert response.content == b""
    assert fake_bubbles(services).receive_calls == 0


@pytest.mark.asyncio
async def test_keepalive_with_payload_is_rejected_without_waiting() -> None:
    services, identity, realtime = stable_services()
    await realtime.open_session(
        session_id=identity.session_id,
        account_id=3,
        expires_at=NOW + timedelta(minutes=5),
        durable_expires_at=NOW + timedelta(hours=1),
    )
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_stable_services] = lambda: services

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://c.test") as client:
        response = await client.post(
            "/",
            content=build_packet(ClientPacket.PING, b"invalid"),
            headers={"User-Agent": "osu!", "osu-token": "stable-token-value"},
        )

    assert list(PacketReader(response.content, packet_enum=ServerPacket))[-1].packet_type is ServerPacket.RESTART
    assert fake_bubbles(services).receive_calls == 0


@pytest.mark.asyncio
async def test_logout_poll_fences_realtime_session_once() -> None:
    services, identity, realtime = stable_services()
    await realtime.open_session(
        session_id=identity.session_id,
        account_id=3,
        expires_at=NOW + timedelta(minutes=5),
        durable_expires_at=NOW + timedelta(hours=1),
    )
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_stable_services] = lambda: services

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://c.test") as client:
        response = await client.post(
            "/",
            content=build_packet(ClientPacket.LOGOUT, (0).to_bytes(4, "little", signed=True)),
            headers={"User-Agent": "osu!", "osu-token": "stable-token-value"},
        )

    assert response.status_code == 200
    assert response.content == b""
    assert identity.close_calls == [("stable-token-value", "client_logout")]
    assert realtime.fenced == [SessionFence(identity.session_id, 1)]


@pytest.mark.asyncio
async def test_login_and_sampled_poll_logs_are_structured_and_secret_free(monkeypatch: pytest.MonkeyPatch) -> None:
    import importlib

    cho_module = importlib.import_module("perfcho.api.stable.router.cho")
    dispatcher_module = importlib.import_module("perfcho.api.stable.dispatcher.packets")
    events: list[tuple[str, str, dict[str, object]]] = []

    def capture(level: str, event: str, **fields: object) -> None:
        events.append((level, event, fields))

    monkeypatch.setattr(cho_module, "log_event", capture)
    monkeypatch.setattr(dispatcher_module, "log_event", capture)
    services, _, _ = stable_services()
    object.__setattr__(services, "settings", Settings(log_hot_path_sample_rate=1))
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_stable_services] = lambda: services

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://c.test") as client:
        await client.post("/", content=login_body(), headers={"User-Agent": "osu!"})
        await client.post(
            "/",
            content=build_packet(ClientPacket.PING),
            headers={"User-Agent": "osu!", "osu-token": "stable-token-value"},
        )

    event_fields = {event: fields for _, event, fields in events}
    assert event_fields["stable.login.completed"]["outcome"] == "success"
    assert event_fields["stable.packet.dispatch_summary"]["packet_histogram"] == {"PING": 1}
    assert event_fields["stable.poll.completed"]["bubble_waited"] is True
    rendered = repr(events)
    for secret in ("player", "stable-token-value", "a" * 32, "path:adapters"):
        assert secret not in rendered


@pytest.mark.asyncio
async def test_invalid_token_and_malformed_packet_request_reconnect(monkeypatch: pytest.MonkeyPatch) -> None:
    import importlib

    cho_module = importlib.import_module("perfcho.api.stable.router.cho")
    events: list[tuple[str, dict[str, object]]] = []

    def capture(level: str, event: str, **fields: object) -> None:
        del level
        events.append((event, fields))

    monkeypatch.setattr(cho_module, "log_event", capture)
    monkeypatch.setattr(cho_module, "rate_limit", lambda *args, **kwargs: True)
    services, identity, realtime = stable_services()
    await realtime.open_session(
        session_id=identity.session_id,
        account_id=3,
        expires_at=NOW + timedelta(minutes=5),
        durable_expires_at=NOW + timedelta(hours=1),
    )
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_stable_services] = lambda: services

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://c.test") as client:
        expired = await client.post("/", content=b"", headers={"User-Agent": "osu!", "osu-token": "invalid"})
        malformed = await client.post(
            "/", content=b"\x01", headers={"User-Agent": "osu!", "osu-token": "stable-token-value"}
        )

    assert list(PacketReader(expired.content, packet_enum=ServerPacket))[-1].packet_type is ServerPacket.RESTART
    assert list(PacketReader(malformed.content, packet_enum=ServerPacket))[-1].packet_type is ServerPacket.RESTART
    invalid_session = next(fields for event, fields in events if event == "stable.poll.invalid_session")
    assert invalid_session["error_code"] == "invalid_session"
    assert invalid_session["error_type"] == "InvalidSession"
    assert "exception" not in invalid_session


@pytest.mark.asyncio
async def test_login_bootstrap_failure_closes_durable_session_and_fences_realtime() -> None:
    services, identity, realtime = stable_services()
    realtime.fail_set_presence = True
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_stable_services] = lambda: services

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://c.test") as client:
        with pytest.raises(RuntimeError, match="presence bootstrap failed"):
            await client.post("/", content=login_body(), headers={"User-Agent": "osu!"})

    assert identity.close_calls == [("stable-token-value", "bootstrap_failed")]
    assert realtime.fenced == [SessionFence(identity.session_id, 1)]
    assert realtime.session is None


@pytest.mark.asyncio
async def test_login_cleanup_failure_is_logged_with_exception_details(monkeypatch: pytest.MonkeyPatch) -> None:
    import importlib

    cho_module = importlib.import_module("perfcho.api.stable.router.cho")
    events: list[tuple[str, dict[str, object]]] = []

    def capture(level: str, event: str, **fields: object) -> None:
        del level
        events.append((event, fields))

    monkeypatch.setattr(cho_module, "log_event", capture)
    services, identity, realtime = stable_services()
    realtime_session = await realtime.open_session(
        session_id=identity.session_id,
        account_id=3,
        expires_at=NOW + timedelta(minutes=5),
        durable_expires_at=NOW + timedelta(hours=1),
    )
    identity.close_error = RuntimeError("durable session close failed")

    await cho_module._compensate_failed_login("stable-token-value", realtime_session, services)

    cleanup = next(fields for event, fields in events if event == "stable.login.cleanup_failed")
    assert cleanup["operation"] == "close_durable_session"
    assert cleanup["error_code"] == "cleanup_failed"
    assert cleanup["error_type"] == "RuntimeError"
    cleanup_exception = cleanup["exception"]
    assert isinstance(cleanup_exception, BaseException)
    assert cleanup_exception.args == ("durable session close failed",)
    assert "durable session close failed" in repr(events)
    assert "stable-token-value" not in repr(events)


@pytest.mark.asyncio
async def test_poll_with_lost_redis_epoch_closes_durable_session_and_restarts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import importlib

    cho_module = importlib.import_module("perfcho.api.stable.router.cho")
    events: list[tuple[str, dict[str, object]]] = []

    def capture(level: str, event: str, **fields: object) -> None:
        del level
        events.append((event, fields))

    monkeypatch.setattr(cho_module, "log_event", capture)
    services, identity, realtime = stable_services()
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_stable_services] = lambda: services

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://c.test") as client:
        response = await client.post(
            "/",
            content=build_packet(ClientPacket.PING),
            headers={"User-Agent": "osu!", "osu-token": "stable-token-value"},
        )

    assert list(PacketReader(response.content, packet_enum=ServerPacket))[-1].packet_type is ServerPacket.RESTART
    assert identity.close_calls == [("stable-token-value", "realtime_state_lost")]
    assert realtime.open_calls == 0
    assert identity.touch_calls == 1
    session_lost = next(fields for event, fields in events if event == "stable.poll.session_lost")
    assert session_lost["error_code"] == "realtime_session_not_found"
    assert session_lost["error_type"] == "RealtimeSessionNotFound"
    assert "exception" not in session_lost


@pytest.mark.asyncio
async def test_poll_subscribes_before_dispatch_and_gate_conflict_is_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import importlib

    cho_module = importlib.import_module("perfcho.api.stable.router.cho")
    services, identity, realtime = stable_services()
    await realtime.open_session(
        session_id=identity.session_id,
        account_id=3,
        expires_at=NOW + timedelta(minutes=5),
        durable_expires_at=NOW + timedelta(hours=1),
    )
    dispatched = False

    async def checked_dispatch(
        body: bytes,
        context: StableRuntimeContext,
        dispatched_services: StableServices,
    ) -> tuple[RealtimeBubble, ...]:
        nonlocal dispatched
        del body
        assert dispatched_services is services
        assert fake_bubbles(services).subscribed
        assert fake_poll_gate(services).active
        context.stable_output.extend(build_packet(ServerPacket.PONG))
        dispatched = True
        return ()

    monkeypatch.setattr(cho_module, "dispatch_packets", checked_dispatch)
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_stable_services] = lambda: services
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://c.test") as client:
        response = await client.post(
            "/",
            content=build_packet(ClientPacket.PING),
            headers={"User-Agent": "osu!", "osu-token": "stable-token-value"},
        )
        fake_poll_gate(services).conflict = True
        conflict = await client.post(
            "/",
            content=build_packet(ClientPacket.PING),
            headers={"User-Agent": "osu!", "osu-token": "stable-token-value"},
        )

    assert dispatched
    assert [packet.packet_type for packet in PacketReader(response.content, packet_enum=ServerPacket)] == [
        ServerPacket.PONG
    ]
    assert conflict.content == b""
    assert len(fake_poll_gate(services).acquired) == 1
    assert len(fake_poll_gate(services).released) == 1


@pytest.mark.asyncio
async def test_login_bootstraps_online_users_channels_silence_and_timestamped_mail() -> None:
    services, identity, realtime = stable_services()
    other_session_id = uuid.uuid7()
    realtime.online_presences[8] = presence_snapshot(
        UserPresence(8, "online", 0, 1, 1, 0, 0.0, 0.0, 50),
        UserStats(8, 0, "", "", 0, 0, 0, 0, 0.0, 0, 0, 50, 0),
        4,
        NOW + timedelta(minutes=5),
        other_session_id,
    )
    community = configure_community(services, realtime)
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_stable_services] = lambda: services

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://c.test") as client:
        response = await client.post("/", content=login_body(), headers={"User-Agent": "osu!"})

    packets = list(PacketReader(response.content, packet_enum=ServerPacket))
    channels = [packet.payload.read_channel() for packet in packets if packet.packet_type is ServerPacket.CHANNEL_INFO]
    presences = [
        packet.payload.read_user_presence() for packet in packets if packet.packet_type is ServerPacket.USER_PRESENCE
    ]
    privilege_packet = next(packet for packet in packets if packet.packet_type is ServerPacket.PRIVILEGES)
    silence = next(packet for packet in packets if packet.packet_type is ServerPacket.SILENCE_END)
    offline = next(packet for packet in packets if packet.packet_type is ServerPacket.SEND_MESSAGE)

    assert len(channels) == 1
    assert (channels[0].name, channels[0].topic, channels[0].player_count) == ("#general", "General", 4)
    assert [presence.user_id for presence in presences] == [3, 8]
    assert privilege_packet.payload.read_i32() == presences[0].privileges == StablePrivilege.PLAYER
    assert silence.payload.read_i32() == 91
    assert offline.payload.read_message().text == "[Wed Jul 29 @ 00:00AM] older message"
    assert community.policy == "friends"
    assert realtime.channel_members == {7: {3}}
    assert realtime.presence is not None
    published = fake_bubbles(services).published
    assert published == [(SessionFence(other_session_id, 4), presence_updated_bubble(realtime.presence))]
    broadcast_packets = list(PacketReader(StableBubbleRenderer().render(published[0][1]), packet_enum=ServerPacket))
    assert [packet.packet_type for packet in broadcast_packets] == [
        ServerPacket.USER_PRESENCE,
        ServerPacket.USER_STATS,
    ]
    assert broadcast_packets[0].payload.read_user_presence().privileges == StablePrivilege.PLAYER


@pytest.mark.asyncio
async def test_login_capacity_closes_new_durable_session_before_presence_truncation() -> None:
    services, identity, realtime = stable_services()
    object.__setattr__(services, "settings", Settings(stable_presence_batch_size=1))
    other_session_id = uuid.uuid7()
    realtime.online_presences[8] = presence_snapshot(
        UserPresence(8, "online", 0, 1, 1, 0, 0.0, 0.0, 0),
        UserStats(8, 0, "", "", 0, 0, 0, 0, 0.0, 0, 0, 0, 0),
        1,
        NOW + timedelta(minutes=5),
        other_session_id,
    )
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_stable_services] = lambda: services

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://c.test") as client:
        response = await client.post("/", content=login_body(), headers={"User-Agent": "osu!"})

    assert response.headers["cho-token"] == "server-full"
    assert identity.close_calls == [("stable-token-value", "bootstrap_failed")]
    assert realtime.open_calls == 0


@pytest.mark.asyncio
async def test_login_atomic_presence_claim_compensates_a_capacity_race() -> None:
    services, identity, realtime = stable_services()
    object.__setattr__(services, "settings", Settings(stable_presence_batch_size=1))
    realtime.fail_presence_capacity = True
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_stable_services] = lambda: services

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://c.test") as client:
        response = await client.post("/", content=login_body(), headers={"User-Agent": "osu!"})

    assert response.headers["cho-token"] == "server-full"
    assert identity.close_calls == [("stable-token-value", "bootstrap_failed")]
    assert realtime.open_calls == 1
    assert realtime.fenced == [SessionFence(identity.session_id, 1)]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("peer", "header", "expected"),
    [
        ("10.2.3.4", "203.0.113.9", "203.0.113.9"),
        ("10.2.3.4", "203.0.113.9, 198.51.100.2", "10.2.3.4"),
        ("192.0.2.8", "203.0.113.9", "192.0.2.8"),
    ],
)
async def test_proxy_headers_require_a_trusted_peer_and_one_strict_ip(peer: str, header: str, expected: str) -> None:
    services, identity, _ = stable_services()
    object.__setattr__(services, "settings", Settings(trusted_proxy_cidrs=["10.0.0.0/8"]))
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_stable_services] = lambda: services

    transport = httpx.ASGITransport(app=app, client=(peer, 12345))
    async with httpx.AsyncClient(transport=transport, base_url="http://c.test") as client:
        await client.post(
            "/",
            content=login_body(),
            headers={"User-Agent": "osu!", "CF-Connecting-IP": header},
        )

    assert identity.login_command is not None
    assert identity.login_command.ip_address == expected
    assert identity.login_command.meta.client.ip_address == expected


def test_trusted_proxy_configuration_rejects_non_network_cidrs() -> None:
    with pytest.raises(ValidationError, match="invalid trusted proxy CIDR"):
        Settings(trusted_proxy_cidrs=["10.0.0.1/8"])
