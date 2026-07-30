import uuid
from datetime import UTC, datetime, timedelta
from typing import cast

import httpx
import pytest
from fastapi import FastAPI
from pydantic import ValidationError

from perfcho.api.stable import router
from perfcho.api.stable.canonize.login import StableLoginParseError, parse_stable_login
from perfcho.api.stable.dependencies import get_stable_services
from perfcho.api.stable.dispatcher import StableRuntimeContext
from perfcho.infra.composition import StableServices
from perfcho.infra.settings import Settings
from perfcho.modules.authorization import AuthorizationQueryService, StablePrivilege
from perfcho.modules.common import Clock, IdGenerator
from perfcho.modules.community import CommunityService, OfflineDirectMessage, StableChannel
from perfcho.modules.identity import IdentityService, ResolvedStableSession, StableLogin, StableSessionResult
from perfcho.modules.realtime import (
    MailboxBatch,
    MailboxPacket,
    PresenceCapacityReached,
    PresenceSnapshot,
    RealtimeRepository,
    RealtimeSession,
    SessionFence,
)
from perfcho.modules.realtime.stable import (
    ClientPacket,
    LoginFailureReason,
    PacketReader,
    ServerPacket,
    UserPresence,
    UserStats,
    build_packet,
    user_presence,
    user_stats,
)
from perfcho.modules.social import SocialService

NOW = datetime(2026, 7, 29, tzinfo=UTC)


class FakeClock:
    def now(self) -> datetime:
        return NOW


class FakeIds:
    def new(self) -> uuid.UUID:
        return uuid.uuid7()


class FakeIdentity:
    def __init__(self) -> None:
        self.login_command = None
        self.session_id = uuid.uuid7()
        self.device_id = uuid.uuid7()
        self.touch_calls = 0
        self.close_calls: list[tuple[str, str]] = []
        self.close_error: Exception | None = None

    async def login_stable(self, command: StableLogin) -> StableSessionResult:
        self.login_command = command
        return StableSessionResult(
            account_id=3,
            current_name="player",
            session_id=self.session_id,
            device_id=self.device_id,
            raw_token="stable-token-value",
            expires_at=NOW + timedelta(hours=1),
        )

    async def resolve_stable_session(self, raw_token: str) -> ResolvedStableSession:
        if raw_token != "stable-token-value":
            from perfcho.modules.identity import InvalidStableSession

            raise InvalidStableSession()
        return ResolvedStableSession(
            account_id=3,
            current_name="player",
            auth_version=1,
            session_id=self.session_id,
            device_id=self.device_id,
            client_version="b20260711.1",
            client_variant=None,
            expires_at=NOW + timedelta(hours=1),
        )

    async def touch_stable_session(self, raw_token: str) -> ResolvedStableSession:
        self.touch_calls += 1
        return await self.resolve_stable_session(raw_token)

    async def close_stable_session(self, raw_token: str, *, reason: str = "client_closed") -> None:
        self.close_calls.append((raw_token, reason))
        if self.close_error is not None:
            raise self.close_error


class FakeAuthorization:
    async def get_stable_privileges(self, account_id: int) -> StablePrivilege:
        assert account_id == 3
        return StablePrivilege.PLAYER


class FakeRealtime:
    def __init__(self) -> None:
        self.session: RealtimeSession | None = None
        self.presence: PresenceSnapshot | None = None
        self.mailbox: list[MailboxPacket] = []
        self.online_presences: dict[int, PresenceSnapshot] = {}
        self.channel_members: dict[int, set[int]] = {}
        self.enqueued: list[tuple[int, bytes, SessionFence]] = []
        self.fenced: list[SessionFence] = []
        self.lease_fences: list[SessionFence] = []
        self.active_lease = False
        self.lease_conflict = False
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

    async def enqueue_mailbox(
        self,
        account_id: int,
        payload: bytes,
        *,
        recipient_fence: SessionFence,
        expires_at: datetime,
    ) -> MailboxPacket:
        del expires_at
        self.enqueued.append((account_id, payload, recipient_fence))
        return MailboxPacket(len(self.enqueued), payload)

    async def lease_mailbox(
        self,
        account_id: int,
        *,
        recipient_fence: SessionFence,
        lease_id: uuid.UUID,
        limit: int,
        expires_at: datetime,
    ) -> MailboxBatch:
        del account_id, limit
        if self.lease_conflict:
            from perfcho.modules.realtime import PollLeaseConflict

            raise PollLeaseConflict()
        self.lease_fences.append(recipient_fence)
        self.active_lease = True
        return MailboxBatch(lease_id, tuple(self.mailbox), expires_at)

    async def ack_mailbox(
        self,
        account_id: int,
        *,
        recipient_fence: SessionFence,
        lease_id: uuid.UUID,
        through_sequence: int,
    ) -> None:
        del account_id, lease_id
        assert self.session is not None
        assert recipient_fence == self.session.fence
        self.mailbox = [packet for packet in self.mailbox if packet.sequence > through_sequence]
        self.active_lease = False

    async def release_mailbox(
        self,
        account_id: int,
        *,
        recipient_fence: SessionFence,
        lease_id: uuid.UUID,
    ) -> None:
        del account_id, lease_id
        assert self.session is not None
        assert recipient_fence == self.session.fence
        self.active_lease = False

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
            StableChannel(7, "#general", "General", True, 2000, True, False),
            StableChannel(8, "#announcements", "News", False, 2000, False, False),
            StableChannel(9, "#lobby", "Lobby", True, 2000, True, False),
        )
        self.offline_messages = (OfflineDirectMessage(10, 20, 8, "online", uuid.uuid7(), "older message", False, NOW),)
        self.realtime: FakeRealtime | None = None

    async def set_private_message_policy(self, account_id: int, policy: str) -> str:
        assert account_id == 3
        self.policy = policy
        return policy

    async def list_public_channels(self, account_id: int) -> tuple[StableChannel, ...]:
        assert account_id == 3
        return self.channels

    async def list_unread_offline_direct_messages(self, account_id: int) -> tuple[OfflineDirectMessage, ...]:
        assert account_id == 3
        return self.offline_messages

    async def get_global_silence_remaining_seconds(self, account_id: int) -> int:
        assert account_id == 3
        return self.silence_seconds

    async def get_channel_member_count(self, account_id: int, channel_id: int) -> int:
        assert account_id == 3 and self.realtime is not None
        return len(await self.realtime.list_channel_members(channel_id)) + 3


def stable_services() -> tuple[StableServices, FakeIdentity, FakeRealtime]:
    identity = FakeIdentity()
    realtime = FakeRealtime()
    config = Settings()
    services = StableServices(
        identity=cast(IdentityService, identity),
        authorization=cast(AuthorizationQueryService, FakeAuthorization()),
        realtime=cast(RealtimeRepository, realtime),
        clock=cast(Clock, FakeClock()),
        id_generator=cast(IdGenerator, FakeIds()),
        settings=config,
    )
    return services, identity, realtime


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
    assert privilege_packet.payload.read_i32() == StablePrivilege.PLAYER | StablePrivilege.SUPPORTER
    own_presence = next(packet for packet in packets if packet.packet_type is ServerPacket.USER_PRESENCE)
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
async def test_authenticated_ping_poll_drains_mailbox() -> None:
    services, identity, realtime = stable_services()
    await realtime.open_session(
        session_id=identity.session_id,
        account_id=3,
        expires_at=NOW + timedelta(minutes=5),
        durable_expires_at=NOW + timedelta(hours=1),
    )
    realtime.mailbox.append(MailboxPacket(1, build_packet(ServerPacket.NOTIFICATION, b"\x00")))
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
        ServerPacket.PONG,
        ServerPacket.NOTIFICATION,
    ]
    assert realtime.mailbox == []
    assert identity.touch_calls == 1
    assert realtime.lease_fences == [SessionFence(identity.session_id, 1)]


@pytest.mark.asyncio
async def test_login_and_sampled_poll_logs_are_structured_and_secret_free(monkeypatch: pytest.MonkeyPatch) -> None:
    import importlib

    cho_module = importlib.import_module("perfcho.api.stable.router.cho")
    dispatcher_module = importlib.import_module("perfcho.api.stable.dispatcher")
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
    assert event_fields["stable.poll.completed"]["mailbox_stage"] == "released"
    rendered = repr(events)
    for secret in ("player", "stable-token-value", "a" * 32, "path:adapters"):
        assert secret not in rendered


@pytest.mark.asyncio
async def test_poll_response_budget_defers_mailbox_packets_without_acknowledging_them() -> None:
    services, identity, realtime = stable_services()
    object.__setattr__(services, "settings", Settings(stable_max_response_bytes=7))
    await realtime.open_session(
        session_id=identity.session_id,
        account_id=3,
        expires_at=NOW + timedelta(minutes=5),
        durable_expires_at=NOW + timedelta(hours=1),
    )
    realtime.mailbox.append(MailboxPacket(1, build_packet(ServerPacket.PONG)))
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_stable_services] = lambda: services

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://c.test") as client:
        full = await client.post(
            "/",
            content=build_packet(ClientPacket.PING),
            headers={"User-Agent": "osu!", "osu-token": "stable-token-value"},
        )
        drained = await client.post(
            "/",
            content=b"",
            headers={"User-Agent": "osu!", "osu-token": "stable-token-value"},
        )

    assert len(full.content) == services.settings.stable_max_response_bytes
    assert [packet.packet_type for packet in PacketReader(full.content, packet_enum=ServerPacket)] == [
        ServerPacket.PONG
    ]
    assert [packet.packet_type for packet in PacketReader(drained.content, packet_enum=ServerPacket)] == [
        ServerPacket.PONG
    ]
    assert realtime.mailbox == []


@pytest.mark.asyncio
async def test_invalid_token_and_malformed_packet_request_reconnect() -> None:
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
    assert not realtime.active_lease


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
async def test_login_cleanup_failure_is_logged_without_token_or_exception_text(monkeypatch: pytest.MonkeyPatch) -> None:
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
    identity.close_error = RuntimeError("stable-token-value must remain private")

    await cho_module._compensate_failed_login("stable-token-value", realtime_session, services)

    cleanup = next(fields for event, fields in events if event == "stable.login.cleanup_failed")
    assert cleanup == {
        "operation": "close_durable_session",
        "error_code": "cleanup_failed",
        "error_type": "RuntimeError",
    }
    assert "stable-token-value" not in repr(events)
    assert "must remain private" not in repr(events)


@pytest.mark.asyncio
async def test_poll_with_lost_redis_epoch_closes_durable_session_and_restarts() -> None:
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
    assert identity.touch_calls == 0


@pytest.mark.asyncio
async def test_poll_acquires_fenced_mailbox_lease_before_dispatch_and_conflict_is_empty(
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
    ) -> bytes:
        nonlocal dispatched
        del body, context
        assert dispatched_services is services
        assert realtime.active_lease
        dispatched = True
        return build_packet(ServerPacket.PONG)

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
        realtime.lease_conflict = True
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


@pytest.mark.asyncio
async def test_login_bootstraps_online_users_channels_silence_and_timestamped_mail() -> None:
    services, identity, realtime = stable_services()
    other_session_id = uuid.uuid7()
    other_payload = user_presence(UserPresence(8, "online", 0, 1, 1, 0, 0.0, 0.0, 50)) + user_stats(
        UserStats(8, 0, "", "", 0, 0, 0, 0, 0.0, 0, 0, 50, 0)
    )
    realtime.online_presences[8] = PresenceSnapshot(
        8,
        4,
        other_payload,
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
    silence = next(packet for packet in packets if packet.packet_type is ServerPacket.SILENCE_END)
    offline = next(packet for packet in packets if packet.packet_type is ServerPacket.SEND_MESSAGE)

    assert len(channels) == 1
    assert (channels[0].name, channels[0].topic, channels[0].player_count) == ("#general", "General", 4)
    assert [presence.user_id for presence in presences] == [8, 3]
    assert presences[0].privileges == StablePrivilege.PLAYER
    assert silence.payload.read_i32() == 91
    assert offline.payload.read_message().text == "[Wed Jul 29 @ 00:00AM] older message"
    assert community.policy == "friends"
    assert realtime.channel_members == {7: {3}}
    assert realtime.presence is not None
    assert realtime.enqueued == [(8, realtime.presence.payload, SessionFence(other_session_id, 4))]


@pytest.mark.asyncio
async def test_login_capacity_closes_new_durable_session_before_presence_truncation() -> None:
    services, identity, realtime = stable_services()
    object.__setattr__(services, "settings", Settings(stable_presence_batch_size=1))
    other_session_id = uuid.uuid7()
    realtime.online_presences[8] = PresenceSnapshot(
        8,
        1,
        user_presence(UserPresence(8, "online", 0, 1, 1, 0, 0.0, 0.0, 0)),
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
