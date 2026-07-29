import uuid
from datetime import UTC, datetime, timedelta
from typing import cast

import httpx
import pytest
from fastapi import FastAPI

from perfcho.api.stable import router
from perfcho.api.stable.dependencies import get_stable_services
from perfcho.api.stable.schema import StableLoginParseError, parse_stable_login
from perfcho.composition import StableServices
from perfcho.infra.settings import Settings
from perfcho.modules.authorization import AuthorizationQueryService, StablePrivilege
from perfcho.modules.common import Clock, IdGenerator
from perfcho.modules.identity import IdentityService, ResolvedStableSession, StableLogin, StableSessionResult
from perfcho.modules.realtime import (
    MailboxBatch,
    MailboxPacket,
    PresenceSnapshot,
    RealtimeRepository,
    RealtimeSession,
)
from perfcho.realtime.stable import (
    ClientPacket,
    LoginFailureReason,
    PacketReader,
    ServerPacket,
    build_packet,
)

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


class FakeAuthorization:
    async def get_stable_privileges(self, account_id: int) -> StablePrivilege:
        assert account_id == 3
        return StablePrivilege.PLAYER


class FakeRealtime:
    def __init__(self) -> None:
        self.session: RealtimeSession | None = None
        self.presence: PresenceSnapshot | None = None
        self.mailbox: list[MailboxPacket] = []

    async def open_session(
        self,
        *,
        session_id: uuid.UUID,
        account_id: int,
        expires_at: datetime,
    ) -> RealtimeSession:
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

    async def set_presence(self, snapshot: PresenceSnapshot, *, session_id: uuid.UUID) -> None:
        assert self.session is not None and session_id == self.session.session_id
        self.presence = snapshot

    async def get_presence(self, account_id: int, *, at: datetime) -> PresenceSnapshot | None:
        del at
        return self.presence if self.presence is not None and self.presence.account_id == account_id else None

    async def lease_mailbox(
        self,
        account_id: int,
        *,
        lease_id: uuid.UUID,
        limit: int,
        expires_at: datetime,
    ) -> MailboxBatch:
        del account_id, limit
        return MailboxBatch(lease_id, tuple(self.mailbox), expires_at)

    async def ack_mailbox(
        self,
        account_id: int,
        *,
        lease_id: uuid.UUID,
        through_sequence: int,
    ) -> None:
        del account_id, lease_id
        self.mailbox = [packet for packet in self.mailbox if packet.sequence > through_sequence]

    async def release_mailbox(self, account_id: int, *, lease_id: uuid.UUID) -> None:
        del account_id, lease_id


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


@pytest.mark.asyncio
async def test_invalid_token_and_malformed_packet_request_reconnect() -> None:
    services, identity, realtime = stable_services()
    await realtime.open_session(
        session_id=identity.session_id,
        account_id=3,
        expires_at=NOW + timedelta(minutes=5),
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
