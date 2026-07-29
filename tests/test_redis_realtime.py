import os
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest
from redis.asyncio import Redis

from perfcho.infra.redis.realtime import RedisRealtimeRepository
from perfcho.infra.redis.state import (
    RealtimeKeys,
    datetime_to_milliseconds,
    decode_ordered_payload,
    decode_presence,
    encode_presence,
    sequence_token,
)
from perfcho.modules.realtime import (
    InvalidFrame,
    MailboxOverflow,
    PollLeaseConflict,
    PresenceSnapshot,
    RealtimeRepository,
    RealtimeSessionFenced,
    RealtimeSessionNotFound,
    SpectatorHostOffline,
)

NOW = datetime.now(UTC).replace(microsecond=0)
SESSION_EXPIRY = NOW + timedelta(seconds=40)
PRESENCE_EXPIRY = NOW + timedelta(seconds=30)
MAILBOX_EXPIRY = NOW + timedelta(seconds=45)
PREFIX = "tests:realtime"


@dataclass(slots=True)
class RepositoryDouble:
    redis: AsyncMock
    repository: RedisRealtimeRepository
    scripts: dict[str, AsyncMock]


@pytest.fixture
def repository_double() -> RepositoryDouble:
    redis = AsyncMock(spec=Redis)
    redis.get = AsyncMock()
    scripts: dict[str, AsyncMock] = {}

    def register(source: str) -> AsyncMock:
        marker = source.splitlines()[0].removeprefix("-- perfcho:").removesuffix(":v1")
        script = AsyncMock(name=marker)
        scripts[marker] = script
        return script

    redis.register_script.side_effect = register
    repository = RedisRealtimeRepository(
        redis,
        prefix=PREFIX,
        session_ttl=timedelta(minutes=1),
        presence_ttl=timedelta(minutes=1),
        mailbox_ttl=timedelta(minutes=1),
        max_packet_count=4,
        max_packet_bytes=64,
        max_frame_count=3,
        max_frame_bytes=48,
    )
    return RepositoryDouble(redis=redis, repository=repository, scripts=scripts)


def ordered_value(sequence: int, expires_at: datetime, payload: bytes) -> bytes:
    expiry_ms = datetime_to_milliseconds(expires_at)
    return f"{sequence_token(sequence)}:{expiry_ms}:".encode() + payload


def test_repository_registers_versioned_bounded_scripts(repository_double: RepositoryDouble) -> None:
    assert isinstance(repository_double.repository, RealtimeRepository)
    assert len(repository_double.scripts) == 17
    assert repository_double.redis.register_script.call_count == 17

    for call in repository_double.redis.register_script.call_args_list:
        source = call.args[0]
        assert source.startswith("-- perfcho:")
        assert ":v1" in source.splitlines()[0]
        assert "settings" not in source

    mutating_scripts = set(repository_double.scripts) - {"resolve-session", "read-frames"}
    for name, script in repository_double.scripts.items():
        source = next(
            call.args[0]
            for call in repository_double.redis.register_script.call_args_list
            if call.args[0].startswith(f"-- perfcho:{name}:v1")
        )
        if name in mutating_scripts and "DEL" not in source:
            assert "PEXPIREAT" in source
        assert script.await_count == 0


def test_state_codecs_preserve_binary_payloads_and_version_keys() -> None:
    payload = b"\x00presence:\xff\n"
    snapshot = PresenceSnapshot(42, 7, payload, PRESENCE_EXPIRY)
    packed = encode_presence(snapshot)

    assert decode_presence(packed) == snapshot
    ordered = decode_ordered_payload(ordered_value(9, MAILBOX_EXPIRY, b"\x00:a:b\xff"))
    assert ordered.sequence == 9
    assert ordered.payload == b"\x00:a:b\xff"

    keys = RealtimeKeys("deployment:")
    assert keys.session(uuid.UUID(int=1)).startswith("deployment:v1:session:")
    assert keys.mailbox_packets(42) == "deployment:v1:mailbox:42:packets"
    assert keys.spectator_frames(42) == "deployment:v1:spectator:host:42:frames"


@pytest.mark.asyncio
async def test_session_calls_use_fenced_versioned_keys(repository_double: RepositoryDouble) -> None:
    repository = repository_double.repository
    session_id = uuid.uuid4()
    expiry_ms = datetime_to_milliseconds(SESSION_EXPIRY)
    open_script = repository_double.scripts["open-session"]
    open_script.return_value = [b"OK", b"42", b"1", str(expiry_ms).encode()]

    opened = await repository.open_session(session_id=session_id, account_id=42, expires_at=SESSION_EXPIRY)

    assert opened.account_id == 42
    assert opened.revision == 1
    call = open_script.await_args
    assert call is not None
    assert call.kwargs["keys"] == [
        f"{PREFIX}:v1:session:{session_id}",
        f"{PREFIX}:v1:presence:42",
        f"{PREFIX}:v1:spectator:host:42:frames",
        f"{PREFIX}:v1:spectator:host:42:frame-bytes",
        f"{PREFIX}:v1:spectator:host:42:frame-sequence",
    ]
    assert call.kwargs["args"][1:3] == [expiry_ms, 60_000]
    assert call.kwargs["client"] is repository_double.redis

    resolve_script = repository_double.scripts["resolve-session"]
    resolve_script.return_value = [b"OK", b"42", b"1", str(expiry_ms).encode()]
    assert (await repository.resolve_session(session_id, at=NOW)).revision == 1
    resolve_call = resolve_script.await_args
    assert resolve_call is not None
    assert resolve_call.kwargs["keys"] == [f"{PREFIX}:v1:session:{session_id}"]

    heartbeat_script = repository_double.scripts["heartbeat-session"]
    heartbeat_script.return_value = [b"FENCED"]
    with pytest.raises(RealtimeSessionFenced):
        await repository.heartbeat_session(session_id, expected_revision=1, expires_at=SESSION_EXPIRY)

    heartbeat_script.return_value = [b"NOT_FOUND"]
    with pytest.raises(RealtimeSessionNotFound):
        await repository.heartbeat_session(session_id, expected_revision=1, expires_at=SESSION_EXPIRY)


@pytest.mark.asyncio
async def test_presence_is_packed_and_revision_guarded(repository_double: RepositoryDouble) -> None:
    repository = repository_double.repository
    session_id = uuid.uuid4()
    snapshot = PresenceSnapshot(42, 3, b"\x00status:\xff", PRESENCE_EXPIRY)
    set_script = repository_double.scripts["set-presence"]
    set_script.return_value = [b"OK"]

    await repository.set_presence(snapshot, session_id=session_id)

    call = set_script.await_args
    assert call is not None
    assert call.kwargs["keys"] == [f"{PREFIX}:v1:session:{session_id}", f"{PREFIX}:v1:presence:42"]
    assert decode_presence(call.kwargs["args"][-1]) == snapshot

    repository_double.redis.get.return_value = encode_presence(snapshot)
    assert await repository.get_presence(42, at=NOW) == snapshot
    assert await repository.get_presence(42, at=PRESENCE_EXPIRY) is None

    clear_script = repository_double.scripts["clear-presence"]
    clear_script.return_value = [b"STALE"]
    assert not await repository.clear_presence(42, expected_revision=2)
    clear_script.return_value = [b"OK"]
    assert await repository.clear_presence(42, expected_revision=3)


@pytest.mark.asyncio
async def test_channel_scripts_carry_session_epoch_and_filter_results(repository_double: RepositoryDouble) -> None:
    repository = repository_double.repository
    session_id = uuid.uuid4()
    join_script = repository_double.scripts["join-channel"]
    join_script.return_value = [b"OK"]

    await repository.join_channel(7, session_id=session_id, expected_revision=2)

    join_call = join_script.await_args
    assert join_call is not None
    assert join_call.kwargs["keys"] == [
        f"{PREFIX}:v1:session:{session_id}",
        f"{PREFIX}:v1:channel:7:members",
        f"{PREFIX}:v1:channel:7:epochs",
    ]
    assert join_call.kwargs["args"] == [2, str(session_id)]

    list_script = repository_double.scripts["list-channel"]
    list_script.return_value = [b"OK", b"42", b"99"]
    assert await repository.list_channel_members(7) == frozenset({42, 99})
    list_call = list_script.await_args
    assert list_call is not None
    assert list_call.kwargs["args"] == [f"{PREFIX}:v1:session:"]


@pytest.mark.asyncio
async def test_mailbox_preserves_order_bounds_and_exclusive_lease(repository_double: RepositoryDouble) -> None:
    repository = repository_double.repository
    payload = b"\x00packet:one\xff"
    enqueue_script = repository_double.scripts["enqueue-mailbox"]
    enqueue_script.return_value = [b"OK", b"11"]

    packet = await repository.enqueue_mailbox(42, payload, expires_at=MAILBOX_EXPIRY)

    assert packet.sequence == 11
    assert packet.payload == payload
    enqueue_call = enqueue_script.await_args
    assert enqueue_call is not None
    assert enqueue_call.kwargs["keys"] == [
        f"{PREFIX}:v1:mailbox:42:packets",
        f"{PREFIX}:v1:mailbox:42:bytes",
        f"{PREFIX}:v1:mailbox:42:sequence",
    ]
    assert enqueue_call.kwargs["args"][0] == payload
    assert enqueue_call.kwargs["args"][3:5] == [4, 64]

    enqueue_script.return_value = [b"OVERFLOW"]
    with pytest.raises(MailboxOverflow):
        await repository.enqueue_mailbox(42, b"overflow", expires_at=MAILBOX_EXPIRY)

    lease_id = uuid.uuid4()
    lease_script = repository_double.scripts["lease-mailbox"]
    lease_script.return_value = [
        b"OK",
        ordered_value(11, MAILBOX_EXPIRY, payload),
        ordered_value(12, MAILBOX_EXPIRY, b"two"),
    ]
    batch = await repository.lease_mailbox(42, lease_id=lease_id, limit=2, expires_at=SESSION_EXPIRY)
    assert [item.sequence for item in batch.packets] == [11, 12]
    assert batch.packets[0].payload == payload

    lease_script.return_value = [b"CONFLICT"]
    with pytest.raises(PollLeaseConflict):
        await repository.lease_mailbox(42, lease_id=uuid.uuid4(), limit=1, expires_at=SESSION_EXPIRY)

    ack_script = repository_double.scripts["ack-mailbox"]
    ack_script.return_value = [b"OK"]
    await repository.ack_mailbox(42, lease_id=lease_id, through_sequence=11)
    ack_call = ack_script.await_args
    assert ack_call is not None
    assert ack_call.kwargs["args"] == [str(lease_id), sequence_token(11), 4]

    release_script = repository_double.scripts["release-mailbox"]
    release_script.return_value = [b"OK"]
    await repository.release_mailbox(42, lease_id=lease_id)
    release_call = release_script.await_args
    assert release_call is not None
    assert release_call.kwargs["keys"] == [f"{PREFIX}:v1:mailbox:42:lease"]


@pytest.mark.asyncio
async def test_spectator_relations_and_frames_validate_host_and_bounds(repository_double: RepositoryDouble) -> None:
    repository = repository_double.repository
    attach_script = repository_double.scripts["attach-spectator"]
    attach_script.return_value = [b"OK", b"4"]

    relation = await repository.attach_spectator(42, 43, expires_at=PRESENCE_EXPIRY)

    assert relation.revision == 4
    attach_call = attach_script.await_args
    assert attach_call is not None
    assert attach_call.kwargs["keys"] == [
        f"{PREFIX}:v1:presence:42",
        f"{PREFIX}:v1:spectator:viewer:43:host",
        f"{PREFIX}:v1:spectator:host:42:viewers",
    ]

    attach_script.return_value = [b"OFFLINE"]
    with pytest.raises(SpectatorHostOffline):
        await repository.attach_spectator(42, 44, expires_at=PRESENCE_EXPIRY)

    publish_script = repository_double.scripts["publish-frame"]
    publish_script.return_value = [b"OK"]
    frame = await repository.publish_spectator_frame(
        42,
        sequence=8,
        payload=b"\x00frame:\xff",
        expires_at=PRESENCE_EXPIRY,
    )
    assert frame.sequence == 8
    publish_call = publish_script.await_args
    assert publish_call is not None
    assert publish_call.kwargs["args"][0] == sequence_token(8)
    assert publish_call.kwargs["args"][4:] == [3, 48]

    publish_script.return_value = [b"NON_MONOTONIC"]
    with pytest.raises(InvalidFrame):
        await repository.publish_spectator_frame(42, sequence=8, payload=b"old", expires_at=PRESENCE_EXPIRY)

    read_script = repository_double.scripts["read-frames"]
    read_script.return_value = [
        b"OK",
        ordered_value(9, PRESENCE_EXPIRY, b"nine"),
        ordered_value(10, PRESENCE_EXPIRY, b"ten:\x00"),
    ]
    frames = await repository.read_spectator_frames(42, after_sequence=8, limit=2, at=NOW)
    assert tuple(frame.sequence for frame in frames) == (9, 10)
    assert frames[1].payload == b"ten:\x00"


@pytest.mark.skipif(not os.getenv("TEST_REDIS_URL"), reason="TEST_REDIS_URL is not configured")
@pytest.mark.asyncio
async def test_real_redis_realtime_lifecycle() -> None:
    redis = Redis.from_url(os.environ["TEST_REDIS_URL"], decode_responses=False)
    prefix = f"tests:realtime:{uuid.uuid4()}"
    repository = RedisRealtimeRepository(
        redis,
        prefix=prefix,
        session_ttl=timedelta(seconds=30),
        presence_ttl=timedelta(seconds=30),
        mailbox_ttl=timedelta(seconds=30),
        max_packet_count=2,
        max_packet_bytes=16,
        max_frame_count=2,
        max_frame_bytes=16,
    )
    session_id = uuid.uuid4()
    now = datetime.now(UTC)

    try:
        session = await repository.open_session(
            session_id=session_id,
            account_id=42,
            expires_at=now + timedelta(seconds=25),
        )
        assert session.revision == 1

        presence = PresenceSnapshot(42, 1, b"\x00online\xff", now + timedelta(seconds=20))
        await repository.set_presence(presence, session_id=session_id)
        assert await repository.get_presence(42, at=now) == PresenceSnapshot(
            42,
            1,
            b"\x00online\xff",
            presence.expires_at.replace(microsecond=presence.expires_at.microsecond // 1000 * 1000),
        )

        await repository.join_channel(7, session_id=session_id, expected_revision=1)
        assert await repository.list_channel_members(7) == frozenset({42})

        first = await repository.enqueue_mailbox(42, b"one\x00", expires_at=now + timedelta(seconds=20))
        second = await repository.enqueue_mailbox(42, b"two\xff", expires_at=now + timedelta(seconds=20))
        with pytest.raises(MailboxOverflow):
            await repository.enqueue_mailbox(42, b"three", expires_at=now + timedelta(seconds=20))

        lease_id = uuid.uuid4()
        batch = await repository.lease_mailbox(
            42,
            lease_id=lease_id,
            limit=2,
            expires_at=now + timedelta(seconds=10),
        )
        assert batch.packets == (first, second)
        await repository.release_mailbox(42, lease_id=lease_id)

        lease_id = uuid.uuid4()
        await repository.lease_mailbox(42, lease_id=lease_id, limit=1, expires_at=now + timedelta(seconds=10))
        await repository.ack_mailbox(42, lease_id=lease_id, through_sequence=first.sequence)

        relation = await repository.attach_spectator(42, 43, expires_at=now + timedelta(seconds=15))
        frame = await repository.publish_spectator_frame(
            42,
            sequence=1,
            payload=b"frame\x00",
            expires_at=now + timedelta(seconds=15),
        )
        assert await repository.read_spectator_frames(42, after_sequence=0, limit=2, at=now) == (frame,)
        await repository.detach_spectator(42, 43, expected_revision=relation.revision)

        reopened = await repository.open_session(
            session_id=session_id,
            account_id=42,
            expires_at=now + timedelta(seconds=25),
        )
        assert reopened.revision == 2
        assert await repository.get_presence(42, at=now) is None
        assert await repository.list_channel_members(7) == frozenset()
        with pytest.raises(RealtimeSessionFenced):
            await repository.heartbeat_session(
                session_id,
                expected_revision=1,
                expires_at=now + timedelta(seconds=26),
            )

        keys = [key async for key in redis.scan_iter(match=f"{prefix}:v1:*")]
        assert keys
        ttls = [await redis.pttl(key) for key in keys]
        assert all(ttl > 0 for ttl in ttls)

        await repository.fence_session(session_id, expected_revision=2)
        with pytest.raises(RealtimeSessionNotFound):
            await repository.resolve_session(session_id, at=now)
    finally:
        keys = [key async for key in redis.scan_iter(match=f"{prefix}:v1:*")]
        if keys:
            await redis.delete(*keys)
        await redis.aclose()
