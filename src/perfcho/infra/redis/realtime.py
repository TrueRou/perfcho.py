"""Implement protocol-neutral realtime state on binary Redis primitives."""

import uuid
from collections.abc import Iterable, Sequence
from datetime import datetime, timedelta

from redis.asyncio import Redis
from redis.commands.core import AsyncScript

from perfcho.infra.redis.scripts import RealtimeScripts
from perfcho.infra.redis.state import (
    RealtimeKeys,
    datetime_from_milliseconds,
    datetime_to_milliseconds,
    decode_ordered_payload,
    decode_presence,
    duration_to_milliseconds,
    encode_presence,
    revision_bytes,
    sequence_token,
)
from perfcho.modules.realtime import (
    MAX_REVISION,
    MAX_SEQUENCE,
    InvalidFrame,
    MailboxBatch,
    MailboxOverflow,
    MailboxPacket,
    PollLeaseConflict,
    PresenceSnapshot,
    RealtimeRepository,
    RealtimeSession,
    RealtimeSessionFenced,
    RealtimeSessionNotFound,
    SpectatorHostOffline,
    SpectatorRelation,
)


def _positive_integer(name: str, value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _bounded_integer(name: str, value: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= maximum:
        raise ValueError(f"{name} must be between 0 and {maximum}")
    return value


def _uuid(name: str, value: uuid.UUID) -> uuid.UUID:
    if not isinstance(value, uuid.UUID):
        raise TypeError(f"{name} must be a UUID")
    return value


def _bytes(value: bytes) -> bytes:
    if not isinstance(value, bytes | bytearray | memoryview):
        raise TypeError("payload must be bytes-like")
    return bytes(value)


def _text(value: object) -> str:
    if isinstance(value, bytes):
        return value.decode("ascii")
    if isinstance(value, str):
        return value
    if isinstance(value, int):
        return str(value)
    raise RuntimeError("Redis script returned a non-scalar status value")


def _binary(value: object) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray | memoryview):
        return bytes(value)
    raise RuntimeError("Redis must be configured with decode_responses=False")


def _result(value: object) -> Sequence[object]:
    if not isinstance(value, list | tuple) or not value:
        raise RuntimeError("Redis script returned an invalid result")
    return value


class RedisRealtimeRepository(RealtimeRepository):
    """Coordinate expiring realtime state through registered atomic scripts."""

    def __init__(
        self,
        redis: Redis,
        *,
        prefix: str,
        session_ttl: timedelta | int | float,
        presence_ttl: timedelta | int | float,
        mailbox_ttl: timedelta | int | float,
        max_packet_count: int,
        max_packet_bytes: int,
        max_frame_count: int,
        max_frame_bytes: int,
    ) -> None:
        """Store the injected client, versioned namespace, and hard state limits."""
        if not isinstance(redis, Redis):
            raise TypeError("redis must be a redis.asyncio.Redis instance")
        self._redis = redis
        self._keys = RealtimeKeys(prefix)
        self._session_ttl_ms = duration_to_milliseconds(session_ttl, name="session_ttl")
        self._presence_ttl_ms = duration_to_milliseconds(presence_ttl, name="presence_ttl")
        self._mailbox_ttl_ms = duration_to_milliseconds(mailbox_ttl, name="mailbox_ttl")
        self._max_packet_count = _positive_integer("max_packet_count", max_packet_count)
        self._max_packet_bytes = _positive_integer("max_packet_bytes", max_packet_bytes)
        self._max_frame_count = _positive_integer("max_frame_count", max_frame_count)
        self._max_frame_bytes = _positive_integer("max_frame_bytes", max_frame_bytes)
        self._scripts = RealtimeScripts.register(redis)

    async def _run(
        self,
        script: AsyncScript,
        *,
        keys: Sequence[str],
        args: Iterable[str | int | bytes],
    ) -> Sequence[object]:
        return _result(await script(keys=keys, args=args, client=self._redis))

    @staticmethod
    def _raise_session_status(status: str) -> None:
        if status == "NOT_FOUND":
            raise RealtimeSessionNotFound("realtime session is absent or expired")
        if status == "FENCED":
            raise RealtimeSessionFenced("realtime session revision was superseded")
        if status == "INVALID_EXPIRY":
            raise ValueError("expiry is outside the configured TTL window")
        if status == "REVISION_OVERFLOW":
            raise OverflowError("realtime session revision is exhausted")
        if status != "OK":
            raise RuntimeError(f"unexpected realtime session script status: {status}")

    async def open_session(
        self,
        *,
        session_id: uuid.UUID,
        account_id: int,
        expires_at: datetime,
    ) -> RealtimeSession:
        """Open a new fenced revision and clear state owned by its old epoch."""
        _uuid("session_id", session_id)
        _positive_integer("account_id", account_id)
        expiry_ms = datetime_to_milliseconds(expires_at)
        result = await self._run(
            self._scripts.open_session,
            keys=[
                self._keys.session(session_id),
                self._keys.presence(account_id),
                self._keys.spectator_frames(account_id),
                self._keys.spectator_frame_bytes(account_id),
                self._keys.spectator_frame_sequence(account_id),
                self._keys.presence_index,
                self._keys.preference(account_id),
            ],
            args=[
                account_id,
                expiry_ms,
                self._session_ttl_ms,
                MAX_REVISION,
                f"{self._keys.base}:presence:",
                f"{self._keys.base}:spectator:host:",
                f"{self._keys.base}:preference:",
            ],
        )
        self._raise_session_status(_text(result[0]))
        return RealtimeSession(
            session_id=session_id,
            account_id=int(_text(result[1])),
            revision=int(_text(result[2])),
            expires_at=datetime_from_milliseconds(int(_text(result[3]))),
        )

    async def resolve_session(self, session_id: uuid.UUID, *, at: datetime) -> RealtimeSession:
        """Resolve a session whose revision and absolute expiry remain valid."""
        _uuid("session_id", session_id)
        result = await self._run(
            self._scripts.resolve_session,
            keys=[self._keys.session(session_id)],
            args=[datetime_to_milliseconds(at)],
        )
        self._raise_session_status(_text(result[0]))
        return RealtimeSession(
            session_id=session_id,
            account_id=int(_text(result[1])),
            revision=int(_text(result[2])),
            expires_at=datetime_from_milliseconds(int(_text(result[3]))),
        )

    async def heartbeat_session(
        self,
        session_id: uuid.UUID,
        *,
        expected_revision: int,
        expires_at: datetime,
    ) -> RealtimeSession:
        """Extend only the current live session revision within its TTL limit."""
        _uuid("session_id", session_id)
        _bounded_integer("expected_revision", expected_revision, MAX_REVISION)
        expiry_ms = datetime_to_milliseconds(expires_at)
        result = await self._run(
            self._scripts.heartbeat_session,
            keys=[self._keys.session(session_id)],
            args=[expected_revision, expiry_ms, self._session_ttl_ms],
        )
        self._raise_session_status(_text(result[0]))
        return RealtimeSession(
            session_id=session_id,
            account_id=int(_text(result[1])),
            revision=int(_text(result[2])),
            expires_at=datetime_from_milliseconds(int(_text(result[3]))),
        )

    async def fence_session(self, session_id: uuid.UUID, *, expected_revision: int) -> None:
        """Fence a live revision and remove its presence and frame state."""
        _uuid("session_id", session_id)
        _bounded_integer("expected_revision", expected_revision, MAX_REVISION)
        result = await self._run(
            self._scripts.fence_session,
            keys=[self._keys.session(session_id), self._keys.presence_index],
            args=[
                expected_revision,
                revision_bytes(expected_revision),
                f"{self._keys.base}:presence:",
                f"{self._keys.base}:spectator:host:",
                f"{self._keys.base}:preference:",
            ],
        )
        self._raise_session_status(_text(result[0]))

    async def set_presence(self, snapshot: PresenceSnapshot, *, session_id: uuid.UUID) -> None:
        """Atomically publish packed presence from its current fenced session."""
        if not isinstance(snapshot, PresenceSnapshot):
            raise TypeError("snapshot must be a PresenceSnapshot")
        _uuid("session_id", session_id)
        expiry_ms = datetime_to_milliseconds(snapshot.expires_at)
        result = await self._run(
            self._scripts.set_presence,
            keys=[
                self._keys.session(session_id),
                self._keys.presence(snapshot.account_id),
                self._keys.presence_index,
            ],
            args=[snapshot.account_id, snapshot.revision, expiry_ms, self._presence_ttl_ms, encode_presence(snapshot)],
        )
        self._raise_session_status(_text(result[0]))

    async def get_presence(self, account_id: int, *, at: datetime) -> PresenceSnapshot | None:
        """Read a live packed presence value without interpreting its payload."""
        _positive_integer("account_id", account_id)
        at_ms = datetime_to_milliseconds(at)
        stored = await self._redis.get(self._keys.presence(account_id))
        if stored is None:
            return None
        snapshot = decode_presence(_binary(stored))
        if snapshot.account_id != account_id:
            raise RuntimeError("stored presence account does not match its Redis key")
        if datetime_to_milliseconds(snapshot.expires_at) <= at_ms:
            return None
        return snapshot

    async def list_presences(self, *, at: datetime, limit: int) -> tuple[PresenceSnapshot, ...]:
        """Read a bounded online index and prune stale presence members."""
        _positive_integer("limit", limit)
        if limit > 8192:
            raise ValueError("presence limit exceeds 8192")
        at_ms = datetime_to_milliseconds(at)
        async with self._redis.pipeline(transaction=True) as pipeline:
            pipeline.zremrangebyscore(self._keys.presence_index, 0, at_ms)
            pipeline.zrangebyscore(self._keys.presence_index, at_ms + 1, "+inf", start=0, num=limit)
            result = await pipeline.execute()
        account_ids = tuple(sorted(int(_text(value)) for value in result[1]))
        if not account_ids:
            return ()
        values = await self._redis.mget([self._keys.presence(account_id) for account_id in account_ids])
        snapshots: list[PresenceSnapshot] = []
        for account_id, value in zip(account_ids, values, strict=True):
            if value is None:
                await self._redis.zrem(self._keys.presence_index, account_id)
                continue
            snapshot = decode_presence(_binary(value))
            if snapshot.account_id == account_id and snapshot.expires_at > at:
                snapshots.append(snapshot)
        return tuple(snapshots)

    async def clear_presence(self, account_id: int, *, expected_revision: int) -> bool:
        """Delete presence only when its packed revision remains current."""
        _positive_integer("account_id", account_id)
        _bounded_integer("expected_revision", expected_revision, MAX_REVISION)
        result = await self._run(
            self._scripts.clear_presence,
            keys=[self._keys.presence(account_id), self._keys.presence_index],
            args=[revision_bytes(expected_revision), account_id],
        )
        status = _text(result[0])
        if status not in {"OK", "STALE"}:
            raise RuntimeError(f"unexpected clear presence script status: {status}")
        return status == "OK"

    async def set_presence_filter(
        self,
        account_id: int,
        *,
        session_id: uuid.UUID,
        expected_revision: int,
        value: int,
    ) -> None:
        """Store a validated Stable presence filter under the session fence."""
        if value not in {0, 1, 2}:
            raise ValueError("presence filter must be zero, one, or two")
        await self._set_preference(account_id, session_id, expected_revision, "presence_filter", str(value))

    async def get_presence_filter(self, account_id: int) -> int:
        """Return the current Stable presence filter or zero when absent."""
        _positive_integer("account_id", account_id)
        value = await self._redis.hget(self._keys.preference(account_id), "presence_filter")
        if value is None:
            return 0
        result = int(_text(value))
        if result not in {0, 1, 2}:
            raise RuntimeError("stored presence filter is invalid")
        return result

    async def set_away_message(
        self,
        account_id: int,
        *,
        session_id: uuid.UUID,
        expected_revision: int,
        message: str,
    ) -> None:
        """Store a bounded away message under the session fence."""
        if len(message) > 1024:
            raise ValueError("away message exceeds 1024 characters")
        await self._set_preference(account_id, session_id, expected_revision, "away_message", message)

    async def get_away_message(self, account_id: int) -> str:
        """Return the online away message or an empty string."""
        _positive_integer("account_id", account_id)
        value = await self._redis.hget(self._keys.preference(account_id), "away_message")
        return _binary(value).decode() if value is not None else ""

    async def _set_preference(
        self,
        account_id: int,
        session_id: uuid.UUID,
        expected_revision: int,
        field: str,
        value: str,
    ) -> None:
        _positive_integer("account_id", account_id)
        _uuid("session_id", session_id)
        _bounded_integer("expected_revision", expected_revision, MAX_REVISION)
        result = await self._run(
            self._scripts.set_preference,
            keys=[self._keys.session(session_id), self._keys.preference(account_id)],
            args=[account_id, expected_revision, field, value],
        )
        self._raise_session_status(_text(result[0]))

    async def join_channel(
        self,
        channel_id: int,
        *,
        session_id: uuid.UUID,
        expected_revision: int,
    ) -> None:
        """Join a channel under the session's current revision and expiry."""
        _positive_integer("channel_id", channel_id)
        _uuid("session_id", session_id)
        _bounded_integer("expected_revision", expected_revision, MAX_REVISION)
        result = await self._run(
            self._scripts.join_channel,
            keys=[
                self._keys.session(session_id),
                self._keys.channel_members(channel_id),
                self._keys.channel_epochs(channel_id),
            ],
            args=[expected_revision, str(session_id)],
        )
        self._raise_session_status(_text(result[0]))

    async def leave_channel(
        self,
        channel_id: int,
        *,
        session_id: uuid.UUID,
        expected_revision: int,
    ) -> None:
        """Leave a channel only from the matching live session revision."""
        _positive_integer("channel_id", channel_id)
        _uuid("session_id", session_id)
        _bounded_integer("expected_revision", expected_revision, MAX_REVISION)
        result = await self._run(
            self._scripts.leave_channel,
            keys=[
                self._keys.session(session_id),
                self._keys.channel_members(channel_id),
                self._keys.channel_epochs(channel_id),
            ],
            args=[expected_revision, str(session_id)],
        )
        self._raise_session_status(_text(result[0]))

    async def list_channel_members(self, channel_id: int) -> frozenset[int]:
        """List account IDs after atomically removing expired or fenced members."""
        _positive_integer("channel_id", channel_id)
        result = await self._run(
            self._scripts.list_channel,
            keys=[self._keys.channel_members(channel_id), self._keys.channel_epochs(channel_id)],
            args=[self._keys.session_prefix],
        )
        status = _text(result[0])
        if status != "OK":
            raise RuntimeError(f"unexpected list channel script status: {status}")
        return frozenset(int(_text(account_id)) for account_id in result[1:])

    async def enqueue_mailbox(
        self,
        account_id: int,
        payload: bytes,
        *,
        expires_at: datetime,
    ) -> MailboxPacket:
        """Append a binary packet under the configured count and byte bounds."""
        _positive_integer("account_id", account_id)
        frozen_payload = _bytes(payload)
        expiry_ms = datetime_to_milliseconds(expires_at)
        result = await self._run(
            self._scripts.enqueue_mailbox,
            keys=[
                self._keys.mailbox_packets(account_id),
                self._keys.mailbox_bytes(account_id),
                self._keys.mailbox_sequence(account_id),
            ],
            args=[
                frozen_payload,
                expiry_ms,
                self._mailbox_ttl_ms,
                self._max_packet_count,
                self._max_packet_bytes,
                MAX_SEQUENCE,
            ],
        )
        status = _text(result[0])
        if status == "OVERFLOW":
            raise MailboxOverflow("mailbox packet or byte bound reached")
        if status == "INVALID_EXPIRY":
            raise ValueError("mailbox expiry is outside the configured TTL window")
        if status == "SEQUENCE_OVERFLOW":
            raise MailboxOverflow("mailbox sequence is exhausted")
        if status != "OK":
            raise RuntimeError(f"unexpected enqueue mailbox script status: {status}")
        return MailboxPacket(sequence=int(_text(result[1])), payload=frozen_payload)

    async def lease_mailbox(
        self,
        account_id: int,
        *,
        lease_id: uuid.UUID,
        limit: int,
        expires_at: datetime,
    ) -> MailboxBatch:
        """Acquire one exclusive expiring lease and return a bounded packet batch."""
        _positive_integer("account_id", account_id)
        _uuid("lease_id", lease_id)
        _positive_integer("limit", limit)
        if limit > self._max_packet_count:
            raise ValueError("limit exceeds max_packet_count")
        expiry_ms = datetime_to_milliseconds(expires_at)
        result = await self._run(
            self._scripts.lease_mailbox,
            keys=[
                self._keys.mailbox_packets(account_id),
                self._keys.mailbox_bytes(account_id),
                self._keys.mailbox_lease(account_id),
            ],
            args=[
                str(lease_id),
                limit,
                expiry_ms,
                self._mailbox_ttl_ms,
                self._max_packet_count,
            ],
        )
        status = _text(result[0])
        if status == "CONFLICT":
            raise PollLeaseConflict("mailbox already has an active poll lease")
        if status == "INVALID_EXPIRY":
            raise ValueError("mailbox lease expiry is outside the configured TTL window")
        if status != "OK":
            raise RuntimeError(f"unexpected lease mailbox script status: {status}")
        packets = tuple(
            MailboxPacket(sequence=decoded.sequence, payload=decoded.payload)
            for decoded in (decode_ordered_payload(_binary(packet)) for packet in result[1:])
        )
        return MailboxBatch(
            lease_id=lease_id,
            packets=packets,
            expires_at=datetime_from_milliseconds(expiry_ms),
        )

    async def ack_mailbox(
        self,
        account_id: int,
        *,
        lease_id: uuid.UUID,
        through_sequence: int,
    ) -> None:
        """Delete packets through a sequence covered by the caller's lease."""
        _positive_integer("account_id", account_id)
        _uuid("lease_id", lease_id)
        _bounded_integer("through_sequence", through_sequence, MAX_SEQUENCE)
        result = await self._run(
            self._scripts.ack_mailbox,
            keys=[
                self._keys.mailbox_packets(account_id),
                self._keys.mailbox_bytes(account_id),
                self._keys.mailbox_lease(account_id),
            ],
            args=[str(lease_id), sequence_token(through_sequence), self._max_packet_count],
        )
        status = _text(result[0])
        if status == "CONFLICT":
            raise PollLeaseConflict("mailbox poll lease is absent or owned by another poller")
        if status == "INVALID_ACK":
            raise ValueError("through_sequence exceeds the leased packet range")
        if status != "OK":
            raise RuntimeError(f"unexpected ack mailbox script status: {status}")

    async def release_mailbox(self, account_id: int, *, lease_id: uuid.UUID) -> None:
        """Release the caller's lease without deleting any mailbox packets."""
        _positive_integer("account_id", account_id)
        _uuid("lease_id", lease_id)
        result = await self._run(
            self._scripts.release_mailbox,
            keys=[self._keys.mailbox_lease(account_id)],
            args=[str(lease_id)],
        )
        status = _text(result[0])
        if status == "CONFLICT":
            raise PollLeaseConflict("mailbox poll lease is owned by another poller")
        if status != "OK":
            raise RuntimeError(f"unexpected release mailbox script status: {status}")

    async def attach_spectator(
        self,
        host_account_id: int,
        spectator_account_id: int,
        *,
        expires_at: datetime,
    ) -> SpectatorRelation:
        """Create both sides of a versioned relation to an online host."""
        _positive_integer("host_account_id", host_account_id)
        _positive_integer("spectator_account_id", spectator_account_id)
        if host_account_id == spectator_account_id:
            raise ValueError("a spectator cannot attach to itself")
        expiry_ms = datetime_to_milliseconds(expires_at)
        result = await self._run(
            self._scripts.attach_spectator,
            keys=[
                self._keys.presence(host_account_id),
                self._keys.spectator_relation(spectator_account_id),
                self._keys.spectator_viewers(host_account_id),
            ],
            args=[
                host_account_id,
                spectator_account_id,
                expiry_ms,
                self._presence_ttl_ms,
                MAX_REVISION,
                f"{self._keys.base}:spectator:host:",
            ],
        )
        status = _text(result[0])
        if status == "OFFLINE":
            raise SpectatorHostOffline("spectator host has no live presence")
        if status == "INVALID_EXPIRY":
            raise ValueError("spectator expiry is outside the host presence window")
        if status == "REVISION_OVERFLOW":
            raise OverflowError("spectator relation revision is exhausted")
        if status != "OK":
            raise RuntimeError(f"unexpected attach spectator script status: {status}")
        return SpectatorRelation(
            host_account_id=host_account_id,
            spectator_account_id=spectator_account_id,
            revision=int(_text(result[1])),
            expires_at=datetime_from_milliseconds(expiry_ms),
        )

    async def detach_spectator(
        self,
        host_account_id: int,
        spectator_account_id: int,
        *,
        expected_revision: int,
    ) -> None:
        """Remove both relation directions only for the matching revision."""
        _positive_integer("host_account_id", host_account_id)
        _positive_integer("spectator_account_id", spectator_account_id)
        _bounded_integer("expected_revision", expected_revision, MAX_REVISION)
        result = await self._run(
            self._scripts.detach_spectator,
            keys=[
                self._keys.spectator_relation(spectator_account_id),
                self._keys.spectator_viewers(host_account_id),
            ],
            args=[host_account_id, spectator_account_id, expected_revision],
        )
        status = _text(result[0])
        if status not in {"OK", "STALE"}:
            raise RuntimeError(f"unexpected detach spectator script status: {status}")

    async def get_spectator_relation(
        self,
        spectator_account_id: int,
        *,
        at: datetime,
    ) -> SpectatorRelation | None:
        """Resolve one unexpired spectator relation and clean its stale inverse member."""
        _positive_integer("spectator_account_id", spectator_account_id)
        values = await self._redis.hgetall(self._keys.spectator_relation(spectator_account_id))
        if not values:
            return None
        try:
            host_account_id = int(_text(values[b"host_account_id"]))
            revision = int(_text(values[b"revision"]))
            expires_at = datetime_from_milliseconds(int(_text(values[b"expires_at"])))
        except (KeyError, ValueError) as error:
            raise RuntimeError("stored spectator relation is invalid") from error
        if expires_at <= at:
            await self._redis.delete(self._keys.spectator_relation(spectator_account_id))
            await self._redis.zrem(self._keys.spectator_viewers(host_account_id), spectator_account_id)
            return None
        return SpectatorRelation(host_account_id, spectator_account_id, revision, expires_at)

    async def list_spectators(self, host_account_id: int, *, at: datetime) -> frozenset[int]:
        """Return unexpired inverse spectator members after pruning stale scores."""
        _positive_integer("host_account_id", host_account_id)
        key = self._keys.spectator_viewers(host_account_id)
        at_ms = datetime_to_milliseconds(at)
        async with self._redis.pipeline(transaction=True) as pipeline:
            pipeline.zremrangebyscore(key, 0, at_ms)
            pipeline.zrangebyscore(key, at_ms + 1, "+inf")
            result = await pipeline.execute()
        return frozenset(int(_text(value)) for value in result[1])

    async def publish_spectator_frame(
        self,
        host_account_id: int,
        *,
        sequence: int,
        payload: bytes,
        expires_at: datetime,
    ) -> MailboxPacket:
        """Append a monotonic frame under the host's live presence and bounds."""
        _positive_integer("host_account_id", host_account_id)
        token = sequence_token(sequence)
        frozen_payload = _bytes(payload)
        expiry_ms = datetime_to_milliseconds(expires_at)
        result = await self._run(
            self._scripts.publish_frame,
            keys=[
                self._keys.presence(host_account_id),
                self._keys.spectator_frames(host_account_id),
                self._keys.spectator_frame_bytes(host_account_id),
                self._keys.spectator_frame_sequence(host_account_id),
            ],
            args=[
                token,
                frozen_payload,
                expiry_ms,
                self._presence_ttl_ms,
                self._max_frame_count,
                self._max_frame_bytes,
            ],
        )
        status = _text(result[0])
        if status == "OFFLINE":
            raise SpectatorHostOffline("spectator host has no live presence")
        if status in {"INVALID_EXPIRY", "OVERFLOW", "NON_MONOTONIC"}:
            raise InvalidFrame(f"spectator frame was rejected: {status.lower()}")
        if status != "OK":
            raise RuntimeError(f"unexpected publish frame script status: {status}")
        return MailboxPacket(sequence=sequence, payload=frozen_payload)

    async def read_spectator_frames(
        self,
        host_account_id: int,
        *,
        after_sequence: int,
        limit: int,
        at: datetime,
    ) -> tuple[MailboxPacket, ...]:
        """Read ordered, unexpired frames strictly after the supplied cursor."""
        _positive_integer("host_account_id", host_account_id)
        cursor = sequence_token(after_sequence)
        _positive_integer("limit", limit)
        if limit > self._max_frame_count:
            raise ValueError("limit exceeds max_frame_count")
        result = await self._run(
            self._scripts.read_frames,
            keys=[self._keys.spectator_frames(host_account_id)],
            args=[cursor, limit, datetime_to_milliseconds(at), self._max_frame_count],
        )
        status = _text(result[0])
        if status != "OK":
            raise RuntimeError(f"unexpected read frames script status: {status}")
        return tuple(
            MailboxPacket(sequence=decoded.sequence, payload=decoded.payload)
            for decoded in (decode_ordered_payload(_binary(frame)) for frame in result[1:])
        )
