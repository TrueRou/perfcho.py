"""Implement protocol-neutral realtime state on binary Redis primitives."""

import uuid
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timedelta

from redis.asyncio import Redis
from redis.commands.core import AsyncScript

from perfcho.infra.redis.scripts import RealtimeScripts
from perfcho.infra.redis.state import (
    RealtimeKeys,
    datetime_from_milliseconds,
    datetime_to_milliseconds,
    decode_ordered_frame,
    decode_ordered_payload,
    duration_to_milliseconds,
    sequence_token,
)
from perfcho.modules.realtime import (
    MAX_FRAME_SEQUENCE,
    MAX_REVISION,
    MAX_SEQUENCE,
    InvalidFrame,
    MailboxBatch,
    MailboxOverflow,
    MailboxPacket,
    PollLeaseConflict,
    PresenceCapacityReached,
    PresenceSnapshot,
    RealtimeRepository,
    RealtimeSession,
    RealtimeSessionFenced,
    RealtimeSessionNotFound,
    SessionFence,
    SpectatorAttachment,
    SpectatorFramePublish,
    SpectatorFrameWindow,
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


def _fence(name: str, value: SessionFence) -> SessionFence:
    if not isinstance(value, SessionFence):
        raise TypeError(f"{name} must be a SessionFence")
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


def _mapping_text(values: Mapping[bytes | str, bytes | str], field: str) -> str:
    value = values.get(field.encode())
    if value is None:
        raise RuntimeError(f"stored Redis value is missing {field}")
    return _text(value)


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
        max_channels_per_session: int = 256,
        max_spectators_per_host: int = 4096,
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
        self._max_channels_per_session = _positive_integer(
            "max_channels_per_session",
            max_channels_per_session,
        )
        self._max_spectators_per_host = _positive_integer(
            "max_spectators_per_host",
            max_spectators_per_host,
        )
        self._scripts = RealtimeScripts.register(redis)

    async def _run(
        self,
        script: AsyncScript,
        *,
        keys: Sequence[str],
        args: Iterable[str | int | bytes],
    ) -> Sequence[object]:
        return _result(await script(keys=keys, args=args, client=self._redis))

    async def _session_account(self, session_id: uuid.UUID) -> int:
        account_id = await self._redis.hget(self._keys.session(session_id), "account_id")
        if account_id is None:
            raise RealtimeSessionNotFound("realtime session is absent or expired")
        return int(_text(account_id))

    @staticmethod
    def _raise_session_status(status: str) -> None:
        if status == "NOT_FOUND":
            raise RealtimeSessionNotFound("realtime session is absent or expired")
        if status == "FENCED":
            raise RealtimeSessionFenced("realtime session epoch was superseded")
        if status == "INVALID_EXPIRY":
            raise ValueError("expiry is outside the configured or durable TTL window")
        if status == "REVISION_OVERFLOW":
            raise OverflowError("realtime revision is exhausted")
        if status == "LIMIT":
            raise ValueError("realtime owned-state limit was reached")
        if status == "CAPACITY":
            raise PresenceCapacityReached("realtime presence capacity was reached")
        if status == "INVALID_CAPACITY":
            raise ValueError("presence capacity is invalid")
        if status != "OK":
            raise RuntimeError(f"unexpected realtime session script status: {status}")

    def _owned_keys(self, session_id: uuid.UUID, account_id: int) -> list[str]:
        return [
            self._keys.session(session_id),
            self._keys.account_session(account_id),
            self._keys.presence(account_id),
            self._keys.presence_index,
            self._keys.preference(account_id),
            self._keys.session_channels(session_id),
            self._keys.mailbox_packets(account_id),
            self._keys.mailbox_bytes(account_id),
            self._keys.mailbox_sequence(account_id),
            self._keys.mailbox_lease(account_id),
            self._keys.spectator_relation(account_id),
            self._keys.spectator_relation_revision(account_id),
            self._keys.spectator_viewers(account_id),
            self._keys.spectator_frames(account_id),
            self._keys.spectator_frame_bytes(account_id),
            self._keys.spectator_frame_sequence(account_id),
            self._keys.session_revision(session_id),
        ]

    async def open_session(
        self,
        *,
        session_id: uuid.UUID,
        account_id: int,
        expires_at: datetime,
        durable_expires_at: datetime,
    ) -> RealtimeSession:
        """Open an account epoch and atomically clear all state owned by its predecessor."""
        _uuid("session_id", session_id)
        _positive_integer("account_id", account_id)
        expiry_ms = datetime_to_milliseconds(expires_at)
        durable_expiry_ms = datetime_to_milliseconds(durable_expires_at)
        result = await self._run(
            self._scripts.open_session,
            keys=self._owned_keys(session_id, account_id),
            args=[
                account_id,
                str(session_id),
                expiry_ms,
                durable_expiry_ms,
                self._session_ttl_ms,
                MAX_REVISION,
                self._keys.session_prefix,
                self._keys.session_prefix,
                f"{self._keys.base}:channel:",
                f"{self._keys.base}:spectator:viewer:",
                f"{self._keys.base}:spectator:host:",
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
        """Resolve a session only while it is also the account's current epoch."""
        _uuid("session_id", session_id)
        result = await self._run(
            self._scripts.resolve_session,
            keys=[self._keys.session(session_id)],
            args=[str(session_id), datetime_to_milliseconds(at), f"{self._keys.base}:account:"],
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
        """Atomically renew the current epoch and all still-owned online state."""
        _uuid("session_id", session_id)
        _bounded_integer("expected_revision", expected_revision, MAX_REVISION)
        account_id = await self._session_account(session_id)
        expiry_ms = datetime_to_milliseconds(expires_at)
        result = await self._run(
            self._scripts.heartbeat_session,
            keys=[
                self._keys.session(session_id),
                self._keys.account_session(account_id),
                self._keys.presence(account_id),
                self._keys.presence_index,
                self._keys.preference(account_id),
                self._keys.session_channels(session_id),
                self._keys.spectator_relation(account_id),
                self._keys.spectator_viewers(account_id),
            ],
            args=[
                account_id,
                str(session_id),
                expected_revision,
                expiry_ms,
                self._session_ttl_ms,
                self._presence_ttl_ms,
                f"{self._keys.base}:channel:",
                f"{self._keys.base}:spectator:host:",
                f"{self._keys.base}:account:",
                self._keys.session_prefix,
                f"{self._keys.base}:spectator:viewer:",
            ],
        )
        self._raise_session_status(_text(result[0]))
        return RealtimeSession(
            session_id=session_id,
            account_id=int(_text(result[1])),
            revision=int(_text(result[2])),
            expires_at=datetime_from_milliseconds(int(_text(result[3]))),
        )

    async def fence_session(self, session_id: uuid.UUID, *, expected_revision: int) -> None:
        """Fence an exact account epoch and atomically remove all of its owned state."""
        _uuid("session_id", session_id)
        _bounded_integer("expected_revision", expected_revision, MAX_REVISION)
        account_id = await self._session_account(session_id)
        result = await self._run(
            self._scripts.fence_session,
            keys=self._owned_keys(session_id, account_id),
            args=[
                account_id,
                str(session_id),
                expected_revision,
                f"{self._keys.base}:channel:",
                f"{self._keys.base}:spectator:viewer:",
                f"{self._keys.base}:spectator:host:",
            ],
        )
        self._raise_session_status(_text(result[0]))

    async def set_presence(
        self,
        snapshot: PresenceSnapshot,
        *,
        session_id: uuid.UUID,
        capacity: int | None = None,
    ) -> None:
        """Atomically publish presence and optionally claim bounded online capacity."""
        if not isinstance(snapshot, PresenceSnapshot):
            raise TypeError("snapshot must be a PresenceSnapshot")
        _uuid("session_id", session_id)
        if capacity is not None:
            _positive_integer("capacity", capacity)
            if capacity > 8192:
                raise ValueError("presence capacity exceeds 8192")
        if snapshot.session_id is not None and snapshot.session_id != session_id:
            raise RealtimeSessionFenced("presence owner does not match session_id")
        result = await self._run(
            self._scripts.set_presence,
            keys=[
                self._keys.session(session_id),
                self._keys.account_session(snapshot.account_id),
                self._keys.presence(snapshot.account_id),
                self._keys.presence_index,
            ],
            args=[
                snapshot.account_id,
                str(session_id),
                snapshot.revision,
                datetime_to_milliseconds(snapshot.expires_at),
                self._presence_ttl_ms,
                snapshot.payload,
                capacity or 0,
            ],
        )
        self._raise_session_status(_text(result[0]))

    async def get_presence(self, account_id: int, *, at: datetime) -> PresenceSnapshot | None:
        """Read a live presence projection with its owning session fence."""
        _positive_integer("account_id", account_id)
        values = await self._redis.hgetall(self._keys.presence(account_id))
        if not values:
            return None
        try:
            stored_account = int(_mapping_text(values, "account_id"))
            revision = int(_mapping_text(values, "revision"))
            expires_at = datetime_from_milliseconds(int(_mapping_text(values, "expires_at")))
            session_id = uuid.UUID(_mapping_text(values, "session_id"))
            payload = _binary(values[b"payload"])
        except (KeyError, ValueError) as error:
            raise RuntimeError("stored presence is invalid") from error
        if stored_account != account_id:
            raise RuntimeError("stored presence account does not match its Redis key")
        if expires_at <= at:
            return None
        return PresenceSnapshot(account_id, revision, payload, expires_at, session_id)

    async def list_presences(self, *, at: datetime, limit: int) -> tuple[PresenceSnapshot, ...]:
        """Read a bounded online index and prune missing presence members."""
        _positive_integer("limit", limit)
        if limit > 8192:
            raise ValueError("presence limit exceeds 8192")
        at_ms = datetime_to_milliseconds(at)
        async with self._redis.pipeline(transaction=True) as pipeline:
            pipeline.zremrangebyscore(self._keys.presence_index, 0, at_ms)
            pipeline.zrangebyscore(self._keys.presence_index, at_ms + 1, "+inf", start=0, num=limit)
            indexed = await pipeline.execute()
        account_ids = tuple(sorted(int(_text(value)) for value in indexed[1]))
        if not account_ids:
            return ()
        async with self._redis.pipeline(transaction=False) as pipeline:
            for account_id in account_ids:
                pipeline.hgetall(self._keys.presence(account_id))
            values = await pipeline.execute()
        snapshots: list[PresenceSnapshot] = []
        for account_id, presence in zip(account_ids, values, strict=True):
            if not presence:
                await self._redis.zrem(self._keys.presence_index, account_id)
                continue
            snapshot = await self.get_presence(account_id, at=at)
            if snapshot is not None:
                snapshots.append(snapshot)
        return tuple(snapshots)

    async def clear_presence(self, account_id: int, *, expected_fence: SessionFence) -> bool:
        """Delete presence only when its full owning epoch still matches."""
        _positive_integer("account_id", account_id)
        fence = _fence("expected_fence", expected_fence)
        result = await self._run(
            self._scripts.clear_presence,
            keys=[self._keys.presence(account_id), self._keys.presence_index],
            args=[account_id, str(fence.session_id), fence.revision],
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
        """Store a Stable presence filter under an exact current epoch."""
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
        """Store a bounded away message under an exact current epoch."""
        if len(message) > 1024:
            raise ValueError("away message exceeds 1024 characters")
        await self._set_preference(account_id, session_id, expected_revision, "away_message", message)

    async def get_away_message(self, account_id: int) -> str:
        """Return an online account's away message or an empty string."""
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
            keys=[
                self._keys.session(session_id),
                self._keys.account_session(account_id),
                self._keys.preference(account_id),
            ],
            args=[account_id, str(session_id), expected_revision, field, value],
        )
        self._raise_session_status(_text(result[0]))

    async def join_channel(
        self,
        channel_id: int,
        *,
        session_id: uuid.UUID,
        expected_revision: int,
    ) -> None:
        """Join a channel and index it under the owning session for heartbeat renewal."""
        _positive_integer("channel_id", channel_id)
        _uuid("session_id", session_id)
        _bounded_integer("expected_revision", expected_revision, MAX_REVISION)
        account_id = await self._session_account(session_id)
        result = await self._run(
            self._scripts.join_channel,
            keys=[
                self._keys.session(session_id),
                self._keys.account_session(account_id),
                self._keys.channel_members(channel_id),
                self._keys.channel_epochs(channel_id),
                self._keys.session_channels(session_id),
            ],
            args=[
                account_id,
                str(session_id),
                expected_revision,
                channel_id,
                self._max_channels_per_session,
            ],
        )
        self._raise_session_status(_text(result[0]))

    async def leave_channel(
        self,
        channel_id: int,
        *,
        session_id: uuid.UUID,
        expected_revision: int,
    ) -> None:
        """Leave a channel only from the matching current session epoch."""
        _positive_integer("channel_id", channel_id)
        _uuid("session_id", session_id)
        _bounded_integer("expected_revision", expected_revision, MAX_REVISION)
        account_id = await self._session_account(session_id)
        result = await self._run(
            self._scripts.leave_channel,
            keys=[
                self._keys.session(session_id),
                self._keys.account_session(account_id),
                self._keys.channel_members(channel_id),
                self._keys.channel_epochs(channel_id),
                self._keys.session_channels(session_id),
            ],
            args=[account_id, str(session_id), expected_revision, channel_id],
        )
        self._raise_session_status(_text(result[0]))

    async def list_channel_members(self, channel_id: int) -> frozenset[int]:
        """List account IDs after atomically pruning expired or fenced epochs."""
        _positive_integer("channel_id", channel_id)
        result = await self._run(
            self._scripts.list_channel,
            keys=[self._keys.channel_members(channel_id), self._keys.channel_epochs(channel_id)],
            args=[self._keys.session_prefix, f"{self._keys.base}:account:"],
        )
        status = _text(result[0])
        if status != "OK":
            raise RuntimeError(f"unexpected list channel script status: {status}")
        return frozenset(int(_text(account_id)) for account_id in result[1:])

    async def is_active_member(self, channel_id: int, account_id: int, *, at: datetime) -> bool:
        """Adapt fenced Redis channel state to the Community membership query port."""
        del at
        return account_id in await self.list_channel_members(channel_id)

    async def count_active_members(self, channel_id: int, *, at: datetime) -> int:
        """Return the current fenced Redis channel membership count."""
        del at
        return len(await self.list_channel_members(channel_id))

    async def enqueue_mailbox(
        self,
        account_id: int,
        payload: bytes,
        *,
        recipient_fence: SessionFence,
        expires_at: datetime,
    ) -> MailboxPacket:
        """Append only to the mailbox owned by the explicit recipient epoch."""
        _positive_integer("account_id", account_id)
        fence = _fence("recipient_fence", recipient_fence)
        frozen_payload = _bytes(payload)
        result = await self._run(
            self._scripts.enqueue_mailbox,
            keys=[
                self._keys.session(fence.session_id),
                self._keys.account_session(account_id),
                self._keys.mailbox_packets(account_id),
                self._keys.mailbox_bytes(account_id),
                self._keys.mailbox_sequence(account_id),
            ],
            args=[
                account_id,
                str(fence.session_id),
                fence.revision,
                frozen_payload,
                datetime_to_milliseconds(expires_at),
                self._mailbox_ttl_ms,
                self._max_packet_count,
                self._max_packet_bytes,
                MAX_SEQUENCE,
            ],
        )
        status = _text(result[0])
        if status == "OVERFLOW":
            raise MailboxOverflow("mailbox packet or byte bound reached")
        if status == "SEQUENCE_OVERFLOW":
            raise MailboxOverflow("mailbox sequence is exhausted")
        self._raise_session_status(status)
        return MailboxPacket(int(_text(result[1])), frozen_payload)

    async def lease_mailbox(
        self,
        account_id: int,
        *,
        recipient_fence: SessionFence,
        lease_id: uuid.UUID,
        limit: int,
        expires_at: datetime,
    ) -> MailboxBatch:
        """Lease packets only from the explicit recipient epoch's mailbox."""
        _positive_integer("account_id", account_id)
        fence = _fence("recipient_fence", recipient_fence)
        _uuid("lease_id", lease_id)
        _positive_integer("limit", limit)
        if limit > self._max_packet_count:
            raise ValueError("limit exceeds max_packet_count")
        result = await self._run(
            self._scripts.lease_mailbox,
            keys=[
                self._keys.session(fence.session_id),
                self._keys.account_session(account_id),
                self._keys.mailbox_packets(account_id),
                self._keys.mailbox_bytes(account_id),
                self._keys.mailbox_lease(account_id),
            ],
            args=[
                account_id,
                str(fence.session_id),
                fence.revision,
                str(lease_id),
                limit,
                datetime_to_milliseconds(expires_at),
                self._mailbox_ttl_ms,
                self._max_packet_count,
            ],
        )
        status = _text(result[0])
        if status == "CONFLICT":
            raise PollLeaseConflict("mailbox already has an active poll lease")
        self._raise_session_status(status)
        packets = tuple(
            MailboxPacket(decoded.sequence, decoded.payload)
            for decoded in (decode_ordered_payload(_binary(packet)) for packet in result[1:])
        )
        return MailboxBatch(lease_id, packets, expires_at)

    async def ack_mailbox(
        self,
        account_id: int,
        *,
        recipient_fence: SessionFence,
        lease_id: uuid.UUID,
        through_sequence: int,
    ) -> None:
        """Acknowledge only a lease owned by the explicit recipient epoch."""
        _positive_integer("account_id", account_id)
        fence = _fence("recipient_fence", recipient_fence)
        _uuid("lease_id", lease_id)
        _bounded_integer("through_sequence", through_sequence, MAX_SEQUENCE)
        result = await self._run(
            self._scripts.ack_mailbox,
            keys=[
                self._keys.session(fence.session_id),
                self._keys.account_session(account_id),
                self._keys.mailbox_packets(account_id),
                self._keys.mailbox_bytes(account_id),
                self._keys.mailbox_lease(account_id),
            ],
            args=[
                account_id,
                str(fence.session_id),
                fence.revision,
                str(lease_id),
                sequence_token(through_sequence),
                self._max_packet_count,
            ],
        )
        status = _text(result[0])
        if status == "CONFLICT":
            raise PollLeaseConflict("mailbox poll lease is absent or owned by another epoch")
        if status == "INVALID_ACK":
            raise ValueError("through_sequence exceeds the leased packet range")
        self._raise_session_status(status)

    async def release_mailbox(
        self,
        account_id: int,
        *,
        recipient_fence: SessionFence,
        lease_id: uuid.UUID,
    ) -> None:
        """Release only a lease owned by the explicit recipient epoch."""
        _positive_integer("account_id", account_id)
        fence = _fence("recipient_fence", recipient_fence)
        _uuid("lease_id", lease_id)
        result = await self._run(
            self._scripts.release_mailbox,
            keys=[
                self._keys.session(fence.session_id),
                self._keys.account_session(account_id),
                self._keys.mailbox_lease(account_id),
            ],
            args=[account_id, str(fence.session_id), fence.revision, str(lease_id)],
        )
        status = _text(result[0])
        if status == "CONFLICT":
            raise PollLeaseConflict("mailbox poll lease is owned by another epoch")
        self._raise_session_status(status)

    async def attach_spectator(
        self,
        host_account_id: int,
        spectator_account_id: int,
        *,
        relation_id: uuid.UUID,
        host_fence: SessionFence,
        spectator_fence: SessionFence,
        expires_at: datetime,
        history_limit: int,
    ) -> SpectatorAttachment:
        """Atomically bind both epochs and return the latest pre-attach frame snapshot."""
        _positive_integer("host_account_id", host_account_id)
        _positive_integer("spectator_account_id", spectator_account_id)
        if host_account_id == spectator_account_id:
            raise ValueError("a spectator cannot attach to itself")
        _uuid("relation_id", relation_id)
        host = _fence("host_fence", host_fence)
        spectator = _fence("spectator_fence", spectator_fence)
        _positive_integer("history_limit", history_limit)
        if history_limit > self._max_frame_count:
            raise ValueError("history_limit exceeds max_frame_count")
        expiry_ms = datetime_to_milliseconds(expires_at)
        result = await self._run(
            self._scripts.attach_spectator,
            keys=[
                self._keys.session(host.session_id),
                self._keys.account_session(host_account_id),
                self._keys.presence(host_account_id),
                self._keys.session(spectator.session_id),
                self._keys.account_session(spectator_account_id),
                self._keys.spectator_relation(spectator_account_id),
                self._keys.spectator_relation_revision(spectator_account_id),
                self._keys.spectator_viewers(host_account_id),
                self._keys.spectator_frames(host_account_id),
                self._keys.spectator_frame_bytes(host_account_id),
                self._keys.spectator_frame_sequence(host_account_id),
            ],
            args=[
                host_account_id,
                spectator_account_id,
                str(relation_id),
                str(host.session_id),
                host.revision,
                str(spectator.session_id),
                spectator.revision,
                expiry_ms,
                self._presence_ttl_ms,
                MAX_REVISION,
                self._max_spectators_per_host,
                history_limit,
                self._max_frame_count,
                f"{self._keys.base}:spectator:viewer:",
                f"{self._keys.base}:spectator:host:",
            ],
        )
        status = _text(result[0])
        if status == "HOST_FENCED":
            raise RealtimeSessionFenced("spectator host session epoch was superseded")
        if status in {"OFFLINE", "HOST_NOT_FOUND"}:
            raise SpectatorHostOffline("spectator host epoch is offline or fenced")
        self._raise_session_status(status)
        relation = SpectatorRelation(
            host_account_id,
            spectator_account_id,
            relation_id,
            int(_text(result[1])),
            host,
            spectator,
            datetime_from_milliseconds(expiry_ms),
        )
        return SpectatorAttachment(relation, self._decode_frame_window(result[2:]))

    async def detach_spectator(
        self,
        host_account_id: int,
        spectator_account_id: int,
        *,
        relation_id: uuid.UUID,
        expected_revision: int,
        host_fence: SessionFence,
        spectator_fence: SessionFence,
    ) -> bool:
        """Remove both relation directions only for the exact relation and epochs."""
        _positive_integer("host_account_id", host_account_id)
        _positive_integer("spectator_account_id", spectator_account_id)
        _uuid("relation_id", relation_id)
        _bounded_integer("expected_revision", expected_revision, MAX_REVISION)
        host = _fence("host_fence", host_fence)
        spectator = _fence("spectator_fence", spectator_fence)
        result = await self._run(
            self._scripts.detach_spectator,
            keys=[
                self._keys.spectator_relation(spectator_account_id),
                self._keys.spectator_viewers(host_account_id),
            ],
            args=[
                host_account_id,
                spectator_account_id,
                str(relation_id),
                expected_revision,
                str(host.session_id),
                host.revision,
                str(spectator.session_id),
                spectator.revision,
            ],
        )
        status = _text(result[0])
        if status not in {"OK", "STALE"}:
            raise RuntimeError(f"unexpected detach spectator script status: {status}")
        return status == "OK"

    async def get_spectator_relation(
        self,
        spectator_account_id: int,
        *,
        spectator_fence: SessionFence,
        at: datetime,
    ) -> SpectatorRelation | None:
        """Resolve a relation only for the calling spectator's exact current epoch."""
        _positive_integer("spectator_account_id", spectator_account_id)
        spectator = _fence("spectator_fence", spectator_fence)
        result = await self._run(
            self._scripts.get_spectator,
            keys=[
                self._keys.session(spectator.session_id),
                self._keys.account_session(spectator_account_id),
                self._keys.spectator_relation(spectator_account_id),
            ],
            args=[
                spectator_account_id,
                str(spectator.session_id),
                spectator.revision,
                datetime_to_milliseconds(at),
                f"{self._keys.base}:account:",
                self._keys.session_prefix,
                f"{self._keys.base}:spectator:host:",
            ],
        )
        status = _text(result[0])
        if status == "NONE":
            return None
        self._raise_session_status(status)
        return SpectatorRelation(
            int(_text(result[1])),
            spectator_account_id,
            uuid.UUID(_text(result[2])),
            int(_text(result[3])),
            SessionFence(uuid.UUID(_text(result[4])), int(_text(result[5]))),
            SessionFence(uuid.UUID(_text(result[6])), int(_text(result[7]))),
            datetime_from_milliseconds(int(_text(result[8]))),
        )

    async def list_spectators(
        self,
        host_account_id: int,
        *,
        host_fence: SessionFence,
        at: datetime,
    ) -> tuple[SpectatorRelation, ...]:
        """Return only inverse members whose relation and both session epochs agree."""
        _positive_integer("host_account_id", host_account_id)
        host = _fence("host_fence", host_fence)
        result = await self._run(
            self._scripts.list_spectators,
            keys=[
                self._keys.session(host.session_id),
                self._keys.account_session(host_account_id),
                self._keys.spectator_viewers(host_account_id),
            ],
            args=[
                host_account_id,
                str(host.session_id),
                host.revision,
                datetime_to_milliseconds(at),
                f"{self._keys.base}:account:",
                self._keys.session_prefix,
                f"{self._keys.base}:spectator:viewer:",
            ],
        )
        self._raise_session_status(_text(result[0]))
        values = result[1:]
        if len(values) % 6:
            raise RuntimeError("Redis returned malformed spectator relations")
        relations: list[SpectatorRelation] = []
        for index in range(0, len(values), 6):
            spectator_account_id = int(_text(values[index]))
            relations.append(
                SpectatorRelation(
                    host_account_id,
                    spectator_account_id,
                    uuid.UUID(_text(values[index + 1])),
                    int(_text(values[index + 2])),
                    host,
                    SessionFence(uuid.UUID(_text(values[index + 3])), int(_text(values[index + 4]))),
                    datetime_from_milliseconds(int(_text(values[index + 5]))),
                )
            )
        return tuple(relations)

    async def publish_spectator_frame(
        self,
        host_account_id: int,
        *,
        host_fence: SessionFence,
        sequence: int,
        payload: bytes,
        expires_at: datetime,
    ) -> SpectatorFramePublish:
        """Roll history and queue live delivery in one host-epoch-fenced transition."""
        _positive_integer("host_account_id", host_account_id)
        host = _fence("host_fence", host_fence)
        _bounded_integer("sequence", sequence, MAX_FRAME_SEQUENCE)
        frozen_payload = _bytes(payload)
        result = await self._run(
            self._scripts.publish_frame,
            keys=[
                self._keys.session(host.session_id),
                self._keys.account_session(host_account_id),
                self._keys.presence(host_account_id),
                self._keys.spectator_frames(host_account_id),
                self._keys.spectator_frame_bytes(host_account_id),
                self._keys.spectator_viewers(host_account_id),
                self._keys.spectator_frame_sequence(host_account_id),
            ],
            args=[
                host_account_id,
                str(host.session_id),
                host.revision,
                sequence,
                frozen_payload,
                datetime_to_milliseconds(expires_at),
                self._presence_ttl_ms,
                MAX_FRAME_SEQUENCE,
                self._max_frame_count,
                self._max_frame_bytes,
                MAX_SEQUENCE,
                self._mailbox_ttl_ms,
                self._max_packet_count,
                self._max_packet_bytes,
                self._max_spectators_per_host,
                f"{self._keys.base}:account:",
                self._keys.session_prefix,
                f"{self._keys.base}:spectator:viewer:",
                f"{self._keys.base}:mailbox:",
            ],
        )
        status = _text(result[0])
        if status == "OFFLINE":
            raise SpectatorHostOffline("spectator host has no live fenced presence")
        if status in {"INVALID_EXPIRY", "FRAME_TOO_LARGE", "NON_MONOTONIC", "SEQUENCE_OVERFLOW"}:
            raise InvalidFrame(f"spectator frame was rejected: {status.lower()}")
        self._raise_session_status(status)
        frame = decode_ordered_frame(
            f"{sequence_token(int(_text(result[1])))}:{datetime_to_milliseconds(expires_at)}:{sequence:05d}:".encode()
            + frozen_payload
        ).frame
        return SpectatorFramePublish(frame, tuple(int(_text(value)) for value in result[2:]))

    async def read_spectator_frames(
        self,
        host_account_id: int,
        *,
        host_fence: SessionFence,
        after_cursor: int | None,
        limit: int,
        at: datetime,
    ) -> SpectatorFrameWindow:
        """Read a latest bounded window or frames after an internal host-epoch cursor."""
        _positive_integer("host_account_id", host_account_id)
        host = _fence("host_fence", host_fence)
        cursor = "" if after_cursor is None else sequence_token(after_cursor)
        _positive_integer("limit", limit)
        if limit > self._max_frame_count:
            raise ValueError("limit exceeds max_frame_count")
        result = await self._run(
            self._scripts.read_frames,
            keys=[
                self._keys.session(host.session_id),
                self._keys.account_session(host_account_id),
                self._keys.spectator_frames(host_account_id),
                self._keys.spectator_frame_bytes(host_account_id),
                self._keys.spectator_frame_sequence(host_account_id),
            ],
            args=[
                host_account_id,
                str(host.session_id),
                host.revision,
                datetime_to_milliseconds(at),
                cursor,
                limit,
                self._max_frame_count,
            ],
        )
        self._raise_session_status(_text(result[0]))
        return self._decode_frame_window(result[1:])

    @staticmethod
    def _decode_frame_window(values: Sequence[object]) -> SpectatorFrameWindow:
        if len(values) < 3:
            raise RuntimeError("Redis returned malformed frame window metadata")
        oldest = _text(values[0])
        latest = _text(values[1])
        truncated = _text(values[2]) == "1"
        frames = tuple(decode_ordered_frame(_binary(value)).frame for value in values[3:])
        return SpectatorFrameWindow(
            frames,
            int(oldest) if oldest else None,
            int(latest) if latest else None,
            truncated,
        )
