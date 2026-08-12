"""MessagePack codec, Redis Stream bus, and fenced poll gate."""

import uuid
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from time import monotonic
from typing import Any, cast

import msgpack  # type: ignore[import-untyped]
from redis.asyncio import Redis
from redis.exceptions import ResponseError

from perfcho.infra.db.mods import project_scoreboard_variant
from perfcho.infra.logging import log_event
from perfcho.modules.multiplayer import SlotStatus, TeamMode, WinCondition
from perfcho.modules.realtime.bubbles import (
    CanonicalReplayFrame,
    CanonicalScoreFrame,
    ChannelMembershipAction,
    ChannelUpdatedBubble,
    ChatMessageBubble,
    MultiplayerInvitationState,
    MultiplayerRoomAction,
    MultiplayerRoomBubble,
    MultiplayerRoomSnapshot,
    MultiplayerScoreState,
    MultiplayerSignalBubble,
    MultiplayerSignalKind,
    MultiplayerSlotSnapshot,
    NotificationBubble,
    PresenceUpdatedBubble,
    RealtimeBubble,
    SessionControlAction,
    SessionControlBubble,
    SpectatorAction,
    SpectatorFrameAction,
    SpectatorFrameBubble,
    SpectatorLifecycleBubble,
    UserLogoutBubble,
)
from perfcho.modules.realtime.models import PlayerActivity, PlayerStatistics, SessionFence
from perfcho.modules.scoring import CanonicalMod, Ruleset

_VERSION = 1


def _milliseconds(value: datetime) -> int:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("datetime must be timezone-aware")
    return int(value.astimezone(UTC).timestamp() * 1000)


def _datetime(value: object) -> datetime:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("datetime value must be an integer")
    return datetime.fromtimestamp(value / 1000, tz=UTC)


def _map(value: object, fields: frozenset[str]) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != fields or any(not isinstance(key, str) for key in value):
        raise ValueError("bubble body has invalid fields")
    return cast(dict[str, Any], value)


def _activity(value: object) -> PlayerActivity:
    body = _map(value, frozenset({"action", "info", "beatmap_id", "beatmap_checksum", "ruleset", "mods"}))
    return PlayerActivity(**body)


def _statistics(value: object) -> PlayerStatistics:
    body = _map(
        value,
        frozenset({"ranked_score", "accuracy", "play_count", "total_score", "global_rank", "performance"}),
    )
    return PlayerStatistics(**body)


def _mod(value: object) -> CanonicalMod:
    return CanonicalMod(**_map(value, frozenset({"acronym", "settings"})))


def _slot(value: object) -> MultiplayerSlotSnapshot:
    body = _map(
        value,
        frozenset({"position", "status", "account_id", "team", "mods", "loaded", "skipped", "failed"}),
    )
    return MultiplayerSlotSnapshot(
        **body | {"status": SlotStatus(body["status"]), "mods": tuple(_mod(mod) for mod in body["mods"])}
    )


def _room(value: object) -> MultiplayerRoomSnapshot:
    body = _map(
        value,
        frozenset(
            {
                "room_public_id",
                "state_revision",
                "capacity",
                "host_account_id",
                "in_progress",
                "name",
                "beatmap_name",
                "external_beatmap_id",
                "beatmap_md5",
                "ruleset",
                "variant",
                "team_mode",
                "win_condition",
                "mods",
                "free_mods",
                "seed",
                "slots",
                "password_protected",
                "round_id",
                "round_participant_account_ids",
            }
        ),
    )
    return MultiplayerRoomSnapshot(
        **body
        | {
            "ruleset": Ruleset(body["ruleset"]),
            "variant": project_scoreboard_variant(tuple(_mod(mod) for mod in body["mods"])),
            "team_mode": TeamMode(body["team_mode"]),
            "win_condition": WinCondition(body["win_condition"]),
            "mods": tuple(_mod(mod) for mod in body["mods"]),
            "slots": tuple(_slot(slot) for slot in body["slots"]),
            "round_id": None if body["round_id"] is None else uuid.UUID(bytes=body["round_id"]),
            "round_participant_account_ids": tuple(body["round_participant_account_ids"]),
        }
    )


def _score(value: object) -> MultiplayerScoreState:
    return MultiplayerScoreState(
        **_map(
            value,
            frozenset(
                {
                    "account_id",
                    "elapsed_milliseconds",
                    "slot_position",
                    "count_300",
                    "count_100",
                    "count_50",
                    "count_geki",
                    "count_katu",
                    "count_miss",
                    "total_score",
                    "max_combo",
                    "current_combo",
                    "perfect",
                    "current_health",
                    "tag",
                    "score_v2",
                    "combo_portion",
                    "bonus_portion",
                }
            ),
        )
    )


def _invitation(value: object) -> MultiplayerInvitationState:
    return MultiplayerInvitationState(
        **_map(
            value,
            frozenset({"sender_account_id", "sender_name", "recipient_name", "room_name", "admission_token"}),
        )
    )


def _replay_frame(value: object) -> CanonicalReplayFrame:
    return CanonicalReplayFrame(
        **_map(
            value,
            frozenset({"timestamp_ms", "position_x", "position_y", "input_state", "auxiliary_state"}),
        )
    )


def _score_frame(value: object) -> CanonicalScoreFrame:
    return CanonicalScoreFrame(
        **_map(
            value,
            frozenset(
                {
                    "elapsed_ms",
                    "frame_index",
                    "count_300",
                    "count_100",
                    "count_50",
                    "count_geki",
                    "count_katu",
                    "count_miss",
                    "total_score",
                    "max_combo",
                    "current_combo",
                    "perfect",
                    "health",
                    "tag",
                    "score_v2",
                    "combo_portion",
                    "bonus_portion",
                }
            ),
        )
    )


def _encoded_mod(mod: CanonicalMod) -> dict[str, object]:
    body = mod.as_json()
    body.setdefault("settings", {})
    return body


def _encode_body(bubble: RealtimeBubble) -> tuple[str, dict[str, Any]]:
    match bubble:
        case PresenceUpdatedBubble():
            return "presence.updated", {
                "account_id": bubble.account_id,
                "display_name": bubble.display_name,
                "country_code": bubble.country_code,
                "utc_offset": bubble.utc_offset,
                "privileges": sorted(bubble.privileges),
                "activity": {
                    "action": bubble.activity.action,
                    "info": bubble.activity.info,
                    "beatmap_id": bubble.activity.beatmap_id,
                    "beatmap_checksum": bubble.activity.beatmap_checksum,
                    "ruleset": bubble.activity.ruleset,
                    "mods": list(bubble.activity.mods),
                },
                "statistics": {
                    "ranked_score": bubble.statistics.ranked_score,
                    "accuracy": bubble.statistics.accuracy,
                    "play_count": bubble.statistics.play_count,
                    "total_score": bubble.statistics.total_score,
                    "global_rank": bubble.statistics.global_rank,
                    "performance": bubble.statistics.performance,
                },
                "longitude": bubble.longitude,
                "latitude": bubble.latitude,
            }
        case UserLogoutBubble():
            return "user.logout", {"account_id": bubble.account_id}
        case ChatMessageBubble():
            return "chat.message", {
                "message_id": bubble.message_id,
                "channel_id": bubble.channel_id,
                "channel_name": bubble.channel_name,
                "sender_account_id": bubble.sender_account_id,
                "sender_name": bubble.sender_name,
                "content": bubble.content,
                "is_action": bubble.is_action,
                "created_at": _milliseconds(bubble.created_at),
                "direct": bubble.direct,
            }
        case ChannelUpdatedBubble():
            return "channel.updated", {
                "channel_id": bubble.channel_id,
                "name": bubble.name,
                "topic": bubble.topic,
                "member_count": bubble.member_count,
                "membership_action": bubble.membership_action.value if bubble.membership_action else None,
            }
        case MultiplayerRoomBubble():
            if bubble.local_admission_credential is not None:
                raise ValueError("local multiplayer admission credentials cannot be encoded for transport")
            room = bubble.room
            return "multiplayer.room", {
                "action": bubble.action.value,
                "local_admission_credential": bubble.local_admission_credential,
                "room": {
                    "room_public_id": room.room_public_id,
                    "state_revision": room.state_revision,
                    "capacity": room.capacity,
                    "host_account_id": room.host_account_id,
                    "in_progress": room.in_progress,
                    "name": room.name,
                    "beatmap_name": room.beatmap_name,
                    "external_beatmap_id": room.external_beatmap_id,
                    "beatmap_md5": room.beatmap_md5,
                    "ruleset": room.ruleset.value,
                    "variant": project_scoreboard_variant(room.mods).value,
                    "team_mode": room.team_mode.value,
                    "win_condition": room.win_condition.value,
                    "mods": [_encoded_mod(mod) for mod in room.mods],
                    "free_mods": room.free_mods,
                    "seed": room.seed,
                    "slots": [
                        {
                            "position": slot.position,
                            "status": slot.status.value,
                            "account_id": slot.account_id,
                            "team": slot.team,
                            "mods": [_encoded_mod(mod) for mod in slot.mods],
                            "loaded": slot.loaded,
                            "skipped": slot.skipped,
                            "failed": slot.failed,
                        }
                        for slot in room.slots
                    ],
                    "password_protected": room.password_protected,
                    "round_id": None if room.round_id is None else room.round_id.bytes,
                    "round_participant_account_ids": list(room.round_participant_account_ids),
                },
            }
        case MultiplayerSignalBubble():
            score = bubble.score
            invitation = bubble.invitation
            return "multiplayer.signal", {
                "kind": bubble.kind.value,
                "room_public_id": bubble.room_public_id,
                "actor_account_id": bubble.actor_account_id,
                "slot_position": bubble.slot_position,
                "score": None
                if score is None
                else {field: getattr(score, field) for field in score.__dataclass_fields__},
                "invitation": None
                if invitation is None
                else {field: getattr(invitation, field) for field in invitation.__dataclass_fields__},
            }
        case SpectatorLifecycleBubble():
            return "spectator.lifecycle", {
                "action": bubble.action.value,
                "host_account_id": bubble.host_account_id,
                "spectator_account_id": bubble.spectator_account_id,
            }
        case SpectatorFrameBubble():
            return "spectator.frame", {
                "host_account_id": bubble.host_account_id,
                "sequence": bubble.sequence,
                "action": bubble.action.value,
                "frames": [
                    {field: getattr(frame, field) for field in frame.__dataclass_fields__} for frame in bubble.frames
                ],
                "score": {field: getattr(bubble.score, field) for field in bubble.score.__dataclass_fields__},
                "extra": bubble.extra,
            }
        case NotificationBubble():
            return "notification", {"message": bubble.message}
        case SessionControlBubble():
            return "session.control", {"action": bubble.action.value, "retry_after_ms": bubble.retry_after_ms}
    raise TypeError(f"unsupported bubble type: {type(bubble).__name__}")


def encode_bubble(bubble: RealtimeBubble) -> bytes:
    """Encode one known Bubble as a versioned MessagePack map."""
    kind, body = _encode_body(bubble)
    return msgpack.packb({"v": _VERSION, "kind": kind, "body": body}, use_bin_type=True)


def _decode_body(kind: str, value: object) -> RealtimeBubble:
    if kind == "presence.updated":
        body = _map(
            value,
            frozenset(
                {
                    "account_id",
                    "display_name",
                    "country_code",
                    "utc_offset",
                    "privileges",
                    "activity",
                    "statistics",
                    "longitude",
                    "latitude",
                }
            ),
        )
        return PresenceUpdatedBubble(
            **body
            | {
                "privileges": frozenset(body["privileges"]),
                "activity": _activity(body["activity"]),
                "statistics": _statistics(body["statistics"]),
            }
        )
    if kind == "user.logout":
        return UserLogoutBubble(**_map(value, frozenset({"account_id"})))
    if kind == "chat.message":
        body = _map(
            value,
            frozenset(
                {
                    "message_id",
                    "channel_id",
                    "channel_name",
                    "sender_account_id",
                    "sender_name",
                    "content",
                    "is_action",
                    "created_at",
                    "direct",
                }
            ),
        )
        return ChatMessageBubble(**body | {"created_at": _datetime(body["created_at"])})
    if kind == "channel.updated":
        body = _map(value, frozenset({"channel_id", "name", "topic", "member_count", "membership_action"}))
        action = body["membership_action"]
        return ChannelUpdatedBubble(
            **body | {"membership_action": None if action is None else ChannelMembershipAction(action)}
        )
    if kind == "multiplayer.room":
        body = _map(value, frozenset({"action", "room", "local_admission_credential"}))
        if body["local_admission_credential"] is not None:
            raise ValueError("transported multiplayer room bubble cannot contain an admission credential")
        return MultiplayerRoomBubble(
            MultiplayerRoomAction(body["action"]), _room(body["room"]), body["local_admission_credential"]
        )
    if kind == "multiplayer.signal":
        body = _map(
            value,
            frozenset({"kind", "room_public_id", "actor_account_id", "slot_position", "score", "invitation"}),
        )
        return MultiplayerSignalBubble(
            MultiplayerSignalKind(body["kind"]),
            body["room_public_id"],
            body["actor_account_id"],
            body["slot_position"],
            None if body["score"] is None else _score(body["score"]),
            None if body["invitation"] is None else _invitation(body["invitation"]),
        )
    if kind == "spectator.lifecycle":
        body = _map(value, frozenset({"action", "host_account_id", "spectator_account_id"}))
        return SpectatorLifecycleBubble(
            SpectatorAction(body["action"]), body["host_account_id"], body["spectator_account_id"]
        )
    if kind == "spectator.frame":
        body = _map(value, frozenset({"host_account_id", "sequence", "action", "frames", "score", "extra"}))
        return SpectatorFrameBubble(
            body["host_account_id"],
            body["sequence"],
            SpectatorFrameAction(body["action"]),
            tuple(_replay_frame(frame) for frame in body["frames"]),
            _score_frame(body["score"]),
            body["extra"],
        )
    if kind == "notification":
        return NotificationBubble(**_map(value, frozenset({"message"})))
    if kind == "session.control":
        body = _map(value, frozenset({"action", "retry_after_ms"}))
        return SessionControlBubble(SessionControlAction(body["action"]), body["retry_after_ms"])
    raise ValueError("unknown bubble kind")


def decode_bubble(payload: bytes) -> RealtimeBubble | None:
    """Decode a whitelisted Bubble, returning None for malformed input."""
    try:
        envelope = msgpack.unpackb(payload, raw=False, strict_map_key=True)
        body = _map(envelope, frozenset({"v", "kind", "body"}))
        if body["v"] != _VERSION or not isinstance(body["kind"], str):
            raise ValueError("unsupported bubble envelope")
        return _decode_body(body["kind"], body["body"])
    except Exception as error:
        log_event(
            "WARNING",
            "redis.bubble.malformed",
            exception=error,
            dropped_reason="decode_failed",
        )
        return None


def bubble_channel(prefix: str, fence: SessionFence) -> str:
    """Return the stream isolated to one exact session epoch."""
    return f"{prefix.rstrip(':')}:events:v1:session:{fence.session_id}:{fence.revision}"


class RedisBubbleSubscription:
    """Consume valid Bubbles from one Redis Stream consumer group."""

    def __init__(self, redis: Redis, stream: str, group: str, consumer: str) -> None:
        """Bind one logical consumer to a session stream."""
        self._redis = redis
        self._stream = stream
        self._group = group
        self._consumer = consumer
        self._pending = True
        self._delivered_ids: list[bytes | str] = []
        self._closed = False

    async def _read(self, *, timeout: float) -> tuple[bytes | str, bytes] | None:
        result: Any = None
        if self._pending:
            result = await self._redis.xreadgroup(
                self._group,
                self._consumer,
                {self._stream: "0"},
                count=1,
            )
            if not result or not cast(Any, result[0])[1]:
                self._pending = False
        if not self._pending:
            result = await self._redis.xreadgroup(
                self._group,
                self._consumer,
                {self._stream: ">"},
                count=1,
                block=None if timeout <= 0 else max(1, int(timeout * 1000)),
            )
        if not result:
            return None
        _, entries = cast(Any, result[0])
        entry_id, fields = cast(Any, entries[0])
        if not isinstance(fields, Mapping):
            await self._redis.xack(self._stream, self._group, entry_id)
            return None
        payload = fields.get(b"payload", fields.get("payload"))
        if not isinstance(payload, bytes):
            await self._redis.xack(self._stream, self._group, entry_id)
            return None
        return entry_id, payload

    async def receive(self, *, timeout: float) -> RealtimeBubble | None:
        """Return the next valid Bubble before timeout."""
        deadline = monotonic() + max(timeout, 0)
        while not self._closed:
            entry = await self._read(timeout=max(0, deadline - monotonic()))
            if entry is None:
                if self._pending:
                    continue
                return None
            entry_id, payload = entry
            bubble = decode_bubble(payload)
            if bubble is not None:
                self._delivered_ids.append(entry_id)
                return bubble
            await self._redis.xack(self._stream, self._group, entry_id)
            if monotonic() >= deadline:
                return None
        return None

    async def drain(self, *, limit: int) -> tuple[RealtimeBubble, ...]:
        """Drain up to limit already buffered valid Bubbles."""
        if limit < 1:
            raise ValueError("limit must be positive")
        bubbles: list[RealtimeBubble] = []
        for _ in range(limit):
            bubble = await self.receive(timeout=0)
            if bubble is None:
                break
            bubbles.append(bubble)
        return tuple(bubbles)

    async def acknowledge(self) -> None:
        """Acknowledge all entries successfully returned to the adapter."""
        if self._delivered_ids:
            await self._redis.xack(self._stream, self._group, *self._delivered_ids)
            self._delivered_ids.clear()

    async def aclose(self) -> None:
        """Stop this request-scoped consumer once."""
        self._closed = True


class RedisRealtimeBubbleBus:
    """Publish typed Bubbles over bounded session-fenced Redis Streams."""

    def __init__(self, redis: Redis, *, prefix: str, max_entries: int = 4096, ttl_seconds: int = 360) -> None:
        """Bind the bus to an isolated Redis client and bounded retention."""
        if max_entries < 1 or ttl_seconds < 1:
            raise ValueError("Bubble Stream limits must be positive")
        self._redis = redis
        self._prefix = prefix
        self._max_entries = max_entries
        self._ttl_seconds = ttl_seconds
        self._group = "delivery-v1"
        self._consumer = "session"

    async def publish(self, recipient_fence: SessionFence, bubble: RealtimeBubble) -> int:
        """Append one encoded Bubble to a bounded session stream."""
        stream = bubble_channel(self._prefix, recipient_fence)
        async with self._redis.pipeline(transaction=False) as pipeline:
            pipeline.xadd(stream, {"payload": encode_bubble(bubble)}, maxlen=self._max_entries)
            pipeline.expire(stream, self._ttl_seconds)
            await pipeline.execute()
        return 1

    async def publish_many(self, recipient_fences: Sequence[SessionFence], bubble: RealtimeBubble) -> int:
        """Append one encoding to many fenced streams in one Redis pipeline."""
        if not recipient_fences:
            return 0
        payload = encode_bubble(bubble)
        async with self._redis.pipeline(transaction=False) as pipeline:
            for fence in recipient_fences:
                stream = bubble_channel(self._prefix, fence)
                pipeline.xadd(stream, {"payload": payload}, maxlen=self._max_entries)
                pipeline.expire(stream, self._ttl_seconds)
            await pipeline.execute()
        return len(recipient_fences)

    @asynccontextmanager
    async def subscribe(self, recipient_fence: SessionFence) -> AsyncIterator[RedisBubbleSubscription]:
        """Ensure the consumer group exists before yielding its typed consumer."""
        stream = bubble_channel(self._prefix, recipient_fence)
        try:
            await self._redis.xgroup_create(stream, self._group, id="0", mkstream=True)
        except ResponseError as error:
            if "BUSYGROUP" not in str(error):
                raise
        await self._redis.expire(stream, self._ttl_seconds)
        subscription = RedisBubbleSubscription(self._redis, stream, self._group, self._consumer)
        try:
            yield subscription
        finally:
            await subscription.aclose()


_ACQUIRE_POLL_GATE = """-- perfcho:acquire-poll-gate:v1
local current = redis.call('GET', KEYS[1])
if current ~= ARGV[1] then return 0 end
local now = redis.call('TIME')
local now_ms = now[1] * 1000 + math.floor(now[2] / 1000)
local expiry = math.min(tonumber(ARGV[3]), now_ms + tonumber(ARGV[4]))
if expiry <= now_ms then return 0 end
local result = redis.call('SET', KEYS[2], ARGV[2], 'NX', 'PXAT', expiry)
if result then return 1 end
return 0
"""

_RELEASE_POLL_GATE = """-- perfcho:release-poll-gate:v1
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""


class RedisRealtimePollGate:
    """Guard one account Poll with a short, fenced, owner-safe Redis lease."""

    def __init__(self, redis: Redis, *, prefix: str, max_ttl_seconds: int = 10) -> None:
        """Register atomic gate scripts and configure the maximum lease TTL."""
        if max_ttl_seconds < 1:
            raise ValueError("max_ttl_seconds must be positive")
        self._redis = redis
        self._prefix = prefix.rstrip(":")
        self._max_ttl_ms = max_ttl_seconds * 1000
        self._acquire = redis.register_script(_ACQUIRE_POLL_GATE)
        self._release = redis.register_script(_RELEASE_POLL_GATE)

    def _session_key(self, account_id: int) -> str:
        return f"{self._prefix}:v2:account:{account_id}:session"

    def _gate_key(self, account_id: int) -> str:
        return f"{self._prefix}:v2:poll-gate:{account_id}"

    @staticmethod
    def _fence(fence: SessionFence) -> str:
        return f"{fence.session_id}|{fence.revision}"

    @classmethod
    def _owner(cls, fence: SessionFence, gate_id: uuid.UUID) -> str:
        return f"{cls._fence(fence)}|{gate_id}"

    async def acquire(
        self, account_id: int, recipient_fence: SessionFence, gate_id: uuid.UUID, *, expires_at: datetime
    ) -> bool:
        """Acquire the gate only when the supplied fence is current."""
        if account_id < 1:
            raise ValueError("account_id must be positive")
        result = await self._acquire(
            keys=[self._session_key(account_id), self._gate_key(account_id)],
            args=[
                self._fence(recipient_fence),
                self._owner(recipient_fence, gate_id),
                _milliseconds(expires_at),
                self._max_ttl_ms,
            ],
        )
        return bool(result)

    async def release(self, account_id: int, recipient_fence: SessionFence, gate_id: uuid.UUID) -> None:
        """Delete the gate only for its exact owner."""
        await self._release(keys=[self._gate_key(account_id)], args=[self._owner(recipient_fence, gate_id)])
