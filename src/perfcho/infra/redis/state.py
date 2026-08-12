"""Encode Redis realtime values and construct its versioned state keys."""

import struct
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import msgpack

from perfcho.modules.realtime import (
    MAX_SEQUENCE,
    PlayerActivity,
    PlayerStatistics,
    PresenceIdentity,
    PresenceSnapshot,
)

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_PRESENCE_VERSION = 1
_PRESENCE_FIELDS = frozenset(
    {"v", "account_id", "revision", "session_id", "expires_at", "identity", "activity", "statistics"}
)
_SEQUENCE_WIDTH = len(str(MAX_SEQUENCE))
_FRAME_SEQUENCE_WIDTH = 5


def datetime_to_milliseconds(value: datetime) -> int:
    """Convert an aware datetime to a non-negative Unix millisecond instant."""
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("datetime must be timezone-aware")
    delta = value.astimezone(UTC) - _EPOCH
    milliseconds = (delta.days * 86_400 + delta.seconds) * 1_000 + delta.microseconds // 1_000
    if milliseconds < 0:
        raise ValueError("datetime must not precede the Unix epoch")
    return milliseconds


def datetime_from_milliseconds(value: int) -> datetime:
    """Convert a Unix millisecond instant to an aware UTC datetime."""
    return _EPOCH + timedelta(milliseconds=value)


def duration_to_milliseconds(value: timedelta | int | float, *, name: str) -> int:
    """Normalize a positive duration expressed as a timedelta or seconds."""
    seconds = value.total_seconds() if isinstance(value, timedelta) else value
    if isinstance(seconds, bool) or not isinstance(seconds, int | float) or seconds <= 0:
        raise ValueError(f"{name} must be a positive duration")
    milliseconds = int(seconds * 1_000)
    if milliseconds < 1:
        raise ValueError(f"{name} must be at least one millisecond")
    return milliseconds


def encode_presence(snapshot: PresenceSnapshot) -> bytes:
    """Encode one canonical presence snapshot as explicit versioned MessagePack."""
    if not isinstance(snapshot, PresenceSnapshot):
        raise TypeError("snapshot must be a PresenceSnapshot")
    identity = snapshot.identity
    activity = snapshot.activity
    statistics = snapshot.statistics
    return msgpack.packb(
        {
            "v": _PRESENCE_VERSION,
            "account_id": snapshot.account_id,
            "revision": snapshot.revision,
            "session_id": snapshot.session_id.bytes,
            "expires_at": datetime_to_milliseconds(snapshot.expires_at),
            "identity": {
                "display_name": identity.display_name,
                "country_code": identity.country_code,
                "utc_offset": identity.utc_offset,
                "privileges": sorted(identity.privileges),
                "longitude": identity.longitude,
                "latitude": identity.latitude,
            },
            "activity": {
                "action": activity.action,
                "info": activity.info,
                "beatmap_id": activity.beatmap_id,
                "beatmap_checksum": activity.beatmap_checksum,
                "ruleset": activity.ruleset,
                "mods": list(activity.mods),
            },
            "statistics": {
                "ranked_score": statistics.ranked_score,
                "accuracy": statistics.accuracy,
                "play_count": statistics.play_count,
                "total_score": statistics.total_score,
                "global_rank": statistics.global_rank,
                "performance": statistics.performance,
            },
        },
        use_bin_type=True,
    )


def _presence_map(value: object, fields: frozenset[str], *, name: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != fields or any(not isinstance(key, str) for key in value):
        raise ValueError(f"stored presence {name} has invalid fields")
    return cast(dict[str, Any], value)


def decode_presence(value: bytes) -> PresenceSnapshot:
    """Strictly decode a versioned canonical presence snapshot."""
    if not isinstance(value, bytes | bytearray | memoryview):
        raise TypeError("stored presence must be bytes-like")
    if len(value) > 64 * 1024:
        raise ValueError("stored presence exceeds the maximum encoded size")
    try:
        unpacked = msgpack.unpackb(bytes(value), raw=False, strict_map_key=True)
        body = _presence_map(unpacked, _PRESENCE_FIELDS, name="envelope")
        if isinstance(body["v"], bool) or body["v"] != _PRESENCE_VERSION:
            raise ValueError("stored presence version is unsupported")
        for field in ("account_id", "revision", "expires_at"):
            if isinstance(body[field], bool) or not isinstance(body[field], int):
                raise ValueError(f"stored presence {field} is invalid")
        identity = _presence_map(
            body["identity"],
            frozenset({"display_name", "country_code", "utc_offset", "privileges", "longitude", "latitude"}),
            name="identity",
        )
        activity = _presence_map(
            body["activity"],
            frozenset({"action", "info", "beatmap_id", "beatmap_checksum", "ruleset", "mods"}),
            name="activity",
        )
        statistics = _presence_map(
            body["statistics"],
            frozenset({"ranked_score", "accuracy", "play_count", "total_score", "global_rank", "performance"}),
            name="statistics",
        )
        session_bytes = body["session_id"]
        if not isinstance(session_bytes, bytes) or len(session_bytes) != 16:
            raise ValueError("stored presence session_id is invalid")
        privileges = identity["privileges"]
        mods = activity["mods"]
        if not isinstance(privileges, list) or not isinstance(mods, list):
            raise ValueError("stored presence collections are invalid")
        return PresenceSnapshot(
            account_id=body["account_id"],
            revision=body["revision"],
            identity=PresenceIdentity(**identity | {"privileges": frozenset(privileges)}),
            activity=PlayerActivity(**activity | {"mods": tuple(mods)}),
            statistics=PlayerStatistics(**statistics),
            expires_at=datetime_from_milliseconds(body["expires_at"]),
            session_id=uuid.UUID(bytes=session_bytes),
        )
    except (KeyError, OverflowError, TypeError, ValueError, msgpack.UnpackException) as error:
        if isinstance(error, ValueError) and str(error).startswith("stored presence"):
            raise
        raise ValueError("stored presence is invalid") from error


def sequence_token(sequence: int) -> str:
    """Return a fixed-width decimal sequence suitable for lexical ordering."""
    if isinstance(sequence, bool) or not isinstance(sequence, int) or not 0 <= sequence <= MAX_SEQUENCE:
        raise ValueError(f"sequence must be between 0 and {MAX_SEQUENCE}")
    return f"{sequence:0{_SEQUENCE_WIDTH}d}"


def revision_bytes(revision: int) -> bytes:
    """Encode a revision for constant-width comparison inside Redis Lua."""
    try:
        return struct.pack(">Q", revision)
    except (struct.error, TypeError) as error:
        raise ValueError("revision exceeds the Redis encoding range") from error


@dataclass(frozen=True, slots=True)
class OrderedFrame:
    """Represent one decoded cursor-ordered canonical frame value."""

    cursor: int
    sequence: int
    payload: bytes
    expires_ms: int


def decode_ordered_frame(value: bytes) -> OrderedFrame:
    """Decode an internal cursor, expiry, stream sequence, and encoded state."""
    try:
        raw_cursor, raw_expiry, raw_sequence, payload = value.split(b":", 3)
        cursor = int(raw_cursor)
        expires_ms = int(raw_expiry)
        sequence = int(raw_sequence)
    except (ValueError, TypeError) as error:
        raise ValueError("stored spectator frame has an invalid header") from error
    if (
        raw_cursor.decode("ascii") != sequence_token(cursor)
        or len(raw_sequence) != _FRAME_SEQUENCE_WIDTH
        or raw_sequence != f"{sequence:0{_FRAME_SEQUENCE_WIDTH}d}".encode()
        or not 0 <= sequence <= 0xFFFF
        or expires_ms < 0
    ):
        raise ValueError("stored spectator frame has an invalid header")
    return OrderedFrame(cursor, sequence, payload, expires_ms)


@dataclass(frozen=True, slots=True)
class RealtimeKeys:
    """Build only version-one keys below an injected deployment prefix."""

    prefix: str

    def __post_init__(self) -> None:
        """Normalize and validate the deployment-owned prefix."""
        if not isinstance(self.prefix, str) or not self.prefix.strip(":"):
            raise ValueError("prefix must contain a non-colon character")
        object.__setattr__(self, "prefix", self.prefix.rstrip(":"))

    @property
    def base(self) -> str:
        """Return the immutable schema-version key prefix."""
        return f"{self.prefix}:v2"

    @property
    def session_prefix(self) -> str:
        """Return the prefix for dynamically resolved session keys."""
        return f"{self.base}:session:"

    def session(self, session_id: uuid.UUID) -> str:
        """Return a fenced session key."""
        return f"{self.session_prefix}{session_id}"

    def account_session(self, account_id: int) -> str:
        """Return the current realtime epoch for one account."""
        return f"{self.base}:account:{account_id}:session"

    def session_channels(self, session_id: uuid.UUID) -> str:
        """Return the channel IDs owned by one exact session lifecycle."""
        return f"{self.base}:session:{session_id}:channels"

    def session_revision(self, session_id: uuid.UUID) -> str:
        """Return a durable-lifetime monotonic revision counter for a session ID."""
        return f"{self.base}:session:{session_id}:revision"

    def presence(self, account_id: int) -> str:
        """Return an account presence key."""
        return f"{self.base}:presence:{account_id}"

    @property
    def presence_index(self) -> str:
        """Return the expiry-sorted online account index."""
        return f"{self.base}:presence:index"

    def preference(self, account_id: int) -> str:
        """Return one account's expiring realtime client preferences."""
        return f"{self.base}:preference:{account_id}"

    def channel_members(self, channel_id: int) -> str:
        """Return a channel's expiry-sorted account membership key."""
        return f"{self.base}:channel:{channel_id}:members"

    def channel_epochs(self, channel_id: int) -> str:
        """Return a channel's account-to-session fence key."""
        return f"{self.base}:channel:{channel_id}:epochs"

    def spectator_relation_revision(self, spectator_account_id: int) -> str:
        """Return a session-lifetime monotonic relation revision counter."""
        return f"{self.base}:spectator:viewer:{spectator_account_id}:revision"

    def spectator_relation(self, spectator_account_id: int) -> str:
        """Return a spectator-to-host relation key."""
        return f"{self.base}:spectator:viewer:{spectator_account_id}:host"

    def spectator_viewers(self, host_account_id: int) -> str:
        """Return a host-to-spectators inverse relation key."""
        return f"{self.base}:spectator:host:{host_account_id}:viewers"

    def spectator_frames(self, host_account_id: int) -> str:
        """Return a host's ordered spectator frame key."""
        return f"{self.base}:spectator:host:{host_account_id}:frames"

    def spectator_frame_bytes(self, host_account_id: int) -> str:
        """Return a host's spectator frame byte counter key."""
        return f"{self.base}:spectator:host:{host_account_id}:frame-bytes"

    def spectator_frame_sequence(self, host_account_id: int) -> str:
        """Return a host epoch's cursor and latest Stable sequence state."""
        return f"{self.base}:spectator:host:{host_account_id}:frame-state"
