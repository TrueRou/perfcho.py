"""Encode Redis realtime values and construct its versioned state keys."""

import struct
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from perfcho.modules.realtime import MAX_SEQUENCE, PresenceSnapshot, SpectatorFrame

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_PRESENCE_HEADER = struct.Struct(">QQQ16s")
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


def encode_presence(snapshot: PresenceSnapshot, *, session_id: uuid.UUID | None = None) -> bytes:
    """Pack presence metadata ahead of an arbitrary binary payload."""
    expires_ms = datetime_to_milliseconds(snapshot.expires_at)
    owner = session_id or snapshot.session_id
    if owner is None:
        raise ValueError("presence session_id is required for Redis storage")
    try:
        header = _PRESENCE_HEADER.pack(snapshot.account_id, snapshot.revision, expires_ms, owner.bytes)
    except struct.error as error:
        raise ValueError("presence metadata exceeds the Redis encoding range") from error
    return header + snapshot.payload


def decode_presence(value: bytes) -> PresenceSnapshot:
    """Decode a packed presence snapshot without interpreting its payload."""
    if len(value) < _PRESENCE_HEADER.size:
        raise ValueError("stored presence is truncated")
    account_id, revision, expires_ms, session_bytes = _PRESENCE_HEADER.unpack_from(value)
    return PresenceSnapshot(
        account_id=account_id,
        revision=revision,
        payload=value[_PRESENCE_HEADER.size :],
        expires_at=datetime_from_milliseconds(expires_ms),
        session_id=uuid.UUID(bytes=session_bytes),
    )


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
class OrderedPayload:
    """Represent one decoded expiring packet or spectator frame."""

    sequence: int
    expires_ms: int
    payload: bytes


def decode_ordered_payload(value: bytes) -> OrderedPayload:
    """Decode a sequence and expiry header while preserving binary payload bytes."""
    try:
        raw_sequence, raw_expiry, payload = value.split(b":", 2)
        sequence = int(raw_sequence)
        expires_ms = int(raw_expiry)
    except (ValueError, TypeError) as error:
        raise ValueError("stored ordered payload has an invalid header") from error
    if raw_sequence.decode("ascii") != sequence_token(sequence) or expires_ms < 0:
        raise ValueError("stored ordered payload has an invalid header")
    return OrderedPayload(sequence=sequence, expires_ms=expires_ms, payload=payload)


@dataclass(frozen=True, slots=True)
class OrderedFrame:
    """Represent one decoded cursor-ordered spectator frame."""

    frame: SpectatorFrame
    expires_ms: int


def decode_ordered_frame(value: bytes) -> OrderedFrame:
    """Decode an internal cursor, expiry, Stable u16 sequence, and payload."""
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
    return OrderedFrame(SpectatorFrame(cursor, sequence, payload), expires_ms)


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

    def mailbox_packets(self, account_id: int) -> str:
        """Return an account's ordered mailbox packet key."""
        return f"{self.base}:mailbox:{account_id}:packets"

    def mailbox_bytes(self, account_id: int) -> str:
        """Return an account's mailbox byte counter key."""
        return f"{self.base}:mailbox:{account_id}:bytes"

    def mailbox_sequence(self, account_id: int) -> str:
        """Return an account's monotonic mailbox sequence key."""
        return f"{self.base}:mailbox:{account_id}:sequence"

    def mailbox_lease(self, account_id: int) -> str:
        """Return an account's exclusive mailbox lease key."""
        return f"{self.base}:mailbox:{account_id}:lease"

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
