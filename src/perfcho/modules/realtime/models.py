"""Define immutable protocol-neutral realtime state values."""

import uuid
from dataclasses import dataclass
from datetime import datetime

MAX_REVISION = 2**63 - 1
MAX_SEQUENCE = 2**63 - 1
MAX_FRAME_SEQUENCE = 2**16 - 1


def _require_positive_id(name: str, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{name} must be a positive integer")


def _require_bounded_counter(name: str, value: int, maximum: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= maximum:
        raise ValueError(f"{name} must be between 0 and {maximum}")


def _require_aware(name: str, value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")


def _freeze_bytes(name: str, value: bytes) -> bytes:
    if not isinstance(value, bytes | bytearray | memoryview):
        raise TypeError(f"{name} must be bytes-like")
    return bytes(value)


@dataclass(frozen=True, slots=True)
class SessionFence:
    """Identify one exact realtime session epoch."""

    session_id: uuid.UUID
    revision: int

    def __post_init__(self) -> None:
        """Require a UUID and bounded revision."""
        if not isinstance(self.session_id, uuid.UUID):
            raise TypeError("session_id must be a UUID")
        _require_bounded_counter("revision", self.revision, MAX_REVISION)


@dataclass(frozen=True, slots=True)
class RealtimeSession:
    """Identify one fenced online lifecycle for an authenticated session."""

    session_id: uuid.UUID
    account_id: int
    revision: int
    expires_at: datetime

    def __post_init__(self) -> None:
        """Require a valid account, bounded fence revision, and TTL instant."""
        _require_positive_id("account_id", self.account_id)
        _require_bounded_counter("revision", self.revision, MAX_REVISION)
        _require_aware("expires_at", self.expires_at)

    @property
    def fence(self) -> SessionFence:
        """Return the immutable epoch token required by fenced operations."""
        return SessionFence(self.session_id, self.revision)


@dataclass(frozen=True, slots=True)
class PresenceSnapshot:
    """Store an opaque immutable presence projection for one account."""

    account_id: int
    revision: int
    payload: bytes
    expires_at: datetime
    session_id: uuid.UUID | None = None

    def __post_init__(self) -> None:
        """Validate the snapshot identity, revision, payload, and TTL instant."""
        _require_positive_id("account_id", self.account_id)
        _require_bounded_counter("revision", self.revision, MAX_REVISION)
        object.__setattr__(self, "payload", _freeze_bytes("payload", self.payload))
        _require_aware("expires_at", self.expires_at)
        if self.session_id is not None and not isinstance(self.session_id, uuid.UUID):
            raise TypeError("session_id must be a UUID or None")

    @property
    def fence(self) -> SessionFence:
        """Return the owning epoch, rejecting legacy unowned snapshots."""
        if self.session_id is None:
            raise ValueError("presence snapshot has no owning session")
        return SessionFence(self.session_id, self.revision)


@dataclass(frozen=True, slots=True)
class MailboxPacket:
    """Carry one ordered immutable packet through an account mailbox."""

    sequence: int
    payload: bytes

    def __post_init__(self) -> None:
        """Require a bounded sequence and defensively copy the payload."""
        _require_bounded_counter("sequence", self.sequence, MAX_SEQUENCE)
        object.__setattr__(self, "payload", _freeze_bytes("payload", self.payload))


@dataclass(frozen=True, slots=True)
class MailboxBatch:
    """Describe packets held by one exclusive poll lease."""

    lease_id: uuid.UUID
    packets: tuple[MailboxPacket, ...]
    expires_at: datetime

    def __post_init__(self) -> None:
        """Freeze the packet collection and require an aware lease expiry."""
        packets = tuple(self.packets)
        if any(not isinstance(packet, MailboxPacket) for packet in packets):
            raise TypeError("packets must contain only MailboxPacket values")
        object.__setattr__(self, "packets", packets)
        _require_aware("expires_at", self.expires_at)


@dataclass(frozen=True, slots=True)
class SpectatorRelation:
    """Describe one versioned, expiring spectator-to-host attachment."""

    host_account_id: int
    spectator_account_id: int
    relation_id: uuid.UUID
    revision: int
    host_fence: SessionFence
    spectator_fence: SessionFence
    expires_at: datetime

    def __post_init__(self) -> None:
        """Require distinct valid accounts, a bounded revision, and an aware expiry."""
        _require_positive_id("host_account_id", self.host_account_id)
        _require_positive_id("spectator_account_id", self.spectator_account_id)
        if self.host_account_id == self.spectator_account_id:
            raise ValueError("a spectator cannot attach to itself")
        if not isinstance(self.relation_id, uuid.UUID):
            raise TypeError("relation_id must be a UUID")
        _require_bounded_counter("revision", self.revision, MAX_REVISION)
        if not isinstance(self.host_fence, SessionFence):
            raise TypeError("host_fence must be a SessionFence")
        if not isinstance(self.spectator_fence, SessionFence):
            raise TypeError("spectator_fence must be a SessionFence")
        _require_aware("expires_at", self.expires_at)


@dataclass(frozen=True, slots=True)
class SpectatorFrame:
    """Carry one host-epoch frame behind a monotonic internal cursor."""

    cursor: int
    sequence: int
    payload: bytes

    def __post_init__(self) -> None:
        """Validate the cursor, Stable u16 sequence, and payload."""
        _require_bounded_counter("cursor", self.cursor, MAX_SEQUENCE)
        _require_bounded_counter("sequence", self.sequence, MAX_FRAME_SEQUENCE)
        object.__setattr__(self, "payload", _freeze_bytes("payload", self.payload))


@dataclass(frozen=True, slots=True)
class SpectatorFrameWindow:
    """Describe a bounded frame snapshot and its retained cursor range."""

    frames: tuple[SpectatorFrame, ...]
    oldest_cursor: int | None
    latest_cursor: int | None
    truncated: bool

    def __post_init__(self) -> None:
        """Freeze frames and validate snapshot cursor metadata."""
        frames = tuple(self.frames)
        if any(not isinstance(frame, SpectatorFrame) for frame in frames):
            raise TypeError("frames must contain only SpectatorFrame values")
        object.__setattr__(self, "frames", frames)
        for name, value in (("oldest_cursor", self.oldest_cursor), ("latest_cursor", self.latest_cursor)):
            if value is not None:
                _require_bounded_counter(name, value, MAX_SEQUENCE)
        if (self.oldest_cursor is None) != (self.latest_cursor is None):
            raise ValueError("frame window cursors must both be present or absent")
        if (
            self.oldest_cursor is not None
            and self.latest_cursor is not None
            and self.oldest_cursor > self.latest_cursor
        ):
            raise ValueError("oldest_cursor must not exceed latest_cursor")


@dataclass(frozen=True, slots=True)
class SpectatorAttachment:
    """Return an atomic relation and history-to-live handoff snapshot."""

    relation: SpectatorRelation
    history: SpectatorFrameWindow

    def __post_init__(self) -> None:
        """Require typed immutable attachment components."""
        if not isinstance(self.relation, SpectatorRelation):
            raise TypeError("relation must be a SpectatorRelation")
        if not isinstance(self.history, SpectatorFrameWindow):
            raise TypeError("history must be a SpectatorFrameWindow")


@dataclass(frozen=True, slots=True)
class SpectatorFramePublish:
    """Report one accepted frame and recipients atomically queued for live delivery."""

    frame: SpectatorFrame
    recipient_account_ids: tuple[int, ...]

    def __post_init__(self) -> None:
        """Freeze and validate the delivery result."""
        if not isinstance(self.frame, SpectatorFrame):
            raise TypeError("frame must be a SpectatorFrame")
        recipients = tuple(self.recipient_account_ids)
        for account_id in recipients:
            _require_positive_id("recipient_account_id", account_id)
        object.__setattr__(self, "recipient_account_ids", recipients)
