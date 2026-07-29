"""Define immutable protocol-neutral realtime state values."""

import uuid
from dataclasses import dataclass
from datetime import datetime

MAX_REVISION = 2**63 - 1
MAX_SEQUENCE = 2**63 - 1


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


@dataclass(frozen=True, slots=True)
class PresenceSnapshot:
    """Store an opaque immutable presence projection for one account."""

    account_id: int
    revision: int
    payload: bytes
    expires_at: datetime

    def __post_init__(self) -> None:
        """Validate the snapshot identity, revision, payload, and TTL instant."""
        _require_positive_id("account_id", self.account_id)
        _require_bounded_counter("revision", self.revision, MAX_REVISION)
        object.__setattr__(self, "payload", _freeze_bytes("payload", self.payload))
        _require_aware("expires_at", self.expires_at)


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
    revision: int
    expires_at: datetime

    def __post_init__(self) -> None:
        """Require distinct valid accounts, a bounded revision, and an aware expiry."""
        _require_positive_id("host_account_id", self.host_account_id)
        _require_positive_id("spectator_account_id", self.spectator_account_id)
        if self.host_account_id == self.spectator_account_id:
            raise ValueError("a spectator cannot attach to itself")
        _require_bounded_counter("revision", self.revision, MAX_REVISION)
        _require_aware("expires_at", self.expires_at)
