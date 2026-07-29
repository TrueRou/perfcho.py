"""Define immutable context and event values shared by application modules."""

import uuid
from dataclasses import dataclass
from datetime import datetime

type JsonScalar = str | int | float | bool | None
type JsonValue = JsonScalar | list[JsonValue] | dict[str, JsonValue]


@dataclass(frozen=True, slots=True)
class Actor:
    """Identify an authenticated actor independently of its protocol."""

    account_id: int
    auth_session_id: uuid.UUID | None


@dataclass(frozen=True, slots=True)
class ClientContext:
    """Capture normalized, non-secret client evidence for an operation."""

    family: str
    version: str | None
    variant: str | None
    ip_address: str
    user_agent: str | None = None


@dataclass(frozen=True, slots=True)
class CommandMeta:
    """Carry tracing and idempotency data through an application command."""

    request_id: uuid.UUID
    idempotency_key: str
    request_digest: bytes
    actor: Actor | None
    client: ClientContext
    received_at: datetime

    def __post_init__(self) -> None:
        """Validate fields used as security and idempotency boundaries."""
        if not self.idempotency_key:
            raise ValueError("idempotency_key must not be empty")
        if len(self.request_digest) != 32:
            raise ValueError("request_digest must contain a SHA-256 digest")


@dataclass(frozen=True, slots=True)
class PendingEvent:
    """Describe one durable application event and its explicit consumers."""

    aggregate_type: str
    aggregate_id: str
    event_type: str
    schema_version: int
    payload: dict[str, JsonValue]
    consumers: tuple[str, ...]
    partition_key: str

    def __post_init__(self) -> None:
        """Reject unrouted or ambiguous durable events."""
        if self.schema_version < 1:
            raise ValueError("schema_version must be positive")
        if not self.consumers or len(self.consumers) != len(set(self.consumers)):
            raise ValueError("consumers must be non-empty and unique")
        if not self.partition_key:
            raise ValueError("partition_key must not be empty")


@dataclass(frozen=True, slots=True)
class StoredObject:
    """Describe one immutable object-store payload without provider details."""

    storage_key: str
    size_bytes: int
    media_type: str
    sha256: bytes | None
    etag: str | None = None

    def __post_init__(self) -> None:
        """Reject invalid object metadata returned by an infrastructure adapter."""
        if not self.storage_key or self.size_bytes < 0 or not self.media_type:
            raise ValueError("stored object metadata is invalid")
        if self.sha256 is not None and len(self.sha256) != 32:
            raise ValueError("stored object sha256 must contain 32 bytes")
