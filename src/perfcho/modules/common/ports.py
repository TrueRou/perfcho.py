"""Define infrastructure ports consumed by application services."""

import uuid
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager
from datetime import datetime
from types import TracebackType
from typing import Protocol, Self, runtime_checkable

from perfcho.modules.common.models import PendingEvent, StoredObject


@runtime_checkable
class Clock(Protocol):
    """Provide an injectable authoritative application clock."""

    def now(self) -> datetime:
        """Return the current timezone-aware instant."""
        ...


@runtime_checkable
class IdGenerator(Protocol):
    """Generate application-owned UUID identifiers."""

    def new(self) -> uuid.UUID:
        """Return a new time-ordered identifier."""
        ...


@runtime_checkable
class UnitOfWork(Protocol):
    """Own one application transaction with explicit commit semantics."""

    async def __aenter__(self) -> Self:
        """Open the transaction resources."""
        ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Roll back uncommitted work and release resources."""
        ...

    async def commit(self) -> None:
        """Commit the owned transaction exactly once."""
        ...


@runtime_checkable
class UnitOfWorkFactory(Protocol):
    """Create request- or task-owned units of work."""

    def __call__(self) -> UnitOfWork:
        """Return a fresh unit of work."""
        ...


@runtime_checkable
class OutboxWriter(Protocol):
    """Append explicit application events inside the current transaction."""

    async def append(self, event: PendingEvent) -> uuid.UUID:
        """Persist an event and all requested deliveries."""
        ...


class ObjectStream(Protocol):
    """Expose one bounded-chunk object body while its provider resource is open."""

    @property
    def metadata(self) -> StoredObject:
        """Return immutable metadata for the open object."""
        ...

    def iter_chunks(self) -> AsyncIterator[bytes]:
        """Yield non-empty payload chunks in order."""
        ...


class ObjectStorage(Protocol):
    """Store and stream immutable binary assets through provider-neutral operations."""

    async def put(
        self,
        storage_key: str,
        content: bytes,
        *,
        media_type: str,
        expected_sha256: bytes | None = None,
    ) -> StoredObject:
        """Write one complete object and return verified metadata."""
        ...

    def open(self, storage_key: str) -> AbstractAsyncContextManager[ObjectStream]:
        """Open an object stream whose lifetime is owned by the context manager."""
        ...

    async def delete(self, storage_key: str) -> None:
        """Idempotently remove one object."""
        ...
