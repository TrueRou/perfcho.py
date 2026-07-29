"""Define infrastructure ports consumed by application services."""

import uuid
from datetime import datetime
from types import TracebackType
from typing import Protocol, Self, runtime_checkable

from perfcho.modules.common.models import PendingEvent


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
