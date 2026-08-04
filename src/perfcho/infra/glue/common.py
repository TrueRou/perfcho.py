"""Shared infrastructure bindings used by process-role wiring roots."""

import uuid
from datetime import UTC, datetime
from typing import cast

from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra.db.repositories.authorization import SqlAlchemyAuthorizationRepository
from perfcho.infra.db.repositories.outbox import SqlAlchemyOutboxWriter


class SystemClock:
    """Return the current UTC instant."""

    def now(self) -> datetime:
        """Return one timezone-aware wall-clock instant."""
        return datetime.now(UTC)


class Uuid7Generator:
    """Generate time-ordered application UUIDs."""

    def new(self) -> uuid.UUID:
        """Return a new UUIDv7 value."""
        return uuid.uuid7()


def authorization_repository(session: object) -> SqlAlchemyAuthorizationRepository:
    """Bind the authorization repository to a caller-owned session."""
    return SqlAlchemyAuthorizationRepository(cast(AsyncSession, session))


def outbox_writer(session: object) -> SqlAlchemyOutboxWriter:
    """Bind the outbox writer to a caller-owned session."""
    return SqlAlchemyOutboxWriter(cast(AsyncSession, session))
