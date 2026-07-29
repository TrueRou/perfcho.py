"""Implement explicit SQLAlchemy units of work for application commands."""

from types import TracebackType

from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra.db.base import DbSessionFactory


class SqlAlchemyUnitOfWork:
    """Own one AsyncSession and never commit implicitly."""

    def __init__(self, session_factory: DbSessionFactory) -> None:
        """Store the factory without opening a connection eagerly."""
        self._session_factory = session_factory
        self._session: AsyncSession | None = None
        self._committed = False

    @property
    def session(self) -> AsyncSession:
        """Return the active session or reject use outside the context."""
        if self._session is None:
            raise RuntimeError("unit of work is not active")
        return self._session

    async def __aenter__(self) -> SqlAlchemyUnitOfWork:
        """Create a request-owned session."""
        if self._session is not None:
            raise RuntimeError("unit of work is already active")
        self._session = self._session_factory()
        self._committed = False
        return self

    async def commit(self) -> None:
        """Commit the transaction explicitly."""
        if self._committed:
            raise RuntimeError("unit of work has already committed")
        await self.session.commit()
        self._committed = True

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Roll back failed or uncommitted work, then close the session."""
        session = self.session
        try:
            if exc_type is not None or not self._committed:
                await session.rollback()
        finally:
            await session.close()
            self._session = None


class SqlAlchemyUnitOfWorkFactory:
    """Create a fresh SQLAlchemy unit of work for each operation."""

    def __init__(self, session_factory: DbSessionFactory) -> None:
        """Store the shared process-owned session factory."""
        self._session_factory = session_factory

    def __call__(self) -> SqlAlchemyUnitOfWork:
        """Return a new inactive unit of work."""
        return SqlAlchemyUnitOfWork(self._session_factory)
