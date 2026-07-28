"""Create and scope asynchronous PostgreSQL engines and sessions."""

import json
from collections.abc import AsyncIterator

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.schema import CreateSchema

import perfcho.infra.db.models  # noqa: F401 - register all tables before create_all.
from perfcho.infra.db.base import MODEL_SCHEMAS, DbBase, DbSessionFactory
from perfcho.infra.settings import settings

_SCHEMA_INITIALIZATION_LOCK_ID = 0x7065726663686F


async def create_engine() -> AsyncEngine:
    """Create a pooled engine and ensure every mapped PostgreSQL table exists."""
    async_engine = create_async_engine(
        settings.database_url,
        pool_size=settings.database_pool_size,
        max_overflow=settings.database_max_overflow,
        pool_pre_ping=True,
        pool_recycle=3600,
        json_serializer=lambda value: json.dumps(value, ensure_ascii=False, default=str),
    )
    try:
        async with async_engine.begin() as connection:
            await connection.execute(
                text("SELECT pg_advisory_xact_lock(:lock_id)"),
                {"lock_id": _SCHEMA_INITIALIZATION_LOCK_ID},
            )
            for schema in MODEL_SCHEMAS:
                await connection.execute(CreateSchema(schema, if_not_exists=True))
            await connection.run_sync(DbBase.metadata.create_all)
    except Exception as e:
        await async_engine.dispose()
        db_url = async_engine.url.render_as_string(hide_password=True)
        raise RuntimeError(f"Failed to connect to database: {db_url}") from e
    return async_engine


def create_session_factory(db_engine: AsyncEngine) -> DbSessionFactory:
    """Bind request-scoped sessions to an existing engine."""
    return async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)


async def session_scope(session_factory: DbSessionFactory) -> AsyncIterator[AsyncSession]:
    """Yield one request-owned session without implicitly committing it."""
    async with session_factory() as session:
        yield session
