import json
from collections.abc import AsyncIterator

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from perfcho.infra.db.base import DbSessionFactory
from perfcho.infra.settings import settings


async def create_engine() -> AsyncEngine:
    async_engine = create_async_engine(
        settings.database_url,
        pool_size=settings.database_pool_size,
        max_overflow=settings.database_max_overflow,
        pool_pre_ping=True,
        pool_recycle=3600,
        json_serializer=lambda value: json.dumps(value, ensure_ascii=False, default=str),
    )
    try:
        async with async_engine.connect() as connection:
            await connection.execute(text("SELECT 1"))
    except Exception as e:
        db_url = async_engine.url.render_as_string(hide_password=True)
        raise RuntimeError(f"Failed to connect to database: {db_url}") from e
    return async_engine


def create_session_factory(db_engine: AsyncEngine) -> DbSessionFactory:
    return async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)


async def session_scope(session_factory: DbSessionFactory) -> AsyncIterator[AsyncSession]:
    async with session_factory() as session:
        yield session
