import json
from collections.abc import AsyncIterator

from loguru import logger
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from perfcho.infra import logging
from perfcho.infra.settings import settings

type DbSessionFactory = async_sessionmaker[AsyncSession]


def create_database_engine() -> AsyncEngine:
    return create_async_engine(
        settings.database_url,
        pool_size=settings.database_pool_size,
        max_overflow=settings.database_max_overflow,
        pool_pre_ping=True,
        pool_recycle=3600,
        json_serializer=lambda value: json.dumps(value, ensure_ascii=False, default=str),
    )


def create_session_factory(db_engine: AsyncEngine) -> DbSessionFactory:
    return async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)


async def session_scope(session_factory: DbSessionFactory) -> AsyncIterator[AsyncSession]:
    async with session_factory() as session:
        yield session


async def check_database(db_engine: AsyncEngine) -> None:
    try:
        async with db_engine.connect() as connection:
            await connection.execute(text("SELECT 1"))
        logger.patch(logging.source()).info("Database ready: {}", db_engine.url.render_as_string(hide_password=True))
    except Exception:
        logger.patch(logging.source()).exception(
            "Failed to connect to database: {}", db_engine.url.render_as_string(hide_password=True)
        )
        raise
