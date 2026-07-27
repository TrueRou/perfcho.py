import json
from collections.abc import AsyncIterator

from loguru import logger
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from perfcho.infra import logging
from perfcho.infra.settings import settings


def _create_engine() -> AsyncEngine:
    return create_async_engine(
        settings.database_url,
        pool_size=settings.database_pool_size,
        max_overflow=settings.database_max_overflow,
        pool_pre_ping=True,
        pool_recycle=3600,
        json_serializer=lambda value: json.dumps(value, ensure_ascii=False, default=str),
    )


db_engine: AsyncEngine = _create_engine()
db_session = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)


async def get_db() -> AsyncIterator[AsyncSession]:
    async with db_session() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def check_database() -> None:
    try:
        async with db_engine.connect() as connection:
            await connection.execute(text("SELECT 1"))
        logger.patch(logging.source()).info("Database ready: {}", settings.database_url)
    except Exception:
        logger.patch(logging.source()).exception("Failed to connect to database: {}", settings.database_url)
        raise
