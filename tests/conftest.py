import asyncio
import os
from collections.abc import Iterator
from pathlib import Path

import pytest
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from perfcho.infra.database import MODEL_SCHEMAS

ROOT_DIR = Path(__file__).parents[1]
TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")

if TEST_DATABASE_URL:
    os.environ["DATABASE_URL"] = TEST_DATABASE_URL


async def _reset_database(database_url: str) -> None:
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            for schema in reversed(MODEL_SCHEMAS):
                await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
            await connection.execute(text("DROP TABLE IF EXISTS public.alembic_version"))
    finally:
        await engine.dispose()


@pytest.fixture()
def postgres_database_url() -> Iterator[str]:
    database_url = TEST_DATABASE_URL
    if database_url is None:
        pytest.skip("TEST_DATABASE_URL is not configured")
    assert database_url is not None

    asyncio.run(_reset_database(database_url))
    yield database_url
    asyncio.run(_reset_database(database_url))


@pytest.fixture()
def alembic_config(postgres_database_url: str) -> Config:
    config = Config(str(ROOT_DIR / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT_DIR / "alembic"))
    config.attributes["database_url"] = postgres_database_url
    return config
