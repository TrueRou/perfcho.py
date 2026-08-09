import asyncio
import os
from collections.abc import Iterator

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")

if TEST_DATABASE_URL:
    os.environ["DATABASE_URL"] = TEST_DATABASE_URL


async def _reset_database(database_url: str) -> None:
    from perfcho.infra.db import MODEL_SCHEMAS

    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            for schema in reversed(MODEL_SCHEMAS):
                await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
    finally:
        await engine.dispose()


def _quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


async def _ensure_database(database_url: str) -> None:
    target_url = make_url(database_url)
    database_name = target_url.database
    if database_name is None:
        raise ValueError("TEST_DATABASE_URL must include a database name")

    engine = create_async_engine(target_url.set(database="postgres"), isolation_level="AUTOCOMMIT")
    try:
        async with engine.connect() as connection:
            exists = await connection.scalar(
                text("SELECT 1 FROM pg_database WHERE datname = :database_name"),
                {"database_name": database_name},
            )
            if exists is None:
                await connection.execute(text(f"CREATE DATABASE {_quote_identifier(database_name)}"))
    finally:
        await engine.dispose()


@pytest.fixture(scope="session")
def test_database_url() -> Iterator[str]:
    database_url = TEST_DATABASE_URL
    if database_url is None:
        pytest.skip("TEST_DATABASE_URL is not configured")

    asyncio.run(_ensure_database(database_url))
    yield database_url


@pytest.fixture()
def postgres_database_url(test_database_url: str) -> Iterator[str]:
    database_url = test_database_url
    asyncio.run(_reset_database(database_url))
    yield database_url
    asyncio.run(_reset_database(database_url))
