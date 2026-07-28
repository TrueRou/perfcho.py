import asyncio
from collections.abc import Mapping

import pytest
from alembic.config import Config
from sqlalchemy import inspect, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine

from alembic import command
from perfcho.infra.database import MODEL_SCHEMAS


async def _database_snapshot(database_url: str) -> tuple[set[str], dict[str, set[str]], Mapping[str, int]]:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            schemas, tables = await connection.run_sync(
                lambda sync_connection: (
                    set(inspect(sync_connection).get_schema_names()),
                    {schema: set(inspect(sync_connection).get_table_names(schema=schema)) for schema in MODEL_SCHEMAS},
                )
            )
            seeds = {
                "accounts": await connection.scalar(text("SELECT count(*) FROM core.accounts")),
                "sources": await connection.scalar(text("SELECT count(*) FROM content.sources")),
                "scoreboards": await connection.scalar(text("SELECT count(*) FROM scoring.scoreboards")),
                "scopes": await connection.scalar(text("SELECT count(*) FROM iam.scopes")),
            }
            return schemas, tables, {key: int(value or 0) for key, value in seeds.items()}
    finally:
        await engine.dispose()


async def _assert_database_constraints(database_url: str) -> None:
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO core.accounts (id, type, status, registered_at, auth_version) VALUES "
                    "(2, 'user', 'active', CURRENT_TIMESTAMP, 1), "
                    "(3, 'user', 'active', CURRENT_TIMESTAMP, 1)"
                )
            )
            await connection.execute(
                text(
                    "INSERT INTO core.account_names (account_id, display_name, name_key, started_at) "
                    "VALUES (2, 'Alice', 'alice', CURRENT_TIMESTAMP)"
                )
            )

        with pytest.raises(IntegrityError):
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        "INSERT INTO core.account_names (account_id, display_name, name_key, started_at) "
                        "VALUES (2, 'Alice2', 'alice2', CURRENT_TIMESTAMP)"
                    )
                )

        with pytest.raises(IntegrityError):
            async with engine.begin() as connection:
                await connection.execute(
                    text("INSERT INTO social.follows (actor_account_id, target_account_id) VALUES (2, 2)")
                )

        async with engine.begin() as connection:
            channel_id = await connection.scalar(
                text(
                    "INSERT INTO community.channels "
                    "(kind, message_length_limit, auto_join, created_at, updated_at) "
                    "VALUES ('private', 2000, false, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) RETURNING id"
                )
            )
            assert channel_id is not None

        with pytest.raises(IntegrityError):
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        "INSERT INTO community.direct_conversations (channel_id, low_account_id, high_account_id) "
                        "VALUES (:channel_id, 3, 2)"
                    ),
                    {"channel_id": channel_id},
                )
    finally:
        await engine.dispose()


@pytest.mark.postgres
def test_migration_lifecycle(alembic_config: Config, postgres_database_url: str) -> None:
    command.upgrade(alembic_config, "head")
    schemas, tables, seeds = asyncio.run(_database_snapshot(postgres_database_url))

    assert set(MODEL_SCHEMAS) <= schemas
    assert sum(len(schema_tables) for schema_tables in tables.values()) == 129
    assert seeds == {"accounts": 1, "sources": 1, "scoreboards": 8, "scopes": 8}
    command.check(alembic_config)

    command.downgrade(alembic_config, "base")
    schemas_after_downgrade = asyncio.run(_database_schemas(postgres_database_url))
    assert set(MODEL_SCHEMAS).isdisjoint(schemas_after_downgrade)

    command.upgrade(alembic_config, "head")
    _, tables_after_reupgrade, _ = asyncio.run(_database_snapshot(postgres_database_url))
    assert sum(len(schema_tables) for schema_tables in tables_after_reupgrade.values()) == 129


async def _database_schemas(database_url: str) -> set[str]:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            schemas = await connection.run_sync(
                lambda sync_connection: set(inspect(sync_connection).get_schema_names())
            )
            return schemas
    finally:
        await engine.dispose()


@pytest.mark.postgres
def test_critical_database_constraints(alembic_config: Config, postgres_database_url: str) -> None:
    command.upgrade(alembic_config, "head")
    asyncio.run(_assert_database_constraints(postgres_database_url))
