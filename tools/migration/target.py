"""Prepare and exclusively coordinate the perfcho PostgreSQL target."""

from __future__ import annotations

import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import orjson
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.schema import CreateSchema

import perfcho.infra.db.models  # noqa: F401 - register all mapped tables.
from perfcho.infra.db.base import MODEL_SCHEMAS, DbBase
from perfcho.infra.db.bootstrap import bootstrap_database
from perfcho.infra.logging import duration_ms, log_event

_TARGET_LOCK_KEY = "perfcho:bancho-migration"

_LEGACY_CREDENTIAL_DDL = (
    "ALTER TABLE iam.password_credentials DROP CONSTRAINT IF EXISTS ck_password_credentials_argon2id_only",
    "ALTER TABLE iam.password_credentials DROP CONSTRAINT IF EXISTS ck_password_credentials_positive_pepper_version",
    "ALTER TABLE iam.password_credentials ALTER COLUMN pepper_version DROP NOT NULL",
    """
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conrelid = 'iam.password_credentials'::regclass
              AND conname = 'ck_password_credentials_algorithm_pepper_consistency'
        ) THEN
            ALTER TABLE iam.password_credentials
            ADD CONSTRAINT ck_password_credentials_algorithm_pepper_consistency CHECK (
                (algorithm = 'argon2id' AND pepper_version IS NOT NULL AND pepper_version > 0)
                OR (algorithm = 'bcrypt_md5' AND pepper_version IS NULL)
            );
        END IF;
    END
    $$
    """,
)


def create_target_engine(database_url: str) -> AsyncEngine:
    """Create a migration-owned target engine without using process-global settings."""
    return create_async_engine(
        database_url,
        pool_size=5,
        max_overflow=0,
        pool_pre_ping=True,
        hide_parameters=True,
        json_serializer=lambda value: orjson.dumps(value, default=str).decode(),
    )


def create_target_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Create explicit transaction sessions for migration phases."""
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def prepare_target(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Create missing perfcho objects, install credential compatibility, and bootstrap catalogs."""
    async with engine.begin() as connection:
        await connection.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": 0x7065726663686F})
        for schema in MODEL_SCHEMAS:
            await connection.execute(CreateSchema(schema, if_not_exists=True))
        await connection.run_sync(DbBase.metadata.create_all)
        for statement in _LEGACY_CREDENTIAL_DDL:
            await connection.execute(text(statement))
    session_factory = create_target_session_factory(engine)
    await bootstrap_database(session_factory)
    return session_factory


@asynccontextmanager
async def target_migration_lock(
    engine: AsyncEngine,
    migration_id: str,
    *,
    invocation_id: str | None = None,
) -> AsyncIterator[None]:
    """Hold a session-level advisory lock for the complete migration run."""
    acquire_started_ns = time.monotonic_ns()
    log_event(
        "INFO",
        "migration.target_lock.acquire_started",
        invocation_id=invocation_id,
        migration_id=migration_id,
    )
    connection = None
    acquired = False
    primary_error: BaseException | None = None
    try:
        connection = await engine.connect()
        lock_result = await connection.scalar(
            text("SELECT pg_try_advisory_lock(hashtext(:key))"),
            {"key": _TARGET_LOCK_KEY},
        )
        if lock_result is not True:
            raise RuntimeError("another process owns the target migration lock")
        acquired = True
        log_event(
            "INFO",
            "migration.target_lock.acquired",
            invocation_id=invocation_id,
            migration_id=migration_id,
            duration_ms=duration_ms(acquire_started_ns),
        )
        yield
    except BaseException as error:
        primary_error = error
        if not acquired:
            log_event(
                "WARNING" if isinstance(error, KeyboardInterrupt) else "ERROR",
                "migration.target_lock.acquire_failed",
                exception=error,
                invocation_id=invocation_id,
                migration_id=migration_id,
                error_type=type(error).__name__,
                duration_ms=duration_ms(acquire_started_ns),
            )
        raise
    finally:
        if connection is not None:
            release_started_ns = time.monotonic_ns()
            cleanup_error: BaseException | None = None
            try:
                if acquired:
                    await connection.execute(
                        text("SELECT pg_advisory_unlock(hashtext(:key))"),
                        {"key": _TARGET_LOCK_KEY},
                    )
            except BaseException as error:
                cleanup_error = error
                log_event(
                    "WARNING" if isinstance(error, KeyboardInterrupt) else "ERROR",
                    "migration.target_lock.release_failed",
                    exception=error,
                    invocation_id=invocation_id,
                    migration_id=migration_id,
                    error_type=type(error).__name__,
                    duration_ms=duration_ms(release_started_ns),
                )
            finally:
                try:
                    await connection.close()
                except BaseException as error:
                    cleanup_error = cleanup_error or error
                    log_event(
                        "WARNING" if isinstance(error, KeyboardInterrupt) else "ERROR",
                        "migration.target_lock.release_failed",
                        exception=error,
                        invocation_id=invocation_id,
                        migration_id=migration_id,
                        phase="connection_close",
                        error_type=type(error).__name__,
                        duration_ms=duration_ms(release_started_ns),
                    )
            if acquired and cleanup_error is None:
                log_event(
                    "INFO",
                    "migration.target_lock.released",
                    invocation_id=invocation_id,
                    migration_id=migration_id,
                    duration_ms=duration_ms(release_started_ns),
                )
            if primary_error is None and cleanup_error is not None:
                raise cleanup_error
