"""Provide deterministic transaction-scoped PostgreSQL advisory locks."""

import hashlib
from collections.abc import Iterable

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


def advisory_lock_key(namespace: str, *parts: object) -> int:
    """Derive one signed PostgreSQL bigint lock key from canonical text parts."""
    if not namespace:
        raise ValueError("namespace must not be empty")
    value = "\x1f".join((namespace, *(str(part) for part in parts))).encode()
    return int.from_bytes(hashlib.blake2b(value, digest_size=8).digest(), signed=True)


async def acquire_transaction_lock(session: AsyncSession, namespace: str, *parts: object) -> int:
    """Acquire one advisory lock until the current transaction ends."""
    lock_key = advisory_lock_key(namespace, *parts)
    await session.execute(text("SELECT pg_advisory_xact_lock(:lock_key)"), {"lock_key": lock_key})
    return lock_key


async def acquire_transaction_locks(session: AsyncSession, lock_keys: Iterable[int]) -> tuple[int, ...]:
    """Acquire unique keys in stable order to avoid application-level deadlocks."""
    ordered_keys = tuple(sorted(set(lock_keys)))
    for lock_key in ordered_keys:
        await session.execute(text("SELECT pg_advisory_xact_lock(:lock_key)"), {"lock_key": lock_key})
    return ordered_keys
