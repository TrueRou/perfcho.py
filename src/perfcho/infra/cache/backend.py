"""Cache ports and the small process-local single-flight primitive."""

import asyncio
from collections.abc import Awaitable, Callable
from typing import Protocol


class CacheBackend(Protocol):
    """Common operations required by every application cache implementation."""

    def key(self, namespace: str, operation: str, identity: str) -> str:
        """Build a namespaced cache key."""
        ...

    async def get(self, key: str) -> bytes | None:
        """Read a cache value."""
        ...

    async def set(self, key: str, value: bytes, *, ttl_seconds: int) -> None:
        """Write a cache value with a finite TTL."""
        ...

    async def delete(self, key: str) -> None:
        """Delete a cache value."""
        ...

    async def increment(self, key: str) -> int:
        """Increment a small invalidation generation and return its value."""
        ...

    async def load_once(self, key: str, loader: Callable[[], Awaitable[bytes]], *, ttl_seconds: int) -> bytes:
        """Load one key once per process when several requests miss together."""
        ...


class InProcessSingleFlight:
    """Deduplicate concurrent loaders without making Redis a dependency of reads."""

    def __init__(self) -> None:
        """Initialize the process-local task registry."""
        self._lock = asyncio.Lock()
        self._tasks: dict[str, asyncio.Task[bytes]] = {}

    async def run(self, key: str, loader: Callable[[], Awaitable[bytes]]) -> bytes:
        """Share one in-flight loader task for a cache key."""
        async with self._lock:
            task = self._tasks.get(key)
            if task is None:
                task = asyncio.ensure_future(loader())
                self._tasks[key] = task
        try:
            return await task
        finally:
            if task.done():
                async with self._lock:
                    if self._tasks.get(key) is task:
                        self._tasks.pop(key, None)
