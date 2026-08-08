"""Small in-process cache used by isolated unit tests."""

from collections.abc import Awaitable, Callable

from perfcho.infra.cache.backend import InProcessSingleFlight


class MemoryCache:
    """CacheBackend implementation that preserves cache behavior without Redis."""

    def __init__(self, prefix: str = "perfcho:test-cache") -> None:
        """Initialize an in-memory cache namespace."""
        self._values: dict[str, bytes] = {}
        self._prefix = prefix
        self._single_flight = InProcessSingleFlight()

    def key(self, namespace: str, operation: str, identity: str) -> str:
        """Build a namespaced memory-cache key."""
        return f"{self._prefix}:v1:{namespace}:{operation}:{identity}"

    async def get(self, key: str) -> bytes | None:
        """Read a value from memory."""
        return self._values.get(key)

    async def set(self, key: str, value: bytes, *, ttl_seconds: int) -> None:
        """Store a value; unit tests do not simulate TTL passage."""
        del ttl_seconds
        self._values[key] = value

    async def delete(self, key: str) -> None:
        """Delete a value from memory."""
        self._values.pop(key, None)

    async def load_once(self, key: str, loader: Callable[[], Awaitable[bytes]], *, ttl_seconds: int) -> bytes:
        """Share one in-flight load for a key."""
        del ttl_seconds
        return await self._single_flight.run(key, loader)
