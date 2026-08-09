"""Redis implementation for non-authoritative query caches."""

from collections.abc import Awaitable, Callable

from redis.asyncio import Redis

from perfcho.infra.cache.backend import InProcessSingleFlight


class RedisCache:
    """Best-effort Redis cache. Every failure is intentionally handled by callers."""

    def __init__(self, redis: Redis, *, prefix: str) -> None:
        """Bind an isolated Redis client and cache prefix."""
        self._redis = redis
        self.prefix = prefix.rstrip(":")
        self._single_flight = InProcessSingleFlight()

    def key(self, namespace: str, operation: str, identity: str) -> str:
        """Build a versioned Redis cache key."""
        return f"{self.prefix}:v1:{namespace}:{operation}:{identity}"

    async def get(self, key: str) -> bytes | None:
        """Read a value, treating Redis failures as misses."""
        try:
            value = await self._redis.get(key)
            return value if isinstance(value, bytes) else None
        except Exception:
            return None

    async def set(self, key: str, value: bytes, *, ttl_seconds: int) -> None:
        """Write a value without allowing cache failures to affect requests."""
        try:
            await self._redis.set(key, value, ex=ttl_seconds)
        except Exception:
            return

    async def delete(self, key: str) -> None:
        """Delete a value without allowing cache failures to affect commands."""
        try:
            await self._redis.delete(key)
        except Exception:
            return

    async def increment(self, key: str) -> int:
        """Increment a Redis generation without making it a business fact."""
        try:
            return int(await self._redis.incr(key))
        except Exception:
            return 0

    async def load_once(self, key: str, loader: Callable[[], Awaitable[bytes]], *, ttl_seconds: int) -> bytes:
        """Share one in-flight load for a key."""
        return await self._single_flight.run(key, loader)

    async def aclose(self) -> None:
        """Close the cache-owned Redis client."""
        await self._redis.aclose()
