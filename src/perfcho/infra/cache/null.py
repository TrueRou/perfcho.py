"""Disabled cache backend used to keep caching optional at composition boundaries."""

from collections.abc import Awaitable, Callable


class NullCache:
    """A no-op cache with the same behavior as a permanently empty cache."""

    def key(self, namespace: str, operation: str, identity: str) -> str:
        """Build a deterministic key without retaining it."""
        return f"null:{namespace}:{operation}:{identity}"

    async def get(self, key: str) -> bytes | None:
        """Report a cache miss."""
        del key
        return None

    async def set(self, key: str, value: bytes, *, ttl_seconds: int) -> None:
        """Discard a value."""
        del key, value, ttl_seconds

    async def delete(self, key: str) -> None:
        """Discard an invalidation request."""
        del key

    async def increment(self, key: str) -> int:
        """Return the initial generation."""
        del key
        return 0

    async def load_once(self, key: str, loader: Callable[[], Awaitable[bytes]], *, ttl_seconds: int) -> bytes:
        """Run the loader directly without deduplicating or storing it."""
        del key, ttl_seconds
        return await loader()
