from collections.abc import Awaitable, Callable

from perfcho.infra.cache.backend import InProcessSingleFlight
from perfcho.infra.cache.redis import RedisCache


class MemoryCache:
    def __init__(self) -> None:
        self.values: dict[str, bytes] = {}
        self.single_flight = InProcessSingleFlight()

    def key(self, namespace: str, operation: str, identity: str) -> str:
        return f"test:{namespace}:{operation}:{identity}"

    async def get(self, key: str) -> bytes | None:
        return self.values.get(key)

    async def set(self, key: str, value: bytes, *, ttl_seconds: int) -> None:
        del ttl_seconds
        self.values[key] = value

    async def delete(self, key: str) -> None:
        self.values.pop(key, None)

    async def increment(self, key: str) -> int:
        current = int(self.values.get(key, b"0")) + 1
        self.values[key] = str(current).encode()
        return current

    async def load_once(self, key: str, loader: Callable[[], Awaitable[bytes]], *, ttl_seconds: int) -> bytes:
        del ttl_seconds
        return await self.single_flight.run(key, loader)


class RedisCacheFake(RedisCache):
    """In-memory RedisCache replacement for composition tests."""

    def __init__(self) -> None:
        self.prefix = "test"
        self._values: dict[str, bytes] = {}
        self._single_flight = InProcessSingleFlight()

    async def get(self, key: str) -> bytes | None:
        return self._values.get(key)

    async def set(self, key: str, value: bytes, *, ttl_seconds: int) -> None:
        del ttl_seconds
        self._values[key] = value

    async def delete(self, key: str) -> None:
        self._values.pop(key, None)

    async def increment(self, key: str) -> int:
        current = int(self._values.get(key, b"0")) + 1
        self._values[key] = str(current).encode()
        return current
