from collections.abc import Awaitable, Callable

from perfcho.infra.cache.backend import InProcessSingleFlight


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

    async def load_once(self, key: str, loader: Callable[[], Awaitable[bytes]], *, ttl_seconds: int) -> bytes:
        del ttl_seconds
        return await self.single_flight.run(key, loader)
