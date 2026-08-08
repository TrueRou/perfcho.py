import asyncio

import pytest

from perfcho.infra.cache import MemoryCache, cached


@pytest.mark.asyncio
async def test_cached_decorator_deduplicates_concurrent_misses() -> None:
    cache = MemoryCache()
    calls = 0

    @cached(
        cache,
        key_builder=lambda value: cache.key("test", "value", str(value)),
        encode=lambda value: str(value).encode(),
        decode=lambda value: int(value),
        ttl_seconds=30,
    )
    async def load(value: int) -> int:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0)
        return value * 2

    assert await asyncio.gather(load(4), load(4), load(4)) == [8, 8, 8]
    assert await load(4) == 8
    assert calls == 1
