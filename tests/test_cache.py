import asyncio

import pytest

from perfcho.infra.cache import MemoryCache, NullCache, cached


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


@pytest.mark.asyncio
async def test_cached_decorator_supports_disabled_cache_and_conditional_cache() -> None:
    cache = NullCache()
    calls = 0

    @cached(
        cache,
        key_builder=lambda value: cache.key("test", "value", str(value)),
        encode=lambda value: str(value).encode(),
        decode=lambda value: int(value),
        ttl_seconds=30,
        enabled=lambda value: value > 0,
    )
    async def load(value: int) -> int:
        nonlocal calls
        calls += 1
        return value * 2

    assert await load(0) == 0
    assert await load(0) == 0
    assert await load(2) == 4
    assert await load(2) == 4
    assert calls == 4


@pytest.mark.asyncio
async def test_cached_decorator_can_cache_none() -> None:
    cache = MemoryCache()
    calls = 0

    @cached(
        cache,
        key_builder=lambda value: cache.key("test", "none", str(value)),
        encode=lambda value: b"null" if value is None else str(value).encode(),
        decode=lambda value: None if value == b"null" else int(value),
        ttl_seconds=30,
        cache_none=True,
    )
    async def load(value: int) -> int | None:
        nonlocal calls
        calls += 1
        return None if value == 1 else value

    assert await load(1) is None
    assert await load(1) is None
    assert calls == 1


@pytest.mark.asyncio
async def test_cached_decorator_discards_invalid_values() -> None:
    cache = MemoryCache()
    key = cache.key("test", "invalid", "1")
    await cache.set(key, b"invalid", ttl_seconds=30)
    calls = 0

    @cached(
        cache,
        key_builder=lambda value: cache.key("test", "invalid", str(value)),
        encode=lambda value: str(value).encode(),
        decode=lambda value: int(value),
        ttl_seconds=30,
    )
    async def load(value: int) -> int:
        nonlocal calls
        calls += 1
        return value * 3

    assert await load(1) == 3
    assert calls == 1
