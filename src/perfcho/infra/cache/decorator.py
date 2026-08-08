"""Declarative cache-aside decorator."""

import functools
from collections.abc import Awaitable, Callable
from typing import Any, ParamSpec, TypeVar

from perfcho.infra.cache.backend import CacheBackend

# This helper intentionally supports Python 3.13-compatible typing syntax.
# ruff: noqa: UP047

P = ParamSpec("P")
T = TypeVar("T")


def cached(
    cache: CacheBackend,
    *,
    key_builder: Callable[..., str],
    encode: Callable[[T], bytes],
    decode: Callable[[bytes], T],
    ttl_seconds: int,
) -> Callable[[Callable[P, Awaitable[T]]], Callable[P, Awaitable[T]]]:
    """Add cache-aside behavior while preserving the wrapped async service method."""

    def decorate(function: Callable[P, Awaitable[T]]) -> Callable[P, Awaitable[T]]:
        @functools.wraps(function)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            key = key_builder(*args, **kwargs)
            raw = await cache.get(key)
            if raw is not None:
                try:
                    return decode(raw)
                except TypeError, ValueError, KeyError:
                    await cache.delete(key)

            async def load() -> bytes:
                value = await function(*args, **kwargs)
                encoded = encode(value)
                await cache.set(key, encoded, ttl_seconds=ttl_seconds)
                return encoded

            raw = await cache.load_once(key, load, ttl_seconds=ttl_seconds)
            return decode(raw)

        return wrapper

    return decorate


def json_codec(
    encode_value: Callable[[T], Any], decode_value: Callable[[Any], T]
) -> tuple[Callable[[T], bytes], Callable[[bytes], T]]:
    """Build a compact JSON codec for an explicitly mapped domain value."""
    import json

    def encode(value: T) -> bytes:
        return json.dumps({"v": 1, "value": encode_value(value)}, separators=(",", ":")).encode()

    def decode(raw: bytes) -> T:
        payload = json.loads(raw)
        if payload.get("v") != 1:
            raise ValueError("unknown cache value version")
        return decode_value(payload["value"])

    return encode, decode
