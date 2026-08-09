"""Declarative cache-aside decorator."""

import functools
import inspect
from collections.abc import Awaitable, Callable
from typing import Any, ParamSpec, TypeVar, cast

from perfcho.infra.cache.backend import CacheBackend

# This helper intentionally supports Python 3.13-compatible typing syntax.
# ruff: noqa: UP047

P = ParamSpec("P")
T = TypeVar("T")


CacheResolver = Callable[..., CacheBackend]
Enabled = Callable[..., bool]


def cached(
    cache: CacheBackend | CacheResolver | None = None,
    *,
    key_builder: Callable[..., str | Awaitable[str]],
    encode: Callable[[T], bytes],
    decode: Callable[[bytes], T],
    ttl_seconds: int | Callable[..., int],
    enabled: Enabled | None = None,
    cache_none: bool = False,
    return_loaded: bool = False,
    ttl_for_value: Callable[[T], int] | None = None,
) -> Callable[[Callable[P, Awaitable[T]]], Callable[P, Awaitable[T]]]:
    """Add cache-aside behavior while preserving the wrapped async service method."""

    def decorate(function: Callable[P, Awaitable[T]]) -> Callable[P, Awaitable[T]]:
        @functools.wraps(function)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            if enabled is not None and not enabled(*args, **kwargs):
                return await function(*args, **kwargs)
            backend = _resolve_cache(cache, args, kwargs)
            ttl = ttl_seconds(*args, **kwargs) if callable(ttl_seconds) else ttl_seconds
            key = key_builder(*args, **kwargs)
            if inspect.isawaitable(key):
                key = await key
            raw = await backend.get(key)
            if raw is not None:
                try:
                    return decode(raw)
                except TypeError, ValueError, KeyError:
                    await backend.delete(key)

            loaded: T | None = None

            async def load() -> bytes:
                nonlocal loaded
                value = await function(*args, **kwargs)
                loaded = value
                encoded = encode(value)
                if value is not None or cache_none:
                    value_ttl = ttl_for_value(value) if ttl_for_value is not None else ttl
                    await backend.set(key, encoded, ttl_seconds=value_ttl)
                return encoded

            raw = await backend.load_once(key, load, ttl_seconds=ttl)
            if return_loaded and loaded is not None:
                return loaded
            return decode(raw)

        return wrapper

    return decorate


def _resolve_cache(
    cache: CacheBackend | CacheResolver | None,
    args: tuple[object, ...],
    kwargs: dict[str, object],
) -> CacheBackend:
    if cache is None:
        if not args or not hasattr(args[0], "_cache"):
            raise TypeError("cached instance methods require self._cache")
        return args[0]._cache  # type: ignore[attr-defined]
    if callable(cache) and not hasattr(cache, "get"):
        return cache(*args, **kwargs)
    return cast(CacheBackend, cache)


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
