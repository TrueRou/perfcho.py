"""Create the Redis connection used for bounded online state."""

from time import monotonic_ns

from redis.asyncio import Redis

from perfcho.infra.logging import duration_ms, log_event
from perfcho.infra.settings import Settings, settings


async def create_state_redis(config: Settings | None = None) -> Redis:
    """Create a binary Redis client and fail fast when unavailable."""
    cfg = config or settings
    started_ns = monotonic_ns()
    log_event(
        "INFO",
        "redis.state.connecting",
        endpoint_label="state",
    )
    redis_engine = Redis.from_url(
        cfg.redis_state_url,
        decode_responses=False,
        socket_timeout=cfg.redis_socket_timeout,
        socket_connect_timeout=cfg.redis_socket_timeout,
    )
    try:
        await redis_engine.ping()
    except Exception as e:
        await redis_engine.aclose()
        log_event(
            "ERROR",
            "redis.state.connection_failed",
            exception=e,
            endpoint_label="state",
            error_type=type(e).__name__,
            duration_ms=duration_ms(started_ns),
        )
        raise RuntimeError("Failed to connect to Redis state storage") from e
    log_event("INFO", "redis.state.connected", endpoint_label="state", duration_ms=duration_ms(started_ns))
    return redis_engine


async def create_cache_redis(config: Settings | None = None) -> Redis:
    """Create the isolated Redis client used by best-effort query caches."""
    cfg = config or settings
    redis_engine = Redis.from_url(
        cfg.redis_cache_url,
        decode_responses=False,
        socket_timeout=cfg.redis_cache_socket_timeout,
        socket_connect_timeout=cfg.redis_cache_socket_timeout,
    )
    try:
        await redis_engine.ping()
    except Exception:
        await redis_engine.aclose()
        raise RuntimeError("Failed to connect to Redis query cache") from None
    return redis_engine


async def create_bubble_redis(config: Settings | None = None) -> Redis:
    """Create the isolated client used for best-effort Pub/Sub bubbles."""
    cfg = config or settings
    redis_engine = Redis.from_url(
        cfg.redis_bubble_url or cfg.redis_state_url,
        decode_responses=False,
        max_connections=cfg.redis_bubble_max_connections,
        socket_timeout=cfg.redis_socket_timeout,
        socket_connect_timeout=cfg.redis_socket_timeout,
    )
    try:
        await redis_engine.ping()
    except Exception:
        await redis_engine.aclose()
        raise RuntimeError("Failed to connect to Redis bubble transport") from None
    return redis_engine
