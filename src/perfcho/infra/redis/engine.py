"""Create the Redis connection used for bounded online state."""

from time import monotonic_ns

from redis.asyncio import Redis

from perfcho.infra.logging import duration_ms, log_event
from perfcho.infra.settings import settings


async def create_redis() -> Redis:
    """Create a binary Redis client and fail fast when unavailable."""
    started_ns = monotonic_ns()
    log_event(
        "INFO",
        "redis.state.connecting",
        endpoint_label="state",
    )
    redis_engine = Redis.from_url(
        settings.redis_state_url,
        decode_responses=False,
        socket_timeout=settings.redis_socket_timeout,
        socket_connect_timeout=settings.redis_socket_timeout,
    )
    try:
        await redis_engine.ping()
    except Exception as e:
        await redis_engine.aclose()
        log_event(
            "ERROR",
            "redis.state.connection_failed",
            endpoint_label="state",
            error_type=type(e).__name__,
            duration_ms=duration_ms(started_ns),
        )
        raise RuntimeError("Failed to connect to Redis state storage") from e
    log_event("INFO", "redis.state.connected", endpoint_label="state", duration_ms=duration_ms(started_ns))
    return redis_engine
