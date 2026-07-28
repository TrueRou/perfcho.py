from redis.asyncio import Redis

from perfcho.infra.settings import settings


async def create_redis() -> Redis:
    redis_engine = Redis.from_url(
        settings.redis_state_url,
        decode_responses=False,
        socket_timeout=settings.redis_socket_timeout,
        socket_connect_timeout=settings.redis_socket_timeout,
    )
    try:
        await redis_engine.ping()
    except Exception as e:
        db_url = settings.redis_state_url
        raise RuntimeError(f"Failed to connect to redis: {db_url}") from e
    return redis_engine
