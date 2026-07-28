from loguru import logger
from redis.asyncio import Redis

from perfcho.infra import logging
from perfcho.infra.settings import settings


def create_state_redis() -> Redis:
    return Redis.from_url(
        settings.redis_state_url,
        decode_responses=False,
        socket_timeout=settings.redis_socket_timeout,
        socket_connect_timeout=settings.redis_socket_timeout,
    )


async def check_redis(client: Redis) -> None:
    try:
        await client.ping()
        logger.patch(logging.source()).info("State Redis ready")
    except Exception:
        logger.patch(logging.source()).exception("Failed to connect to state Redis")
        raise
