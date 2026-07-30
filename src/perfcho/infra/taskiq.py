"""Configure the Redis Stream Taskiq worker process lifecycle."""

import httpx
from taskiq import TaskiqEvents, TaskiqState
from taskiq_redis import RedisStreamBroker

from perfcho.infra.db import engine as infra_db
from perfcho.infra.s3 import S3ObjectStorage
from perfcho.infra.scoring import HttpPerformanceCalculator
from perfcho.infra.settings import settings

broker = RedisStreamBroker(
    url=settings.taskiq_broker_url,
    queue_name=settings.taskiq_queue_name,
    consumer_group_name=settings.taskiq_consumer_group,
    consumer_id="0-0",
    maxlen=settings.taskiq_stream_max_length,
    idle_timeout=settings.outbox_lease_seconds * 1000,
    unacknowledged_lock_timeout=float(settings.outbox_lease_seconds),
)


@broker.on_event(TaskiqEvents.WORKER_STARTUP)
async def worker_startup(state: TaskiqState) -> None:
    """Create worker-owned database resources after process startup."""
    state.db_engine = await infra_db.create_engine()
    state.db_session_factory = infra_db.create_session_factory(state.db_engine)
    state.performance_http_client = httpx.AsyncClient(timeout=settings.performance_http_timeout_seconds)
    state.performance_calculator = HttpPerformanceCalculator(
        state.performance_http_client,
        settings.performance_calculator_urls,
    )
    state.object_storage = S3ObjectStorage.from_settings(settings)


@broker.on_event(TaskiqEvents.WORKER_SHUTDOWN)
async def worker_shutdown(state: TaskiqState) -> None:
    """Close worker-owned network and database resources."""
    await state.performance_http_client.aclose()
    await state.db_engine.dispose()
