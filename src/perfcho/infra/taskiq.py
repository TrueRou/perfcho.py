from taskiq import TaskiqEvents, TaskiqState
from taskiq_redis import RedisStreamBroker

from perfcho.infra.db import engine as infra_db
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
    state.db_engine = await infra_db.create_engine()
    state.db_session_factory = infra_db.create_session_factory(state.db_engine)
