"""Configure the Redis Stream Taskiq broker transport."""

from taskiq_redis import RedisStreamBroker

from perfcho.infra.settings import settings

_redelivery_timeout_seconds = max(
    settings.outbox_delivery_lease_seconds,
    settings.performance_calculation_lease_seconds,
)

broker = RedisStreamBroker(
    url=settings.taskiq_broker_url,
    queue_name=settings.taskiq_queue_name,
    consumer_group_name=settings.taskiq_consumer_group,
    consumer_id="0-0",
    maxlen=settings.taskiq_stream_max_length,
    idle_timeout=_redelivery_timeout_seconds * 1000,
    unacknowledged_lock_timeout=float(_redelivery_timeout_seconds),
)
