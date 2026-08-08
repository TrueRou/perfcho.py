"""Configure the Redis Stream Taskiq broker transport."""

from typing import TYPE_CHECKING

from taskiq import TaskiqMessage, TaskiqMiddleware
from taskiq_redis import RedisStreamBroker

from perfcho.infra import logging
from perfcho.infra.settings import settings

if TYPE_CHECKING:
    from taskiq import TaskiqResult

_redelivery_timeout_seconds = settings.outbox_delivery_lease_seconds

broker = RedisStreamBroker(
    url=settings.taskiq_broker_url,
    queue_name=settings.taskiq_queue_name,
    consumer_group_name=settings.taskiq_consumer_group,
    consumer_id="0-0",
    maxlen=settings.taskiq_stream_max_length,
    idle_timeout=_redelivery_timeout_seconds * 1000,
    unacknowledged_lock_timeout=float(_redelivery_timeout_seconds),
)


class RelayTaskLoggingMiddleware(TaskiqMiddleware):
    """Attach the concrete Taskiq task name to worker logs during execution."""

    def pre_execute(self, message: TaskiqMessage) -> TaskiqMessage:
        """Bind the incoming task name before Taskiq emits execution logs."""
        _ = logging.set_relay_task(message.task_name)
        return message

    def post_execute(self, message: TaskiqMessage, result: TaskiqResult[object]) -> None:
        """Clear task context after Taskiq and application execution completes."""
        del message, result
        logging.clear_relay_task()


broker.add_middlewares(RelayTaskLoggingMiddleware())
