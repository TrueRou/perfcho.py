"""Expose transactional outbox deliveries as Taskiq tasks."""

import uuid
from typing import Annotated, cast

from taskiq import Context, TaskiqDepends

from perfcho.infra.db.engine import DbSessionFactory
from perfcho.infra.outbox import process_delivery
from perfcho.infra.taskiq import broker


@broker.task(task_name="perfcho.outbox.dispatch")
async def dispatch_outbox_delivery(
    event_id: str,
    consumer: str,
    delivery_token: str,
    context: Annotated[Context, TaskiqDepends()],
) -> None:
    """Process one fenced delivery through its registered consumer."""
    session_factory = cast(DbSessionFactory, context.state.db_session_factory)
    await process_delivery(session_factory, uuid.UUID(event_id), consumer, uuid.UUID(delivery_token))
