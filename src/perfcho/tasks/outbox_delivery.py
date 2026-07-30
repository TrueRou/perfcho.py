"""Expose fenced outbox delivery processing as a Taskiq task."""

import uuid
from typing import Annotated, cast

from taskiq import Context, TaskiqDepends

from perfcho.infra.db.relays.outbox_delivery import OutboxDeliveryProcessor, OutboxDeliveryReference
from perfcho.infra.taskiq import broker


@broker.task(task_name="perfcho.outbox.dispatch")
async def dispatch_outbox_delivery(
    event_id: str,
    consumer: str,
    delivery_token: str,
    context: Annotated[Context, TaskiqDepends()],
) -> None:
    """Pass one broker message to the worker-composed delivery processor."""
    processor = cast(OutboxDeliveryProcessor, context.state.outbox_delivery_processor)
    await processor.execute(
        OutboxDeliveryReference(
            event_id=uuid.UUID(event_id),
            consumer=consumer,
            delivery_token=uuid.UUID(delivery_token),
        )
    )
