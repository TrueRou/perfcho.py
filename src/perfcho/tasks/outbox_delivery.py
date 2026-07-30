"""Expose fenced outbox delivery processing as a Taskiq task."""

import uuid
from typing import Annotated, cast

from taskiq import Context, TaskiqDepends

from perfcho.infra.db.relays.outbox_delivery import (
    OutboxDeliveryProcessor,
    OutboxDeliveryReference,
    is_handled_outbox_failure,
)
from perfcho.infra.logging import log_event
from perfcho.infra.taskiq import broker


@broker.task(task_name="perfcho.outbox.dispatch")
async def dispatch_outbox_delivery(
    event_id: str,
    consumer: str,
    delivery_token: str,
    context: Annotated[Context, TaskiqDepends()],
) -> None:
    """Pass one broker message to the worker-composed delivery processor."""
    parsed_event_id: uuid.UUID | None = None
    try:
        parsed_event_id = uuid.UUID(event_id)
        parsed_delivery_token = uuid.UUID(delivery_token)
    except (AttributeError, TypeError, ValueError) as error:
        if parsed_event_id is None:
            log_event(
                "ERROR",
                "task.outbox_delivery.malformed_payload",
                consumer=consumer,
                error_type=type(error).__name__,
            )
        else:
            log_event(
                "ERROR",
                "task.outbox_delivery.malformed_payload",
                event_id=str(parsed_event_id),
                consumer=consumer,
                error_type=type(error).__name__,
            )
        raise

    try:
        processor = cast(OutboxDeliveryProcessor, context.state.outbox_delivery_processor)
        await processor.execute(
            OutboxDeliveryReference(
                event_id=parsed_event_id,
                consumer=consumer,
                delivery_token=parsed_delivery_token,
            )
        )
    except Exception as error:
        if not is_handled_outbox_failure(error):
            log_event(
                "ERROR",
                "task.outbox_delivery.failed",
                event_id=str(parsed_event_id),
                consumer=consumer,
                error_type=type(error).__name__,
            )
        raise
