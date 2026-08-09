"""Expose fenced outbox delivery processing as a Taskiq task."""

import uuid
from datetime import UTC, datetime
from typing import Annotated, cast

from taskiq import Context, TaskiqDepends

from perfcho.infra.db.relays.outbox_delivery import (
    OutboxDeliveryProcessor,
    OutboxDeliveryReference,
    is_handled_outbox_failure,
)
from perfcho.infra.logging import log_event, set_relay_delay_ms, set_relay_event_type
from perfcho.infra.taskiq import broker
from perfcho.infra.tracing import trace_context


@broker.task(task_name="perfcho.outbox.dispatch")
async def dispatch_outbox_delivery(
    event_id: str,
    consumer: str,
    delivery_token: str,
    trace_id: str | None,
    event_type: str | None,
    event_created_at: str | None,
    context: Annotated[Context, TaskiqDepends()],
) -> None:
    """Pass one broker message to the worker-composed delivery processor."""
    set_relay_event_type(event_type)
    set_relay_delay_ms(_consumer_delay_ms(event_created_at))
    try:
        with trace_context(trace_id):
            await _dispatch_outbox_delivery(event_id, consumer, delivery_token, context)
    finally:
        set_relay_event_type(None)
        set_relay_delay_ms(None)


def _consumer_delay_ms(event_created_at: str | None) -> float | None:
    """Return elapsed milliseconds from durable event creation to consumer start."""
    if event_created_at is None:
        return None
    try:
        created_at = datetime.fromisoformat(event_created_at)
    except ValueError:
        return None
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=UTC)
    return max(0.0, round((datetime.now(UTC) - created_at).total_seconds() * 1000, 3))


async def _dispatch_outbox_delivery(
    event_id: str,
    consumer: str,
    delivery_token: str,
    context: Context,
) -> None:
    """Execute one delivery inside the caller's trace context."""
    parsed_event_id: uuid.UUID | None = None
    try:
        parsed_event_id = uuid.UUID(event_id)
        parsed_delivery_token = uuid.UUID(delivery_token)
    except (AttributeError, TypeError, ValueError) as error:
        if parsed_event_id is None:
            log_event(
                "ERROR",
                "task.outbox_delivery.malformed_payload",
                exception=error,
                consumer=consumer,
                error_type=type(error).__name__,
            )
        else:
            log_event(
                "ERROR",
                "task.outbox_delivery.malformed_payload",
                exception=error,
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
                exception=error,
                event_id=str(parsed_event_id),
                consumer=consumer,
                error_type=type(error).__name__,
            )
        raise
