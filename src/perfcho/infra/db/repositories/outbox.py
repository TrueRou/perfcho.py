"""Persist transactional outbox events through a caller-owned session."""

import hashlib
import uuid

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra.db.models.events import OutboxDelivery, OutboxEvent
from perfcho.infra.tracing import current_trace_id
from perfcho.modules.common.models import PendingEvent


class SqlAlchemyOutboxWriter:
    """Adapt outbox persistence to the transaction-bound writer port."""

    def __init__(self, session: AsyncSession) -> None:
        """Bind event writes to the caller-owned session."""
        self._session = session

    async def append(self, event: PendingEvent) -> uuid.UUID:
        """Append one event and its deliveries without committing."""
        return (await append_outbox_event(self._session, event)).id


async def append_outbox_event(session: AsyncSession, event: PendingEvent) -> OutboxEvent:
    """Append one application event and explicit ordered deliveries."""
    lock_keys = sorted({_partition_lock_key(consumer, event.partition_key) for consumer in event.consumers})
    for lock_key in lock_keys:
        await session.execute(
            text("SELECT pg_advisory_xact_lock(:lock_key)"),
            {"lock_key": lock_key},
        )

    available_at = await session.scalar(select(func.clock_timestamp()))
    if available_at is None:
        raise RuntimeError("PostgreSQL did not return an outbox timestamp")
    trace_id = current_trace_id()
    persisted = OutboxEvent(
        aggregate_type=event.aggregate_type,
        aggregate_id=event.aggregate_id,
        event_type=event.event_type,
        schema_version=event.schema_version,
        payload=dict(event.payload),
        trace_id=uuid.UUID(hex=trace_id) if trace_id is not None else None,
    )
    session.add(persisted)
    await session.flush()
    for consumer in event.consumers:
        session.add(
            OutboxDelivery(
                event_id=persisted.id,
                consumer=consumer,
                source_position=persisted.position,
                partition_key=event.partition_key,
                available_at=available_at,
            )
        )
    return persisted


def _partition_lock_key(consumer: str, partition_key: str) -> int:
    digest = hashlib.blake2b(f"{consumer}\0{partition_key}".encode(), digest_size=8).digest()
    return int.from_bytes(digest, byteorder="big", signed=True)
