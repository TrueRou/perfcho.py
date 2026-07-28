import asyncio
import hashlib
import os
import socket
import uuid
from collections.abc import Awaitable, Callable, Collection
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, cast

from sqlalchemy import or_, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from perfcho.infra.db import engine as infra_db
from perfcho.infra.db.base import DbSessionFactory
from perfcho.infra.db.models.events import OutboxDelivery, OutboxEvent
from perfcho.infra.settings import settings
from perfcho.infra.taskiq import broker

type ConsumerHandler = Callable[[AsyncSession, OutboxEvent, str], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class ConsumerRegistration:
    name: str
    event_types: frozenset[str]
    handler: ConsumerHandler


@dataclass(frozen=True, slots=True)
class DeliveryReference:
    event_id: uuid.UUID
    consumer: str
    delivery_token: uuid.UUID


_consumers: dict[str, ConsumerRegistration] = {}


def register_consumer(
    name: str,
    event_types: Collection[str],
) -> Callable[[ConsumerHandler], ConsumerHandler]:
    def decorator(handler: ConsumerHandler) -> ConsumerHandler:
        if name in _consumers:
            raise ValueError(f"Outbox consumer is already registered: {name}")
        _consumers[name] = ConsumerRegistration(name, frozenset(event_types), handler)
        return handler

    return decorator


async def write_outbox_event(
    session: AsyncSession,
    *,
    aggregate_type: str,
    aggregate_id: str,
    event_type: str,
    schema_version: int,
    payload: dict[str, object],
    consumers: Collection[str],
    available_at: datetime | None = None,
    partition_key: str = "default",
) -> OutboxEvent:
    consumer_names = tuple(consumers)
    if not consumer_names:
        raise ValueError("Outbox events require at least one consumer")
    if len(consumer_names) != len(set(consumer_names)):
        raise ValueError("Outbox event consumers must be unique")

    lock_keys = sorted({_partition_lock_key(consumer, partition_key) for consumer in consumer_names})
    for lock_key in lock_keys:
        await session.execute(text("SELECT pg_advisory_xact_lock(:lock_key)"), {"lock_key": lock_key})

    delivery_time = available_at or datetime.now(UTC)
    event = OutboxEvent(
        aggregate_type=aggregate_type,
        aggregate_id=aggregate_id,
        event_type=event_type,
        schema_version=schema_version,
        payload=payload,
        available_at=delivery_time,
    )
    session.add(event)
    await session.flush()

    for consumer in consumer_names:
        session.add(
            OutboxDelivery(
                event_id=event.id,
                consumer=consumer,
                partition_key=partition_key,
                available_at=delivery_time,
            )
        )

    return event


async def claim_deliveries(
    session_factory: DbSessionFactory,
    owner: str,
) -> list[DeliveryReference]:
    now = datetime.now(UTC)
    lease_expires_at = now + timedelta(seconds=settings.outbox_lease_seconds)

    async with session_factory.begin() as session:
        await session.execute(
            update(OutboxDelivery)
            .where(
                OutboxDelivery.completed_at.is_(None),
                OutboxDelivery.dead_lettered_at.is_(None),
                OutboxDelivery.attempt_count >= settings.outbox_max_attempts,
                or_(OutboxDelivery.lease_expires_at.is_(None), OutboxDelivery.lease_expires_at <= now),
            )
            .values(dead_lettered_at=now, lease_owner=None, lease_expires_at=None)
        )

        prior_delivery = aliased(OutboxDelivery)
        prior_event = aliased(OutboxEvent)
        earlier_unfinished = (
            select(1)
            .select_from(prior_delivery)
            .join(prior_event, prior_event.id == prior_delivery.event_id)
            .where(
                prior_delivery.consumer == OutboxDelivery.consumer,
                prior_delivery.partition_key == OutboxDelivery.partition_key,
                prior_delivery.completed_at.is_(None),
                prior_delivery.dead_lettered_at.is_(None),
                prior_event.position < OutboxEvent.position,
            )
            .correlate(OutboxDelivery, OutboxEvent)
        )
        result = await session.scalars(
            select(OutboxDelivery)
            .join(OutboxEvent, OutboxEvent.id == OutboxDelivery.event_id)
            .where(
                OutboxDelivery.completed_at.is_(None),
                OutboxDelivery.dead_lettered_at.is_(None),
                OutboxDelivery.attempt_count < settings.outbox_max_attempts,
                OutboxDelivery.available_at <= now,
                or_(OutboxDelivery.lease_expires_at.is_(None), OutboxDelivery.lease_expires_at <= now),
                ~earlier_unfinished.exists(),
            )
            .order_by(OutboxEvent.position, OutboxDelivery.consumer)
            .limit(settings.outbox_batch_size)
            .with_for_update(skip_locked=True)
        )
        deliveries = list(result)
        for delivery in deliveries:
            delivery_token = uuid.uuid4()
            delivery.lease_owner = owner
            delivery.lease_expires_at = lease_expires_at
            delivery.delivery_token = delivery_token
            delivery.enqueue_count += 1

    return [
        DeliveryReference(delivery.event_id, delivery.consumer, delivery.delivery_token)
        for delivery in deliveries
        if delivery.delivery_token is not None
    ]


async def mark_delivery_enqueued(
    session_factory: DbSessionFactory,
    reference: DeliveryReference,
    owner: str,
    broker_task_id: str,
) -> None:
    async with session_factory.begin() as session:
        delivery = await session.get(
            OutboxDelivery,
            {"event_id": reference.event_id, "consumer": reference.consumer},
            with_for_update=True,
        )
        if (
            delivery is not None
            and delivery.lease_owner == owner
            and delivery.delivery_token == reference.delivery_token
        ):
            delivery.enqueued_at = datetime.now(UTC)
            delivery.broker_task_id = broker_task_id
            delivery.last_error = None


async def mark_delivery_enqueue_failed(
    session_factory: DbSessionFactory,
    reference: DeliveryReference,
    owner: str,
    error: Exception,
) -> None:
    async with session_factory.begin() as session:
        delivery = await session.get(
            OutboxDelivery,
            {"event_id": reference.event_id, "consumer": reference.consumer},
            with_for_update=True,
        )
        if delivery is None or delivery.lease_owner != owner or delivery.delivery_token != reference.delivery_token:
            return
        delivery.available_at = datetime.now(UTC) + _retry_delay(delivery.enqueue_count)
        delivery.lease_owner = None
        delivery.lease_expires_at = None
        delivery.delivery_token = None
        delivery.last_error = _error_message(error)


async def process_delivery(
    session_factory: DbSessionFactory,
    event_id: uuid.UUID,
    consumer: str,
    delivery_token: uuid.UUID,
) -> None:
    try:
        async with session_factory.begin() as session:
            delivery = await session.get(
                OutboxDelivery,
                {"event_id": event_id, "consumer": consumer},
                with_for_update=True,
            )
            if delivery is None:
                raise LookupError(f"Outbox delivery does not exist: {event_id}/{consumer}")
            if delivery.completed_at is not None or delivery.dead_lettered_at is not None:
                return
            if delivery.delivery_token != delivery_token:
                return

            registration = _consumers.get(consumer)
            if registration is None:
                raise LookupError(f"Outbox consumer is not registered: {consumer}")

            event = await session.get(OutboxEvent, event_id)
            if event is None:
                raise LookupError(f"Outbox event does not exist: {event_id}")
            if event.event_type not in registration.event_types:
                raise LookupError(f"Outbox consumer {consumer} does not accept event type {event.event_type}")

            await registration.handler(session, event, delivery.partition_key)
            delivery.attempt_count += 1
            delivery.completed_at = datetime.now(UTC)
            delivery.lease_owner = None
            delivery.lease_expires_at = None
            delivery.last_error = None
    except Exception as error:
        await _record_delivery_failure(session_factory, event_id, consumer, delivery_token, error)
        raise


async def relay_once(session_factory: DbSessionFactory, owner: str) -> int:
    from perfcho.tasks.outbox import dispatch_outbox_delivery

    deliveries = await claim_deliveries(session_factory, owner)
    for reference in deliveries:
        try:
            task = await cast(Any, dispatch_outbox_delivery).kiq(
                str(reference.event_id),
                reference.consumer,
                str(reference.delivery_token),
            )
            await mark_delivery_enqueued(session_factory, reference, owner, str(task.task_id))
        except Exception as error:
            await mark_delivery_enqueue_failed(session_factory, reference, owner, error)
    return len(deliveries)


async def run_relay() -> None:
    owner = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4()}"
    db_engine = await infra_db.create_engine()
    session_factory = infra_db.create_session_factory(db_engine)

    try:
        await broker.startup()
        while True:
            claimed = await relay_once(session_factory, owner)
            if claimed == 0:
                await asyncio.sleep(settings.outbox_poll_interval)
    finally:
        await broker.shutdown()
        await db_engine.dispose()


def main() -> None:
    asyncio.run(run_relay())


async def _record_delivery_failure(
    session_factory: DbSessionFactory,
    event_id: uuid.UUID,
    consumer: str,
    delivery_token: uuid.UUID,
    error: Exception,
) -> None:
    async with session_factory.begin() as session:
        delivery = await session.get(
            OutboxDelivery,
            {"event_id": event_id, "consumer": consumer},
            with_for_update=True,
        )
        if delivery is None or delivery.completed_at is not None or delivery.delivery_token != delivery_token:
            return
        delivery.attempt_count += 1
        delivery.available_at = datetime.now(UTC) + _retry_delay(delivery.attempt_count)
        delivery.enqueued_at = None
        delivery.lease_owner = None
        delivery.lease_expires_at = None
        delivery.delivery_token = None
        delivery.broker_task_id = None
        delivery.last_error = _error_message(error)
        if delivery.attempt_count >= settings.outbox_max_attempts:
            delivery.dead_lettered_at = datetime.now(UTC)


def _retry_delay(attempt_count: int) -> timedelta:
    return timedelta(seconds=min(2 ** max(attempt_count, 1), settings.outbox_lease_seconds))


def _error_message(error: Exception) -> str:
    return f"{type(error).__name__}: {error}"[:4000]


def _partition_lock_key(consumer: str, partition_key: str) -> int:
    digest = hashlib.blake2b(f"{consumer}\0{partition_key}".encode(), digest_size=8).digest()
    return int.from_bytes(digest, byteorder="big", signed=True)


if __name__ == "__main__":
    main()
