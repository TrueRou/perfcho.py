"""Persist and execute ordered outbox delivery leases."""

import uuid
from collections.abc import Collection
from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from perfcho.infra.db.base import DbSessionFactory
from perfcho.infra.db.enums import OutboxDeliveryStatus
from perfcho.infra.db.models.events import OutboxDelivery, OutboxEvent
from perfcho.infra.db.projectors.catalog import ConsumerCatalog


@dataclass(frozen=True, slots=True)
class OutboxDeliveryReference:
    """Identify one leased delivery and its current fencing token."""

    event_id: uuid.UUID
    consumer: str
    delivery_token: uuid.UUID


class SqlAlchemyOutboxDeliveryRelayStore:
    """Claim ordered deliveries and persist their broker enqueue outcomes."""

    def __init__(
        self,
        session_factory: DbSessionFactory,
        *,
        batch_size: int,
        lease_seconds: int,
        max_attempts: int,
        max_retry_seconds: int,
    ) -> None:
        """Bind relay state to PostgreSQL and explicit retry limits."""
        if min(batch_size, lease_seconds, max_attempts, max_retry_seconds) < 1:
            raise ValueError("Outbox relay limits must be positive")
        self._session_factory = session_factory
        self._batch_size = batch_size
        self._lease_duration = timedelta(seconds=lease_seconds)
        self._max_attempts = max_attempts
        self._max_retry_seconds = max_retry_seconds

    async def claim(self, owner: str) -> tuple[OutboxDeliveryReference, ...]:
        """Lease the earliest due delivery in each unblocked consumer partition."""
        async with self._session_factory.begin() as session:
            now = await _database_now(session)
            claimable = or_(
                OutboxDelivery.status == OutboxDeliveryStatus.PENDING,
                (OutboxDelivery.status == OutboxDeliveryStatus.RUNNING) & (OutboxDelivery.lease_expires_at <= now),
            )
            await session.execute(
                update(OutboxDelivery)
                .where(claimable, OutboxDelivery.attempt_count >= self._max_attempts)
                .values(
                    status=OutboxDeliveryStatus.DEAD,
                    dead_lettered_at=now,
                    lease_owner=None,
                    delivery_token=None,
                    lease_expires_at=None,
                )
            )

            prior = aliased(OutboxDelivery)
            earlier_unfinished = (
                select(1)
                .select_from(prior)
                .where(
                    prior.consumer == OutboxDelivery.consumer,
                    prior.partition_key == OutboxDelivery.partition_key,
                    prior.status != OutboxDeliveryStatus.SUCCEEDED,
                    prior.source_position < OutboxDelivery.source_position,
                )
                .correlate(OutboxDelivery)
            )
            deliveries = tuple(
                await session.scalars(
                    select(OutboxDelivery)
                    .where(
                        claimable,
                        OutboxDelivery.attempt_count < self._max_attempts,
                        OutboxDelivery.available_at <= now,
                        ~earlier_unfinished.exists(),
                    )
                    .order_by(OutboxDelivery.source_position, OutboxDelivery.consumer)
                    .limit(self._batch_size)
                    .with_for_update(skip_locked=True)
                )
            )
            lease_expires_at = now + self._lease_duration
            references: list[OutboxDeliveryReference] = []
            for delivery in deliveries:
                delivery_token = uuid.uuid4()
                delivery.status = OutboxDeliveryStatus.RUNNING
                delivery.lease_owner = owner
                delivery.delivery_token = delivery_token
                delivery.lease_expires_at = lease_expires_at
                delivery.enqueued_at = None
                delivery.broker_task_id = None
                delivery.enqueue_count += 1
                references.append(OutboxDeliveryReference(delivery.event_id, delivery.consumer, delivery_token))
            return tuple(references)

    async def mark_enqueued(
        self,
        reference: OutboxDeliveryReference,
        owner: str,
        broker_task_id: str,
    ) -> None:
        """Record a broker task identifier while the relay owns a live lease."""
        async with self._session_factory.begin() as session:
            delivery = await _locked_delivery(session, reference)
            now = await _database_now(session)
            if not _owns_live_lease(delivery, reference, owner, now):
                return
            assert delivery is not None
            delivery.enqueued_at = now
            delivery.broker_task_id = broker_task_id
            delivery.last_error = None

    async def mark_enqueue_failed(
        self,
        reference: OutboxDeliveryReference,
        owner: str,
        error: Exception,
    ) -> None:
        """Release a failed enqueue lease without consuming a business attempt."""
        async with self._session_factory.begin() as session:
            delivery = await _locked_delivery(session, reference)
            now = await _database_now(session)
            if not _owns_live_lease(delivery, reference, owner, now):
                return
            assert delivery is not None
            delivery.status = OutboxDeliveryStatus.PENDING
            delivery.available_at = now + _retry_delay(
                delivery.enqueue_count,
                self._max_retry_seconds,
            )
            _clear_delivery_lease(delivery)
            delivery.enqueued_at = None
            delivery.broker_task_id = None
            delivery.last_error = _error_message(error)

    async def release(self, reference: OutboxDeliveryReference, owner: str) -> None:
        """Release one unattempted claim during graceful relay cancellation."""
        async with self._session_factory.begin() as session:
            delivery = await _locked_delivery(session, reference)
            if (
                delivery is None
                or delivery.status is not OutboxDeliveryStatus.RUNNING
                or delivery.lease_owner != owner
                or delivery.delivery_token != reference.delivery_token
                or delivery.enqueued_at is not None
            ):
                return
            delivery.status = OutboxDeliveryStatus.PENDING
            _clear_delivery_lease(delivery)

    async def unknown_consumers(self, known_consumers: Collection[str]) -> frozenset[str]:
        """Return unfinished delivery consumers absent from the worker catalog."""
        async with self._session_factory() as session:
            consumers = set(
                await session.scalars(
                    select(OutboxDelivery.consumer)
                    .where(OutboxDelivery.status != OutboxDeliveryStatus.SUCCEEDED)
                    .distinct()
                )
            )
        return frozenset(consumers.difference(known_consumers))


class OutboxDeliveryProcessor:
    """Execute one fenced projector transaction and persist bounded failures."""

    def __init__(
        self,
        session_factory: DbSessionFactory,
        consumer_catalog: ConsumerCatalog,
        *,
        max_attempts: int,
        max_retry_seconds: int,
    ) -> None:
        """Bind delivery execution to PostgreSQL, consumers, and retry policy."""
        if min(max_attempts, max_retry_seconds) < 1:
            raise ValueError("Outbox processor limits must be positive")
        self._session_factory = session_factory
        self._consumer_catalog = consumer_catalog
        self._max_attempts = max_attempts
        self._max_retry_seconds = max_retry_seconds

    async def execute(self, reference: OutboxDeliveryReference) -> None:
        """Run one delivery if its lease is current and unexpired at task start."""
        try:
            async with self._session_factory.begin() as session:
                delivery = await _locked_delivery(session, reference)
                if delivery is None:
                    raise LookupError(f"Outbox delivery does not exist: {reference.event_id}/{reference.consumer}")
                if delivery.status in {OutboxDeliveryStatus.SUCCEEDED, OutboxDeliveryStatus.DEAD}:
                    return
                now = await _database_now(session)
                if (
                    delivery.status is not OutboxDeliveryStatus.RUNNING
                    or delivery.delivery_token != reference.delivery_token
                    or delivery.lease_expires_at is None
                    or delivery.lease_expires_at <= now
                ):
                    return

                registration = self._consumer_catalog.get(reference.consumer)
                if registration is None:
                    raise LookupError(f"Outbox consumer is not registered: {reference.consumer}")
                event = await session.get(OutboxEvent, reference.event_id)
                if event is None:
                    raise LookupError(f"Outbox event does not exist: {reference.event_id}")
                if event.event_type not in registration.event_types:
                    raise LookupError(
                        f"Outbox consumer {reference.consumer} does not accept event type {event.event_type}"
                    )

                await registration.handler(session, event, delivery.partition_key)
                delivery.attempt_count += 1
                delivery.status = OutboxDeliveryStatus.SUCCEEDED
                delivery.completed_at = now
                _clear_delivery_lease(delivery)
                delivery.last_error = None
        except Exception as error:
            await self._record_failure(reference, error)
            raise

    async def _record_failure(self, reference: OutboxDeliveryReference, error: Exception) -> None:
        async with self._session_factory.begin() as session:
            delivery = await _locked_delivery(session, reference)
            if (
                delivery is None
                or delivery.status is not OutboxDeliveryStatus.RUNNING
                or delivery.delivery_token != reference.delivery_token
            ):
                return
            now = await _database_now(session)
            delivery.attempt_count += 1
            delivery.enqueued_at = None
            delivery.broker_task_id = None
            delivery.last_error = _error_message(error)
            if delivery.attempt_count >= self._max_attempts:
                delivery.status = OutboxDeliveryStatus.DEAD
                delivery.dead_lettered_at = now
            else:
                delivery.status = OutboxDeliveryStatus.PENDING
                delivery.available_at = now + _retry_delay(
                    delivery.attempt_count,
                    self._max_retry_seconds,
                )
            _clear_delivery_lease(delivery)


async def _database_now(session: AsyncSession) -> datetime:
    now = await session.scalar(select(func.clock_timestamp()))
    if now is None:
        raise RuntimeError("PostgreSQL did not return a relay timestamp")
    return now


async def _locked_delivery(
    session: AsyncSession,
    reference: OutboxDeliveryReference,
) -> OutboxDelivery | None:
    return await session.get(
        OutboxDelivery,
        {"event_id": reference.event_id, "consumer": reference.consumer},
        with_for_update=True,
    )


def _owns_live_lease(
    delivery: OutboxDelivery | None,
    reference: OutboxDeliveryReference,
    owner: str,
    now: datetime,
) -> bool:
    return bool(
        delivery is not None
        and delivery.status is OutboxDeliveryStatus.RUNNING
        and delivery.lease_owner == owner
        and delivery.delivery_token == reference.delivery_token
        and delivery.lease_expires_at is not None
        and delivery.lease_expires_at > now
    )


def _clear_delivery_lease(delivery: OutboxDelivery) -> None:
    delivery.lease_owner = None
    delivery.delivery_token = None
    delivery.lease_expires_at = None


def _retry_delay(attempt_count: int, max_seconds: int) -> timedelta:
    return timedelta(seconds=min(2 ** max(attempt_count, 1), max_seconds))


def _error_message(error: Exception) -> str:
    return f"{type(error).__name__}: {error}"[:4000]
