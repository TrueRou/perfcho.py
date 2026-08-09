"""Persist durable outbox delivery leases and broker enqueue outcomes."""

import uuid
from collections.abc import Collection, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal

from sqlalchemy import func, or_, select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from perfcho.infra.db.base import DbSessionFactory
from perfcho.infra.db.enums import OutboxDeliveryStatus
from perfcho.infra.db.models.events import OutboxDelivery, OutboxEvent
from perfcho.infra.logging import log_event

type DeliveryFailureOutcome = tuple[Literal["retry", "dead"], int]


@dataclass(frozen=True, slots=True)
class OutboxDeliveryReference:
    """Identify one leased delivery and its current fencing token."""

    event_id: uuid.UUID
    consumer: str
    delivery_token: uuid.UUID
    trace_id: uuid.UUID | None = None
    event_type: str | None = None
    event_created_at: datetime | None = None


class SqlAlchemyOutboxDeliveryRepository:
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
        """Bind delivery persistence to PostgreSQL and explicit retry limits."""
        if min(batch_size, lease_seconds, max_attempts, max_retry_seconds) < 1:
            raise ValueError("Outbox relay limits must be positive")
        self._session_factory = session_factory
        self._batch_size = batch_size
        self._lease_duration = timedelta(seconds=lease_seconds)
        self._max_attempts = max_attempts
        self._max_retry_seconds = max_retry_seconds

    async def claim(self, owner: str) -> tuple[OutboxDeliveryReference, ...]:
        """Lease the earliest due delivery in each unblocked consumer partition."""
        dead_count = 0
        async with self._session_factory.begin() as session:
            now = await _database_now(session)
            claimable = or_(
                OutboxDelivery.status == OutboxDeliveryStatus.PENDING,
                (OutboxDelivery.status == OutboxDeliveryStatus.RUNNING) & (OutboxDelivery.lease_expires_at <= now),
            )
            exhausted = tuple(
                await session.scalars(
                    select(OutboxDelivery)
                    .where(claimable, OutboxDelivery.attempt_count >= self._max_attempts)
                    .order_by(OutboxDelivery.source_position, OutboxDelivery.consumer)
                    .limit(self._batch_size)
                    .with_for_update(skip_locked=True)
                )
            )
            for delivery in exhausted:
                delivery.status = OutboxDeliveryStatus.DEAD
                delivery.dead_lettered_at = now
                _clear_delivery_lease(delivery)
            dead_count = len(exhausted)

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
            event_context = {
                event_id: (trace_id, event_type, created_at)
                for event_id, trace_id, event_type, created_at in await session.execute(
                    select(OutboxEvent.id, OutboxEvent.trace_id, OutboxEvent.event_type, OutboxEvent.created_at).where(
                        OutboxEvent.id.in_(delivery.event_id for delivery in deliveries)
                    )
                )
            }
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
                references.append(
                    OutboxDeliveryReference(
                        delivery.event_id,
                        delivery.consumer,
                        delivery_token,
                        *(event_context.get(delivery.event_id) or (None, None, None)),
                    )
                )
        if dead_count:
            log_event(
                "ERROR",
                "outbox.delivery.dead",
                failure_count=dead_count,
                maximum_attempts=self._max_attempts,
                reason="attempts_exhausted",
            )
        return tuple(references)

    async def record_enqueue_outcomes(
        self,
        outcomes: Sequence[tuple[OutboxDeliveryReference, str | Exception]],
        owner: str,
    ) -> None:
        """Persist all broker enqueue outcomes in one fenced transaction."""
        if not outcomes:
            return
        async with self._session_factory.begin() as session:
            now = await _database_now(session)
            deliveries = {
                (delivery.event_id, delivery.consumer): delivery
                for delivery in await session.scalars(
                    select(OutboxDelivery)
                    .where(
                        tuple_(OutboxDelivery.event_id, OutboxDelivery.consumer).in_(
                            (reference.event_id, reference.consumer) for reference, _ in outcomes
                        )
                    )
                    .with_for_update()
                )
            }
            for reference, outcome in outcomes:
                delivery = deliveries.get((reference.event_id, reference.consumer))
                if not _owns_live_lease(delivery, reference, owner, now):
                    continue
                assert delivery is not None
                if isinstance(outcome, Exception):
                    delivery.status = OutboxDeliveryStatus.PENDING
                    delivery.available_at = now + _retry_delay(
                        delivery.enqueue_count,
                        self._max_retry_seconds,
                    )
                    _clear_delivery_lease(delivery)
                    delivery.enqueued_at = None
                    delivery.broker_task_id = None
                    delivery.last_error = _error_message(outcome)
                else:
                    delivery.enqueued_at = now
                    delivery.broker_task_id = outcome
                    delivery.last_error = None

    async def release(self, references: Sequence[OutboxDeliveryReference], owner: str) -> None:
        """Release unattempted claims in one graceful-cancellation transaction."""
        if not references:
            return
        async with self._session_factory.begin() as session:
            deliveries = {
                (delivery.event_id, delivery.consumer): delivery
                for delivery in await session.scalars(
                    select(OutboxDelivery)
                    .where(
                        tuple_(OutboxDelivery.event_id, OutboxDelivery.consumer).in_(
                            (reference.event_id, reference.consumer) for reference in references
                        )
                    )
                    .with_for_update()
                )
            }
            for reference in references:
                delivery = deliveries.get((reference.event_id, reference.consumer))
                if (
                    delivery is None
                    or delivery.status is not OutboxDeliveryStatus.RUNNING
                    or delivery.lease_owner != owner
                    or delivery.delivery_token != reference.delivery_token
                    or delivery.enqueued_at is not None
                ):
                    continue
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


async def _database_now(session: AsyncSession) -> datetime:
    now = await session.scalar(select(func.clock_timestamp()))
    if now is None:
        raise RuntimeError("PostgreSQL did not return an outbox timestamp")
    return now


def _clear_delivery_lease(delivery: OutboxDelivery) -> None:
    delivery.lease_owner = None
    delivery.delivery_token = None
    delivery.lease_expires_at = None


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


def _retry_delay(attempt_count: int, max_seconds: int) -> timedelta:
    return timedelta(seconds=min(2 ** max(attempt_count, 1), max_seconds))


def _error_message(error: Exception) -> str:
    return f"{type(error).__name__}: {error}"[:4000]
