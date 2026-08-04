"""Persist broker relay leases for durable Performance calculation jobs."""

import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from time import monotonic_ns

from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra.db.base import DbSessionFactory
from perfcho.infra.db.enums import CalculationJobStatus
from perfcho.infra.db.models.scoring import PerformanceCalculationJob
from perfcho.infra.logging import duration_ms, log_event


@dataclass(frozen=True, slots=True)
class PerformanceJobReference:
    """Identify one leased Performance job and its fencing token."""

    job_id: uuid.UUID
    lease_token: uuid.UUID


class SqlAlchemyPerformanceJobRelayStore:
    """Claim Performance jobs and persist broker enqueue outcomes."""

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
            raise ValueError("Performance relay limits must be positive")
        self._session_factory = session_factory
        self._batch_size = batch_size
        self._lease_duration = timedelta(seconds=lease_seconds)
        self._max_attempts = max_attempts
        self._max_retry_seconds = max_retry_seconds

    async def claim(self, owner: str) -> tuple[PerformanceJobReference, ...]:
        """Lease a bounded batch of pending or abandoned calculation jobs."""
        started_ns = monotonic_ns()
        dead_count = 0
        async with self._session_factory.begin() as session:
            now = await _database_now(session)
            claimable = or_(
                PerformanceCalculationJob.status == CalculationJobStatus.PENDING,
                and_(
                    PerformanceCalculationJob.status == CalculationJobStatus.RUNNING,
                    PerformanceCalculationJob.lease_expires_at <= now,
                ),
            )
            exhausted = tuple(
                await session.scalars(
                    select(PerformanceCalculationJob)
                    .where(claimable, PerformanceCalculationJob.attempt_count >= self._max_attempts)
                    .order_by(PerformanceCalculationJob.available_at, PerformanceCalculationJob.created_at)
                    .limit(self._batch_size)
                    .with_for_update(skip_locked=True)
                )
            )
            for job in exhausted:
                job.status = CalculationJobStatus.DEAD
                job.dead_lettered_at = now
                _clear_job_lease(job)
            dead_count = len(exhausted)
            jobs = tuple(
                await session.scalars(
                    select(PerformanceCalculationJob)
                    .where(
                        claimable,
                        PerformanceCalculationJob.attempt_count < self._max_attempts,
                        PerformanceCalculationJob.available_at <= now,
                    )
                    .order_by(PerformanceCalculationJob.available_at, PerformanceCalculationJob.created_at)
                    .limit(self._batch_size)
                    .with_for_update(skip_locked=True)
                )
            )
            lease_expires_at = now + self._lease_duration
            references: list[PerformanceJobReference] = []
            for job in jobs:
                lease_token = uuid.uuid4()
                job.status = CalculationJobStatus.RUNNING
                job.lease_owner = owner
                job.lease_token = lease_token
                job.lease_expires_at = lease_expires_at
                job.attempt_started_at = None
                job.enqueued_at = None
                job.broker_task_id = None
                job.enqueue_count += 1
                references.append(PerformanceJobReference(job.id, lease_token))
        if dead_count:
            log_event(
                "ERROR",
                "performance.calculation.dead",
                failure_count=dead_count,
                phase="relay",
                maximum_attempts=self._max_attempts,
                reason="attempts_exhausted",
                duration_ms=duration_ms(started_ns),
            )
        return tuple(references)

    async def record_enqueue_outcomes(
        self,
        outcomes: Sequence[tuple[PerformanceJobReference, str | Exception]],
        owner: str,
    ) -> None:
        """Persist all Taskiq enqueue outcomes in one fenced transaction."""
        if not outcomes:
            return
        async with self._session_factory.begin() as session:
            now = await _database_now(session)
            jobs = {
                job.id: job
                for job in await session.scalars(
                    select(PerformanceCalculationJob)
                    .where(PerformanceCalculationJob.id.in_(reference.job_id for reference, _ in outcomes))
                    .with_for_update()
                )
            }
            for reference, outcome in outcomes:
                job = jobs.get(reference.job_id)
                if not _owns_live_lease(job, reference, owner, now):
                    continue
                assert job is not None
                if isinstance(outcome, Exception):
                    job.status = CalculationJobStatus.PENDING
                    job.available_at = now + _retry_delay(job.enqueue_count, self._max_retry_seconds)
                    job.enqueued_at = None
                    job.broker_task_id = None
                    job.attempt_started_at = None
                    job.last_error = _error_message(outcome)
                    _clear_job_lease(job)
                else:
                    job.enqueued_at = now
                    job.broker_task_id = outcome
                    job.last_error = None

    async def release(self, references: Sequence[PerformanceJobReference], owner: str) -> None:
        """Release unattempted claims in one graceful-cancellation transaction."""
        if not references:
            return
        async with self._session_factory.begin() as session:
            jobs = {
                job.id: job
                for job in await session.scalars(
                    select(PerformanceCalculationJob)
                    .where(PerformanceCalculationJob.id.in_(reference.job_id for reference in references))
                    .with_for_update()
                )
            }
            for reference in references:
                job = jobs.get(reference.job_id)
                if (
                    job is None
                    or job.status is not CalculationJobStatus.RUNNING
                    or job.lease_owner != owner
                    or job.lease_token != reference.lease_token
                    or job.enqueued_at is not None
                    or job.attempt_started_at is not None
                ):
                    continue
                job.status = CalculationJobStatus.PENDING
                _clear_job_lease(job)


async def _database_now(session: AsyncSession) -> datetime:
    now = await session.scalar(select(func.clock_timestamp()))
    if now is None:
        raise RuntimeError("PostgreSQL did not return a relay timestamp")
    return now


def _owns_live_lease(
    job: PerformanceCalculationJob | None,
    reference: PerformanceJobReference,
    owner: str,
    now: datetime,
) -> bool:
    return bool(
        job is not None
        and job.status is CalculationJobStatus.RUNNING
        and job.lease_owner == owner
        and job.lease_token == reference.lease_token
        and job.lease_expires_at is not None
        and job.lease_expires_at > now
    )


def _clear_job_lease(job: PerformanceCalculationJob) -> None:
    job.lease_owner = None
    job.lease_token = None
    job.lease_expires_at = None


def _retry_delay(attempt_count: int, max_seconds: int) -> timedelta:
    return timedelta(seconds=min(2 ** max(attempt_count, 1), max_seconds))


def _error_message(error: Exception) -> str:
    return f"{type(error).__name__}: {error}"[:4000]
