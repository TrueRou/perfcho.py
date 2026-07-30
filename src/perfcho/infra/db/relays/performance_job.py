"""Persist broker relay leases for durable Performance calculation jobs."""

import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra.db.base import DbSessionFactory
from perfcho.infra.db.enums import CalculationJobStatus
from perfcho.infra.db.models.scoring import PerformanceCalculationJob


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
        async with self._session_factory.begin() as session:
            now = await _database_now(session)
            claimable = or_(
                PerformanceCalculationJob.status == CalculationJobStatus.PENDING,
                and_(
                    PerformanceCalculationJob.status == CalculationJobStatus.RUNNING,
                    PerformanceCalculationJob.lease_expires_at <= now,
                ),
            )
            await session.execute(
                update(PerformanceCalculationJob)
                .where(claimable, PerformanceCalculationJob.attempt_count >= self._max_attempts)
                .values(
                    status=CalculationJobStatus.DEAD,
                    dead_lettered_at=now,
                    lease_owner=None,
                    lease_token=None,
                    lease_expires_at=None,
                )
            )
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
            return tuple(references)

    async def mark_enqueued(
        self,
        reference: PerformanceJobReference,
        owner: str,
        broker_task_id: str,
    ) -> None:
        """Record Taskiq transport identity under a live relay lease."""
        async with self._session_factory.begin() as session:
            job = await _locked_job(session, reference.job_id)
            now = await _database_now(session)
            if not _owns_live_lease(job, reference, owner, now):
                return
            assert job is not None
            job.enqueued_at = now
            job.broker_task_id = broker_task_id
            job.last_error = None

    async def mark_enqueue_failed(
        self,
        reference: PerformanceJobReference,
        owner: str,
        error: Exception,
    ) -> None:
        """Release a failed enqueue without consuming a calculation attempt."""
        async with self._session_factory.begin() as session:
            job = await _locked_job(session, reference.job_id)
            now = await _database_now(session)
            if not _owns_live_lease(job, reference, owner, now):
                return
            assert job is not None
            job.status = CalculationJobStatus.PENDING
            job.available_at = now + _retry_delay(job.enqueue_count, self._max_retry_seconds)
            job.enqueued_at = None
            job.broker_task_id = None
            job.attempt_started_at = None
            job.last_error = _error_message(error)
            _clear_job_lease(job)

    async def release(self, reference: PerformanceJobReference, owner: str) -> None:
        """Release one unattempted claim during graceful relay cancellation."""
        async with self._session_factory.begin() as session:
            job = await _locked_job(session, reference.job_id)
            if (
                job is None
                or job.status is not CalculationJobStatus.RUNNING
                or job.lease_owner != owner
                or job.lease_token != reference.lease_token
                or job.enqueued_at is not None
                or job.attempt_started_at is not None
            ):
                return
            job.status = CalculationJobStatus.PENDING
            _clear_job_lease(job)


async def _database_now(session: AsyncSession) -> datetime:
    now = await session.scalar(select(func.clock_timestamp()))
    if now is None:
        raise RuntimeError("PostgreSQL did not return a relay timestamp")
    return now


async def _locked_job(session: AsyncSession, job_id: uuid.UUID) -> PerformanceCalculationJob | None:
    return await session.get(PerformanceCalculationJob, job_id, with_for_update=True)


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
