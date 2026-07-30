"""Reliably relay durable performance calculation jobs through Taskiq."""

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, cast

from sqlalchemy import and_, or_, select, update

from perfcho.infra.db.base import DbSessionFactory
from perfcho.infra.db.enums import CalculationJobStatus
from perfcho.infra.db.models.scoring import PerformanceCalculationJob
from perfcho.infra.settings import settings


@dataclass(frozen=True, slots=True)
class CalculationJobReference:
    """Identify one leased calculation and its current fencing token."""

    job_id: uuid.UUID
    lease_token: uuid.UUID


async def claim_calculation_jobs(
    session_factory: DbSessionFactory,
    owner: str,
) -> list[CalculationJobReference]:
    """Lease a bounded batch of pending or abandoned calculation jobs."""
    now = datetime.now(UTC)
    lease_expires_at = now + timedelta(seconds=settings.performance_calculation_lease_seconds)
    claimable = or_(
        PerformanceCalculationJob.status == CalculationJobStatus.PENDING,
        and_(
            PerformanceCalculationJob.status == CalculationJobStatus.RUNNING,
            PerformanceCalculationJob.lease_expires_at <= now,
        ),
    )
    async with session_factory.begin() as session:
        await session.execute(
            update(PerformanceCalculationJob)
            .where(
                claimable,
                PerformanceCalculationJob.attempt_count >= settings.performance_calculation_max_attempts,
            )
            .values(
                status=CalculationJobStatus.DEAD,
                dead_lettered_at=now,
                lease_owner=None,
                lease_token=None,
                lease_expires_at=None,
            )
        )
        jobs = list(
            await session.scalars(
                select(PerformanceCalculationJob)
                .where(
                    claimable,
                    PerformanceCalculationJob.attempt_count < settings.performance_calculation_max_attempts,
                    PerformanceCalculationJob.available_at <= now,
                )
                .order_by(PerformanceCalculationJob.available_at, PerformanceCalculationJob.created_at)
                .limit(settings.performance_calculation_batch_size)
                .with_for_update(skip_locked=True)
            )
        )
        for job in jobs:
            lease_token = uuid.uuid4()
            job.status = CalculationJobStatus.RUNNING
            job.lease_owner = owner
            job.lease_token = lease_token
            job.lease_expires_at = lease_expires_at
            job.enqueue_count += 1

    return [CalculationJobReference(job.id, job.lease_token) for job in jobs if job.lease_token is not None]


async def mark_calculation_enqueued(
    session_factory: DbSessionFactory,
    reference: CalculationJobReference,
    owner: str,
    broker_task_id: str,
) -> None:
    """Record Taskiq transport identity while the relay owns the lease."""
    async with session_factory.begin() as session:
        job = await session.get(PerformanceCalculationJob, reference.job_id, with_for_update=True)
        if job is not None and job.lease_owner == owner and job.lease_token == reference.lease_token:
            job.enqueued_at = datetime.now(UTC)
            job.broker_task_id = broker_task_id
            job.last_error = None


async def mark_calculation_enqueue_failed(
    session_factory: DbSessionFactory,
    reference: CalculationJobReference,
    owner: str,
    error: Exception,
) -> None:
    """Release a failed enqueue without consuming a business attempt."""
    async with session_factory.begin() as session:
        job = await session.get(PerformanceCalculationJob, reference.job_id, with_for_update=True)
        if job is None or job.lease_owner != owner or job.lease_token != reference.lease_token:
            return
        job.status = CalculationJobStatus.PENDING
        job.available_at = datetime.now(UTC) + _retry_delay(job.enqueue_count)
        job.enqueued_at = None
        job.lease_owner = None
        job.lease_token = None
        job.lease_expires_at = None
        job.broker_task_id = None
        job.last_error = f"{type(error).__name__}: {error}"[:4000]


async def relay_calculations_once(session_factory: DbSessionFactory, owner: str) -> int:
    """Claim and enqueue one bounded calculation batch."""
    from perfcho.tasks.performance import calculate_performance

    jobs = await claim_calculation_jobs(session_factory, owner)
    for reference in jobs:
        try:
            task = await cast(Any, calculate_performance).kiq(str(reference.job_id), str(reference.lease_token))
            await mark_calculation_enqueued(session_factory, reference, owner, str(task.task_id))
        except Exception as error:
            await mark_calculation_enqueue_failed(session_factory, reference, owner, error)
    return len(jobs)


def _retry_delay(attempt_count: int) -> timedelta:
    seconds = min(2 ** max(attempt_count, 1), settings.performance_calculation_max_retry_seconds)
    return timedelta(seconds=seconds)
