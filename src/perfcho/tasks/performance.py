"""Execute durable multi-formula performance jobs through Taskiq."""

import uuid
from datetime import UTC, datetime
from typing import Annotated, cast

from sqlalchemy.ext.asyncio import AsyncSession
from taskiq import Context, TaskiqDepends

from perfcho.infra.db.base import DbSessionFactory
from perfcho.infra.db.repositories.account import SqlAlchemyOutboxWriter
from perfcho.infra.db.repositories.performance import SqlAlchemyPerformanceCalculationRepository
from perfcho.infra.db.uow import SqlAlchemyUnitOfWorkFactory
from perfcho.infra.settings import settings
from perfcho.infra.taskiq import broker
from perfcho.modules.common import Clock, ObjectStorage
from perfcho.modules.scoring.ports import PerformanceCalculator
from perfcho.modules.scoring.services import PerformanceCalculationService


class _SystemClock:
    def now(self) -> datetime:
        return datetime.now(UTC)


@broker.task(task_name="perfcho.performance.calculate")
async def calculate_performance(
    job_id: str,
    lease_token: str,
    context: Annotated[Context, TaskiqDepends()],
) -> None:
    """Run one fenced calculation through short database phases."""
    session_factory = cast(DbSessionFactory, context.state.db_session_factory)
    service = PerformanceCalculationService(
        SqlAlchemyUnitOfWorkFactory(session_factory),
        lambda session: SqlAlchemyPerformanceCalculationRepository(cast(AsyncSession, session)),
        lambda session: SqlAlchemyOutboxWriter(cast(AsyncSession, session)),
        cast(PerformanceCalculator, context.state.performance_calculator),
        cast(ObjectStorage, context.state.object_storage),
        cast(Clock, _SystemClock()),
        max_attempts=settings.performance_calculation_max_attempts,
        max_beatmap_bytes=settings.performance_beatmap_max_bytes,
        max_retry_seconds=settings.performance_calculation_max_retry_seconds,
    )
    await service.execute(uuid.UUID(job_id), uuid.UUID(lease_token))
