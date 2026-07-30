"""Execute durable multi-formula performance jobs through Taskiq."""

import uuid
from typing import Annotated, cast

from taskiq import Context, TaskiqDepends

from perfcho.infra.taskiq import broker
from perfcho.modules.performance.services import PerformanceCalculationService


@broker.task(task_name="perfcho.performance.calculate")
async def calculate_performance(
    job_id: str,
    lease_token: str,
    context: Annotated[Context, TaskiqDepends()],
) -> None:
    """Run one fenced calculation through short database phases."""
    service = cast(PerformanceCalculationService, context.state.performance_calculation_service)
    await service.execute(uuid.UUID(job_id), uuid.UUID(lease_token))
