"""Execute durable multi-formula performance jobs through Taskiq."""

import uuid
from typing import Annotated, cast

from taskiq import Context, TaskiqDepends

from perfcho.infra.logging import log_event
from perfcho.infra.taskiq import broker
from perfcho.modules.performance.services import PerformanceCalculationService


@broker.task(task_name="perfcho.performance.calculate")
async def calculate_performance(
    job_id: str,
    lease_token: str,
    context: Annotated[Context, TaskiqDepends()],
) -> None:
    """Run one fenced calculation through short database phases."""
    parsed_job_id: uuid.UUID | None = None
    try:
        parsed_job_id = uuid.UUID(job_id)
        parsed_lease_token = uuid.UUID(lease_token)
    except (AttributeError, TypeError, ValueError) as error:
        if parsed_job_id is None:
            log_event(
                "ERROR",
                "task.performance_calculation.malformed_payload",
                exception=error,
                error_type=type(error).__name__,
            )
        else:
            log_event(
                "ERROR",
                "task.performance_calculation.malformed_payload",
                exception=error,
                job_id=str(parsed_job_id),
                error_type=type(error).__name__,
            )
        raise

    try:
        service = cast(PerformanceCalculationService, context.state.performance_calculation_service)
        await service.execute(parsed_job_id, parsed_lease_token)
    except Exception as error:
        log_event(
            "ERROR",
            "task.performance_calculation.failed",
            exception=error,
            job_id=str(parsed_job_id),
            error_type=type(error).__name__,
        )
        raise
