"""Coordinate external performance calculation and Formula-owned queries."""

import hashlib
import json
import uuid
from collections.abc import Callable
from datetime import timedelta
from time import monotonic_ns
from typing import Literal

from perfcho.infra.logging import duration_ms, log_event
from perfcho.modules.common.models import PendingEvent
from perfcho.modules.common.ports import ObjectUrlProvider, OutboxWriterFactory
from perfcho.modules.performance.errors import PerformanceCalculationError
from perfcho.modules.performance.models import PerformanceResult, ScorePerformanceView, thaw_json_mapping
from perfcho.modules.performance.ports import (
    PerformanceCalculationRepositoryFactory,
    PerformanceCalculator,
    PerformanceQueryRepositoryFactory,
    PerformanceUnitOfWork,
)

_RANKING_CONSUMER = "ranking-projector.v1"


class PerformanceCalculationService:
    """Execute one leased calculation without holding a transaction over external I/O."""

    def __init__(
        self,
        uow_factory: Callable[[], PerformanceUnitOfWork],
        repository_factory: PerformanceCalculationRepositoryFactory,
        outbox_writer_factory: OutboxWriterFactory,
        calculator: PerformanceCalculator,
        object_url_provider: ObjectUrlProvider,
        *,
        max_attempts: int,
        beatmap_url_expiry_seconds: int,
        max_retry_seconds: int,
    ) -> None:
        """Bind phased persistence, pure calculation, storage, and retry policy."""
        if min(max_attempts, beatmap_url_expiry_seconds, max_retry_seconds) < 1:
            raise ValueError("performance calculation limits must be positive")
        self._uow_factory = uow_factory
        self._repository_factory = repository_factory
        self._outbox_writer_factory = outbox_writer_factory
        self._calculator = calculator
        self._object_url_provider = object_url_provider
        self._max_attempts = max_attempts
        self._beatmap_url_expiry_seconds = beatmap_url_expiry_seconds
        self._max_retry_seconds = max_retry_seconds

    async def execute(self, job_id: uuid.UUID, lease_token: uuid.UUID) -> None:
        """Load, calculate, and persist one fenced job through separate transactions."""
        phase = "start"
        phase_started_ns = monotonic_ns()
        try:
            async with self._uow_factory() as uow:
                repository = self._repository_factory(uow.session)
                calculation = await repository.start(job_id, lease_token)
                await uow.commit()
        except Exception as error:
            retryable = not isinstance(error, PerformanceCalculationError) or error.retryable
            async with self._uow_factory() as uow:
                repository = self._repository_factory(uow.session)
                persisted = await repository.fail(
                    job_id,
                    lease_token,
                    error=f"{type(error).__name__}: {error}"[:4000],
                    retry_delay=timedelta(seconds=min(2, self._max_retry_seconds)),
                    dead=not retryable,
                    consume_attempt=True,
                )
                await uow.commit()
            _log_calculation_outcome(
                "fenced" if not persisted else "retry" if retryable else "dead",
                job_id,
                phase,
                phase_started_ns,
                error_type=type(error).__name__,
            )
            return
        if calculation is None:
            _log_calculation_outcome("fenced", job_id, phase, phase_started_ns)
            return
        _log_calculation_outcome("started", job_id, phase, phase_started_ns)

        try:
            phase = "object_url"
            phase_started_ns = monotonic_ns()
            beatmap_url = await self._object_url_provider.presign_read(
                calculation.beatmap_storage_key,
                expires_in_seconds=self._beatmap_url_expiry_seconds,
            )
            phase = "calculate"
            phase_started_ns = monotonic_ns()
            result = await self._calculator.calculate(calculation, beatmap_url=beatmap_url)
            output_digest = _performance_output_digest(result)
            phase = "complete"
            phase_started_ns = monotonic_ns()
            async with self._uow_factory() as uow:
                completion = await self._repository_factory(uow.session).complete(
                    calculation,
                    lease_token,
                    result,
                    output_digest=output_digest,
                )
                if completion is not None:
                    await self._outbox_writer_factory(uow.session).append(
                        PendingEvent(
                            aggregate_type="score",
                            aggregate_id=str(completion.score_id),
                            event_type="score.performance-calculated.v1",
                            schema_version=1,
                            payload={
                                "score_id": completion.score_id,
                                "scoreboard_id": completion.scoreboard_id,
                                "formula_id": str(completion.formula_id),
                                "formula_code": completion.formula_code,
                                "release_id": str(completion.release_id),
                                "pp": str(completion.pp),
                                "output_digest": completion.output_digest.hex(),
                            },
                            consumers=(_RANKING_CONSUMER,),
                            partition_key=f"scoreboard:{completion.scoreboard_id}",
                        )
                    )
                await uow.commit()
        except Exception as error:
            outcome = await self._record_failure(calculation.job_id, lease_token, calculation.attempt_count, error)
            _log_calculation_outcome(
                outcome,
                job_id,
                phase,
                phase_started_ns,
                error_type=type(error).__name__,
            )
            return
        _log_calculation_outcome(
            "succeeded" if completion is not None else "fenced",
            job_id,
            phase,
            phase_started_ns,
        )

    async def _record_failure(
        self,
        job_id: uuid.UUID,
        lease_token: uuid.UUID,
        attempt_count: int,
        error: Exception,
    ) -> Literal["retry", "dead", "fenced"]:
        retryable = not isinstance(error, PerformanceCalculationError) or error.retryable
        dead = not retryable or attempt_count >= self._max_attempts
        delay = min(2 ** max(attempt_count, 1), self._max_retry_seconds)
        async with self._uow_factory() as uow:
            persisted = await self._repository_factory(uow.session).fail(
                job_id,
                lease_token,
                error=f"{type(error).__name__}: {error}"[:4000],
                retry_delay=timedelta(seconds=delay),
                dead=dead,
                consume_attempt=False,
            )
            await uow.commit()
        if not persisted:
            return "fenced"
        return "dead" if dead else "retry"


class PerformanceQueryService:
    """Read all Formula-owned PP results for one accepted score."""

    def __init__(
        self,
        uow_factory: Callable[[], PerformanceUnitOfWork],
        repository_factory: PerformanceQueryRepositoryFactory,
    ) -> None:
        """Bind short-lived query transactions and performance persistence."""
        self._uow_factory = uow_factory
        self._repository_factory = repository_factory

    async def list_for_score(self, score_id: int) -> tuple[ScorePerformanceView, ...]:
        """Return all persisted releases grouped by Formula metadata."""
        if isinstance(score_id, bool) or not isinstance(score_id, int) or score_id < 1:
            raise ValueError("score_id must be a positive integer")
        async with self._uow_factory() as uow:
            return await self._repository_factory(uow.session).list_for_score(score_id)


def _performance_output_digest(result: PerformanceResult) -> bytes:
    payload = {
        "pp": str(result.pp),
        "difficulty": {
            "star_rating": str(result.difficulty.star_rating),
            "max_combo": result.difficulty.max_combo,
            "attributes": thaw_json_mapping(result.difficulty.attributes),
        },
        "breakdown": thaw_json_mapping(result.breakdown),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    return hashlib.sha256(encoded).digest()


def _log_calculation_outcome(
    outcome: Literal["started", "succeeded", "retry", "dead", "fenced"],
    job_id: uuid.UUID,
    phase: str,
    started_ns: int,
    *,
    error_type: str | None = None,
) -> None:
    level = {
        "started": "INFO",
        "succeeded": "INFO",
        "retry": "WARNING",
        "dead": "ERROR",
        "fenced": "DEBUG",
    }[outcome]
    if error_type is None:
        log_event(
            level,
            f"performance.calculation.{outcome}",
            job_id=str(job_id),
            phase=phase,
            duration_ms=duration_ms(started_ns),
        )
    else:
        log_event(
            level,
            f"performance.calculation.{outcome}",
            job_id=str(job_id),
            phase=phase,
            error_type=error_type,
            duration_ms=duration_ms(started_ns),
        )
