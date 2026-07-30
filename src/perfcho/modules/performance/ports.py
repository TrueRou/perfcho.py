"""Define transaction and calculator ports for performance workflows."""

import uuid
from datetime import timedelta
from typing import Protocol

from perfcho.modules.common.ports import UnitOfWork
from perfcho.modules.performance.models import (
    PerformanceCalculationInput,
    PerformanceCompletion,
    PerformanceResult,
    ScorePerformanceView,
)


class PerformanceUnitOfWork(UnitOfWork, Protocol):
    """Expose the transaction resource used to bind performance adapters."""

    @property
    def session(self) -> object:
        """Return the active transaction resource."""
        ...


class PerformanceCalculator(Protocol):
    """Calculate difficulty and PP through a versioned external engine."""

    async def calculate(
        self,
        calculation: PerformanceCalculationInput,
        *,
        beatmap_url: str,
    ) -> PerformanceResult:
        """Return deterministic output after reading the immutable Beatmap URL."""
        ...


class PerformanceCalculationRepository(Protocol):
    """Coordinate short transaction phases around external calculation I/O."""

    async def start(
        self,
        job_id: uuid.UUID,
        lease_token: uuid.UUID,
    ) -> PerformanceCalculationInput | None:
        """Fence and materialize one leased immutable calculation input."""
        ...

    async def complete(
        self,
        calculation: PerformanceCalculationInput,
        lease_token: uuid.UUID,
        result: PerformanceResult,
        *,
        output_digest: bytes,
    ) -> PerformanceCompletion | None:
        """Persist one idempotent result if the caller still owns the lease."""
        ...

    async def fail(
        self,
        job_id: uuid.UUID,
        lease_token: uuid.UUID,
        *,
        error: str,
        retry_delay: timedelta,
        dead: bool,
        consume_attempt: bool,
    ) -> None:
        """Release or dead-letter a failed fenced job."""
        ...


class PerformanceQueryRepository(Protocol):
    """Read Formula-owned performance results."""

    async def list_for_score(self, score_id: int) -> tuple[ScorePerformanceView, ...]:
        """Return every persisted Formula release result for one score."""
        ...


class PerformanceCalculationRepositoryFactory(Protocol):
    """Bind calculation persistence to one caller-owned transaction."""

    def __call__(self, session: object) -> PerformanceCalculationRepository:
        """Return a transaction-bound calculation repository."""
        ...


class PerformanceQueryRepositoryFactory(Protocol):
    """Bind performance queries to one caller-owned transaction."""

    def __call__(self, session: object) -> PerformanceQueryRepository:
        """Return a transaction-bound query repository."""
        ...
