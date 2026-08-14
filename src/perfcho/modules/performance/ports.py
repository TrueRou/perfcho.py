"""Define transaction and calculator ports for performance workflows."""

import uuid
from typing import Protocol

from perfcho.modules.common.ports import UnitOfWork
from perfcho.modules.performance.models import (
    DifficultyCalculationResult,
    DifficultyRequest,
    PerformanceCalculationInput,
    PerformanceResult,
    ScorePerformanceView,
)
from perfcho.modules.scoring.models import Ruleset


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

    async def calculate_difficulty(
        self,
        request: DifficultyRequest,
        *,
        beatmap_url: str,
    ) -> DifficultyCalculationResult:
        """Return deterministic difficulty attributes for one beatmap and mods."""
        ...


class PerformanceQueryRepository(Protocol):
    """Read Formula-owned performance results."""

    async def list_for_score(self, score_id: int) -> tuple[ScorePerformanceView, ...]:
        """Return every persisted Formula release result for one score."""
        ...


class PerformanceQueryRepositoryFactory(Protocol):
    """Bind performance queries to one caller-owned transaction."""

    def __call__(self, session: object) -> PerformanceQueryRepository:
        """Return a transaction-bound query repository."""
        ...


class DifficultyRepository(Protocol):
    """Read the active difficulty release and persist deterministic attributes."""

    async def active_difficulty_release(self, ruleset: Ruleset) -> dict[str, object] | None:
        """Return the newest active difficulty release metadata for one ruleset."""
        ...

    async def get(
        self,
        *,
        beatmap_revision_id: int,
        ruleset: Ruleset,
        mods_digest: bytes,
        release_id: uuid.UUID,
    ) -> DifficultyCalculationResult | None:
        """Return a persisted difficulty result, or None when absent."""
        ...

    async def put(self, request: DifficultyRequest, result: DifficultyCalculationResult) -> None:
        """Idempotently persist one deterministic difficulty result."""
        ...


class DifficultyRepositoryFactory(Protocol):
    """Bind difficulty persistence to one caller-owned transaction."""

    def __call__(self, session: object) -> DifficultyRepository:
        """Return a transaction-bound difficulty repository."""
        ...
