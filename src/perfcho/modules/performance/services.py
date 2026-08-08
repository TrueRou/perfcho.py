"""Coordinate external performance calculation and Formula-owned queries."""

from collections.abc import Callable

from perfcho.modules.performance.models import ScorePerformanceView
from perfcho.modules.performance.ports import (
    PerformanceQueryRepositoryFactory,
    PerformanceUnitOfWork,
)


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
