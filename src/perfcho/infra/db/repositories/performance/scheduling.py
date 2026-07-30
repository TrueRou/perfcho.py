"""Schedule durable Performance jobs inside score acceptance transactions."""

import uuid
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra.db.enums import CalculationKind
from perfcho.infra.db.enums import Ruleset as DbRuleset
from perfcho.infra.db.models.scoring import (
    CalculationFormula,
    CalculationFormulaScoreboard,
    CalculationRelease,
    PerformanceCalculationJob,
)
from perfcho.modules.scoring.models import ScoreboardInfo


class SqlAlchemyPerformanceJobScheduler:
    """Schedule one job for each active Formula release matching a score."""

    def __init__(self, session: AsyncSession) -> None:
        """Bind scheduling to one caller-owned score transaction."""
        self._session = session

    async def schedule(
        self,
        *,
        score_id: int,
        scoreboard: ScoreboardInfo,
        now: datetime,
    ) -> None:
        """Create one job for every enabled active Formula on this Scoreboard."""
        release_ids = tuple(
            await self._session.scalars(
                select(CalculationRelease.id)
                .join(CalculationFormula, CalculationFormula.id == CalculationRelease.formula_id)
                .join(
                    CalculationFormulaScoreboard,
                    CalculationFormulaScoreboard.formula_id == CalculationFormula.id,
                )
                .where(
                    CalculationFormula.kind == CalculationKind.PERFORMANCE,
                    CalculationFormulaScoreboard.scoreboard_id == scoreboard.scoreboard_id,
                    CalculationFormula.enabled.is_(True),
                    CalculationRelease.ruleset == DbRuleset(scoreboard.ruleset.value),
                    CalculationRelease.active.is_(True),
                    CalculationRelease.difficulty_release_id.is_not(None),
                )
                .order_by(CalculationFormula.code)
            )
        )
        if release_ids:
            await self._session.execute(
                insert(PerformanceCalculationJob)
                .values(
                    [
                        {
                            "id": uuid.uuid7(),
                            "score_id": score_id,
                            "release_id": release_id,
                            "available_at": now,
                        }
                        for release_id in release_ids
                    ]
                )
                .on_conflict_do_nothing(
                    index_elements=(PerformanceCalculationJob.score_id, PerformanceCalculationJob.release_id)
                )
            )
