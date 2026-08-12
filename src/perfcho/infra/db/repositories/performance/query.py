"""Query Formula-owned Performance results."""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra.db.models.scoring import CalculationFormula, CalculationRelease, ScorePerformance
from perfcho.modules.performance.models import ScorePerformanceView
from perfcho.modules.scoring.models import Ruleset


class SqlAlchemyPerformanceQueryRepository:
    """Read persisted PP results without exposing ORM entities."""

    def __init__(self, session: AsyncSession) -> None:
        """Bind queries to one caller-owned session."""
        self._session = session

    async def list_for_score(self, score_id: int) -> tuple[ScorePerformanceView, ...]:
        """Return all Formula and immutable Release results for one Score."""
        rows = (
            await self._session.execute(
                select(
                    ScorePerformance.score_id,
                    CalculationFormula.id.label("formula_id"),
                    CalculationFormula.code.label("formula_code"),
                    CalculationFormula.name.label("formula_name"),
                    CalculationFormula.calculator,
                    CalculationRelease.id.label("release_id"),
                    CalculationRelease.ruleset,
                    CalculationRelease.version.label("release_version"),
                    CalculationRelease.active.label("release_active"),
                    ScorePerformance.pp,
                    ScorePerformance.breakdown,
                )
                .join(CalculationRelease, CalculationRelease.id == ScorePerformance.release_id)
                .join(CalculationFormula, CalculationFormula.id == CalculationRelease.formula_id)
                .where(ScorePerformance.score_id == score_id)
                .order_by(CalculationFormula.code, CalculationRelease.created_at.desc())
            )
        ).all()
        return tuple(
            ScorePerformanceView(
                score_id=row.score_id,
                formula_id=row.formula_id,
                formula_code=row.formula_code,
                formula_name=row.formula_name,
                calculator=row.calculator,
                release_id=row.release_id,
                ruleset=Ruleset(row.ruleset.value),
                release_version=row.release_version,
                release_active=row.release_active,
                pp=row.pp,
                breakdown=row.breakdown,
            )
            for row in rows
        )
