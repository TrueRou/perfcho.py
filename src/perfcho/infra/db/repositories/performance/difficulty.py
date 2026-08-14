"""Persist and read standalone difficulty attributes for beatmap/mod inputs."""

from __future__ import annotations

import uuid
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra.db.enums import CalculationKind
from perfcho.infra.db.models.scoring import BeatmapDifficultyAttribute, CalculationFormula, CalculationRelease
from perfcho.modules.performance.models import DifficultyCalculationResult, DifficultyRequest
from perfcho.modules.scoring.models import Ruleset


class SqlAlchemyDifficultyRepository:
    """Read and write difficulty attributes without exposing ORM entities."""

    def __init__(self, session: AsyncSession) -> None:
        """Bind operations to one caller-owned session."""
        self._session = session

    async def active_difficulty_release(self, ruleset: Ruleset) -> dict[str, object] | None:
        """Return the newest active difficulty release metadata for one ruleset."""
        row = (
            await self._session.execute(
                select(
                    CalculationFormula.code.label("formula_code"),
                    CalculationFormula.calculator,
                    CalculationRelease.id.label("release_id"),
                    CalculationRelease.version,
                )
                .join(CalculationRelease, CalculationRelease.formula_id == CalculationFormula.id)
                .where(
                    CalculationFormula.kind == CalculationKind.DIFFICULTY,
                    CalculationFormula.enabled.is_(True),
                    CalculationRelease.ruleset == ruleset,
                    CalculationRelease.active.is_(True),
                )
                .order_by(CalculationRelease.created_at.desc())
                .limit(1)
            )
        ).one_or_none()
        if row is None:
            return None
        return {
            "formula_code": row.formula_code,
            "calculator": row.calculator,
            "release_id": row.release_id,
            "version": row.version,
        }

    async def get(
        self,
        *,
        beatmap_revision_id: int,
        ruleset: Ruleset,
        mods_digest: bytes,
        release_id: uuid.UUID,
    ) -> DifficultyCalculationResult | None:
        """Return a persisted difficulty result, or None when absent."""
        row = (
            await self._session.execute(
                select(
                    BeatmapDifficultyAttribute.star_rating,
                    BeatmapDifficultyAttribute.max_combo,
                    BeatmapDifficultyAttribute.attributes,
                ).where(
                    BeatmapDifficultyAttribute.beatmap_revision_id == beatmap_revision_id,
                    BeatmapDifficultyAttribute.ruleset == ruleset,
                    BeatmapDifficultyAttribute.mods_digest == mods_digest,
                    BeatmapDifficultyAttribute.release_id == release_id,
                )
            )
        ).one_or_none()
        if row is None:
            return None
        return DifficultyCalculationResult(
            star_rating=row.star_rating,
            max_combo=row.max_combo,
            attributes=row.attributes,
        )

    async def put(self, request: DifficultyRequest, result: DifficultyCalculationResult) -> None:
        """Idempotently persist one deterministic difficulty result."""
        await self._session.execute(
            insert(BeatmapDifficultyAttribute)
            .values(
                beatmap_revision_id=request.beatmap_revision_id,
                ruleset=request.ruleset,
                mods_digest=request.mods_digest,
                release_id=request.difficulty_release_id,
                star_rating=result.star_rating,
                max_combo=result.max_combo,
                attributes=dict(result.attributes),
            )
            .on_conflict_do_nothing(
                index_elements=(
                    BeatmapDifficultyAttribute.beatmap_revision_id,
                    BeatmapDifficultyAttribute.ruleset,
                    BeatmapDifficultyAttribute.mods_digest,
                    BeatmapDifficultyAttribute.release_id,
                )
            )
        )


def _decimal(value: object) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value))
