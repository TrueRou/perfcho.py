"""Materialize and persist Performance projection results."""

from __future__ import annotations

import hashlib
from dataclasses import replace
from typing import cast

import orjson
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from perfcho.infra.db.enums import CalculationKind
from perfcho.infra.db.models.content import BeatmapRevision
from perfcho.infra.db.models.core import MediaAsset
from perfcho.infra.db.models.scoring import (
    BeatmapDifficultyAttribute,
    CalculationFormula,
    CalculationFormulaScoreboard,
    CalculationRelease,
    ModSet,
    PlayAttempt,
    Score,
    Scoreboard,
    ScoreHitStatistic,
    ScorePerformance,
)
from perfcho.modules.performance.errors import PerformanceCalculationError
from perfcho.modules.performance.models import (
    PerformanceCalculationInput,
    PerformanceCompletion,
    PerformanceResult,
    thaw_json_mapping,
)
from perfcho.modules.scoring.models import (
    CanonicalMod,
    ClientFamily,
    HitStatistic,
    Ruleset,
    ScoreboardInfo,
    ScoreboardVariant,
    ScoreGrade,
    ScoreOutcome,
    ScoreSubmission,
)


class SqlAlchemyPerformanceProjectionRepository:
    """Read immutable calculation inputs and persist idempotent outputs."""

    def __init__(self, session: AsyncSession) -> None:
        """Bind projection operations to one caller-owned session."""
        self._session = session

    async def materialize(self, score_id: int) -> tuple[PerformanceCalculationInput, ...]:
        """Build one immutable input for every active release matching a score."""
        difficulty_release = aliased(CalculationRelease)
        difficulty_formula = aliased(CalculationFormula)
        rows = (
            await self._session.execute(
                select(
                    Score,
                    PlayAttempt.protocol,
                    BeatmapRevision.sha256,
                    MediaAsset.storage_key,
                    Scoreboard.id.label("scoreboard_id"),
                    Scoreboard.code.label("scoreboard_code"),
                    Scoreboard.ruleset,
                    Scoreboard.variant,
                    ModSet.canonical,
                    CalculationRelease.id.label("release_id"),
                    CalculationRelease.formula_id,
                    CalculationRelease.ruleset.label("release_ruleset"),
                    CalculationRelease.version,
                    CalculationRelease.configuration,
                    CalculationRelease.difficulty_release_id,
                    CalculationFormula.code.label("formula_code"),
                    CalculationFormula.calculator,
                    CalculationFormula.kind,
                    difficulty_release.formula_id.label("difficulty_formula_id"),
                    difficulty_release.ruleset.label("difficulty_ruleset"),
                    difficulty_release.version.label("difficulty_release_version"),
                    difficulty_release.configuration.label("difficulty_release_configuration"),
                    difficulty_formula.code.label("difficulty_formula_code"),
                    difficulty_formula.calculator.label("difficulty_calculator"),
                    difficulty_formula.kind.label("difficulty_kind"),
                )
                .join(PlayAttempt, PlayAttempt.id == Score.attempt_id)
                .join(BeatmapRevision, BeatmapRevision.id == Score.beatmap_revision_id)
                .join(MediaAsset, MediaAsset.id == BeatmapRevision.file_asset_id)
                .join(Scoreboard, Scoreboard.id == Score.scoreboard_id)
                .join(ModSet, ModSet.id == Score.mod_set_id)
                .join(CalculationFormulaScoreboard, CalculationFormulaScoreboard.scoreboard_id == Score.scoreboard_id)
                .join(CalculationFormula, CalculationFormula.id == CalculationFormulaScoreboard.formula_id)
                .join(CalculationRelease, CalculationRelease.formula_id == CalculationFormula.id)
                .join(difficulty_release, difficulty_release.id == CalculationRelease.difficulty_release_id)
                .join(difficulty_formula, difficulty_formula.id == difficulty_release.formula_id)
                .where(
                    Score.id == score_id,
                    CalculationFormula.kind == CalculationKind.PERFORMANCE,
                    CalculationFormula.enabled.is_(True),
                    CalculationRelease.active.is_(True),
                    CalculationRelease.difficulty_release_id.is_not(None),
                    CalculationRelease.ruleset == Scoreboard.ruleset,
                )
                .order_by(CalculationFormula.code, CalculationRelease.created_at)
            )
        ).all()
        if not rows:
            score_exists = await self._session.scalar(select(Score.id).where(Score.id == score_id))
            if score_exists is None:
                raise RuntimeError("performance projection references a missing score")
            return ()

        hits = tuple(
            HitStatistic(hit_result, actual, maximum)
            for hit_result, actual, maximum in (
                await self._session.execute(
                    select(
                        ScoreHitStatistic.hit_result,
                        ScoreHitStatistic.actual,
                        ScoreHitStatistic.maximum,
                    )
                    .where(ScoreHitStatistic.score_id == score_id)
                    .order_by(ScoreHitStatistic.hit_result)
                )
            ).all()
        )
        calculations: list[PerformanceCalculationInput] = []
        for row in rows:
            if (
                row.kind is not CalculationKind.PERFORMANCE
                or row.difficulty_release_id is None
                or row.ruleset != row.release_ruleset
                or row.difficulty_kind is not CalculationKind.DIFFICULTY
                or row.difficulty_ruleset != row.ruleset
                or row.difficulty_calculator != row.calculator
            ):
                raise RuntimeError("performance calculation release is incompatible with the score")
            score = cast(Score, row[0])
            calculation = PerformanceCalculationInput(
                score_id=score.id,
                account_id=score.account_id,
                formula_id=row.formula_id,
                formula_code=row.formula_code,
                calculator=row.calculator,
                release_id=row.release_id,
                release_version=row.version,
                release_configuration=row.configuration,
                difficulty_formula_id=row.difficulty_formula_id,
                difficulty_formula_code=row.difficulty_formula_code,
                difficulty_release_id=row.difficulty_release_id,
                difficulty_release_version=row.difficulty_release_version,
                difficulty_release_configuration=row.difficulty_release_configuration,
                input_digest=bytes(32),
                beatmap_revision_id=score.beatmap_revision_id,
                beatmap_sha256=row.sha256,
                beatmap_storage_key=row.storage_key,
                scoreboard=ScoreboardInfo(
                    row.scoreboard_id,
                    row.scoreboard_code,
                    Ruleset(row.ruleset.value),
                    ScoreboardVariant(row.variant.value),
                ),
                mod_set_id=score.mod_set_id,
                mods=_canonical_mods(row.canonical),
                client_family=ClientFamily(row.protocol.value),
                score=ScoreSubmission(
                    total_score=score.total_score,
                    classic_score=score.classic_score,
                    accuracy=score.accuracy,
                    max_combo=score.max_combo,
                    grade=ScoreGrade(score.grade.value),
                    outcome=ScoreOutcome(score.outcome.value),
                    perfect=score.perfect,
                    hits=hits,
                    client_flags=score.client_flags,
                    online_checksum=score.online_checksum,
                ),
            )
            calculations.append(replace(calculation, input_digest=_canonical_digest(calculation.digest_payload())))
        return tuple(calculations)

    async def complete(
        self,
        calculation: PerformanceCalculationInput,
        result: PerformanceResult,
        *,
        output_digest: bytes,
    ) -> PerformanceCompletion:
        """Persist one deterministic release result and reject inconsistent replays."""
        difficulty_id = await self._session.scalar(
            insert(BeatmapDifficultyAttribute)
            .values(
                beatmap_revision_id=calculation.beatmap_revision_id,
                scoreboard_id=calculation.scoreboard.scoreboard_id,
                mod_set_id=calculation.mod_set_id,
                release_id=calculation.difficulty_release_id,
                star_rating=result.difficulty.star_rating,
                max_combo=result.difficulty.max_combo,
                attributes=thaw_json_mapping(result.difficulty.attributes),
            )
            .on_conflict_do_nothing(
                index_elements=(
                    BeatmapDifficultyAttribute.beatmap_revision_id,
                    BeatmapDifficultyAttribute.scoreboard_id,
                    BeatmapDifficultyAttribute.mod_set_id,
                    BeatmapDifficultyAttribute.release_id,
                )
            )
            .returning(BeatmapDifficultyAttribute.id)
        )
        if difficulty_id is None:
            difficulty = (
                await self._session.execute(
                    select(BeatmapDifficultyAttribute).where(
                        BeatmapDifficultyAttribute.beatmap_revision_id == calculation.beatmap_revision_id,
                        BeatmapDifficultyAttribute.scoreboard_id == calculation.scoreboard.scoreboard_id,
                        BeatmapDifficultyAttribute.mod_set_id == calculation.mod_set_id,
                        BeatmapDifficultyAttribute.release_id == calculation.difficulty_release_id,
                    )
                )
            ).scalar_one()
            expected = (
                result.difficulty.star_rating,
                result.difficulty.max_combo,
                thaw_json_mapping(result.difficulty.attributes),
            )
            if (difficulty.star_rating, difficulty.max_combo, difficulty.attributes) != expected:
                raise PerformanceCalculationError(
                    "difficulty release produced inconsistent output",
                    retryable=False,
                )
            difficulty_id = difficulty.id

        existing = await self._session.get(
            ScorePerformance,
            {"score_id": calculation.score_id, "release_id": calculation.release_id},
        )
        breakdown = thaw_json_mapping(result.breakdown)
        if existing is None:
            self._session.add(
                ScorePerformance(
                    score_id=calculation.score_id,
                    release_id=calculation.release_id,
                    difficulty_attribute_id=difficulty_id,
                    pp=result.pp,
                    breakdown=breakdown,
                    input_digest=calculation.input_digest,
                    output_digest=output_digest,
                )
            )
        elif (
            existing.difficulty_attribute_id != difficulty_id
            or existing.pp != result.pp
            or existing.breakdown != breakdown
            or existing.input_digest != calculation.input_digest
            or existing.output_digest != output_digest
        ):
            raise PerformanceCalculationError(
                "performance release produced inconsistent output",
                retryable=False,
            )

        return PerformanceCompletion(
            score_id=calculation.score_id,
            account_id=calculation.account_id,
            scoreboard_id=calculation.scoreboard.scoreboard_id,
            formula_id=calculation.formula_id,
            formula_code=calculation.formula_code,
            release_id=calculation.release_id,
            pp=result.pp,
            output_digest=output_digest,
        )


def _canonical_mods(value: object) -> tuple[CanonicalMod, ...]:
    if not isinstance(value, list):
        raise RuntimeError("persisted mod set is not a JSON array")
    mods: list[CanonicalMod] = []
    for item in value:
        if not isinstance(item, dict) or not isinstance(item.get("acronym"), str):
            raise RuntimeError("persisted mod set contains an invalid mod")
        settings = item.get("settings", {})
        if not isinstance(settings, dict):
            raise RuntimeError("persisted mod settings are not a JSON object")
        mods.append(CanonicalMod(item["acronym"], settings))
    return tuple(mods)


def _canonical_digest(value: object) -> bytes:
    encoded = orjson.dumps(value, option=orjson.OPT_SORT_KEYS)
    return hashlib.sha256(encoded).digest()
