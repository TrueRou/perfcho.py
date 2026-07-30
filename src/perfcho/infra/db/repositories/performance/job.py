"""Persist phased, fenced multi-formula Performance job execution."""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import replace
from datetime import datetime, timedelta
from typing import cast

from sqlalchemy import and_, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from perfcho.infra.db.enums import CalculationJobStatus, CalculationKind
from perfcho.infra.db.models.content import BeatmapRevision
from perfcho.infra.db.models.core import MediaAsset
from perfcho.infra.db.models.scoring import (
    BeatmapDifficultyAttribute,
    CalculationFormula,
    CalculationFormulaScoreboard,
    CalculationRelease,
    ModSet,
    PerformanceCalculationJob,
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


class SqlAlchemyPerformanceJobRepository:
    """Execute phased Performance jobs through caller-owned transactions."""

    def __init__(self, session: AsyncSession, *, execution_lease_seconds: int) -> None:
        """Bind job operations to one caller-owned session."""
        if execution_lease_seconds < 1:
            raise ValueError("execution_lease_seconds must be positive")
        self._session = session
        self._execution_lease_duration = timedelta(seconds=execution_lease_seconds)

    async def start(
        self,
        job_id: uuid.UUID,
        lease_token: uuid.UUID,
    ) -> PerformanceCalculationInput | None:
        """Start one execution attempt and materialize immutable engine input."""
        job = await self._session.get(PerformanceCalculationJob, job_id, with_for_update=True)
        now = await _database_now(self._session)
        if (
            job is None
            or job.status is not CalculationJobStatus.RUNNING
            or job.lease_token != lease_token
            or job.lease_expires_at is None
            or job.lease_expires_at <= now
            or job.attempt_started_at is not None
            or job.completed_at is not None
            or job.dead_lettered_at is not None
        ):
            return None
        job.attempt_count += 1
        job.attempt_started_at = now
        job.lease_expires_at = now + self._execution_lease_duration

        difficulty_release = aliased(CalculationRelease)
        difficulty_formula = aliased(CalculationFormula)
        row = (
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
                    CalculationRelease.formula_id,
                    CalculationRelease.ruleset.label("release_ruleset"),
                    CalculationRelease.version,
                    CalculationRelease.artifact_digest,
                    CalculationRelease.configuration,
                    CalculationRelease.difficulty_release_id,
                    CalculationFormula.code.label("formula_code"),
                    CalculationFormula.calculator,
                    CalculationFormula.kind,
                    CalculationFormulaScoreboard.scoreboard_id.label("formula_scoreboard_id"),
                    difficulty_release.formula_id.label("difficulty_formula_id"),
                    difficulty_release.ruleset.label("difficulty_ruleset"),
                    difficulty_release.version.label("difficulty_release_version"),
                    difficulty_release.artifact_digest.label("difficulty_artifact_digest"),
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
                .join(CalculationRelease, CalculationRelease.id == job.release_id)
                .join(CalculationFormula, CalculationFormula.id == CalculationRelease.formula_id)
                .join(difficulty_release, difficulty_release.id == CalculationRelease.difficulty_release_id)
                .join(difficulty_formula, difficulty_formula.id == difficulty_release.formula_id)
                .join(
                    CalculationFormulaScoreboard,
                    and_(
                        CalculationFormulaScoreboard.formula_id == CalculationFormula.id,
                        CalculationFormulaScoreboard.scoreboard_id == Score.scoreboard_id,
                    ),
                )
                .where(Score.id == job.score_id)
            )
        ).one_or_none()
        if row is None:
            raise RuntimeError("performance calculation references incomplete score or beatmap facts")
        if (
            row.kind is not CalculationKind.PERFORMANCE
            or row.formula_scoreboard_id != row.scoreboard_id
            or row.difficulty_release_id is None
            or row.ruleset != row.release_ruleset
            or row.difficulty_kind is not CalculationKind.DIFFICULTY
            or row.difficulty_ruleset != row.ruleset
            or row.difficulty_calculator != row.calculator
        ):
            raise RuntimeError("performance calculation release is incompatible with the score")

        hits = tuple(
            HitStatistic(hit_result, actual, maximum)
            for hit_result, actual, maximum in (
                await self._session.execute(
                    select(
                        ScoreHitStatistic.hit_result,
                        ScoreHitStatistic.actual,
                        ScoreHitStatistic.maximum,
                    )
                    .where(ScoreHitStatistic.score_id == job.score_id)
                    .order_by(ScoreHitStatistic.hit_result)
                )
            ).all()
        )
        score = cast(Score, row[0])
        calculation = PerformanceCalculationInput(
            job_id=job.id,
            score_id=score.id,
            attempt_count=job.attempt_count,
            formula_id=row.formula_id,
            formula_code=row.formula_code,
            calculator=row.calculator,
            release_id=job.release_id,
            release_version=row.version,
            artifact_digest=row.artifact_digest,
            release_configuration=row.configuration,
            difficulty_formula_id=row.difficulty_formula_id,
            difficulty_formula_code=row.difficulty_formula_code,
            difficulty_release_id=row.difficulty_release_id,
            difficulty_release_version=row.difficulty_release_version,
            difficulty_artifact_digest=row.difficulty_artifact_digest,
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
        input_digest = _canonical_digest(calculation.digest_payload())
        if job.input_digest is not None and job.input_digest != input_digest:
            raise PerformanceCalculationError(
                "performance calculation input changed after the first attempt",
                retryable=False,
            )
        job.input_digest = input_digest
        job.last_error = None
        return replace(calculation, input_digest=input_digest)

    async def complete(
        self,
        calculation: PerformanceCalculationInput,
        lease_token: uuid.UUID,
        result: PerformanceResult,
        *,
        output_digest: bytes,
    ) -> PerformanceCompletion | None:
        """Persist reproducible difficulty and PP output under the current fence."""
        job = await self._session.get(PerformanceCalculationJob, calculation.job_id, with_for_update=True)
        now = await _database_now(self._session)
        if (
            job is None
            or job.status is not CalculationJobStatus.RUNNING
            or job.lease_token != lease_token
            or job.lease_expires_at is None
            or job.lease_expires_at <= now
            or job.attempt_started_at is None
            or job.input_digest != calculation.input_digest
        ):
            return None
        if job.score_id != calculation.score_id or job.release_id != calculation.release_id:
            raise PerformanceCalculationError(
                "performance completion dimensions do not match the leased job",
                retryable=False,
            )

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
            actual = (difficulty.star_rating, difficulty.max_combo, difficulty.attributes)
            if actual != expected:
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

        job.status = CalculationJobStatus.SUCCEEDED
        job.output_digest = output_digest
        job.completed_at = now
        _clear_job_lease(job)
        job.last_error = None
        return PerformanceCompletion(
            score_id=calculation.score_id,
            scoreboard_id=calculation.scoreboard.scoreboard_id,
            formula_id=calculation.formula_id,
            formula_code=calculation.formula_code,
            release_id=calculation.release_id,
            pp=result.pp,
            output_digest=output_digest,
        )

    async def fail(
        self,
        job_id: uuid.UUID,
        lease_token: uuid.UUID,
        *,
        error: str,
        retry_delay: timedelta,
        dead: bool,
        consume_attempt: bool,
    ) -> bool:
        """Release or dead-letter a failed job only under its current fence."""
        job = await self._session.get(PerformanceCalculationJob, job_id, with_for_update=True)
        now = await _database_now(self._session)
        if (
            job is None
            or job.status is not CalculationJobStatus.RUNNING
            or job.lease_token != lease_token
            or job.lease_expires_at is None
            or job.lease_expires_at <= now
        ):
            return False
        if consume_attempt:
            job.attempt_count += 1
        job.status = CalculationJobStatus.DEAD if dead else CalculationJobStatus.PENDING
        job.available_at = now + retry_delay
        job.dead_lettered_at = now if dead else None
        job.enqueued_at = None
        job.broker_task_id = None
        if not dead:
            job.attempt_started_at = None
        _clear_job_lease(job)
        job.last_error = error[:4000]
        return True


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
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    return hashlib.sha256(encoded).digest()


async def _database_now(session: AsyncSession) -> datetime:
    now = await session.scalar(select(func.clock_timestamp()))
    if now is None:
        raise RuntimeError("PostgreSQL did not return a Performance job timestamp")
    return now


def _clear_job_lease(job: PerformanceCalculationJob) -> None:
    job.lease_owner = None
    job.lease_token = None
    job.lease_expires_at = None
