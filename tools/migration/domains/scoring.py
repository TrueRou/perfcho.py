"""Migrate legacy scores, replays, statistics, and ranking projections."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra.db.enums import AttemptStatus, CalculationKind, ClientFamily, Ruleset, ScoreGrade, ScoreOutcome
from perfcho.infra.db.models.content import Beatmap, BeatmapRevision, Comment
from perfcho.infra.db.models.events import OutboxEvent
from perfcho.infra.db.models.scoring import (
    BeatmapDifficultyAttribute,
    CalculationFormula,
    CalculationFormulaScoreboard,
    CalculationRelease,
    ModSet,
    PlayAttempt,
    RankingPolicy,
    Replay,
    Score,
    ScoreAttestation,
    ScoreHitStatistic,
    ScorePerformance,
    UserPlayStat,
    UserRankedStat,
)
from perfcho.infra.db.projectors.ranking import project_accepted_score
from perfcho.infra.db.projectors.scoring_stats import project_scoring_stats
from tools.migration.domains.common import complete_phase, run_batched_phase, run_single_phase
from tools.migration.models import DiagnosticSeverity, MigrationRuntime, SourceRow
from tools.migration.observability import PhaseObserver
from tools.migration.state import MigrationCheckpoint, next_checkpoint
from tools.migration.storage import (
    MigrationStorageError,
    ObjectUploadFailed,
    ReplayFileMetadata,
    read_replay_file,
    upload_replay_file,
)
from tools.migration.transforms import (
    aware_datetime,
    bounded_integer,
    canonical_json_digest,
    decimal_value,
    mod_set,
    normalized_accuracy,
    score_grade,
    scoreboard,
)

_PHASE_RELEASES = "scoring.legacy_releases"
_PHASE_DIFFICULTY = "scoring.difficulty"
_PHASE_SCORES = "scoring.scores"
_PHASE_COMMENTS = "scoring.comments"
_PHASE_RANKING = "scoring.ranking"
_PHASE_STATS = "scoring.stats"
_LEGACY_ENGINE = "bancho.py"


@dataclass(frozen=True, slots=True)
class _StagedReplay:
    metadata: ReplayFileMetadata
    storage_key: str
    size_bytes: int
    sha256: bytes


@dataclass(frozen=True, slots=True)
class _PreparedScore:
    source_id: int
    account_id: int
    beatmap_id: int
    revision_id: int
    total_length_ms: int
    scoreboard_id: int
    mod_set_id: int
    total_score: int
    accuracy: Decimal
    max_combo: int
    grade: ScoreGrade
    outcome: ScoreOutcome
    perfect: bool
    client_flags: int
    online_checksum: bytes | None
    attestation_checksum: bytes | None
    started_at: datetime
    ended_at: datetime
    legacy_pp: Decimal
    hits: tuple[tuple[str, int], ...]
    replay: _StagedReplay | None


async def migrate_scoring(runtime: MigrationRuntime) -> None:
    """Migrate score facts and rebuild canonical target-owned ranking projections."""
    if runtime.object_storage is None:
        raise RuntimeError("scoring migration requires an injected ObjectStorage")
    await _migrate_releases(runtime)
    await _reconstruct_score_mappings(runtime)
    await _migrate_difficulty(runtime)
    await _migrate_scores(runtime)
    await _reconstruct_score_mappings(runtime)
    await _migrate_replay_comments(runtime)
    await _rebuild_rankings(runtime)
    await _migrate_stats(runtime)


async def _migrate_releases(runtime: MigrationRuntime) -> None:
    async def handler(session: AsyncSession) -> None:
        proposed_formulas = {
            CalculationKind.DIFFICULTY: runtime.ids.make("calculation-formula", "difficulty"),
            CalculationKind.PERFORMANCE: runtime.ids.make("calculation-formula", "performance"),
        }
        await session.execute(
            insert(CalculationFormula)
            .values(
                [
                    {
                        "id": formula_id,
                        "code": f"legacy-bancho-{kind.value}",
                        "name": f"Legacy bancho.py {kind.value}",
                        "kind": kind,
                        "calculator": "legacy_import",
                        "description": "Read-only provenance for values imported from bancho.py v5.2.2.",
                        "enabled": False,
                        "created_at": runtime.started_at,
                    }
                    for kind, formula_id in proposed_formulas.items()
                ]
            )
            .on_conflict_do_nothing(index_elements=(CalculationFormula.code,))
        )
        formula_rows = (
            await session.execute(
                select(CalculationFormula.kind, CalculationFormula.id).where(
                    CalculationFormula.code.in_(("legacy-bancho-difficulty", "legacy-bancho-performance"))
                )
            )
        ).all()
        formulas = dict(formula_rows)
        if set(formulas) != {CalculationKind.DIFFICULTY, CalculationKind.PERFORMANCE}:
            raise RuntimeError("legacy calculation formula merge is incomplete")
        await session.execute(
            insert(CalculationFormulaScoreboard)
            .values(
                [
                    {"formula_id": formula_id, "scoreboard_id": scoreboard_id}
                    for formula_id in formulas.values()
                    for scoreboard_id in range(1, 9)
                ]
            )
            .on_conflict_do_nothing()
        )
        for ruleset in ("osu", "taiko", "fruits", "mania"):
            difficulty_release_id = await _ensure_release(
                session,
                runtime,
                kind=CalculationKind.DIFFICULTY,
                formula_id=formulas[CalculationKind.DIFFICULTY],
                ruleset=Ruleset(ruleset),
                difficulty_release_id=None,
            )
            runtime.mappings.calculation_releases[(CalculationKind.DIFFICULTY.value, ruleset)] = difficulty_release_id
            performance_release_id = await _ensure_release(
                session,
                runtime,
                kind=CalculationKind.PERFORMANCE,
                formula_id=formulas[CalculationKind.PERFORMANCE],
                ruleset=Ruleset(ruleset),
                difficulty_release_id=difficulty_release_id,
            )
            runtime.mappings.calculation_releases[(CalculationKind.PERFORMANCE.value, ruleset)] = performance_release_id
        runtime.report.increment(_PHASE_RELEASES, "merged", 8)

    await run_single_phase(runtime, phase=_PHASE_RELEASES, handler=handler)
    async with runtime.session_factory() as session:
        releases = (
            await session.execute(
                select(CalculationFormula.kind, CalculationRelease.ruleset, CalculationRelease.id)
                .join(CalculationRelease, CalculationRelease.formula_id == CalculationFormula.id)
                .where(CalculationFormula.code.in_(("legacy-bancho-difficulty", "legacy-bancho-performance")))
            )
        ).all()
    for kind, ruleset, release_id in releases:
        runtime.mappings.calculation_releases[(kind.value, ruleset.value)] = release_id


async def _migrate_difficulty(runtime: MigrationRuntime) -> None:
    async def handler(session: AsyncSession, rows: list[SourceRow]) -> None:
        for row in rows:
            source_map_id = row.get("id")
            try:
                map_id = _positive(source_map_id, "map id")
                md5 = _md5(row.get("md5")).hex()
                revision_id = runtime.mappings.revisions_by_md5[md5]
                scoreboard_id, ruleset, _ = scoreboard(row.get("mode"))
                no_mod_id = await _ensure_mod_set(session, scoreboard_id, [], canonical_json_digest([]), 0)
                release_id = _release_id(runtime, CalculationKind.DIFFICULTY, ruleset)
                stars = decimal_value(row.get("diff"), "star rating")
                if stars < 0:
                    raise ValueError("star rating must not be negative")
                max_combo = bounded_integer(row.get("max_combo"), "max combo", minimum=0, maximum=2_147_483_647)
                statement = insert(BeatmapDifficultyAttribute).values(
                    beatmap_revision_id=revision_id,
                    scoreboard_id=scoreboard_id,
                    mod_set_id=no_mod_id,
                    release_id=release_id,
                    star_rating=stars,
                    max_combo=max_combo,
                    attributes={"migration_id": runtime.config.migration_id, "source_map_id": map_id},
                    created_at=runtime.started_at,
                )
                await session.execute(statement.on_conflict_do_nothing())
                runtime.report.increment(_PHASE_DIFFICULTY, "merged")
            except (KeyError, TypeError, ValueError) as error:
                _warning(runtime, _PHASE_DIFFICULTY, "difficulty_skipped", error, "maps", source_map_id)

    await run_batched_phase(
        runtime,
        phase=_PHASE_DIFFICULTY,
        table="maps",
        key="id",
        columns=("id", "md5", "mode", "diff", "max_combo"),
        handler=handler,
    )


async def _migrate_scores(runtime: MigrationRuntime) -> None:
    with PhaseObserver(runtime, _PHASE_SCORES) as observer:
        checkpoint = await _checkpoint(runtime)
        if _PHASE_SCORES in checkpoint.completed_phases:
            observer.skipped()
            return
        cursor = checkpoint.cursor if checkpoint.phase == _PHASE_SCORES else 0
        object_storage = runtime.object_storage
        assert object_storage is not None
        for rows in runtime.source.iter_batches(
            "scores",
            key="id",
            batch_size=runtime.config.batch_size,
            start_after=cursor,
        ):
            snapshot = runtime.report.snapshot()
            staged_replays: dict[int, _StagedReplay] = {}
            for row in rows:
                source_id = row.get("id")
                try:
                    score_id = _positive(source_id, "score id")
                    if bounded_integer(row.get("status"), "score status", minimum=0, maximum=2) == 0:
                        continue
                    metadata = read_replay_file(runtime.config.data_directory, score_id)
                    account_id = runtime.mappings.accounts[_positive(row.get("userid"), "score user id")]
                    stored = await upload_replay_file(
                        object_storage,
                        metadata,
                        account_id=account_id,
                        invocation_id=runtime.report.invocation_id,
                        migration_id=runtime.config.migration_id,
                    )
                    digest = stored.sha256
                    if digest is None:
                        raise ValueError("uploaded replay has no SHA-256 digest")
                    staged_replays[score_id] = _StagedReplay(metadata, stored.storage_key, stored.size_bytes, digest)
                except ObjectUploadFailed:
                    runtime.report.restore(snapshot)
                    raise
                except MigrationStorageError as error:
                    runtime.report.add(
                        DiagnosticSeverity.WARNING,
                        "replay_unavailable",
                        str(error),
                        entity="scores",
                        source_id=source_id,
                    )
                    runtime.report.increment(_PHASE_SCORES, "replay_skipped")
                except KeyError, TypeError, ValueError:
                    # The score handler emits the authoritative dependency diagnostic.
                    continue
            try:
                async with runtime.session_factory.begin() as session:
                    for row in rows:
                        source_id = row.get("id")
                        try:
                            prepared = await _prepare_score(session, runtime, row, staged_replays)
                            target_score_id, created = await _persist_score(session, runtime, prepared)
                            runtime.mappings.scores[prepared.source_id] = target_score_id
                            runtime.report.increment(
                                _PHASE_SCORES,
                                "inserted" if created else "target_reused",
                            )
                        except (KeyError, TypeError, ValueError) as error:
                            _warning(runtime, _PHASE_SCORES, "score_skipped", error, "scores", source_id)
                    cursor = int(rows[-1]["id"])
                    checkpoint = next_checkpoint(checkpoint, phase=_PHASE_SCORES, cursor=cursor)
                    await runtime.state.save(session, checkpoint)
            except BaseException:
                runtime.report.restore(snapshot)
                raise
            observer.batch_committed(len(rows))
            runtime.report.write(runtime.config.report_path)
        await complete_phase(runtime, checkpoint, _PHASE_SCORES)


async def _prepare_score(
    session: AsyncSession,
    runtime: MigrationRuntime,
    row: SourceRow,
    staged_replays: dict[int, _StagedReplay],
) -> _PreparedScore:
    source_id = _positive(row.get("id"), "score id")
    account_id = runtime.mappings.accounts[_positive(row.get("userid"), "score user id")]
    md5 = _md5(row.get("map_md5"))
    revision = (
        await session.execute(
            select(
                BeatmapRevision.id,
                BeatmapRevision.beatmap_id,
                BeatmapRevision.total_length_ms,
            ).where(BeatmapRevision.md5 == md5)
        )
    ).one_or_none()
    if revision is None:
        raise ValueError("score beatmap revision was not migrated")
    scoreboard_id, canonical, canonical_digest, legacy_bits = mod_set(row.get("mode"), row.get("mods"))
    mod_set_id = await _ensure_mod_set(session, scoreboard_id, canonical, canonical_digest, legacy_bits)
    status = bounded_integer(row.get("status"), "score status", minimum=0, maximum=2)
    passed = status in {1, 2}
    outcome = ScoreOutcome.PASSED if passed else ScoreOutcome.FAILED
    ended_at = aware_datetime(row.get("play_time"), runtime.config.source_timezone, fallback=runtime.started_at)
    elapsed_ms = bounded_integer(row.get("time_elapsed"), "elapsed time", minimum=0, maximum=86_400_000)
    started_at = ended_at - timedelta(milliseconds=elapsed_ms)
    total_score = bounded_integer(row.get("score"), "score", minimum=0, maximum=2_147_483_647)
    legacy_pp = decimal_value(row.get("pp"), "performance")
    if legacy_pp < 0:
        raise ValueError("performance must not be negative")
    ruleset = await session.scalar(select(Beatmap.ruleset).where(Beatmap.id == revision.beatmap_id))
    if ruleset is None:
        raise RuntimeError("score beatmap ruleset is unavailable")
    hits = _hit_statistics(row, ruleset)
    attestation_checksum = _optional_md5(row.get("online_checksum"))
    checksum = attestation_checksum
    if (
        checksum is not None
        and await session.scalar(select(Score.id).where(Score.online_checksum == checksum)) is not None
    ):
        runtime.report.add(
            DiagnosticSeverity.WARNING,
            "score_checksum_duplicate",
            "legacy online checksum already exists in the target and was retained only as attestation evidence",
            entity="scores",
            source_id=source_id,
        )
        checksum = None
    return _PreparedScore(
        source_id=source_id,
        account_id=account_id,
        beatmap_id=revision.beatmap_id,
        revision_id=revision.id,
        total_length_ms=revision.total_length_ms,
        scoreboard_id=scoreboard_id,
        mod_set_id=mod_set_id,
        total_score=total_score,
        accuracy=normalized_accuracy(row.get("acc")),
        max_combo=bounded_integer(row.get("max_combo"), "max combo", minimum=0, maximum=2_147_483_647),
        grade=score_grade(row.get("grade"), passed=passed),
        outcome=outcome,
        perfect=_legacy_bool(row.get("perfect"), "perfect"),
        client_flags=bounded_integer(row.get("client_flags"), "client flags", minimum=0, maximum=2_147_483_647),
        online_checksum=checksum,
        attestation_checksum=attestation_checksum,
        started_at=started_at,
        ended_at=ended_at,
        legacy_pp=legacy_pp,
        hits=hits,
        replay=staged_replays.get(source_id),
    )


async def _persist_score(
    session: AsyncSession,
    runtime: MigrationRuntime,
    item: _PreparedScore,
) -> tuple[int, bool]:
    attempt_id = runtime.ids.make("play-attempt", item.source_id)
    existing_score_id = await session.scalar(select(Score.id).where(Score.attempt_id == attempt_id))
    created = existing_score_id is None
    if existing_score_id is None:
        elapsed = max(0, int((item.ended_at - item.started_at).total_seconds() * 1000))
        progress = (
            Decimal(1)
            if item.outcome is ScoreOutcome.PASSED
            else min(Decimal(1), Decimal(elapsed) / Decimal(max(1, item.total_length_ms)))
        )
        await session.execute(
            insert(PlayAttempt)
            .values(
                id=attempt_id,
                account_id=item.account_id,
                beatmap_id=item.beatmap_id,
                beatmap_revision_id=item.revision_id,
                scoreboard_id=item.scoreboard_id,
                mod_set_id=item.mod_set_id,
                protocol=ClientFamily.STABLE,
                idempotency_key=f"bancho:{runtime.config.migration_id}:score:{item.source_id}",
                status=AttemptStatus.VERIFIED,
                started_at=item.started_at,
                ended_at=item.ended_at,
                outcome=item.outcome,
                progress=progress,
                client_metadata={"migration_id": runtime.config.migration_id, "source_score_id": item.source_id},
                created_at=item.ended_at,
            )
            .on_conflict_do_nothing(index_elements=(PlayAttempt.id,))
        )
        existing_score_id = await session.scalar(
            insert(Score)
            .values(
                attempt_id=attempt_id,
                account_id=item.account_id,
                beatmap_id=item.beatmap_id,
                beatmap_revision_id=item.revision_id,
                scoreboard_id=item.scoreboard_id,
                mod_set_id=item.mod_set_id,
                total_score=item.total_score,
                classic_score=item.total_score,
                accuracy=item.accuracy,
                max_combo=item.max_combo,
                grade=item.grade,
                outcome=item.outcome,
                perfect=item.perfect,
                client_flags=item.client_flags,
                online_checksum=item.online_checksum,
                started_at=item.started_at,
                ended_at=item.ended_at,
                processed_at=item.ended_at,
                created_at=item.ended_at,
            )
            .on_conflict_do_nothing(index_elements=(Score.attempt_id,))
            .returning(Score.id)
        )
        if existing_score_id is None:
            existing_score_id = await session.scalar(select(Score.id).where(Score.attempt_id == attempt_id))
    if existing_score_id is None:
        raise RuntimeError("score merge did not resolve a target identity")
    score_id = int(existing_score_id)
    await session.execute(
        insert(ScoreHitStatistic)
        .values([{"score_id": score_id, "hit_result": result, "actual": count} for result, count in item.hits])
        .on_conflict_do_nothing()
    )
    ruleset = await session.scalar(select(Beatmap.ruleset).where(Beatmap.id == item.beatmap_id))
    if ruleset is None:
        raise RuntimeError("score beatmap disappeared during migration")
    release_id = _release_id(runtime, CalculationKind.PERFORMANCE, ruleset)
    difficulty_attribute_id = await _ensure_score_difficulty_attribute(session, runtime, item, ruleset)
    input_digest = canonical_json_digest(
        {
            "source_score_id": item.source_id,
            "revision_id": item.revision_id,
            "scoreboard_id": item.scoreboard_id,
            "mod_set_id": item.mod_set_id,
            "total_score": item.total_score,
            "accuracy": str(item.accuracy),
            "max_combo": item.max_combo,
            "hits": list(item.hits),
        }
    )
    output_digest = canonical_json_digest({"pp": str(item.legacy_pp)})
    await session.execute(
        insert(ScorePerformance)
        .values(
            score_id=score_id,
            release_id=release_id,
            difficulty_attribute_id=difficulty_attribute_id,
            pp=item.legacy_pp,
            breakdown={"migration_id": runtime.config.migration_id, "source_score_id": item.source_id},
            input_digest=input_digest,
            output_digest=output_digest,
            created_at=item.ended_at,
        )
        .on_conflict_do_nothing()
    )
    await session.execute(
        insert(ScoreAttestation)
        .values(
            score_id=score_id,
            client_family=ClientFamily.STABLE,
            client_version=f"bancho.py-{runtime.source_schema.version or '5.2.2'}",
            client_flags=item.client_flags,
            checksum=item.attestation_checksum,
            verification_state="verified",
            evidence={
                "migration_id": runtime.config.migration_id,
                "source_score_id": item.source_id,
                "verification_method": "legacy_import",
            },
            created_at=item.ended_at,
        )
        .on_conflict_do_nothing(index_elements=(ScoreAttestation.score_id,))
    )
    if item.replay is not None:
        await session.execute(
            insert(Replay)
            .values(
                score_id=score_id,
                format="stable",
                sha256=item.replay.sha256,
                size_bytes=item.replay.size_bytes,
                storage_key=item.replay.storage_key,
                state="ready",
                client_version=None,
                verified_at=item.ended_at,
                created_at=item.ended_at,
            )
            .on_conflict_do_nothing(index_elements=(Replay.score_id,))
        )
    await _ensure_score_event(session, runtime, item, score_id)
    return score_id, created


async def _migrate_replay_comments(runtime: MigrationRuntime) -> None:
    async def handler(session: AsyncSession, rows: list[SourceRow]) -> None:
        for row in rows:
            if str(row.get("target_type", "")).casefold() != "replay":
                continue
            source_id = row.get("id")
            try:
                _positive(source_id, "comment id")
                source_score_id = _positive(row.get("target_id"), "comment score id")
                score_id = runtime.mappings.scores.get(source_score_id)
                if score_id is None:
                    score_id = await _target_score_id(session, runtime, source_score_id)
                if score_id is None:
                    raise ValueError("comment score was not migrated")
                account_id = runtime.mappings.accounts[_positive(row.get("userid"), "comment user id")]
                body = str(row.get("comment", ""))
                if not 1 <= len(body) <= 1000:
                    raise ValueError("comment body length is invalid")
                color = _comment_color(row.get("colour"))
                position = int(decimal_value(row.get("time"), "comment position") * 1000)
                if position < 0:
                    raise ValueError("comment position must not be negative")
                await session.execute(
                    insert(Comment).values(
                        author_account_id=account_id,
                        score_id=score_id,
                        position_ms=position,
                        body=body,
                        color=color,
                        moderation_state="visible",
                        created_at=runtime.started_at,
                    )
                )
                runtime.report.increment(_PHASE_COMMENTS, "inserted")
            except (KeyError, TypeError, ValueError) as error:
                _warning(runtime, _PHASE_COMMENTS, "score_comment_skipped", error, "comments", source_id)

    await run_batched_phase(runtime, phase=_PHASE_COMMENTS, table="comments", key="id", handler=handler)


async def _rebuild_rankings(runtime: MigrationRuntime) -> None:
    async def handler(session: AsyncSession, rows: list[SourceRow]) -> None:
        projected = 0
        for row in rows:
            source_id = _positive(row.get("id"), "score id")
            score_id = runtime.mappings.scores.get(source_id)
            if score_id is None:
                score_id = await _target_score_id(session, runtime, source_id)
            if score_id is None:
                continue
            event = await session.get(OutboxEvent, runtime.ids.make("score-accepted-event", source_id))
            if event is None:
                raise RuntimeError("imported score is missing its outbox event")
            partition_key = f"scoreboard:{event.payload['scoreboard_id']}"
            await project_accepted_score(session, event, partition_key)
            await project_scoring_stats(session, event, partition_key)
            projected += 1
        runtime.report.increment(_PHASE_RANKING, "projected", projected)

    await run_batched_phase(
        runtime,
        phase=_PHASE_RANKING,
        table="scores",
        key="id",
        columns=("id",),
        handler=handler,
    )


async def _migrate_stats(runtime: MigrationRuntime) -> None:
    async def handler(session: AsyncSession, rows: list[SourceRow]) -> None:
        policies = dict(
            (
                await session.execute(
                    select(RankingPolicy.scoreboard_id, RankingPolicy.id).where(
                        RankingPolicy.active.is_(True),
                        RankingPolicy.is_default.is_(True),
                    )
                )
            ).all()
        )
        for row in rows:
            source_id = f"{row.get('id')}:{row.get('mode')}"
            try:
                account_id = runtime.mappings.accounts[_positive(row.get("id"), "stats user id")]
                scoreboard_id, _, _ = scoreboard(row.get("mode"))
                play_statement = insert(UserPlayStat).values(
                    account_id=account_id,
                    scoreboard_id=scoreboard_id,
                    play_count=_nonnegative(row.get("plays"), "plays"),
                    play_time_ms=_nonnegative(row.get("playtime"), "playtime") * 1000,
                    total_score=_nonnegative(row.get("tscore"), "total score"),
                    total_hits=_nonnegative(row.get("total_hits"), "total hits"),
                    max_combo=_nonnegative(row.get("max_combo"), "max combo"),
                    replay_views=_nonnegative(row.get("replay_views"), "replay views"),
                    created_at=runtime.started_at,
                    updated_at=runtime.started_at,
                )
                await session.execute(
                    play_statement.on_conflict_do_update(
                        index_elements=(UserPlayStat.account_id, UserPlayStat.scoreboard_id),
                        set_={
                            "play_count": play_statement.excluded.play_count,
                            "play_time_ms": play_statement.excluded.play_time_ms,
                            "total_score": play_statement.excluded.total_score,
                            "total_hits": play_statement.excluded.total_hits,
                            "max_combo": play_statement.excluded.max_combo,
                            "replay_views": play_statement.excluded.replay_views,
                            "updated_at": runtime.started_at,
                        },
                    )
                )
                policy_id = policies.get(scoreboard_id)
                if policy_id is not None:
                    performance = decimal_value(row.get("pp"), "performance")
                    if performance < 0:
                        raise ValueError("performance must not be negative")
                    ranked_statement = insert(UserRankedStat).values(
                        account_id=account_id,
                        policy_id=policy_id,
                        ranked_score=_nonnegative(row.get("rscore"), "ranked score"),
                        performance=performance,
                        accuracy=normalized_accuracy(row.get("acc")),
                        grade_counts={
                            "XH": _nonnegative(row.get("xh_count"), "XH count"),
                            "X": _nonnegative(row.get("x_count"), "X count"),
                            "SH": _nonnegative(row.get("sh_count"), "SH count"),
                            "S": _nonnegative(row.get("s_count"), "S count"),
                            "A": _nonnegative(row.get("a_count"), "A count"),
                        },
                        created_at=runtime.started_at,
                        updated_at=runtime.started_at,
                    )
                    await session.execute(
                        ranked_statement.on_conflict_do_update(
                            index_elements=(UserRankedStat.account_id, UserRankedStat.policy_id),
                            set_={
                                "ranked_score": ranked_statement.excluded.ranked_score,
                                "performance": ranked_statement.excluded.performance,
                                "accuracy": ranked_statement.excluded.accuracy,
                                "grade_counts": ranked_statement.excluded.grade_counts,
                                "updated_at": runtime.started_at,
                            },
                        )
                    )
                runtime.report.increment(_PHASE_STATS, "target_first_merged")
            except (KeyError, TypeError, ValueError) as error:
                _warning(runtime, _PHASE_STATS, "stats_skipped", error, "stats", source_id)

    # stats has a composite key. Batch over users to keep all modes together.
    async def users_handler(session: AsyncSession, users: list[SourceRow]) -> None:
        source_ids = [_positive(row.get("id"), "user id") for row in users]
        placeholders = ", ".join("%s" for _ in source_ids)
        rows = runtime.source.fetch_all(
            "stats",
            where=f"`id` IN ({placeholders})",
            parameters=source_ids,
            order_by=("id", "mode"),
        )
        await handler(session, rows)

    await run_batched_phase(
        runtime,
        phase=_PHASE_STATS,
        table="users",
        key="id",
        columns=("id",),
        handler=users_handler,
    )


async def _reconstruct_score_mappings(runtime: MigrationRuntime) -> None:
    async with runtime.session_factory() as session:
        rows = (
            await session.execute(
                select(PlayAttempt.idempotency_key, Score.id)
                .join(Score, Score.attempt_id == PlayAttempt.id)
                .where(PlayAttempt.idempotency_key.like(f"bancho:{runtime.config.migration_id}:score:%"))
            )
        ).all()
    prefix = f"bancho:{runtime.config.migration_id}:score:"
    for key, score_id in rows:
        try:
            runtime.mappings.scores[int(key.removeprefix(prefix))] = score_id
        except ValueError:
            continue


async def _target_score_id(session: AsyncSession, runtime: MigrationRuntime, source_id: int) -> int | None:
    return await session.scalar(
        select(Score.id)
        .join(PlayAttempt, PlayAttempt.id == Score.attempt_id)
        .where(PlayAttempt.id == runtime.ids.make("play-attempt", source_id))
    )


async def _ensure_mod_set(
    session: AsyncSession,
    scoreboard_id: int,
    canonical: list[dict[str, object]],
    digest: bytes,
    legacy_bits: int,
) -> int:
    statement = (
        insert(ModSet)
        .values(
            scoreboard_id=scoreboard_id,
            canonical=canonical,
            canonical_digest=digest,
            legacy_bits=legacy_bits,
        )
        .on_conflict_do_nothing(index_elements=(ModSet.scoreboard_id, ModSet.canonical_digest))
        .returning(ModSet.id)
    )
    mod_set_id = await session.scalar(statement)
    if mod_set_id is None:
        mod_set_id = await session.scalar(
            select(ModSet.id).where(ModSet.scoreboard_id == scoreboard_id, ModSet.canonical_digest == digest)
        )
    if mod_set_id is None:
        raise RuntimeError("mod set merge did not resolve an identity")
    return int(mod_set_id)


async def _ensure_score_difficulty_attribute(
    session: AsyncSession,
    runtime: MigrationRuntime,
    item: _PreparedScore,
    ruleset: Ruleset,
) -> int:
    release_id = _release_id(runtime, CalculationKind.DIFFICULTY, ruleset)
    existing_id = await session.scalar(
        select(BeatmapDifficultyAttribute.id).where(
            BeatmapDifficultyAttribute.beatmap_revision_id == item.revision_id,
            BeatmapDifficultyAttribute.scoreboard_id == item.scoreboard_id,
            BeatmapDifficultyAttribute.mod_set_id == item.mod_set_id,
            BeatmapDifficultyAttribute.release_id == release_id,
        )
    )
    if existing_id is not None:
        return int(existing_id)
    base = (
        await session.execute(
            select(BeatmapDifficultyAttribute.star_rating, BeatmapDifficultyAttribute.max_combo)
            .where(
                BeatmapDifficultyAttribute.beatmap_revision_id == item.revision_id,
                BeatmapDifficultyAttribute.release_id == release_id,
            )
            .limit(1)
        )
    ).one_or_none()
    if base is None:
        raise RuntimeError("legacy score has no imported base difficulty attribute")
    existing_id = await session.scalar(
        insert(BeatmapDifficultyAttribute)
        .values(
            beatmap_revision_id=item.revision_id,
            scoreboard_id=item.scoreboard_id,
            mod_set_id=item.mod_set_id,
            release_id=release_id,
            star_rating=base.star_rating,
            max_combo=base.max_combo,
            attributes={
                "migration_id": runtime.config.migration_id,
                "source_score_id": item.source_id,
                "legacy_base_star_unadjusted": True,
            },
            created_at=item.ended_at,
        )
        .on_conflict_do_nothing()
        .returning(BeatmapDifficultyAttribute.id)
    )
    if existing_id is None:
        existing_id = await session.scalar(
            select(BeatmapDifficultyAttribute.id).where(
                BeatmapDifficultyAttribute.beatmap_revision_id == item.revision_id,
                BeatmapDifficultyAttribute.scoreboard_id == item.scoreboard_id,
                BeatmapDifficultyAttribute.mod_set_id == item.mod_set_id,
                BeatmapDifficultyAttribute.release_id == release_id,
            )
        )
    if existing_id is None:
        raise RuntimeError("score difficulty attribute merge did not resolve an identity")
    return int(existing_id)


async def _ensure_score_event(
    session: AsyncSession,
    runtime: MigrationRuntime,
    item: _PreparedScore,
    score_id: int,
) -> None:
    event_id = runtime.ids.make("score-accepted-event", item.source_id)
    await session.execute(
        insert(OutboxEvent)
        .values(
            id=event_id,
            aggregate_type="score",
            aggregate_id=str(score_id),
            event_type="score.accepted.v1",
            schema_version=1,
            payload={"score_id": score_id, "scoreboard_id": item.scoreboard_id},
            created_at=item.ended_at,
        )
        .on_conflict_do_nothing(index_elements=(OutboxEvent.id,))
    )


async def _ensure_release(
    session: AsyncSession,
    runtime: MigrationRuntime,
    *,
    kind: CalculationKind,
    formula_id: uuid.UUID,
    ruleset: Ruleset,
    difficulty_release_id: uuid.UUID | None,
) -> uuid.UUID:
    version = runtime.source_schema.version or "5.2.2"
    configuration = {
        "migration_id": runtime.config.migration_id,
        "source": "bancho.py v5.2.2",
        "legacy_values_only": True,
    }
    release_id = await session.scalar(
        insert(CalculationRelease)
        .values(
            id=runtime.ids.make("calculation-release", f"{kind.value}:{ruleset.value}"),
            formula_id=formula_id,
            ruleset=ruleset,
            version=version,
            configuration=configuration,
            difficulty_release_id=difficulty_release_id,
            active=False,
            created_at=runtime.started_at,
        )
        .on_conflict_do_nothing()
        .returning(CalculationRelease.id)
    )
    if release_id is None:
        release_id = await session.scalar(
            select(CalculationRelease.id).where(
                CalculationRelease.formula_id == formula_id,
                CalculationRelease.ruleset == ruleset,
                CalculationRelease.version == version,
            )
        )
    if release_id is None:
        raise RuntimeError("legacy calculation release merge did not resolve an identity")
    return release_id


def _release_id(runtime: MigrationRuntime, kind: CalculationKind, ruleset: Ruleset) -> uuid.UUID:
    release_id = runtime.mappings.calculation_releases.get((kind.value, ruleset.value))
    if release_id is None:
        raise RuntimeError(f"legacy {kind.value} release for {ruleset.value} was not resolved")
    return release_id


async def _checkpoint(runtime: MigrationRuntime) -> MigrationCheckpoint:
    checkpoint = await runtime.state.load()
    if checkpoint is None:
        raise RuntimeError("migration state is not initialized")
    return checkpoint


def _md5(value: object) -> bytes:
    if isinstance(value, bytes) and len(value) == 16:
        return value
    if not isinstance(value, str) or len(value) != 32:
        raise ValueError("MD5 value must contain 32 hexadecimal characters")
    try:
        result = bytes.fromhex(value)
    except ValueError as error:
        raise ValueError("MD5 value must be hexadecimal") from error
    if len(result) != 16:
        raise ValueError("MD5 value must contain 16 bytes")
    return result


def _optional_md5(value: object) -> bytes | None:
    if value in {None, "", b""}:
        return None
    return _md5(value)


def _positive(value: object, name: str) -> int:
    return bounded_integer(value, name, minimum=1, maximum=9_223_372_036_854_775_807)


def _nonnegative(value: object, name: str) -> int:
    return bounded_integer(value, name, minimum=0, maximum=9_223_372_036_854_775_807)


def _legacy_bool(value: object, name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in {0, 1}:
        return bool(value)
    raise ValueError(f"{name} must be zero or one")


def _hit_statistics(row: SourceRow, ruleset: Ruleset) -> tuple[tuple[str, int], ...]:
    source = {
        "n300": _nonnegative(row.get("n300"), "n300"),
        "n100": _nonnegative(row.get("n100"), "n100"),
        "n50": _nonnegative(row.get("n50"), "n50"),
        "nmiss": _nonnegative(row.get("nmiss"), "nmiss"),
        "ngeki": _nonnegative(row.get("ngeki"), "ngeki"),
        "nkatu": _nonnegative(row.get("nkatu"), "nkatu"),
    }
    if ruleset is Ruleset.OSU:
        names = (("great", "n300"), ("ok", "n100"), ("meh", "n50"), ("miss", "nmiss"))
    elif ruleset is Ruleset.TAIKO:
        names = (("great", "n300"), ("ok", "n100"), ("miss", "nmiss"))
    elif ruleset is Ruleset.FRUITS:
        names = (
            ("great", "n300"),
            ("large_tick_hit", "n100"),
            ("small_tick_hit", "n50"),
            ("small_tick_miss", "nkatu"),
            ("large_tick_miss", "ngeki"),
            ("miss", "nmiss"),
        )
    else:
        names = (
            ("perfect", "ngeki"),
            ("great", "n300"),
            ("good", "nkatu"),
            ("ok", "n100"),
            ("meh", "n50"),
            ("miss", "nmiss"),
        )
    return tuple((target, source[column]) for target, column in names)


def _comment_color(value: object) -> str | None:
    if value in {None, ""}:
        return None
    candidate = str(value).removeprefix("#")
    if len(candidate) != 6:
        raise ValueError("comment color must contain six hexadecimal characters")
    try:
        bytes.fromhex(candidate)
    except ValueError as error:
        raise ValueError("comment color must be hexadecimal") from error
    return f"#{candidate.upper()}"


def _warning(
    runtime: MigrationRuntime,
    phase: str,
    code: str,
    error: Exception,
    entity: str,
    source_id: object,
) -> None:
    runtime.report.add(
        DiagnosticSeverity.WARNING,
        code,
        str(error),
        entity=entity,
        source_id=source_id,
    )
    runtime.report.increment(phase, "skipped")
