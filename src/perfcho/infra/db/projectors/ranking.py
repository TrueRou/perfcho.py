"""Project accepted score events into ranking and account statistics read models."""

import uuid
from datetime import UTC, datetime
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra.db.models.content import Beatmap
from perfcho.infra.db.models.core import Account
from perfcho.infra.db.models.events import OutboxEvent
from perfcho.infra.db.models.scoring import (
    LeaderboardEntry,
    ModPolicy,
    ModSet,
    RankingPolicy,
    Score,
    ScoreEligibility,
    ScorePerformance,
    UserRankedStat,
)
from perfcho.infra.db.projectors.common import advance_checkpoint, payload_integer, require_event_context
from perfcho.modules.scoring.models import weighted_total_performance

CONSUMER_NAME = "ranking-projector.v1"
EVENT_TYPES = frozenset({"score.accepted.v1", "score.performance-calculated.v1"})


async def project_accepted_score(session: AsyncSession, event: OutboxEvent, partition_key: str) -> None:
    """Project one accepted score under the active versioned ranking policy."""
    score_id = payload_integer(event.payload, "score_id")
    scoreboard_id = payload_integer(event.payload, "scoreboard_id")
    require_event_context(
        event,
        partition_key,
        aggregate_type="score",
        aggregate_id=str(score_id),
        expected_partition_key=f"scoreboard:{scoreboard_id}",
    )
    row = (
        await session.execute(
            select(Score, Beatmap.status, Account.country_code, ModSet.canonical)
            .join(Beatmap, Beatmap.id == Score.beatmap_id)
            .join(Account, Account.id == Score.account_id)
            .join(ModSet, ModSet.id == Score.mod_set_id)
            .where(Score.id == score_id)
        )
    ).one_or_none()
    if row is None:
        raise RuntimeError("accepted score event references a missing score")
    score, beatmap_status, country_code, canonical_mods = row
    if score.scoreboard_id != scoreboard_id:
        raise RuntimeError("score event scoreboard does not match the authoritative score")
    policy_rows = list(
        (
            await session.execute(
                select(RankingPolicy, ModPolicy.rules)
                .join(ModPolicy, ModPolicy.id == RankingPolicy.mod_policy_id)
                .where(RankingPolicy.scoreboard_id == score.scoreboard_id, RankingPolicy.active.is_(True))
                .with_for_update(read=True)
            )
        ).all()
    )
    mod_acronyms = {
        acronym
        for item in canonical_mods
        if isinstance(item, dict) and isinstance((acronym := item.get("acronym")), str)
    }
    if len(mod_acronyms) != len(canonical_mods):
        raise RuntimeError("score mod set contains invalid canonical entries")
    for policy, mod_rules in policy_rows:
        await _project_policy(
            session,
            score,
            beatmap_status.value,
            country_code,
            policy,
            mod_acronyms,
            mod_rules,
        )
        await _project_user_ranked_stat(session, score.account_id, policy, event.id)
    await advance_checkpoint(session, event, projector=CONSUMER_NAME, partition_key=partition_key)


async def _project_user_ranked_stat(
    session: AsyncSession,
    account_id: int,
    policy: RankingPolicy,
    source_event_id: uuid.UUID,
) -> None:
    """Rebuild one account's policy-owned ranked statistics."""
    rows = list(
        await session.execute(
            select(
                LeaderboardEntry.metric_value,
                Score.total_score,
                Score.classic_score,
                Score.accuracy,
                Score.grade,
            )
            .join(Score, Score.id == LeaderboardEntry.score_id)
            .where(
                LeaderboardEntry.policy_id == policy.id,
                LeaderboardEntry.scope == "overall",
                LeaderboardEntry.filter_mod_set_id.is_(None),
                LeaderboardEntry.account_id == account_id,
            )
            .order_by(LeaderboardEntry.metric_value.desc(), LeaderboardEntry.score_id.asc())
        )
    )
    ranked_score = sum(row.classic_score if policy.metric == "classic_score" else row.total_score for row in rows)
    performance = (
        Decimal(weighted_total_performance([row.metric_value for row in rows])) if policy.metric == "pp" else Decimal(0)
    )
    accuracy = sum((Decimal(row.accuracy) for row in rows), Decimal(0)) / len(rows) if rows else Decimal(0)
    grade_counts = {grade: 0 for grade in ("XH", "X", "SH", "S", "A")}
    for row in rows:
        if row.grade.value in grade_counts:
            grade_counts[row.grade.value] += 1

    statement = insert(UserRankedStat).values(
        account_id=account_id,
        policy_id=policy.id,
        ranked_score=ranked_score,
        performance=performance,
        accuracy=accuracy,
        grade_counts=grade_counts,
        source_event_id=source_event_id,
    )
    excluded = statement.excluded
    await session.execute(
        statement.on_conflict_do_update(
            index_elements=(UserRankedStat.account_id, UserRankedStat.policy_id),
            set_={
                "ranked_score": excluded.ranked_score,
                "performance": excluded.performance,
                "accuracy": excluded.accuracy,
                "grade_counts": excluded.grade_counts,
                "source_event_id": excluded.source_event_id,
            },
        )
    )


async def _project_policy(
    session: AsyncSession,
    score: Score,
    beatmap_status: str,
    country_code: str | None,
    policy: RankingPolicy,
    mod_acronyms: set[str],
    mod_rules: dict[str, object],
) -> None:
    """Project one score under one explicit policy and Formula release."""
    configured_statuses = policy.configuration.get("eligible_beatmap_statuses", [])
    eligible_statuses = (
        {value for value in configured_statuses if isinstance(value, str)}
        if isinstance(configured_statuses, list)
        else set()
    )
    policy_eligible = (
        score.outcome.value == "passed"
        and beatmap_status in eligible_statuses
        and _mods_are_eligible(mod_acronyms, mod_rules)
    )
    metric_value = await _metric_value(session, score, policy)
    eligible = policy_eligible and metric_value is not None
    state = "eligible" if eligible else "ineligible"
    reason = (
        None
        if eligible
        else "performance_pending"
        if policy_eligible and policy.metric == "pp"
        else "policy_requirements"
    )
    await session.execute(
        insert(ScoreEligibility)
        .values(
            score_id=score.id,
            policy_id=policy.id,
            state=state,
            reason=reason,
            input_version=policy.version,
        )
        .on_conflict_do_update(
            index_elements=(ScoreEligibility.score_id, ScoreEligibility.policy_id),
            set_={"state": state, "reason": reason, "input_version": policy.version},
        )
    )
    if not eligible or metric_value is None:
        return

    tie_break_value = _tie_break_value(score, policy.tie_breaker)
    await _upsert_entry(
        session,
        policy_id=policy.id,
        beatmap_id=score.beatmap_id,
        account_id=score.account_id,
        score_id=score.id,
        country_code=country_code,
        scope="overall",
        filter_mod_set_id=None,
        metric_value=metric_value,
        tie_break_value=tie_break_value,
    )
    await _upsert_entry(
        session,
        policy_id=policy.id,
        beatmap_id=score.beatmap_id,
        account_id=score.account_id,
        score_id=score.id,
        country_code=country_code,
        scope="exact_mods",
        filter_mod_set_id=score.mod_set_id,
        metric_value=metric_value,
        tie_break_value=tie_break_value,
    )


def _mods_are_eligible(acronyms: set[str], rules: dict[str, object]) -> bool:
    allowed = _rule_acronyms(rules, "allowed_acronyms")
    required = _rule_acronyms(rules, "required_acronyms")
    forbidden = _rule_acronyms(rules, "forbidden_acronyms")
    return acronyms <= allowed and required <= acronyms and acronyms.isdisjoint(forbidden)


def _rule_acronyms(rules: dict[str, object], key: str) -> set[str]:
    values = rules.get(key)
    if not isinstance(values, list) or any(not isinstance(value, str) for value in values):
        raise RuntimeError(f"ranking mod policy field {key} must be a string list")
    return set(values)


async def _metric_value(
    session: AsyncSession,
    score: Score,
    policy: RankingPolicy,
) -> Decimal | None:
    if policy.metric == "total_score":
        return Decimal(score.total_score)
    if policy.metric == "classic_score":
        return Decimal(score.classic_score)
    if policy.metric == "pp":
        if policy.calculation_release_id is None:
            return None
        return await session.scalar(
            select(ScorePerformance.pp).where(
                ScorePerformance.score_id == score.id,
                ScorePerformance.release_id == policy.calculation_release_id,
            )
        )
    raise RuntimeError(f"unsupported ranking policy metric: {policy.metric}")


def _tie_break_value(score: Score, tie_breaker: str) -> Decimal:
    if tie_breaker == "ended_at":
        ended_at = score.ended_at.astimezone(UTC)
        elapsed = ended_at - datetime(1970, 1, 1, tzinfo=UTC)
        microseconds = (elapsed.days * 86_400 + elapsed.seconds) * 1_000_000 + elapsed.microseconds
        return Decimal(microseconds)
    if tie_breaker == "classic_score":
        return Decimal(score.classic_score)
    if tie_breaker == "total_score":
        return Decimal(score.total_score)
    raise RuntimeError(f"unsupported ranking policy tie breaker: {tie_breaker}")


async def _upsert_entry(
    session: AsyncSession,
    *,
    policy_id: uuid.UUID,
    beatmap_id: int,
    account_id: int,
    score_id: int,
    country_code: str | None,
    scope: str,
    filter_mod_set_id: int | None,
    metric_value: Decimal,
    tie_break_value: Decimal,
) -> None:
    statement = insert(LeaderboardEntry).values(
        policy_id=policy_id,
        beatmap_id=beatmap_id,
        account_id=account_id,
        score_id=score_id,
        country_code=country_code,
        scope=scope,
        filter_mod_set_id=filter_mod_set_id,
        metric_value=metric_value,
        tie_break_value=tie_break_value,
    )
    excluded = statement.excluded
    await session.execute(
        statement.on_conflict_do_update(
            index_elements=(
                LeaderboardEntry.policy_id,
                LeaderboardEntry.beatmap_id,
                LeaderboardEntry.scope,
                LeaderboardEntry.filter_mod_set_id,
                LeaderboardEntry.account_id,
            ),
            set_={
                "score_id": score_id,
                "country_code": country_code,
                "metric_value": metric_value,
                "tie_break_value": tie_break_value,
            },
            where=(
                (excluded.metric_value > LeaderboardEntry.metric_value)
                | (
                    (excluded.metric_value == LeaderboardEntry.metric_value)
                    & (excluded.tie_break_value > LeaderboardEntry.tie_break_value)
                )
                | (
                    (excluded.metric_value == LeaderboardEntry.metric_value)
                    & (excluded.tie_break_value == LeaderboardEntry.tie_break_value)
                    & (excluded.score_id < LeaderboardEntry.score_id)
                )
            ),
        )
    )
