"""Project accepted score events into ranking and account statistics read models."""

import uuid
from datetime import UTC, datetime
from decimal import Decimal

from sqlalchemy import func, literal, select, true
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra.db.enums import ScoreGrade
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
from perfcho.infra.db.projectors.common import (
    advance_checkpoint,
    payload_integer,
    payload_uuid,
    require_event_context,
)

CONSUMER_NAME = "ranking-projector.v1"
EVENT_TYPES = frozenset({"score.accepted.v1", "score.performance-calculated.v1"})


async def project_accepted_score(session: AsyncSession, event: OutboxEvent, partition_key: str) -> None:
    """Project one accepted score under the active versioned ranking policy."""
    score_id = payload_integer(event.payload, "score_id")
    account_id = payload_integer(event.payload, "account_id")
    scoreboard_id = payload_integer(event.payload, "scoreboard_id")
    require_event_context(
        event,
        partition_key,
        aggregate_type="score",
        aggregate_id=str(score_id),
        expected_partition_key=f"account:{account_id}:scoreboard:{scoreboard_id}",
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
    if score.account_id != account_id:
        raise RuntimeError("score event account does not match the authoritative score")
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
    performance_release_id = (
        payload_uuid(event.payload, "release_id") if event.event_type == "score.performance-calculated.v1" else None
    )
    for policy, mod_rules in policy_rows:
        overall_changed = await _project_policy(
            session,
            score,
            beatmap_status.value,
            country_code,
            policy,
            mod_acronyms,
            mod_rules,
        )
        if overall_changed or performance_release_id == policy.calculation_release_id:
            await _project_user_ranked_stat(session, score.account_id, policy, event.id)
    await advance_checkpoint(session, event, projector=CONSUMER_NAME, partition_key=partition_key)


async def _project_user_ranked_stat(
    session: AsyncSession,
    account_id: int,
    policy: RankingPolicy,
    source_event_id: uuid.UUID,
) -> None:
    """Rebuild one account's policy-owned ranked statistics."""
    ranked_entries = (
        select(Score.total_score, Score.classic_score, Score.accuracy, Score.grade)
        .select_from(LeaderboardEntry)
        .join(Score, Score.id == LeaderboardEntry.score_id)
        .where(
            LeaderboardEntry.policy_id == policy.id,
            LeaderboardEntry.scope == "overall",
            LeaderboardEntry.filter_mod_set_id.is_(None),
            LeaderboardEntry.account_id == account_id,
        )
        .cte("ranked_entries")
    )
    ranked_score_value = (
        ranked_entries.c.classic_score if policy.metric == "classic_score" else ranked_entries.c.total_score
    )
    grade_counts = func.jsonb_build_object(
        "XH",
        func.count().filter(ranked_entries.c.grade == ScoreGrade.XH),
        "X",
        func.count().filter(ranked_entries.c.grade == ScoreGrade.X),
        "SH",
        func.count().filter(ranked_entries.c.grade == ScoreGrade.SH),
        "S",
        func.count().filter(ranked_entries.c.grade == ScoreGrade.S),
        "A",
        func.count().filter(ranked_entries.c.grade == ScoreGrade.A),
    )
    overall_stats = select(
        func.coalesce(func.sum(ranked_score_value), 0).label("ranked_score"),
        func.coalesce(func.avg(ranked_entries.c.accuracy), 0).label("accuracy"),
        grade_counts.label("grade_counts"),
    ).cte("overall_stats")

    per_beatmap = (
        select(
            Score.beatmap_id,
            ScorePerformance.pp,
            func.row_number()
            .over(
                partition_by=Score.beatmap_id,
                order_by=(ScorePerformance.pp.desc(), Score.id.asc()),
            )
            .label("beatmap_position"),
        )
        .join(ScorePerformance, ScorePerformance.score_id == Score.id)
        .join(
            ScoreEligibility,
            (ScoreEligibility.score_id == Score.id) & (ScoreEligibility.policy_id == policy.id),
        )
        .where(
            Score.account_id == account_id,
            Score.scoreboard_id == policy.scoreboard_id,
            ScoreEligibility.state == "eligible",
            ScorePerformance.release_id == policy.calculation_release_id,
        )
        .cte("per_beatmap_performance")
    )
    best_per_beatmap = (
        select(per_beatmap.c.beatmap_id, per_beatmap.c.pp)
        .where(per_beatmap.c.beatmap_position == 1)
        .cte("best_per_beatmap_performance")
    )
    ordered_performance = select(
        best_per_beatmap.c.pp,
        (
            func.row_number().over(order_by=(best_per_beatmap.c.pp.desc(), best_per_beatmap.c.beatmap_id.asc())) - 1
        ).label("performance_index"),
    ).cte("ordered_performance")
    performance_count = func.count()
    weighted_performance = func.coalesce(
        func.sum(
            ordered_performance.c.pp * func.power(literal(Decimal("0.95")), ordered_performance.c.performance_index)
        ),
        Decimal(0),
    )
    bonus_performance = (Decimal(1) - func.power(literal(Decimal("0.9994")), performance_count)) * literal(
        Decimal("416.6667")
    )
    performance_stats = (
        select(func.round(weighted_performance + bonus_performance).label("performance"))
        .select_from(ordered_performance)
        .cte("performance_stats")
    )

    statement = insert(UserRankedStat).from_select(
        (
            "account_id",
            "policy_id",
            "ranked_score",
            "performance",
            "accuracy",
            "grade_counts",
            "source_event_id",
        ),
        select(
            literal(account_id),
            literal(policy.id),
            overall_stats.c.ranked_score,
            performance_stats.c.performance,
            overall_stats.c.accuracy,
            overall_stats.c.grade_counts,
            literal(source_event_id),
        )
        .select_from(overall_stats)
        .join(performance_stats, true()),
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
) -> bool:
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
        return False

    tie_break_value = _tie_break_value(score, policy.tie_breaker)
    overall_changed = await _upsert_entry(
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
    return overall_changed


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
) -> bool:
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
    entry_id = await session.scalar(
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
        ).returning(LeaderboardEntry.id)
    )
    return entry_id is not None
