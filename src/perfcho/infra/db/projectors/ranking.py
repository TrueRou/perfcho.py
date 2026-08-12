"""Project score eligibility and account ranked statistics."""

import uuid
from collections.abc import Awaitable, Callable
from decimal import Decimal

from sqlalchemy import func, literal, select, true
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import InstrumentedAttribute

from perfcho.infra.db.enums import ScoreGrade
from perfcho.infra.db.models.content import Beatmap
from perfcho.infra.db.models.events import OutboxEvent
from perfcho.infra.db.models.scoring import (
    RankingPolicy,
    Score,
    ScoreEligibility,
    ScorePerformance,
    UserRanking,
)
from perfcho.infra.db.projectors.common import (
    advance_checkpoint,
    payload_integer,
    payload_string,
    require_event_context,
)

CONSUMER_NAME = "ranking-projector.v1"
EVENT_TYPES = frozenset({"score.accepted.v1", "score.performance-calculated.v1"})


async def project_accepted_score(
    session: AsyncSession,
    event: OutboxEvent,
    partition_key: str,
    invalidate: Callable[[int], Awaitable[None]] | None = None,
) -> None:
    """Project one score under every active policy for its ruleset."""
    score_id = payload_integer(event.payload, "score_id")
    account_id = payload_integer(event.payload, "account_id")
    ruleset = payload_string(event.payload, "ruleset")
    require_event_context(
        event,
        partition_key,
        aggregate_type="score",
        aggregate_id=str(score_id),
        expected_partition_key=f"account:{account_id}:ruleset:{ruleset}",
    )
    row = (
        await session.execute(
            select(Score, Beatmap.status).join(Beatmap, Beatmap.id == Score.beatmap_id).where(Score.id == score_id)
        )
    ).one_or_none()
    if row is None:
        raise RuntimeError("ranking event references a missing score")
    score, beatmap_status = row
    if score.ruleset.value != ruleset:
        raise RuntimeError("score event ruleset does not match the authoritative score")
    if score.account_id != account_id:
        raise RuntimeError("score event account does not match the authoritative score")

    policies = list(
        await session.scalars(
            select(RankingPolicy)
            .where(RankingPolicy.ruleset == score.ruleset, RankingPolicy.active.is_(True))
            .with_for_update(read=True)
        )
    )
    for policy in policies:
        await _project_policy(session, score, beatmap_status.value, policy)
        await _project_user_ranking(session, score.account_id, policy, event.id)
    await advance_checkpoint(session, event, projector=CONSUMER_NAME, partition_key=partition_key)
    if invalidate is not None:
        await invalidate(score.beatmap_id)


async def _project_policy(
    session: AsyncSession,
    score: Score,
    beatmap_status: str,
    policy: RankingPolicy,
) -> None:
    """Project one score's eligibility under one configured policy."""
    metric = _configuration_string(policy.configuration, "metric")
    calculation_release_id = _calculation_release_id(policy.configuration, metric)
    mod_rules = _configuration_mapping(policy.configuration, "mod_rules")
    beatmap_rules = _configuration_mapping(policy.configuration, "beatmap_rules")
    policy_eligible = (
        score.outcome.value == "passed"
        and _beatmap_is_eligible(beatmap_status, beatmap_rules)
        and _mods_are_eligible(set(score.mods_acronyms), mod_rules)
    )
    performance_pending = (
        policy_eligible
        and metric == "pp"
        and await _score_performance(session, score.id, calculation_release_id) is None
    )
    eligible = policy_eligible and not performance_pending
    state = "eligible" if eligible else "ineligible"
    reason = None if eligible else "performance_pending" if performance_pending else "policy_requirements"
    await session.execute(
        insert(ScoreEligibility)
        .values(
            score_id=score.id,
            policy_id=policy.id,
            state=state,
            reason=reason,
            input_version=1,
        )
        .on_conflict_do_update(
            index_elements=(ScoreEligibility.score_id, ScoreEligibility.policy_id),
            set_={"state": state, "reason": reason, "input_version": 1},
        )
    )


async def _project_user_ranking(
    session: AsyncSession,
    account_id: int,
    policy: RankingPolicy,
    source_event_id: uuid.UUID,
) -> None:
    """Rebuild one account from its eligible per-beatmap best scores."""
    metric = _configuration_string(policy.configuration, "metric")
    calculation_release_id = _calculation_release_id(policy.configuration, metric)
    metric_value = _metric_expression(metric, calculation_release_id)
    candidates = (
        select(
            Score.id,
            Score.beatmap_id,
            Score.total_score,
            Score.classic_score,
            Score.accuracy,
            Score.grade,
            ScorePerformance.pp,
            func.row_number()
            .over(
                partition_by=Score.beatmap_id,
                order_by=(metric_value.desc(), Score.id.asc()),
            )
            .label("beatmap_position"),
        )
        .join(
            ScoreEligibility,
            (ScoreEligibility.score_id == Score.id) & (ScoreEligibility.policy_id == policy.id),
        )
        .outerjoin(
            ScorePerformance,
            (ScorePerformance.score_id == Score.id) & (ScorePerformance.release_id == calculation_release_id),
        )
        .where(
            Score.account_id == account_id,
            Score.ruleset == policy.ruleset,
            ScoreEligibility.state == "eligible",
        )
        .cte("ranked_score_candidates")
    )
    best_scores = (
        select(
            candidates.c.id,
            candidates.c.beatmap_id,
            candidates.c.total_score,
            candidates.c.classic_score,
            candidates.c.accuracy,
            candidates.c.grade,
            candidates.c.pp,
        )
        .where(candidates.c.beatmap_position == 1)
        .cte("ranked_best_scores")
    )
    ranked_score_value = best_scores.c.classic_score if metric == "classic_score" else best_scores.c.total_score
    grade_counts = func.jsonb_build_object(
        "XH",
        func.count().filter(best_scores.c.grade == ScoreGrade.XH),
        "X",
        func.count().filter(best_scores.c.grade == ScoreGrade.X),
        "SH",
        func.count().filter(best_scores.c.grade == ScoreGrade.SH),
        "S",
        func.count().filter(best_scores.c.grade == ScoreGrade.S),
        "A",
        func.count().filter(best_scores.c.grade == ScoreGrade.A),
    )
    overall_stats = select(
        func.coalesce(func.sum(ranked_score_value), 0).label("ranked_score"),
        func.coalesce(func.avg(best_scores.c.accuracy), 0).label("accuracy"),
        grade_counts.label("grade_counts"),
    ).cte("ranked_overall_stats")
    ordered_performance = (
        select(
            best_scores.c.pp,
            (func.row_number().over(order_by=(best_scores.c.pp.desc(), best_scores.c.id.asc())) - 1).label(
                "performance_index"
            ),
        )
        .where(best_scores.c.pp.is_not(None))
        .cte("ranked_ordered_performance")
    )
    performance_stats = (
        select(
            func.coalesce(
                func.sum(
                    ordered_performance.c.pp
                    * func.power(literal(Decimal("0.95")), ordered_performance.c.performance_index)
                ),
                Decimal(0),
            ).label("performance")
        )
        .select_from(ordered_performance)
        .cte("ranked_performance_stats")
    )
    statement = insert(UserRanking).from_select(
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
    await session.execute(
        statement.on_conflict_do_update(
            index_elements=(UserRanking.account_id, UserRanking.policy_id),
            set_={
                "ranked_score": statement.excluded.ranked_score,
                "performance": statement.excluded.performance,
                "accuracy": statement.excluded.accuracy,
                "grade_counts": statement.excluded.grade_counts,
                "source_event_id": statement.excluded.source_event_id,
            },
        )
    )


def _configuration_mapping(configuration: dict[str, object], key: str) -> dict[str, object]:
    value = configuration.get(key)
    if not isinstance(value, dict) or any(not isinstance(item_key, str) for item_key in value):
        raise RuntimeError(f"ranking policy configuration {key} must be an object")
    return value


def _configuration_string(configuration: dict[str, object], key: str) -> str:
    value = configuration.get(key)
    if not isinstance(value, str):
        raise RuntimeError(f"ranking policy configuration {key} must be a string")
    return value


def _calculation_release_id(configuration: dict[str, object], metric: str) -> uuid.UUID | None:
    value = configuration.get("calculation_release_id")
    if value is None and metric != "pp":
        return None
    if not isinstance(value, str):
        raise RuntimeError("ranking policy calculation_release_id must be a UUID string")
    try:
        return uuid.UUID(value)
    except ValueError as error:
        raise RuntimeError("ranking policy calculation_release_id must be a UUID string") from error


def _beatmap_is_eligible(status: str, rules: dict[str, object]) -> bool:
    statuses = _rule_strings(rules, "allowed_statuses")
    return status in statuses


def _mods_are_eligible(acronyms: set[str], rules: dict[str, object]) -> bool:
    allowed = _rule_strings(rules, "allowed_acronyms")
    required = _rule_strings(rules, "required_acronyms")
    forbidden = _rule_strings(rules, "forbidden_acronyms")
    return acronyms <= allowed and required <= acronyms and acronyms.isdisjoint(forbidden)


def _rule_strings(rules: dict[str, object], key: str) -> set[str]:
    values = rules.get(key)
    if not isinstance(values, list) or any(not isinstance(value, str) for value in values):
        raise RuntimeError(f"ranking policy rule {key} must be a string list")
    return set(values)


async def _score_performance(
    session: AsyncSession,
    score_id: int,
    release_id: uuid.UUID | None,
) -> Decimal | None:
    if release_id is None:
        return None
    return await session.scalar(
        select(ScorePerformance.pp).where(
            ScorePerformance.score_id == score_id,
            ScorePerformance.release_id == release_id,
        )
    )


def _metric_expression(
    metric: str,
    release_id: uuid.UUID | None,
) -> InstrumentedAttribute[int] | InstrumentedAttribute[Decimal]:
    if metric == "total_score":
        return Score.total_score
    if metric == "classic_score":
        return Score.classic_score
    if metric == "pp":
        if release_id is None:
            raise RuntimeError("pp ranking policy requires calculation_release_id")
        return ScorePerformance.pp
    raise RuntimeError(f"unsupported ranking policy metric: {metric}")
