"""Project accepted score events into eligibility and leaderboard entries."""

import uuid
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra.db.models.content import Beatmap
from perfcho.infra.db.models.core import Account
from perfcho.infra.db.models.events import OutboxEvent
from perfcho.infra.db.models.scoring import (
    LeaderboardEntry,
    RankingPolicy,
    Score,
    ScoreEligibility,
    ScorePerformance,
)
from perfcho.infra.outbox import register_consumer


@register_consumer("ranking-projector.v1", ("score.accepted.v1",))
async def project_accepted_score(session: AsyncSession, event: OutboxEvent, partition_key: str) -> None:
    """Project one accepted score under the active versioned ranking policy."""
    del partition_key
    score_id = _payload_integer(event.payload, "score_id")
    row = (
        await session.execute(
            select(Score, Beatmap.status, Account.country_code)
            .join(Beatmap, Beatmap.id == Score.beatmap_id)
            .join(Account, Account.id == Score.account_id)
            .where(Score.id == score_id)
        )
    ).one_or_none()
    if row is None:
        raise RuntimeError("accepted score event references a missing score")
    score, beatmap_status, country_code = row
    policy = (
        await session.execute(
            select(RankingPolicy)
            .where(RankingPolicy.scoreboard_id == score.scoreboard_id, RankingPolicy.active.is_(True))
            .with_for_update(read=True)
        )
    ).scalar_one_or_none()
    if policy is None:
        return

    configured_statuses = policy.configuration.get("eligible_beatmap_statuses", [])
    eligible_statuses = (
        {value for value in configured_statuses if isinstance(value, str)}
        if isinstance(configured_statuses, list)
        else set()
    )
    eligible = score.outcome.value == "passed" and beatmap_status.value in eligible_statuses
    metric_value = await _metric_value(session, score, policy)
    if metric_value is None:
        eligible = False
    state = "eligible" if eligible else "ineligible"
    reason = None if eligible else "policy_requirements"
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


async def _metric_value(
    session: AsyncSession,
    score: Score,
    policy: RankingPolicy,
) -> Decimal | None:
    if policy.metric == "total_score":
        return Decimal(score.total_score)
    if policy.metric == "classic_score":
        return Decimal(score.classic_score)
    if policy.metric == "pp" and policy.calculation_release_id is not None:
        return await session.scalar(
            select(ScorePerformance.pp).where(
                ScorePerformance.score_id == score.id,
                ScorePerformance.release_id == policy.calculation_release_id,
            )
        )
    raise RuntimeError(f"unsupported ranking policy metric: {policy.metric}")


def _tie_break_value(score: Score, tie_breaker: str) -> Decimal:
    if tie_breaker == "ended_at":
        return Decimal(int(score.ended_at.timestamp() * 1_000_000))
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


def _payload_integer(payload: dict[str, object], key: str) -> int:
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise RuntimeError(f"event payload field {key} must be an integer")
    return value
