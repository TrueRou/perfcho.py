"""Project score and replay-view facts into canonical gameplay statistics."""

from datetime import UTC, date, datetime

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.consumers.common import (
    advance_checkpoint,
    payload_integer,
    payload_string,
    require_event_context,
)
from perfcho.infra.db.enums import ScoreOutcome
from perfcho.infra.db.models.events import ConsumerCheckpoint, OutboxEvent
from perfcho.infra.db.models.scoring import (
    BeatmapActivity,
    BeatmapFailHistogram,
    PlayAttempt,
    Score,
    ScoreHitStatistic,
    UserBeatmapActivity,
    UserMonthlyActivity,
    UserPlayStat,
)

CONSUMER_NAME = "scoring-stats-consumer.v1"
EVENT_TYPES = frozenset({"score.accepted.v1", "score.replay-viewed.v1"})


async def consume_scoring_stats(session: AsyncSession, event: OutboxEvent, partition_key: str) -> None:
    """Apply one ordered score or replay-view event to factual statistics."""
    if not await _is_new_event(session, event, partition_key):
        return
    if event.event_type == "score.accepted.v1":
        await _apply_score(session, event, partition_key)
    elif event.event_type == "score.replay-viewed.v1":
        await _apply_replay_view(session, event, partition_key)
    else:
        raise RuntimeError(f"unsupported scoring statistics event: {event.event_type}")
    await advance_checkpoint(session, event, consumer=CONSUMER_NAME, partition_key=partition_key)


async def _apply_score(session: AsyncSession, event: OutboxEvent, partition_key: str) -> None:
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
            select(Score, PlayAttempt.progress)
            .join(PlayAttempt, PlayAttempt.id == Score.attempt_id)
            .where(Score.id == score_id)
        )
    ).one_or_none()
    if row is None:
        raise RuntimeError("score statistics event references a missing score")
    score, progress = row
    if score.ruleset.value != ruleset:
        raise RuntimeError("score statistics event ruleset does not match the authoritative score")
    if score.account_id != account_id:
        raise RuntimeError("score statistics event account does not match the authoritative score")
    total_hits = int(
        await session.scalar(
            select(func.coalesce(func.sum(ScoreHitStatistic.actual), 0)).where(ScoreHitStatistic.score_id == score.id)
        )
        or 0
    )
    play_time_ms = max(0, int((score.ended_at - score.started_at).total_seconds() * 1000))
    passed = score.outcome is ScoreOutcome.PASSED

    play_stat = await session.get(
        UserPlayStat,
        {"account_id": score.account_id, "ruleset": score.ruleset},
        with_for_update=True,
    )
    if play_stat is None:
        play_stat = UserPlayStat(
            account_id=score.account_id,
            ruleset=score.ruleset,
            play_count=0,
            play_time_ms=0,
            total_score=0,
            total_hits=0,
            max_combo=0,
            replay_views=0,
        )
        session.add(play_stat)
    play_stat.play_count += 1
    play_stat.play_time_ms += play_time_ms
    play_stat.total_score += score.total_score
    play_stat.total_hits += total_hits
    play_stat.max_combo = max(play_stat.max_combo, score.max_combo)
    play_stat.source_event_id = event.id

    month = _month_start(score.ended_at)
    monthly = await session.get(
        UserMonthlyActivity,
        {"account_id": score.account_id, "ruleset": score.ruleset, "month": month},
        with_for_update=True,
    )
    if monthly is None:
        monthly = UserMonthlyActivity(
            account_id=score.account_id,
            ruleset=score.ruleset,
            month=month,
            play_count=0,
            play_time_ms=0,
            replay_views=0,
        )
        session.add(monthly)
    monthly.play_count += 1
    monthly.play_time_ms += play_time_ms

    user_beatmap = await session.get(
        UserBeatmapActivity,
        {"account_id": score.account_id, "beatmap_id": score.beatmap_id, "ruleset": score.ruleset},
        with_for_update=True,
    )
    if user_beatmap is None:
        user_beatmap = UserBeatmapActivity(
            account_id=score.account_id,
            beatmap_id=score.beatmap_id,
            ruleset=score.ruleset,
            attempt_count=0,
            pass_count=0,
        )
        session.add(user_beatmap)
    user_beatmap.attempt_count += 1
    user_beatmap.pass_count += int(passed)
    user_beatmap.last_played_at = max(user_beatmap.last_played_at or score.ended_at, score.ended_at)

    beatmap = await session.get(
        BeatmapActivity,
        {"beatmap_id": score.beatmap_id, "ruleset": score.ruleset},
        with_for_update=True,
    )
    if beatmap is None:
        beatmap = BeatmapActivity(
            beatmap_id=score.beatmap_id,
            ruleset=score.ruleset,
            attempt_count=0,
            pass_count=0,
        )
        session.add(beatmap)
    beatmap.attempt_count += 1
    beatmap.pass_count += int(passed)

    if score.outcome in {ScoreOutcome.FAILED, ScoreOutcome.ABANDONED}:
        histogram = await session.get(
            BeatmapFailHistogram,
            {"beatmap_id": score.beatmap_id, "ruleset": score.ruleset},
            with_for_update=True,
        )
        if histogram is None:
            histogram = BeatmapFailHistogram(
                beatmap_id=score.beatmap_id,
                ruleset=score.ruleset,
                failed=[0] * 100,
                quit=[0] * 100,
            )
            session.add(histogram)
        bucket = min(int(progress * 100), 99)
        values = list(histogram.failed if score.outcome is ScoreOutcome.FAILED else histogram.quit)
        values[bucket] += 1
        if score.outcome is ScoreOutcome.FAILED:
            histogram.failed = values
        else:
            histogram.quit = values


async def _apply_replay_view(session: AsyncSession, event: OutboxEvent, partition_key: str) -> None:
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
    dimensions = (
        await session.execute(select(Score.account_id, Score.ruleset).where(Score.id == score_id))
    ).one_or_none()
    if dimensions is None or dimensions.account_id != account_id or dimensions.ruleset.value != ruleset:
        raise RuntimeError("replay-view event does not match the authoritative score")

    play_stat = await session.get(
        UserPlayStat,
        {"account_id": account_id, "ruleset": dimensions.ruleset},
        with_for_update=True,
    )
    if play_stat is None:
        play_stat = UserPlayStat(
            account_id=account_id,
            ruleset=dimensions.ruleset,
            play_count=0,
            play_time_ms=0,
            total_score=0,
            total_hits=0,
            max_combo=0,
            replay_views=0,
        )
        session.add(play_stat)
    play_stat.replay_views += 1
    play_stat.source_event_id = event.id

    month = _month_start(event.created_at)
    monthly = await session.get(
        UserMonthlyActivity,
        {"account_id": account_id, "ruleset": dimensions.ruleset, "month": month},
        with_for_update=True,
    )
    if monthly is None:
        monthly = UserMonthlyActivity(
            account_id=account_id,
            ruleset=dimensions.ruleset,
            month=month,
            play_count=0,
            play_time_ms=0,
            replay_views=0,
        )
        session.add(monthly)
    monthly.replay_views += 1


async def _is_new_event(session: AsyncSession, event: OutboxEvent, partition_key: str) -> bool:
    checkpoint = await session.get(
        ConsumerCheckpoint,
        {"consumer": CONSUMER_NAME, "partition_key": partition_key},
        with_for_update=True,
    )
    return checkpoint is None or event.position > checkpoint.source_position


def _month_start(value: datetime) -> date:
    normalized = value.astimezone(UTC)
    return date(normalized.year, normalized.month, 1)
