"""Project completed multiplayer rounds from authoritative attempts and scores."""

import hashlib
import uuid
from collections import defaultdict
from decimal import Decimal
from typing import NamedTuple, NotRequired, TypedDict

import orjson
from sqlalchemy import Numeric, String, and_, case, cast, delete, func, literal, or_, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import InstrumentedAttribute

from perfcho.infra.db.models.events import OutboxEvent, ProjectionCheckpoint
from perfcho.infra.db.models.multiplayer import (
    MultiplayerAttempt,
    MultiplayerSession,
    PlaylistItemUserSummary,
    PlaylistRevision,
    RoomUserSummary,
    Round,
    RoundParticipant,
    RoundResult,
    SessionStanding,
)
from perfcho.infra.db.models.scoring import Score
from perfcho.infra.db.projectors.common import (
    advance_checkpoint,
    payload_boolean,
    payload_integer,
    payload_string,
    payload_uuid,
    require_event_context,
)

CONSUMER_NAME = "multiplayer-results-projector.v1"
EVENT_TYPES = frozenset({"multiplayer.round-completed.v1", "score.accepted.v1"})
_TEAM_MODES = frozenset({"team_vs", "tag_team_vs"})


class _MetricRow(NamedTuple):
    """Hold score values used by deterministic result calculations."""

    account_id: int
    team_number: int
    score_id: int
    total_score: int
    accuracy: Decimal
    max_combo: int


class _RoundResultValue(TypedDict):
    """Describe one row in a bulk RoundResult upsert."""

    round_id: uuid.UUID
    account_id: int | None
    team_number: int | None
    score_id: int | None
    rank: int
    metric_value: Decimal
    points: Decimal
    result_digest: NotRequired[bytes]


async def project_multiplayer_results(session: AsyncSession, event: OutboxEvent, partition_key: str) -> None:
    """Rebuild affected multiplayer result projections and advance one checkpoint."""
    if not await _is_new_event(session, event, partition_key):
        return

    round_id: uuid.UUID | None
    if event.event_type == "multiplayer.round-completed.v1":
        round_id = payload_uuid(event.payload, "round_id")
        payload_boolean(event.payload, "aborted")
        require_event_context(
            event,
            partition_key,
            aggregate_type="multiplayer_round",
            aggregate_id=str(round_id),
            expected_partition_key=f"round:{round_id}",
        )
    elif event.event_type == "score.accepted.v1":
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
            await session.execute(
                select(Score.account_id, Score.ruleset, MultiplayerAttempt.round_id)
                .outerjoin(MultiplayerAttempt, MultiplayerAttempt.score_id == Score.id)
                .where(Score.id == score_id)
            )
        ).one_or_none()
        if dimensions is None:
            raise RuntimeError("accepted score event does not match the authoritative score")
        authoritative_account_id, authoritative_ruleset, round_id = dimensions._tuple()
        if authoritative_ruleset.value != ruleset or authoritative_account_id != account_id:
            raise RuntimeError("accepted score event does not match the authoritative score")
    else:
        raise RuntimeError(f"unsupported multiplayer results event: {event.event_type}")

    if round_id is not None:
        await _project_round(session, round_id)
    await advance_checkpoint(session, event, projector=CONSUMER_NAME, partition_key=partition_key)


async def _project_round(session: AsyncSession, round_id: uuid.UUID) -> None:
    lifecycle = (
        await session.execute(
            select(Round, MultiplayerSession)
            .join(MultiplayerSession, MultiplayerSession.id == Round.session_id)
            .where(Round.id == round_id)
            .with_for_update(of=Round)
        )
    ).one_or_none()
    if lifecycle is None:
        raise RuntimeError("multiplayer result event references a missing round")
    round_row, multiplayer_session = lifecycle
    if round_row.status == "in_progress":
        return
    if round_row.status not in {"completed", "aborted"}:
        raise RuntimeError("multiplayer result event references an unprojectable round state")

    rows = (
        await session.execute(
            select(
                RoundParticipant.account_id,
                RoundParticipant.team_number,
                Score.id.label("score_id"),
                Score.total_score,
                Score.accuracy,
                Score.max_combo,
            )
            .outerjoin(
                MultiplayerAttempt,
                and_(
                    MultiplayerAttempt.round_id == RoundParticipant.round_id,
                    MultiplayerAttempt.account_id == RoundParticipant.account_id,
                ),
            )
            .outerjoin(Score, Score.id == MultiplayerAttempt.score_id)
            .where(RoundParticipant.round_id == round_id)
            .order_by(RoundParticipant.account_id)
        )
    ).all()
    scored: list[_MetricRow] = []
    if round_row.status == "completed":
        for row in rows:
            account_id, team_number, score_id, total_score, accuracy, max_combo = row._tuple()
            if score_id is not None and total_score is not None and accuracy is not None and max_combo is not None:
                scored.append(_MetricRow(account_id, team_number, score_id, total_score, accuracy, max_combo))
    team_mode = multiplayer_session.team_mode in _TEAM_MODES
    await _replace_round_results(session, round_row, scored, team_mode=team_mode)
    await _rebuild_session_standings(session, multiplayer_session, team_mode=team_mode)
    if round_row.playlist_revision_id is not None:
        await _rebuild_playlist_summaries(session, round_row.playlist_revision_id)
    await _rebuild_room_summaries(session, multiplayer_session.room_id)


async def _replace_round_results(
    session: AsyncSession,
    round_row: Round,
    scored: list[_MetricRow],
    *,
    team_mode: bool,
) -> None:
    scoring_mode = str(round_row.configuration.get("win_condition", "score"))
    if team_mode:
        grouped: dict[int, list[_MetricRow]] = defaultdict(list)
        for row in scored:
            if row.team_number > 0:
                grouped[row.team_number].append(row)
        team_ranked = sorted(
            ((team, _team_metric(values, scoring_mode)) for team, values in grouped.items()),
            key=lambda item: (-item[1], item[0]),
        )
        await session.execute(delete(RoundResult).where(RoundResult.round_id == round_row.id))
        await _upsert_round_results(
            session,
            [
                {
                    "round_id": round_row.id,
                    "account_id": None,
                    "team_number": team,
                    "score_id": None,
                    "rank": index,
                    "metric_value": metric,
                    "points": Decimal(len(team_ranked) - index + 1),
                }
                for index, (team, metric) in enumerate(team_ranked, start=1)
            ],
            subject=RoundResult.team_number,
        )
        return

    account_ranked = sorted(scored, key=lambda row: (-_score_metric(row, scoring_mode), row.account_id))
    await session.execute(delete(RoundResult).where(RoundResult.round_id == round_row.id))
    await _upsert_round_results(
        session,
        [
            {
                "round_id": round_row.id,
                "account_id": row.account_id,
                "team_number": None,
                "score_id": row.score_id,
                "rank": index,
                "metric_value": _score_metric(row, scoring_mode),
                "points": Decimal(len(account_ranked) - index + 1),
            }
            for index, row in enumerate(account_ranked, start=1)
        ],
        subject=RoundResult.account_id,
    )


async def _upsert_round_results(
    session: AsyncSession,
    values: list[_RoundResultValue],
    *,
    subject: InstrumentedAttribute,
) -> None:
    if not values:
        return
    for value in values:
        value["result_digest"] = _result_digest(
            value["round_id"],
            value["account_id"],
            value["team_number"],
            value["score_id"],
            value["rank"],
            value["metric_value"],
            value["points"],
        )
    statement = insert(RoundResult).values(values)
    await session.execute(
        statement.on_conflict_do_update(
            index_elements=(RoundResult.round_id, subject),
            index_where=subject.is_not(None),
            set_={
                "score_id": statement.excluded.score_id,
                "rank": statement.excluded.rank,
                "metric_value": statement.excluded.metric_value,
                "points": statement.excluded.points,
                "result_digest": statement.excluded.result_digest,
            },
        )
    )


async def _rebuild_session_standings(
    session: AsyncSession,
    multiplayer_session: MultiplayerSession,
    *,
    team_mode: bool,
) -> None:
    if team_mode:
        participant_subject = RoundParticipant.team_number
        result_subject = RoundResult.team_number
        participant_filter = RoundParticipant.team_number > 0
        subject_type = "team"
    else:
        participant_subject = RoundParticipant.account_id
        result_subject = RoundResult.account_id
        participant_filter = RoundParticipant.account_id.is_not(None)
        subject_type = "account"

    participant_keys = (
        select(cast(participant_subject, String).label("subject_key"))
        .join(Round, Round.id == RoundParticipant.round_id)
        .where(Round.session_id == multiplayer_session.id, participant_filter)
        .distinct()
        .cte("session_participant_keys")
    )
    point_totals = (
        select(
            cast(result_subject, String).label("subject_key"),
            func.sum(RoundResult.points).label("points"),
        )
        .join(Round, Round.id == RoundResult.round_id)
        .where(Round.session_id == multiplayer_session.id, result_subject.is_not(None))
        .group_by(result_subject)
        .cte("session_point_totals")
    )
    subject_still_exists = (
        select(participant_keys.c.subject_key)
        .where(participant_keys.c.subject_key == SessionStanding.subject_key)
        .exists()
    )
    await session.execute(
        delete(SessionStanding).where(
            SessionStanding.session_id == multiplayer_session.id,
            or_(SessionStanding.subject_type != subject_type, ~subject_still_exists),
        )
    )
    statement = insert(SessionStanding).from_select(
        ("session_id", "subject_type", "subject_key", "points", "version"),
        select(
            literal(multiplayer_session.id),
            literal(subject_type),
            participant_keys.c.subject_key,
            func.coalesce(point_totals.c.points, Decimal(0)),
            literal(multiplayer_session.version),
        )
        .select_from(participant_keys)
        .outerjoin(point_totals, point_totals.c.subject_key == participant_keys.c.subject_key),
    )
    await session.execute(
        statement.on_conflict_do_update(
            index_elements=(
                SessionStanding.session_id,
                SessionStanding.subject_type,
                SessionStanding.subject_key,
            ),
            set_={"points": statement.excluded.points, "version": statement.excluded.version},
        )
    )


async def _rebuild_playlist_summaries(session: AsyncSession, playlist_revision_id: uuid.UUID) -> None:
    item_id = await session.scalar(select(PlaylistRevision.item_id).where(PlaylistRevision.id == playlist_revision_id))
    if item_id is None:
        raise RuntimeError("multiplayer round references a missing playlist revision")

    metric_value = cast(
        case(
            (PlaylistRevision.scoring_mode == "accuracy", Score.accuracy),
            (PlaylistRevision.scoring_mode == "combo", Score.max_combo),
            else_=Score.total_score,
        ),
        Numeric(20, 5),
    )
    attempts = (
        select(
            MultiplayerAttempt.account_id,
            Round.status,
            Score.id.label("score_id"),
            metric_value.label("metric_value"),
        )
        .join(Round, Round.id == MultiplayerAttempt.round_id)
        .join(PlaylistRevision, PlaylistRevision.id == Round.playlist_revision_id)
        .outerjoin(Score, Score.id == MultiplayerAttempt.score_id)
        .where(PlaylistRevision.item_id == item_id)
        .cte("playlist_attempts")
    )
    aggregates = (
        select(
            attempts.c.account_id,
            func.count().label("attempt_count"),
            func.count(attempts.c.score_id).filter(attempts.c.status == "completed").label("completion_count"),
        )
        .group_by(attempts.c.account_id)
        .cte("playlist_aggregates")
    )
    ranked_scores = (
        select(
            attempts.c.account_id,
            attempts.c.score_id,
            attempts.c.metric_value,
            func.row_number()
            .over(
                partition_by=attempts.c.account_id,
                order_by=(attempts.c.metric_value.desc(), attempts.c.score_id.asc()),
            )
            .label("score_position"),
        )
        .where(attempts.c.status == "completed", attempts.c.score_id.is_not(None))
        .cte("playlist_ranked_scores")
    )
    best_scores = (
        select(ranked_scores.c.account_id, ranked_scores.c.score_id, ranked_scores.c.metric_value)
        .where(ranked_scores.c.score_position == 1)
        .cte("playlist_best_scores")
    )
    statement = insert(PlaylistItemUserSummary).from_select(
        (
            "playlist_item_id",
            "account_id",
            "attempt_count",
            "completion_count",
            "best_score_id",
            "best_metric_value",
        ),
        select(
            literal(item_id),
            aggregates.c.account_id,
            aggregates.c.attempt_count,
            aggregates.c.completion_count,
            best_scores.c.score_id,
            best_scores.c.metric_value,
        )
        .select_from(aggregates)
        .outerjoin(best_scores, best_scores.c.account_id == aggregates.c.account_id),
    )
    await session.execute(
        statement.on_conflict_do_update(
            index_elements=(PlaylistItemUserSummary.playlist_item_id, PlaylistItemUserSummary.account_id),
            set_={
                "attempt_count": statement.excluded.attempt_count,
                "completion_count": statement.excluded.completion_count,
                "best_score_id": statement.excluded.best_score_id,
                "best_metric_value": statement.excluded.best_metric_value,
            },
        )
    )


async def _rebuild_room_summaries(session: AsyncSession, room_id: uuid.UUID) -> None:
    attempts = (
        select(
            MultiplayerAttempt.account_id,
            Round.status,
            Score.id.label("score_id"),
            Score.total_score,
            Score.accuracy,
        )
        .join(Round, Round.id == MultiplayerAttempt.round_id)
        .join(MultiplayerSession, MultiplayerSession.id == Round.session_id)
        .outerjoin(Score, Score.id == MultiplayerAttempt.score_id)
        .where(MultiplayerSession.room_id == room_id)
        .cte("room_attempts")
    )
    completed = and_(attempts.c.status == "completed", attempts.c.score_id.is_not(None))
    summaries = (
        select(
            attempts.c.account_id,
            func.count().label("attempt_count"),
            func.count(attempts.c.score_id).filter(completed).label("completion_count"),
            func.coalesce(func.sum(attempts.c.total_score).filter(completed), 0).label("total_score"),
            cast(literal(Decimal(0)), Numeric(14, 5)).label("total_performance"),
            cast(
                func.coalesce(func.avg(attempts.c.accuracy).filter(completed), Decimal(0)),
                Numeric(10, 9),
            ).label("average_accuracy"),
        )
        .group_by(attempts.c.account_id)
        .cte("room_summaries")
    )
    statement = insert(RoomUserSummary).from_select(
        (
            "room_id",
            "account_id",
            "attempt_count",
            "completion_count",
            "total_score",
            "total_performance",
            "average_accuracy",
        ),
        select(
            literal(room_id),
            summaries.c.account_id,
            summaries.c.attempt_count,
            summaries.c.completion_count,
            summaries.c.total_score,
            summaries.c.total_performance,
            summaries.c.average_accuracy,
        ),
    )
    await session.execute(
        statement.on_conflict_do_update(
            index_elements=(RoomUserSummary.room_id, RoomUserSummary.account_id),
            set_={
                "attempt_count": statement.excluded.attempt_count,
                "completion_count": statement.excluded.completion_count,
                "total_score": statement.excluded.total_score,
                "total_performance": statement.excluded.total_performance,
                "average_accuracy": statement.excluded.average_accuracy,
            },
        )
    )


def _score_metric(row: _MetricRow, scoring_mode: str) -> Decimal:
    if scoring_mode == "accuracy":
        return Decimal(row.accuracy)
    if scoring_mode == "combo":
        return Decimal(row.max_combo)
    return Decimal(row.total_score)


def _team_metric(rows: list[_MetricRow], scoring_mode: str) -> Decimal:
    values = [_score_metric(row, scoring_mode) for row in rows]
    if scoring_mode == "accuracy":
        return sum(values, start=Decimal(0)) / len(values)
    return sum(values, start=Decimal(0))


def _result_digest(
    round_id: uuid.UUID,
    account_id: int | None,
    team_number: int | None,
    score_id: int | None,
    rank: int,
    metric: Decimal,
    points: Decimal,
) -> bytes:
    payload = {
        "account_id": account_id,
        "metric": format(metric.normalize(), "f"),
        "points": format(points.normalize(), "f"),
        "rank": rank,
        "round_id": str(round_id),
        "score_id": score_id,
        "team_number": team_number,
    }
    return hashlib.sha256(orjson.dumps(payload, option=orjson.OPT_SORT_KEYS)).digest()


async def _is_new_event(session: AsyncSession, event: OutboxEvent, partition_key: str) -> bool:
    checkpoint = await session.get(
        ProjectionCheckpoint,
        {"projector": CONSUMER_NAME, "partition_key": partition_key},
        with_for_update=True,
    )
    return checkpoint is None or event.position > checkpoint.source_position
