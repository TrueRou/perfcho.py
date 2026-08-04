"""Project completed multiplayer rounds from authoritative attempts and scores."""

import hashlib
import json
import uuid
from collections import defaultdict
from decimal import Decimal
from typing import Protocol

from sqlalchemy import and_, delete, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

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
    payload_uuid,
    require_event_context,
)

CONSUMER_NAME = "multiplayer-results-projector.v1"
EVENT_TYPES = frozenset({"multiplayer.round-completed.v1", "score.accepted.v1"})
_TEAM_MODES = frozenset({"team_vs", "tag_team_vs"})


class _MetricRow(Protocol):
    """Expose score columns used by deterministic result calculations."""

    total_score: int
    accuracy: Decimal
    max_combo: int


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
        scoreboard_id = payload_integer(event.payload, "scoreboard_id")
        require_event_context(
            event,
            partition_key,
            aggregate_type="score",
            aggregate_id=str(score_id),
            expected_partition_key=f"scoreboard:{scoreboard_id}",
        )
        dimensions = (
            await session.execute(
                select(Score.scoreboard_id, MultiplayerAttempt.round_id)
                .outerjoin(MultiplayerAttempt, MultiplayerAttempt.score_id == Score.id)
                .where(Score.id == score_id)
            )
        ).one_or_none()
        if dimensions is None or dimensions.scoreboard_id != scoreboard_id:
            raise RuntimeError("accepted score event does not match the authoritative score")
        round_id = dimensions.round_id
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
    scored = [row for row in rows if round_row.status == "completed" and row.score_id is not None]
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
        ranked = sorted(
            ((team, _team_metric(values, scoring_mode)) for team, values in grouped.items()),
            key=lambda item: (-item[1], item[0]),
        )
        await session.execute(delete(RoundResult).where(RoundResult.round_id == round_row.id))
        for index, (team, metric) in enumerate(ranked, start=1):
            points = Decimal(len(ranked) - index + 1)
            await _upsert_round_result(
                session,
                round_id=round_row.id,
                account_id=None,
                team_number=team,
                score_id=None,
                rank=index,
                metric=metric,
                points=points,
            )
        return

    ranked = sorted(scored, key=lambda row: (-_score_metric(row, scoring_mode), row.account_id))
    await session.execute(delete(RoundResult).where(RoundResult.round_id == round_row.id))
    for index, row in enumerate(ranked, start=1):
        metric = _score_metric(row, scoring_mode)
        points = Decimal(len(ranked) - index + 1)
        await _upsert_round_result(
            session,
            round_id=round_row.id,
            account_id=row.account_id,
            team_number=None,
            score_id=row.score_id,
            rank=index,
            metric=metric,
            points=points,
        )


async def _upsert_round_result(
    session: AsyncSession,
    *,
    round_id: uuid.UUID,
    account_id: int | None,
    team_number: int | None,
    score_id: int | None,
    rank: int,
    metric: Decimal,
    points: Decimal,
) -> None:
    digest = _result_digest(round_id, account_id, team_number, score_id, rank, metric, points)
    statement = insert(RoundResult).values(
        round_id=round_id,
        account_id=account_id,
        team_number=team_number,
        score_id=score_id,
        rank=rank,
        metric_value=metric,
        points=points,
        result_digest=digest,
    )
    subject = RoundResult.account_id if account_id is not None else RoundResult.team_number
    await session.execute(
        statement.on_conflict_do_update(
            index_elements=(RoundResult.round_id, subject),
            index_where=subject.is_not(None),
            set_={
                "score_id": score_id,
                "rank": rank,
                "metric_value": metric,
                "points": points,
                "result_digest": digest,
            },
        )
    )


async def _rebuild_session_standings(
    session: AsyncSession,
    multiplayer_session: MultiplayerSession,
    *,
    team_mode: bool,
) -> None:
    participant_rows = (
        await session.execute(
            select(RoundParticipant.account_id, RoundParticipant.team_number)
            .join(Round, Round.id == RoundParticipant.round_id)
            .where(Round.session_id == multiplayer_session.id)
        )
    ).all()
    result_rows = (
        await session.execute(
            select(RoundResult.account_id, RoundResult.team_number, RoundResult.points)
            .join(Round, Round.id == RoundResult.round_id)
            .where(Round.session_id == multiplayer_session.id)
        )
    ).all()
    if team_mode:
        keys = {str(row.team_number) for row in participant_rows if row.team_number > 0}
        points = defaultdict(Decimal)
        for row in result_rows:
            if row.team_number is not None:
                points[str(row.team_number)] += row.points
        subject_type = "team"
    else:
        keys = {str(row.account_id) for row in participant_rows}
        points = defaultdict(Decimal)
        for row in result_rows:
            if row.account_id is not None:
                points[str(row.account_id)] += row.points
        subject_type = "account"

    await session.execute(
        delete(SessionStanding).where(
            SessionStanding.session_id == multiplayer_session.id,
            (SessionStanding.subject_type != subject_type) | SessionStanding.subject_key.not_in(keys),
        )
    )
    for key in sorted(keys, key=int):
        statement = insert(SessionStanding).values(
            session_id=multiplayer_session.id,
            subject_type=subject_type,
            subject_key=key,
            points=points[key],
            version=multiplayer_session.version,
        )
        await session.execute(
            statement.on_conflict_do_update(
                index_elements=(
                    SessionStanding.session_id,
                    SessionStanding.subject_type,
                    SessionStanding.subject_key,
                ),
                set_={"points": points[key], "version": multiplayer_session.version},
            )
        )


async def _rebuild_playlist_summaries(session: AsyncSession, playlist_revision_id: uuid.UUID) -> None:
    item_id = await session.scalar(select(PlaylistRevision.item_id).where(PlaylistRevision.id == playlist_revision_id))
    if item_id is None:
        raise RuntimeError("multiplayer round references a missing playlist revision")
    rows = (
        await session.execute(
            select(
                MultiplayerAttempt.account_id,
                Round.status,
                PlaylistRevision.scoring_mode,
                Score.id.label("score_id"),
                Score.total_score,
                Score.accuracy,
                Score.max_combo,
            )
            .join(Round, Round.id == MultiplayerAttempt.round_id)
            .join(PlaylistRevision, PlaylistRevision.id == Round.playlist_revision_id)
            .outerjoin(Score, Score.id == MultiplayerAttempt.score_id)
            .where(PlaylistRevision.item_id == item_id)
        )
    ).all()
    by_account: dict[int, list[_MetricRow]] = defaultdict(list)
    for row in rows:
        by_account[row.account_id].append(row)
    for account_id, attempts in by_account.items():
        completed = [row for row in attempts if row.status == "completed" and row.score_id is not None]
        best = max(completed, key=lambda row: (_score_metric(row, row.scoring_mode), -row.score_id), default=None)
        statement = insert(PlaylistItemUserSummary).values(
            playlist_item_id=item_id,
            account_id=account_id,
            attempt_count=len(attempts),
            completion_count=len(completed),
            best_score_id=best.score_id if best is not None else None,
            best_metric_value=_score_metric(best, best.scoring_mode) if best is not None else None,
        )
        await session.execute(
            statement.on_conflict_do_update(
                index_elements=(PlaylistItemUserSummary.playlist_item_id, PlaylistItemUserSummary.account_id),
                set_={
                    "attempt_count": len(attempts),
                    "completion_count": len(completed),
                    "best_score_id": best.score_id if best is not None else None,
                    "best_metric_value": _score_metric(best, best.scoring_mode) if best is not None else None,
                },
            )
        )


async def _rebuild_room_summaries(session: AsyncSession, room_id: uuid.UUID) -> None:
    rows = (
        await session.execute(
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
        )
    ).all()
    by_account: dict[int, list[_MetricRow]] = defaultdict(list)
    for row in rows:
        by_account[row.account_id].append(row)
    for account_id, attempts in by_account.items():
        completed = [row for row in attempts if row.status == "completed" and row.score_id is not None]
        total_score = sum((row.total_score for row in completed), start=0)
        average_accuracy = (
            sum((row.accuracy for row in completed), start=Decimal(0)) / len(completed) if completed else Decimal(0)
        )
        statement = insert(RoomUserSummary).values(
            room_id=room_id,
            account_id=account_id,
            attempt_count=len(attempts),
            completion_count=len(completed),
            total_score=total_score,
            total_performance=Decimal(0),
            average_accuracy=average_accuracy,
        )
        await session.execute(
            statement.on_conflict_do_update(
                index_elements=(RoomUserSummary.room_id, RoomUserSummary.account_id),
                set_={
                    "attempt_count": len(attempts),
                    "completion_count": len(completed),
                    "total_score": total_score,
                    "total_performance": Decimal(0),
                    "average_accuracy": average_accuracy,
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
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).digest()


async def _is_new_event(session: AsyncSession, event: OutboxEvent, partition_key: str) -> bool:
    checkpoint = await session.get(
        ProjectionCheckpoint,
        {"projector": CONSUMER_NAME, "partition_key": partition_key},
        with_for_update=True,
    )
    return checkpoint is None or event.position > checkpoint.source_position
