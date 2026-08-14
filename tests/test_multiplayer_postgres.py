import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import Table, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.consumers.multiplayer import CONSUMER_NAME, consume_multiplayer_results
from perfcho.infra.db import engine as infra_db
from perfcho.infra.db.enums import AccountStatus, AccountType, AttemptStatus, BeatmapStatus
from perfcho.infra.db.enums import ClientFamily as DbClientFamily
from perfcho.infra.db.enums import Ruleset as DbRuleset
from perfcho.infra.db.enums import ScoreGrade as DbScoreGrade
from perfcho.infra.db.enums import ScoreOutcome as DbScoreOutcome
from perfcho.infra.db.models.content import Beatmap, BeatmapRevision, Beatmapset
from perfcho.infra.db.models.core import Account
from perfcho.infra.db.models.events import ConsumerCheckpoint, OutboxDelivery, OutboxEvent
from perfcho.infra.db.models.multiplayer import (
    MultiplayerAttempt,
    MultiplayerEvent,
    MultiplayerSession,
    PlaylistItemUserSummary,
    PlaylistRevision,
    Room,
    RoomUserSummary,
    Round,
    RoundParticipant,
    RoundResult,
    SessionPresence,
    SessionStanding,
)
from perfcho.infra.db.models.scoring import PlayAttempt, Score
from perfcho.infra.db.repositories.multiplayer import SqlAlchemyMultiplayerRepository
from perfcho.infra.db.repositories.outbox import append_outbox_event
from perfcho.modules.common import PendingEvent
from perfcho.modules.multiplayer import (
    MatchAlreadyJoined,
    MatchStateRejected,
    RoomRecord,
    RoomSettings,
    RoundParticipantSelection,
    TeamMode,
    WinCondition,
)
from perfcho.modules.scoring import Ruleset, ScoreboardVariant

NOW = datetime(2099, 7, 29, 12, tzinfo=UTC)


async def _bind_round_score(
    session: AsyncSession,
    round_id: uuid.UUID,
    account_id: int,
    *,
    total_score: int,
    ended_at: datetime,
) -> tuple[int, int]:
    row = (
        await session.execute(
            select(
                MultiplayerAttempt,
                Round.started_at,
                PlaylistRevision.beatmap_revision_id,
                PlaylistRevision.scoreboard_id,
                RoundParticipant.mod_set_id,
                BeatmapRevision.beatmap_id,
            )
            .join(Round, Round.id == MultiplayerAttempt.round_id)
            .join(PlaylistRevision, PlaylistRevision.id == Round.playlist_revision_id)
            .join(
                RoundParticipant,
                (RoundParticipant.round_id == round_id) & (RoundParticipant.account_id == account_id),
            )
            .join(BeatmapRevision, BeatmapRevision.id == PlaylistRevision.beatmap_revision_id)
            .where(MultiplayerAttempt.round_id == round_id, MultiplayerAttempt.account_id == account_id)
            .with_for_update(of=MultiplayerAttempt)
        )
    ).one()
    multiplayer_attempt, round_started_at, revision_id, scoreboard_id, mod_set_id, beatmap_id = row
    play_attempt = PlayAttempt(
        id=uuid.uuid7(),
        account_id=account_id,
        beatmap_id=beatmap_id,
        beatmap_revision_id=revision_id,
        scoreboard_id=scoreboard_id,
        mod_set_id=mod_set_id,
        protocol=DbClientFamily.STABLE,
        idempotency_key=f"round:{round_id}:{account_id}",
        status=AttemptStatus.VERIFIED,
        started_at=round_started_at,
        ended_at=ended_at,
        outcome=DbScoreOutcome.PASSED,
        progress=Decimal(1),
    )
    session.add(play_attempt)
    await session.flush()
    score = Score(
        attempt_id=play_attempt.id,
        account_id=account_id,
        beatmap_id=beatmap_id,
        beatmap_revision_id=revision_id,
        scoreboard_id=scoreboard_id,
        mod_set_id=mod_set_id,
        total_score=total_score,
        classic_score=total_score,
        accuracy=Decimal("0.98"),
        max_combo=100,
        grade=DbScoreGrade.A,
        outcome=DbScoreOutcome.PASSED,
        perfect=False,
        client_flags=0,
        started_at=play_attempt.started_at,
        ended_at=ended_at,
        processed_at=ended_at,
    )
    session.add(score)
    await session.flush()
    multiplayer_attempt.play_attempt_id = play_attempt.id
    multiplayer_attempt.score_id = score.id
    multiplayer_attempt.status = AttemptStatus.VERIFIED
    multiplayer_attempt.consumed_at = ended_at
    return score.id, scoreboard_id


async def _append_round_completed(
    session: AsyncSession,
    room: RoomRecord,
    round_id: uuid.UUID,
    *,
    aborted: bool,
) -> uuid.UUID:
    event = await append_outbox_event(
        session,
        PendingEvent(
            aggregate_type="multiplayer_round",
            aggregate_id=str(round_id),
            event_type="multiplayer.round-completed.v1",
            schema_version=1,
            payload={
                "round_id": str(round_id),
                "session_id": str(room.session_id),
                "room_id": str(room.room_id),
                "aborted": aborted,
            },
            consumers=(CONSUMER_NAME,),
            partition_key=f"round:{round_id}",
        ),
    )
    return event.id


async def _append_score_accepted(session: AsyncSession, score_id: int, scoreboard_id: int) -> uuid.UUID:
    account_id = await session.scalar(select(Score.account_id).where(Score.id == score_id))
    assert account_id is not None
    event = await append_outbox_event(
        session,
        PendingEvent(
            aggregate_type="score",
            aggregate_id=str(score_id),
            event_type="score.accepted.v1",
            schema_version=1,
            payload={"score_id": score_id, "account_id": account_id, "scoreboard_id": scoreboard_id},
            consumers=(CONSUMER_NAME,),
            partition_key=f"account:{account_id}:scoreboard:{scoreboard_id}",
        ),
    )
    return event.id


def test_multiplayer_partial_unique_indexes_cover_global_presence_and_active_round() -> None:
    presence_table = SessionPresence.__table__
    round_table = Round.__table__
    assert isinstance(presence_table, Table)
    assert isinstance(round_table, Table)
    presence_index = next(
        index for index in presence_table.indexes if index.name == "uq_session_presence_account_current"
    )
    round_index = next(index for index in round_table.indexes if index.name == "uq_round_session_active")

    assert presence_index.unique
    assert str(presence_index.dialect_options["postgresql"]["where"])
    assert round_index.unique
    assert str(round_index.dialect_options["postgresql"]["where"])
    assert Room.__table__.c.public_id_epoch.identity is not None


def settings() -> RoomSettings:
    return RoomSettings(
        "PostgreSQL Room",
        "Artist - Title [Hard]",
        100,
        b"m" * 16,
        Ruleset.OSU,
        ScoreboardVariant.VANILLA,
        TeamMode.HEAD_TO_HEAD,
        WinCondition.SCORE,
    )


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_postgres_multiplayer_lifecycle_and_known_map_attempts(postgres_database_url: str) -> None:
    del postgres_database_url
    engine = await infra_db.create_engine()
    session_factory = infra_db.create_session_factory(engine)
    try:
        async with session_factory.begin() as session:
            session.add(
                Account(
                    id=2,
                    type=AccountType.USER,
                    status=AccountStatus.ACTIVE,
                    registered_at=NOW,
                    activated_at=NOW,
                )
            )
            beatmapset = Beatmapset(
                source_id=1,
                external_id=200,
                creator_name="Creator",
                artist="Artist",
                title="Title",
                status=BeatmapStatus.RANKED,
                source_status=BeatmapStatus.RANKED,
                available=True,
            )
            session.add(beatmapset)
            await session.flush()
            beatmap = Beatmap(
                beatmapset_id=beatmapset.id,
                source_id=1,
                external_id=100,
                ruleset=DbRuleset.OSU,
                difficulty_name="Hard",
            )
            session.add(beatmap)
            await session.flush()
            beatmap_revision = BeatmapRevision(
                beatmap_id=beatmap.id,
                md5=b"m" * 16,
                sha256=b"s" * 32,
                file_name="Artist - Title (Creator) [Hard].osu",
                file_name_key="artist - title (creator) [hard].osu",
                source_updated_at=NOW,
                total_length_ms=60_000,
                drain_length_ms=55_000,
                bpm=Decimal(180),
                circle_size=Decimal(4),
                overall_difficulty=Decimal(8),
                approach_rate=Decimal(9),
                health_drain=Decimal(6),
                object_count=100,
                circle_count=80,
                slider_count=20,
                spinner_count=0,
                max_combo=120,
                is_current=True,
            )
            session.add(beatmap_revision)
            await session.flush()
            beatmap_revision_id = beatmap_revision.id

        async with session_factory.begin() as session:
            repository = SqlAlchemyMultiplayerRepository(session)
            room = await repository.create_room(
                command_id=uuid.uuid7(),
                actor_account_id=1,
                connection_session_id=uuid.uuid7(),
                settings=settings(),
                capacity=16,
                public_id_limit=32767,
                protocol="test",
                password_salt=None,
                password_verifier=None,
                now=NOW,
            )
            joined = await repository.join_room(
                room,
                command_id=uuid.uuid7(),
                account_id=2,
                connection_session_id=uuid.uuid7(),
                now=NOW,
            )
            started, round_id = await repository.start_round(
                joined,
                command_id=uuid.uuid7(),
                actor_account_id=1,
                participants=(
                    RoundParticipantSelection(1, 0, 0),
                    RoundParticipantSelection(2, 1, 0),
                ),
                now=NOW,
            )
            assert round_id is not None
            snapshot = await repository.load_snapshot(started)
            assert snapshot.round_id == round_id
            assert tuple((item.account_id, item.slot_position, item.team) for item in snapshot.round_participants) == (
                (1, 0, 0),
                (2, 1, 0),
            )
            with pytest.raises(MatchStateRejected):
                await repository.update_settings(
                    started,
                    command_id=uuid.uuid7(),
                    actor_account_id=1,
                    settings=settings(),
                    now=NOW,
                )
            completed = await repository.complete_round(
                started,
                command_id=uuid.uuid7(),
                actor_account_id=1,
                round_id=round_id,
                aborted=False,
                now=NOW,
            )
            first_attempt_id = await session.scalar(
                select(MultiplayerAttempt.id).where(
                    MultiplayerAttempt.round_id == round_id,
                    MultiplayerAttempt.account_id == 1,
                )
            )
            rematch, rematch_id = await repository.start_round(
                completed,
                command_id=uuid.uuid7(),
                actor_account_id=1,
                participants=(
                    RoundParticipantSelection(1, 0, 0),
                    RoundParticipantSelection(2, 1, 0),
                ),
                now=NOW + timedelta(seconds=30),
            )
            context = await repository.resolve_submission_context(
                1,
                beatmap_revision_id,
                started_at=NOW,
                ended_at=NOW + timedelta(microseconds=1),
                at=NOW + timedelta(seconds=30),
            )
            assert context is not None and context.attempt_id == first_attempt_id
            assert rematch_id is not None
            completed = await repository.complete_round(
                rematch,
                command_id=uuid.uuid7(),
                actor_account_id=1,
                round_id=rematch_id,
                aborted=False,
                now=NOW + timedelta(seconds=31),
            )
            left = await repository.leave_room(
                completed,
                command_id=uuid.uuid7(),
                account_id=1,
                connection_session_id=None,
                reason="client_parted",
                now=NOW,
            )
            assert left is not None and left.host_account_id == 2

        async with session_factory() as session:
            assert await session.scalar(select(func.count()).select_from(Room)) == 1
            assert await session.scalar(select(func.count()).select_from(MultiplayerSession)) == 1
            assert await session.scalar(select(func.count()).select_from(SessionPresence)) == 2
            assert await session.scalar(select(func.count()).select_from(MultiplayerAttempt)) == 4
            assert await session.scalar(select(func.count()).select_from(MultiplayerEvent)) == 7
            room_row = (await session.execute(select(Room))).scalar_one()
            assert room_row.public_id == 1
            assert room_row.public_id_epoch > 0
            attempts = tuple((await session.scalars(select(MultiplayerAttempt))).all())
            assert all(
                attempt.expires_at <= NOW.replace(microsecond=0) + timedelta(minutes=2, seconds=32)
                for attempt in attempts
            )
    finally:
        await engine.dispose()


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_postgres_multiplayer_results_handle_normal_late_duplicate_and_abort(
    postgres_database_url: str,
) -> None:
    del postgres_database_url
    engine = await infra_db.create_engine()
    session_factory = infra_db.create_session_factory(engine)
    try:
        async with session_factory.begin() as session:
            session.add(
                Account(
                    id=2,
                    type=AccountType.USER,
                    status=AccountStatus.ACTIVE,
                    registered_at=NOW,
                    activated_at=NOW,
                )
            )
            beatmapset = Beatmapset(
                source_id=1,
                external_id=201,
                creator_name="Creator",
                artist="Artist",
                title="Projection",
                status=BeatmapStatus.RANKED,
                source_status=BeatmapStatus.RANKED,
                available=True,
            )
            session.add(beatmapset)
            await session.flush()
            beatmap = Beatmap(
                beatmapset_id=beatmapset.id,
                source_id=1,
                external_id=100,
                ruleset=DbRuleset.OSU,
                difficulty_name="Hard",
            )
            session.add(beatmap)
            await session.flush()
            session.add(
                BeatmapRevision(
                    beatmap_id=beatmap.id,
                    md5=b"m" * 16,
                    sha256=b"p" * 32,
                    file_name="Artist - Projection (Creator) [Hard].osu",
                    file_name_key="artist - projection (creator) [hard].osu",
                    source_updated_at=NOW,
                    total_length_ms=60_000,
                    drain_length_ms=55_000,
                    bpm=Decimal(180),
                    circle_size=Decimal(4),
                    overall_difficulty=Decimal(8),
                    approach_rate=Decimal(9),
                    health_drain=Decimal(6),
                    object_count=100,
                    circle_count=80,
                    slider_count=20,
                    spinner_count=0,
                    max_combo=120,
                    is_current=True,
                )
            )

        participants = (RoundParticipantSelection(1, 0, 0), RoundParticipantSelection(2, 1, 0))
        async with session_factory.begin() as session:
            repository = SqlAlchemyMultiplayerRepository(session)
            room = await repository.create_room(
                command_id=uuid.uuid7(),
                actor_account_id=1,
                connection_session_id=uuid.uuid7(),
                settings=settings(),
                capacity=16,
                public_id_limit=32767,
                protocol="test",
                password_salt=None,
                password_verifier=None,
                now=NOW,
            )
            room = await repository.join_room(
                room,
                command_id=uuid.uuid7(),
                account_id=2,
                connection_session_id=uuid.uuid7(),
                now=NOW,
            )
            room, first_round_id = await repository.start_round(
                room,
                command_id=uuid.uuid7(),
                actor_account_id=1,
                participants=participants,
                now=NOW,
            )
            assert first_round_id is not None
            first_score_id, _ = await _bind_round_score(
                session,
                first_round_id,
                1,
                total_score=950_000,
                ended_at=NOW + timedelta(seconds=30),
            )
            room = await repository.complete_round(
                room,
                command_id=uuid.uuid7(),
                actor_account_id=1,
                round_id=first_round_id,
                aborted=False,
                now=NOW + timedelta(seconds=31),
            )
            first_event_id = await _append_round_completed(session, room, first_round_id, aborted=False)
            playlist_item_id = await session.scalar(
                select(PlaylistRevision.item_id)
                .join(Round, Round.playlist_revision_id == PlaylistRevision.id)
                .where(Round.id == first_round_id)
            )
            assert playlist_item_id is not None

        async with session_factory.begin() as session:
            first_event = await session.get(OutboxEvent, first_event_id)
            assert first_event is not None
            await consume_multiplayer_results(session, first_event, f"round:{first_round_id}")

        async with session_factory() as session:
            first_result = await session.scalar(select(RoundResult).where(RoundResult.round_id == first_round_id))
            assert first_result is not None
            assert (
                first_result.account_id,
                first_result.score_id,
                first_result.rank,
                first_result.metric_value,
                first_result.points,
            ) == (1, first_score_id, 1, Decimal("950000.00000"), Decimal("1.0000"))
            standings = {
                standing.subject_key: standing.points
                for standing in await session.scalars(
                    select(SessionStanding).where(SessionStanding.session_id == room.session_id)
                )
            }
            assert standings == {"1": Decimal("1.0000"), "2": Decimal("0.0000")}

        async with session_factory.begin() as session:
            repository = SqlAlchemyMultiplayerRepository(session)
            room, second_round_id = await repository.start_round(
                room,
                command_id=uuid.uuid7(),
                actor_account_id=1,
                participants=participants,
                now=NOW + timedelta(seconds=60),
            )
            assert second_round_id is not None
            room = await repository.complete_round(
                room,
                command_id=uuid.uuid7(),
                actor_account_id=1,
                round_id=second_round_id,
                aborted=False,
                now=NOW + timedelta(seconds=90),
            )
            second_event_id = await _append_round_completed(session, room, second_round_id, aborted=False)

        async with session_factory.begin() as session:
            second_event = await session.get(OutboxEvent, second_event_id)
            assert second_event is not None
            await consume_multiplayer_results(session, second_event, f"round:{second_round_id}")

        async with session_factory() as session:
            assert (
                await session.scalar(
                    select(func.count()).select_from(RoundResult).where(RoundResult.round_id == second_round_id)
                )
                == 0
            )
            no_score_summary = await session.get(
                PlaylistItemUserSummary,
                {"playlist_item_id": playlist_item_id, "account_id": 1},
            )
            assert no_score_summary is not None
            assert (no_score_summary.attempt_count, no_score_summary.completion_count) == (2, 1)

        async with session_factory.begin() as session:
            late_score_id, scoreboard_id = await _bind_round_score(
                session,
                second_round_id,
                1,
                total_score=950_000,
                ended_at=NOW + timedelta(seconds=91),
            )
            score_event_id = await _append_score_accepted(session, late_score_id, scoreboard_id)

        async with session_factory.begin() as session:
            score_event = await session.get(OutboxEvent, score_event_id)
            assert score_event is not None
            score_partition = f"account:1:scoreboard:{scoreboard_id}"
            await consume_multiplayer_results(session, score_event, score_partition)
            late_result = await session.scalar(select(RoundResult).where(RoundResult.round_id == second_round_id))
            assert late_result is not None
            digest = late_result.result_digest
            await consume_multiplayer_results(session, score_event, score_partition)
            assert late_result.result_digest == digest

        async with session_factory.begin() as session:
            repository = SqlAlchemyMultiplayerRepository(session)
            room, aborted_round_id = await repository.start_round(
                room,
                command_id=uuid.uuid7(),
                actor_account_id=1,
                participants=participants,
                now=NOW + timedelta(seconds=120),
            )
            assert aborted_round_id is not None
            room = await repository.complete_round(
                room,
                command_id=uuid.uuid7(),
                actor_account_id=1,
                round_id=aborted_round_id,
                aborted=True,
                now=NOW + timedelta(seconds=150),
            )
            aborted_event_id = await _append_round_completed(session, room, aborted_round_id, aborted=True)

        async with session_factory.begin() as session:
            aborted_event = await session.get(OutboxEvent, aborted_event_id)
            assert aborted_event is not None
            await consume_multiplayer_results(session, aborted_event, f"round:{aborted_round_id}")

        async with session_factory() as session:
            assert (
                await session.scalar(
                    select(func.count()).select_from(RoundResult).where(RoundResult.round_id == aborted_round_id)
                )
                == 0
            )
            late_result = await session.scalar(select(RoundResult).where(RoundResult.round_id == second_round_id))
            assert late_result is not None and late_result.score_id == late_score_id
            standing = await session.get(
                SessionStanding,
                {"session_id": room.session_id, "subject_type": "account", "subject_key": "1"},
            )
            playlist_summary = await session.get(
                PlaylistItemUserSummary,
                {"playlist_item_id": playlist_item_id, "account_id": 1},
            )
            room_summary = await session.get(
                RoomUserSummary,
                {"room_id": room.room_id, "account_id": 1},
            )
            empty_playlist_summary = await session.get(
                PlaylistItemUserSummary,
                {"playlist_item_id": playlist_item_id, "account_id": 2},
            )
            empty_room_summary = await session.get(
                RoomUserSummary,
                {"room_id": room.room_id, "account_id": 2},
            )
            checkpoint = await session.get(
                ConsumerCheckpoint,
                {"consumer": CONSUMER_NAME, "partition_key": f"account:1:scoreboard:{scoreboard_id}"},
            )
            assert standing is not None and standing.points == Decimal("2.0000")
            assert playlist_summary is not None
            assert (
                playlist_summary.attempt_count,
                playlist_summary.completion_count,
                playlist_summary.best_score_id,
                playlist_summary.best_metric_value,
            ) == (3, 2, first_score_id, Decimal("950000.00000"))
            assert room_summary is not None
            assert (
                room_summary.attempt_count,
                room_summary.completion_count,
                room_summary.total_score,
                room_summary.total_performance,
                room_summary.average_accuracy,
            ) == (3, 2, 1_900_000, Decimal("0.00000"), Decimal("0.980000000"))
            assert empty_playlist_summary is not None
            assert (
                empty_playlist_summary.attempt_count,
                empty_playlist_summary.completion_count,
                empty_playlist_summary.best_score_id,
                empty_playlist_summary.best_metric_value,
            ) == (3, 0, None, None)
            assert empty_room_summary is not None
            assert (
                empty_room_summary.attempt_count,
                empty_room_summary.completion_count,
                empty_room_summary.total_score,
                empty_room_summary.total_performance,
                empty_room_summary.average_accuracy,
            ) == (3, 0, 0, Decimal("0.00000"), Decimal("0E-9"))
            assert checkpoint is not None and checkpoint.source_event_id == score_event_id
            assert (
                await session.scalar(
                    select(func.count()).select_from(OutboxDelivery).where(OutboxDelivery.consumer == CONSUMER_NAME)
                )
                == 4
            )
    finally:
        await engine.dispose()


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_postgres_concurrent_rooms_map_global_presence_conflict(postgres_database_url: str) -> None:
    del postgres_database_url
    engine = await infra_db.create_engine()
    session_factory = infra_db.create_session_factory(engine)
    try:
        async with session_factory.begin() as session:
            session.add_all(
                Account(
                    id=account_id,
                    type=AccountType.USER,
                    status=AccountStatus.ACTIVE,
                    registered_at=NOW,
                    activated_at=NOW,
                )
                for account_id in (1001, 1002)
            )

        async with session_factory.begin() as session:
            repository = SqlAlchemyMultiplayerRepository(session)
            first = await repository.create_room(
                command_id=uuid.uuid7(),
                actor_account_id=1,
                connection_session_id=uuid.uuid7(),
                settings=settings(),
                capacity=16,
                public_id_limit=32767,
                protocol="test",
                password_salt=None,
                password_verifier=None,
                now=NOW,
            )
            second = await repository.create_room(
                command_id=uuid.uuid7(),
                actor_account_id=1001,
                connection_session_id=uuid.uuid7(),
                settings=settings(),
                capacity=16,
                public_id_limit=32767,
                protocol="test",
                password_salt=None,
                password_verifier=None,
                now=NOW,
            )

        async def join(room: RoomRecord) -> object:
            async with session_factory() as session:
                repository = SqlAlchemyMultiplayerRepository(session)
                try:
                    result = await repository.join_room(
                        room,
                        command_id=uuid.uuid7(),
                        account_id=1002,
                        connection_session_id=uuid.uuid7(),
                        now=NOW,
                    )
                    await session.commit()
                    return result
                except Exception as error:
                    await session.rollback()
                    return error

        results = await asyncio.gather(join(first), join(second))

        assert sum(not isinstance(result, Exception) for result in results) == 1
        assert sum(isinstance(result, MatchAlreadyJoined) for result in results) == 1
        async with session_factory() as session:
            active_count = await session.scalar(
                select(func.count())
                .select_from(SessionPresence)
                .where(SessionPresence.account_id == 1002, SessionPresence.left_at.is_(None))
            )
            assert active_count == 1
    finally:
        await engine.dispose()
