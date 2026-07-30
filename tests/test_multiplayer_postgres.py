import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import func, select

from perfcho.infra.db import engine as infra_db
from perfcho.infra.db.enums import AccountStatus, AccountType, BeatmapStatus
from perfcho.infra.db.enums import Ruleset as DbRuleset
from perfcho.infra.db.models.content import Beatmap, BeatmapRevision, Beatmapset
from perfcho.infra.db.models.core import Account
from perfcho.infra.db.models.multiplayer import (
    MultiplayerAttempt,
    MultiplayerEvent,
    MultiplayerSession,
    Room,
    Round,
    SessionPresence,
)
from perfcho.infra.db.repositories.multiplayer import SqlAlchemyMultiplayerRepository
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


def test_multiplayer_partial_unique_indexes_cover_global_presence_and_active_round() -> None:
    presence_indexes = {index.name: index for index in SessionPresence.__table__.indexes}
    round_indexes = {index.name: index for index in Round.__table__.indexes}

    assert presence_indexes["uq_session_presences_account_current"].unique
    assert str(presence_indexes["uq_session_presences_account_current"].dialect_options["postgresql"]["where"])
    assert round_indexes["uq_rounds_session_active"].unique
    assert str(round_indexes["uq_rounds_session_active"].dialect_options["postgresql"]["where"])


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
                status=BeatmapStatus.RANKED,
            )
            session.add(beatmap)
            await session.flush()
            session.add(
                BeatmapRevision(
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
            )

        async with session_factory.begin() as session:
            repository = SqlAlchemyMultiplayerRepository(session)
            room = await repository.create_room(
                command_id=uuid.uuid7(),
                actor_account_id=1,
                connection_session_id=uuid.uuid7(),
                settings=settings(),
                capacity=16,
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
            assert await session.scalar(select(func.count()).select_from(MultiplayerAttempt)) == 2
            assert await session.scalar(select(func.count()).select_from(MultiplayerEvent)) == 5
            room_row = (await session.execute(select(Room))).scalar_one()
            assert room_row.public_id == 1
            attempts = tuple((await session.scalars(select(MultiplayerAttempt))).all())
            assert all(
                attempt.expires_at <= NOW.replace(microsecond=0) + timedelta(minutes=2, seconds=1)
                for attempt in attempts
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
