"""Persist authoritative multiplayer room lifecycles in caller-owned transactions."""

import secrets
import uuid
from datetime import datetime, timedelta

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra.db.advisory_lock import acquire_transaction_lock
from perfcho.infra.db.enums import (
    AttemptStatus,
    RoomStatus,
    SessionStatus,
)
from perfcho.infra.db.enums import (
    Ruleset as DbRuleset,
)
from perfcho.infra.db.enums import (
    ScoreboardVariant as DbScoreboardVariant,
)
from perfcho.infra.db.models.content import Beatmap, BeatmapRevision
from perfcho.infra.db.models.multiplayer import (
    MultiplayerAttempt,
    MultiplayerEvent,
    MultiplayerSession,
    PlaylistItem,
    PlaylistRevision,
    Room,
    RoomParticipant,
    Round,
    RoundParticipant,
    SessionPresence,
)
from perfcho.infra.db.models.scoring import ModSet, RankingPolicy, Scoreboard
from perfcho.modules.multiplayer import (
    MatchAlreadyJoined,
    MatchConcurrencyConflict,
    MatchFull,
    MatchNotFound,
    MatchPermissionDenied,
    RoomRecord,
    RoomSettings,
    RoundParticipantSelection,
    TeamMode,
    WinCondition,
)
from perfcho.modules.scoring.models import CanonicalMod, MultiplayerSubmissionContext, Ruleset, ScoreboardVariant
from perfcho.modules.scoring.mods import canonical_json_digest, normalize_mods


class SqlAlchemyMultiplayerRepository:
    """Write room facts and ordered events through one AsyncSession."""

    def __init__(self, session: AsyncSession) -> None:
        """Bind all operations to a caller-owned transaction."""
        self._session = session

    async def create_room(
        self,
        *,
        command_id: uuid.UUID,
        actor_account_id: int,
        settings: RoomSettings,
        capacity: int,
        password_salt: str | None,
        password_verifier: str | None,
        now: datetime,
    ) -> RoomRecord:
        """Create room, session, host admission, presence, and first event."""
        replay = await self._room_for_command(command_id)
        if replay is not None:
            return replay
        if await self.find_room_for_account(actor_account_id) is not None:
            raise MatchAlreadyJoined("account already has an active multiplayer presence")

        await acquire_transaction_lock(self._session, "stable-multiplayer-public-id")
        active_ids = set(
            (
                await self._session.scalars(
                    select(Room.public_id).where(Room.status.in_((RoomStatus.OPEN, RoomStatus.STARTED)))
                )
            ).all()
        )
        public_id = next((identifier for identifier in range(1, 32768) if identifier not in active_ids), None)
        if public_id is None:
            raise MatchFull("no Stable multiplayer identifiers are available")

        room = Room(
            id=uuid.uuid7(),
            public_id=public_id,
            creator_account_id=actor_account_id,
            name=settings.name,
            password_verifier=password_verifier,
            password_prefix=password_salt,
            category="realtime",
            format="playlist",
            visibility="public",
            status=RoomStatus.OPEN,
            ranked=False,
            capacity=capacity,
            starts_at=now,
            configuration=_settings_json(settings),
        )
        self._session.add(room)
        await self._session.flush()
        session = MultiplayerSession(
            id=uuid.uuid7(),
            room_id=room.id,
            ordinal=1,
            host_account_id=actor_account_id,
            protocol="stable",
            team_mode=settings.team_mode.value,
            scoring_mode=settings.win_condition.value,
            status=SessionStatus.ACTIVE,
            version=1,
        )
        self._session.add(session)
        await self._session.flush()
        self._session.add_all(
            (
                RoomParticipant(
                    room_id=room.id,
                    account_id=actor_account_id,
                    admission_jti=uuid.uuid7(),
                    participant_kind="player",
                    status="active",
                    last_activity_at=now,
                ),
                SessionPresence(
                    id=uuid.uuid7(),
                    session_id=session.id,
                    room_id=room.id,
                    account_id=actor_account_id,
                    join_number=1,
                ),
            )
        )
        await self._append_event(
            room,
            session,
            command_id=command_id,
            actor_account_id=actor_account_id,
            event_type="multiplayer.room-created.v1",
            payload={"public_id": room.public_id, "capacity": capacity},
        )
        await self._session.flush()
        return _record(room, session)

    async def get_room(self, public_id: int, *, for_update: bool = False) -> RoomRecord | None:
        """Resolve an active room and its current hosting session."""
        statement = (
            select(Room, MultiplayerSession)
            .join(MultiplayerSession, MultiplayerSession.room_id == Room.id)
            .where(
                Room.public_id == public_id,
                Room.status.in_((RoomStatus.OPEN, RoomStatus.STARTED)),
                MultiplayerSession.status == SessionStatus.ACTIVE,
            )
            .order_by(MultiplayerSession.ordinal.desc())
            .limit(1)
        )
        if for_update:
            statement = statement.with_for_update(of=(Room, MultiplayerSession))
        row = (await self._session.execute(statement)).one_or_none()
        if row is None:
            return None
        return _record(row[0], row[1])

    async def find_room_for_account(self, account_id: int) -> RoomRecord | None:
        """Resolve one current presence across active hosted sessions."""
        row = (
            await self._session.execute(
                select(Room, MultiplayerSession)
                .join(MultiplayerSession, MultiplayerSession.room_id == Room.id)
                .join(
                    SessionPresence,
                    (SessionPresence.session_id == MultiplayerSession.id) & (SessionPresence.room_id == Room.id),
                )
                .where(
                    SessionPresence.account_id == account_id,
                    SessionPresence.left_at.is_(None),
                    Room.status.in_((RoomStatus.OPEN, RoomStatus.STARTED)),
                    MultiplayerSession.status == SessionStatus.ACTIVE,
                )
                .order_by(SessionPresence.created_at.desc())
                .limit(1)
            )
        ).one_or_none()
        return _record(row[0], row[1]) if row is not None else None

    async def list_participant_account_ids(self, room: RoomRecord) -> tuple[int, ...]:
        """List current presences in deterministic join order with host first."""
        accounts = tuple(
            (
                await self._session.scalars(
                    select(SessionPresence.account_id)
                    .where(
                        SessionPresence.session_id == room.session_id,
                        SessionPresence.left_at.is_(None),
                    )
                    .order_by(SessionPresence.created_at, SessionPresence.id)
                )
            ).all()
        )
        if room.host_account_id not in accounts:
            return accounts
        return (room.host_account_id, *(account for account in accounts if account != room.host_account_id))

    async def join_room(self, room: RoomRecord, *, command_id: uuid.UUID, account_id: int, now: datetime) -> RoomRecord:
        """Admit an account and append exactly one active presence."""
        durable = await self.get_room(room.public_id, for_update=True)
        if durable is None:
            raise MatchNotFound("room is not active")
        current_room = await self.find_room_for_account(account_id)
        if current_room is not None:
            if current_room.public_id == room.public_id:
                return durable
            raise MatchAlreadyJoined("account already has an active multiplayer presence")
        active_count = await self._session.scalar(
            select(func.count())
            .select_from(SessionPresence)
            .where(SessionPresence.session_id == durable.session_id, SessionPresence.left_at.is_(None))
        )
        if active_count is None or active_count >= durable.capacity:
            raise MatchFull("room has no available participant capacity")
        participant = await self._session.get(RoomParticipant, {"room_id": durable.room_id, "account_id": account_id})
        if participant is None:
            self._session.add(
                RoomParticipant(
                    room_id=durable.room_id,
                    account_id=account_id,
                    admission_jti=uuid.uuid7(),
                    participant_kind="player",
                    status="active",
                    last_activity_at=now,
                )
            )
        else:
            if participant.banned_at is not None or participant.status == "banned":
                raise MatchPermissionDenied("account is banned from the room")
            participant.status = "active"
            participant.last_activity_at = now
        join_number = (
            await self._session.scalar(
                select(func.coalesce(func.max(SessionPresence.join_number), 0)).where(
                    SessionPresence.session_id == durable.session_id,
                    SessionPresence.account_id == account_id,
                )
            )
            or 0
        ) + 1
        self._session.add(
            SessionPresence(
                id=uuid.uuid7(),
                session_id=durable.session_id,
                room_id=durable.room_id,
                account_id=account_id,
                join_number=join_number,
            )
        )
        db_room, session = await self._entities(durable.public_id)
        session.version += 1
        await self._append_event(
            db_room,
            session,
            command_id=command_id,
            actor_account_id=account_id,
            event_type="multiplayer.participant-joined.v1",
            payload={"account_id": account_id, "join_number": join_number},
        )
        await self._session.flush()
        return _record(db_room, session)

    async def leave_room(
        self,
        room: RoomRecord,
        *,
        command_id: uuid.UUID,
        account_id: int,
        now: datetime,
    ) -> RoomRecord | None:
        """Close current presence and transfer host or close an empty room."""
        db_room, session = await self._entities(room.public_id)
        presence = (
            await self._session.execute(
                select(SessionPresence)
                .where(
                    SessionPresence.session_id == session.id,
                    SessionPresence.account_id == account_id,
                    SessionPresence.left_at.is_(None),
                )
                .with_for_update()
            )
        ).scalar_one_or_none()
        if presence is None:
            return _record(db_room, session)
        presence.left_at = now
        presence.leave_reason = "client_parted"
        participant = await self._session.get(RoomParticipant, {"room_id": db_room.id, "account_id": account_id})
        if participant is not None:
            participant.last_activity_at = now
        remaining = (
            (
                await self._session.execute(
                    select(SessionPresence.account_id)
                    .where(
                        SessionPresence.session_id == session.id,
                        SessionPresence.account_id != account_id,
                        SessionPresence.left_at.is_(None),
                    )
                    .order_by(SessionPresence.created_at, SessionPresence.id)
                )
            )
            .scalars()
            .all()
        )
        session.version += 1
        if session.host_account_id == account_id:
            session.host_account_id = remaining[0] if remaining else None
        closing = not remaining
        if closing:
            room_start = db_room.starts_at or session.created_at
            end_at = max(now, room_start + timedelta(microseconds=1))
            if session.created_at is not None:
                end_at = max(end_at, session.created_at + timedelta(microseconds=1))
            session.status = SessionStatus.COMPLETED
            session.ended_at = end_at
            db_room.status = RoomStatus.ENDED
            db_room.ends_at = end_at
        await self._append_event(
            db_room,
            session,
            command_id=command_id,
            actor_account_id=account_id,
            event_type="multiplayer.room-closed.v1" if closing else "multiplayer.participant-left.v1",
            payload={"account_id": account_id, "new_host_account_id": session.host_account_id},
        )
        await self._session.flush()
        return None if closing else _record(db_room, session)

    async def kick_participant(
        self,
        room: RoomRecord,
        *,
        command_id: uuid.UUID,
        actor_account_id: int,
        target_account_id: int,
        now: datetime,
    ) -> RoomRecord:
        """Close a target presence and record the host-authorized removal."""
        db_room, session = await self._entities(room.public_id)
        _require_host_version(room, session, actor_account_id)
        if target_account_id == actor_account_id:
            raise MatchPermissionDenied("the host cannot kick itself")
        presence = (
            await self._session.execute(
                select(SessionPresence)
                .where(
                    SessionPresence.session_id == session.id,
                    SessionPresence.account_id == target_account_id,
                    SessionPresence.left_at.is_(None),
                )
                .with_for_update()
            )
        ).scalar_one_or_none()
        if presence is None:
            raise MatchNotFound("target participant is not present")
        presence.left_at = now
        presence.leave_reason = "kicked"
        participant = await self._session.get(
            RoomParticipant,
            {"room_id": db_room.id, "account_id": target_account_id},
        )
        if participant is not None:
            participant.last_activity_at = now
        session.version += 1
        await self._append_event(
            db_room,
            session,
            command_id=command_id,
            actor_account_id=actor_account_id,
            event_type="multiplayer.participant-kicked.v1",
            payload={"account_id": target_account_id},
        )
        await self._session.flush()
        return _record(db_room, session)

    async def update_settings(
        self,
        room: RoomRecord,
        *,
        command_id: uuid.UUID,
        actor_account_id: int,
        settings: RoomSettings,
        now: datetime,
    ) -> RoomRecord:
        """Replace durable room settings and increment the session version."""
        del now
        db_room, session = await self._entities(room.public_id)
        _require_host_version(room, session, actor_account_id)
        db_room.name = settings.name
        db_room.configuration = _settings_json(settings)
        session.team_mode = settings.team_mode.value
        session.scoring_mode = settings.win_condition.value
        session.version += 1
        await self._append_event(
            db_room,
            session,
            command_id=command_id,
            actor_account_id=actor_account_id,
            event_type="multiplayer.settings-updated.v1",
            payload={"settings": _settings_json(settings)},
        )
        await self._session.flush()
        return _record(db_room, session)

    async def change_host(
        self,
        room: RoomRecord,
        *,
        command_id: uuid.UUID,
        actor_account_id: int,
        target_account_id: int,
        now: datetime,
    ) -> RoomRecord:
        """Transfer host to a currently present account."""
        del now
        db_room, session = await self._entities(room.public_id)
        _require_host_version(room, session, actor_account_id)
        target = await self._session.scalar(
            select(SessionPresence.id).where(
                SessionPresence.session_id == session.id,
                SessionPresence.account_id == target_account_id,
                SessionPresence.left_at.is_(None),
            )
        )
        if target is None:
            raise MatchNotFound("target participant is not present")
        session.host_account_id = target_account_id
        session.version += 1
        await self._append_event(
            db_room,
            session,
            command_id=command_id,
            actor_account_id=actor_account_id,
            event_type="multiplayer.host-changed.v1",
            payload={"host_account_id": target_account_id},
        )
        await self._session.flush()
        return _record(db_room, session)

    async def change_password(
        self,
        room: RoomRecord,
        *,
        command_id: uuid.UUID,
        actor_account_id: int,
        password_salt: str | None,
        password_verifier: str | None,
        now: datetime,
    ) -> RoomRecord:
        """Replace password proof fields without persisting plaintext."""
        del now
        db_room, session = await self._entities(room.public_id)
        _require_host_version(room, session, actor_account_id)
        db_room.password_prefix = password_salt
        db_room.password_verifier = password_verifier
        session.version += 1
        await self._append_event(
            db_room,
            session,
            command_id=command_id,
            actor_account_id=actor_account_id,
            event_type="multiplayer.password-changed.v1",
            payload={"password_protected": password_verifier is not None},
        )
        await self._session.flush()
        return _record(db_room, session)

    async def start_round(
        self,
        room: RoomRecord,
        *,
        command_id: uuid.UUID,
        actor_account_id: int,
        participants: tuple[RoundParticipantSelection, ...],
        now: datetime,
    ) -> tuple[RoomRecord, uuid.UUID | None]:
        """Freeze known content dimensions or append an explicitly unranked start."""
        db_room, session = await self._entities(room.public_id)
        _require_host_version(room, session, actor_account_id)
        frozen_round = await self._create_known_content_round(
            db_room,
            session,
            participants=participants,
            now=now,
        )
        session.version += 1
        await self._append_event(
            db_room,
            session,
            command_id=command_id,
            actor_account_id=actor_account_id,
            event_type="multiplayer.round-started.v1",
            payload={
                "account_ids": [participant.account_id for participant in participants],
                "settings": dict(db_room.configuration),
                "round_id": str(frozen_round.id) if frozen_round is not None else None,
                "ranked_context": frozen_round is not None,
            },
        )
        await self._session.flush()
        return _record(db_room, session), frozen_round.id if frozen_round is not None else None

    async def complete_round(
        self,
        room: RoomRecord,
        *,
        command_id: uuid.UUID,
        actor_account_id: int,
        round_id: uuid.UUID | None,
        aborted: bool,
        now: datetime,
    ) -> RoomRecord:
        """Append completion for the current Stable round."""
        db_room, session = await self._entities(room.public_id)
        _require_version(room, session)
        if round_id is not None:
            frozen_round = await self._session.get(Round, round_id, with_for_update=True)
            if frozen_round is not None and frozen_round.ended_at is None:
                frozen_round.status = "aborted" if aborted else "completed"
                started_at = frozen_round.started_at or frozen_round.created_at
                frozen_round.ended_at = max(now, started_at + timedelta(microseconds=1))
        session.version += 1
        await self._append_event(
            db_room,
            session,
            command_id=command_id,
            actor_account_id=actor_account_id,
            event_type="multiplayer.round-aborted.v1" if aborted else "multiplayer.round-completed.v1",
            payload={"round_id": str(round_id) if round_id is not None else None},
        )
        await self._session.flush()
        return _record(db_room, session)

    async def resolve_submission_context(
        self,
        account_id: int,
        beatmap_revision_id: int,
        *,
        at: datetime,
    ) -> MultiplayerSubmissionContext | None:
        """Resolve the newest usable attempt with matching frozen content."""
        row = (
            await self._session.execute(
                select(MultiplayerAttempt.id, MultiplayerAttempt.token_digest)
                .join(Round, Round.id == MultiplayerAttempt.round_id)
                .join(PlaylistRevision, PlaylistRevision.id == Round.playlist_revision_id)
                .where(
                    MultiplayerAttempt.account_id == account_id,
                    MultiplayerAttempt.status.in_((AttemptStatus.ISSUED, AttemptStatus.STARTED)),
                    MultiplayerAttempt.expires_at > at,
                    MultiplayerAttempt.score_id.is_(None),
                    PlaylistRevision.beatmap_revision_id == beatmap_revision_id,
                )
                .order_by(MultiplayerAttempt.created_at.desc())
                .limit(1)
            )
        ).one_or_none()
        return MultiplayerSubmissionContext(row.id, row.token_digest) if row is not None else None

    async def _create_known_content_round(
        self,
        room: Room,
        session: MultiplayerSession,
        *,
        participants: tuple[RoundParticipantSelection, ...],
        now: datetime,
    ) -> Round | None:
        settings = _settings(room, session)
        if settings.external_beatmap_id < 1:
            return None
        revision_statement = (
            select(BeatmapRevision)
            .join(Beatmap, Beatmap.id == BeatmapRevision.beatmap_id)
            .where(
                Beatmap.external_id == settings.external_beatmap_id,
                BeatmapRevision.is_current.is_(True),
            )
        )
        if settings.beatmap_md5 is not None:
            revision_statement = revision_statement.where(BeatmapRevision.md5 == settings.beatmap_md5)
        beatmap_revision = (await self._session.execute(revision_statement.limit(1))).scalar_one_or_none()
        if beatmap_revision is None:
            return None
        scoreboard = (
            await self._session.execute(
                select(Scoreboard).where(
                    Scoreboard.ruleset == DbRuleset(settings.ruleset.value),
                    Scoreboard.variant == DbScoreboardVariant(settings.variant.value),
                    Scoreboard.active.is_(True),
                )
            )
        ).scalar_one_or_none()
        if scoreboard is None:
            return None
        ranking_policy = (
            await self._session.execute(
                select(RankingPolicy).where(
                    RankingPolicy.scoreboard_id == scoreboard.id,
                    RankingPolicy.active.is_(True),
                )
            )
        ).scalar_one_or_none()
        if ranking_policy is None:
            return None
        normalized = normalize_mods(settings.ruleset, settings.variant, settings.mods)
        mod_statement = (
            insert(ModSet)
            .values(
                scoreboard_id=scoreboard.id,
                canonical=list(normalized.canonical),
                canonical_digest=normalized.canonical_digest,
                legacy_bits=normalized.legacy_bits,
            )
            .on_conflict_do_update(
                index_elements=(ModSet.scoreboard_id, ModSet.canonical_digest),
                set_={"legacy_bits": normalized.legacy_bits},
            )
            .returning(ModSet.id)
        )
        mod_set_id = (await self._session.execute(mod_statement)).scalar_one()
        item = (
            await self._session.execute(
                select(PlaylistItem).where(PlaylistItem.room_id == room.id, PlaylistItem.client_item_id == 1)
            )
        ).scalar_one_or_none()
        if item is None:
            item = PlaylistItem(id=uuid.uuid7(), room_id=room.id, client_item_id=1, state="active", position=0)
            self._session.add(item)
            await self._session.flush()
        configuration = _settings_json(settings)
        digest = canonical_json_digest(configuration)
        playlist_revision = (
            await self._session.execute(
                select(PlaylistRevision).where(
                    PlaylistRevision.item_id == item.id,
                    PlaylistRevision.configuration_digest == digest,
                )
            )
        ).scalar_one_or_none()
        if playlist_revision is None:
            await self._session.execute(
                update(PlaylistRevision)
                .where(
                    PlaylistRevision.item_id == item.id,
                    PlaylistRevision.is_current.is_(True),
                )
                .values(is_current=False)
            )
            revision_number = (
                await self._session.scalar(
                    select(func.coalesce(func.max(PlaylistRevision.revision_number), 0)).where(
                        PlaylistRevision.item_id == item.id
                    )
                )
                or 0
            ) + 1
            playlist_revision = PlaylistRevision(
                id=uuid.uuid7(),
                item_id=item.id,
                revision_number=revision_number,
                owner_account_id=session.host_account_id,
                beatmap_revision_id=beatmap_revision.id,
                scoreboard_id=scoreboard.id,
                mod_policy_id=ranking_policy.mod_policy_id,
                required_mod_set_id=None if settings.free_mods else mod_set_id,
                scoring_mode=settings.win_condition.value,
                configuration=configuration,
                configuration_digest=digest,
                is_current=True,
            )
            self._session.add(playlist_revision)
            await self._session.flush()
        elif not playlist_revision.is_current:
            await self._session.execute(
                update(PlaylistRevision)
                .where(PlaylistRevision.item_id == item.id, PlaylistRevision.is_current.is_(True))
                .values(is_current=False)
            )
            playlist_revision.is_current = True
        round_number = (
            await self._session.scalar(
                select(func.coalesce(func.max(Round.round_number), 0)).where(Round.session_id == session.id)
            )
            or 0
        ) + 1
        frozen_round = Round(
            id=uuid.uuid7(),
            session_id=session.id,
            round_number=round_number,
            playlist_revision_id=playlist_revision.id,
            status="in_progress",
            configuration=configuration,
            configuration_digest=digest,
            started_at=now,
        )
        self._session.add(frozen_round)
        await self._session.flush()
        for participant in participants:
            participant_mods = settings.mods
            if settings.free_mods:
                by_acronym = {mod.acronym: mod for mod in (*settings.mods, *participant.mods)}
                participant_mods = tuple(by_acronym.values())
            participant_normalized = normalize_mods(settings.ruleset, settings.variant, participant_mods)
            participant_mod_statement = (
                insert(ModSet)
                .values(
                    scoreboard_id=scoreboard.id,
                    canonical=list(participant_normalized.canonical),
                    canonical_digest=participant_normalized.canonical_digest,
                    legacy_bits=participant_normalized.legacy_bits,
                )
                .on_conflict_do_update(
                    index_elements=(ModSet.scoreboard_id, ModSet.canonical_digest),
                    set_={"legacy_bits": participant_normalized.legacy_bits},
                )
                .returning(ModSet.id)
            )
            participant_mod_set_id = (await self._session.execute(participant_mod_statement)).scalar_one()
            self._session.add_all(
                (
                    RoundParticipant(
                        round_id=frozen_round.id,
                        account_id=participant.account_id,
                        slot_number=participant.slot_position,
                        team_number=participant.team or None,
                        mod_set_id=participant_mod_set_id,
                    ),
                    MultiplayerAttempt(
                        id=uuid.uuid7(),
                        account_id=participant.account_id,
                        round_id=frozen_round.id,
                        token_digest=secrets.token_bytes(32),
                        attempt_number=1,
                        status=AttemptStatus.ISSUED,
                        expires_at=now + timedelta(hours=6),
                    ),
                )
            )
        return frozen_round

    async def _entities(self, public_id: int) -> tuple[Room, MultiplayerSession]:
        row = (
            await self._session.execute(
                select(Room, MultiplayerSession)
                .join(MultiplayerSession, MultiplayerSession.room_id == Room.id)
                .where(
                    Room.public_id == public_id,
                    Room.status.in_((RoomStatus.OPEN, RoomStatus.STARTED)),
                    MultiplayerSession.status == SessionStatus.ACTIVE,
                )
                .order_by(MultiplayerSession.ordinal.desc())
                .limit(1)
                .with_for_update(of=(Room, MultiplayerSession))
            )
        ).one_or_none()
        if row is None:
            raise MatchNotFound("room is not active")
        return row[0], row[1]

    async def _room_for_command(self, command_id: uuid.UUID) -> RoomRecord | None:
        public_id = await self._session.scalar(
            select(Room.public_id)
            .join(MultiplayerEvent, MultiplayerEvent.room_id == Room.id)
            .where(MultiplayerEvent.command_id == command_id)
        )
        return await self.get_room(public_id) if public_id is not None else None

    async def _append_event(
        self,
        room: Room,
        session: MultiplayerSession,
        *,
        command_id: uuid.UUID,
        actor_account_id: int,
        event_type: str,
        payload: dict[str, object],
    ) -> None:
        existing = await self._session.scalar(
            select(MultiplayerEvent.id).where(MultiplayerEvent.command_id == command_id)
        )
        if existing is not None:
            return
        last_sequence = await self._session.scalar(
            select(func.coalesce(func.max(MultiplayerEvent.room_sequence), 0)).where(
                MultiplayerEvent.room_id == room.id
            )
        )
        self._session.add(
            MultiplayerEvent(
                room_id=room.id,
                room_sequence=(last_sequence or 0) + 1,
                command_id=command_id,
                session_id=session.id,
                actor_account_id=actor_account_id,
                aggregate_version=session.version,
                event_type=event_type,
                visibility="room",
                payload=payload,
            )
        )


def _settings_json(settings: RoomSettings) -> dict[str, object]:
    return {
        "beatmap_name": settings.beatmap_name,
        "external_beatmap_id": settings.external_beatmap_id,
        "beatmap_md5": settings.beatmap_md5.hex() if settings.beatmap_md5 is not None else None,
        "ruleset": settings.ruleset.value,
        "variant": settings.variant.value,
        "team_mode": settings.team_mode.value,
        "win_condition": settings.win_condition.value,
        "mods": [mod.as_json() for mod in settings.mods],
        "free_mods": settings.free_mods,
        "seed": settings.seed,
    }


def _settings(room: Room, session: MultiplayerSession) -> RoomSettings:
    value = room.configuration
    mods_value = value.get("mods", [])
    if not isinstance(mods_value, list):
        raise RuntimeError("stored multiplayer mods are invalid")
    mods: list[CanonicalMod] = []
    for item in mods_value:
        if not isinstance(item, dict) or not isinstance(item.get("acronym"), str):
            raise RuntimeError("stored multiplayer mod is invalid")
        settings = item.get("settings", {})
        if not isinstance(settings, dict):
            raise RuntimeError("stored multiplayer mod settings are invalid")
        mods.append(CanonicalMod(item["acronym"], settings))
    checksum = value.get("beatmap_md5")
    try:
        beatmap_md5 = bytes.fromhex(checksum) if isinstance(checksum, str) else None
        return RoomSettings(
            name=room.name,
            beatmap_name=str(value.get("beatmap_name", "")),
            external_beatmap_id=_json_int(value.get("external_beatmap_id"), default=0),
            beatmap_md5=beatmap_md5,
            ruleset=Ruleset(str(value.get("ruleset", "osu"))),
            variant=ScoreboardVariant(str(value.get("variant", "vanilla"))),
            team_mode=TeamMode(str(value.get("team_mode", session.team_mode))),
            win_condition=WinCondition(str(value.get("win_condition", session.scoring_mode))),
            mods=tuple(mods),
            free_mods=bool(value.get("free_mods", False)),
            seed=_json_int(value.get("seed"), default=0),
        )
    except (TypeError, ValueError) as error:
        raise RuntimeError("stored multiplayer settings are invalid") from error


def _record(room: Room, session: MultiplayerSession) -> RoomRecord:
    if session.host_account_id is None:
        raise RuntimeError("active multiplayer session has no host")
    return RoomRecord(
        room_id=room.id,
        public_id=room.public_id,
        session_id=session.id,
        version=session.version,
        creator_account_id=room.creator_account_id,
        host_account_id=session.host_account_id,
        capacity=room.capacity,
        settings=_settings(room, session),
        requires_password=room.password_verifier is not None,
        password_salt=room.password_prefix,
        password_verifier=room.password_verifier,
    )


def _require_host_version(room: RoomRecord, session: MultiplayerSession, actor_account_id: int) -> None:
    if session.host_account_id != actor_account_id:
        raise MatchPermissionDenied("only the current host can mutate the room")
    _require_version(room, session)


def _require_version(room: RoomRecord, session: MultiplayerSession) -> None:
    if session.version != room.version:
        raise MatchConcurrencyConflict("room aggregate version changed")


def _json_int(value: object, *, default: int) -> int:
    if value is None:
        return default
    if isinstance(value, bool) or not isinstance(value, int | str):
        raise ValueError("stored multiplayer integer is invalid")
    return int(value)
