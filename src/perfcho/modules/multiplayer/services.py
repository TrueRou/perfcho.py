"""Coordinate durable multiplayer facts and expiring room projections."""

import base64
import hashlib
import hmac
import json
import secrets
import time
import uuid
from collections.abc import Callable
from dataclasses import replace
from datetime import datetime, timedelta

from perfcho.infra.logging import duration_ms, log_event, rate_limit
from perfcho.modules.common.models import CommandMeta
from perfcho.modules.common.ports import Clock
from perfcho.modules.multiplayer.errors import (
    MatchAlreadyJoined,
    MatchConcurrencyConflict,
    MatchNotFound,
    MatchPasswordRejected,
    MatchPermissionDenied,
    MatchProjectionUnavailable,
    MatchStateRejected,
)
from perfcho.modules.multiplayer.models import (
    ChangeHost,
    ChangeRoomPassword,
    CleanupPresence,
    CompleteRound,
    CreateRoom,
    DurableRoomSnapshot,
    JoinRoom,
    KickParticipant,
    LeaveRoom,
    ProjectionStatus,
    RoomRecord,
    RoomSettings,
    RoomSlot,
    RoomState,
    RoundParticipantSelection,
    SlotStatus,
    StartRound,
    TeamMode,
    UpdateRoomSettings,
)
from perfcho.modules.multiplayer.ports import (
    MultiplayerAccessPolicyFactory,
    MultiplayerRepository,
    MultiplayerRepositoryFactory,
    MultiplayerStateRepository,
    MultiplayerUnitOfWork,
)
from perfcho.modules.scoring.errors import ScoreRejected
from perfcho.modules.scoring.models import CanonicalMod, MultiplayerSubmissionContext
from perfcho.modules.scoring.mods import normalize_mods

_STATE_LIFETIME = timedelta(minutes=15)
_ADMISSION_LIFETIME = timedelta(minutes=2)
_COMMAND_NAMESPACE = uuid.UUID("d184efea-948d-5e23-80d2-0d66fa0e813a")
_SPEED_MODS = frozenset({"DT", "NC", "HT"})


class MultiplayerService:
    """Apply canonical room rules across PostgreSQL and Redis adapters."""

    def __init__(
        self,
        uow_factory: Callable[[], MultiplayerUnitOfWork],
        repository_factory: MultiplayerRepositoryFactory,
        state: MultiplayerStateRepository,
        clock: Clock,
        password_key: bytes,
        *,
        access_policy_factory: MultiplayerAccessPolicyFactory,
        admission_key: bytes | None = None,
        state_lifetime: timedelta = _STATE_LIFETIME,
        admission_lifetime: timedelta = _ADMISSION_LIFETIME,
    ) -> None:
        """Bind transaction, persistence, state, time, and password-proof dependencies."""
        if not password_key:
            raise ValueError("password_key must not be empty")
        if state_lifetime <= timedelta(0):
            raise ValueError("state_lifetime must be positive")
        if admission_lifetime <= timedelta(0):
            raise ValueError("admission_lifetime must be positive")
        self._uow_factory = uow_factory
        self._repository_factory = repository_factory
        self._state = state
        self._access_policy_factory = access_policy_factory
        self._clock = clock
        self._password_key = bytes(password_key)
        self._admission_key = bytes(admission_key or password_key)
        self._state_lifetime = state_lifetime
        self._admission_lifetime = admission_lifetime

    async def create_room(self, command: CreateRoom) -> RoomState:
        """Create durable room facts before publishing the initial host projection."""
        started_ns = time.monotonic_ns()
        actor = _actor_id(command.meta)
        salt, verifier = self._password_fields(command.password)
        now = self._clock.now()
        command_id = _command_id(command.meta)
        replayed = False
        async with self._uow_factory() as uow:
            await self._access_policy_factory(uow.session).require(
                actor,
                ("multiplayer.play", "multiplayer.host"),
                at=now,
            )
            repository = self._repository_factory(uow.session)
            room = await repository.find_command_room(command_id)
            if room is None:
                room = await repository.create_room(
                    command_id=command_id,
                    actor_account_id=actor,
                    connection_session_id=_actor_session_id(command.meta),
                    settings=_normalized_settings(command.settings),
                    capacity=command.capacity,
                    password_salt=salt,
                    password_verifier=verifier,
                    now=now,
                )
                snapshot = await repository.load_snapshot(room)
                await uow.commit()
            else:
                replayed = True
                snapshot = await repository.load_snapshot(room)
        _log_room_event(
            "DEBUG" if replayed else "INFO",
            "multiplayer.room.created",
            room,
            actor_account_id=actor,
            replayed=replayed,
            started_ns=started_ns,
        )
        return await self._publish_snapshot(snapshot)

    async def join_room(self, command: JoinRoom) -> RoomState:
        """Verify room credentials, persist admission, then occupy a realtime slot."""
        started_ns = time.monotonic_ns()
        actor = _actor_id(command.meta)
        now = self._clock.now()
        current = await self._cached_room_for_account(actor)
        if current is not None and current.room.public_id != command.public_id:
            raise MatchAlreadyJoined("account already occupies another room")
        command_id = _command_id(command.meta)
        replayed = False
        async with self._uow_factory() as uow:
            await self._access_policy_factory(uow.session).require(actor, ("multiplayer.play",), at=now)
            repository = self._repository_factory(uow.session)
            room = await repository.find_command_room(command_id)
            if room is None:
                room = await repository.get_room(command.public_id, for_update=True)
                if room is None:
                    raise MatchNotFound("room is not active")
                self._verify_admission(room, actor, command.password, now=now)
                room = await repository.join_room(
                    room,
                    command_id=command_id,
                    account_id=actor,
                    connection_session_id=_actor_session_id(command.meta),
                    now=now,
                )
                snapshot = await repository.load_snapshot(room)
                await uow.commit()
            else:
                replayed = True
                snapshot = await repository.load_snapshot(room)
        _log_room_event(
            "DEBUG" if replayed else "INFO",
            "multiplayer.room.joined",
            room,
            actor_account_id=actor,
            replayed=replayed,
            started_ns=started_ns,
        )
        if current is not None and current.slot_for(actor) is not None:
            return await self._reconcile_snapshot(current, snapshot)
        try:
            projected = await self._state.get(room.public_id, at=now)
        except Exception as error:
            _log_projection_failure("join_get", error, public_id=room.public_id, version=room.version)
        else:
            if projected is not None and projected.slot_for(actor) is None:
                try:
                    return await self._state.join(room, account_id=actor, expires_at=now + self._state_lifetime)
                except Exception as error:
                    _log_projection_failure("join", error, public_id=room.public_id, version=room.version)
        return await self._publish_snapshot(snapshot)

    async def leave_room(self, command: LeaveRoom) -> RoomState | None:
        """Persist a leave and reflect closure or host transfer in realtime state."""
        started_ns = time.monotonic_ns()
        actor = _actor_id(command.meta)
        now = self._clock.now()
        command_id = _command_id(command.meta)
        durable_room: RoomRecord | None = None
        async with self._uow_factory() as uow:
            await self._access_policy_factory(uow.session).require(actor, ("multiplayer.play",), at=now)
            repository = self._repository_factory(uow.session)
            replay = await repository.find_command_room(command_id)
            if replay is not None:
                snapshot = await repository.load_snapshot(replay)
                durable_room = replay
                replayed = True
            else:
                replayed = False
                snapshot = None
            room = await repository.get_room(command.public_id, for_update=True)
            if room is None:
                if replayed:
                    assert durable_room is not None
                    _log_room_event(
                        "DEBUG",
                        "multiplayer.room.left",
                        durable_room,
                        actor_account_id=actor,
                        replayed=True,
                        started_ns=started_ns,
                    )
                return None
            if not replayed:
                durable_room = await repository.leave_room(
                    room,
                    command_id=command_id,
                    account_id=actor,
                    connection_session_id=_actor_session_id(command.meta),
                    reason="client_parted",
                    now=now,
                )
                snapshot = await repository.load_snapshot(durable_room) if durable_room is not None else None
                await uow.commit()
        if replayed:
            assert snapshot is not None
            assert durable_room is not None
            _log_room_event(
                "DEBUG",
                "multiplayer.room.left",
                durable_room,
                actor_account_id=actor,
                replayed=True,
                started_ns=started_ns,
            )
            return await self._publish_snapshot(snapshot)
        _log_room_event(
            "INFO",
            "multiplayer.room.left" if durable_room is not None else "multiplayer.room.disposed",
            durable_room or room,
            actor_account_id=actor,
            replayed=False,
            started_ns=started_ns,
        )
        try:
            return await self._state.leave(command.public_id, account_id=actor, durable_room=durable_room)
        except Exception as error:
            _log_projection_failure(
                "leave",
                error,
                public_id=command.public_id,
                version=durable_room.version if durable_room is not None else room.version,
            )
            return self._state_from_snapshot(snapshot, degraded=True) if snapshot is not None else None

    async def cleanup_presence(self, command: CleanupPresence) -> RoomState | None:
        """Durably close a presence only when it belongs to the expiring session."""
        started_ns = time.monotonic_ns()
        now = self._clock.now()
        command_id = _command_id(command.meta, account_id=command.account_id)
        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            room = await repository.find_room_for_account(command.account_id)
            if room is None:
                return None
            durable_room = await repository.leave_room(
                room,
                command_id=command_id,
                account_id=command.account_id,
                connection_session_id=command.connection_session_id,
                reason=command.reason,
                now=now,
            )
            snapshot = await repository.load_snapshot(durable_room) if durable_room is not None else None
            await uow.commit()
        _log_room_event(
            "INFO",
            "multiplayer.room.left" if durable_room is not None else "multiplayer.room.disposed",
            durable_room or room,
            actor_account_id=command.account_id,
            replayed=False,
            started_ns=started_ns,
        )
        try:
            return await self._state.leave(
                room.public_id,
                account_id=command.account_id,
                durable_room=durable_room,
            )
        except Exception as error:
            _log_projection_failure(
                "cleanup_leave",
                error,
                public_id=room.public_id,
                version=durable_room.version if durable_room is not None else room.version,
            )
            return self._state_from_snapshot(snapshot, degraded=True) if snapshot is not None else None

    async def kick_participant(self, command: KickParticipant) -> RoomState:
        """Persist a host-authorized kick before removing the target projection."""
        started_ns = time.monotonic_ns()
        actor = _actor_id(command.meta)
        replay = await self._replay_snapshot(command.meta, ("multiplayer.play", "multiplayer.host"))
        if replay is not None:
            _log_room_event(
                "DEBUG",
                "multiplayer.room.participant_kicked",
                replay.room,
                actor_account_id=actor,
                target_account_id=command.target_account_id,
                replayed=True,
                started_ns=started_ns,
            )
            return await self._publish_snapshot(replay)
        current = await self._require_state(command.public_id)
        target_slot = current.slot_for(command.target_account_id)
        if target_slot is None:
            raise MatchNotFound("target participant is not in the room")
        if command.target_account_id == current.room.host_account_id:
            raise MatchStateRejected("the room host cannot be kicked")
        async with self._uow_factory() as uow:
            await self._access_policy_factory(uow.session).require(
                actor,
                ("multiplayer.play", "multiplayer.host"),
                at=self._clock.now(),
            )
            repository = self._repository_factory(uow.session)
            room = await self._locked_host_room(repository, command.public_id, actor, command.expected_version)
            room = await repository.kick_participant(
                room,
                command_id=_command_id(command.meta),
                actor_account_id=actor,
                target_account_id=command.target_account_id,
                now=self._clock.now(),
            )
            snapshot = await repository.load_snapshot(room)
            await uow.commit()
        _log_room_event(
            "INFO",
            "multiplayer.room.participant_kicked",
            room,
            actor_account_id=actor,
            target_account_id=command.target_account_id,
            replayed=False,
            started_ns=started_ns,
        )
        try:
            updated = await self._state.leave(
                command.public_id,
                account_id=command.target_account_id,
                durable_room=room,
            )
            if updated is not None:
                return updated
        except Exception as error:
            _log_projection_failure(
                "kick_leave",
                error,
                public_id=command.public_id,
                version=room.version,
            )
        return self._state_from_snapshot(snapshot, degraded=True)

    async def update_settings(self, command: UpdateRoomSettings) -> RoomState:
        """Persist a host-authorized setting replacement and update its projection."""
        started_ns = time.monotonic_ns()
        actor = _actor_id(command.meta)
        now = self._clock.now()
        replay = await self._replay_snapshot(command.meta, ("multiplayer.play", "multiplayer.host"))
        if replay is not None:
            _log_room_event(
                "DEBUG",
                "multiplayer.room.settings_changed",
                replay.room,
                actor_account_id=actor,
                replayed=True,
                started_ns=started_ns,
            )
            return await self._publish_snapshot(replay)
        current = await self._require_state(command.public_id)
        settings, transitioned_slots = _settings_transition(current, command.settings)
        async with self._uow_factory() as uow:
            await self._access_policy_factory(uow.session).require(
                actor,
                ("multiplayer.play", "multiplayer.host"),
                at=now,
            )
            repository = self._repository_factory(uow.session)
            room = await self._locked_host_room(repository, command.public_id, actor, command.expected_version)
            room = await repository.update_settings(
                room,
                command_id=_command_id(command.meta),
                actor_account_id=actor,
                settings=settings,
                now=now,
            )
            snapshot = await repository.load_snapshot(room)
            await uow.commit()
        _log_room_event(
            "INFO",
            "multiplayer.room.settings_changed",
            room,
            actor_account_id=actor,
            replayed=False,
            started_ns=started_ns,
        )
        slots = tuple(
            replace(slot, status=SlotStatus.NOT_READY, loaded=False, skipped=False, failed=False)
            if slot.account_id is not None
            else slot
            for slot in transitioned_slots
        )
        updated = replace(
            current,
            room=room,
            state_revision=current.state_revision + 1,
            slots=slots,
            in_progress=False,
            round_id=None,
            round_participant_account_ids=(),
            expires_at=now + self._state_lifetime,
        )
        return await self._replace_after_commit(updated, current, snapshot)

    async def change_host(self, command: ChangeHost) -> RoomState:
        """Persist and project a host transfer to an active participant."""
        started_ns = time.monotonic_ns()
        actor = _actor_id(command.meta)
        replay = await self._replay_snapshot(command.meta, ("multiplayer.play", "multiplayer.host"))
        if replay is not None:
            _log_room_event(
                "DEBUG",
                "multiplayer.room.host_changed",
                replay.room,
                actor_account_id=actor,
                target_account_id=command.target_account_id,
                replayed=True,
                started_ns=started_ns,
            )
            return await self._publish_snapshot(replay)
        current = await self._require_state(command.public_id)
        if current.slot_for(command.target_account_id) is None:
            raise MatchStateRejected("target account is not in the room")
        async with self._uow_factory() as uow:
            await self._access_policy_factory(uow.session).require(
                actor,
                ("multiplayer.play", "multiplayer.host"),
                at=self._clock.now(),
            )
            repository = self._repository_factory(uow.session)
            room = await self._locked_host_room(repository, command.public_id, actor, command.expected_version)
            room = await repository.change_host(
                room,
                command_id=_command_id(command.meta),
                actor_account_id=actor,
                target_account_id=command.target_account_id,
                now=self._clock.now(),
            )
            snapshot = await repository.load_snapshot(room)
            await uow.commit()
        _log_room_event(
            "INFO",
            "multiplayer.room.host_changed",
            room,
            actor_account_id=actor,
            target_account_id=command.target_account_id,
            replayed=False,
            started_ns=started_ns,
        )
        updated = replace(current, room=room, state_revision=current.state_revision + 1)
        return await self._replace_after_commit(updated, current, snapshot)

    async def change_password(self, command: ChangeRoomPassword) -> RoomState:
        """Persist a host-authorized password replacement and preserve public secrecy."""
        started_ns = time.monotonic_ns()
        actor = _actor_id(command.meta)
        salt, verifier = self._password_fields(command.password)
        replay = await self._replay_snapshot(command.meta, ("multiplayer.play", "multiplayer.host"))
        if replay is not None:
            _log_room_event(
                "DEBUG",
                "multiplayer.room.password_changed",
                replay.room,
                actor_account_id=actor,
                replayed=True,
                started_ns=started_ns,
            )
            return await self._publish_snapshot(replay)
        current = await self._require_state(command.public_id)
        async with self._uow_factory() as uow:
            await self._access_policy_factory(uow.session).require(
                actor,
                ("multiplayer.play", "multiplayer.host"),
                at=self._clock.now(),
            )
            repository = self._repository_factory(uow.session)
            room = await self._locked_host_room(repository, command.public_id, actor, command.expected_version)
            room = await repository.change_password(
                room,
                command_id=_command_id(command.meta),
                actor_account_id=actor,
                password_salt=salt,
                password_verifier=verifier,
                now=self._clock.now(),
            )
            snapshot = await repository.load_snapshot(room)
            await uow.commit()
        _log_room_event(
            "INFO",
            "multiplayer.room.password_changed",
            room,
            actor_account_id=actor,
            replayed=False,
            started_ns=started_ns,
        )
        updated = replace(current, room=room, state_revision=current.state_revision + 1)
        return await self._replace_after_commit(updated, current, snapshot)

    async def start_round(self, command: StartRound) -> RoomState:
        """Freeze active participants, persist a start, and mark occupied slots playing."""
        started_ns = time.monotonic_ns()
        actor = _actor_id(command.meta)
        replay = await self._replay_snapshot(command.meta, ("multiplayer.play", "multiplayer.host"))
        if replay is not None:
            _log_room_event(
                "DEBUG",
                "multiplayer.round.started",
                replay.room,
                actor_account_id=actor,
                round_id=replay.round_id,
                replayed=True,
                started_ns=started_ns,
            )
            return await self._publish_snapshot(replay)
        current = await self._require_state(command.public_id)
        if current.in_progress:
            raise MatchStateRejected("room already has a round in progress")
        participants = tuple(
            RoundParticipantSelection(slot.account_id, slot.position, slot.team, slot.mods)
            for slot in current.slots
            if slot.account_id is not None and slot.status is not SlotStatus.NO_BEATMAP
        )
        if not participants:
            raise MatchStateRejected("no participants have the selected beatmap")
        async with self._uow_factory() as uow:
            await self._access_policy_factory(uow.session).require(
                actor,
                ("multiplayer.play", "multiplayer.host"),
                at=self._clock.now(),
            )
            repository = self._repository_factory(uow.session)
            room = await self._locked_host_room(repository, command.public_id, actor, command.expected_version)
            room, round_id = await repository.start_round(
                room,
                command_id=_command_id(command.meta),
                actor_account_id=actor,
                participants=participants,
                now=self._clock.now(),
            )
            snapshot = await repository.load_snapshot(room)
            await uow.commit()
        _log_room_event(
            "INFO",
            "multiplayer.round.started",
            room,
            actor_account_id=actor,
            round_id=round_id,
            replayed=False,
            started_ns=started_ns,
        )
        slots = tuple(
            replace(slot, status=SlotStatus.PLAYING, loaded=False, skipped=False, failed=False)
            if slot.account_id is not None and slot.status is not SlotStatus.NO_BEATMAP
            else slot
            for slot in current.slots
        )
        updated = replace(
            current,
            room=room,
            state_revision=current.state_revision + 1,
            slots=slots,
            in_progress=True,
            round_id=round_id,
            round_participant_account_ids=tuple(participant.account_id for participant in participants),
        )
        return await self._replace_after_commit(updated, current, snapshot)

    async def complete_round(self, command: CompleteRound) -> RoomState:
        """Persist round completion and reset occupied slots to not-ready."""
        started_ns = time.monotonic_ns()
        actor = _actor_id(command.meta)
        permissions = ("multiplayer.play", "multiplayer.host") if command.aborted else ("multiplayer.play",)
        replay = await self._replay_snapshot(command.meta, permissions)
        if replay is not None:
            _log_room_event(
                "DEBUG",
                "multiplayer.round.completed",
                replay.room,
                actor_account_id=actor,
                round_id=replay.round_id,
                replayed=True,
                started_ns=started_ns,
            )
            return await self._publish_snapshot(replay)
        current = await self._require_state(command.public_id)
        if not current.in_progress:
            raise MatchStateRejected("room has no round in progress")
        if current.slot_for(actor) is None:
            raise MatchPermissionDenied("only a current participant can complete a round")
        if command.aborted and current.room.host_account_id != actor:
            raise MatchPermissionDenied("only the host can abort a round")
        async with self._uow_factory() as uow:
            await self._access_policy_factory(uow.session).require(actor, permissions, at=self._clock.now())
            repository = self._repository_factory(uow.session)
            room = await self._locked_room(repository, command.public_id, command.expected_version)
            room = await repository.complete_round(
                room,
                command_id=_command_id(command.meta),
                actor_account_id=actor,
                round_id=current.round_id,
                aborted=command.aborted,
                now=self._clock.now(),
            )
            snapshot = await repository.load_snapshot(room)
            await uow.commit()
        _log_room_event(
            "INFO",
            "multiplayer.round.completed",
            room,
            actor_account_id=actor,
            round_id=current.round_id,
            replayed=False,
            started_ns=started_ns,
        )
        slots = tuple(
            replace(slot, status=SlotStatus.NOT_READY, loaded=False, skipped=False, failed=False)
            if slot.account_id in current.round_participant_account_ids
            else slot
            for slot in current.slots
        )
        updated = replace(
            current,
            room=room,
            state_revision=current.state_revision + 1,
            slots=slots,
            in_progress=False,
            round_id=None,
            round_participant_account_ids=(),
        )
        return await self._replace_after_commit(updated, current, snapshot)

    async def get_room(self, public_id: int) -> RoomState:
        """Return one live room projection."""
        return await self._require_state(public_id)

    async def find_room_for_account(self, account_id: int) -> RoomState | None:
        """Return the account's room, rebuilding Redis from durable facts when needed."""
        state = await self._cached_room_for_account(account_id)
        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            room = (
                await repository.get_room(state.room.public_id)
                if state is not None
                else await repository.find_room_for_account(account_id)
            )
            snapshot = await repository.load_snapshot(room) if room is not None else None
        if room is None:
            if state is not None:
                try:
                    await self._state.remove(
                        state.room.public_id,
                        expected_state_revision=state.state_revision,
                        expected_session_id=state.room.session_id,
                    )
                except Exception as error:
                    _log_projection_failure(
                        "remove_stale",
                        error,
                        public_id=state.room.public_id,
                        version=state.room.version,
                    )
            return None
        assert snapshot is not None
        state = (
            await self._publish_snapshot(snapshot) if state is None else await self._reconcile_snapshot(state, snapshot)
        )
        return state if state.slot_for(account_id) is not None else None

    async def get_realtime_room_for_account(self, account_id: int) -> RoomState | None:
        """Resolve only Redis state for frame hot paths without querying PostgreSQL."""
        return await self._cached_room_for_account(account_id)

    async def list_public_rooms(self, *, limit: int = 100) -> tuple[RoomState, ...]:
        """Return a bounded lobby snapshot."""
        if not 1 <= limit <= 256:
            raise ValueError("room list limit must be between 1 and 256")
        try:
            return await self._state.list_public(at=self._clock.now(), limit=limit)
        except Exception as error:
            _log_projection_failure("list_public", error, public_id=None, version=None)
            async with self._uow_factory() as uow:
                repository = self._repository_factory(uow.session)
                rooms = await repository.list_active_rooms(limit=limit)
                snapshots = tuple([await repository.load_snapshot(room) for room in rooms])
            return tuple(self._state_from_snapshot(snapshot, degraded=True) for snapshot in snapshots)

    async def resolve_submission_context(
        self,
        account_id: int,
        beatmap_revision_id: int,
        *,
        started_at: datetime,
        ended_at: datetime,
    ) -> MultiplayerSubmissionContext | None:
        """Resolve an authoritative Stable multiplayer attempt when one exists."""
        if account_id < 1 or beatmap_revision_id < 1:
            raise ValueError("submission context identifiers must be positive")
        if started_at.tzinfo is None or started_at.utcoffset() is None:
            raise ValueError("submission started_at must be timezone-aware")
        if ended_at.tzinfo is None or ended_at.utcoffset() is None or ended_at < started_at:
            raise ValueError("submission ended_at must be aware and not precede started_at")
        async with self._uow_factory() as uow:
            return await self._repository_factory(uow.session).resolve_submission_context(
                account_id,
                beatmap_revision_id,
                started_at=started_at,
                ended_at=ended_at,
                at=self._clock.now(),
            )

    async def move_slot(self, public_id: int, account_id: int, position: int) -> RoomState:
        """Move one participant between realtime slots."""
        await self._require_access(account_id, ("multiplayer.play",))
        await self._require_live_state(public_id)
        return await self._state.move_slot(public_id, account_id=account_id, target_position=position)

    async def lock_slot(self, public_id: int, actor_account_id: int, position: int) -> RoomState:
        """Toggle an empty slot lock or remove an occupied participant as host."""
        await self._require_access(actor_account_id, ("multiplayer.play", "multiplayer.host"))
        state = await self._require_live_state(public_id)
        if state.room.host_account_id != actor_account_id:
            raise MatchPermissionDenied("only the host can lock a slot")
        return await self._state.lock_slot(public_id, actor_account_id=actor_account_id, position=position)

    async def set_slot_status(self, public_id: int, account_id: int, status: SlotStatus) -> RoomState:
        """Set one participant readiness state."""
        await self._require_access(account_id, ("multiplayer.play",))
        state = await self._require_live_state(public_id)
        if state.in_progress and account_id not in state.round_participant_account_ids:
            raise MatchStateRejected("account is not a participant in the active round")
        return await self._state.set_slot_status(public_id, account_id=account_id, status=status)

    async def set_slot_team(self, public_id: int, account_id: int, team: int) -> RoomState:
        """Set one participant team."""
        await self._require_access(account_id, ("multiplayer.play",))
        await self._require_live_state(public_id)
        return await self._state.set_slot_team(public_id, account_id=account_id, team=team)

    async def set_slot_mods(
        self,
        public_id: int,
        account_id: int,
        mods: tuple[CanonicalMod, ...],
    ) -> RoomState:
        """Set one participant free-mod selection."""
        await self._require_access(account_id, ("multiplayer.play",))
        state = await self._require_live_state(public_id)
        if not state.room.settings.free_mods:
            raise MatchStateRejected("personal mods require Free Mod mode")
        if state.in_progress:
            raise MatchStateRejected("personal mods cannot change during an active round")
        try:
            normalized = normalize_mods(state.room.settings.ruleset, state.room.settings.variant, mods).mods
        except ScoreRejected as error:
            raise MatchStateRejected(str(error)) from error
        if any(mod.acronym in _SPEED_MODS for mod in normalized):
            raise MatchStateRejected("personal Free Mod selection cannot contain speed mods")
        return await self._state.set_slot_mods(public_id, account_id=account_id, mods=normalized)

    async def mark_loaded(self, public_id: int, account_id: int) -> RoomState:
        """Mark one participant loaded."""
        await self._require_access(account_id, ("multiplayer.play",))
        await self._require_live_state(public_id)
        return await self._state.mark_loaded(public_id, account_id=account_id)

    async def mark_skipped(self, public_id: int, account_id: int) -> RoomState:
        """Mark one participant skipped."""
        await self._require_access(account_id, ("multiplayer.play",))
        await self._require_live_state(public_id)
        return await self._state.mark_skipped(public_id, account_id=account_id)

    async def mark_failed(self, public_id: int, account_id: int) -> RoomState:
        """Mark one participant failed."""
        await self._require_access(account_id, ("multiplayer.play",))
        await self._require_live_state(public_id)
        return await self._state.mark_failed(public_id, account_id=account_id)

    async def _require_state(self, public_id: int) -> RoomState:
        try:
            state = await self._state.get(public_id, at=self._clock.now())
        except Exception as error:
            _log_projection_failure("get", error, public_id=public_id, version=None)
            state = None
        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            room = await repository.get_room(public_id)
            snapshot = await repository.load_snapshot(room) if room is not None else None
        if room is None:
            raise MatchNotFound("room has no live state")
        assert snapshot is not None
        if state is None:
            return await self._publish_snapshot(snapshot)
        return await self._reconcile_snapshot(state, snapshot)

    async def _require_live_state(self, public_id: int) -> RoomState:
        state = await self._require_state(public_id)
        if state.projection_status is not ProjectionStatus.LIVE:
            raise MatchProjectionUnavailable("multiplayer projection is recovering; retry the action")
        return state

    async def _publish_snapshot(self, snapshot: DurableRoomSnapshot) -> RoomState:
        """Best-effort publish a durable snapshot without changing command success semantics."""
        durable_state = self._state_from_snapshot(snapshot)
        try:
            current = await self._state.get(snapshot.room.public_id, at=self._clock.now())
        except Exception as error:
            _log_projection_failure(
                "publish_get",
                error,
                public_id=snapshot.room.public_id,
                version=snapshot.room.version,
            )
            return replace(durable_state, projection_status=ProjectionStatus.DURABLE_RECOVERY)
        if current is None:
            try:
                return await self._state.create(durable_state)
            except Exception as error:
                _log_projection_failure(
                    "publish_create",
                    error,
                    public_id=snapshot.room.public_id,
                    version=snapshot.room.version,
                )
                return replace(durable_state, projection_status=ProjectionStatus.DURABLE_RECOVERY)
        return await self._reconcile_snapshot(current, snapshot)

    async def _reconcile_snapshot(self, state: RoomState, snapshot: DurableRoomSnapshot) -> RoomState:
        """Repair stale Redis state and enforce its durable room/session epoch."""
        room = snapshot.room
        durable_accounts = frozenset(snapshot.active_account_ids)
        projected_accounts = frozenset(slot.account_id for slot in state.slots if slot.account_id is not None)
        same_epoch = state.room.room_id == room.room_id and state.room.session_id == room.session_id
        if (
            same_epoch
            and state.room.version == room.version
            and projected_accounts == durable_accounts
            and state.round_id == snapshot.round_id
        ):
            return replace(state, projection_status=ProjectionStatus.LIVE)
        if same_epoch and state.round_id == snapshot.round_id:
            by_account = {slot.account_id: slot for slot in state.slots if slot.account_id in durable_accounts}
            retained = list(by_account.values())
            used_positions = {slot.position for slot in retained}
            available = iter(position for position in range(room.capacity) if position not in used_positions)
            for account_id in snapshot.active_account_ids:
                if account_id not in by_account:
                    retained.append(RoomSlot(next(available), SlotStatus.NOT_READY, account_id, _default_team(room)))
            occupied = {slot.position: slot for slot in retained}
            slots = tuple(
                occupied.get(position, RoomSlot(position, SlotStatus.OPEN)) for position in range(room.capacity)
            )
            updated = replace(
                state,
                room=room,
                state_revision=state.state_revision + 1,
                slots=slots,
                projection_status=ProjectionStatus.LIVE,
            )
        else:
            updated = replace(
                self._state_from_snapshot(snapshot),
                state_revision=max(state.state_revision + 1, room.version),
            )
        try:
            return await self._state.replace(
                updated,
                expected_state_revision=state.state_revision,
                expected_session_id=state.room.session_id,
            )
        except Exception as error:
            _log_projection_failure(
                "reconcile_replace",
                error,
                public_id=room.public_id,
                version=room.version,
            )
            return replace(self._state_from_snapshot(snapshot), projection_status=ProjectionStatus.DURABLE_RECOVERY)

    def _state_from_snapshot(self, snapshot: DurableRoomSnapshot | None, *, degraded: bool = False) -> RoomState:
        if snapshot is None:
            raise MatchNotFound("room has no durable snapshot")
        room = snapshot.room
        if room.host_account_id not in snapshot.active_account_ids:
            raise MatchNotFound("active room has no durable host presence")
        participant_by_account = {participant.account_id: participant for participant in snapshot.round_participants}
        occupied: dict[int, RoomSlot] = {}
        for participant in snapshot.round_participants:
            personal_mods = _personal_mods(room, participant.mods)
            occupied[participant.slot_position] = RoomSlot(
                participant.slot_position,
                SlotStatus.PLAYING,
                participant.account_id,
                participant.team,
                personal_mods,
            )
        available = iter(position for position in range(room.capacity) if position not in occupied)
        for account_id in snapshot.active_account_ids:
            if account_id in participant_by_account:
                continue
            position = next(available)
            status = SlotStatus.NO_BEATMAP if snapshot.round_id is not None else SlotStatus.NOT_READY
            occupied[position] = RoomSlot(position, status, account_id, _default_team(room))
        slots = tuple(occupied.get(position, RoomSlot(position, SlotStatus.OPEN)) for position in range(room.capacity))
        return RoomState(
            room,
            room.version,
            slots,
            snapshot.round_id is not None,
            self._clock.now() + self._state_lifetime,
            snapshot.round_id,
            tuple(participant.account_id for participant in snapshot.round_participants),
            ProjectionStatus.DURABLE_RECOVERY if degraded else ProjectionStatus.LIVE,
        )

    async def _replace_after_commit(
        self,
        updated: RoomState,
        previous: RoomState,
        snapshot: DurableRoomSnapshot,
    ) -> RoomState:
        try:
            return await self._state.replace(
                updated,
                expected_state_revision=previous.state_revision,
                expected_session_id=previous.room.session_id,
            )
        except Exception as error:
            _log_projection_failure(
                "replace_after_commit",
                error,
                public_id=snapshot.room.public_id,
                version=snapshot.room.version,
            )
            return replace(updated, projection_status=ProjectionStatus.DURABLE_RECOVERY)

    async def _replay_snapshot(
        self,
        meta: CommandMeta,
        permissions: tuple[str, ...],
    ) -> DurableRoomSnapshot | None:
        actor = _actor_id(meta)
        async with self._uow_factory() as uow:
            await self._access_policy_factory(uow.session).require(actor, permissions, at=self._clock.now())
            repository = self._repository_factory(uow.session)
            room = await repository.find_command_room(_command_id(meta))
            return await repository.load_snapshot(room) if room is not None else None

    async def _require_access(self, account_id: int, permissions: tuple[str, ...]) -> None:
        async with self._uow_factory() as uow:
            await self._access_policy_factory(uow.session).require(account_id, permissions, at=self._clock.now())

    async def _cached_room_for_account(self, account_id: int) -> RoomState | None:
        try:
            return await self._state.find_for_account(account_id, at=self._clock.now())
        except Exception as error:
            _log_projection_failure("find_for_account", error, public_id=None, version=None)
            return None

    async def issue_admission_token(
        self,
        public_id: int,
        *,
        inviter_account_id: int,
        recipient_account_id: int,
    ) -> str:
        """Issue a short signed room/session token without disclosing its password."""
        if recipient_account_id < 1 or recipient_account_id == inviter_account_id:
            raise ValueError("admission recipient must be another positive account")
        await self._require_access(inviter_account_id, ("multiplayer.play",))
        state = await self._require_state(public_id)
        if state.slot_for(inviter_account_id) is None:
            raise MatchPermissionDenied("only a current participant can invite players")
        expires_at = self._clock.now() + self._admission_lifetime
        payload = json.dumps(
            {
                "exp": int(expires_at.timestamp()),
                "recipient": recipient_account_id,
                "room": str(state.room.room_id),
                "session": str(state.room.session_id),
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        encoded = _base64url_encode(payload)
        signature = hmac.new(self._admission_key, encoded.encode("ascii"), hashlib.sha256).digest()
        return f"adm.{encoded}.{_base64url_encode(signature)}"

    def _verify_admission(self, room: RoomRecord, account_id: int, credential: str, *, now: datetime) -> None:
        if not credential.startswith("adm."):
            self._verify_password(room.password_salt, room.password_verifier, credential)
            return
        try:
            _, encoded, encoded_signature = credential.split(".", 2)
            signature = _base64url_decode(encoded_signature)
            expected = hmac.new(self._admission_key, encoded.encode("ascii"), hashlib.sha256).digest()
            if not hmac.compare_digest(signature, expected):
                raise ValueError
            payload = json.loads(_base64url_decode(encoded))
            if (
                not isinstance(payload, dict)
                or payload.get("recipient") != account_id
                or payload.get("room") != str(room.room_id)
                or payload.get("session") != str(room.session_id)
                or not isinstance(payload.get("exp"), int)
                or payload["exp"] <= int(now.timestamp())
                or payload["exp"] > int((now + self._admission_lifetime).timestamp())
            ):
                raise ValueError
        except (UnicodeError, ValueError, TypeError, json.JSONDecodeError) as error:
            raise MatchPasswordRejected("room admission token is invalid or expired") from error

    @staticmethod
    async def _locked_host_room(
        repository: MultiplayerRepository,
        public_id: int,
        actor_account_id: int,
        expected_version: int,
    ) -> RoomRecord:
        room = await repository.get_room(public_id, for_update=True)
        if room is None:
            raise MatchNotFound("room is not active")
        if room.host_account_id != actor_account_id:
            raise MatchPermissionDenied("only the current host can mutate the room")
        if room.version != expected_version:
            raise MatchConcurrencyConflict("room aggregate version changed")
        return room

    @staticmethod
    async def _locked_room(
        repository: MultiplayerRepository,
        public_id: int,
        expected_version: int,
    ) -> RoomRecord:
        room = await repository.get_room(public_id, for_update=True)
        if room is None:
            raise MatchNotFound("room is not active")
        if room.version != expected_version:
            raise MatchConcurrencyConflict("room aggregate version changed")
        return room

    def _password_fields(self, password: str) -> tuple[str | None, str | None]:
        if not password:
            return None, None
        salt = secrets.token_hex(8)
        return salt, self._password_verifier(salt, password)

    def _verify_password(self, salt: str | None, verifier: str | None, password: str) -> None:
        if salt is None or verifier is None:
            if password:
                raise MatchPasswordRejected("room does not accept a password")
            return
        if not password or not hmac.compare_digest(verifier, self._password_verifier(salt, password)):
            raise MatchPasswordRejected("room password is incorrect")

    def _password_verifier(self, salt: str, password: str) -> str:
        return hmac.new(self._password_key, salt.encode() + b"\0" + password.encode(), hashlib.sha256).hexdigest()


def _log_room_event(
    level: str,
    event: str,
    room: RoomRecord,
    *,
    actor_account_id: int,
    replayed: bool,
    started_ns: int,
    target_account_id: int | None = None,
    round_id: uuid.UUID | None = None,
) -> None:
    log_event(
        level,
        event,
        room_id=str(room.room_id),
        public_id=room.public_id,
        session_id=str(room.session_id),
        version=room.version,
        actor_account_id=actor_account_id,
        target_account_id=target_account_id,
        round_id=str(round_id) if round_id is not None else None,
        replayed=replayed,
        duration_ms=duration_ms(started_ns),
    )


def _log_projection_failure(
    operation: str,
    error: BaseException,
    *,
    public_id: int | None,
    version: int | None,
) -> None:
    if not rate_limit(f"multiplayer.projection:{operation}"):
        return
    log_event(
        "WARNING",
        "multiplayer.projection.degraded",
        operation=operation,
        public_id=public_id,
        version=version,
        error_type=type(error).__name__,
    )


def _actor_id(meta: CommandMeta) -> int:
    actor = meta.actor
    if actor is None:
        raise ValueError("multiplayer command requires an authenticated actor")
    return actor.account_id


def _actor_session_id(meta: CommandMeta) -> uuid.UUID:
    actor = meta.actor
    if actor is None or actor.auth_session_id is None:
        raise ValueError("multiplayer presence requires an authenticated session")
    return actor.auth_session_id


def _command_id(meta: CommandMeta, *, account_id: int | None = None) -> uuid.UUID:
    actor_id = account_id if account_id is not None else _actor_id(meta)
    return uuid.uuid5(_COMMAND_NAMESPACE, f"{actor_id}:{meta.idempotency_key}")


def _normalized_settings(settings: RoomSettings) -> RoomSettings:
    try:
        normalized = normalize_mods(settings.ruleset, settings.variant, settings.mods)
    except ScoreRejected as error:
        raise MatchStateRejected(str(error)) from error
    return replace(settings, mods=normalized.mods)


def _settings_transition(state: RoomState, requested: RoomSettings) -> tuple[RoomSettings, tuple[RoomSlot, ...]]:
    settings = _normalized_settings(requested)
    current = state.room.settings
    slots = state.slots
    if current.team_mode is not settings.team_mode:
        team = _default_team(replace(state.room, settings=settings))
        slots = tuple(replace(slot, team=team) if slot.account_id is not None else slot for slot in slots)
    if not current.free_mods and settings.free_mods:
        inherited = tuple(mod for mod in current.mods if mod.acronym not in _SPEED_MODS)
        settings = replace(settings, mods=tuple(mod for mod in settings.mods if mod.acronym in _SPEED_MODS))
        slots = tuple(replace(slot, mods=inherited) if slot.account_id is not None else slot for slot in slots)
    elif current.free_mods and not settings.free_mods:
        host_slot = state.slot_for(state.room.host_account_id)
        host_mods = host_slot.mods if host_slot is not None else ()
        speed_mods = tuple(mod for mod in settings.mods if mod.acronym in _SPEED_MODS)
        try:
            merged = normalize_mods(settings.ruleset, settings.variant, (*speed_mods, *host_mods)).mods
        except ScoreRejected as error:
            raise MatchStateRejected(str(error)) from error
        settings = replace(settings, mods=merged)
        slots = tuple(replace(slot, mods=()) if slot.account_id is not None else slot for slot in slots)
    return settings, slots


def _default_team(room: RoomRecord) -> int:
    return 1 if room.settings.team_mode in {TeamMode.TEAM_VS, TeamMode.TAG_TEAM_VS} else 0


def _personal_mods(room: RoomRecord, frozen_mods: tuple[CanonicalMod, ...]) -> tuple[CanonicalMod, ...]:
    if not room.settings.free_mods:
        return ()
    room_acronyms = {mod.acronym for mod in room.settings.mods}
    return tuple(mod for mod in frozen_mods if mod.acronym not in room_acronyms)


def _base64url_encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _base64url_decode(value: str) -> bytes:
    return base64.b64decode(value + "=" * (-len(value) % 4), altchars=b"-_", validate=True)
