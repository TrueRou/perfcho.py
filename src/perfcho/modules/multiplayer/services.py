"""Coordinate durable multiplayer facts and expiring room projections."""

import hashlib
import hmac
import secrets
from collections.abc import Callable
from contextlib import suppress
from dataclasses import replace
from datetime import timedelta

from perfcho.modules.common.models import CommandMeta
from perfcho.modules.common.ports import Clock
from perfcho.modules.multiplayer.errors import (
    MatchAlreadyJoined,
    MatchConcurrencyConflict,
    MatchNotFound,
    MatchPasswordRejected,
    MatchPermissionDenied,
    MatchStateRejected,
)
from perfcho.modules.multiplayer.models import (
    ChangeHost,
    ChangeRoomPassword,
    CompleteRound,
    CreateRoom,
    JoinRoom,
    KickParticipant,
    LeaveRoom,
    RoomRecord,
    RoomSlot,
    RoomState,
    RoundParticipantSelection,
    SlotStatus,
    StartRound,
    UpdateRoomSettings,
)
from perfcho.modules.multiplayer.ports import (
    MultiplayerRepository,
    MultiplayerRepositoryFactory,
    MultiplayerStateRepository,
    MultiplayerUnitOfWork,
)
from perfcho.modules.scoring.models import CanonicalMod, MultiplayerSubmissionContext

_STATE_LIFETIME = timedelta(minutes=15)


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
        state_lifetime: timedelta = _STATE_LIFETIME,
    ) -> None:
        """Bind transaction, persistence, state, time, and password-proof dependencies."""
        if not password_key:
            raise ValueError("password_key must not be empty")
        if state_lifetime <= timedelta(0):
            raise ValueError("state_lifetime must be positive")
        self._uow_factory = uow_factory
        self._repository_factory = repository_factory
        self._state = state
        self._clock = clock
        self._password_key = bytes(password_key)
        self._state_lifetime = state_lifetime

    async def create_room(self, command: CreateRoom) -> RoomState:
        """Create durable room facts before publishing the initial host projection."""
        actor = _actor_id(command.meta)
        existing = await self._state.find_for_account(actor, at=self._clock.now())
        if existing is not None:
            return existing
        async with self._uow_factory() as uow:
            durable_existing = await self._repository_factory(uow.session).find_room_for_account(actor)
        if durable_existing is not None:
            return await self._restore_state(durable_existing)
        salt, verifier = self._password_fields(command.password)
        now = self._clock.now()
        async with self._uow_factory() as uow:
            room = await self._repository_factory(uow.session).create_room(
                command_id=command.meta.request_id,
                actor_account_id=actor,
                settings=command.settings,
                capacity=command.capacity,
                password_salt=salt,
                password_verifier=verifier,
                now=now,
            )
            await uow.commit()
        slots = (RoomSlot(0, SlotStatus.NOT_READY, actor),) + tuple(
            RoomSlot(position, SlotStatus.OPEN) for position in range(1, room.capacity)
        )
        return await self._state.create(RoomState(room, room.version, slots, False, now + self._state_lifetime))

    async def join_room(self, command: JoinRoom) -> RoomState:
        """Verify room credentials, persist admission, then occupy a realtime slot."""
        actor = _actor_id(command.meta)
        now = self._clock.now()
        current = await self._state.find_for_account(actor, at=now)
        if current is not None and current.room.public_id != command.public_id:
            raise MatchAlreadyJoined("account already occupies another room")
        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            room = await repository.get_room(command.public_id, for_update=True)
            if room is None:
                raise MatchNotFound("room is not active")
            self._verify_password(room.password_salt, room.password_verifier, command.password)
            room = await repository.join_room(
                room,
                command_id=command.meta.request_id,
                account_id=actor,
                now=now,
            )
            await uow.commit()
        if current is not None:
            return current
        state = await self._state.get(room.public_id, at=now)
        if state is None:
            return await self._restore_state(room)
        return await self._state.join(room, account_id=actor, expires_at=now + self._state_lifetime)

    async def leave_room(self, command: LeaveRoom) -> RoomState | None:
        """Persist a leave and reflect closure or host transfer in realtime state."""
        actor = _actor_id(command.meta)
        now = self._clock.now()
        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            room = await repository.get_room(command.public_id, for_update=True)
            if room is None:
                return None
            durable_room = await repository.leave_room(
                room,
                command_id=command.meta.request_id,
                account_id=actor,
                now=now,
            )
            await uow.commit()
        return await self._state.leave(command.public_id, account_id=actor, durable_room=durable_room)

    async def kick_participant(self, command: KickParticipant) -> RoomState:
        """Persist a host-authorized kick before removing the target projection."""
        actor = _actor_id(command.meta)
        current = await self._require_state(command.public_id)
        target_slot = current.slot_for(command.target_account_id)
        if target_slot is None:
            raise MatchNotFound("target participant is not in the room")
        if command.target_account_id == current.room.host_account_id:
            raise MatchStateRejected("the room host cannot be kicked")
        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            room = await self._locked_host_room(repository, command.public_id, actor, command.expected_version)
            room = await repository.kick_participant(
                room,
                command_id=command.meta.request_id,
                actor_account_id=actor,
                target_account_id=command.target_account_id,
                now=self._clock.now(),
            )
            await uow.commit()
        updated = await self._state.leave(
            command.public_id,
            account_id=command.target_account_id,
            durable_room=room,
        )
        if updated is None:
            raise MatchNotFound("room projection disappeared during kick")
        return updated

    async def update_settings(self, command: UpdateRoomSettings) -> RoomState:
        """Persist a host-authorized setting replacement and update its projection."""
        actor = _actor_id(command.meta)
        now = self._clock.now()
        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            room = await self._locked_host_room(repository, command.public_id, actor, command.expected_version)
            room = await repository.update_settings(
                room,
                command_id=command.meta.request_id,
                actor_account_id=actor,
                settings=command.settings,
                now=now,
            )
            await uow.commit()
        current = await self._require_state(command.public_id)
        slots = tuple(
            replace(slot, status=SlotStatus.NOT_READY, loaded=False, skipped=False, failed=False)
            if slot.account_id is not None
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
            expires_at=now + self._state_lifetime,
        )
        return await self._state.replace(updated, expected_state_revision=current.state_revision)

    async def change_host(self, command: ChangeHost) -> RoomState:
        """Persist and project a host transfer to an active participant."""
        actor = _actor_id(command.meta)
        current = await self._require_state(command.public_id)
        if current.slot_for(command.target_account_id) is None:
            raise MatchStateRejected("target account is not in the room")
        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            room = await self._locked_host_room(repository, command.public_id, actor, command.expected_version)
            room = await repository.change_host(
                room,
                command_id=command.meta.request_id,
                actor_account_id=actor,
                target_account_id=command.target_account_id,
                now=self._clock.now(),
            )
            await uow.commit()
        updated = replace(current, room=room, state_revision=current.state_revision + 1)
        return await self._state.replace(updated, expected_state_revision=current.state_revision)

    async def change_password(self, command: ChangeRoomPassword) -> RoomState:
        """Persist a host-authorized password replacement and preserve public secrecy."""
        actor = _actor_id(command.meta)
        salt, verifier = self._password_fields(command.password)
        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            room = await self._locked_host_room(repository, command.public_id, actor, command.expected_version)
            room = await repository.change_password(
                room,
                command_id=command.meta.request_id,
                actor_account_id=actor,
                password_salt=salt,
                password_verifier=verifier,
                now=self._clock.now(),
            )
            await uow.commit()
        current = await self._require_state(command.public_id)
        updated = replace(current, room=room, state_revision=current.state_revision + 1)
        return await self._state.replace(updated, expected_state_revision=current.state_revision)

    async def start_round(self, command: StartRound) -> RoomState:
        """Freeze active participants, persist a start, and mark occupied slots playing."""
        actor = _actor_id(command.meta)
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
            repository = self._repository_factory(uow.session)
            room = await self._locked_host_room(repository, command.public_id, actor, command.expected_version)
            room, round_id = await repository.start_round(
                room,
                command_id=command.meta.request_id,
                actor_account_id=actor,
                participants=participants,
                now=self._clock.now(),
            )
            await uow.commit()
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
        )
        return await self._state.replace(updated, expected_state_revision=current.state_revision)

    async def complete_round(self, command: CompleteRound) -> RoomState:
        """Persist round completion and reset occupied slots to not-ready."""
        actor = _actor_id(command.meta)
        current = await self._require_state(command.public_id)
        if not current.in_progress:
            raise MatchStateRejected("room has no round in progress")
        if current.slot_for(actor) is None:
            raise MatchPermissionDenied("only a current participant can complete a round")
        if command.aborted and current.room.host_account_id != actor:
            raise MatchPermissionDenied("only the host can abort a round")
        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            room = await self._locked_room(repository, command.public_id, command.expected_version)
            room = await repository.complete_round(
                room,
                command_id=command.meta.request_id,
                actor_account_id=actor,
                round_id=current.round_id,
                aborted=command.aborted,
                now=self._clock.now(),
            )
            await uow.commit()
        slots = tuple(
            replace(slot, status=SlotStatus.NOT_READY, loaded=False, skipped=False, failed=False)
            if slot.account_id is not None
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
        )
        return await self._state.replace(updated, expected_state_revision=current.state_revision)

    async def get_room(self, public_id: int) -> RoomState:
        """Return one live room projection."""
        return await self._require_state(public_id)

    async def find_room_for_account(self, account_id: int) -> RoomState | None:
        """Return the account's live room projection."""
        state = await self._state.find_for_account(account_id, at=self._clock.now())
        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            room = (
                await repository.get_room(state.room.public_id)
                if state is not None
                else await repository.find_room_for_account(account_id)
            )
            account_ids = await repository.list_participant_account_ids(room) if room is not None else ()
        if room is None:
            if state is not None:
                with suppress(MatchConcurrencyConflict):
                    await self._state.remove(state.room.public_id, expected_state_revision=state.state_revision)
            return None
        if state is None:
            state = await self._restore_state(room, account_ids=account_ids)
        else:
            state = await self._reconcile_state(state, room, account_ids)
        return state if state.slot_for(account_id) is not None else None

    async def list_public_rooms(self, *, limit: int = 100) -> tuple[RoomState, ...]:
        """Return a bounded lobby snapshot."""
        if not 1 <= limit <= 256:
            raise ValueError("room list limit must be between 1 and 256")
        return await self._state.list_public(at=self._clock.now(), limit=limit)

    async def resolve_submission_context(
        self,
        account_id: int,
        beatmap_revision_id: int,
    ) -> MultiplayerSubmissionContext | None:
        """Resolve an authoritative Stable multiplayer attempt when one exists."""
        if account_id < 1 or beatmap_revision_id < 1:
            raise ValueError("submission context identifiers must be positive")
        async with self._uow_factory() as uow:
            return await self._repository_factory(uow.session).resolve_submission_context(
                account_id,
                beatmap_revision_id,
                at=self._clock.now(),
            )

    async def move_slot(self, public_id: int, account_id: int, position: int) -> RoomState:
        """Move one participant between realtime slots."""
        return await self._state.move_slot(public_id, account_id=account_id, target_position=position)

    async def lock_slot(self, public_id: int, actor_account_id: int, position: int) -> RoomState:
        """Toggle an empty slot lock or remove an occupied participant as host."""
        state = await self._require_state(public_id)
        if state.room.host_account_id != actor_account_id:
            raise MatchPermissionDenied("only the host can lock a slot")
        return await self._state.lock_slot(public_id, actor_account_id=actor_account_id, position=position)

    async def set_slot_status(self, public_id: int, account_id: int, status: SlotStatus) -> RoomState:
        """Set one participant readiness state."""
        return await self._state.set_slot_status(public_id, account_id=account_id, status=status)

    async def set_slot_team(self, public_id: int, account_id: int, team: int) -> RoomState:
        """Set one participant team."""
        return await self._state.set_slot_team(public_id, account_id=account_id, team=team)

    async def set_slot_mods(
        self,
        public_id: int,
        account_id: int,
        mods: tuple[CanonicalMod, ...],
    ) -> RoomState:
        """Set one participant free-mod selection."""
        return await self._state.set_slot_mods(public_id, account_id=account_id, mods=mods)

    async def mark_loaded(self, public_id: int, account_id: int) -> RoomState:
        """Mark one participant loaded."""
        return await self._state.mark_loaded(public_id, account_id=account_id)

    async def mark_skipped(self, public_id: int, account_id: int) -> RoomState:
        """Mark one participant skipped."""
        return await self._state.mark_skipped(public_id, account_id=account_id)

    async def mark_failed(self, public_id: int, account_id: int) -> RoomState:
        """Mark one participant failed."""
        return await self._state.mark_failed(public_id, account_id=account_id)

    async def _require_state(self, public_id: int) -> RoomState:
        state = await self._state.get(public_id, at=self._clock.now())
        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            room = await repository.get_room(public_id)
            account_ids = await repository.list_participant_account_ids(room) if room is not None else ()
        if room is None:
            raise MatchNotFound("room has no live state")
        if state is None:
            return await self._restore_state(room, account_ids=account_ids)
        return await self._reconcile_state(state, room, account_ids)

    async def _restore_state(
        self,
        room: RoomRecord,
        *,
        account_ids: tuple[int, ...] | None = None,
    ) -> RoomState:
        """Rebuild a conservative not-ready projection from durable presences."""
        if account_ids is None:
            async with self._uow_factory() as uow:
                account_ids = await self._repository_factory(uow.session).list_participant_account_ids(room)
        if room.host_account_id not in account_ids:
            raise MatchNotFound("active room has no durable host presence")
        occupied = tuple(
            RoomSlot(index, SlotStatus.NOT_READY, account_id) for index, account_id in enumerate(account_ids)
        )
        slots = occupied + tuple(RoomSlot(index, SlotStatus.OPEN) for index in range(len(occupied), room.capacity))
        state = RoomState(
            room,
            room.version,
            slots,
            False,
            self._clock.now() + self._state_lifetime,
        )
        try:
            return await self._state.create(state)
        except MatchAlreadyJoined:
            restored = await self._state.get(room.public_id, at=self._clock.now())
            if restored is None:
                raise
            return restored

    async def _reconcile_state(
        self,
        state: RoomState,
        room: RoomRecord,
        account_ids: tuple[int, ...],
    ) -> RoomState:
        """Repair a stale projection while preserving valid participant slot state."""
        projected_accounts = frozenset(slot.account_id for slot in state.slots if slot.account_id is not None)
        if state.room.version == room.version and projected_accounts == frozenset(account_ids):
            return state
        by_account = {slot.account_id: slot for slot in state.slots if slot.account_id is not None}
        retained = [by_account[account_id] for account_id in account_ids if account_id in by_account]
        used_positions = {slot.position for slot in retained}
        available_positions = iter(position for position in range(room.capacity) if position not in used_positions)
        for account_id in account_ids:
            if account_id not in by_account:
                retained.append(RoomSlot(next(available_positions), SlotStatus.NOT_READY, account_id))
        occupied_by_position = {slot.position: slot for slot in retained}
        slots = tuple(
            occupied_by_position.get(position, RoomSlot(position, SlotStatus.OPEN)) for position in range(room.capacity)
        )
        updated = replace(
            state,
            room=room,
            state_revision=state.state_revision + 1,
            slots=slots,
        )
        try:
            return await self._state.replace(updated, expected_state_revision=state.state_revision)
        except MatchConcurrencyConflict:
            latest = await self._state.get(room.public_id, at=self._clock.now())
            if latest is None:
                return await self._restore_state(room, account_ids=account_ids)
            if latest.room.version == room.version:
                return latest
            raise

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


def _actor_id(meta: CommandMeta) -> int:
    actor = meta.actor
    if actor is None:
        raise ValueError("multiplayer command requires an authenticated actor")
    return actor.account_id
