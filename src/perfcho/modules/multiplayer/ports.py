"""Define transaction-bound and realtime multiplayer ports."""

import uuid
from datetime import datetime
from typing import Protocol

from perfcho.modules.common.ports import UnitOfWork
from perfcho.modules.multiplayer.models import (
    DurableRoomSnapshot,
    RoomRecord,
    RoomSettings,
    RoomState,
    RoundParticipantSelection,
    SlotStatus,
)
from perfcho.modules.scoring.models import CanonicalMod, MultiplayerSubmissionContext


class MultiplayerUnitOfWork(UnitOfWork, Protocol):
    """Expose the transaction resource used to bind multiplayer persistence."""

    @property
    def session(self) -> object:
        """Return the active transaction resource."""
        ...


class MultiplayerRepository(Protocol):
    """Persist authoritative room, session, participation, and event facts."""

    async def create_room(
        self,
        *,
        command_id: uuid.UUID,
        actor_account_id: int,
        connection_session_id: uuid.UUID,
        settings: RoomSettings,
        capacity: int,
        password_salt: str | None,
        password_verifier: str | None,
        now: datetime,
    ) -> RoomRecord:
        """Create a room, active session, host presence, and initial event."""
        ...

    async def get_room(self, public_id: int, *, for_update: bool = False) -> RoomRecord | None:
        """Resolve one active durable room and current session."""
        ...

    async def find_room_for_account(self, account_id: int) -> RoomRecord | None:
        """Resolve the account's current active room."""
        ...

    async def list_active_rooms(self, *, limit: int) -> tuple[RoomRecord, ...]:
        """List active durable rooms for lobby projection recovery."""
        ...

    async def load_snapshot(self, room: RoomRecord) -> DurableRoomSnapshot:
        """Load active presences and any active frozen round for recovery."""
        ...

    async def find_command_room(self, command_id: uuid.UUID) -> RoomRecord | None:
        """Resolve an active room previously mutated by an idempotent command."""
        ...

    async def list_participant_account_ids(self, room: RoomRecord) -> tuple[int, ...]:
        """List current participant accounts in deterministic join order."""
        ...

    async def join_room(
        self,
        room: RoomRecord,
        *,
        command_id: uuid.UUID,
        account_id: int,
        connection_session_id: uuid.UUID,
        now: datetime,
    ) -> RoomRecord:
        """Persist participant admission and one open session presence."""
        ...

    async def leave_room(
        self,
        room: RoomRecord,
        *,
        command_id: uuid.UUID,
        account_id: int,
        connection_session_id: uuid.UUID | None,
        reason: str,
        now: datetime,
    ) -> RoomRecord | None:
        """Close a presence, transfer host, or close an empty room."""
        ...

    async def kick_participant(
        self,
        room: RoomRecord,
        *,
        command_id: uuid.UUID,
        actor_account_id: int,
        target_account_id: int,
        now: datetime,
    ) -> RoomRecord:
        """Close a non-host presence under host authority."""
        ...

    async def update_settings(
        self,
        room: RoomRecord,
        *,
        command_id: uuid.UUID,
        actor_account_id: int,
        settings: RoomSettings,
        now: datetime,
    ) -> RoomRecord:
        """Replace durable room settings and increment aggregate version."""
        ...

    async def change_host(
        self,
        room: RoomRecord,
        *,
        command_id: uuid.UUID,
        actor_account_id: int,
        target_account_id: int,
        now: datetime,
    ) -> RoomRecord:
        """Transfer the active session host and append an event."""
        ...

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
        """Replace durable room password proof fields and append an event."""
        ...

    async def start_round(
        self,
        room: RoomRecord,
        *,
        command_id: uuid.UUID,
        actor_account_id: int,
        participants: tuple[RoundParticipantSelection, ...],
        now: datetime,
    ) -> tuple[RoomRecord, uuid.UUID | None]:
        """Persist a frozen round when content is known, otherwise append an unranked start event."""
        ...

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
        """Complete or abort the current round and append an event."""
        ...

    async def resolve_submission_context(
        self,
        account_id: int,
        beatmap_revision_id: int,
        *,
        at: datetime,
    ) -> MultiplayerSubmissionContext | None:
        """Resolve the latest unconsumed frozen attempt for a Stable submission."""
        ...


class MultiplayerRepositoryFactory(Protocol):
    """Bind a multiplayer repository to one Unit of Work resource."""

    def __call__(self, session: object) -> MultiplayerRepository:
        """Return a transaction-bound repository."""
        ...


class MultiplayerAccessPolicy(Protocol):
    """Enforce canonical multiplayer permissions and account restrictions."""

    async def require(self, account_id: int, permissions: tuple[str, ...], *, at: datetime) -> None:
        """Raise a domain error unless every permission is currently effective."""
        ...


class MultiplayerAccessPolicyFactory(Protocol):
    """Bind multiplayer access policy evaluation to one transaction resource."""

    def __call__(self, session: object) -> MultiplayerAccessPolicy:
        """Return a transaction-bound policy evaluator."""
        ...


class MultiplayerStateRepository(Protocol):
    """Coordinate expiring room projections with compare-and-set semantics."""

    async def create(self, state: RoomState) -> RoomState:
        """Publish a new state if its room and host are not already active."""
        ...

    async def get(self, public_id: int, *, at: datetime) -> RoomState | None:
        """Return one unexpired state."""
        ...

    async def find_for_account(self, account_id: int, *, at: datetime) -> RoomState | None:
        """Return the live room containing an account."""
        ...

    async def list_public(self, *, at: datetime, limit: int) -> tuple[RoomState, ...]:
        """Return bounded active public-room projections."""
        ...

    async def replace(
        self,
        state: RoomState,
        *,
        expected_state_revision: int,
        expected_session_id: uuid.UUID,
    ) -> RoomState:
        """Replace one state only at the expected revision."""
        ...

    async def remove(
        self,
        public_id: int,
        *,
        expected_state_revision: int,
        expected_session_id: uuid.UUID,
    ) -> None:
        """Remove one state and its account indexes at the expected revision."""
        ...

    async def join(
        self,
        room: RoomRecord,
        *,
        account_id: int,
        expires_at: datetime,
    ) -> RoomState:
        """Occupy the first open slot using a bounded CAS retry."""
        ...

    async def leave(self, public_id: int, *, account_id: int, durable_room: RoomRecord | None) -> RoomState | None:
        """Release an occupied slot and apply durable host/closure state."""
        ...

    async def move_slot(self, public_id: int, *, account_id: int, target_position: int) -> RoomState:
        """Move an account to an open slot."""
        ...

    async def lock_slot(self, public_id: int, *, actor_account_id: int, position: int) -> RoomState:
        """Toggle an empty slot lock or remove an occupied participant."""
        ...

    async def set_slot_status(self, public_id: int, *, account_id: int, status: SlotStatus) -> RoomState:
        """Update one participant's ephemeral readiness status."""
        ...

    async def set_slot_team(self, public_id: int, *, account_id: int, team: int) -> RoomState:
        """Update one participant's team selection."""
        ...

    async def set_slot_mods(
        self,
        public_id: int,
        *,
        account_id: int,
        mods: tuple[CanonicalMod, ...],
    ) -> RoomState:
        """Update one participant's free-mod selection."""
        ...

    async def mark_loaded(self, public_id: int, *, account_id: int) -> RoomState:
        """Mark one playing participant loaded."""
        ...

    async def mark_skipped(self, public_id: int, *, account_id: int) -> RoomState:
        """Mark one playing participant as requesting a skip."""
        ...

    async def mark_failed(self, public_id: int, *, account_id: int) -> RoomState:
        """Mark one playing participant failed."""
        ...
