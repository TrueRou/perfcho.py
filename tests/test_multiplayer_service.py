import hashlib
import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import TracebackType
from typing import cast

import pytest

from perfcho.modules.common import Actor, ClientContext, CommandMeta
from perfcho.modules.common.models import PendingEvent
from perfcho.modules.common.ports import Clock
from perfcho.modules.multiplayer import (
    ChangeHost,
    CompleteRound,
    CreateRoom,
    DurableRoomSnapshot,
    JoinRoom,
    MatchPasswordRejected,
    MatchPermissionDenied,
    MatchStateRejected,
    MultiplayerAccessPolicy,
    MultiplayerMutationKind,
    MultiplayerRepository,
    MultiplayerService,
    MultiplayerStateRepository,
    ProjectionStatus,
    RoomRecord,
    RoomSettings,
    RoomSlot,
    RoomState,
    RoundParticipantSelection,
    SlotStatus,
    TeamMode,
    WinCondition,
)
from perfcho.modules.multiplayer.services import _settings_transition
from perfcho.modules.scoring import CanonicalMod, Ruleset, ScoreboardVariant

NOW = datetime(2026, 7, 29, 12, tzinfo=UTC)


class FixedClock:
    def now(self) -> datetime:
        return NOW


class FakeUnitOfWork:
    def __init__(self) -> None:
        self.session = object()
        self.committed = False

    async def __aenter__(self) -> FakeUnitOfWork:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc_value, traceback

    async def commit(self) -> None:
        self.committed = True


class FakeOutbox:
    def __init__(self) -> None:
        self.events: list[PendingEvent] = []

    async def append(self, event: PendingEvent) -> uuid.UUID:
        self.events.append(event)
        return uuid.uuid7()


class FakeRepository:
    def __init__(self) -> None:
        self.room: RoomRecord | None = None
        self.accounts: tuple[int, ...] = ()
        self.created_password: tuple[str | None, str | None] | None = None
        self.command_rooms: dict[uuid.UUID, RoomRecord] = {}
        self.submission_query: tuple[int, int, datetime, datetime, datetime] | None = None
        self.round_id: uuid.UUID | None = None
        self.round_participants: tuple[RoundParticipantSelection, ...] = ()
        self.result_outbox: FakeOutbox | None = None

    async def create_room(self, **values: object) -> RoomRecord:
        actor = cast(int, values["actor_account_id"])
        settings = cast(RoomSettings, values["settings"])
        salt = cast(str | None, values["password_salt"])
        verifier = cast(str | None, values["password_verifier"])
        self.created_password = (salt, verifier)
        self.room = RoomRecord(
            uuid.uuid7(),
            7,
            uuid.uuid7(),
            1,
            actor,
            actor,
            cast(int, values["capacity"]),
            settings,
            verifier is not None,
            salt,
            verifier,
        )
        self.accounts = (actor,)
        self.command_rooms[cast(uuid.UUID, values["command_id"])] = self.room
        return self.room

    async def find_command_room(self, command_id: uuid.UUID) -> RoomRecord | None:
        return self.command_rooms.get(command_id)

    async def load_snapshot(self, room: RoomRecord) -> DurableRoomSnapshot:
        return DurableRoomSnapshot(room, self.accounts, self.round_id, self.round_participants)

    async def find_room_for_account(self, account_id: int) -> RoomRecord | None:
        return self.room if self.room is not None and account_id in self.accounts else None

    async def list_participant_account_ids(self, room: RoomRecord) -> tuple[int, ...]:
        assert self.room is not None and room.public_id == self.room.public_id
        return self.accounts

    async def get_room(self, public_id: int, *, for_update: bool = False) -> RoomRecord | None:
        del for_update
        return self.room if self.room is not None and self.room.public_id == public_id else None

    async def join_room(self, room: RoomRecord, *, account_id: int, **values: object) -> RoomRecord:
        command_id = cast(uuid.UUID, values["command_id"])
        if account_id not in self.accounts:
            self.accounts += (account_id,)
            room = replace(room, version=room.version + 1)
            self.room = room
        self.command_rooms[command_id] = room
        return room

    async def change_host(
        self,
        room: RoomRecord,
        *,
        target_account_id: int,
        **values: object,
    ) -> RoomRecord:
        self.room = replace(room, version=room.version + 1, host_account_id=target_account_id)
        self.command_rooms[cast(uuid.UUID, values["command_id"])] = self.room
        return self.room

    async def complete_round(self, room: RoomRecord, **values: object) -> RoomRecord:
        self.round_id = None
        self.round_participants = ()
        return replace(room, version=room.version + 1)

    async def resolve_submission_context(
        self,
        account_id: int,
        beatmap_revision_id: int,
        *,
        started_at: datetime,
        ended_at: datetime,
        at: datetime,
    ) -> None:
        self.submission_query = (account_id, beatmap_revision_id, started_at, ended_at, at)
        return None


class AllowMultiplayer:
    async def require(self, account_id: int, permissions: tuple[str, ...], *, at: datetime) -> None:
        assert account_id > 0 and permissions and at == NOW


class FakeState:
    def __init__(self) -> None:
        self.state: RoomState | None = None

    async def create(self, state: RoomState) -> RoomState:
        self.state = replace(state, room=replace(state.room, password_salt=None, password_verifier=None))
        return self.state

    async def get(self, public_id: int, *, at: datetime) -> RoomState | None:
        del at
        return self.state if self.state is not None and self.state.room.public_id == public_id else None

    async def find_for_account(self, account_id: int, *, at: datetime) -> RoomState | None:
        del at
        return self.state if self.state is not None and self.state.slot_for(account_id) is not None else None

    async def replace(
        self,
        state: RoomState,
        *,
        expected_state_revision: int,
        expected_session_id: uuid.UUID,
    ) -> RoomState:
        assert self.state is not None and self.state.state_revision == expected_state_revision
        assert self.state.room.session_id == expected_session_id
        self.state = replace(state, room=replace(state.room, password_salt=None, password_verifier=None))
        return self.state

    async def join(self, room: RoomRecord, *, account_id: int, expires_at: datetime) -> RoomState:
        assert self.state is not None
        target = next(slot for slot in self.state.slots if slot.status is SlotStatus.OPEN)
        slots = tuple(
            RoomSlot(slot.position, SlotStatus.NOT_READY, account_id) if slot.position == target.position else slot
            for slot in self.state.slots
        )
        self.state = replace(
            self.state,
            room=replace(room, password_salt=None, password_verifier=None),
            state_revision=self.state.state_revision + 1,
            slots=slots,
            expires_at=expires_at,
        )
        return self.state


def settings() -> RoomSettings:
    return RoomSettings(
        "Room",
        "Artist - Title [Hard]",
        42,
        b"m" * 16,
        Ruleset.OSU,
        ScoreboardVariant.VANILLA,
        TeamMode.HEAD_TO_HEAD,
        WinCondition.SCORE,
    )


def meta(account_id: int, label: str) -> CommandMeta:
    return CommandMeta(
        uuid.uuid7(),
        f"multi:{label}:{account_id}",
        hashlib.sha256(label.encode()).digest(),
        Actor(account_id, uuid.uuid7()),
        ClientContext("stable", "b20260711.1", None, "127.0.0.1"),
        NOW,
    )


def service(
    repository: FakeRepository,
    state: FakeState,
    *,
    access_policy: object | None = None,
) -> MultiplayerService:
    outbox = FakeOutbox()
    repository.result_outbox = outbox
    return MultiplayerService(
        FakeUnitOfWork,
        lambda session: cast(MultiplayerRepository, repository),
        lambda session: outbox,
        cast(MultiplayerStateRepository, state),
        cast(Clock, FixedClock()),
        b"room-password-key",
        access_policy_factory=lambda session: cast(MultiplayerAccessPolicy, access_policy or AllowMultiplayer()),
        state_lifetime=timedelta(hours=1),
    )


def test_round_participant_slot_uses_canonical_room_capacity() -> None:
    participant = RoundParticipantSelection(10, 16, 0)

    assert participant.slot_position == 16


@pytest.mark.asyncio
async def test_create_hashes_password_and_publishes_no_secret() -> None:
    repository = FakeRepository()
    state = FakeState()

    created = await service(repository, state).create_room(CreateRoom(meta(10, "create"), settings(), 16, "secret"))

    assert repository.created_password is not None
    salt, verifier = repository.created_password
    assert salt and verifier and "secret" not in verifier
    assert created.room.password_protected
    assert created.room.password_salt is None
    assert created.room.password_verifier is None
    assert created.slot_for(10) is not None


@pytest.mark.asyncio
async def test_join_rejects_wrong_password_before_persisting_presence() -> None:
    repository = FakeRepository()
    state = FakeState()
    multiplayer = service(repository, state)
    await multiplayer.create_room(CreateRoom(meta(10, "create"), settings(), 16, "secret"))

    with pytest.raises(MatchPasswordRejected):
        await multiplayer.join_room(
            __import__("perfcho.modules.multiplayer", fromlist=["JoinRoom"]).JoinRoom(meta(11, "join"), 7, "wrong")
        )

    assert repository.accounts == (10,)


@pytest.mark.asyncio
async def test_missing_redis_state_is_rebuilt_from_durable_participants() -> None:
    repository = FakeRepository()
    state = FakeState()
    multiplayer = service(repository, state)
    await multiplayer.create_room(CreateRoom(meta(10, "create"), settings(), 16))
    repository.accounts = (10, 11)
    state.state = None

    restored = await multiplayer.find_room_for_account(11)

    assert restored is not None
    assert tuple(slot.account_id for slot in restored.slots[:2]) == (10, 11)
    assert restored.room.host_account_id == 10


@pytest.mark.asyncio
async def test_only_current_host_can_transfer_host() -> None:
    repository = FakeRepository()
    state = FakeState()
    multiplayer = service(repository, state)
    created = await multiplayer.create_room(CreateRoom(meta(10, "create"), settings(), 16))
    repository.accounts = (10, 11)
    state.state = replace(
        created,
        slots=(created.slots[0], RoomSlot(1, SlotStatus.NOT_READY, 11), *created.slots[2:]),
    )

    with pytest.raises(MatchPermissionDenied):
        await multiplayer.change_host(ChangeHost(meta(11, "host"), 7, 1, 11))


@pytest.mark.asyncio
async def test_committed_create_returns_durable_snapshot_when_redis_is_down(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = FakeRepository()
    state = FakeState()

    async def unavailable(value: RoomState) -> RoomState:
        del value
        raise ConnectionError("redis unavailable")

    monkeypatch.setattr(state, "create", unavailable)
    logged: list[tuple[str, str, dict[str, object]]] = []
    monkeypatch.setattr("perfcho.modules.multiplayer.services.rate_limit", lambda key: True)
    monkeypatch.setattr(
        "perfcho.modules.multiplayer.services.log_event",
        lambda level, event, **fields: logged.append((level, event, fields)),
    )

    created = await service(repository, state).create_room(CreateRoom(meta(10, "create"), settings(), 16))

    assert created.projection_status is ProjectionStatus.DURABLE_RECOVERY
    assert created.slot_for(10) is not None
    assert logged[0][:2] == ("INFO", "multiplayer.room.created")
    assert logged[1][:2] == ("WARNING", "multiplayer.projection.degraded")
    projection_fields = logged[1][2]
    projection_exception = projection_fields["exception"]
    assert isinstance(projection_exception, BaseException)
    assert projection_fields == {
        "operation": "publish_create",
        "public_id": 7,
        "version": 1,
        "error_type": "ConnectionError",
        "exception": projection_exception,
    }
    assert projection_exception.args == ("redis unavailable",)


@pytest.mark.asyncio
async def test_admission_token_is_bound_to_room_session_and_recipient() -> None:
    repository = FakeRepository()
    state = FakeState()
    multiplayer = service(repository, state)
    await multiplayer.create_room(CreateRoom(meta(10, "create"), settings(), 16, "secret"))

    token = await multiplayer.issue_admission_token(7, inviter_account_id=10, recipient_account_id=11)

    assert "secret" not in token
    with pytest.raises(MatchPasswordRejected):
        await multiplayer.join_room(JoinRoom(meta(12, "join-wrong-recipient"), 7, token))
    joined = await multiplayer.join_room(JoinRoom(meta(11, "join-token"), 7, token))
    assert joined.slot_for(11) is not None


@pytest.mark.asyncio
async def test_same_idempotency_key_replays_create_without_new_room() -> None:
    repository = FakeRepository()
    multiplayer = service(repository, FakeState())
    command = CreateRoom(meta(10, "same-create"), settings(), 16)

    first = await multiplayer.create_room(command)
    replayed = await multiplayer.create_room(command)

    assert replayed.room.room_id == first.room.room_id
    assert len(repository.command_rooms) == 1


@pytest.mark.asyncio
async def test_same_join_command_after_leave_creates_new_presence() -> None:
    repository = FakeRepository()
    state = FakeState()
    multiplayer = service(repository, state)
    await multiplayer.create_room(CreateRoom(meta(10, "create"), settings(), 16, "secret"))
    command = JoinRoom(meta(11, "same-join"), 7, "secret")

    first = await multiplayer.join_room(command)
    assert first.slot_for(11) is not None
    with pytest.raises(MatchPasswordRejected):
        await multiplayer.join_room(JoinRoom(meta(11, "new-wrong-join"), 7, "wrong"))
    replayed = await multiplayer.join_room(command)
    assert replayed.room.version == first.room.version
    assert len(repository.command_rooms) == 2
    assert repository.room is not None
    repository.accounts = (10,)
    repository.room = replace(repository.room, version=repository.room.version + 1)
    assert state.state is not None
    state.state = replace(
        state.state,
        room=repository.room,
        state_revision=state.state.state_revision + 1,
        slots=tuple(
            RoomSlot(slot.position, SlotStatus.OPEN) if slot.account_id == 11 else slot for slot in state.state.slots
        ),
    )

    rejoined = await multiplayer.join_room(command)

    assert rejoined.slot_for(11) is not None
    assert repository.accounts == (10, 11)
    assert rejoined.room.version == first.room.version + 2
    assert len(repository.command_rooms) == 3


@pytest.mark.asyncio
async def test_canonical_service_enforces_host_permission_before_create() -> None:
    class DenyMultiplayer:
        async def require(self, account_id: int, permissions: tuple[str, ...], *, at: datetime) -> None:
            del account_id, permissions, at
            raise MatchPermissionDenied("restricted")

    repository = FakeRepository()
    multiplayer = service(repository, FakeState(), access_policy=DenyMultiplayer())

    with pytest.raises(MatchPermissionDenied):
        await multiplayer.create_room(CreateRoom(meta(10, "denied-create"), settings(), 16))
    assert repository.room is None


@pytest.mark.asyncio
async def test_submission_context_preserves_gameplay_interval_for_rematch_selection() -> None:
    repository = FakeRepository()
    multiplayer = service(repository, FakeState())
    started_at = NOW - timedelta(minutes=2)
    ended_at = NOW - timedelta(minutes=1)

    assert (
        await multiplayer.resolve_submission_context(
            10,
            20,
            started_at=started_at,
            ended_at=ended_at,
        )
        is None
    )
    assert repository.submission_query == (10, 20, started_at, ended_at, NOW)


@pytest.mark.asyncio
async def test_personal_free_mods_cannot_change_during_an_active_round() -> None:
    repository = FakeRepository()
    state = FakeState()
    multiplayer = service(repository, state)
    created = await multiplayer.create_room(
        CreateRoom(meta(10, "create-free-mod"), replace(settings(), free_mods=True), 16)
    )
    round_id = uuid.uuid7()
    repository.round_id = round_id
    repository.round_participants = (RoundParticipantSelection(10, 0, 0),)
    state.state = replace(
        created,
        in_progress=True,
        round_id=round_id,
        round_participant_account_ids=(10,),
    )

    with pytest.raises(MatchStateRejected, match="active round"):
        await multiplayer.set_slot_mods(7, 10, (CanonicalMod("HD"),))


@pytest.mark.asyncio
async def test_complete_round_writes_results_projection_event_in_command_transaction() -> None:
    repository = FakeRepository()
    state = FakeState()
    multiplayer = service(repository, state)
    created = await multiplayer.create_room(CreateRoom(meta(10, "create-complete"), settings(), 16))
    round_id = uuid.uuid7()
    repository.round_id = round_id
    repository.round_participants = (RoundParticipantSelection(10, 0, 0),)
    state.state = replace(
        created,
        in_progress=True,
        round_id=round_id,
        round_participant_account_ids=(10,),
    )

    result = await multiplayer.complete_round(CompleteRound(meta(10, "complete"), 7, created.room.version, False))

    assert result.kind is MultiplayerMutationKind.ROUND_COMPLETED
    assert result.round_participant_account_ids == (10,)
    assert repository.result_outbox is not None
    event = repository.result_outbox.events[-1]
    assert event.event_type == "multiplayer.round-completed.v1"
    assert event.payload == {
        "round_id": str(round_id),
        "session_id": str(created.room.session_id),
        "room_id": str(created.room.room_id),
        "aborted": False,
    }
    assert event.consumers == ("multiplayer-results-consumer.v1",)
    assert event.partition_key == f"round:{round_id}"


def test_stable_settings_transition_migrates_free_mods_and_team_defaults() -> None:
    room = RoomRecord(
        uuid.uuid7(),
        7,
        uuid.uuid7(),
        1,
        10,
        10,
        2,
        replace(settings(), mods=(CanonicalMod("HD"), CanonicalMod("DT"))),
    )
    current = RoomState(
        room,
        1,
        (RoomSlot(0, SlotStatus.NOT_READY, 10), RoomSlot(1, SlotStatus.NOT_READY, 11)),
        False,
        NOW + timedelta(hours=1),
    )

    transitioned, slots = _settings_transition(
        current,
        replace(room.settings, free_mods=True, team_mode=TeamMode.TEAM_VS),
    )

    assert tuple(mod.acronym for mod in transitioned.mods) == ("DT",)
    assert all(tuple(mod.acronym for mod in slot.mods) == ("HD",) for slot in slots)
    assert tuple(slot.team for slot in slots) == (1, 1)
