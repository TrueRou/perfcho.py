import hashlib
import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import TracebackType
from typing import cast

import pytest

from perfcho.modules.common import Actor, ClientContext, CommandMeta
from perfcho.modules.common.ports import Clock
from perfcho.modules.multiplayer import (
    ChangeHost,
    CreateRoom,
    MatchPasswordRejected,
    MatchPermissionDenied,
    MultiplayerRepository,
    MultiplayerService,
    MultiplayerStateRepository,
    RoomRecord,
    RoomSettings,
    RoomSlot,
    RoomState,
    SlotStatus,
    TeamMode,
    WinCondition,
)
from perfcho.modules.scoring import Ruleset, ScoreboardVariant

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


class FakeRepository:
    def __init__(self) -> None:
        self.room: RoomRecord | None = None
        self.accounts: tuple[int, ...] = ()
        self.created_password: tuple[str | None, str | None] | None = None

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
        return self.room

    async def find_room_for_account(self, account_id: int) -> RoomRecord | None:
        return self.room if self.room is not None and account_id in self.accounts else None

    async def list_participant_account_ids(self, room: RoomRecord) -> tuple[int, ...]:
        assert self.room is not None and room.public_id == self.room.public_id
        return self.accounts

    async def get_room(self, public_id: int, *, for_update: bool = False) -> RoomRecord | None:
        del for_update
        return self.room if self.room is not None and self.room.public_id == public_id else None

    async def join_room(self, room: RoomRecord, *, account_id: int, **values: object) -> RoomRecord:
        del values
        if account_id not in self.accounts:
            self.accounts += (account_id,)
            room = replace(room, version=room.version + 1)
            self.room = room
        return room

    async def change_host(
        self,
        room: RoomRecord,
        *,
        target_account_id: int,
        **values: object,
    ) -> RoomRecord:
        del values
        self.room = replace(room, version=room.version + 1, host_account_id=target_account_id)
        return self.room


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

    async def replace(self, state: RoomState, *, expected_state_revision: int) -> RoomState:
        assert self.state is not None and self.state.state_revision == expected_state_revision
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


def service(repository: FakeRepository, state: FakeState) -> MultiplayerService:
    return MultiplayerService(
        FakeUnitOfWork,
        lambda session: cast(MultiplayerRepository, repository),
        cast(MultiplayerStateRepository, state),
        cast(Clock, FixedClock()),
        b"room-password-key",
        state_lifetime=timedelta(hours=1),
    )


@pytest.mark.asyncio
async def test_create_hashes_password_and_publishes_no_secret() -> None:
    repository = FakeRepository()
    state = FakeState()

    created = await service(repository, state).create_room(CreateRoom(meta(10, "create"), settings(), "secret"))

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
    await multiplayer.create_room(CreateRoom(meta(10, "create"), settings(), "secret"))

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
    await multiplayer.create_room(CreateRoom(meta(10, "create"), settings()))
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
    created = await multiplayer.create_room(CreateRoom(meta(10, "create"), settings()))
    repository.accounts = (10, 11)
    state.state = replace(
        created,
        slots=(created.slots[0], RoomSlot(1, SlotStatus.NOT_READY, 11), *created.slots[2:]),
    )

    with pytest.raises(MatchPermissionDenied):
        await multiplayer.change_host(ChangeHost(meta(11, "host"), 7, 1, 11))
