import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import cast

import pytest

from perfcho.composition import StableServices
from perfcho.infra.settings import Settings
from perfcho.modules.authorization import AuthorizationQueryService
from perfcho.modules.common import Clock, IdGenerator
from perfcho.modules.identity import IdentityService, ResolvedStableSession
from perfcho.modules.multiplayer import (
    CreateRoom,
    MultiplayerService,
    RoomRecord,
    RoomSettings,
    RoomSlot,
    RoomState,
    SlotStatus,
    TeamMode,
    WinCondition,
)
from perfcho.modules.realtime import RealtimeRepository, RealtimeSession
from perfcho.modules.scoring import Ruleset, ScoreboardVariant
from perfcho.realtime.stable import (
    ClientPacket,
    MultiplayerMatch,
    PacketReader,
    PacketWriter,
    ServerPacket,
    UserPresence,
    UserStats,
)
from perfcho.realtime.stable.dispatcher import StableRuntimeContext, dispatch_packets

NOW = datetime(2026, 7, 29, 12, tzinfo=UTC)
EXPIRY = NOW + timedelta(minutes=5)


class FixedClock:
    def now(self) -> datetime:
        return NOW


class FakeIds:
    def new(self) -> uuid.UUID:
        return uuid.uuid7()


class FakeMultiplayer:
    def __init__(self, state: RoomState) -> None:
        self.state = state
        self.created: CreateRoom | None = None

    async def create_room(self, command: CreateRoom) -> RoomState:
        self.created = command
        return self.state

    async def find_room_for_account(self, account_id: int) -> RoomState | None:
        return self.state if self.state.slot_for(account_id) is not None else None

    async def set_slot_status(self, public_id: int, account_id: int, status: SlotStatus) -> RoomState:
        assert public_id == self.state.room.public_id
        slots = tuple(
            replace(slot, status=status) if slot.account_id == account_id else slot for slot in self.state.slots
        )
        self.state = replace(self.state, state_revision=self.state.state_revision + 1, slots=slots)
        return self.state


def room_state() -> RoomState:
    settings = RoomSettings(
        "Room",
        "Artist - Title [Hard]",
        42,
        b"m" * 16,
        Ruleset.OSU,
        ScoreboardVariant.VANILLA,
        TeamMode.HEAD_TO_HEAD,
        WinCondition.SCORE,
    )
    room = RoomRecord(uuid.uuid7(), 7, uuid.uuid7(), 1, 10, 10, 16, settings, True)
    slots = (RoomSlot(0, SlotStatus.NOT_READY, 10),) + tuple(
        RoomSlot(position, SlotStatus.OPEN) for position in range(1, 16)
    )
    return RoomState(room, 1, slots, False, EXPIRY)


def context() -> StableRuntimeContext:
    session_id = uuid.uuid7()
    return StableRuntimeContext(
        ResolvedStableSession(10, "host", 1, session_id, None, "b20260711.1", None, EXPIRY),
        RealtimeSession(session_id, 10, 1, EXPIRY),
        UserPresence(10, "host", 0, 0, 1, 0, 0.0, 0.0, 0),
        UserStats(10, 0, "", "", 0, 0, 0, 0, 0.0, 0, 0, 0, 0),
    )


def services(multiplayer: FakeMultiplayer) -> StableServices:
    return StableServices(
        identity=cast(IdentityService, object()),
        authorization=cast(AuthorizationQueryService, object()),
        realtime=cast(RealtimeRepository, object()),
        clock=cast(Clock, FixedClock()),
        id_generator=cast(IdGenerator, FakeIds()),
        settings=Settings(),
        multiplayer=cast(MultiplayerService, multiplayer),
    )


def client_packet(packet_type: ClientPacket, write: object | None = None) -> bytes:
    writer = PacketWriter()
    with writer.packet(packet_type):
        if isinstance(write, MultiplayerMatch):
            writer.write_multiplayer_match(write)
    return writer.to_bytes()


@pytest.mark.asyncio
async def test_create_match_maps_wire_settings_and_returns_join_success() -> None:
    multiplayer = FakeMultiplayer(room_state())
    incoming = MultiplayerMatch(
        name="Room",
        password="secret",
        beatmap_name="Artist - Title [Hard]",
        beatmap_id=42,
        beatmap_md5=(b"m" * 16).hex(),
        host_id=10,
    )

    response = await dispatch_packets(
        client_packet(ClientPacket.CREATE_MATCH, incoming),
        context(),
        services(multiplayer),
    )

    packet = next(PacketReader(response, packet_enum=ServerPacket))
    assert packet.packet_type is ServerPacket.MATCH_JOIN_SUCCESS
    match = packet.payload.read_multiplayer_match()
    assert match.match_id == 7 and match.password == "secret"
    assert multiplayer.created is not None
    assert multiplayer.created.settings.external_beatmap_id == 42


@pytest.mark.asyncio
async def test_ready_updates_slot_and_returns_hidden_password_match_state() -> None:
    multiplayer = FakeMultiplayer(room_state())

    response = await dispatch_packets(
        client_packet(ClientPacket.MATCH_READY),
        context(),
        services(multiplayer),
    )

    packet = next(PacketReader(response, packet_enum=ServerPacket))
    assert packet.packet_type is ServerPacket.UPDATE_MATCH
    match = packet.payload.read_multiplayer_match()
    assert match.slot_statuses[0] == 8
    assert match.password == ""
