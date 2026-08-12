import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Protocol, cast

import pytest

from perfcho.api.stable.bubbles import StableBubbleRenderer, canonicalize_presence
from perfcho.api.stable.dispatcher import StableRuntimeContext
from perfcho.api.stable.dispatcher import dispatch_packets as dispatch_bubbles
from perfcho.api.stable.dispatcher.multiplayer import _broadcast_lobby, _settings_from_wire
from perfcho.api.stable.realtime import (
    ClientPacket,
    Message,
    MultiplayerMatch,
    PacketReader,
    PacketWriter,
    ScoreFrame,
    ServerPacket,
    UserPresence,
    UserStats,
)
from perfcho.infra.compose import StableServices
from perfcho.infra.settings import Settings
from perfcho.modules.authorization import AuthorizationQueryService
from perfcho.modules.bot import BotCommandService
from perfcho.modules.common import Clock, IdGenerator
from perfcho.modules.community import CommunityService
from perfcho.modules.identity import IdentityService, ResolvedClientSession
from perfcho.modules.multiplayer import (
    CreateRoom,
    MultiplayerMutationKind,
    MultiplayerMutationResult,
    MultiplayerService,
    RoomRecord,
    RoomSettings,
    RoomSlot,
    RoomState,
    SlotStatus,
    TeamMode,
    WinCondition,
)
from perfcho.modules.multiplayer.commands import (
    AccountResolver,
    BeatmapResolver,
    MultiplayerCommandDependencies,
    build_multiplayer_commands,
)
from perfcho.modules.realtime import (
    MultiplayerRoomAction,
    MultiplayerRoomBubble,
    PresenceSnapshot,
    RealtimeBubble,
    RealtimeBubbleBus,
    RealtimeSession,
    RealtimeStateRepository,
    SessionFence,
    multiplayer_room_snapshot,
)
from perfcho.modules.scoring import Ruleset, ScoreboardVariant
from perfcho.modules.social import SocialService

NOW = datetime(2026, 7, 29, 12, tzinfo=UTC)


async def dispatch_packets(body: bytes, context: StableRuntimeContext, services: StableServices) -> bytes:
    bubbles = await dispatch_bubbles(body, context, services)
    rendered = StableBubbleRenderer().render_many(bubbles, max_bytes=services.settings.stable_max_response_bytes)
    return rendered + bytes(context.stable_output)


EXPIRY = NOW + timedelta(minutes=5)


def presence_snapshot(account_id: int, name: str) -> PresenceSnapshot:
    presence = UserPresence(account_id, name, 0, 0, 1, 0, 0.0, 0.0, 0)
    stats = UserStats(account_id, 0, "", "", 0, 0, 0, 0, 0.0, 0, 0, 0, 0)
    identity, activity, statistics = canonicalize_presence(presence, stats, country_code=None)
    return PresenceSnapshot(account_id, 1, identity, activity, statistics, EXPIRY, uuid.uuid7())


class FixedClock:
    def now(self) -> datetime:
        return NOW


class FakeIds:
    def new(self) -> uuid.UUID:
        return uuid.uuid7()


class SettingsCommand(Protocol):
    settings: RoomSettings


class CompletionCommand(Protocol):
    aborted: bool


class FakeMultiplayer:
    def __init__(self, state: RoomState) -> None:
        self.state = state
        self.created: CreateRoom | None = None
        self.durable_find_calls = 0
        self.started: object | None = None
        self.admission: tuple[int, int, int] | None = None

    async def create_room(self, command: CreateRoom) -> RoomState:
        self.created = command
        return self.state

    async def join_room(self, command: object) -> RoomState:
        del command
        return self.state

    async def leave_room(self, command: object) -> None:
        del command
        return None

    async def find_room_for_account(self, account_id: int) -> RoomState | None:
        self.durable_find_calls += 1
        return self.state if self.state.slot_for(account_id) is not None else None

    async def get_realtime_room_for_account(self, account_id: int) -> RoomState | None:
        return self.state if self.state.slot_for(account_id) is not None else None

    async def set_slot_status(self, public_id: int, account_id: int, status: SlotStatus) -> RoomState:
        assert public_id == self.state.room.public_id
        slots = tuple(
            replace(slot, status=status) if slot.account_id == account_id else slot for slot in self.state.slots
        )
        self.state = replace(self.state, state_revision=self.state.state_revision + 1, slots=slots)
        return self.state

    async def mark_skipped(self, public_id: int, account_id: int) -> RoomState:
        assert public_id == self.state.room.public_id
        slots = tuple(
            replace(slot, skipped=True) if slot.account_id == account_id else slot for slot in self.state.slots
        )
        self.state = replace(self.state, state_revision=self.state.state_revision + 1, slots=slots)
        return self.state

    async def mark_loaded(self, public_id: int, account_id: int) -> RoomState:
        assert public_id == self.state.room.public_id
        self.state = replace(
            self.state,
            state_revision=self.state.state_revision + 1,
            slots=tuple(
                replace(slot, loaded=True) if slot.account_id == account_id else slot for slot in self.state.slots
            ),
        )
        return self.state

    async def mark_failed(self, public_id: int, account_id: int) -> RoomState:
        assert public_id == self.state.room.public_id
        self.state = replace(
            self.state,
            state_revision=self.state.state_revision + 1,
            slots=tuple(
                replace(slot, failed=True) if slot.account_id == account_id else slot for slot in self.state.slots
            ),
        )
        return self.state

    async def start_round(self, command: object) -> MultiplayerMutationResult:
        self.started = command
        slots = tuple(
            replace(slot, status=SlotStatus.PLAYING) if slot.account_id is not None else slot
            for slot in self.state.slots
        )
        participants = tuple(slot.account_id for slot in slots if slot.account_id is not None)
        self.state = replace(
            self.state,
            slots=slots,
            in_progress=True,
            round_id=uuid.uuid7(),
            round_participant_account_ids=participants,
        )
        return MultiplayerMutationResult(
            MultiplayerMutationKind.ROUND_STARTED,
            self.state,
            round_participant_account_ids=participants,
        )

    async def update_settings(self, command: SettingsCommand) -> MultiplayerMutationResult:
        settings = command.settings
        self.state = replace(
            self.state,
            room=replace(self.state.room, version=self.state.room.version + 1, settings=settings),
        )
        return MultiplayerMutationResult(MultiplayerMutationKind.SETTINGS_UPDATED, self.state)

    async def complete_round(self, command: CompletionCommand) -> MultiplayerMutationResult:
        participants = self.state.round_participant_account_ids
        self.state = replace(
            self.state,
            slots=tuple(
                replace(slot, status=SlotStatus.NOT_READY) if slot.account_id in participants else slot
                for slot in self.state.slots
            ),
            in_progress=False,
            round_id=None,
            round_participant_account_ids=(),
        )
        kind = MultiplayerMutationKind.ROUND_ABORTED if command.aborted else MultiplayerMutationKind.ROUND_COMPLETED
        return MultiplayerMutationResult(kind, self.state, round_participant_account_ids=participants)

    async def issue_admission_token(
        self,
        public_id: int,
        *,
        inviter_account_id: int,
        recipient_account_id: int,
    ) -> str:
        self.admission = (public_id, inviter_account_id, recipient_account_id)
        return "token"


class InviteRealtime:
    def __init__(self) -> None:
        self.delivered: list[tuple[int, bytes]] = []
        self.target = presence_snapshot(11, "target")

    async def get_presence(self, account_id: int, *, at: datetime) -> PresenceSnapshot | None:
        del at
        return self.target if account_id == self.target.account_id else None


class RoomRealtime:
    def __init__(self, *account_ids: int) -> None:
        self.presences = {account_id: presence_snapshot(account_id, f"user-{account_id}") for account_id in account_ids}
        self.delivered: list[tuple[int, bytes]] = []

    async def get_presence(self, account_id: int, *, at: datetime) -> PresenceSnapshot | None:
        del at
        return self.presences.get(account_id)


class FakeBubbleBus:
    def __init__(self, realtime: object | None) -> None:
        self.realtime = realtime

    async def publish(self, recipient_fence: SessionFence, bubble: RealtimeBubble) -> int:
        if isinstance(self.realtime, RoomRealtime):
            account_id = next(
                account_id
                for account_id, presence in self.realtime.presences.items()
                if presence.fence == recipient_fence
            )
            self.realtime.delivered.append((account_id, StableBubbleRenderer().render(bubble)))
        elif isinstance(self.realtime, InviteRealtime) and self.realtime.target.fence == recipient_fence:
            self.realtime.delivered.append((self.realtime.target.account_id, StableBubbleRenderer().render(bubble)))
        return 1


class FakeCommunity:
    def __init__(self) -> None:
        self.public_message_calls = 0

    async def get_global_silence_remaining_seconds(self, account_id: int) -> int:
        assert account_id == 10
        return 0

    async def send_public_message(self, *args: object, **kwargs: object) -> None:
        del args, kwargs
        self.public_message_calls += 1
        raise AssertionError("virtual multiplayer chat must not use a persistent public channel")


class LobbyCommunity(FakeCommunity):
    async def get_public_channel(self, account_id: int, selector: object) -> object:
        del account_id, selector
        return type("Lobby", (), {"channel_id": 7})()


class LobbyRealtime(RoomRealtime):
    async def list_channel_members(self, channel_id: int) -> frozenset[int]:
        assert channel_id == 7
        return frozenset(self.presences)


class FakeSocial:
    async def filter_message_recipients(
        self,
        sender_account_id: int,
        recipient_account_ids: tuple[int, ...],
    ) -> tuple[int, ...]:
        assert sender_account_id == 10
        return recipient_account_ids


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
        ResolvedClientSession(10, "host", 1, session_id, None, "b20260711.1", None, EXPIRY),
        RealtimeSession(session_id, 10, 1, EXPIRY),
        UserPresence(10, "host", 0, 0, 1, 0, 0.0, 0.0, 0),
        UserStats(10, 0, "", "", 0, 0, 0, 0, 0.0, 0, 0, 0, 0),
    )


def services(
    multiplayer: FakeMultiplayer,
    *,
    community: FakeCommunity | None = None,
    social: FakeSocial | None = None,
    realtime: object | None = None,
    bot: BotCommandService | None = None,
) -> StableServices:
    return StableServices(
        identity=cast(IdentityService, object()),
        authorization=cast(AuthorizationQueryService, object()),
        realtime=cast(RealtimeStateRepository, realtime if realtime is not None else object()),
        clock=cast(Clock, FixedClock()),
        id_generator=cast(IdGenerator, FakeIds()),
        settings=Settings(),
        bubbles=cast(RealtimeBubbleBus, FakeBubbleBus(realtime)),
        multiplayer=cast(MultiplayerService, multiplayer),
        community=cast(CommunityService, community) if community is not None else None,
        social=cast(SocialService, social) if social is not None else None,
        bot=bot,
    )


def bot_service(multiplayer: FakeMultiplayer) -> BotCommandService:
    bot = BotCommandService()
    bot.register_group(
        build_multiplayer_commands(
            MultiplayerCommandDependencies(
                service=cast(MultiplayerService, multiplayer),
                resolve_account=cast(AccountResolver, object()),
                resolve_beatmap=cast(BeatmapResolver, object()),
            )
        )
    )
    return bot


def client_packet(packet_type: ClientPacket, write: object | None = None) -> bytes:
    writer = PacketWriter()
    with writer.packet(packet_type):
        if isinstance(write, MultiplayerMatch):
            writer.write_multiplayer_match(write)
        elif isinstance(write, ScoreFrame):
            writer.write_score_frame(write)
        elif isinstance(write, Message):
            writer.write_message(write)
    return writer.to_bytes()


@pytest.mark.asyncio
async def test_create_match_maps_wire_settings_and_returns_join_success(monkeypatch: pytest.MonkeyPatch) -> None:
    import importlib

    multiplayer_module = importlib.import_module("perfcho.api.stable.dispatcher.multiplayer")
    events: list[tuple[str, dict[str, object]]] = []

    def capture(level: str, event: str, **fields: object) -> None:
        del level
        events.append((event, fields))

    monkeypatch.setattr(multiplayer_module, "log_event", capture)
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

    packets = list(PacketReader(response, packet_enum=ServerPacket))
    assert [packet.packet_type for packet in packets] == [
        ServerPacket.CHANNEL_KICK,
        ServerPacket.CHANNEL_JOIN_SUCCESS,
        ServerPacket.MATCH_JOIN_SUCCESS,
    ]
    assert packets[0].payload.read_string() == "#lobby"
    assert packets[1].payload.read_string() == "#multiplayer"
    match = packets[2].payload.read_multiplayer_match()
    assert match.match_id == 7 and match.password == "secret"
    assert multiplayer.created is not None
    assert multiplayer.created.settings.external_beatmap_id == 42
    assert multiplayer.created.capacity == 16
    assert multiplayer.created.public_id_limit == 32767
    lifecycle = next(fields for event, fields in events if event == "stable.multiplayer.room_lifecycle")
    assert lifecycle == {
        "action": "created",
        "outcome": "success",
        "account_id": 10,
        "room_id": 7,
        "participant_count": 1,
        "delivery_failure_count": 0,
    }
    for secret in ("Room", "secret", "Artist - Title [Hard]", (b"m" * 16).hex()):
        assert secret not in repr(events)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("capacity", "public_id", "action"),
    [
        (1024, 7, MultiplayerRoomAction.CREATED),
        (16, 32768, MultiplayerRoomAction.DISPOSED),
    ],
)
async def test_canonical_rooms_outside_stable_bounds_are_not_published_to_lobby(
    capacity: int,
    public_id: int,
    action: MultiplayerRoomAction,
) -> None:
    base = room_state()
    state = replace(
        base,
        room=replace(base.room, capacity=capacity, public_id=public_id),
        slots=(RoomSlot(0, SlotStatus.NOT_READY, 10),)
        + tuple(RoomSlot(position, SlotStatus.OPEN) for position in range(1, capacity)),
    )
    realtime = LobbyRealtime(20)
    stable_services = services(FakeMultiplayer(state), community=LobbyCommunity(), realtime=realtime)

    failed = await _broadcast_lobby(
        MultiplayerRoomBubble(action, multiplayer_room_snapshot(state)),
        None,
        stable_services,
    )

    assert failed == frozenset()
    assert realtime.delivered == []


@pytest.mark.asyncio
async def test_join_match_opens_virtual_multiplayer_channel_before_join_success() -> None:
    multiplayer = FakeMultiplayer(room_state())
    writer = PacketWriter()
    with writer.packet(ClientPacket.JOIN_MATCH):
        writer.write_i32(multiplayer.state.room.public_id)
        writer.write_string("")

    response = await dispatch_packets(writer.to_bytes(), context(), services(multiplayer))

    packets = list(PacketReader(response, packet_enum=ServerPacket))
    assert [packet.packet_type for packet in packets] == [
        ServerPacket.CHANNEL_KICK,
        ServerPacket.CHANNEL_JOIN_SUCCESS,
        ServerPacket.MATCH_JOIN_SUCCESS,
    ]
    assert packets[0].payload.read_string() == "#lobby"
    assert packets[1].payload.read_string() == "#multiplayer"


@pytest.mark.asyncio
async def test_match_start_uses_canonical_state_and_returns_match_start() -> None:
    multiplayer = FakeMultiplayer(room_state())

    response = await dispatch_packets(
        client_packet(ClientPacket.MATCH_START),
        context(),
        services(multiplayer),
    )

    packet = next(PacketReader(response, packet_enum=ServerPacket))
    assert packet.packet_type is ServerPacket.MATCH_START
    assert packet.payload.read_multiplayer_match().in_progress
    assert multiplayer.started is not None


@pytest.mark.asyncio
async def test_mp_start_immediately_notifies_command_sender_and_other_players() -> None:
    multiplayer = FakeMultiplayer(room_state())
    multiplayer.state = replace(
        multiplayer.state,
        slots=(
            multiplayer.state.slots[0],
            RoomSlot(1, SlotStatus.NOT_READY, 20),
            *multiplayer.state.slots[2:],
        ),
    )
    realtime = RoomRealtime(20)

    response = await dispatch_packets(
        client_packet(ClientPacket.SEND_PUBLIC_MESSAGE, Message("", "!mp start", "#multiplayer", 0)),
        context(),
        services(multiplayer, social=FakeSocial(), realtime=realtime, bot=bot_service(multiplayer)),
    )

    assert [packet.packet_type for packet in PacketReader(response, packet_enum=ServerPacket)] == [
        ServerPacket.SEND_MESSAGE,
        ServerPacket.MATCH_START,
    ]
    delivered_types = [
        next(PacketReader(payload, packet_enum=ServerPacket)).packet_type for _, payload in realtime.delivered
    ]
    assert delivered_types == [ServerPacket.SEND_MESSAGE, ServerPacket.SEND_MESSAGE, ServerPacket.MATCH_START]


@pytest.mark.asyncio
async def test_mp_mods_immediately_broadcasts_updated_match() -> None:
    multiplayer = FakeMultiplayer(room_state())
    multiplayer.state = replace(
        multiplayer.state,
        slots=(
            multiplayer.state.slots[0],
            RoomSlot(1, SlotStatus.NOT_READY, 20),
            *multiplayer.state.slots[2:],
        ),
    )
    realtime = RoomRealtime(20)

    response = await dispatch_packets(
        client_packet(ClientPacket.SEND_PUBLIC_MESSAGE, Message("", "!mp mods HD", "#multiplayer", 0)),
        context(),
        services(multiplayer, social=FakeSocial(), realtime=realtime, bot=bot_service(multiplayer)),
    )

    packets = list(PacketReader(response, packet_enum=ServerPacket))
    assert [packet.packet_type for packet in packets] == [ServerPacket.SEND_MESSAGE, ServerPacket.UPDATE_MATCH]
    assert packets[1].payload.read_multiplayer_match().mods != 0
    delivered = next(PacketReader(realtime.delivered[-1][1], packet_enum=ServerPacket))
    assert delivered.packet_type is ServerPacket.UPDATE_MATCH


@pytest.mark.asyncio
async def test_mp_abort_immediately_notifies_active_round_players() -> None:
    multiplayer = FakeMultiplayer(room_state())
    multiplayer.state = replace(
        multiplayer.state,
        slots=(
            multiplayer.state.slots[0],
            RoomSlot(1, SlotStatus.NOT_READY, 20),
            *multiplayer.state.slots[2:],
        ),
    )
    realtime = RoomRealtime(20)
    stable_services = services(
        multiplayer,
        social=FakeSocial(),
        realtime=realtime,
        bot=bot_service(multiplayer),
    )
    await dispatch_packets(
        client_packet(ClientPacket.SEND_PUBLIC_MESSAGE, Message("", "!mp start", "#multiplayer", 0)),
        context(),
        stable_services,
    )
    realtime.delivered.clear()

    response = await dispatch_packets(
        client_packet(ClientPacket.SEND_PUBLIC_MESSAGE, Message("", "!mp abort", "#multiplayer", 0)),
        context(),
        stable_services,
    )

    assert [packet.packet_type for packet in PacketReader(response, packet_enum=ServerPacket)] == [
        ServerPacket.SEND_MESSAGE,
        ServerPacket.MATCH_ABORT,
        ServerPacket.UPDATE_MATCH,
    ]
    delivered_types = [
        next(PacketReader(payload, packet_enum=ServerPacket)).packet_type for _, payload in realtime.delivered
    ]
    assert delivered_types == [
        ServerPacket.SEND_MESSAGE,
        ServerPacket.SEND_MESSAGE,
        ServerPacket.MATCH_ABORT,
        ServerPacket.UPDATE_MATCH,
    ]


@pytest.mark.asyncio
async def test_match_invite_builds_message_and_delivers_admission_token() -> None:
    multiplayer = FakeMultiplayer(room_state())
    realtime = InviteRealtime()
    writer = PacketWriter()
    with writer.packet(ClientPacket.MATCH_INVITE):
        writer.write_i32(11)

    response = await dispatch_packets(writer.to_bytes(), context(), services(multiplayer, realtime=realtime))

    assert response == b""
    assert multiplayer.admission == (7, 10, 11)
    assert len(realtime.delivered) == 1
    account_id, payload = realtime.delivered[0]
    assert account_id == 11
    packet = next(PacketReader(payload, packet_enum=ServerPacket))
    assert packet.packet_type is ServerPacket.MATCH_INVITE
    message = packet.payload.read_message()
    assert message.sender == "host"
    assert message.recipient == "target"
    assert "osump://7/token Room" in message.text


@pytest.mark.asyncio
async def test_multiplayer_public_message_uses_room_members_instead_of_persistent_channel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import importlib

    dispatcher_module = importlib.import_module("perfcho.api.stable.dispatcher.packets")
    multiplayer = FakeMultiplayer(room_state())
    multiplayer.state = replace(
        multiplayer.state,
        slots=(
            multiplayer.state.slots[0],
            RoomSlot(1, SlotStatus.NOT_READY, 20),
            *multiplayer.state.slots[2:],
        ),
    )
    community = FakeCommunity()
    delivered: list[tuple[int, bytes]] = []

    async def capture(account_id: int, bubble: RealtimeBubble, stable_services: StableServices) -> bool:
        del stable_services
        delivered.append((account_id, StableBubbleRenderer().render(bubble)))
        return True

    monkeypatch.setattr(dispatcher_module, "_publish_online_recipient", capture)

    response = await dispatch_packets(
        client_packet(ClientPacket.SEND_PUBLIC_MESSAGE, Message("", "hello", "#multiplayer", 0)),
        context(),
        services(multiplayer, community=community, social=FakeSocial()),
    )

    assert response == b""
    assert community.public_message_calls == 0
    assert [account_id for account_id, _ in delivered] == [20]
    packet = next(PacketReader(delivered[0][1], packet_enum=ServerPacket))
    assert packet.packet_type is ServerPacket.SEND_MESSAGE
    assert packet.payload.read_message() == Message("host", "hello", "#multiplayer", 10)


@pytest.mark.asyncio
async def test_part_match_revokes_virtual_multiplayer_channel() -> None:
    multiplayer = FakeMultiplayer(room_state())

    response = await dispatch_packets(
        client_packet(ClientPacket.PART_MATCH),
        context(),
        services(multiplayer),
    )

    packet = next(PacketReader(response, packet_enum=ServerPacket))
    assert packet.packet_type is ServerPacket.CHANNEL_KICK
    assert packet.payload.read_string() == "#multiplayer"


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


@pytest.mark.asyncio
async def test_change_slot_rejects_positions_outside_stable_projection() -> None:
    multiplayer = FakeMultiplayer(room_state())
    writer = PacketWriter()
    with writer.packet(ClientPacket.MATCH_CHANGE_SLOT):
        writer.write_i32(16)

    response = await dispatch_packets(writer.to_bytes(), context(), services(multiplayer))

    packet = next(PacketReader(response, packet_enum=ServerPacket))
    assert packet.packet_type is ServerPacket.NOTIFICATION
    assert packet.payload.read_string() == "The multiplayer request is invalid."


def test_map_change_sentinel_is_preserved_in_canonical_settings() -> None:
    converted = _settings_from_wire(MultiplayerMatch(name="Room", beatmap_id=-1))
    assert converted.external_beatmap_id == -1


@pytest.mark.asyncio
async def test_score_frame_hot_path_echoes_immediately_and_broadcasts_to_other_round_players() -> None:
    multiplayer = FakeMultiplayer(room_state())
    round_id = uuid.uuid7()
    multiplayer.state = replace(
        multiplayer.state,
        in_progress=True,
        round_id=round_id,
        round_participant_account_ids=(10, 20),
        slots=(
            replace(multiplayer.state.slots[0], status=SlotStatus.PLAYING),
            RoomSlot(1, SlotStatus.PLAYING, 20),
            RoomSlot(2, SlotStatus.NOT_READY, 30),
            *multiplayer.state.slots[3:],
        ),
    )
    realtime = RoomRealtime(10, 20, 30)
    frame = ScoreFrame(100, 255, 1, 2, 3, 4, 5, 6, 1000, 10, 5, False, 200, 0, False)

    response = await dispatch_packets(
        client_packet(ClientPacket.MATCH_SCORE_UPDATE, frame),
        context(),
        services(multiplayer, realtime=realtime),
    )

    assert multiplayer.durable_find_calls == 0
    echoed = next(PacketReader(response, packet_enum=ServerPacket))
    assert echoed.packet_type is ServerPacket.MATCH_SCORE_UPDATE
    assert echoed.payload.read_score_frame() == replace(frame, frame_id=0)
    echoed.payload.require_exhausted()
    assert [account_id for account_id, _ in realtime.delivered] == [20]
    for _, payload in realtime.delivered:
        packet = next(PacketReader(payload, packet_enum=ServerPacket))
        assert packet.packet_type is ServerPacket.MATCH_SCORE_UPDATE
        forwarded = packet.payload.read_score_frame()
        packet.payload.require_exhausted()
        assert forwarded == replace(frame, frame_id=0)


@pytest.mark.asyncio
async def test_skip_update_uses_account_id_expected_by_stable_protocol() -> None:
    multiplayer = FakeMultiplayer(room_state())
    multiplayer.state = replace(
        multiplayer.state,
        in_progress=True,
        round_id=uuid.uuid7(),
        round_participant_account_ids=(10, 20),
        slots=(
            replace(multiplayer.state.slots[0], status=SlotStatus.PLAYING),
            RoomSlot(1, SlotStatus.PLAYING, 20),
            *multiplayer.state.slots[2:],
        ),
    )
    realtime = RoomRealtime(20)

    response = await dispatch_packets(
        client_packet(ClientPacket.MATCH_SKIP_REQUEST),
        context(),
        services(multiplayer, realtime=realtime),
    )

    packet = next(PacketReader(response, packet_enum=ServerPacket))
    assert packet.packet_type is ServerPacket.MATCH_PLAYER_SKIPPED
    assert packet.payload.read_i32() == 10
    delivered = next(PacketReader(realtime.delivered[0][1], packet_enum=ServerPacket))
    assert delivered.packet_type is ServerPacket.MATCH_PLAYER_SKIPPED
    assert delivered.payload.read_i32() == 10


@pytest.mark.asyncio
async def test_load_complete_and_failed_round_signals_fan_out() -> None:
    multiplayer = FakeMultiplayer(room_state())
    multiplayer.state = replace(
        multiplayer.state,
        in_progress=True,
        round_id=uuid.uuid7(),
        round_participant_account_ids=(10, 20),
        slots=(
            replace(multiplayer.state.slots[0], status=SlotStatus.PLAYING),
            RoomSlot(1, SlotStatus.PLAYING, 20, loaded=True),
            *multiplayer.state.slots[2:],
        ),
    )
    realtime = RoomRealtime(20)
    stable_services = services(multiplayer, realtime=realtime)

    loaded = await dispatch_packets(client_packet(ClientPacket.MATCH_LOAD_COMPLETE), context(), stable_services)
    failed = await dispatch_packets(client_packet(ClientPacket.MATCH_FAILED), context(), stable_services)

    assert [packet.packet_type for packet in PacketReader(loaded, packet_enum=ServerPacket)] == [
        ServerPacket.MATCH_ALL_PLAYERS_LOADED
    ]
    failed_packet = next(PacketReader(failed, packet_enum=ServerPacket))
    assert failed_packet.packet_type is ServerPacket.MATCH_PLAYER_FAILED
    assert failed_packet.payload.read_i32() == 0
    assert [next(PacketReader(payload, packet_enum=ServerPacket)).packet_type for _, payload in realtime.delivered] == [
        ServerPacket.MATCH_ALL_PLAYERS_LOADED,
        ServerPacket.MATCH_PLAYER_FAILED,
    ]


@pytest.mark.asyncio
async def test_round_completion_sends_complete_before_reset_state_to_other_players() -> None:
    multiplayer = FakeMultiplayer(room_state())
    multiplayer.state = replace(
        multiplayer.state,
        in_progress=True,
        round_id=uuid.uuid7(),
        round_participant_account_ids=(10, 20),
        slots=(
            replace(multiplayer.state.slots[0], status=SlotStatus.PLAYING),
            RoomSlot(1, SlotStatus.COMPLETE, 20),
            RoomSlot(2, SlotStatus.NOT_READY, 30),
            *multiplayer.state.slots[3:],
        ),
    )
    realtime = RoomRealtime(10, 20, 30)

    response = await dispatch_packets(
        client_packet(ClientPacket.MATCH_COMPLETE),
        context(),
        services(multiplayer, realtime=realtime),
    )

    assert [packet.packet_type for packet in PacketReader(response, packet_enum=ServerPacket)] == [
        ServerPacket.MATCH_COMPLETE,
        ServerPacket.UPDATE_MATCH,
    ]
    delivered = {
        account_id: [
            next(PacketReader(payload, packet_enum=ServerPacket)).packet_type
            for delivered_account_id, payload in realtime.delivered
            if delivered_account_id == account_id
        ]
        for account_id in (10, 20, 30)
    }
    assert delivered == {
        10: [],
        20: [ServerPacket.MATCH_COMPLETE, ServerPacket.UPDATE_MATCH],
        30: [ServerPacket.UPDATE_MATCH],
    }
