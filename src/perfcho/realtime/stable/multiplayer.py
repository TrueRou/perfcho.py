"""Adapt Stable lobby and match packets to the canonical multiplayer service."""

import hashlib
from dataclasses import replace
from typing import Protocol

from perfcho.composition import StableServices
from perfcho.modules.common import Actor, ClientContext, CommandMeta
from perfcho.modules.common.errors import ApplicationError
from perfcho.modules.identity import ResolvedStableSession
from perfcho.modules.multiplayer import (
    ChangeHost,
    ChangeRoomPassword,
    CompleteRound,
    CreateRoom,
    JoinRoom,
    KickParticipant,
    LeaveRoom,
    MultiplayerError,
    RoomSettings,
    RoomState,
    SlotStatus,
    StartRound,
    TeamMode,
    UpdateRoomSettings,
    WinCondition,
)
from perfcho.modules.realtime import MailboxOverflow, RealtimeSession
from perfcho.modules.scoring import CanonicalMod, Ruleset, ScoreboardVariant
from perfcho.modules.scoring.mods import normalize_mods, parse_legacy_mods
from perfcho.realtime.stable.builders import (
    dispose_match,
    match_all_players_loaded,
    match_complete,
    match_invite,
    match_join_fail,
    match_join_success,
    match_player_failed,
    match_player_skipped,
    match_score_update,
    match_skip,
    match_start,
    match_transfer_host,
    new_match,
    notification,
    update_match,
)
from perfcho.realtime.stable.codec import Packet, ProtocolError
from perfcho.realtime.stable.models import ClientPacket, Message, MultiplayerMatch

_RULESETS = (Ruleset.OSU, Ruleset.TAIKO, Ruleset.FRUITS, Ruleset.MANIA)
_TEAM_MODES = (TeamMode.HEAD_TO_HEAD, TeamMode.TAG_COOP, TeamMode.TEAM_VS, TeamMode.TAG_TEAM_VS)
_WIN_CONDITIONS = (WinCondition.SCORE, WinCondition.ACCURACY, WinCondition.COMBO, WinCondition.SCORE_V2)
_TEAM_MODE_IDS = {value: index for index, value in enumerate(_TEAM_MODES)}
_WIN_CONDITION_IDS = {value: index for index, value in enumerate(_WIN_CONDITIONS)}
_RULESET_IDS = {value: index for index, value in enumerate(_RULESETS)}
_SLOT_STATUS_TO_WIRE = {
    SlotStatus.OPEN: 1,
    SlotStatus.LOCKED: 2,
    SlotStatus.NOT_READY: 4,
    SlotStatus.READY: 8,
    SlotStatus.NO_BEATMAP: 16,
    SlotStatus.PLAYING: 32,
    SlotStatus.COMPLETE: 64,
}
_SPEED_MODS = frozenset({"DT", "NC", "HT"})

MULTIPLAYER_PACKETS = frozenset(
    {
        ClientPacket.PART_LOBBY,
        ClientPacket.JOIN_LOBBY,
        ClientPacket.CREATE_MATCH,
        ClientPacket.JOIN_MATCH,
        ClientPacket.PART_MATCH,
        ClientPacket.MATCH_CHANGE_SLOT,
        ClientPacket.MATCH_READY,
        ClientPacket.MATCH_LOCK,
        ClientPacket.MATCH_CHANGE_SETTINGS,
        ClientPacket.MATCH_START,
        ClientPacket.MATCH_SCORE_UPDATE,
        ClientPacket.MATCH_COMPLETE,
        ClientPacket.MATCH_CHANGE_MODS,
        ClientPacket.MATCH_LOAD_COMPLETE,
        ClientPacket.MATCH_NO_BEATMAP,
        ClientPacket.MATCH_NOT_READY,
        ClientPacket.MATCH_FAILED,
        ClientPacket.MATCH_HAS_BEATMAP,
        ClientPacket.MATCH_SKIP_REQUEST,
        ClientPacket.MATCH_TRANSFER_HOST,
        ClientPacket.MATCH_CHANGE_TEAM,
        ClientPacket.MATCH_CHANGE_PASSWORD,
        ClientPacket.MATCH_INVITE,
    }
)


class MultiplayerRuntimeContext(Protocol):
    """Describe the Stable session fields needed by the multiplayer adapter."""

    @property
    def identity(self) -> ResolvedStableSession:
        """Return the authenticated Stable identity."""
        ...

    @property
    def realtime(self) -> RealtimeSession:
        """Return the current fenced realtime session."""
        ...

    @property
    def client(self) -> ClientContext | None:
        """Return normalized request client evidence when available."""
        ...


async def dispatch_multiplayer_packet(
    packet: Packet,
    context: MultiplayerRuntimeContext,
    services: StableServices,
) -> bytes:
    """Dispatch one supported lobby or match packet and map controlled failures."""
    multiplayer = services.multiplayer
    packet_type = packet.packet_type
    if multiplayer is None or not isinstance(packet_type, ClientPacket):
        return match_join_fail()
    raw_payload = packet.payload_view.tobytes()
    try:
        if packet_type is ClientPacket.JOIN_LOBBY:
            packet.payload.require_exhausted()
            await _set_lobby_membership(context, services, joining=True)
            states = await multiplayer.list_public_rooms(limit=services.settings.stable_lobby_match_limit)
            return b"".join(new_match(_wire_match(state)) for state in states if _stable_visible(state))
        if packet_type is ClientPacket.PART_LOBBY:
            packet.payload.require_exhausted()
            await _set_lobby_membership(context, services, joining=False)
            return b""
        if packet_type is ClientPacket.CREATE_MATCH:
            incoming = packet.payload.read_multiplayer_match()
            packet.payload.require_exhausted()
            if incoming.host_id != context.identity.account_id:
                return match_join_fail()
            state = await multiplayer.create_room(
                CreateRoom(
                    _meta("create", packet_type, raw_payload, context, services),
                    _settings_from_wire(incoming),
                    incoming.password,
                    16,
                )
            )
            if not _stable_visible(state):
                await multiplayer.leave_room(
                    LeaveRoom(_meta("dispose", packet_type, raw_payload, context, services), state.room.public_id)
                )
                return match_join_fail()
            await _broadcast_lobby(new_match(_wire_match(state)), context.identity.account_id, services)
            return match_join_success(_wire_match(state, password=incoming.password))
        if packet_type is ClientPacket.JOIN_MATCH:
            public_id = packet.payload.read_i32()
            password = packet.payload.read_string()
            packet.payload.require_exhausted()
            state = await multiplayer.join_room(
                JoinRoom(_meta("join", packet_type, raw_payload, context, services), public_id, password)
            )
            await _broadcast_state(state, context.identity.account_id, services)
            return match_join_success(_wire_match(state, password=password))

        current = await multiplayer.find_room_for_account(context.identity.account_id)
        if current is None:
            packet.payload.read_remaining()
            return b""
        public_id = current.room.public_id
        if packet_type is ClientPacket.PART_MATCH:
            packet.payload.require_exhausted()
            state = await multiplayer.leave_room(
                LeaveRoom(_meta("part", packet_type, raw_payload, context, services), public_id)
            )
            if state is None:
                await _broadcast_lobby(dispose_match(public_id), context.identity.account_id, services)
            else:
                await _broadcast_state(state, context.identity.account_id, services)
            return b""
        if packet_type is ClientPacket.MATCH_CHANGE_SLOT:
            target = packet.payload.read_i32()
            packet.payload.require_exhausted()
            state = await multiplayer.move_slot(public_id, context.identity.account_id, target)
            return await _state_response(state, context.identity.account_id, services)
        if packet_type is ClientPacket.MATCH_READY:
            packet.payload.require_exhausted()
            state = await multiplayer.set_slot_status(public_id, context.identity.account_id, SlotStatus.READY)
            return await _state_response(state, context.identity.account_id, services, lobby=False)
        if packet_type is ClientPacket.MATCH_LOCK:
            target = packet.payload.read_i32()
            packet.payload.require_exhausted()
            if not 0 <= target < len(current.slots):
                return b""
            target_account_id = current.slots[target].account_id
            if target_account_id is not None:
                state = await multiplayer.kick_participant(
                    KickParticipant(
                        _meta("kick", packet_type, raw_payload, context, services),
                        public_id,
                        current.room.version,
                        target_account_id,
                    )
                )
                await _enqueue(target_account_id, match_join_fail(), current, services)
                state = await multiplayer.lock_slot(public_id, context.identity.account_id, target)
            else:
                state = await multiplayer.lock_slot(public_id, context.identity.account_id, target)
            return await _state_response(state, context.identity.account_id, services)
        if packet_type is ClientPacket.MATCH_CHANGE_SETTINGS:
            incoming = packet.payload.read_multiplayer_match()
            packet.payload.require_exhausted()
            if incoming.host_id != context.identity.account_id:
                return b""
            state = await multiplayer.update_settings(
                UpdateRoomSettings(
                    _meta("settings", packet_type, raw_payload, context, services),
                    public_id,
                    current.room.version,
                    _settings_from_wire(incoming),
                )
            )
            return await _state_response(state, context.identity.account_id, services)
        if packet_type is ClientPacket.MATCH_CHANGE_PASSWORD:
            incoming = packet.payload.read_multiplayer_match()
            packet.payload.require_exhausted()
            if incoming.host_id != context.identity.account_id:
                return b""
            state = await multiplayer.change_password(
                ChangeRoomPassword(
                    _meta("password", packet_type, raw_payload, context, services),
                    public_id,
                    current.room.version,
                    incoming.password,
                )
            )
            return await _state_response(state, context.identity.account_id, services)
        if packet_type is ClientPacket.MATCH_START:
            packet.payload.require_exhausted()
            state = await multiplayer.start_round(
                StartRound(
                    _meta("start", packet_type, raw_payload, context, services),
                    public_id,
                    current.room.version,
                )
            )
            wire = match_start(_wire_match(state))
            await _broadcast_playing(wire, state, context.identity.account_id, services)
            await _broadcast_lobby(update_match(_wire_match(state), send_password=False), None, services)
            return wire
        if packet_type is ClientPacket.MATCH_SCORE_UPDATE:
            frame = packet.payload.read_score_frame()
            packet.payload.require_exhausted()
            slot = current.slot_for(context.identity.account_id)
            if slot is None or not current.in_progress:
                return b""
            wire = match_score_update(replace(frame, frame_id=slot.position))
            await _broadcast_match(wire, current, context.identity.account_id, services)
            return b""
        if packet_type is ClientPacket.MATCH_COMPLETE:
            packet.payload.require_exhausted()
            state = await multiplayer.set_slot_status(public_id, context.identity.account_id, SlotStatus.COMPLETE)
            if any(slot.status is SlotStatus.PLAYING for slot in state.slots):
                return b""
            completed = await multiplayer.complete_round(
                CompleteRound(
                    _meta("complete", packet_type, raw_payload, context, services),
                    public_id,
                    state.room.version,
                )
            )
            wire = match_complete()
            await _broadcast_match(wire, completed, context.identity.account_id, services)
            await _broadcast_state(completed, None, services)
            return wire
        if packet_type is ClientPacket.MATCH_CHANGE_MODS:
            legacy_bits = packet.payload.read_i32()
            packet.payload.require_exhausted()
            mods, variant = parse_legacy_mods(legacy_bits)
            if current.room.settings.free_mods:
                slot_mods = tuple(mod for mod in mods if mod.acronym not in _SPEED_MODS)
                state = current
                if current.room.host_account_id == context.identity.account_id:
                    room_mods = tuple(mod for mod in mods if mod.acronym in _SPEED_MODS)
                    state = await multiplayer.update_settings(
                        UpdateRoomSettings(
                            _meta("speed-mods", packet_type, raw_payload, context, services),
                            public_id,
                            current.room.version,
                            replace(current.room.settings, mods=room_mods, variant=variant),
                        )
                    )
                state = await multiplayer.set_slot_mods(public_id, context.identity.account_id, slot_mods)
            elif current.room.host_account_id == context.identity.account_id:
                state = await multiplayer.update_settings(
                    UpdateRoomSettings(
                        _meta("mods", packet_type, raw_payload, context, services),
                        public_id,
                        current.room.version,
                        replace(current.room.settings, mods=mods, variant=variant),
                    )
                )
            else:
                return b""
            return await _state_response(state, context.identity.account_id, services)
        if packet_type is ClientPacket.MATCH_LOAD_COMPLETE:
            packet.payload.require_exhausted()
            state = await multiplayer.mark_loaded(public_id, context.identity.account_id)
            if all(slot.loaded for slot in state.slots if slot.status is SlotStatus.PLAYING):
                wire = match_all_players_loaded()
                await _broadcast_match(wire, state, context.identity.account_id, services)
                return wire
            return b""
        if packet_type is ClientPacket.MATCH_NO_BEATMAP:
            packet.payload.require_exhausted()
            state = await multiplayer.set_slot_status(public_id, context.identity.account_id, SlotStatus.NO_BEATMAP)
            return await _state_response(state, context.identity.account_id, services, lobby=False)
        if packet_type in {ClientPacket.MATCH_NOT_READY, ClientPacket.MATCH_HAS_BEATMAP}:
            packet.payload.require_exhausted()
            state = await multiplayer.set_slot_status(public_id, context.identity.account_id, SlotStatus.NOT_READY)
            return await _state_response(state, context.identity.account_id, services, lobby=False)
        if packet_type is ClientPacket.MATCH_FAILED:
            packet.payload.require_exhausted()
            state = await multiplayer.mark_failed(public_id, context.identity.account_id)
            slot = state.slot_for(context.identity.account_id)
            if slot is None:
                return b""
            wire = match_player_failed(slot.position)
            await _broadcast_match(wire, state, context.identity.account_id, services)
            return wire
        if packet_type is ClientPacket.MATCH_SKIP_REQUEST:
            packet.payload.require_exhausted()
            state = await multiplayer.mark_skipped(public_id, context.identity.account_id)
            player_wire = match_player_skipped(context.identity.account_id)
            await _broadcast_match(player_wire, state, context.identity.account_id, services)
            if all(slot.skipped for slot in state.slots if slot.status is SlotStatus.PLAYING):
                skip_wire = match_skip()
                await _broadcast_match(skip_wire, state, context.identity.account_id, services)
                return player_wire + skip_wire
            return player_wire
        if packet_type is ClientPacket.MATCH_TRANSFER_HOST:
            position = packet.payload.read_i32()
            packet.payload.require_exhausted()
            if not 0 <= position < len(current.slots):
                return b""
            target = current.slots[position].account_id
            if target is None:
                return b""
            state = await multiplayer.change_host(
                ChangeHost(
                    _meta("host", packet_type, raw_payload, context, services),
                    public_id,
                    current.room.version,
                    target,
                )
            )
            await _enqueue(target, match_transfer_host(), state, services)
            return await _state_response(state, context.identity.account_id, services)
        if packet_type is ClientPacket.MATCH_CHANGE_TEAM:
            packet.payload.require_exhausted()
            slot = current.slot_for(context.identity.account_id)
            if slot is None:
                return b""
            state = await multiplayer.set_slot_team(
                public_id,
                context.identity.account_id,
                1 if slot.team == 2 else 2,
            )
            return await _state_response(state, context.identity.account_id, services, lobby=False)
        if packet_type is ClientPacket.MATCH_INVITE:
            target_account_id = packet.payload.read_i32()
            packet.payload.require_exhausted()
            if target_account_id < 1 or target_account_id == context.identity.account_id:
                return b""
            target_presence = await services.realtime.get_presence(target_account_id, at=services.clock.now())
            if target_presence is None:
                return notification("The invited player is offline.")
            target_name = _presence_name(target_presence.payload, target_account_id)
            wire = match_invite(
                Message(
                    context.identity.current_name,
                    f"Come join my game: [osump://{public_id}/ {current.room.settings.name}].",
                    target_name,
                    context.identity.account_id,
                )
            )
            await _enqueue(target_account_id, wire, current, services)
            return b""
    except MultiplayerError as error:
        if packet_type in {ClientPacket.CREATE_MATCH, ClientPacket.JOIN_MATCH}:
            return match_join_fail() + notification(_failure_message(error))
        return notification(_failure_message(error))
    except ProtocolError:
        raise
    except ValueError:
        if packet_type in {ClientPacket.CREATE_MATCH, ClientPacket.JOIN_MATCH}:
            return match_join_fail() + notification("The multiplayer request is invalid.")
        return notification("The multiplayer request is invalid.")
    return b""


def _settings_from_wire(match: MultiplayerMatch) -> RoomSettings:
    if not 0 <= match.mode < len(_RULESETS):
        raise ValueError("Stable match ruleset is invalid")
    if not 0 <= match.team_type < len(_TEAM_MODES):
        raise ValueError("Stable match team mode is invalid")
    if not 0 <= match.win_condition < len(_WIN_CONDITIONS):
        raise ValueError("Stable match win condition is invalid")
    mods, variant = parse_legacy_mods(match.mods)
    checksum = bytes.fromhex(match.beatmap_md5) if match.beatmap_md5 else None
    settings = RoomSettings(
        name=match.name,
        beatmap_name=match.beatmap_name,
        external_beatmap_id=max(0, match.beatmap_id),
        beatmap_md5=checksum,
        ruleset=_RULESETS[match.mode],
        variant=variant,
        team_mode=_TEAM_MODES[match.team_type],
        win_condition=_WIN_CONDITIONS[match.win_condition],
        mods=mods,
        free_mods=match.freemods,
        seed=match.seed,
    )
    normalize_mods(settings.ruleset, settings.variant, settings.mods)
    return settings


def _wire_match(state: RoomState, *, password: str | None = None) -> MultiplayerMatch:
    settings = state.room.settings
    legacy_bits = normalize_mods(settings.ruleset, settings.variant, settings.mods).legacy_bits
    statuses = tuple(_SLOT_STATUS_TO_WIRE[slot.status] for slot in state.slots)
    teams = tuple(slot.team for slot in state.slots)
    users = tuple(slot.account_id for slot in state.slots)
    slot_mods = (
        tuple(_slot_legacy_bits(settings.ruleset, slot.mods) for slot in state.slots) if settings.free_mods else ()
    )
    return MultiplayerMatch(
        match_id=state.room.public_id,
        in_progress=state.in_progress,
        mods=legacy_bits,
        name=settings.name,
        password=password if password is not None else "*" if state.room.password_protected else "",
        beatmap_name=settings.beatmap_name,
        beatmap_id=settings.external_beatmap_id,
        beatmap_md5=settings.beatmap_md5.hex() if settings.beatmap_md5 is not None else "",
        slot_statuses=statuses,
        slot_teams=teams,
        slot_user_ids=users,
        host_id=state.room.host_account_id,
        mode=_RULESET_IDS[settings.ruleset],
        win_condition=_WIN_CONDITION_IDS[settings.win_condition],
        team_type=_TEAM_MODE_IDS[settings.team_mode],
        freemods=settings.free_mods,
        slot_mods=slot_mods,
        seed=settings.seed,
    )


async def _state_response(
    state: RoomState,
    caller_account_id: int,
    services: StableServices,
    *,
    lobby: bool = True,
) -> bytes:
    wire = update_match(_wire_match(state), send_password=False)
    await _broadcast_match(wire, state, caller_account_id, services)
    if lobby:
        await _broadcast_lobby(wire, caller_account_id, services)
    return wire


async def _broadcast_state(state: RoomState, caller_account_id: int | None, services: StableServices) -> None:
    wire = update_match(_wire_match(state), send_password=False)
    await _broadcast_match(wire, state, caller_account_id, services)
    await _broadcast_lobby(wire, caller_account_id, services)


async def _broadcast_match(
    payload: bytes,
    state: RoomState,
    excluded_account_id: int | None,
    services: StableServices,
) -> None:
    for account_id in {slot.account_id for slot in state.slots if slot.account_id is not None}:
        if account_id != excluded_account_id:
            await _enqueue(account_id, payload, state, services)


async def _broadcast_playing(
    payload: bytes,
    state: RoomState,
    excluded_account_id: int | None,
    services: StableServices,
) -> None:
    for slot in state.slots:
        if slot.status is SlotStatus.PLAYING and slot.account_id is not None and slot.account_id != excluded_account_id:
            await _enqueue(slot.account_id, payload, state, services)


async def _broadcast_lobby(
    payload: bytes,
    excluded_account_id: int | None,
    services: StableServices,
) -> None:
    if services.community is None:
        return
    lookup_account_id = excluded_account_id or 1
    try:
        lobby = await services.community.get_public_channel_by_stable_name(lookup_account_id, "#lobby")
    except ApplicationError:
        return
    for account_id in await services.realtime.list_channel_members(lobby.channel_id):
        if account_id == excluded_account_id:
            continue
        presence = await services.realtime.get_presence(account_id, at=services.clock.now())
        if presence is None:
            continue
        try:
            await services.realtime.enqueue_mailbox(account_id, payload, expires_at=presence.expires_at)
        except MailboxOverflow:
            continue


async def _enqueue(account_id: int, payload: bytes, state: RoomState, services: StableServices) -> None:
    presence = await services.realtime.get_presence(account_id, at=services.clock.now())
    if presence is None:
        return
    try:
        await services.realtime.enqueue_mailbox(account_id, payload, expires_at=presence.expires_at)
    except MailboxOverflow:
        return


async def _set_lobby_membership(
    context: MultiplayerRuntimeContext,
    services: StableServices,
    *,
    joining: bool,
) -> None:
    if services.community is None:
        return
    lobby = await services.community.get_public_channel_by_stable_name(context.identity.account_id, "#lobby")
    if joining:
        await services.realtime.join_channel(
            lobby.channel_id,
            session_id=context.identity.session_id,
            expected_revision=context.realtime.revision,
        )
    else:
        await services.realtime.leave_channel(
            lobby.channel_id,
            session_id=context.identity.session_id,
            expected_revision=context.realtime.revision,
        )


def _meta(
    operation: str,
    packet_type: ClientPacket,
    payload: bytes,
    context: MultiplayerRuntimeContext,
    services: StableServices,
) -> CommandMeta:
    digest = hashlib.sha256(packet_type.to_bytes(2, "little") + payload).digest()
    client = context.client or ClientContext(
        family="stable",
        version=context.identity.client_version,
        variant=context.identity.client_variant,
        ip_address="127.0.0.1",
        user_agent="osu!",
    )
    request_id = services.id_generator.new()
    return CommandMeta(
        request_id=request_id,
        idempotency_key=f"stable-multiplayer:{operation}:{context.identity.session_id}:{digest.hex()}",
        request_digest=digest,
        actor=Actor(context.identity.account_id, context.identity.session_id),
        client=client,
        received_at=services.clock.now(),
    )


def _stable_visible(state: RoomState) -> bool:
    return state.room.capacity == 16 and 0 < state.room.public_id <= 32767


def _slot_legacy_bits(ruleset: Ruleset, mods: tuple[CanonicalMod, ...]) -> int:
    acronyms = {mod.acronym for mod in mods}
    variant = (
        ScoreboardVariant.AUTOPILOT
        if "AP" in acronyms
        else ScoreboardVariant.RELAX
        if "RX" in acronyms
        else ScoreboardVariant.VANILLA
    )
    return normalize_mods(ruleset, variant, mods).legacy_bits


def _presence_name(payload: bytes, account_id: int) -> str:
    from perfcho.realtime.stable.codec import PacketReader
    from perfcho.realtime.stable.models import ServerPacket

    for packet in PacketReader(payload, packet_enum=ServerPacket):
        if packet.packet_type is ServerPacket.USER_PRESENCE:
            presence = packet.payload.read_user_presence()
            if presence.user_id == account_id:
                return presence.username
    raise ValueError("invited player has no valid presence")


def _failure_message(error: MultiplayerError) -> str:
    messages = {
        "match_password_rejected": "The multiplayer password is incorrect.",
        "match_full": "The multiplayer room is full.",
        "match_already_joined": "You are already in a multiplayer room.",
        "match_permission_denied": "You cannot change this multiplayer room.",
        "match_not_found": "The multiplayer room is no longer available.",
        "match_concurrency_conflict": "The multiplayer room changed; please retry.",
        "match_state_rejected": "That multiplayer action is not valid now.",
    }
    return messages.get(error.code, "The multiplayer action could not be completed.")
