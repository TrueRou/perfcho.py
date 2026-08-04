"""Adapt Stable lobby and match packets to the canonical multiplayer service."""

import hashlib
from dataclasses import dataclass, replace

from perfcho.api.stable.dispatcher.models import MultiplayerRuntimeContext
from perfcho.infra.glue.stable import StableServices
from perfcho.infra.logging import log_event, rate_limit, sampled
from perfcho.modules.common import Actor, ClientContext, CommandMeta
from perfcho.modules.common.errors import ApplicationError
from perfcho.modules.multiplayer import (
    ChangeHost,
    ChangeRoomPassword,
    CompleteRound,
    CreateRoom,
    JoinRoom,
    KickParticipant,
    LeaveRoom,
    MultiplayerError,
    MultiplayerService,
    RoomSettings,
    RoomState,
    SlotStatus,
    StartRound,
    TeamMode,
    UpdateRoomSettings,
    WinCondition,
)
from perfcho.modules.realtime import MailboxOverflow
from perfcho.modules.realtime.stable.builders import (
    channel_join,
    channel_kick,
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
from perfcho.modules.realtime.stable.codec import Packet, ProtocolError
from perfcho.modules.realtime.stable.models import ClientPacket, Message, MultiplayerMatch
from perfcho.modules.scoring import CanonicalMod, Ruleset, ScoreboardVariant
from perfcho.modules.scoring.mods import normalize_mods, parse_legacy_mods

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
    dispatch = _MultiplayerPacketContext(
        packet=packet,
        packet_type=packet_type,
        raw_payload=packet.payload_view.tobytes(),
        context=context,
        services=services,
        multiplayer=multiplayer,
    )
    try:
        return await _dispatch_multiplayer_packet(dispatch)
    except MultiplayerError as error:
        log_event(
            "INFO",
            "stable.multiplayer.rejected",
            exception=error,
            operation=packet_type.name,
            outcome="rejected",
            account_id=context.identity.account_id,
            error_code=error.code,
            error_type=type(error).__name__,
        )
        if packet_type in {ClientPacket.CREATE_MATCH, ClientPacket.JOIN_MATCH}:
            return match_join_fail() + notification(_failure_message(error))
        return notification(_failure_message(error))
    except ProtocolError:
        raise
    except ValueError as error:
        log_event(
            "INFO",
            "stable.multiplayer.rejected",
            exception=error,
            operation=packet_type.name,
            outcome="invalid_input",
            account_id=context.identity.account_id,
            error_code="stable_multiplayer_input_rejected",
            error_type=type(error).__name__,
        )
        if packet_type in {ClientPacket.CREATE_MATCH, ClientPacket.JOIN_MATCH}:
            return match_join_fail() + notification("The multiplayer request is invalid.")
        return notification("The multiplayer request is invalid.")
    return b""


@dataclass(frozen=True, slots=True)
class _MultiplayerPacketContext:
    packet: Packet
    packet_type: ClientPacket
    raw_payload: bytes
    context: MultiplayerRuntimeContext
    services: StableServices
    multiplayer: MultiplayerService


async def _dispatch_multiplayer_packet(dispatch: _MultiplayerPacketContext) -> bytes:
    if dispatch.packet_type in {
        ClientPacket.JOIN_LOBBY,
        ClientPacket.PART_LOBBY,
        ClientPacket.CREATE_MATCH,
        ClientPacket.JOIN_MATCH,
    }:
        return await _dispatch_lobby_packet(dispatch)
    if dispatch.packet_type is ClientPacket.MATCH_SCORE_UPDATE:
        return await _dispatch_score_update(dispatch)
    current = await dispatch.multiplayer.find_room_for_account(dispatch.context.identity.account_id)
    if current is None:
        dispatch.packet.payload.read_remaining()
        return b""
    return await _dispatch_room_packet(dispatch, current)


async def _dispatch_lobby_packet(dispatch: _MultiplayerPacketContext) -> bytes:
    packet = dispatch.packet
    context = dispatch.context
    services = dispatch.services
    if dispatch.packet_type is ClientPacket.JOIN_LOBBY:
        packet.payload.require_exhausted()
        await _set_lobby_membership(context, services, joining=True)
        states = await dispatch.multiplayer.list_public_rooms(limit=services.settings.stable_lobby_match_limit)
        return b"".join(new_match(_wire_match(state)) for state in states if _stable_visible(state))
    if dispatch.packet_type is ClientPacket.PART_LOBBY:
        packet.payload.require_exhausted()
        await _set_lobby_membership(context, services, joining=False)
        return b""
    if dispatch.packet_type is ClientPacket.CREATE_MATCH:
        return await _create_match(dispatch)
    return await _join_match(dispatch)


async def _create_match(dispatch: _MultiplayerPacketContext) -> bytes:
    packet = dispatch.packet
    context = dispatch.context
    services = dispatch.services
    incoming = packet.payload.read_multiplayer_match()
    packet.payload.require_exhausted()
    if incoming.host_id != context.identity.account_id:
        return match_join_fail()
    state = await dispatch.multiplayer.create_room(
        CreateRoom(
            _meta("create", dispatch.packet_type, dispatch.raw_payload, context, services),
            _settings_from_wire(incoming),
            incoming.password,
            16,
        )
    )
    if not _stable_visible(state):
        await dispatch.multiplayer.leave_room(
            LeaveRoom(
                _meta("dispose", dispatch.packet_type, dispatch.raw_payload, context, services), state.room.public_id
            )
        )
        log_event(
            "INFO",
            "stable.multiplayer.room_lifecycle",
            action="create",
            outcome="not_stable_visible",
            account_id=context.identity.account_id,
            room_id=state.room.public_id,
            participant_count=_participant_count(state),
        )
        return match_join_fail()
    await _leave_lobby_after_join(context, state, services)
    failed = await _broadcast_lobby(new_match(_wire_match(state)), context.identity.account_id, services)
    log_event(
        "INFO",
        "stable.multiplayer.room_lifecycle",
        action="created",
        outcome="success",
        account_id=context.identity.account_id,
        room_id=state.room.public_id,
        participant_count=_participant_count(state),
        delivery_failure_count=len(failed),
    )
    return (
        channel_kick("#lobby")
        + channel_join("#multiplayer")
        + match_join_success(_wire_match(state, password=incoming.password))
    )


async def _join_match(dispatch: _MultiplayerPacketContext) -> bytes:
    packet = dispatch.packet
    context = dispatch.context
    services = dispatch.services
    public_id = packet.payload.read_i32()
    password = packet.payload.read_string()
    packet.payload.require_exhausted()
    state = await dispatch.multiplayer.join_room(
        JoinRoom(_meta("join", dispatch.packet_type, dispatch.raw_payload, context, services), public_id, password)
    )
    await _leave_lobby_after_join(context, state, services)
    failed = await _broadcast_state(state, context.identity.account_id, services)
    log_event(
        "INFO",
        "stable.multiplayer.room_lifecycle",
        action="joined",
        outcome="success",
        account_id=context.identity.account_id,
        room_id=state.room.public_id,
        participant_count=_participant_count(state),
        delivery_failure_count=len(failed),
    )
    return (
        channel_kick("#lobby")
        + channel_join("#multiplayer")
        + match_join_success(_wire_match(state, password=password))
        + _delivery_warning(failed)
    )


async def _dispatch_score_update(dispatch: _MultiplayerPacketContext) -> bytes:
    frame = dispatch.packet.payload.read_score_frame()
    dispatch.packet.payload.require_exhausted()
    account_id = dispatch.context.identity.account_id
    cached = await dispatch.multiplayer.get_realtime_room_for_account(account_id)
    if cached is None or not cached.in_progress:
        return b""
    slot = cached.slot_for(account_id)
    if slot is None or account_id not in cached.round_participant_account_ids:
        return b""
    wire = match_score_update(replace(frame, frame_id=slot.position))
    failed = await _broadcast_match(wire, cached, account_id, dispatch.services)
    _log_ephemeral_state(
        dispatch.packet_type, cached, dispatch.context, dispatch.services, delivery_failure_count=len(failed)
    )
    return b""


async def _dispatch_room_packet(dispatch: _MultiplayerPacketContext, current: RoomState) -> bytes:
    if dispatch.packet_type is ClientPacket.PART_MATCH:
        return await _part_match(dispatch, current)
    if dispatch.packet_type in {
        ClientPacket.MATCH_CHANGE_SLOT,
        ClientPacket.MATCH_READY,
        ClientPacket.MATCH_LOCK,
        ClientPacket.MATCH_CHANGE_SETTINGS,
        ClientPacket.MATCH_CHANGE_PASSWORD,
        ClientPacket.MATCH_CHANGE_MODS,
        ClientPacket.MATCH_CHANGE_TEAM,
    }:
        return await _dispatch_room_settings(dispatch, current)
    if dispatch.packet_type in {
        ClientPacket.MATCH_START,
        ClientPacket.MATCH_COMPLETE,
        ClientPacket.MATCH_LOAD_COMPLETE,
        ClientPacket.MATCH_NO_BEATMAP,
        ClientPacket.MATCH_NOT_READY,
        ClientPacket.MATCH_HAS_BEATMAP,
        ClientPacket.MATCH_FAILED,
        ClientPacket.MATCH_SKIP_REQUEST,
    }:
        return await _dispatch_room_round(dispatch, current)
    if dispatch.packet_type in {ClientPacket.MATCH_TRANSFER_HOST, ClientPacket.MATCH_INVITE}:
        return await _dispatch_room_host(dispatch, current)
    dispatch.packet.payload.read_remaining()
    return b""


async def _part_match(dispatch: _MultiplayerPacketContext, current: RoomState) -> bytes:
    dispatch.packet.payload.require_exhausted()
    account_id = dispatch.context.identity.account_id
    public_id = current.room.public_id
    state = await dispatch.multiplayer.leave_room(
        LeaveRoom(
            _meta(
                "part",
                dispatch.packet_type,
                dispatch.raw_payload,
                dispatch.context,
                dispatch.services,
                epoch=f"{current.room.session_id}:{current.room.version}",
            ),
            public_id,
        )
    )
    if state is None:
        failed = await _broadcast_lobby(dispose_match(public_id), account_id, dispatch.services)
        action = "closed"
        participant_count = 0
    else:
        failed = set(await _broadcast_state(state, account_id, dispatch.services))
        action = "left"
        participant_count = _participant_count(state)
        if current.room.host_account_id == account_id and not await _enqueue(
            state.room.host_account_id,
            match_transfer_host(),
            state,
            dispatch.services,
        ):
            failed.add(state.room.host_account_id)
    log_event(
        "INFO",
        "stable.multiplayer.room_lifecycle",
        action=action,
        outcome="success",
        account_id=account_id,
        room_id=public_id,
        participant_count=participant_count,
        delivery_failure_count=len(failed),
    )
    return channel_kick("#multiplayer") + _delivery_warning(failed)


async def _dispatch_room_settings(dispatch: _MultiplayerPacketContext, current: RoomState) -> bytes:
    packet = dispatch.packet
    account_id = dispatch.context.identity.account_id
    public_id = current.room.public_id
    if dispatch.packet_type is ClientPacket.MATCH_CHANGE_SLOT:
        target = packet.payload.read_i32()
        packet.payload.require_exhausted()
        state = await dispatch.multiplayer.move_slot(public_id, account_id, target)
        _log_ephemeral_state(dispatch.packet_type, state, dispatch.context, dispatch.services)
        return await _state_response(state, account_id, dispatch.services)
    if dispatch.packet_type is ClientPacket.MATCH_READY:
        packet.payload.require_exhausted()
        state = await dispatch.multiplayer.set_slot_status(public_id, account_id, SlotStatus.READY)
        _log_ephemeral_state(dispatch.packet_type, state, dispatch.context, dispatch.services)
        return await _state_response(state, account_id, dispatch.services, lobby=False)
    if dispatch.packet_type is ClientPacket.MATCH_LOCK:
        return await _lock_slot(dispatch, current)
    if dispatch.packet_type is ClientPacket.MATCH_CHANGE_SETTINGS:
        return await _change_settings(dispatch, current)
    if dispatch.packet_type is ClientPacket.MATCH_CHANGE_PASSWORD:
        return await _change_password(dispatch, current)
    if dispatch.packet_type is ClientPacket.MATCH_CHANGE_MODS:
        return await _change_mods(dispatch, current)
    packet.payload.require_exhausted()
    slot = current.slot_for(account_id)
    if slot is None:
        return b""
    state = await dispatch.multiplayer.set_slot_team(public_id, account_id, 1 if slot.team == 2 else 2)
    _log_ephemeral_state(dispatch.packet_type, state, dispatch.context, dispatch.services)
    return await _state_response(state, account_id, dispatch.services, lobby=False)


async def _lock_slot(dispatch: _MultiplayerPacketContext, current: RoomState) -> bytes:
    target = dispatch.packet.payload.read_i32()
    dispatch.packet.payload.require_exhausted()
    if not 0 <= target < len(current.slots):
        return b""
    account_id = dispatch.context.identity.account_id
    target_account_id = current.slots[target].account_id
    if target_account_id is not None:
        await dispatch.multiplayer.kick_participant(
            KickParticipant(
                _meta(
                    "kick",
                    dispatch.packet_type,
                    dispatch.raw_payload,
                    dispatch.context,
                    dispatch.services,
                    epoch=f"{current.room.session_id}:{current.room.version}",
                ),
                current.room.public_id,
                current.room.version,
                target_account_id,
            )
        )
        await _enqueue(
            target_account_id,
            channel_kick("#multiplayer") + match_join_fail(),
            current,
            dispatch.services,
        )
    state = await dispatch.multiplayer.lock_slot(current.room.public_id, account_id, target)
    _log_ephemeral_state(dispatch.packet_type, state, dispatch.context, dispatch.services)
    return await _state_response(state, account_id, dispatch.services)


async def _change_settings(dispatch: _MultiplayerPacketContext, current: RoomState) -> bytes:
    incoming = dispatch.packet.payload.read_multiplayer_match()
    dispatch.packet.payload.require_exhausted()
    account_id = dispatch.context.identity.account_id
    if incoming.host_id != account_id:
        return b""
    state = await dispatch.multiplayer.update_settings(
        UpdateRoomSettings(
            _meta(
                "settings",
                dispatch.packet_type,
                dispatch.raw_payload,
                dispatch.context,
                dispatch.services,
                epoch=f"{current.room.session_id}:{current.room.version}",
            ),
            current.room.public_id,
            current.room.version,
            _settings_from_wire(incoming),
        )
    )
    return await _state_response(state, account_id, dispatch.services)


async def _change_password(dispatch: _MultiplayerPacketContext, current: RoomState) -> bytes:
    incoming = dispatch.packet.payload.read_multiplayer_match()
    dispatch.packet.payload.require_exhausted()
    account_id = dispatch.context.identity.account_id
    if incoming.host_id != account_id:
        return b""
    state = await dispatch.multiplayer.change_password(
        ChangeRoomPassword(
            _meta(
                "password",
                dispatch.packet_type,
                dispatch.raw_payload,
                dispatch.context,
                dispatch.services,
                epoch=f"{current.room.session_id}:{current.room.version}",
            ),
            current.room.public_id,
            current.room.version,
            incoming.password,
        )
    )
    return await _state_response(state, account_id, dispatch.services)


async def _change_mods(dispatch: _MultiplayerPacketContext, current: RoomState) -> bytes:
    legacy_bits = dispatch.packet.payload.read_i32()
    dispatch.packet.payload.require_exhausted()
    mods, variant = parse_legacy_mods(legacy_bits)
    account_id = dispatch.context.identity.account_id
    public_id = current.room.public_id
    if current.room.settings.free_mods:
        slot_mods = tuple(mod for mod in mods if mod.acronym not in _SPEED_MODS)
        state = current
        if current.room.host_account_id == account_id:
            room_mods = tuple(mod for mod in mods if mod.acronym in _SPEED_MODS)
            state = await dispatch.multiplayer.update_settings(
                UpdateRoomSettings(
                    _meta(
                        "speed-mods",
                        dispatch.packet_type,
                        dispatch.raw_payload,
                        dispatch.context,
                        dispatch.services,
                        epoch=f"{current.room.session_id}:{current.room.version}",
                    ),
                    public_id,
                    current.room.version,
                    replace(current.room.settings, mods=room_mods, variant=variant),
                )
            )
        state = await dispatch.multiplayer.set_slot_mods(public_id, account_id, slot_mods)
    elif current.room.host_account_id == account_id:
        state = await dispatch.multiplayer.update_settings(
            UpdateRoomSettings(
                _meta(
                    "mods",
                    dispatch.packet_type,
                    dispatch.raw_payload,
                    dispatch.context,
                    dispatch.services,
                    epoch=f"{current.room.session_id}:{current.room.version}",
                ),
                public_id,
                current.room.version,
                replace(current.room.settings, mods=mods, variant=variant),
            )
        )
    else:
        return b""
    _log_ephemeral_state(dispatch.packet_type, state, dispatch.context, dispatch.services)
    return await _state_response(state, account_id, dispatch.services)


async def _dispatch_room_round(dispatch: _MultiplayerPacketContext, current: RoomState) -> bytes:
    packet = dispatch.packet
    account_id = dispatch.context.identity.account_id
    public_id = current.room.public_id
    if dispatch.packet_type is ClientPacket.MATCH_START:
        return await _start_round(dispatch, current)
    if dispatch.packet_type is ClientPacket.MATCH_COMPLETE:
        return await _complete_round(dispatch, current)
    if dispatch.packet_type is ClientPacket.MATCH_LOAD_COMPLETE:
        packet.payload.require_exhausted()
        state = await dispatch.multiplayer.mark_loaded(public_id, account_id)
        if all(slot.loaded for slot in state.slots if slot.status is SlotStatus.PLAYING):
            wire = match_all_players_loaded()
            failed = await _broadcast_match(wire, state, account_id, dispatch.services)
            _log_ephemeral_state(
                dispatch.packet_type, state, dispatch.context, dispatch.services, delivery_failure_count=len(failed)
            )
            return wire
        _log_ephemeral_state(dispatch.packet_type, state, dispatch.context, dispatch.services)
        return b""
    if dispatch.packet_type is ClientPacket.MATCH_NO_BEATMAP:
        status = SlotStatus.NO_BEATMAP
    elif dispatch.packet_type in {ClientPacket.MATCH_NOT_READY, ClientPacket.MATCH_HAS_BEATMAP}:
        status = SlotStatus.NOT_READY
    elif dispatch.packet_type is ClientPacket.MATCH_FAILED:
        return await _fail_round(dispatch, current)
    else:
        return await _skip_round(dispatch, current)
    packet.payload.require_exhausted()
    state = await dispatch.multiplayer.set_slot_status(public_id, account_id, status)
    _log_ephemeral_state(dispatch.packet_type, state, dispatch.context, dispatch.services)
    return await _state_response(state, account_id, dispatch.services, lobby=False)


async def _start_round(dispatch: _MultiplayerPacketContext, current: RoomState) -> bytes:
    dispatch.packet.payload.require_exhausted()
    state = await dispatch.multiplayer.start_round(
        StartRound(
            _meta(
                "start",
                dispatch.packet_type,
                dispatch.raw_payload,
                dispatch.context,
                dispatch.services,
                epoch=f"{current.room.session_id}:{current.room.version}",
            ),
            current.room.public_id,
            current.room.version,
        )
    )
    wire = match_start(_wire_match(state))
    failed = set(await _broadcast_playing(wire, state, dispatch.context.identity.account_id, dispatch.services))
    failed.update(
        await _broadcast_lobby(update_match(_wire_match(state), send_password=False), None, dispatch.services)
    )
    log_event(
        "INFO",
        "stable.multiplayer.round_started",
        outcome="started",
        account_id=dispatch.context.identity.account_id,
        room_id=state.room.public_id,
        participant_count=len(state.round_participant_account_ids),
        delivery_failure_count=len(failed),
    )
    return wire + _delivery_warning(failed)


async def _complete_round(dispatch: _MultiplayerPacketContext, current: RoomState) -> bytes:
    dispatch.packet.payload.require_exhausted()
    account_id = dispatch.context.identity.account_id
    round_accounts = frozenset(current.round_participant_account_ids)
    state = await dispatch.multiplayer.set_slot_status(current.room.public_id, account_id, SlotStatus.COMPLETE)
    if any(slot.status is SlotStatus.PLAYING for slot in state.slots):
        return b""
    completed = await dispatch.multiplayer.complete_round(
        CompleteRound(
            _meta(
                "complete",
                dispatch.packet_type,
                dispatch.raw_payload,
                dispatch.context,
                dispatch.services,
                epoch=str(current.round_id),
            ),
            current.room.public_id,
            state.room.version,
        )
    )
    wire = match_complete()
    failed = set(await _broadcast_accounts(wire, completed, round_accounts, account_id, dispatch.services))
    failed.update(await _broadcast_state(completed, None, dispatch.services))
    log_event(
        "INFO",
        "stable.multiplayer.round_completed",
        outcome="completed",
        account_id=account_id,
        room_id=completed.room.public_id,
        participant_count=len(round_accounts),
        delivery_failure_count=len(failed),
    )
    return wire + _delivery_warning(failed)


async def _fail_round(dispatch: _MultiplayerPacketContext, current: RoomState) -> bytes:
    dispatch.packet.payload.require_exhausted()
    account_id = dispatch.context.identity.account_id
    state = await dispatch.multiplayer.mark_failed(current.room.public_id, account_id)
    slot = state.slot_for(account_id)
    if slot is None:
        return b""
    wire = match_player_failed(slot.position)
    failed = await _broadcast_match(wire, state, account_id, dispatch.services)
    _log_ephemeral_state(
        dispatch.packet_type, state, dispatch.context, dispatch.services, delivery_failure_count=len(failed)
    )
    return wire


async def _skip_round(dispatch: _MultiplayerPacketContext, current: RoomState) -> bytes:
    dispatch.packet.payload.require_exhausted()
    account_id = dispatch.context.identity.account_id
    state = await dispatch.multiplayer.mark_skipped(current.room.public_id, account_id)
    player_wire = match_player_skipped(account_id)
    failed = set(await _broadcast_match(player_wire, state, account_id, dispatch.services))
    if all(slot.skipped for slot in state.slots if slot.status is SlotStatus.PLAYING):
        skip_wire = match_skip()
        failed.update(await _broadcast_match(skip_wire, state, account_id, dispatch.services))
        _log_ephemeral_state(
            dispatch.packet_type, state, dispatch.context, dispatch.services, delivery_failure_count=len(failed)
        )
        return player_wire + skip_wire
    _log_ephemeral_state(
        dispatch.packet_type, state, dispatch.context, dispatch.services, delivery_failure_count=len(failed)
    )
    return player_wire


async def _dispatch_room_host(dispatch: _MultiplayerPacketContext, current: RoomState) -> bytes:
    if dispatch.packet_type is ClientPacket.MATCH_TRANSFER_HOST:
        return await _transfer_host(dispatch, current)
    return await _invite_player(dispatch, current)


async def _transfer_host(dispatch: _MultiplayerPacketContext, current: RoomState) -> bytes:
    position = dispatch.packet.payload.read_i32()
    dispatch.packet.payload.require_exhausted()
    if not 0 <= position < len(current.slots):
        return b""
    target = current.slots[position].account_id
    if target is None:
        return b""
    state = await dispatch.multiplayer.change_host(
        ChangeHost(
            _meta(
                "host",
                dispatch.packet_type,
                dispatch.raw_payload,
                dispatch.context,
                dispatch.services,
                epoch=f"{current.room.session_id}:{current.room.version}",
            ),
            current.room.public_id,
            current.room.version,
            target,
        )
    )
    delivered = await _enqueue(target, match_transfer_host(), state, dispatch.services)
    _log_ephemeral_state(
        dispatch.packet_type,
        state,
        dispatch.context,
        dispatch.services,
        delivery_failure_count=int(not delivered),
    )
    response = await _state_response(state, dispatch.context.identity.account_id, dispatch.services)
    return response + _delivery_warning(() if delivered else (target,))


async def _invite_player(dispatch: _MultiplayerPacketContext, current: RoomState) -> bytes:
    target_account_id = dispatch.packet.payload.read_i32()
    dispatch.packet.payload.require_exhausted()
    account_id = dispatch.context.identity.account_id
    if target_account_id < 1 or target_account_id == account_id:
        return b""
    target_presence = await dispatch.services.realtime.get_presence(target_account_id, at=dispatch.services.clock.now())
    if target_presence is None:
        return notification("The invited player is offline.")
    target_name = _presence_name(target_presence.payload, target_account_id)
    admission = await dispatch.multiplayer.issue_admission_token(
        current.room.public_id,
        inviter_account_id=account_id,
        recipient_account_id=target_account_id,
    )
    wire = match_invite(
        Message(
            dispatch.context.identity.current_name,
            f"Come join my game: [osump://{current.room.public_id}/{admission} {current.room.settings.name}].",
            target_name,
            account_id,
        )
    )
    delivered = await _enqueue(target_account_id, wire, current, dispatch.services)
    return b"" if delivered else notification("The invite could not be delivered; the room is still available.")


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
        external_beatmap_id=match.beatmap_id,
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


def _participant_count(state: RoomState) -> int:
    return sum(slot.account_id is not None for slot in state.slots)


def _log_ephemeral_state(
    packet_type: ClientPacket,
    state: RoomState,
    context: MultiplayerRuntimeContext,
    services: StableServices,
    *,
    delivery_failure_count: int = 0,
) -> None:
    sample_key = (
        context.realtime.session_id,
        packet_type.value,
        state.room.public_id,
        state.state_revision,
    )
    if not sampled(sample_key, services.settings.log_hot_path_sample_rate):
        return
    log_event(
        "DEBUG",
        "stable.multiplayer.ephemeral_state",
        outcome="updated",
        operation=packet_type.name,
        account_id=context.identity.account_id,
        room_id=state.room.public_id,
        state_revision=state.state_revision,
        participant_count=_participant_count(state),
        delivery_failure_count=delivery_failure_count,
    )


async def _state_response(
    state: RoomState,
    caller_account_id: int,
    services: StableServices,
    *,
    lobby: bool = True,
) -> bytes:
    wire = update_match(_wire_match(state), send_password=False)
    failed = set(await _broadcast_match(wire, state, caller_account_id, services))
    if lobby:
        failed.update(await _broadcast_lobby(wire, caller_account_id, services))
    return wire + _delivery_warning(failed)


async def _broadcast_state(
    state: RoomState,
    caller_account_id: int | None,
    services: StableServices,
) -> frozenset[int]:
    wire = update_match(_wire_match(state), send_password=False)
    failed = set(await _broadcast_match(wire, state, caller_account_id, services))
    failed.update(await _broadcast_lobby(wire, caller_account_id, services))
    return frozenset(failed)


async def _broadcast_match(
    payload: bytes,
    state: RoomState,
    excluded_account_id: int | None,
    services: StableServices,
) -> frozenset[int]:
    failed: dict[int, BaseException] = {}
    for account_id in {slot.account_id for slot in state.slots if slot.account_id is not None}:
        if account_id != excluded_account_id:
            await _enqueue(account_id, payload, state, services, failure_errors=failed)
    _log_broadcast_failures("match", failed, room_id=state.room.public_id)
    return frozenset(failed)


async def _broadcast_accounts(
    payload: bytes,
    state: RoomState,
    account_ids: frozenset[int],
    excluded_account_id: int | None,
    services: StableServices,
) -> frozenset[int]:
    failed: dict[int, BaseException] = {}
    for account_id in account_ids:
        if account_id != excluded_account_id:
            await _enqueue(account_id, payload, state, services, failure_errors=failed)
    _log_broadcast_failures("round_accounts", failed, room_id=state.room.public_id)
    return frozenset(failed)


async def _broadcast_playing(
    payload: bytes,
    state: RoomState,
    excluded_account_id: int | None,
    services: StableServices,
) -> frozenset[int]:
    failed: dict[int, BaseException] = {}
    for slot in state.slots:
        if slot.status is SlotStatus.PLAYING and slot.account_id is not None and slot.account_id != excluded_account_id:
            await _enqueue(slot.account_id, payload, state, services, failure_errors=failed)
    _log_broadcast_failures("playing", failed, room_id=state.room.public_id)
    return frozenset(failed)


async def _broadcast_lobby(
    payload: bytes,
    excluded_account_id: int | None,
    services: StableServices,
) -> frozenset[int]:
    if services.community is None:
        return frozenset()
    lookup_account_id = excluded_account_id or 1
    try:
        lobby = await services.community.get_public_channel_by_stable_name(lookup_account_id, "#lobby")
    except ApplicationError as error:
        log_event(
            "WARNING",
            "stable.multiplayer.broadcast_failed",
            exception=error,
            scope="lobby_lookup",
            failure_count=1,
            error_code=error.code,
            error_type=type(error).__name__,
        )
        return frozenset()
    failed: dict[int, BaseException] = {}
    for account_id in await services.realtime.list_channel_members(lobby.channel_id):
        if account_id == excluded_account_id:
            continue
        presence = await services.realtime.get_presence(account_id, at=services.clock.now())
        if presence is None:
            continue
        try:
            await services.realtime.enqueue_mailbox(
                account_id,
                payload,
                recipient_fence=presence.fence,
                expires_at=presence.expires_at,
            )
        except MailboxOverflow as error:
            failed[account_id] = error
    _log_broadcast_failures("lobby", failed)
    return frozenset(failed)


async def _enqueue(
    account_id: int,
    payload: bytes,
    state: RoomState,
    services: StableServices,
    *,
    failure_errors: dict[int, BaseException] | None = None,
) -> bool:
    presence = await services.realtime.get_presence(account_id, at=services.clock.now())
    if presence is None:
        return True
    try:
        await services.realtime.enqueue_mailbox(
            account_id,
            payload,
            recipient_fence=presence.fence,
            expires_at=presence.expires_at,
        )
    except MailboxOverflow as error:
        if failure_errors is not None:
            failure_errors[account_id] = error
        elif rate_limit("stable-multiplayer-delivery-failed", interval_seconds=5):
            log_event(
                "WARNING",
                "stable.multiplayer.broadcast_failed",
                exception=error,
                scope="recipient",
                room_id=state.room.public_id,
                failure_count=1,
            )
        return False
    return True


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


async def _leave_lobby_after_join(
    context: MultiplayerRuntimeContext,
    state: RoomState,
    services: StableServices,
) -> None:
    try:
        await _set_lobby_membership(context, services, joining=False)
    except Exception as error:
        log_event(
            "WARNING",
            "stable.multiplayer.cleanup_failed",
            exception=error,
            operation="leave_lobby_after_join",
            account_id=context.identity.account_id,
            room_id=state.room.public_id,
            error_code=getattr(error, "code", "cleanup_failed"),
            error_type=type(error).__name__,
        )


def _meta(
    operation: str,
    packet_type: ClientPacket,
    payload: bytes,
    context: MultiplayerRuntimeContext,
    services: StableServices,
    *,
    epoch: str | None = None,
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
        idempotency_key=(
            f"stable-multiplayer:{operation}:{context.identity.session_id}:{epoch or 'session'}:{digest.hex()}"
        ),
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
    from perfcho.modules.realtime.stable.codec import PacketReader
    from perfcho.modules.realtime.stable.models import ServerPacket

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
        "match_projection_unavailable": "The multiplayer state is recovering; please retry.",
    }
    return messages.get(error.code, "The multiplayer action could not be completed.")


def _delivery_warning(failed_account_ids: object) -> bytes:
    if not failed_account_ids:
        return b""
    return notification("A multiplayer update was deferred; affected players will recover the current room state.")


def _log_broadcast_failures(
    scope: str,
    failures: dict[int, BaseException],
    *,
    room_id: int | None = None,
) -> None:
    if not failures:
        return
    log_event(
        "WARNING",
        "stable.multiplayer.broadcast_failed",
        exception=next(iter(failures.values())),
        scope=scope,
        room_id=room_id,
        failure_count=len(failures),
    )
