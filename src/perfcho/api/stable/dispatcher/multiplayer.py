"""Adapt Stable lobby and match packets to the canonical multiplayer service."""

import hashlib
from dataclasses import dataclass, replace

from perfcho.api.stable.canonize.scoring import parse_legacy_mods
from perfcho.api.stable.channels import parse_stable_channel_selector
from perfcho.api.stable.dispatcher.models import MultiplayerRuntimeContext
from perfcho.api.stable.realtime.codec import Packet, ProtocolError
from perfcho.api.stable.realtime.models import ClientPacket, MultiplayerMatch
from perfcho.infra.compose import StableServices
from perfcho.infra.db.mods import project_scoreboard_variant
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
    MultiplayerMutationKind,
    MultiplayerMutationResult,
    MultiplayerService,
    RoomSettings,
    RoomState,
    SlotStatus,
    StartRound,
    TeamMode,
    UpdateRoomSettings,
    WinCondition,
)
from perfcho.modules.realtime import (
    MultiplayerInvitationState,
    MultiplayerRoomAction,
    MultiplayerRoomBubble,
    MultiplayerScoreState,
    MultiplayerSignalBubble,
    MultiplayerSignalKind,
    RealtimeBubble,
    ToastBubble,
    multiplayer_room_snapshot,
)
from perfcho.modules.scoring import Ruleset
from perfcho.modules.scoring.mods import normalize_mods

_RULESETS = (Ruleset.OSU, Ruleset.TAIKO, Ruleset.FRUITS, Ruleset.MANIA)
_TEAM_MODES = (TeamMode.HEAD_TO_HEAD, TeamMode.TAG_COOP, TeamMode.TEAM_VS, TeamMode.TAG_TEAM_VS)
_WIN_CONDITIONS = (WinCondition.SCORE, WinCondition.ACCURACY, WinCondition.COMBO, WinCondition.SCORE_V2)
_TEAM_MODE_IDS = {value: index for index, value in enumerate(_TEAM_MODES)}
_WIN_CONDITION_IDS = {value: index for index, value in enumerate(_WIN_CONDITIONS)}
_SPEED_MODS = frozenset({"DT", "NC", "HT"})
_STABLE_ROOM_CAPACITY = 16
_STABLE_PUBLIC_ID_LIMIT = 32767

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
        context.local_bubbles.append(MultiplayerSignalBubble(MultiplayerSignalKind.JOIN_FAILED, None))
        return b""
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
            context.local_bubbles.append(MultiplayerSignalBubble(MultiplayerSignalKind.JOIN_FAILED, None))
        context.local_bubbles.append(ToastBubble(_failure_message(error)))
        return b""
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
            context.local_bubbles.append(MultiplayerSignalBubble(MultiplayerSignalKind.JOIN_FAILED, None))
        context.local_bubbles.append(ToastBubble("The multiplayer request is invalid."))
        return b""
    return b""


async def dispatch_multiplayer_mutation(
    mutation: MultiplayerMutationResult,
    caller_account_id: int,
    services: StableServices,
    local_bubbles: list[RealtimeBubble],
) -> bytes:
    """Map one canonical multiplayer mutation to local and remote Bubbles."""
    if mutation.replayed:
        return b""
    state = mutation.state
    kind = mutation.kind
    if kind in {MultiplayerMutationKind.SETTINGS_UPDATED, MultiplayerMutationKind.PASSWORD_CHANGED}:
        return await _state_response(state, caller_account_id, services, local_bubbles)
    if kind is MultiplayerMutationKind.ROUND_STARTED:
        started = MultiplayerRoomBubble(MultiplayerRoomAction.ROUND_STARTED, multiplayer_room_snapshot(state))
        local_bubbles.append(started)
        failed = set(await _broadcast_playing(started, state, caller_account_id, services))
        failed.update(
            await _broadcast_lobby(
                MultiplayerRoomBubble(MultiplayerRoomAction.UPDATED, multiplayer_room_snapshot(state)),
                None,
                services,
            )
        )
        log_event(
            "INFO",
            "stable.multiplayer.round_started",
            outcome="started",
            account_id=caller_account_id,
            room_id=state.room.public_id,
            participant_count=len(mutation.round_participant_account_ids),
            delivery_failure_count=len(failed),
        )
        _append_delivery_warning(local_bubbles, failed)
        return b""
    if kind in {MultiplayerMutationKind.ROUND_COMPLETED, MultiplayerMutationKind.ROUND_ABORTED}:
        action = (
            MultiplayerRoomAction.ROUND_ABORTED
            if kind is MultiplayerMutationKind.ROUND_ABORTED
            else MultiplayerRoomAction.ROUND_COMPLETED
        )
        lifecycle = MultiplayerRoomBubble(action, multiplayer_room_snapshot(state))
        updated = MultiplayerRoomBubble(MultiplayerRoomAction.UPDATED, multiplayer_room_snapshot(state))
        local_bubbles.extend((lifecycle, updated))
        round_accounts = frozenset(mutation.round_participant_account_ids)
        failed = set(await _broadcast_accounts(lifecycle, state, round_accounts, caller_account_id, services))
        failed.update(await _broadcast_state(state, caller_account_id, services))
        log_event(
            "INFO",
            "stable.multiplayer.round_aborted"
            if kind is MultiplayerMutationKind.ROUND_ABORTED
            else "stable.multiplayer.round_completed",
            outcome="aborted" if kind is MultiplayerMutationKind.ROUND_ABORTED else "completed",
            account_id=caller_account_id,
            room_id=state.room.public_id,
            participant_count=len(round_accounts),
            delivery_failure_count=len(failed),
        )
        _append_delivery_warning(local_bubbles, failed)
        return b""
    if kind is MultiplayerMutationKind.HOST_CHANGED:
        host_target = mutation.target_account_id or state.room.host_account_id
        signal = MultiplayerSignalBubble(
            MultiplayerSignalKind.HOST_TRANSFERRED,
            state.room.public_id,
            actor_account_id=host_target,
        )
        delivered = await _publish(host_target, signal, state, services)
        await _state_response(state, caller_account_id, services, local_bubbles)
        _append_delivery_warning(local_bubbles, () if delivered else (host_target,))
        return b""
    if kind is MultiplayerMutationKind.PARTICIPANT_KICKED:
        target = mutation.target_account_id
        if target is None:
            raise ValueError("participant-kicked mutation requires a target account")
        delivered = await _publish(
            target,
            MultiplayerRoomBubble(MultiplayerRoomAction.KICKED, multiplayer_room_snapshot(state)),
            state,
            services,
        )
        await _state_response(state, caller_account_id, services, local_bubbles)
        _append_delivery_warning(local_bubbles, () if delivered else (target,))
        return b""
    raise ValueError(f"unsupported multiplayer mutation kind: {kind}")


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
        context.local_bubbles.extend(
            MultiplayerRoomBubble(MultiplayerRoomAction.CREATED, multiplayer_room_snapshot(state))
            for state in states
            if _stable_visible(state)
        )
        return b""
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
        context.local_bubbles.append(MultiplayerSignalBubble(MultiplayerSignalKind.JOIN_FAILED, None))
        return b""
    state = await dispatch.multiplayer.create_room(
        CreateRoom(
            meta=_meta("create", dispatch.packet_type, dispatch.raw_payload, context, services),
            settings=_settings_from_wire(incoming),
            capacity=_STABLE_ROOM_CAPACITY,
            password=incoming.password,
            public_id_limit=_STABLE_PUBLIC_ID_LIMIT,
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
        context.local_bubbles.append(MultiplayerSignalBubble(MultiplayerSignalKind.JOIN_FAILED, None))
        return b""
    await _leave_lobby_after_join(context, state, services)
    snapshot = multiplayer_room_snapshot(state)
    failed = await _broadcast_lobby(
        MultiplayerRoomBubble(MultiplayerRoomAction.CREATED, snapshot),
        context.identity.account_id,
        services,
    )
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
    context.local_bubbles.append(
        MultiplayerRoomBubble(
            MultiplayerRoomAction.JOINED,
            snapshot,
            local_admission_credential=incoming.password,
        )
    )
    _append_delivery_warning(context.local_bubbles, failed)
    return b""


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
    context.local_bubbles.append(
        MultiplayerRoomBubble(
            MultiplayerRoomAction.JOINED,
            multiplayer_room_snapshot(state),
            local_admission_credential=password,
        )
    )
    _append_delivery_warning(context.local_bubbles, failed)
    return b""


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
    score = MultiplayerScoreState(
        account_id,
        frame.time,
        slot.position,
        frame.count_300,
        frame.count_100,
        frame.count_50,
        frame.count_geki,
        frame.count_katu,
        frame.count_miss,
        frame.total_score,
        frame.max_combo,
        frame.current_combo,
        frame.perfect,
        frame.current_hp,
        frame.tag_byte,
        frame.score_v2,
        frame.combo_portion,
        frame.bonus_portion,
    )
    bubble = MultiplayerSignalBubble(
        MultiplayerSignalKind.SCORE_UPDATED,
        cached.room.public_id,
        actor_account_id=account_id,
        score=score,
    )
    dispatch.context.local_bubbles.append(bubble)
    failed = await _broadcast_accounts(
        bubble,
        cached,
        frozenset(cached.round_participant_account_ids),
        account_id,
        dispatch.services,
    )
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
    failed: set[int]
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
        failed = set(
            await _broadcast_lobby(
                MultiplayerRoomBubble(MultiplayerRoomAction.DISPOSED, multiplayer_room_snapshot(current)),
                account_id,
                dispatch.services,
            )
        )
        action = "closed"
        participant_count = 0
    else:
        failed = set(await _broadcast_state(state, account_id, dispatch.services))
        action = "left"
        participant_count = _participant_count(state)
        if current.room.host_account_id == account_id and not await _publish(
            state.room.host_account_id,
            MultiplayerSignalBubble(
                MultiplayerSignalKind.HOST_TRANSFERRED,
                state.room.public_id,
                actor_account_id=state.room.host_account_id,
            ),
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
    dispatch.context.local_bubbles.append(
        MultiplayerRoomBubble(MultiplayerRoomAction.LEFT, multiplayer_room_snapshot(current))
    )
    _append_delivery_warning(dispatch.context.local_bubbles, failed)
    return b""


async def _dispatch_room_settings(dispatch: _MultiplayerPacketContext, current: RoomState) -> bytes:
    packet = dispatch.packet
    account_id = dispatch.context.identity.account_id
    public_id = current.room.public_id
    if dispatch.packet_type is ClientPacket.MATCH_CHANGE_SLOT:
        target = packet.payload.read_i32()
        packet.payload.require_exhausted()
        if not 0 <= target < _STABLE_ROOM_CAPACITY:
            raise ValueError("Stable slot is outside the supported range")
        state = await dispatch.multiplayer.move_slot(public_id, account_id, target)
        _log_ephemeral_state(dispatch.packet_type, state, dispatch.context, dispatch.services)
        return await _state_response(state, account_id, dispatch.services, dispatch.context.local_bubbles)
    if dispatch.packet_type is ClientPacket.MATCH_READY:
        packet.payload.require_exhausted()
        state = await dispatch.multiplayer.set_slot_status(public_id, account_id, SlotStatus.READY)
        _log_ephemeral_state(dispatch.packet_type, state, dispatch.context, dispatch.services)
        return await _state_response(state, account_id, dispatch.services, dispatch.context.local_bubbles, lobby=False)
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
    return await _state_response(state, account_id, dispatch.services, dispatch.context.local_bubbles, lobby=False)


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
        await _publish(
            target_account_id,
            MultiplayerRoomBubble(MultiplayerRoomAction.KICKED, multiplayer_room_snapshot(current)),
            current,
            dispatch.services,
        )
    state = await dispatch.multiplayer.lock_slot(current.room.public_id, account_id, target)
    _log_ephemeral_state(dispatch.packet_type, state, dispatch.context, dispatch.services)
    return await _state_response(state, account_id, dispatch.services, dispatch.context.local_bubbles)


async def _change_settings(dispatch: _MultiplayerPacketContext, current: RoomState) -> bytes:
    incoming = dispatch.packet.payload.read_multiplayer_match()
    dispatch.packet.payload.require_exhausted()
    account_id = dispatch.context.identity.account_id
    if incoming.host_id != account_id:
        return b""
    mutation = await dispatch.multiplayer.update_settings(
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
    return await dispatch_multiplayer_mutation(mutation, account_id, dispatch.services, dispatch.context.local_bubbles)


async def _change_password(dispatch: _MultiplayerPacketContext, current: RoomState) -> bytes:
    incoming = dispatch.packet.payload.read_multiplayer_match()
    dispatch.packet.payload.require_exhausted()
    account_id = dispatch.context.identity.account_id
    if incoming.host_id != account_id:
        return b""
    mutation = await dispatch.multiplayer.change_password(
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
    return await dispatch_multiplayer_mutation(mutation, account_id, dispatch.services, dispatch.context.local_bubbles)


async def _change_mods(dispatch: _MultiplayerPacketContext, current: RoomState) -> bytes:
    legacy_bits = dispatch.packet.payload.read_i32()
    dispatch.packet.payload.require_exhausted()
    mods = parse_legacy_mods(legacy_bits)
    account_id = dispatch.context.identity.account_id
    public_id = current.room.public_id
    if current.room.settings.free_mods:
        slot_mods = tuple(mod for mod in mods if mod.acronym not in _SPEED_MODS)
        state = current
        if current.room.host_account_id == account_id:
            room_mods = tuple(mod for mod in mods if mod.acronym in _SPEED_MODS)
            mutation = await dispatch.multiplayer.update_settings(
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
                    replace(
                        current.room.settings,
                        mods=room_mods,
                        variant=project_scoreboard_variant(room_mods),
                    ),
                )
            )
            state = mutation.state
        state = await dispatch.multiplayer.set_slot_mods(public_id, account_id, slot_mods)
    elif current.room.host_account_id == account_id:
        mutation = await dispatch.multiplayer.update_settings(
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
                replace(current.room.settings, mods=mods, variant=project_scoreboard_variant(mods)),
            )
        )
        state = mutation.state
    else:
        return b""
    _log_ephemeral_state(dispatch.packet_type, state, dispatch.context, dispatch.services)
    return await _state_response(state, account_id, dispatch.services, dispatch.context.local_bubbles)


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
            bubble = MultiplayerSignalBubble(MultiplayerSignalKind.PARTICIPANT_LOADING_COMPLETED, public_id)
            dispatch.context.local_bubbles.append(bubble)
            failed = await _broadcast_match(bubble, state, account_id, dispatch.services)
            _log_ephemeral_state(
                dispatch.packet_type, state, dispatch.context, dispatch.services, delivery_failure_count=len(failed)
            )
            return b""
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
    return await _state_response(state, account_id, dispatch.services, dispatch.context.local_bubbles, lobby=False)


async def _start_round(dispatch: _MultiplayerPacketContext, current: RoomState) -> bytes:
    dispatch.packet.payload.require_exhausted()
    mutation = await dispatch.multiplayer.start_round(
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
    return await dispatch_multiplayer_mutation(
        mutation,
        dispatch.context.identity.account_id,
        dispatch.services,
        dispatch.context.local_bubbles,
    )


async def _complete_round(dispatch: _MultiplayerPacketContext, current: RoomState) -> bytes:
    dispatch.packet.payload.require_exhausted()
    account_id = dispatch.context.identity.account_id
    state = await dispatch.multiplayer.set_slot_status(current.room.public_id, account_id, SlotStatus.COMPLETE)
    if any(slot.status is SlotStatus.PLAYING for slot in state.slots):
        return b""
    mutation = await dispatch.multiplayer.complete_round(
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
    return await dispatch_multiplayer_mutation(mutation, account_id, dispatch.services, dispatch.context.local_bubbles)


async def _fail_round(dispatch: _MultiplayerPacketContext, current: RoomState) -> bytes:
    dispatch.packet.payload.require_exhausted()
    account_id = dispatch.context.identity.account_id
    state = await dispatch.multiplayer.mark_failed(current.room.public_id, account_id)
    slot = state.slot_for(account_id)
    if slot is None:
        return b""
    bubble = MultiplayerSignalBubble(
        MultiplayerSignalKind.FAILED,
        state.room.public_id,
        actor_account_id=account_id,
        slot_position=slot.position,
    )
    dispatch.context.local_bubbles.append(bubble)
    failed = await _broadcast_match(bubble, state, account_id, dispatch.services)
    _log_ephemeral_state(
        dispatch.packet_type, state, dispatch.context, dispatch.services, delivery_failure_count=len(failed)
    )
    return b""


async def _skip_round(dispatch: _MultiplayerPacketContext, current: RoomState) -> bytes:
    dispatch.packet.payload.require_exhausted()
    account_id = dispatch.context.identity.account_id
    state = await dispatch.multiplayer.mark_skipped(current.room.public_id, account_id)
    player = MultiplayerSignalBubble(
        MultiplayerSignalKind.SKIPPED,
        state.room.public_id,
        actor_account_id=account_id,
    )
    dispatch.context.local_bubbles.append(player)
    failed = set(await _broadcast_match(player, state, account_id, dispatch.services))
    if all(slot.skipped for slot in state.slots if slot.status is SlotStatus.PLAYING):
        skipped = MultiplayerSignalBubble(MultiplayerSignalKind.ALL_PLAYERS_SKIPPED, state.room.public_id)
        dispatch.context.local_bubbles.append(skipped)
        failed.update(await _broadcast_match(skipped, state, account_id, dispatch.services))
        _log_ephemeral_state(
            dispatch.packet_type, state, dispatch.context, dispatch.services, delivery_failure_count=len(failed)
        )
        return b""
    _log_ephemeral_state(
        dispatch.packet_type, state, dispatch.context, dispatch.services, delivery_failure_count=len(failed)
    )
    return b""


async def _dispatch_room_host(dispatch: _MultiplayerPacketContext, current: RoomState) -> bytes:
    if dispatch.packet_type is ClientPacket.MATCH_TRANSFER_HOST:
        return await _transfer_host(dispatch, current)
    return await _invite_player(dispatch, current)


async def _transfer_host(dispatch: _MultiplayerPacketContext, current: RoomState) -> bytes:
    position = dispatch.packet.payload.read_i32()
    dispatch.packet.payload.require_exhausted()
    if not 0 <= position < min(len(current.slots), _STABLE_ROOM_CAPACITY):
        return b""
    target = current.slots[position].account_id
    if target is None:
        return b""
    mutation = await dispatch.multiplayer.change_host(
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
    return await dispatch_multiplayer_mutation(
        mutation,
        dispatch.context.identity.account_id,
        dispatch.services,
        dispatch.context.local_bubbles,
    )


async def _invite_player(dispatch: _MultiplayerPacketContext, current: RoomState) -> bytes:
    target_account_id = dispatch.packet.payload.read_i32()
    dispatch.packet.payload.require_exhausted()
    account_id = dispatch.context.identity.account_id
    if target_account_id < 1 or target_account_id == account_id:
        return b""
    target_presence = await dispatch.services.realtime.get_presence(target_account_id, at=dispatch.services.clock.now())
    if target_presence is None:
        dispatch.context.local_bubbles.append(ToastBubble("The invited player is offline."))
        return b""
    target_name = target_presence.identity.display_name
    admission = await dispatch.multiplayer.issue_admission_token(
        current.room.public_id,
        inviter_account_id=account_id,
        recipient_account_id=target_account_id,
    )
    bubble = MultiplayerSignalBubble(
        MultiplayerSignalKind.INVITED,
        current.room.public_id,
        actor_account_id=account_id,
        invitation=MultiplayerInvitationState(
            account_id,
            dispatch.context.identity.current_name,
            target_name,
            current.room.settings.name,
            admission,
        ),
    )
    delivered = await _publish(target_account_id, bubble, current, dispatch.services)
    if not delivered:
        dispatch.context.local_bubbles.append(
            ToastBubble("The invite could not be delivered; the room is still available.")
        )
    return b""


def _settings_from_wire(match: MultiplayerMatch) -> RoomSettings:
    if not 0 <= match.mode < len(_RULESETS):
        raise ValueError("Stable match ruleset is invalid")
    if not 0 <= match.team_type < len(_TEAM_MODES):
        raise ValueError("Stable match team mode is invalid")
    if not 0 <= match.win_condition < len(_WIN_CONDITIONS):
        raise ValueError("Stable match win condition is invalid")
    mods = parse_legacy_mods(match.mods)
    variant = project_scoreboard_variant(mods)
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
    local_bubbles: list[RealtimeBubble],
    *,
    lobby: bool = True,
) -> bytes:
    bubble = MultiplayerRoomBubble(MultiplayerRoomAction.UPDATED, multiplayer_room_snapshot(state))
    local_bubbles.append(bubble)
    failed = set(await _broadcast_match(bubble, state, caller_account_id, services))
    if lobby:
        failed.update(await _broadcast_lobby(bubble, caller_account_id, services))
    _append_delivery_warning(local_bubbles, failed)
    return b""


async def _broadcast_state(
    state: RoomState,
    caller_account_id: int | None,
    services: StableServices,
) -> frozenset[int]:
    bubble = MultiplayerRoomBubble(MultiplayerRoomAction.UPDATED, multiplayer_room_snapshot(state))
    failed = set(await _broadcast_match(bubble, state, caller_account_id, services))
    failed.update(await _broadcast_lobby(bubble, caller_account_id, services))
    return frozenset(failed)


async def _broadcast_match(
    bubble: RealtimeBubble,
    state: RoomState,
    excluded_account_id: int | None,
    services: StableServices,
) -> frozenset[int]:
    account_ids = tuple(
        account_id
        for account_id in {slot.account_id for slot in state.slots if slot.account_id is not None}
        if account_id != excluded_account_id
    )
    failed = await _publish_many(account_ids, bubble, services)
    _log_broadcast_failures("match", failed, room_id=state.room.public_id)
    return frozenset(failed)


async def _broadcast_accounts(
    bubble: RealtimeBubble,
    state: RoomState,
    account_ids: frozenset[int],
    excluded_account_id: int | None,
    services: StableServices,
) -> frozenset[int]:
    recipients = tuple(account_id for account_id in account_ids if account_id != excluded_account_id)
    failed = await _publish_many(recipients, bubble, services)
    _log_broadcast_failures("round_accounts", failed, room_id=state.room.public_id)
    return frozenset(failed)


async def _broadcast_playing(
    bubble: RealtimeBubble,
    state: RoomState,
    excluded_account_id: int | None,
    services: StableServices,
) -> frozenset[int]:
    account_ids = tuple(
        slot.account_id
        for slot in state.slots
        if slot.status is SlotStatus.PLAYING
        and slot.account_id is not None
        and slot.account_id != excluded_account_id
    )
    failed = await _publish_many(account_ids, bubble, services)
    _log_broadcast_failures("playing", failed, room_id=state.room.public_id)
    return frozenset(failed)


async def _broadcast_lobby(
    bubble: RealtimeBubble,
    excluded_account_id: int | None,
    services: StableServices,
) -> frozenset[int]:
    if isinstance(bubble, MultiplayerRoomBubble) and not _stable_visible(bubble):
        return frozenset()
    if services.community is None or services.bubbles is None:
        return frozenset()
    lookup_account_id = excluded_account_id or 1
    try:
        lobby = await services.community.get_public_channel(
            lookup_account_id,
            parse_stable_channel_selector("#lobby"),
        )
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
    account_ids = tuple(
        account_id
        for account_id in await services.realtime.list_channel_members(lobby.channel_id)
        if account_id != excluded_account_id
    )
    failed = await _publish_many(account_ids, bubble, services)
    _log_broadcast_failures("lobby", failed)
    return frozenset(failed)


async def _publish_many(
    account_ids: tuple[int, ...],
    bubble: RealtimeBubble,
    services: StableServices,
) -> dict[int, BaseException]:
    if isinstance(bubble, MultiplayerRoomBubble) and bubble.local_admission_credential is not None:
        raise ValueError("local multiplayer admission credentials cannot be published")
    if services.bubbles is None:
        return {}
    presences = await services.realtime.get_presences(account_ids, at=services.clock.now())
    online_account_ids = tuple(presence.account_id for presence in presences)
    if not online_account_ids:
        return {}
    try:
        await services.bubbles.publish_many(online_account_ids, bubble)
    except Exception as error:
        return dict.fromkeys(online_account_ids, error)
    return {}


async def _publish(
    account_id: int,
    bubble: RealtimeBubble,
    state: RoomState,
    services: StableServices,
    *,
    failure_errors: dict[int, BaseException] | None = None,
) -> bool:
    if isinstance(bubble, MultiplayerRoomBubble) and bubble.local_admission_credential is not None:
        raise ValueError("local multiplayer admission credentials cannot be published")
    presence = await services.realtime.get_presence(account_id, at=services.clock.now())
    if presence is None:
        return True
    if services.bubbles is None:
        return False
    try:
        await services.bubbles.publish(presence.account_id, bubble)
    except Exception as error:
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
    lobby = await services.community.get_public_channel(
        context.identity.account_id,
        parse_stable_channel_selector("#lobby"),
    )
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


def _stable_visible(state: RoomState | MultiplayerRoomBubble) -> bool:
    capacity = state.room.capacity
    public_id = state.room.room_public_id if isinstance(state, MultiplayerRoomBubble) else state.room.public_id
    return capacity == _STABLE_ROOM_CAPACITY and 0 < public_id <= _STABLE_PUBLIC_ID_LIMIT


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


def _append_delivery_warning(local_bubbles: list[RealtimeBubble], failed_account_ids: object) -> None:
    if failed_account_ids:
        local_bubbles.append(
            ToastBubble("A multiplayer update was lost; affected players can recover the current room state.")
        )


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
