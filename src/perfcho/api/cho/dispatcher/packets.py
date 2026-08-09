"""Dispatch core Stable packets over canonical identity and realtime services."""

import asyncio
import hashlib
import uuid
from collections.abc import Awaitable, Callable, Sequence
from datetime import datetime, timedelta
from time import monotonic_ns

from perfcho.api.cho.dispatcher.models import StableRuntimeContext
from perfcho.api.cho.dispatcher.multiplayer import (
    MULTIPLAYER_PACKETS,
    _broadcast_lobby,
    _broadcast_state,
    _enqueue,
    dispatch_multiplayer_mutation,
    dispatch_multiplayer_packet,
)
from perfcho.infra.compose import StableServices
from perfcho.infra.logging import duration_ms, log_event, rate_limit, sampled
from perfcho.infra.settings import settings
from perfcho.modules.bot import BotDirective, BotInvocation
from perfcho.modules.common import Actor, ClientContext, CommandMeta
from perfcho.modules.common.errors import ApplicationError
from perfcho.modules.community import (
    AccountSilenced,
    ChannelAccessDenied,
    ChannelMembershipRequired,
    ChannelMembershipUnavailable,
    ChannelNotFound,
    CommunityInputRejected,
    DirectMessageBlocked,
    MessageIdempotencyConflict,
    PrivateMessageRejected,
    TargetAccountSilenced,
)
from perfcho.modules.identity import ResolvedStableSession
from perfcho.modules.multiplayer import CleanupPresence, MultiplayerMutationResult
from perfcho.modules.realtime import (
    InvalidFrame,
    MailboxOverflow,
    PresenceSnapshot,
    RealtimeSessionFenced,
    RealtimeSessionNotFound,
    SessionFence,
    SpectatorHostOffline,
    SpectatorRelation,
)
from perfcho.modules.realtime.stable.builders import (
    channel_info,
    channel_join,
    channel_kick,
    dispose_match,
    fellow_spectator_joined,
    fellow_spectator_left,
    friends_list,
    match_transfer_host,
    notification,
    restart,
    send_message,
    spectate_frames,
    spectator_cant_spectate,
    spectator_joined,
    spectator_left,
    target_is_silenced,
    user_dm_blocked,
    user_logout,
    user_presence,
    user_stats,
)
from perfcho.modules.realtime.stable.codec import Packet, PacketReader, ProtocolError, build_packet
from perfcho.modules.realtime.stable.models import (
    Channel,
    ClientPacket,
    ClientStatus,
    Message,
    ReplayAction,
    ServerPacket,
    UserPresence,
    UserStats,
)
from perfcho.modules.scoring import Ruleset
from perfcho.modules.scoring.mods import LEGACY_MOD_BITS, parse_legacy_mods
from perfcho.modules.social import SocialAccountNotFound, SocialInteractionBlocked, SocialRelationRejected

_MESSAGE_ID_WINDOW_SECONDS = 5


async def dispatch_packets(body: bytes, context: StableRuntimeContext, services: StableServices) -> bytes:
    """Map application failures into bounded Stable responses instead of JSON errors."""
    started_ns = monotonic_ns()
    collect_summary = sampled(started_ns, services.settings.log_hot_path_sample_rate)
    packet_histogram: dict[str, int] | None = {} if collect_summary else None
    outcome = "success"
    output = b""
    dispatch_error: BaseException | None = None
    try:
        output = await _dispatch_packets(body, context, services, packet_histogram)
    except (RealtimeSessionFenced, RealtimeSessionNotFound) as error:
        dispatch_error = error
        outcome = "realtime_fenced"
        local_output = bytearray()
        _extend_response(
            local_output,
            notification("Session state changed. Please reconnect.") + restart(0),
            services.settings.stable_max_response_bytes,
        )
        output = bytes(local_output)
        if rate_limit(f"stable-packet-fenced:{error.code}", interval_seconds=5):
            log_event(
                "WARNING",
                "stable.packet.realtime_fenced",
                exception=error,
                outcome="reconnect",
                account_id=context.identity.account_id,
                error_code=error.code,
                error_type=type(error).__name__,
            )
    except ApplicationError as error:
        dispatch_error = error
        outcome = "application_rejected"
        local_output = bytearray()
        _extend_response(
            local_output,
            notification("The request could not be completed."),
            services.settings.stable_max_response_bytes,
        )
        output = bytes(local_output)
        if rate_limit(f"stable-packet-rejected:{error.code}", interval_seconds=5):
            log_event(
                "INFO",
                "stable.packet.application_rejected",
                exception=error,
                outcome="rejected",
                account_id=context.identity.account_id,
                error_code=error.code,
                error_type=type(error).__name__,
            )
    except (ProtocolError, ValueError) as error:
        dispatch_error = error
        outcome = "malformed"
        raise
    except BaseException:
        outcome = "failed"
        raise
    finally:
        if packet_histogram is not None:
            log_event(
                "DEBUG",
                "stable.packet.dispatch_summary",
                exception=dispatch_error,
                outcome=outcome,
                account_id=context.identity.account_id,
                packet_count=sum(packet_histogram.values()),
                packet_histogram=packet_histogram,
                input_bytes=len(body),
                output_bytes=len(output),
                duration_ms=duration_ms(started_ns),
            )
    return output


async def _dispatch_packets(
    body: bytes,
    context: StableRuntimeContext,
    services: StableServices,
    packet_histogram: dict[str, int] | None,
) -> bytes:
    """Process supported core packets sequentially and return request-local output."""
    output = bytearray()
    response_limit = services.settings.stable_max_response_bytes
    current_presence = context.presence
    current_stats = context.stats
    for packet in PacketReader(body):
        packet_type = packet.packet_type
        if packet_histogram is not None:
            packet_name = packet_type.name if isinstance(packet_type, ClientPacket) else "UNKNOWN"
            packet_histogram[packet_name] = packet_histogram.get(packet_name, 0) + 1
        if packet_type is ClientPacket.PING:
            # Stable client packet ID 4 is Osu_Pong, a keepalive response.
            # Bancho does not need to answer it; an empty successful Poll is valid.
            packet.payload.require_exhausted()
        elif packet_type is ClientPacket.REQUEST_STATUS_UPDATE:
            packet.payload.require_exhausted()
            current_stats = await account_stats(current_stats, services)
            current_presence = _presence_from_stats(current_presence, current_stats)
            await _store_presence(current_presence, current_stats, context, services)
            _extend_response(output, user_stats(current_stats), response_limit)
        elif packet_type is ClientPacket.CHANGE_ACTION:
            status = packet.payload.read_client_status()
            packet.payload.require_exhausted()
            status = _normalize_status_mods(status)
            current_stats = await account_stats(
                replace_stats_mode(current_stats, mode=status.mode, mods=status.mods),
                services,
            )
            current_stats = _stats_from_status(current_stats, status)
            current_presence = _presence_from_stats(current_presence, current_stats)
            payload = user_presence(current_presence) + user_stats(current_stats)
            await _store_presence(current_presence, current_stats, context, services)
            await broadcast_presence_update(payload, context.identity.account_id, services)
            _extend_response(output, user_stats(current_stats), response_limit)
        elif packet_type is ClientPacket.USER_STATS_REQUEST:
            account_ids = packet.payload.read_i32_list_u16(
                max_length=services.settings.stable_presence_batch_size,
            )
            packet.payload.require_exhausted()
            _extend_response(
                output,
                await _requested_packets(
                    account_ids,
                    ServerPacket.USER_STATS,
                    context.identity.account_id,
                    services,
                    max_bytes=response_limit - len(output),
                ),
                response_limit,
            )
        elif packet_type is ClientPacket.USER_PRESENCE_REQUEST:
            account_ids = packet.payload.read_i32_list_u16(
                max_length=services.settings.stable_presence_batch_size,
            )
            packet.payload.require_exhausted()
            _extend_response(
                output,
                await _requested_packets(
                    account_ids,
                    ServerPacket.USER_PRESENCE,
                    context.identity.account_id,
                    services,
                    max_bytes=response_limit - len(output),
                ),
                response_limit,
            )
        elif packet_type is ClientPacket.USER_PRESENCE_REQUEST_ALL:
            packet.payload.read_i32()
            packet.payload.require_exhausted()
            _extend_response(
                output,
                await _all_presence_packets(services, max_bytes=response_limit - len(output)),
                response_limit,
            )
        elif packet_type is ClientPacket.RECEIVE_UPDATES:
            update_filter = packet.payload.read_i32()
            packet.payload.require_exhausted()
            if update_filter not in {0, 1, 2}:
                raise ValueError("invalid Stable presence update filter")
            await services.realtime.set_presence_filter(
                context.identity.account_id,
                session_id=context.identity.session_id,
                expected_revision=context.realtime.revision,
                value=update_filter,
            )
        elif packet_type is ClientPacket.START_SPECTATING:
            host_account_id = packet.payload.read_i32()
            packet.payload.require_exhausted()
            _extend_response(
                output,
                await _start_spectating(
                    host_account_id,
                    context,
                    services,
                    max_bytes=response_limit - len(output),
                ),
                response_limit,
            )
        elif packet_type is ClientPacket.STOP_SPECTATING:
            packet.payload.require_exhausted()
            await _stop_spectating(context, services)
        elif packet_type is ClientPacket.SPECTATE_FRAMES:
            bundle = packet.payload.read_replay_frame_bundle()
            packet.payload.require_exhausted()
            sequence = bundle.sequence if bundle.sequence is not None else bundle.score_frame.time & 0xFFFF
            await _publish_spectator_frames(
                sequence,
                bundle.raw_data,
                context,
                services,
                reset_sequence=bundle.action is ReplayAction.NEW_SONG,
            )
        elif packet_type is ClientPacket.CANT_SPECTATE:
            packet.payload.require_exhausted()
            await _cant_spectate(context, services)
        elif packet_type in MULTIPLAYER_PACKETS:
            _extend_response(
                output,
                await dispatch_multiplayer_packet(packet, context, services),
                response_limit,
            )
        elif packet_type is ClientPacket.CHANNEL_JOIN:
            channel_name = packet.payload.read_string()
            packet.payload.require_exhausted()
            _extend_response(output, await _join_channel(channel_name, context, services), response_limit)
        elif packet_type is ClientPacket.CHANNEL_PART:
            channel_name = packet.payload.read_string()
            packet.payload.require_exhausted()
            _extend_response(output, await _part_channel(channel_name, context, services), response_limit)
        elif packet_type is ClientPacket.SEND_PUBLIC_MESSAGE:
            message = packet.payload.read_message()
            packet.payload.require_exhausted()
            _extend_response(output, await _send_public_message(message, context, services), response_limit)
        elif packet_type is ClientPacket.SEND_PRIVATE_MESSAGE:
            message = packet.payload.read_message()
            packet.payload.require_exhausted()
            _extend_response(output, await _send_private_message(message, context, services), response_limit)
        elif packet_type is ClientPacket.SET_AWAY_MESSAGE:
            message = packet.payload.read_message()
            packet.payload.require_exhausted()
            away_text = message.text.strip()
            await services.realtime.set_away_message(
                context.identity.account_id,
                session_id=context.identity.session_id,
                expected_revision=context.realtime.revision,
                message=away_text,
            )
            log_event(
                "DEBUG",
                "stable.message.away_state",
                outcome="updated",
                account_id=context.identity.account_id,
                message_length=len(away_text),
            )
        elif packet_type is ClientPacket.TOGGLE_BLOCK_NON_FRIEND_DMS:
            value = packet.payload.read_i32()
            packet.payload.require_exhausted()
            if value not in {0, 1}:
                raise ValueError("invalid Stable private-message policy value")
            if services.community is not None:
                try:
                    await services.community.set_private_message_policy(
                        context.identity.account_id,
                        "friends" if value == 1 else "all",
                    )
                except ApplicationError:
                    _extend_response(
                        output,
                        notification("The private-message policy could not be updated."),
                        response_limit,
                    )
        elif packet_type is ClientPacket.FRIEND_ADD:
            target_id = packet.payload.read_i32()
            packet.payload.require_exhausted()
            _extend_response(
                output,
                await _change_friend(
                    target_id,
                    adding=True,
                    context=context,
                    services=services,
                    max_bytes=response_limit - len(output),
                ),
                response_limit,
            )
        elif packet_type is ClientPacket.FRIEND_REMOVE:
            target_id = packet.payload.read_i32()
            packet.payload.require_exhausted()
            _extend_response(
                output,
                await _change_friend(
                    target_id,
                    adding=False,
                    context=context,
                    services=services,
                    max_bytes=response_limit - len(output),
                ),
                response_limit,
            )
        elif packet_type is ClientPacket.LOGOUT:
            packet.payload.read_i32()
            packet.payload.require_exhausted()
            if _is_spurious_logout(context, services):
                log_event(
                    "DEBUG",
                    "stable.logout.ignored",
                    outcome="spurious",
                    account_id=context.identity.account_id,
                )
                continue
            context.session_closed = True
            _extend_response(output, await _logout(context, services), response_limit)
            break
        elif packet_type in {ClientPacket.ERROR_REPORT, ClientPacket.IRC_ONLY, ClientPacket.BEATMAP_INFO_REQUEST}:
            packet.payload.read_remaining()
        else:
            _ignore_unsupported(packet)
    return bytes(output)


async def _requested_packets(
    account_ids: tuple[int, ...],
    packet_type: ServerPacket,
    requester_account_id: int,
    services: StableServices,
    *,
    max_bytes: int,
) -> bytes:
    output = bytearray()
    now = services.clock.now()
    for account_id in dict.fromkeys(account_ids):
        if len(output) >= max_bytes:
            break
        if account_id < 1 or account_id == requester_account_id:
            continue
        snapshot = await services.realtime.get_presence(account_id, at=now)
        if snapshot is None:
            continue
        wire = _packet_from_snapshot(snapshot, packet_type)
        if wire and not _extend_response(output, wire, max_bytes):
            break
    return bytes(output)


async def _all_presence_packets(services: StableServices, *, max_bytes: int) -> bytes:
    snapshots = await services.realtime.list_presences(
        at=services.clock.now(),
        limit=services.settings.stable_presence_batch_size,
    )
    output = bytearray()
    for snapshot in snapshots:
        wire = _packet_from_snapshot(snapshot, ServerPacket.USER_PRESENCE)
        if wire and not _extend_response(output, wire, max_bytes):
            break
    return bytes(output)


def _packet_from_snapshot(snapshot: PresenceSnapshot, packet_type: ServerPacket) -> bytes:
    for packet in PacketReader(snapshot.payload, packet_enum=ServerPacket):
        if packet.packet_type is packet_type:
            return build_packet(packet.packet_id, packet.payload_view)
    return b""


async def broadcast_presence_update(payload: bytes, account_id: int, services: StableServices) -> None:
    """Fan out one presence change according to each online recipient's filter."""
    snapshots = await services.realtime.list_presences(
        at=services.clock.now(),
        limit=services.settings.stable_presence_batch_size,
    )
    candidate_account_ids = tuple(snapshot.account_id for snapshot in snapshots if snapshot.account_id != account_id)
    followers = (
        await services.social.list_incoming_follower_account_ids(account_id, candidate_account_ids)
        if services.social is not None and candidate_account_ids
        else frozenset()
    )

    async def enqueue_snapshot(snapshot: PresenceSnapshot) -> BaseException | None:
        if snapshot.account_id == account_id:
            return None
        update_filter = await services.realtime.get_presence_filter(snapshot.account_id)
        if update_filter != 1 and not (update_filter == 2 and snapshot.account_id in followers):
            return None
        try:
            await services.realtime.enqueue_mailbox(
                snapshot.account_id,
                payload,
                recipient_fence=snapshot.fence,
                expires_at=snapshot.expires_at,
            )
        except (MailboxOverflow, RealtimeSessionFenced, RealtimeSessionNotFound) as error:
            return error
        return None

    errors = tuple(
        error
        for error in await _bounded_gather(
            snapshots,
            enqueue_snapshot,
            limit=services.settings.stable_presence_fanout_concurrency,
        )
        if error is not None
    )
    failure_count = len(errors)
    representative_error = errors[0] if errors else None
    if representative_error is not None and rate_limit("stable-presence-broadcast-failed", interval_seconds=5):
        log_event(
            "WARNING",
            "stable.presence.broadcast_failed",
            exception=representative_error,
            account_id=account_id,
            failure_count=failure_count,
        )


def _extend_response(output: bytearray, payload: bytes, maximum: int) -> bool:
    """Append only complete Stable packets within one poll's local response budget."""
    remaining = maximum - len(output)
    if remaining <= 0:
        return False
    if len(payload) <= remaining:
        output.extend(payload)
        return True
    for packet in PacketReader(payload, packet_enum=ServerPacket):
        wire = build_packet(packet.packet_id, packet.payload_view)
        if len(wire) > maximum - len(output):
            return False
        output.extend(wire)
    return True


async def _bounded_gather[T, R](
    values: Sequence[T],
    operation: Callable[[T], Awaitable[R]],
    *,
    limit: int,
) -> tuple[R, ...]:
    semaphore = asyncio.Semaphore(limit)

    async def run(value: T) -> R:
        async with semaphore:
            return await operation(value)

    return tuple(await asyncio.gather(*(run(value) for value in values)))


async def _store_presence(
    presence: UserPresence,
    stats: UserStats,
    context: StableRuntimeContext,
    services: StableServices,
) -> None:
    await services.realtime.set_presence(
        PresenceSnapshot(
            account_id=context.identity.account_id,
            revision=context.realtime.revision,
            payload=user_presence(presence) + user_stats(stats),
            expires_at=context.realtime.expires_at,
            session_id=context.identity.session_id,
        ),
        session_id=context.identity.session_id,
    )


def _presence_from_stats(presence: UserPresence, stats: UserStats) -> UserPresence:
    return UserPresence(
        user_id=presence.user_id,
        username=presence.username,
        utc_offset=presence.utc_offset,
        country_code=presence.country_code,
        privileges=presence.privileges,
        mode=stats.mode,
        longitude=presence.longitude,
        latitude=presence.latitude,
        global_rank=stats.global_rank,
    )


def _normalize_status_mods(status: ClientStatus) -> ClientStatus:
    mods = status.mods
    if status.mode != 0:
        mods &= ~LEGACY_MOD_BITS["AP"]
    if status.mode == 3:
        mods &= ~LEGACY_MOD_BITS["RX"]
    if mods == status.mods:
        return status
    return ClientStatus(
        status.action,
        status.info_text,
        status.beatmap_md5,
        mods,
        status.mode,
        status.beatmap_id,
    )


def _stats_from_status(stats: UserStats, status: ClientStatus) -> UserStats:
    return UserStats(
        user_id=stats.user_id,
        action=status.action,
        info_text=status.info_text,
        beatmap_md5=status.beatmap_md5,
        mods=status.mods,
        mode=status.mode,
        beatmap_id=status.beatmap_id,
        ranked_score=stats.ranked_score,
        accuracy=stats.accuracy,
        play_count=stats.play_count,
        total_score=stats.total_score,
        global_rank=stats.global_rank,
        performance=stats.performance,
    )


def replace_stats_mode(stats: UserStats, *, mode: int, mods: int) -> UserStats:
    """Replace dimensions used by an authoritative statistics query."""
    return UserStats(
        stats.user_id,
        stats.action,
        stats.info_text,
        stats.beatmap_md5,
        mods,
        mode,
        stats.beatmap_id,
        stats.ranked_score,
        stats.accuracy,
        stats.play_count,
        stats.total_score,
        stats.global_rank,
        stats.performance,
    )


async def account_stats(stats: UserStats, services: StableServices) -> UserStats:
    """Overlay canonical score totals, Performance, and rank for the current mode."""
    if (services.account_statistics is None and services.ranking_query is None) or not 0 <= stats.mode <= 3:
        return stats
    try:
        _, variant = parse_legacy_mods(stats.mods)
    except ValueError:
        return stats
    if services.account_statistics is not None:
        view = await services.account_statistics.get_for_display(stats.user_id, tuple(Ruleset)[stats.mode], variant)
    else:
        ranking_query = services.ranking_query
        assert ranking_query is not None
        view = await ranking_query.get_account_stats(stats.user_id, tuple(Ruleset)[stats.mode], variant)
    return UserStats(
        stats.user_id,
        stats.action,
        stats.info_text,
        stats.beatmap_md5,
        stats.mods,
        stats.mode,
        stats.beatmap_id,
        view.ranked_score,
        float(view.accuracy),
        view.play_count,
        view.total_score,
        view.global_rank,
        view.performance,
    )


async def _join_channel(name: str, context: StableRuntimeContext, services: StableServices) -> bytes:
    if _is_multiplayer_channel(name):
        if services.multiplayer is None:
            return channel_kick("#multiplayer")
        state = await services.multiplayer.find_room_for_account(context.identity.account_id)
        return channel_join("#multiplayer") if state is not None else channel_kick("#multiplayer")
    if services.community is None:
        return b""
    try:
        channel = await services.community.get_public_channel_by_stable_name(context.identity.account_id, name)
        if _is_lobby_channel(name):
            members = await services.realtime.list_channel_members(channel.channel_id)
            if context.identity.account_id not in members:
                return notification("Use the multiplayer lobby to join #lobby.")
        else:
            await services.realtime.join_channel(
                channel.channel_id,
                session_id=context.identity.session_id,
                expected_revision=context.realtime.revision,
            )
            members = await services.realtime.list_channel_members(channel.channel_id)
        count = await services.community.get_channel_member_count(
            context.identity.account_id,
            channel.channel_id,
        )
    except ApplicationError as error:
        return _channel_error_response(error)
    info = channel_info(Channel(channel.name, channel.topic, min(count, 0xFFFF)))
    await _broadcast_channel_count(info, members, context.identity.account_id, services)
    return channel_join(channel.name) + info


async def _part_channel(name: str, context: StableRuntimeContext, services: StableServices) -> bytes:
    if services.community is None:
        return b""
    if _is_lobby_channel(name) or _is_multiplayer_channel(name):
        return b""
    try:
        channel = await services.community.get_public_channel_by_stable_name(context.identity.account_id, name)
    except ChannelNotFound:
        return b""
    try:
        await services.realtime.leave_channel(
            channel.channel_id,
            session_id=context.identity.session_id,
            expected_revision=context.realtime.revision,
        )
        count = await services.community.get_channel_member_count(
            context.identity.account_id,
            channel.channel_id,
        )
        members = await services.realtime.list_channel_members(channel.channel_id)
    except ApplicationError as error:
        return _channel_error_response(error)
    info = channel_info(Channel(channel.name, channel.topic, min(count, 0xFFFF)))
    await _broadcast_channel_count(info, members, context.identity.account_id, services)
    return channel_kick(channel.name)


async def _send_public_message(message: Message, context: StableRuntimeContext, services: StableServices) -> bytes:
    content = message.text.strip()
    if not content:
        _log_message_state("public", "empty", context.identity.account_id, message_length=0)
        return b""
    if _is_multiplayer_channel(message.recipient):
        return await _send_multiplayer_message(content, context, services)
    if services.community is None:
        return b""
    try:
        result = await services.community.send_public_message(
            context.identity.account_id,
            message.recipient,
            _stable_message_id("public", message.recipient, content, context, services),
            content,
        )
        channel = result.resolved_channel
        if channel is None:
            community_query = services.community_query
            if community_query is not None:
                channel = await community_query.get_public_channel_by_stable_name(
                    context.identity.account_id, message.recipient
                )
            else:
                channel = await services.community.get_public_channel_by_stable_name(
                    context.identity.account_id, message.recipient
                )
        channel_member_ids = tuple(sorted(await services.realtime.list_channel_members(channel.channel_id)))
        member_ids = tuple(account_id for account_id in channel_member_ids if account_id != context.identity.account_id)
        if services.social is not None:
            member_ids = await services.social.filter_message_recipients(
                context.identity.account_id,
                member_ids,
            )
    except ApplicationError as error:
        _log_message_state(
            "public",
            "rejected",
            context.identity.account_id,
            message_length=len(content),
            error=error,
        )
        return _public_message_error_response(error)
    if not result.created:
        _log_message_state(
            "public",
            "duplicate",
            context.identity.account_id,
            message_length=len(content),
        )
        return await _execute_bot_command(
            content,
            channel.name,
            private=False,
            context=context,
            services=services,
            public_recipient_ids=channel_member_ids,
        )
    wire = send_message(
        Message(
            sender=context.identity.current_name,
            text=result.content,
            recipient=channel.name,
            sender_id=context.identity.account_id,
        )
    )
    delivered_count = 0
    for account_id in member_ids:
        delivered_count += int(await _enqueue_online_recipient(account_id, wire, services))
    _log_message_state(
        "public",
        "persisted",
        context.identity.account_id,
        message_length=len(content),
        recipient_count=len(member_ids),
        delivered_count=delivered_count,
    )
    return await _execute_bot_command(
        content,
        channel.name,
        private=False,
        context=context,
        services=services,
        public_recipient_ids=channel_member_ids,
    )


async def _send_multiplayer_message(
    content: str,
    context: StableRuntimeContext,
    services: StableServices,
) -> bytes:
    if services.multiplayer is None:
        return channel_kick("#multiplayer")
    if len(content) > 2000:
        _log_message_state("multiplayer", "rejected", context.identity.account_id, message_length=len(content))
        return notification("The message is invalid.")
    if services.community is not None:
        silence_remaining = await services.community.get_global_silence_remaining_seconds(context.identity.account_id)
        if silence_remaining > 0:
            _log_message_state("multiplayer", "silenced", context.identity.account_id, message_length=len(content))
            return notification("You cannot send messages while silenced.")
    try:
        state = await services.multiplayer.find_room_for_account(context.identity.account_id)
    except ApplicationError as error:
        _log_message_state(
            "multiplayer",
            "rejected",
            context.identity.account_id,
            message_length=len(content),
            error=error,
        )
        return notification("The multiplayer channel is unavailable.")
    if state is None:
        _log_message_state("multiplayer", "not_joined", context.identity.account_id, message_length=len(content))
        return channel_kick("#multiplayer")

    recipient_ids = tuple(
        account_id
        for account_id in sorted({slot.account_id for slot in state.slots if slot.account_id is not None})
        if account_id != context.identity.account_id
    )
    if services.social is not None:
        recipient_ids = await services.social.filter_message_recipients(
            context.identity.account_id,
            recipient_ids,
        )
    wire = send_message(
        Message(
            sender=context.identity.current_name,
            text=content,
            recipient="#multiplayer",
            sender_id=context.identity.account_id,
        )
    )
    delivered_count = 0
    for account_id in recipient_ids:
        delivered_count += int(await _enqueue_online_recipient(account_id, wire, services))
    _log_message_state(
        "multiplayer",
        "delivered",
        context.identity.account_id,
        message_length=len(content),
        recipient_count=len(recipient_ids),
        delivered_count=delivered_count,
    )
    return await _execute_bot_command(
        content,
        "#multiplayer",
        private=False,
        context=context,
        services=services,
        public_recipient_ids=(context.identity.account_id, *recipient_ids),
    )


async def _send_private_message(message: Message, context: StableRuntimeContext, services: StableServices) -> bytes:
    if services.community is None or services.social is None:
        return b""

    content = message.text.strip()
    if not content:
        _log_message_state("direct", "empty", context.identity.account_id, message_length=0)
        return b""
    try:
        target = await services.social.resolve_account_by_name(message.recipient)
        result = await services.community.send_direct_message(
            context.identity.account_id,
            target.account_id,
            _stable_message_id("direct", str(target.account_id), content, context, services),
            content,
        )
    except SocialAccountNotFound as error:
        _log_message_state(
            "direct",
            "recipient_not_found",
            context.identity.account_id,
            message_length=len(content),
            error=error,
        )
        return notification("The direct-message recipient does not exist.")
    except (DirectMessageBlocked, PrivateMessageRejected) as error:
        _log_message_state(
            "direct",
            "blocked",
            context.identity.account_id,
            message_length=len(content),
            error=error,
        )
        return user_dm_blocked(message.recipient)
    except TargetAccountSilenced as error:
        _log_message_state(
            "direct",
            "target_silenced",
            context.identity.account_id,
            message_length=len(content),
            error=error,
        )
        return target_is_silenced(message.recipient)
    except AccountSilenced as error:
        _log_message_state(
            "direct",
            "sender_silenced",
            context.identity.account_id,
            message_length=len(content),
            error=error,
        )
        return notification("You cannot send messages while silenced.")
    except ApplicationError as error:
        _log_message_state(
            "direct",
            "rejected",
            context.identity.account_id,
            message_length=len(content),
            error=error,
        )
        return notification("The direct message could not be sent.")

    if services.bot is not None and target.account_id == services.bot.bot_account_id:
        return await _execute_bot_command(
            content,
            target.display_name,
            private=True,
            context=context,
            services=services,
        )
    if not result.created:
        _log_message_state(
            "direct",
            "duplicate",
            context.identity.account_id,
            message_length=len(content),
        )
        return b""
    wire = send_message(
        Message(
            sender=context.identity.current_name,
            text=result.content,
            recipient=target.display_name,
            sender_id=context.identity.account_id,
        )
    )
    target_presence = await services.realtime.get_presence(target.account_id, at=services.clock.now())
    if target_presence is None:
        _log_message_state(
            "direct",
            "persisted_offline",
            context.identity.account_id,
            message_length=len(content),
            recipient_count=1,
        )
        return notification(f"{target.display_name} is offline and will receive your message on their next login.")
    try:
        await services.realtime.enqueue_mailbox(
            target.account_id,
            wire,
            recipient_fence=target_presence.fence,
            expires_at=target_presence.expires_at,
        )
    except (MailboxOverflow, RealtimeSessionFenced, RealtimeSessionNotFound) as error:
        _log_message_state(
            "direct",
            "delivery_deferred",
            context.identity.account_id,
            message_length=len(content),
            recipient_count=1,
            error=error,
        )
        return notification("The recipient is temporarily unable to receive messages.")

    target_stats_packet = _packet_from_snapshot(target_presence, ServerPacket.USER_STATS)
    if target_stats_packet:
        packet = next(PacketReader(target_stats_packet, packet_enum=ServerPacket))
        target_stats = packet.payload.read_user_stats()
        if target_stats.action == 1:
            away_message = await services.realtime.get_away_message(target.account_id)
            if away_message:
                _log_message_state(
                    "direct",
                    "delivered_with_away_reply",
                    context.identity.account_id,
                    message_length=len(content),
                    recipient_count=1,
                    delivered_count=1,
                    away_message_length=len(away_message),
                )
                return send_message(
                    Message(target.display_name, away_message, context.identity.current_name, target.account_id)
                )
    _log_message_state(
        "direct",
        "delivered",
        context.identity.account_id,
        message_length=len(content),
        recipient_count=1,
        delivered_count=1,
    )
    return b""


async def _execute_bot_command(
    content: str,
    recipient: str,
    *,
    private: bool,
    context: StableRuntimeContext,
    services: StableServices,
    public_recipient_ids: tuple[int, ...] = (),
) -> bytes:
    """Execute a command candidate and adapt its result to Stable packets."""
    if services.bot is None:
        return b""
    request_id = _stable_message_id("bot-command", recipient, content, context, services)
    invocation = BotInvocation(
        CommandMeta(
            request_id,
            f"bot:{context.identity.session_id}:{request_id}",
            hashlib.sha256(f"{recipient.casefold()}\0{content}".encode()).digest(),
            Actor(context.identity.account_id, context.identity.session_id),
            context.client
            or ClientContext(
                "stable",
                context.identity.client_version,
                context.identity.client_variant,
                "127.0.0.1",
                "osu!",
            ),
            services.clock.now(),
        ),
        context.identity.current_name,
        content,
        recipient,
        private,
    )
    result = await services.bot.try_execute(invocation)
    if result is None:
        return b""

    output = bytearray()
    if result.response:
        target = context.identity.current_name if private else recipient
        wire = send_message(
            Message(
                services.bot.bot_name,
                result.response[:2000],
                target,
                services.bot.bot_account_id,
            )
        )
        _extend_response(output, wire, services.settings.stable_max_response_bytes)
        if not private:
            for account_id in public_recipient_ids:
                if account_id != context.identity.account_id:
                    await _enqueue_online_recipient(account_id, wire, services)

    if result.directive is BotDirective.RECONNECT:
        _extend_response(output, restart(0), services.settings.stable_max_response_bytes)
    elif result.directive is BotDirective.QUIT:
        context.session_closed = True
        _extend_response(output, await _logout(context, services), services.settings.stable_max_response_bytes)

    if isinstance(result.effect, MultiplayerMutationResult):
        _extend_response(
            output,
            await dispatch_multiplayer_mutation(result.effect, context.identity.account_id, services),
            services.settings.stable_max_response_bytes,
        )

    log_event(
        "DEBUG",
        "bot.command.executed",
        outcome="success",
        account_id=context.identity.account_id,
        private=private,
        response_length=len(result.response or ""),
        directive=result.directive.value if result.directive is not None else None,
        duration_ms=result.execution_time_ms,
    )
    return bytes(output)


def _log_message_state(
    message_kind: str,
    outcome: str,
    account_id: int,
    *,
    message_length: int,
    recipient_count: int = 0,
    delivered_count: int = 0,
    away_message_length: int = 0,
    error: ApplicationError | None = None,
) -> None:
    if error is None:
        if not sampled(monotonic_ns(), settings.log_hot_path_sample_rate):
            return
        level = "DEBUG"
    else:
        if not rate_limit(f"stable-message-rejected:{error.code}", interval_seconds=5):
            return
        level = "INFO"
    if error is None:
        log_event(
            level,
            "stable.message.state",
            message_kind=message_kind,
            outcome=outcome,
            account_id=account_id,
            message_length=message_length,
            recipient_count=recipient_count,
            delivered_count=delivered_count,
            away_message_length=away_message_length,
            error_code=None,
            error_type=None,
        )
    else:
        log_event(
            level,
            "stable.message.state",
            exception=error,
            message_kind=message_kind,
            outcome=outcome,
            account_id=account_id,
            message_length=message_length,
            recipient_count=recipient_count,
            delivered_count=delivered_count,
            away_message_length=away_message_length,
            error_code=error.code,
            error_type=type(error).__name__,
        )


def _stable_message_id(
    kind: str,
    recipient: str,
    content: str,
    context: StableRuntimeContext,
    services: StableServices,
) -> uuid.UUID:
    bucket = int(services.clock.now().timestamp()) // _MESSAGE_ID_WINDOW_SECONDS
    material = "\0".join(
        (
            kind,
            str(context.identity.session_id),
            str(context.identity.account_id),
            recipient.strip().casefold(),
            content,
            str(bucket),
        )
    ).encode()
    return uuid.UUID(bytes=hashlib.sha256(material).digest()[:16], version=5)


def _is_lobby_channel(name: str) -> bool:
    return name.strip().casefold() in {"lobby", "#lobby"}


def _is_multiplayer_channel(name: str) -> bool:
    return name.strip().casefold() in {"multiplayer", "#multiplayer"}


def _channel_error_response(error: ApplicationError) -> bytes:
    if isinstance(error, ChannelNotFound):
        return notification("Channel is unavailable.")
    if isinstance(error, (ChannelAccessDenied, ChannelMembershipUnavailable)):
        return notification("The channel cannot be joined right now.")
    return notification("The channel request could not be completed.")


def _public_message_error_response(error: ApplicationError) -> bytes:
    if isinstance(error, AccountSilenced):
        return notification("You cannot send messages while silenced.")
    if isinstance(error, ChannelNotFound):
        return notification("Channel is unavailable.")
    if isinstance(error, ChannelMembershipRequired):
        return notification("Join the channel before sending messages.")
    if isinstance(error, ChannelAccessDenied):
        return notification("You are not allowed to send messages to this channel.")
    if isinstance(error, CommunityInputRejected):
        return notification("The message is invalid.")
    if isinstance(error, MessageIdempotencyConflict):
        return notification("The message could not be retried safely.")
    return notification("The message could not be sent.")


async def _broadcast_channel_count(
    payload: bytes,
    member_ids: frozenset[int],
    excluded_account_id: int,
    services: StableServices,
) -> None:
    for account_id in member_ids:
        if account_id != excluded_account_id:
            await _enqueue_online_recipient(account_id, payload, services)


async def _enqueue_online_recipient(account_id: int, payload: bytes, services: StableServices) -> bool:
    presence = await services.realtime.get_presence(account_id, at=services.clock.now())
    if presence is None:
        return False
    try:
        await services.realtime.enqueue_mailbox(
            account_id,
            payload,
            recipient_fence=presence.fence,
            expires_at=presence.expires_at,
        )
    except (MailboxOverflow, RealtimeSessionFenced, RealtimeSessionNotFound) as error:
        if rate_limit("stable-message-delivery-failed", interval_seconds=5):
            log_event(
                "WARNING",
                "stable.message.delivery_failed",
                exception=error,
                outcome="deferred",
                account_id=account_id,
                failure_count=1,
            )
        return False
    return True


async def _logout(context: StableRuntimeContext, services: StableServices) -> bytes:
    started_ns = monotonic_ns()
    now = services.clock.now()
    output = bytearray()
    multiplayer_outcome = "not_configured"
    durable_session_outcome = "not_available"
    realtime_outcome = "fenced"
    snapshots = await services.realtime.list_presences(
        at=now,
        limit=services.settings.stable_presence_batch_size,
    )
    await _stop_spectating(context, services)
    if services.multiplayer is not None:
        multiplayer_outcome = "cleaned"
        try:
            previous = await services.multiplayer.find_room_for_account(context.identity.account_id)
            digest = hashlib.sha256(f"logout:{context.identity.session_id}".encode()).digest()
            state = await services.multiplayer.cleanup_presence(
                CleanupPresence(
                    CommandMeta(
                        services.id_generator.new(),
                        f"stable-multiplayer:logout:{context.identity.session_id}",
                        digest,
                        Actor(context.identity.account_id, context.identity.session_id),
                        context.client
                        or ClientContext(
                            "stable",
                            context.identity.client_version,
                            context.identity.client_variant,
                            "127.0.0.1",
                            "osu!",
                        ),
                        now,
                    ),
                    context.identity.account_id,
                    context.identity.session_id,
                    "client_logout",
                )
            )
            if previous is not None:
                if state is None:
                    await _broadcast_lobby(
                        dispose_match(previous.room.public_id),
                        context.identity.account_id,
                        services,
                    )
                else:
                    await _broadcast_state(state, context.identity.account_id, services)
                    if (
                        previous.room.host_account_id == context.identity.account_id
                        and state.room.host_account_id != context.identity.account_id
                    ):
                        await _enqueue(
                            state.room.host_account_id,
                            match_transfer_host(),
                            state,
                            services,
                        )
        except ApplicationError as error:
            multiplayer_outcome = "failed"
            log_event(
                "WARNING",
                "stable.logout.cleanup_failed",
                exception=error,
                operation="multiplayer_presence",
                account_id=context.identity.account_id,
                error_code=error.code,
                error_type=type(error).__name__,
            )
            output.extend(notification("Multiplayer presence cleanup could not be completed."))
    if context.raw_token is not None:
        durable_session_outcome = "closed"
        try:
            await services.identity.close_stable_session(context.raw_token, reason="client_logout")
        except ApplicationError as error:
            durable_session_outcome = "failed"
            log_event(
                "WARNING",
                "stable.logout.cleanup_failed",
                exception=error,
                operation="close_durable_session",
                account_id=context.identity.account_id,
                error_code=error.code,
                error_type=type(error).__name__,
            )
            output.extend(notification("The durable session could not be closed cleanly."))
    try:
        await services.realtime.fence_session(
            context.identity.session_id,
            expected_revision=context.realtime.revision,
        )
    except (RealtimeSessionNotFound, RealtimeSessionFenced) as error:
        realtime_outcome = "already_fenced"
        log_event(
            "DEBUG",
            "stable.logout.cleanup_state",
            exception=error,
            operation="fence_realtime_session",
            outcome=realtime_outcome,
            account_id=context.identity.account_id,
            error_code=error.code,
            error_type=type(error).__name__,
        )
    wire = user_logout(context.identity.account_id)
    recipient_count = 0
    delivery_failure_count = 0
    delivery_error: BaseException | None = None
    for snapshot in snapshots:
        if snapshot.account_id != context.identity.account_id:
            recipient_count += 1
            try:
                await services.realtime.enqueue_mailbox(
                    snapshot.account_id,
                    wire,
                    recipient_fence=snapshot.fence,
                    expires_at=snapshot.expires_at,
                )
            except (MailboxOverflow, RealtimeSessionFenced, RealtimeSessionNotFound) as error:
                delivery_failure_count += 1
                delivery_error = delivery_error or error
    log_event(
        "INFO",
        "stable.logout.completed",
        exception=delivery_error,
        outcome="completed",
        account_id=context.identity.account_id,
        multiplayer_outcome=multiplayer_outcome,
        durable_session_outcome=durable_session_outcome,
        realtime_outcome=realtime_outcome,
        recipient_count=recipient_count,
        delivery_failure_count=delivery_failure_count,
        duration_ms=duration_ms(started_ns),
    )
    return bytes(output)


def _is_spurious_logout(context: StableRuntimeContext, services: StableServices) -> bool:
    opened_at = context.identity.opened_at
    return opened_at is not None and services.clock.now() < opened_at + timedelta(seconds=1)


async def _change_friend(
    target_id: int,
    *,
    adding: bool,
    context: StableRuntimeContext,
    services: StableServices,
    max_bytes: int,
) -> bytes:
    if services.social is None or target_id < 1:
        return b""
    try:
        if adding:
            await services.social.follow(context.identity.account_id, target_id)
        else:
            await services.social.unfollow(context.identity.account_id, target_id)
    except SocialAccountNotFound, SocialInteractionBlocked, SocialRelationRejected:
        pass
    except ApplicationError:
        return notification("The friend list could not be updated.")
    try:
        friends = await services.social.list_friends(context.identity.account_id)
    except ApplicationError:
        return notification("The friend list could not be loaded.")
    capacity = min(
        services.settings.stable_presence_batch_size,
        max(0, (max_bytes - 9) // 4),
    )
    if capacity == 0:
        return b""
    account_ids = [1]
    for friend in friends:
        if friend.account_id not in account_ids:
            account_ids.append(friend.account_id)
            if len(account_ids) >= capacity:
                break
    return friends_list(tuple(account_ids))


async def _start_spectating(
    host_account_id: int,
    context: StableRuntimeContext,
    services: StableServices,
    *,
    max_bytes: int,
) -> bytes:
    if host_account_id < 1 or host_account_id == context.identity.account_id:
        log_event(
            "INFO",
            "stable.spectator.attach",
            outcome="invalid_target",
            spectator_account_id=context.identity.account_id,
        )
        return b""
    now = services.clock.now()
    host_presence = await services.realtime.get_presence(host_account_id, at=now)
    if host_presence is None:
        log_event(
            "INFO",
            "stable.spectator.attach",
            outcome="host_offline",
            host_account_id=host_account_id,
            spectator_account_id=context.identity.account_id,
        )
        return b""
    try:
        current = await services.realtime.get_spectator_relation(
            context.identity.account_id,
            spectator_fence=context.realtime.fence,
            at=now,
        )
        if current is not None and current.host_account_id == host_account_id:
            log_event(
                "DEBUG",
                "stable.spectator.attach",
                outcome="already_attached",
                host_account_id=host_account_id,
                spectator_account_id=context.identity.account_id,
            )
            return b""
        if current is not None:
            await _detach_spectator(current, services, at=now)
        existing = await services.realtime.list_spectators(
            host_account_id,
            host_fence=host_presence.fence,
            at=now,
        )
    except (RealtimeSessionFenced, RealtimeSessionNotFound) as error:
        log_event(
            "INFO",
            "stable.spectator.attach",
            exception=error,
            outcome="realtime_fenced",
            host_account_id=host_account_id,
            spectator_account_id=context.identity.account_id,
            error_code=error.code,
            error_type=type(error).__name__,
        )
        return b""
    expiry = min(context.realtime.expires_at, host_presence.expires_at)
    try:
        attachment = await services.realtime.attach_spectator(
            host_account_id,
            context.identity.account_id,
            relation_id=services.id_generator.new(),
            host_fence=host_presence.fence,
            spectator_fence=context.realtime.fence,
            expires_at=expiry,
            history_limit=services.settings.stable_spectator_frame_batch_size,
        )
    except (SpectatorHostOffline, RealtimeSessionFenced, RealtimeSessionNotFound) as error:
        log_event(
            "INFO",
            "stable.spectator.attach",
            exception=error,
            outcome="rejected",
            host_account_id=host_account_id,
            spectator_account_id=context.identity.account_id,
            error_code=error.code,
            error_type=type(error).__name__,
        )
        return b""
    relation = attachment.relation
    delivery_failure_count = int(
        not await _enqueue_spectator_packet(
            host_account_id,
            spectator_joined(context.identity.account_id),
            relation.host_fence,
            relation.expires_at,
            services,
        )
    )
    output = bytearray()
    fellow_count = 0
    for existing_relation in existing:
        if existing_relation.spectator_account_id == context.identity.account_id:
            continue
        fellow_count += 1
        _extend_response(
            output,
            fellow_spectator_joined(existing_relation.spectator_account_id),
            max_bytes,
        )
        delivered = await _enqueue_spectator_packet(
            existing_relation.spectator_account_id,
            fellow_spectator_joined(context.identity.account_id),
            existing_relation.spectator_fence,
            min(relation.expires_at, existing_relation.expires_at),
            services,
        )
        delivery_failure_count += int(not delivered)
    history_frame_count = 0
    for frame in attachment.history.frames:
        if not _extend_response(output, frame.payload, max_bytes):
            break
        history_frame_count += 1
    log_event(
        "INFO",
        "stable.spectator.attach",
        outcome="attached",
        host_account_id=host_account_id,
        spectator_account_id=context.identity.account_id,
        fellow_count=fellow_count,
        history_frame_count=history_frame_count,
        delivery_failure_count=delivery_failure_count,
    )
    return bytes(output)


async def _stop_spectating(context: StableRuntimeContext, services: StableServices) -> None:
    relation = await services.realtime.get_spectator_relation(
        context.identity.account_id,
        spectator_fence=context.realtime.fence,
        at=services.clock.now(),
    )
    if relation is not None:
        await _detach_spectator(relation, services, at=services.clock.now())


async def _detach_spectator(
    relation: SpectatorRelation,
    services: StableServices,
    *,
    at: datetime,
) -> None:
    try:
        spectators = await services.realtime.list_spectators(
            relation.host_account_id,
            host_fence=relation.host_fence,
            at=at,
        )
    except (RealtimeSessionFenced, RealtimeSessionNotFound) as error:
        spectators = ()
        _log_spectator_delivery_failure("list_spectators", error, host_account_id=relation.host_account_id)
    detached = await services.realtime.detach_spectator(
        relation.host_account_id,
        relation.spectator_account_id,
        relation_id=relation.relation_id,
        expected_revision=relation.revision,
        host_fence=relation.host_fence,
        spectator_fence=relation.spectator_fence,
    )
    if not detached:
        log_event(
            "DEBUG",
            "stable.spectator.detach",
            outcome="stale_relation",
            host_account_id=relation.host_account_id,
            spectator_account_id=relation.spectator_account_id,
        )
        return
    delivery_failure_count = int(
        not await _enqueue_spectator_packet(
            relation.host_account_id,
            spectator_left(relation.spectator_account_id),
            relation.host_fence,
            relation.expires_at,
            services,
        )
    )
    wire = fellow_spectator_left(relation.spectator_account_id)
    fellow_count = 0
    for spectator in spectators:
        if spectator.spectator_account_id != relation.spectator_account_id:
            fellow_count += 1
            delivered = await _enqueue_spectator_packet(
                spectator.spectator_account_id,
                wire,
                spectator.spectator_fence,
                min(relation.expires_at, spectator.expires_at),
                services,
            )
            delivery_failure_count += int(not delivered)
    log_event(
        "INFO",
        "stable.spectator.detach",
        outcome="detached",
        host_account_id=relation.host_account_id,
        spectator_account_id=relation.spectator_account_id,
        fellow_count=fellow_count,
        delivery_failure_count=delivery_failure_count,
    )


async def _publish_spectator_frames(
    sequence: int,
    raw_data: memoryview,
    context: StableRuntimeContext,
    services: StableServices,
    *,
    reset_sequence: bool,
) -> None:
    started_ns = monotonic_ns()
    wire = spectate_frames(raw_data)
    try:
        result = await services.realtime.publish_spectator_frame(
            context.identity.account_id,
            host_fence=context.realtime.fence,
            sequence=sequence,
            reset_sequence=reset_sequence,
            payload=wire,
            expires_at=context.realtime.expires_at,
        )
    except (InvalidFrame, SpectatorHostOffline, RealtimeSessionFenced, RealtimeSessionNotFound) as error:
        if sampled((started_ns, sequence, "spectator_frame"), services.settings.log_hot_path_sample_rate):
            log_event(
                "DEBUG",
                "stable.spectator.frame_summary",
                exception=error,
                outcome="rejected",
                host_account_id=context.identity.account_id,
                frame_bytes=len(raw_data),
                recipient_count=0,
                error_code=error.code,
                error_type=type(error).__name__,
                duration_ms=duration_ms(started_ns),
            )
        return
    if sampled((started_ns, sequence, "spectator_frame"), services.settings.log_hot_path_sample_rate):
        log_event(
            "DEBUG",
            "stable.spectator.frame_summary",
            outcome="published",
            host_account_id=context.identity.account_id,
            frame_bytes=len(raw_data),
            recipient_count=len(result.recipient_account_ids),
            duration_ms=duration_ms(started_ns),
        )


async def _cant_spectate(context: StableRuntimeContext, services: StableServices) -> None:
    now = services.clock.now()
    relation = await services.realtime.get_spectator_relation(
        context.identity.account_id,
        spectator_fence=context.realtime.fence,
        at=now,
    )
    if relation is None:
        return
    wire = spectator_cant_spectate(context.identity.account_id)
    try:
        recipients = await services.realtime.list_spectators(
            relation.host_account_id,
            host_fence=relation.host_fence,
            at=now,
        )
    except (RealtimeSessionFenced, RealtimeSessionNotFound) as error:
        recipients = ()
        _log_spectator_delivery_failure("list_spectators", error, host_account_id=relation.host_account_id)
    await _enqueue_spectator_packet(
        relation.host_account_id,
        wire,
        relation.host_fence,
        relation.expires_at,
        services,
    )
    for recipient in recipients:
        await _enqueue_spectator_packet(
            recipient.spectator_account_id,
            wire,
            recipient.spectator_fence,
            recipient.expires_at,
            services,
        )


async def _enqueue_spectator_packet(
    account_id: int,
    payload: bytes,
    recipient_fence: SessionFence,
    expires_at: datetime,
    services: StableServices,
) -> bool:
    try:
        await services.realtime.enqueue_mailbox(
            account_id,
            payload,
            recipient_fence=recipient_fence,
            expires_at=expires_at,
        )
    except (MailboxOverflow, RealtimeSessionFenced, RealtimeSessionNotFound) as error:
        _log_spectator_delivery_failure("enqueue_mailbox", error, account_id=account_id)
        return False
    return True


def _log_spectator_delivery_failure(
    operation: str,
    error: BaseException,
    *,
    account_id: int | None = None,
    host_account_id: int | None = None,
) -> None:
    if not rate_limit(f"stable-spectator-delivery-failed:{operation}:{type(error).__name__}", interval_seconds=5):
        return
    log_event(
        "WARNING",
        "stable.spectator.delivery_failed",
        exception=error,
        operation=operation,
        account_id=account_id,
        host_account_id=host_account_id,
        failure_count=1,
    )


def _ignore_unsupported(packet: Packet) -> None:
    # PacketReader already isolated the payload, so skipping cannot desynchronise the poll.
    del packet


def realtime_expiry(context: ResolvedStableSession, services: StableServices) -> datetime:
    """Bound Redis state by both durable session expiry and configured online TTL."""
    online_expiry = services.clock.now() + timedelta(seconds=services.settings.redis_session_ttl_seconds)
    return min(context.expires_at, online_expiry)
