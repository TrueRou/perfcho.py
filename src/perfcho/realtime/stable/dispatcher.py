"""Dispatch core Stable packets over canonical identity and realtime services."""

import hashlib
import uuid
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from perfcho.composition import StableServices
from perfcho.modules.common import Actor, ClientContext, CommandMeta
from perfcho.modules.common.errors import ApplicationError
from perfcho.modules.community import (
    AccountSilenced,
    ChannelAccessDenied,
    ChannelMembershipUnavailable,
    ChannelNotFound,
    CommunityInputRejected,
    DirectMessageBlocked,
    MessageIdempotencyConflict,
    PrivateMessageRejected,
    TargetAccountSilenced,
)
from perfcho.modules.identity import ResolvedStableSession
from perfcho.modules.multiplayer import CleanupPresence
from perfcho.modules.realtime import (
    InvalidFrame,
    MailboxOverflow,
    PresenceSnapshot,
    RealtimeSession,
    RealtimeSessionFenced,
    RealtimeSessionNotFound,
    SessionFence,
    SpectatorHostOffline,
    SpectatorRelation,
)
from perfcho.modules.scoring import Ruleset
from perfcho.modules.scoring.mods import LEGACY_MOD_BITS, parse_legacy_mods
from perfcho.modules.social import SocialAccountNotFound, SocialInteractionBlocked, SocialRelationRejected
from perfcho.realtime.stable.builders import (
    channel_info,
    channel_join,
    channel_kick,
    dispose_match,
    fellow_spectator_joined,
    fellow_spectator_left,
    friends_list,
    match_transfer_host,
    notification,
    pong,
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
from perfcho.realtime.stable.codec import Packet, PacketReader, build_packet
from perfcho.realtime.stable.models import (
    Channel,
    ClientPacket,
    ClientStatus,
    Message,
    ServerPacket,
    UserPresence,
    UserStats,
)
from perfcho.realtime.stable.multiplayer import (
    MULTIPLAYER_PACKETS,
    _broadcast_lobby,
    _broadcast_state,
    _enqueue,
    dispatch_multiplayer_packet,
)

_MESSAGE_ID_WINDOW_SECONDS = 5


@dataclass(frozen=True, slots=True)
class StableRuntimeContext:
    """Carry current wire projections while dispatching one poll."""

    identity: ResolvedStableSession
    realtime: RealtimeSession
    presence: UserPresence
    stats: UserStats
    client: ClientContext | None = None
    raw_token: str | None = field(default=None, repr=False)


async def dispatch_packets(body: bytes, context: StableRuntimeContext, services: StableServices) -> bytes:
    """Map application failures into bounded Stable responses instead of JSON errors."""
    try:
        return await _dispatch_packets(body, context, services)
    except RealtimeSessionFenced, RealtimeSessionNotFound:
        output = bytearray()
        _extend_response(
            output,
            notification("Session state changed. Please reconnect.") + restart(0),
            services.settings.stable_max_response_bytes,
        )
        return bytes(output)
    except ApplicationError:
        output = bytearray()
        _extend_response(
            output,
            notification("The request could not be completed."),
            services.settings.stable_max_response_bytes,
        )
        return bytes(output)


async def _dispatch_packets(body: bytes, context: StableRuntimeContext, services: StableServices) -> bytes:
    """Process supported core packets sequentially and return request-local output."""
    output = bytearray()
    response_limit = services.settings.stable_max_response_bytes
    current_presence = context.presence
    current_stats = context.stats
    for packet in PacketReader(body):
        packet_type = packet.packet_type
        if packet_type is ClientPacket.PING:
            packet.payload.require_exhausted()
            _extend_response(output, pong(), response_limit)
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
            await _publish_spectator_frames(bundle.sequence, bundle.raw_data, context, services)
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
            await services.realtime.set_away_message(
                context.identity.account_id,
                session_id=context.identity.session_id,
                expected_revision=context.realtime.revision,
                message=message.text.strip(),
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
                continue
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
    followers = (
        await services.social.list_follower_account_ids(account_id) if services.social is not None else frozenset()
    )
    for snapshot in snapshots:
        if snapshot.account_id == account_id:
            continue
        update_filter = await services.realtime.get_presence_filter(snapshot.account_id)
        if update_filter == 1 or update_filter == 2 and snapshot.account_id in followers:
            with suppress(MailboxOverflow, RealtimeSessionFenced, RealtimeSessionNotFound):
                await services.realtime.enqueue_mailbox(
                    snapshot.account_id,
                    payload,
                    recipient_fence=snapshot.fence,
                    expires_at=snapshot.expires_at,
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
    """Overlay canonical score totals while leaving deferred Performance at zero."""
    if services.ranking_query is None or not 0 <= stats.mode <= 3:
        return stats
    try:
        _, variant = parse_legacy_mods(stats.mods)
    except ValueError:
        return stats
    view = await services.ranking_query.get_account_stats(stats.user_id, tuple(Ruleset)[stats.mode], variant)
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
    if services.community is None:
        return b""
    if _is_lobby_channel(name):
        return notification("Use the multiplayer lobby to join #lobby.")
    try:
        channel = await services.community.get_public_channel_by_stable_name(context.identity.account_id, name)
        await services.realtime.join_channel(
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
    return channel_join(channel.name) + info


async def _part_channel(name: str, context: StableRuntimeContext, services: StableServices) -> bytes:
    if services.community is None:
        return b""
    if _is_lobby_channel(name):
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
    if services.community is None:
        return b""
    content = message.text.strip()
    if not content:
        return b""
    try:
        result = await services.community.send_public_message(
            context.identity.account_id,
            message.recipient,
            _stable_message_id("public", message.recipient, content, context, services),
            content,
        )
        if not result.created:
            return b""
        channel = await services.community.get_public_channel_by_stable_name(
            context.identity.account_id,
            message.recipient,
        )
        member_ids = tuple(
            account_id
            for account_id in sorted(await services.realtime.list_channel_members(channel.channel_id))
            if account_id != context.identity.account_id
        )
        if services.social is not None:
            member_ids = await services.social.filter_message_recipients(
                context.identity.account_id,
                member_ids,
            )
    except ApplicationError as error:
        return _public_message_error_response(error)
    wire = send_message(
        Message(
            sender=context.identity.current_name,
            text=result.content,
            recipient=channel.name,
            sender_id=context.identity.account_id,
        )
    )
    for account_id in member_ids:
        await _enqueue_online_recipient(account_id, wire, services)
    return b""


async def _send_private_message(message: Message, context: StableRuntimeContext, services: StableServices) -> bytes:
    if services.community is None or services.social is None:
        return b""

    content = message.text.strip()
    if not content:
        return b""
    try:
        target = await services.social.resolve_account_by_name(message.recipient)
        result = await services.community.send_direct_message(
            context.identity.account_id,
            target.account_id,
            _stable_message_id("direct", str(target.account_id), content, context, services),
            content,
        )
    except SocialAccountNotFound:
        return notification("The direct-message recipient does not exist.")
    except DirectMessageBlocked, PrivateMessageRejected:
        return user_dm_blocked(message.recipient)
    except TargetAccountSilenced:
        return target_is_silenced(message.recipient)
    except AccountSilenced:
        return notification("You cannot send messages while silenced.")
    except ApplicationError:
        return notification("The direct message could not be sent.")

    if not result.created:
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
        return notification(f"{target.display_name} is offline and will receive your message on their next login.")
    try:
        await services.realtime.enqueue_mailbox(
            target.account_id,
            wire,
            recipient_fence=target_presence.fence,
            expires_at=target_presence.expires_at,
        )
    except MailboxOverflow, RealtimeSessionFenced, RealtimeSessionNotFound:
        return notification("The recipient is temporarily unable to receive messages.")

    target_stats_packet = _packet_from_snapshot(target_presence, ServerPacket.USER_STATS)
    if target_stats_packet:
        packet = next(PacketReader(target_stats_packet, packet_enum=ServerPacket))
        target_stats = packet.payload.read_user_stats()
        if target_stats.action == 1:
            away_message = await services.realtime.get_away_message(target.account_id)
            if away_message:
                return send_message(
                    Message(target.display_name, away_message, context.identity.current_name, target.account_id)
                )
    return b""


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
    except MailboxOverflow, RealtimeSessionFenced, RealtimeSessionNotFound:
        return False
    return True


async def _logout(context: StableRuntimeContext, services: StableServices) -> bytes:
    now = services.clock.now()
    output = bytearray()
    snapshots = await services.realtime.list_presences(
        at=now,
        limit=services.settings.stable_presence_batch_size,
    )
    await _stop_spectating(context, services)
    if services.multiplayer is not None:
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
        except ApplicationError:
            output.extend(notification("Multiplayer presence cleanup could not be completed."))
    if context.raw_token is not None:
        try:
            await services.identity.close_stable_session(context.raw_token, reason="client_logout")
        except ApplicationError:
            output.extend(notification("The durable session could not be closed cleanly."))
    with suppress(RealtimeSessionNotFound, RealtimeSessionFenced):
        await services.realtime.fence_session(
            context.identity.session_id,
            expected_revision=context.realtime.revision,
        )
    wire = user_logout(context.identity.account_id)
    for snapshot in snapshots:
        if snapshot.account_id != context.identity.account_id:
            with suppress(MailboxOverflow, RealtimeSessionFenced, RealtimeSessionNotFound):
                await services.realtime.enqueue_mailbox(
                    snapshot.account_id,
                    wire,
                    recipient_fence=snapshot.fence,
                    expires_at=snapshot.expires_at,
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
        return b""
    now = services.clock.now()
    host_presence = await services.realtime.get_presence(host_account_id, at=now)
    if host_presence is None:
        return b""
    try:
        current = await services.realtime.get_spectator_relation(
            context.identity.account_id,
            spectator_fence=context.realtime.fence,
            at=now,
        )
        if current is not None and current.host_account_id == host_account_id:
            return b""
        if current is not None:
            await _detach_spectator(current, services, at=now)
        existing = await services.realtime.list_spectators(
            host_account_id,
            host_fence=host_presence.fence,
            at=now,
        )
    except RealtimeSessionFenced, RealtimeSessionNotFound:
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
    except SpectatorHostOffline, RealtimeSessionFenced, RealtimeSessionNotFound:
        return b""
    relation = attachment.relation
    await _enqueue_spectator_packet(
        host_account_id,
        spectator_joined(context.identity.account_id),
        relation.host_fence,
        relation.expires_at,
        services,
    )
    output = bytearray()
    for existing_relation in existing:
        if existing_relation.spectator_account_id == context.identity.account_id:
            continue
        _extend_response(
            output,
            fellow_spectator_joined(existing_relation.spectator_account_id),
            max_bytes,
        )
        await _enqueue_spectator_packet(
            existing_relation.spectator_account_id,
            fellow_spectator_joined(context.identity.account_id),
            existing_relation.spectator_fence,
            min(relation.expires_at, existing_relation.expires_at),
            services,
        )
    for frame in attachment.history.frames:
        if not _extend_response(output, frame.payload, max_bytes):
            break
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
    except RealtimeSessionFenced, RealtimeSessionNotFound:
        spectators = ()
    detached = await services.realtime.detach_spectator(
        relation.host_account_id,
        relation.spectator_account_id,
        relation_id=relation.relation_id,
        expected_revision=relation.revision,
        host_fence=relation.host_fence,
        spectator_fence=relation.spectator_fence,
    )
    if not detached:
        return
    await _enqueue_spectator_packet(
        relation.host_account_id,
        spectator_left(relation.spectator_account_id),
        relation.host_fence,
        relation.expires_at,
        services,
    )
    wire = fellow_spectator_left(relation.spectator_account_id)
    for spectator in spectators:
        if spectator.spectator_account_id != relation.spectator_account_id:
            await _enqueue_spectator_packet(
                spectator.spectator_account_id,
                wire,
                spectator.spectator_fence,
                min(relation.expires_at, spectator.expires_at),
                services,
            )


async def _publish_spectator_frames(
    sequence: int,
    raw_data: memoryview,
    context: StableRuntimeContext,
    services: StableServices,
) -> None:
    wire = spectate_frames(raw_data)
    try:
        await services.realtime.publish_spectator_frame(
            context.identity.account_id,
            host_fence=context.realtime.fence,
            sequence=sequence,
            payload=wire,
            expires_at=context.realtime.expires_at,
        )
    except InvalidFrame, SpectatorHostOffline, RealtimeSessionFenced, RealtimeSessionNotFound:
        return


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
    except RealtimeSessionFenced, RealtimeSessionNotFound:
        recipients = ()
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
) -> None:
    with suppress(MailboxOverflow, RealtimeSessionFenced, RealtimeSessionNotFound):
        await services.realtime.enqueue_mailbox(
            account_id,
            payload,
            recipient_fence=recipient_fence,
            expires_at=expires_at,
        )


def _ignore_unsupported(packet: Packet) -> None:
    # PacketReader already isolated the payload, so skipping cannot desynchronise the poll.
    del packet


def realtime_expiry(context: ResolvedStableSession, services: StableServices) -> datetime:
    """Bound Redis state by both durable session expiry and configured online TTL."""
    online_expiry = services.clock.now() + timedelta(seconds=services.settings.redis_session_ttl_seconds)
    return min(context.expires_at, online_expiry)
