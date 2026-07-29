"""Dispatch core Stable packets over canonical identity and realtime services."""

from dataclasses import dataclass
from datetime import datetime, timedelta

from perfcho.composition import StableServices
from perfcho.modules.identity import ResolvedStableSession
from perfcho.modules.realtime import PresenceSnapshot, RealtimeSession
from perfcho.realtime.stable.builders import (
    channel_info,
    channel_join,
    channel_kick,
    friends_list,
    notification,
    pong,
    send_message,
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


@dataclass(frozen=True, slots=True)
class StableRuntimeContext:
    """Carry current wire projections while dispatching one poll."""

    identity: ResolvedStableSession
    realtime: RealtimeSession
    presence: UserPresence
    stats: UserStats


async def dispatch_packets(body: bytes, context: StableRuntimeContext, services: StableServices) -> bytes:
    """Process supported core packets sequentially and return request-local output."""
    output = bytearray()
    for packet in PacketReader(body):
        packet_type = packet.packet_type
        if packet_type is ClientPacket.PING:
            packet.payload.require_exhausted()
            output.extend(pong())
        elif packet_type is ClientPacket.REQUEST_STATUS_UPDATE:
            packet.payload.require_exhausted()
            output.extend(user_stats(context.stats))
        elif packet_type is ClientPacket.CHANGE_ACTION:
            status = packet.payload.read_client_status()
            packet.payload.require_exhausted()
            updated_stats = _stats_from_status(context.stats, status)
            payload = user_presence(context.presence) + user_stats(updated_stats)
            await services.realtime.set_presence(
                PresenceSnapshot(
                    account_id=context.identity.account_id,
                    revision=context.realtime.revision,
                    payload=payload,
                    expires_at=context.realtime.expires_at,
                ),
                session_id=context.identity.session_id,
            )
            output.extend(user_stats(updated_stats))
        elif packet_type is ClientPacket.USER_STATS_REQUEST:
            account_ids = packet.payload.read_i32_list_u16()
            packet.payload.require_exhausted()
            output.extend(await _requested_packets(account_ids, ServerPacket.USER_STATS, services))
        elif packet_type is ClientPacket.USER_PRESENCE_REQUEST:
            account_ids = packet.payload.read_i32_list_u16()
            packet.payload.require_exhausted()
            output.extend(await _requested_packets(account_ids, ServerPacket.USER_PRESENCE, services))
        elif packet_type is ClientPacket.USER_PRESENCE_REQUEST_ALL:
            if packet.payload.remaining == 4:
                packet.payload.read_i32()
            packet.payload.require_exhausted()
            output.extend(user_presence(context.presence))
        elif packet_type is ClientPacket.RECEIVE_UPDATES:
            update_filter = packet.payload.read_i32()
            packet.payload.require_exhausted()
            if update_filter not in {0, 1, 2}:
                raise ValueError("invalid Stable presence update filter")
        elif packet_type is ClientPacket.CHANNEL_JOIN:
            channel_name = packet.payload.read_string()
            packet.payload.require_exhausted()
            output.extend(await _join_channel(channel_name, context, services))
        elif packet_type is ClientPacket.CHANNEL_PART:
            channel_name = packet.payload.read_string()
            packet.payload.require_exhausted()
            output.extend(await _part_channel(channel_name, context, services))
        elif packet_type is ClientPacket.SEND_PUBLIC_MESSAGE:
            message = packet.payload.read_message()
            packet.payload.require_exhausted()
            output.extend(await _send_public_message(message, context, services))
        elif packet_type is ClientPacket.FRIEND_ADD:
            target_id = packet.payload.read_i32()
            packet.payload.require_exhausted()
            output.extend(await _change_friend(target_id, adding=True, context=context, services=services))
        elif packet_type is ClientPacket.FRIEND_REMOVE:
            target_id = packet.payload.read_i32()
            packet.payload.require_exhausted()
            output.extend(await _change_friend(target_id, adding=False, context=context, services=services))
        elif packet_type is ClientPacket.LOGOUT:
            if packet.payload.remaining == 4:
                packet.payload.read_i32()
            packet.payload.require_exhausted()
        else:
            _ignore_unsupported(packet)
    return bytes(output)


async def _requested_packets(
    account_ids: tuple[int, ...],
    packet_type: ServerPacket,
    services: StableServices,
) -> bytes:
    output = bytearray()
    now = services.clock.now()
    for account_id in account_ids:
        if account_id < 1:
            continue
        snapshot = await services.realtime.get_presence(account_id, at=now)
        if snapshot is None:
            continue
        for packet in PacketReader(snapshot.payload, packet_enum=ServerPacket):
            if packet.packet_type is packet_type:
                output.extend(build_packet(packet.packet_id, packet.payload_view))
    return bytes(output)


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


async def _join_channel(name: str, context: StableRuntimeContext, services: StableServices) -> bytes:
    if services.community is None:
        return b""
    from perfcho.modules.community import ChannelNotFound

    try:
        channel = await services.community.get_public_channel_by_stable_name(context.identity.account_id, name)
    except ChannelNotFound:
        return notification("Channel is unavailable.")
    await services.realtime.join_channel(
        channel.channel_id,
        session_id=context.identity.session_id,
        expected_revision=context.realtime.revision,
    )
    members = await services.realtime.list_channel_members(channel.channel_id)
    return channel_join(channel.name) + channel_info(Channel(channel.name, channel.topic, len(members)))


async def _part_channel(name: str, context: StableRuntimeContext, services: StableServices) -> bytes:
    if services.community is None:
        return b""
    from perfcho.modules.community import ChannelNotFound

    try:
        channel = await services.community.get_public_channel_by_stable_name(context.identity.account_id, name)
    except ChannelNotFound:
        return b""
    await services.realtime.leave_channel(
        channel.channel_id,
        session_id=context.identity.session_id,
        expected_revision=context.realtime.revision,
    )
    return channel_kick(channel.name)


async def _send_public_message(message: Message, context: StableRuntimeContext, services: StableServices) -> bytes:
    if services.community is None:
        return b""
    result = await services.community.send_public_message(
        context.identity.account_id,
        message.recipient,
        services.id_generator.new(),
        message.text.strip(),
    )
    channel = await services.community.get_public_channel_by_stable_name(
        context.identity.account_id,
        message.recipient,
    )
    wire = send_message(
        Message(
            sender=context.identity.current_name,
            text=result.content,
            recipient=channel.name,
            sender_id=context.identity.account_id,
        )
    )
    for account_id in await services.realtime.list_channel_members(channel.channel_id):
        if account_id != context.identity.account_id:
            await services.realtime.enqueue_mailbox(account_id, wire, expires_at=context.realtime.expires_at)
    return b""


async def _change_friend(
    target_id: int,
    *,
    adding: bool,
    context: StableRuntimeContext,
    services: StableServices,
) -> bytes:
    if services.social is None or target_id < 1:
        return b""
    if adding:
        await services.social.follow(context.identity.account_id, target_id)
    else:
        await services.social.unfollow(context.identity.account_id, target_id)
    friends = await services.social.list_friends(context.identity.account_id)
    return friends_list(tuple(dict.fromkeys((1, *(friend.account_id for friend in friends)))))


def _ignore_unsupported(packet: Packet) -> None:
    # PacketReader already isolated the payload, so skipping cannot desynchronise the poll.
    del packet


def realtime_expiry(context: ResolvedStableSession, services: StableServices) -> datetime:
    """Bound Redis state by both durable session expiry and configured online TTL."""
    online_expiry = services.clock.now() + timedelta(seconds=services.settings.redis_session_ttl_seconds)
    return min(context.expires_at, online_expiry)
