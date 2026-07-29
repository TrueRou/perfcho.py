"""Dispatch core Stable packets over canonical identity and realtime services."""

import hashlib
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from perfcho.composition import StableServices
from perfcho.modules.common import Actor, ClientContext, CommandMeta
from perfcho.modules.common.errors import ApplicationError
from perfcho.modules.identity import ResolvedStableSession
from perfcho.modules.multiplayer import LeaveRoom
from perfcho.modules.realtime import (
    InvalidFrame,
    MailboxOverflow,
    PresenceSnapshot,
    RealtimeSession,
    RealtimeSessionFenced,
    RealtimeSessionNotFound,
    SpectatorHostOffline,
    SpectatorRelation,
)
from perfcho.modules.scoring import Ruleset
from perfcho.modules.scoring.mods import parse_legacy_mods
from perfcho.realtime.stable.builders import (
    channel_info,
    channel_join,
    channel_kick,
    fellow_spectator_joined,
    fellow_spectator_left,
    friends_list,
    notification,
    pong,
    send_message,
    spectate_frames,
    spectator_cant_spectate,
    spectator_joined,
    spectator_left,
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
from perfcho.realtime.stable.multiplayer import MULTIPLAYER_PACKETS, dispatch_multiplayer_packet


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
    """Process supported core packets sequentially and return request-local output."""
    output = bytearray()
    for packet in PacketReader(body):
        packet_type = packet.packet_type
        if packet_type is ClientPacket.PING:
            packet.payload.require_exhausted()
            output.extend(pong())
        elif packet_type is ClientPacket.REQUEST_STATUS_UPDATE:
            packet.payload.require_exhausted()
            output.extend(user_stats(await account_stats(context.stats, services)))
        elif packet_type is ClientPacket.CHANGE_ACTION:
            status = packet.payload.read_client_status()
            packet.payload.require_exhausted()
            current_stats = await account_stats(
                replace_stats_mode(context.stats, mode=status.mode, mods=status.mods),
                services,
            )
            updated_stats = _stats_from_status(current_stats, status)
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
            await broadcast_presence_update(payload, context.identity.account_id, services)
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
            output.extend(await _all_presence_packets(services))
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
            output.extend(await _start_spectating(host_account_id, context, services))
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
            output.extend(await dispatch_multiplayer_packet(packet, context, services))
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
        elif packet_type is ClientPacket.SEND_PRIVATE_MESSAGE:
            message = packet.payload.read_message()
            packet.payload.require_exhausted()
            output.extend(await _send_private_message(message, context, services))
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
                await services.community.set_private_message_policy(
                    context.identity.account_id,
                    "friends" if value == 1 else "all",
                )
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
            await _logout(context, services)
            break
        elif packet_type in {ClientPacket.ERROR_REPORT, ClientPacket.IRC_ONLY, ClientPacket.BEATMAP_INFO_REQUEST}:
            packet.payload.read_remaining()
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


async def _all_presence_packets(services: StableServices) -> bytes:
    snapshots = await services.realtime.list_presences(
        at=services.clock.now(),
        limit=services.settings.stable_presence_batch_size,
    )
    return b"".join(_packet_from_snapshot(snapshot, ServerPacket.USER_PRESENCE) for snapshot in snapshots)


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
            with suppress(MailboxOverflow):
                await services.realtime.enqueue_mailbox(
                    snapshot.account_id,
                    payload,
                    expires_at=snapshot.expires_at,
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


async def _send_private_message(message: Message, context: StableRuntimeContext, services: StableServices) -> bytes:
    if services.community is None or services.social is None:
        return b""
    from perfcho.modules.community import AccountSilenced, DirectMessageBlocked, PrivateMessageRejected
    from perfcho.modules.social import SocialAccountNotFound

    content = message.text.strip()
    if not content:
        return b""
    try:
        target = await services.social.resolve_account_by_name(message.recipient)
        result = await services.community.send_direct_message(
            context.identity.account_id,
            target.account_id,
            services.id_generator.new(),
            content,
        )
    except SocialAccountNotFound:
        return notification("The direct-message recipient does not exist.")
    except DirectMessageBlocked, PrivateMessageRejected:
        return user_dm_blocked(message.recipient)
    except AccountSilenced:
        return notification("You cannot send messages while silenced.")
    except ApplicationError:
        return notification("The direct message could not be sent.")

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
        await services.realtime.enqueue_mailbox(target.account_id, wire, expires_at=target_presence.expires_at)
    except MailboxOverflow:
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


async def _logout(context: StableRuntimeContext, services: StableServices) -> None:
    now = services.clock.now()
    snapshots = await services.realtime.list_presences(
        at=now,
        limit=services.settings.stable_presence_batch_size,
    )
    await _stop_spectating(context, services)
    if services.multiplayer is not None:
        with suppress(ApplicationError):
            room = await services.multiplayer.find_room_for_account(context.identity.account_id)
            if room is not None:
                client = context.client or ClientContext(
                    "stable",
                    context.identity.client_version,
                    context.identity.client_variant,
                    "127.0.0.1",
                    "osu!",
                )
                digest = hashlib.sha256(f"logout:{context.identity.session_id}".encode()).digest()
                await services.multiplayer.leave_room(
                    LeaveRoom(
                        CommandMeta(
                            services.id_generator.new(),
                            f"stable-multiplayer:logout:{context.identity.session_id}",
                            digest,
                            Actor(context.identity.account_id, context.identity.session_id),
                            client,
                            now,
                        ),
                        room.room.public_id,
                    )
                )
    if context.raw_token is not None:
        with suppress(ApplicationError):
            await services.identity.close_stable_session(context.raw_token, reason="client_logout")
    with suppress(RealtimeSessionNotFound, RealtimeSessionFenced):
        await services.realtime.fence_session(
            context.identity.session_id,
            expected_revision=context.realtime.revision,
        )
    wire = user_logout(context.identity.account_id)
    for snapshot in snapshots:
        if snapshot.account_id != context.identity.account_id:
            with suppress(MailboxOverflow):
                await services.realtime.enqueue_mailbox(
                    snapshot.account_id,
                    wire,
                    expires_at=snapshot.expires_at,
                )


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


async def _start_spectating(
    host_account_id: int,
    context: StableRuntimeContext,
    services: StableServices,
) -> bytes:
    if host_account_id < 1 or host_account_id == context.identity.account_id:
        return b""
    now = services.clock.now()
    host_presence = await services.realtime.get_presence(host_account_id, at=now)
    if host_presence is None:
        return b""
    current = await services.realtime.get_spectator_relation(context.identity.account_id, at=now)
    if current is not None and current.host_account_id != host_account_id:
        await _detach_spectator(current, services, at=now)
    existing = await services.realtime.list_spectators(host_account_id, at=now)
    expiry = min(context.realtime.expires_at, host_presence.expires_at)
    try:
        relation = await services.realtime.attach_spectator(
            host_account_id,
            context.identity.account_id,
            expires_at=expiry,
        )
    except SpectatorHostOffline:
        return b""
    await _enqueue_spectator_packet(
        host_account_id,
        spectator_joined(context.identity.account_id),
        relation.expires_at,
        services,
    )
    output = bytearray()
    for spectator_account_id in existing:
        if spectator_account_id == context.identity.account_id:
            continue
        output.extend(fellow_spectator_joined(spectator_account_id))
        await _enqueue_spectator_packet(
            spectator_account_id,
            fellow_spectator_joined(context.identity.account_id),
            relation.expires_at,
            services,
        )
    frames = await services.realtime.read_spectator_frames(
        host_account_id,
        after_sequence=0,
        limit=services.settings.stable_spectator_frame_batch_size,
        at=now,
    )
    output.extend(b"".join(frame.payload for frame in frames))
    return bytes(output)


async def _stop_spectating(context: StableRuntimeContext, services: StableServices) -> None:
    relation = await services.realtime.get_spectator_relation(
        context.identity.account_id,
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
    spectators = await services.realtime.list_spectators(relation.host_account_id, at=at)
    await services.realtime.detach_spectator(
        relation.host_account_id,
        relation.spectator_account_id,
        expected_revision=relation.revision,
    )
    await _enqueue_spectator_packet(
        relation.host_account_id,
        spectator_left(relation.spectator_account_id),
        relation.expires_at,
        services,
    )
    wire = fellow_spectator_left(relation.spectator_account_id)
    for spectator_account_id in spectators:
        if spectator_account_id != relation.spectator_account_id:
            await _enqueue_spectator_packet(spectator_account_id, wire, relation.expires_at, services)


async def _publish_spectator_frames(
    sequence: int,
    raw_data: memoryview,
    context: StableRuntimeContext,
    services: StableServices,
) -> None:
    wire = spectate_frames(raw_data)
    try:
        frame = await services.realtime.publish_spectator_frame(
            context.identity.account_id,
            sequence=sequence,
            payload=wire,
            expires_at=context.realtime.expires_at,
        )
    except InvalidFrame, SpectatorHostOffline:
        return
    spectators = await services.realtime.list_spectators(context.identity.account_id, at=services.clock.now())
    for spectator_account_id in spectators:
        await _enqueue_spectator_packet(spectator_account_id, frame.payload, context.realtime.expires_at, services)


async def _cant_spectate(context: StableRuntimeContext, services: StableServices) -> None:
    now = services.clock.now()
    relation = await services.realtime.get_spectator_relation(context.identity.account_id, at=now)
    if relation is None:
        return
    wire = spectator_cant_spectate(context.identity.account_id)
    recipients = await services.realtime.list_spectators(relation.host_account_id, at=now)
    for account_id in recipients | {relation.host_account_id}:
        await _enqueue_spectator_packet(account_id, wire, relation.expires_at, services)


async def _enqueue_spectator_packet(
    account_id: int,
    payload: bytes,
    expires_at: datetime,
    services: StableServices,
) -> None:
    with suppress(MailboxOverflow):
        await services.realtime.enqueue_mailbox(account_id, payload, expires_at=expires_at)


def _ignore_unsupported(packet: Packet) -> None:
    # PacketReader already isolated the payload, so skipping cannot desynchronise the poll.
    del packet


def realtime_expiry(context: ResolvedStableSession, services: StableServices) -> datetime:
    """Bound Redis state by both durable session expiry and configured online TTL."""
    online_expiry = services.clock.now() + timedelta(seconds=services.settings.redis_session_ttl_seconds)
    return min(context.expires_at, online_expiry)
