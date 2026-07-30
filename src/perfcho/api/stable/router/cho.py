"""Adapt Stable login and binary HTTP polling to shared application services."""

import hashlib
import uuid
from contextlib import suppress
from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Header, Request, Response

from perfcho.api.stable.canonize.ipaddr import resolve_client_ip
from perfcho.api.stable.canonize.login import StableLoginParseError, parse_stable_login
from perfcho.api.stable.dependencies import StableServicesDependency
from perfcho.api.stable.dispatcher import StableRuntimeContext, account_stats, dispatch_packets, realtime_expiry
from perfcho.infra.composition import StableServices
from perfcho.modules.authorization import StablePrivilege
from perfcho.modules.common import ClientContext, CommandMeta
from perfcho.modules.identity import InvalidCredentials, InvalidStableSession, StableLogin, StableSessionAlreadyActive
from perfcho.modules.realtime import (
    MailboxOverflow,
    MailboxPacket,
    PollLeaseConflict,
    PresenceCapacityReached,
    PresenceSnapshot,
    RealtimeSession,
    RealtimeSessionFenced,
    RealtimeSessionNotFound,
)
from perfcho.modules.realtime.stable import (
    Channel,
    LoginFailureReason,
    Message,
    PacketReader,
    ProtocolError,
    ServerPacket,
    UserPresence,
    UserStats,
    build_packet,
    channel_info,
    channel_info_end,
    friends_list,
    login_reply,
    notification,
    privileges,
    protocol_version,
    restart,
    send_message,
    silence_end,
    user_presence,
    user_stats,
)
from perfcho.modules.realtime.stable.countries import stable_country_id

router = APIRouter()

_BINARY_MEDIA_TYPE = "application/octet-stream"


@router.post("/", response_class=Response)
async def bancho(
    request: Request,
    services: StableServicesDependency,
    user_agent: str | None = Header(default=None, alias="User-Agent"),
    osu_token: str | None = Header(default=None, alias="osu-token"),
) -> Response:
    """Authenticate a Stable client or execute one bounded packet poll."""
    if user_agent != "osu!":
        return _binary_response(login_reply(LoginFailureReason.ERROR), token="invalid-request")
    try:
        body = await _read_limited_body(request, services.settings.stable_max_body_bytes)
    except ValueError:
        return _protocol_failure("Request body is too large.")
    if osu_token is None:
        return await _login(request, body, services)
    return await _poll(request, body, osu_token, services)


async def _login(request: Request, body: bytes, services: StableServices) -> Response:
    try:
        parsed = parse_stable_login(body, expected_build=services.settings.stable_build)
    except StableLoginParseError as error:
        reason = LoginFailureReason.OLD_CLIENT if "unsupported Stable build" in str(error) else LoginFailureReason.ERROR
        return _binary_response(
            notification(str(error)) + login_reply(reason),
            token="invalid-request",
        )

    now = services.clock.now()
    request_id = services.id_generator.new()
    command = StableLogin(
        meta=CommandMeta(
            request_id=request_id,
            idempotency_key=f"stable-login:{request_id}",
            request_digest=hashlib.sha256(body).digest(),
            actor=None,
            client=ClientContext(
                family="stable",
                version=parsed.client_version,
                variant=None,
                ip_address=resolve_client_ip(request, services.settings.trusted_proxy_cidrs),
                user_agent="osu!",
            ),
            received_at=now,
        ),
        identifier=parsed.identifier,
        password_token=parsed.password_token,
        client_version=parsed.client_version,
        client_variant=None,
        ip_address=resolve_client_ip(request, services.settings.trusted_proxy_cidrs),
        user_agent="osu!",
        device_components=parsed.device_components,
        session_lifetime=timedelta(seconds=services.settings.stable_session_lifetime_seconds),
    )
    try:
        result = await services.identity.login_stable(command)
    except InvalidCredentials:
        return _binary_response(
            notification("Authentication failed.") + login_reply(LoginFailureReason.AUTHENTICATION_FAILED),
            token="invalid-credentials",
        )
    except StableSessionAlreadyActive:
        return _binary_response(
            login_reply(LoginFailureReason.AUTHENTICATION_FAILED) + notification("You are already logged in."),
            token="already-logged-in",
        )

    realtime: RealtimeSession | None = None
    try:
        online_presences = await services.realtime.list_presences(
            at=now,
            limit=services.settings.stable_presence_batch_size,
        )
        if len(online_presences) >= services.settings.stable_presence_batch_size:
            raise PresenceCapacityReached

        stable_privileges = await services.authorization.get_stable_privileges(result.account_id)
        if services.community is not None:
            await services.community.set_private_message_policy(
                result.account_id,
                "friends" if parsed.private_messages_from_friends_only else "all",
            )
        channels = (
            await services.community.list_public_channels(result.account_id) if services.community is not None else ()
        )
        social_friends = await services.social.list_friends(result.account_id) if services.social is not None else ()
        friend_ids = tuple(dict.fromkeys((1, *(friend.account_id for friend in social_friends))))
        offline_messages = (
            await services.community.list_unread_offline_direct_messages(result.account_id)
            if services.community is not None
            else ()
        )
        silence_seconds = (
            await services.community.get_global_silence_remaining_seconds(result.account_id)
            if services.community is not None
            else 0
        )
        online_expiry = min(
            result.expires_at,
            now + timedelta(seconds=services.settings.redis_session_ttl_seconds),
        )
        realtime = await services.realtime.open_session(
            session_id=result.session_id,
            account_id=result.account_id,
            expires_at=online_expiry,
            durable_expires_at=result.expires_at,
        )

        channel_packets: list[bytes] = []
        if services.community is not None:
            for channel in channels:
                if not channel.auto_join or channel.name == "#lobby":
                    continue
                await services.realtime.join_channel(
                    channel.channel_id,
                    session_id=result.session_id,
                    expected_revision=realtime.revision,
                )
                member_count = await services.community.get_channel_member_count(
                    result.account_id,
                    channel.channel_id,
                )
                channel_packets.append(channel_info(Channel(channel.name, channel.topic, member_count)))

        presence = UserPresence(
            user_id=result.account_id,
            username=result.current_name,
            utc_offset=parsed.utc_offset,
            country_code=stable_country_id(result.country_code),
            privileges=int(stable_privileges),
            mode=0,
            longitude=0.0,
            latitude=0.0,
            global_rank=0,
        )
        stats = await account_stats(_empty_stats(result.account_id), services)
        presence_packet = user_presence(presence)
        stats_packet = user_stats(stats)
        own_presence_payload = presence_packet + stats_packet
        await services.realtime.set_presence(
            PresenceSnapshot(
                account_id=result.account_id,
                revision=realtime.revision,
                payload=own_presence_payload,
                expires_at=online_expiry,
                session_id=result.session_id,
            ),
            session_id=result.session_id,
            capacity=services.settings.stable_presence_batch_size,
        )
        online_presences = tuple(
            snapshot
            for snapshot in await services.realtime.list_presences(
                at=now,
                limit=services.settings.stable_presence_batch_size,
            )
            if snapshot.account_id != result.account_id
        )
        for snapshot in online_presences:
            with suppress(MailboxOverflow, RealtimeSessionFenced, RealtimeSessionNotFound):
                await services.realtime.enqueue_mailbox(
                    snapshot.account_id,
                    own_presence_payload,
                    recipient_fence=snapshot.fence,
                    expires_at=snapshot.expires_at,
                )

        online_packets = tuple(_presence_projection(snapshot) for snapshot in online_presences)
        offline_packets = tuple(
            send_message(
                Message(
                    sender=message.sender_name,
                    text=_offline_message_text(message.created_at, message.content),
                    recipient=result.current_name,
                    sender_id=message.sender_account_id,
                )
            )
            for message in offline_messages
        )
        payload = b"".join(
            (
                protocol_version(services.settings.stable_protocol_version),
                login_reply(result.account_id),
                privileges(int(stable_privileges | StablePrivilege.SUPPORTER)),
                notification(services.settings.stable_welcome_notification),
                *channel_packets,
                channel_info_end(),
                friends_list(friend_ids),
                silence_end(silence_seconds),
                *online_packets,
                presence_packet,
                stats_packet,
                *offline_packets,
            )
        )
        return _binary_response(payload, token=result.raw_token)
    except PresenceCapacityReached:
        await _compensate_failed_login(result.raw_token, realtime, services)
        return _binary_response(
            login_reply(LoginFailureReason.ERROR) + notification("The server has reached its online capacity."),
            token="server-full",
        )
    except BaseException:
        await _compensate_failed_login(result.raw_token, realtime, services)
        raise


async def _poll(request: Request, body: bytes, raw_token: str, services: StableServices) -> Response:
    try:
        identity = await services.identity.resolve_stable_session(raw_token)
    except InvalidStableSession:
        return _binary_response(notification("Session expired. Please reconnect.") + restart(0))

    try:
        realtime = await services.realtime.resolve_session(identity.session_id, at=services.clock.now())
    except RealtimeSessionNotFound, RealtimeSessionFenced:
        return await _realtime_lost(raw_token, services)
    if realtime.account_id != identity.account_id:
        return await _realtime_lost(raw_token, services)

    try:
        identity = await services.identity.touch_stable_session(raw_token)
    except InvalidStableSession:
        return _binary_response(notification("Session expired. Please reconnect.") + restart(0))
    if identity.session_id != realtime.session_id or identity.account_id != realtime.account_id:
        return await _realtime_lost(raw_token, services)

    expiry = realtime_expiry(identity, services)
    try:
        realtime = await services.realtime.heartbeat_session(
            identity.session_id,
            expected_revision=realtime.revision,
            expires_at=expiry,
        )
    except RealtimeSessionNotFound, RealtimeSessionFenced:
        return await _realtime_lost(raw_token, services)

    try:
        stored_presence = await services.realtime.get_presence(identity.account_id, at=services.clock.now())
        presence, stats = _presence_and_stats(
            identity.account_id,
            identity.current_name,
            stable_country_id(identity.country_code),
            stored_presence,
        )
        if stored_presence is None:
            await services.realtime.set_presence(
                PresenceSnapshot(
                    account_id=identity.account_id,
                    revision=realtime.revision,
                    payload=user_presence(presence) + user_stats(stats),
                    expires_at=expiry,
                    session_id=identity.session_id,
                ),
                session_id=identity.session_id,
                capacity=services.settings.stable_presence_batch_size,
            )
    except RealtimeSessionNotFound, RealtimeSessionFenced, PresenceCapacityReached:
        return await _realtime_lost(raw_token, services)

    lease_id = services.id_generator.new()
    try:
        batch = await services.realtime.lease_mailbox(
            identity.account_id,
            recipient_fence=realtime.fence,
            lease_id=lease_id,
            limit=services.settings.stable_mailbox_batch_size,
            expires_at=services.clock.now() + timedelta(seconds=services.settings.stable_mailbox_lease_seconds),
        )
    except PollLeaseConflict:
        return _binary_response(b"")
    except RealtimeSessionNotFound, RealtimeSessionFenced:
        return await _realtime_lost(raw_token, services)

    context = StableRuntimeContext(
        identity=identity,
        realtime=realtime,
        presence=presence,
        stats=stats,
        client=ClientContext(
            family="stable",
            version=identity.client_version,
            variant=identity.client_variant,
            ip_address=resolve_client_ip(request, services.settings.trusted_proxy_cidrs),
            user_agent="osu!",
        ),
        raw_token=raw_token,
    )
    try:
        local_output = await dispatch_packets(body, context, services)
    except ProtocolError, ValueError:
        await _release_mailbox(identity.account_id, realtime, lease_id, services)
        return _protocol_failure("Malformed Bancho packet.")
    except BaseException:
        await _release_mailbox(identity.account_id, realtime, lease_id, services)
        raise

    mailbox_packets = _mailbox_packets_within_budget(
        batch.packets,
        services.settings.stable_max_response_bytes - len(local_output),
    )
    mailbox_output = b"".join(packet.payload for packet in mailbox_packets)
    try:
        if mailbox_packets:
            await services.realtime.ack_mailbox(
                identity.account_id,
                recipient_fence=realtime.fence,
                lease_id=lease_id,
                through_sequence=mailbox_packets[-1].sequence,
            )
        else:
            await services.realtime.release_mailbox(
                identity.account_id,
                recipient_fence=realtime.fence,
                lease_id=lease_id,
            )
    except PollLeaseConflict:
        return _binary_response(b"")
    except RealtimeSessionNotFound, RealtimeSessionFenced:
        return await _realtime_lost(raw_token, services)
    except BaseException:
        await _release_mailbox(identity.account_id, realtime, lease_id, services)
        raise
    return _binary_response(local_output + mailbox_output)


async def _read_limited_body(request: Request, maximum: int) -> bytes:
    body = bytearray()
    async for chunk in request.stream():
        if len(body) + len(chunk) > maximum:
            raise ValueError("request body exceeds configured limit")
        body.extend(chunk)
    return bytes(body)


def _empty_stats(account_id: int) -> UserStats:
    return UserStats(account_id, 0, "", "", 0, 0, 0, 0, 0.0, 0, 0, 0, 0)


def _presence_and_stats(
    account_id: int,
    current_name: str,
    country_code: int,
    snapshot: PresenceSnapshot | None,
) -> tuple[UserPresence, UserStats]:
    if snapshot is not None:
        parsed_presence: UserPresence | None = None
        parsed_stats: UserStats | None = None

        for packet in PacketReader(snapshot.payload, packet_enum=ServerPacket):
            if packet.packet_type is ServerPacket.USER_PRESENCE:
                parsed_presence = packet.payload.read_user_presence()
            elif packet.packet_type is ServerPacket.USER_STATS:
                parsed_stats = packet.payload.read_user_stats()
        if parsed_presence is not None and parsed_stats is not None:
            return parsed_presence, parsed_stats
    return (
        UserPresence(account_id, current_name, 0, country_code, 1, 0, 0.0, 0.0, 0),
        _empty_stats(account_id),
    )


def _presence_projection(snapshot: PresenceSnapshot) -> bytes:
    output = bytearray()
    for packet in PacketReader(snapshot.payload, packet_enum=ServerPacket):
        if packet.packet_type in {ServerPacket.USER_PRESENCE, ServerPacket.USER_STATS}:
            output.extend(build_packet(packet.packet_id, packet.payload_view))
    return bytes(output)


def _offline_message_text(created_at: datetime, content: str) -> str:
    timestamp = created_at.astimezone(UTC)
    return f"[{timestamp:%a %b %d @ %H:%M%p}] {content}"


def _mailbox_packets_within_budget(
    packets: tuple[MailboxPacket, ...],
    remaining_bytes: int,
) -> tuple[MailboxPacket, ...]:
    selected: list[MailboxPacket] = []
    used = 0
    for packet in packets:
        if used + len(packet.payload) > remaining_bytes:
            break
        selected.append(packet)
        used += len(packet.payload)
    return tuple(selected)


async def _compensate_failed_login(
    raw_token: str,
    realtime: RealtimeSession | None,
    services: StableServices,
) -> None:
    if realtime is not None:
        with suppress(Exception):
            await services.realtime.fence_session(
                realtime.session_id,
                expected_revision=realtime.revision,
            )
    with suppress(Exception):
        await services.identity.close_stable_session(raw_token, reason="bootstrap_failed")


async def _realtime_lost(raw_token: str, services: StableServices) -> Response:
    with suppress(Exception):
        await services.identity.close_stable_session(raw_token, reason="realtime_state_lost")
    return _binary_response(notification("Session state was lost. Please reconnect.") + restart(0))


async def _release_mailbox(
    account_id: int,
    realtime: RealtimeSession,
    lease_id: uuid.UUID,
    services: StableServices,
) -> None:
    with suppress(Exception):
        await services.realtime.release_mailbox(
            account_id,
            recipient_fence=realtime.fence,
            lease_id=lease_id,
        )


def _protocol_failure(message: str) -> Response:
    return _binary_response(notification(message) + restart(0))


def _binary_response(payload: bytes, *, token: str | None = None) -> Response:
    headers = {"cho-token": token} if token is not None else None
    return Response(payload, media_type=_BINARY_MEDIA_TYPE, headers=headers)
