"""Adapt Stable login and binary HTTP polling to shared application services."""

import hashlib
from datetime import timedelta

from fastapi import APIRouter, Header, Request, Response

from perfcho.api.stable.dependencies import StableServicesDependency
from perfcho.api.stable.schema import StableLoginParseError, parse_stable_login
from perfcho.composition import StableServices
from perfcho.modules.common import ClientContext, CommandMeta
from perfcho.modules.identity import InvalidCredentials, InvalidStableSession, StableLogin, StableSessionAlreadyActive
from perfcho.modules.realtime import MailboxBatch, PresenceSnapshot, RealtimeSessionNotFound
from perfcho.realtime.stable import (
    LoginFailureReason,
    ProtocolError,
    UserPresence,
    UserStats,
    channel_info_end,
    friends_list,
    login_reply,
    notification,
    privileges,
    protocol_version,
    restart,
    silence_end,
    user_presence,
    user_stats,
)
from perfcho.realtime.stable.dispatcher import StableRuntimeContext, dispatch_packets, realtime_expiry

router = APIRouter(include_in_schema=False)

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
    return await _poll(body, osu_token, services)


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
                ip_address=_client_ip(request),
                user_agent="osu!",
            ),
            received_at=now,
        ),
        identifier=parsed.identifier,
        password_token=parsed.password_token,
        client_version=parsed.client_version,
        client_variant=None,
        ip_address=_client_ip(request),
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

    stable_privileges = await services.authorization.get_stable_privileges(result.account_id)
    online_expiry = min(
        result.expires_at,
        now + timedelta(seconds=services.settings.redis_session_ttl_seconds),
    )
    realtime = await services.realtime.open_session(
        session_id=result.session_id,
        account_id=result.account_id,
        expires_at=online_expiry,
    )
    presence = UserPresence(
        user_id=result.account_id,
        username=result.current_name,
        utc_offset=parsed.utc_offset,
        country_code=0,
        privileges=int(stable_privileges),
        mode=0,
        longitude=0.0,
        latitude=0.0,
        global_rank=0,
    )
    stats = _empty_stats(result.account_id)
    presence_packet = user_presence(presence)
    stats_packet = user_stats(stats)
    await services.realtime.set_presence(
        PresenceSnapshot(
            account_id=result.account_id,
            revision=realtime.revision,
            payload=presence_packet + stats_packet,
            expires_at=online_expiry,
        ),
        session_id=result.session_id,
    )
    payload = b"".join(
        (
            protocol_version(services.settings.stable_protocol_version),
            login_reply(result.account_id),
            privileges(int(stable_privileges)),
            notification(services.settings.stable_welcome_notification),
            channel_info_end(),
            friends_list((1,)),
            silence_end(0),
            presence_packet,
            stats_packet,
        )
    )
    return _binary_response(payload, token=result.raw_token)


async def _poll(body: bytes, raw_token: str, services: StableServices) -> Response:
    try:
        identity = await services.identity.resolve_stable_session(raw_token)
    except InvalidStableSession:
        return _binary_response(notification("Session expired. Please reconnect.") + restart(0))

    expiry = realtime_expiry(identity, services)
    try:
        realtime = await services.realtime.resolve_session(identity.session_id, at=services.clock.now())
        realtime = await services.realtime.heartbeat_session(
            identity.session_id,
            expected_revision=realtime.revision,
            expires_at=expiry,
        )
    except RealtimeSessionNotFound:
        realtime = await services.realtime.open_session(
            session_id=identity.session_id,
            account_id=identity.account_id,
            expires_at=expiry,
        )

    stored_presence = await services.realtime.get_presence(identity.account_id, at=services.clock.now())
    presence, stats = _presence_and_stats(identity.account_id, identity.current_name, stored_presence)
    if stored_presence is None:
        await services.realtime.set_presence(
            PresenceSnapshot(
                account_id=identity.account_id,
                revision=realtime.revision,
                payload=user_presence(presence) + user_stats(stats),
                expires_at=expiry,
            ),
            session_id=identity.session_id,
        )

    context = StableRuntimeContext(identity=identity, realtime=realtime, presence=presence, stats=stats)
    try:
        local_output = await dispatch_packets(body, context, services)
    except ProtocolError, ValueError:
        return _protocol_failure("Malformed Bancho packet.")

    lease_id = services.id_generator.new()
    batch: MailboxBatch | None = None
    try:
        batch = await services.realtime.lease_mailbox(
            identity.account_id,
            lease_id=lease_id,
            limit=services.settings.stable_mailbox_batch_size,
            expires_at=services.clock.now() + timedelta(seconds=services.settings.stable_mailbox_lease_seconds),
        )
        mailbox_output = b"".join(packet.payload for packet in batch.packets)
        if batch.packets:
            await services.realtime.ack_mailbox(
                identity.account_id,
                lease_id=lease_id,
                through_sequence=batch.packets[-1].sequence,
            )
        else:
            await services.realtime.release_mailbox(identity.account_id, lease_id=lease_id)
    except Exception:
        if batch is not None:
            await services.realtime.release_mailbox(identity.account_id, lease_id=lease_id)
        raise
    return _binary_response(local_output + mailbox_output)


async def _read_limited_body(request: Request, maximum: int) -> bytes:
    body = bytearray()
    async for chunk in request.stream():
        if len(body) + len(chunk) > maximum:
            raise ValueError("request body exceeds configured limit")
        body.extend(chunk)
    return bytes(body)


def _client_ip(request: Request) -> str:
    forwarded = request.headers.get("CF-Connecting-IP") or request.headers.get("X-Real-IP")
    if forwarded:
        return forwarded
    return request.client.host if request.client is not None else "127.0.0.1"


def _empty_stats(account_id: int) -> UserStats:
    return UserStats(account_id, 0, "", "", 0, 0, 0, 0, 0.0, 0, 0, 0, 0)


def _presence_and_stats(
    account_id: int,
    current_name: str,
    snapshot: PresenceSnapshot | None,
) -> tuple[UserPresence, UserStats]:
    if snapshot is not None:
        parsed_presence: UserPresence | None = None
        parsed_stats: UserStats | None = None
        from perfcho.realtime.stable import PacketReader, ServerPacket

        for packet in PacketReader(snapshot.payload, packet_enum=ServerPacket):
            if packet.packet_type is ServerPacket.USER_PRESENCE:
                parsed_presence = packet.payload.read_user_presence()
            elif packet.packet_type is ServerPacket.USER_STATS:
                parsed_stats = packet.payload.read_user_stats()
        if parsed_presence is not None and parsed_stats is not None:
            return parsed_presence, parsed_stats
    return (
        UserPresence(account_id, current_name, 0, 0, 1, 0, 0.0, 0.0, 0),
        _empty_stats(account_id),
    )


def _protocol_failure(message: str) -> Response:
    return _binary_response(notification(message) + restart(0))


def _binary_response(payload: bytes, *, token: str | None = None) -> Response:
    headers = {"cho-token": token} if token is not None else None
    return Response(payload, media_type=_BINARY_MEDIA_TYPE, headers=headers)
