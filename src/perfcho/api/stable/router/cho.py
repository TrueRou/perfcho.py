"""Adapt Stable login and binary HTTP polling to shared application services."""

import asyncio
import hashlib
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime, timedelta
from time import monotonic_ns

from fastapi import APIRouter, Header, Request, Response

from perfcho.api.stable.authorization import project_stable_privileges
from perfcho.api.stable.bubbles import StableBubbleRenderer, canonicalize_presence, stable_presence_models
from perfcho.api.stable.canonize.ipaddr import resolve_client_ip
from perfcho.api.stable.canonize.login import StableLoginParseError, parse_stable_login
from perfcho.api.stable.channels import stable_channel_name
from perfcho.api.stable.dependencies import StableServicesDependency
from perfcho.api.stable.dispatcher import StableRuntimeContext, account_stats, dispatch_packets, realtime_expiry
from perfcho.api.stable.realtime import (
    Channel,
    ClientPacket,
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
)
from perfcho.api.stable.realtime.countries import stable_country_id
from perfcho.infra.compose import StableServices
from perfcho.infra.logging import duration_ms, log_event, rate_limit, sampled
from perfcho.modules.common import ClientContext, CommandMeta
from perfcho.modules.community import ChannelView
from perfcho.modules.identity import (
    AuthenticateClientSession,
    InvalidCredentials,
    InvalidSession,
    SessionAlreadyActive,
)
from perfcho.modules.realtime import (
    PresenceCapacityReached,
    PresenceSnapshot,
    RealtimeBubble,
    RealtimeBubbleSubscription,
    RealtimeSession,
    RealtimeSessionFenced,
    RealtimeSessionNotFound,
    presence_updated_bubble,
)
from perfcho.modules.social import FollowView

router = APIRouter()

_BINARY_MEDIA_TYPE = "application/octet-stream"
_BUBBLE_RENDERER = StableBubbleRenderer()


@router.post("/", response_class=Response)
async def bancho(
    request: Request,
    services: StableServicesDependency,
    user_agent: str | None = Header(default=None, alias="User-Agent"),
    osu_token: str | None = Header(default=None, alias="osu-token"),
) -> Response:
    """Authenticate a Stable client or execute one bounded packet poll."""
    if user_agent != "osu!":
        if rate_limit("stable-bancho-invalid-client", interval_seconds=5):
            log_event(
                "INFO",
                "stable.bancho.request_rejected",
                operation="poll" if osu_token is not None else "login",
                stage="user_agent",
                outcome="invalid_client",
            )
        return _binary_response(login_reply(LoginFailureReason.ERROR), token="invalid-request")
    try:
        body = await _read_limited_body(request, services.settings.stable_max_body_bytes)
    except ValueError as error:
        if rate_limit("stable-bancho-body-limit", interval_seconds=5):
            log_event(
                "INFO",
                "stable.bancho.request_rejected",
                exception=error,
                operation="poll" if osu_token is not None else "login",
                stage="body_limit",
                outcome="input_rejected",
                error_type=type(error).__name__,
            )
        return _protocol_failure("Request body is too large.")
    if osu_token is None:
        return await _login(request, body, services)
    return await _poll(request, body, osu_token, services)


async def _login(request: Request, body: bytes, services: StableServices) -> Response:
    started_ns = monotonic_ns()
    try:
        parsed = parse_stable_login(body, expected_build=services.settings.stable_build)
    except StableLoginParseError as error:
        old_client = "unsupported Stable build" in str(error)
        reason = LoginFailureReason.OLD_CLIENT if old_client else LoginFailureReason.ERROR
        outcome = "unsupported_build" if old_client else "malformed_login"
        if rate_limit(f"stable.login.parse_rejected:{outcome}", interval_seconds=5.0):
            log_event(
                "INFO",
                "stable.login.rejected",
                exception=error,
                stage="parse",
                outcome=outcome,
                error_type=type(error).__name__,
                input_bytes=len(body),
                duration_ms=duration_ms(started_ns),
            )
        return _binary_response(
            notification(str(error)) + login_reply(reason),
            token="invalid-request",
        )

    now = services.clock.now()
    request_id = services.id_generator.new()
    command = AuthenticateClientSession(
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
        password_preverification=parsed.password_token,
        client_version=parsed.client_version,
        client_variant=None,
        ip_address=resolve_client_ip(request, services.settings.trusted_proxy_cidrs),
        user_agent="osu!",
        device_components=parsed.device_components,
        session_lifetime=timedelta(seconds=services.settings.stable_session_lifetime_seconds),
    )
    try:
        result = await services.identity.authenticate_client_session(command)
    except InvalidCredentials as error:
        if rate_limit("stable-login-invalid-credentials", interval_seconds=5):
            log_event(
                "INFO",
                "stable.login.rejected",
                exception=error,
                stage="authentication",
                outcome="invalid_credentials",
                error_code=error.code,
                error_type=type(error).__name__,
                duration_ms=duration_ms(started_ns),
            )
        return _binary_response(
            notification("Authentication failed.") + login_reply(LoginFailureReason.AUTHENTICATION_FAILED),
            token="invalid-credentials",
        )
    except SessionAlreadyActive as error:
        log_event(
            "INFO",
            "stable.login.rejected",
            exception=error,
            stage="active_session",
            outcome="already_active",
            error_code=error.code,
            error_type=type(error).__name__,
            duration_ms=duration_ms(started_ns),
        )
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

        authorization, social_friends = await asyncio.gather(
            services.authorization.get_effective(result.account_id),
            services.social.list_friends(result.account_id) if services.social is not None else _empty_friends(),
        )
        stable_privileges = project_stable_privileges(authorization)
        friend_ids = tuple(dict.fromkeys((1, *(friend.account_id for friend in social_friends))))
        if services.community is not None:
            channels, offline_messages, silence_seconds, _ = await asyncio.gather(
                services.community.list_public_channels(result.account_id),
                services.community.list_unread_offline_direct_messages(result.account_id),
                services.community.get_global_silence_remaining_seconds(result.account_id),
                services.community.set_private_message_policy(
                    result.account_id,
                    "friends" if parsed.private_messages_from_friends_only else "all",
                ),
            )
        else:
            channels, offline_messages, silence_seconds = (), (), 0
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

        channel_packets: tuple[bytes, ...] = ()
        community = services.community
        if community is not None:
            auto_join_channels = tuple(
                channel for channel in channels if channel.auto_join and stable_channel_name(channel) != "#lobby"
            )

            async def initialize_channel(channel: ChannelView) -> bytes:
                await services.realtime.join_channel(
                    channel.channel_id,
                    session_id=result.session_id,
                    expected_revision=realtime.revision,
                )
                member_count = await community.get_channel_member_count(
                    result.account_id,
                    channel.channel_id,
                    already_authorized=True,
                )
                return channel_info(Channel(stable_channel_name(channel), channel.topic, member_count))

            channel_packets = await _bounded_gather(
                auto_join_channels,
                initialize_channel,
                limit=services.settings.stable_presence_fanout_concurrency,
            )

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
        identity_state, activity, statistics = canonicalize_presence(
            presence,
            stats,
            country_code=result.country_code,
            privilege_codes=frozenset(
                authorization.permission_codes | authorization.role_codes | authorization.entitlement_codes
            ),
        )
        own_snapshot = PresenceSnapshot(
            account_id=result.account_id,
            revision=realtime.revision,
            identity=identity_state,
            activity=activity,
            statistics=statistics,
            expires_at=realtime.expires_at,
            session_id=result.session_id,
        )
        own_bubble = presence_updated_bubble(own_snapshot)
        presence_packet = _BUBBLE_RENDERER.render_presence(own_bubble, include_statistics=False)
        stats_packet = _BUBBLE_RENDERER.render_presence(own_bubble, include_identity=False)
        await services.realtime.set_presence(
            own_snapshot,
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

        async def publish_presence(snapshot: PresenceSnapshot) -> BaseException | None:
            if services.bubbles is None:
                return RuntimeError("realtime Bubble bus is unavailable")
            try:
                await services.bubbles.publish(snapshot.fence, own_bubble)
            except Exception as error:
                return error
            return None

        broadcast_errors = tuple(
            error
            for error in await _bounded_gather(
                online_presences,
                publish_presence,
                limit=services.settings.stable_presence_fanout_concurrency,
            )
            if error is not None
        )
        presence_broadcast_failure_count = len(broadcast_errors)
        presence_broadcast_error = broadcast_errors[0] if broadcast_errors else None

        online_packets = tuple(
            _BUBBLE_RENDERER.render(presence_updated_bubble(snapshot)) for snapshot in online_presences
        )
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
                privileges(int(stable_privileges)),
                notification(services.settings.stable_welcome_notification),
                *channel_packets,
                channel_info_end(),
                friends_list(friend_ids),
                silence_end(silence_seconds),
                presence_packet,
                stats_packet,
                *online_packets,
                *offline_packets,
            )
        )
        log_event(
            "INFO",
            "stable.login.completed",
            exception=presence_broadcast_error,
            outcome="success",
            account_id=result.account_id,
            channel_count=len(channel_packets),
            friend_count=len(friend_ids),
            online_presence_count=len(online_packets),
            offline_message_count=len(offline_packets),
            presence_broadcast_failure_count=presence_broadcast_failure_count,
            response_bytes=len(payload),
            duration_ms=duration_ms(started_ns),
        )
        return _binary_response(payload, token=result.raw_token)
    except PresenceCapacityReached as error:
        await _compensate_failed_login(result.raw_token, realtime, services)
        log_event(
            "INFO",
            "stable.login.rejected",
            exception=error,
            stage="capacity",
            outcome="capacity_reached",
            account_id=result.account_id,
            error_code=error.code,
            error_type=type(error).__name__,
            duration_ms=duration_ms(started_ns),
        )
        return _binary_response(
            login_reply(LoginFailureReason.ERROR) + notification("The server has reached its online capacity."),
            token="server-full",
        )
    except BaseException as error:
        await _compensate_failed_login(result.raw_token, realtime, services)
        log_event(
            "ERROR",
            "stable.login.bootstrap_failed",
            exception=error,
            outcome="failed",
            account_id=result.account_id,
            error_type=type(error).__name__,
            duration_ms=duration_ms(started_ns),
        )
        raise


async def _poll(request: Request, body: bytes, raw_token: str, services: StableServices) -> Response:
    started_ns = monotonic_ns()
    now = services.clock.now()
    idle_ping = _is_idle_ping(body)
    try:
        identity = await services.identity.touch_client_session(raw_token)
    except InvalidSession as error:
        _log_invalid_poll_session("touch_identity", error)
        return _binary_response(notification("Session expired. Please reconnect.") + restart(0))

    try:
        realtime = await services.realtime.resolve_session(identity.session_id, at=now)
    except (RealtimeSessionNotFound, RealtimeSessionFenced) as error:
        return await _realtime_lost(
            raw_token,
            services,
            account_id=identity.account_id,
            stage="resolve_realtime",
            error=error,
        )
    if realtime.account_id != identity.account_id:
        return await _realtime_lost(
            raw_token,
            services,
            account_id=identity.account_id,
            stage="account_fence",
        )

    if identity.session_id != realtime.session_id or identity.account_id != realtime.account_id:
        return await _realtime_lost(
            raw_token,
            services,
            account_id=identity.account_id,
            stage="identity_fence",
        )

    expiry = realtime_expiry(identity, services)
    try:
        realtime = await services.realtime.heartbeat_session(
            identity.session_id,
            expected_revision=realtime.revision,
            expires_at=expiry,
        )
    except (RealtimeSessionNotFound, RealtimeSessionFenced) as error:
        return await _realtime_lost(
            raw_token,
            services,
            account_id=identity.account_id,
            stage="heartbeat",
            error=error,
        )

    try:
        stored_presence = await services.realtime.get_presence(identity.account_id, at=now)
        if stored_presence is None:
            presence = UserPresence(
                identity.account_id,
                identity.current_name,
                0,
                stable_country_id(identity.country_code),
                1,
                0,
                0.0,
                0.0,
                0,
            )
            stats = _empty_stats(identity.account_id)
            identity_state, activity, statistics = canonicalize_presence(
                presence,
                stats,
                country_code=identity.country_code,
            )
            stored_presence = PresenceSnapshot(
                account_id=identity.account_id,
                revision=realtime.revision,
                identity=identity_state,
                activity=activity,
                statistics=statistics,
                expires_at=expiry,
                session_id=identity.session_id,
            )
            await services.realtime.set_presence(
                stored_presence,
                session_id=identity.session_id,
                capacity=services.settings.stable_presence_batch_size,
            )
        else:
            presence, stats = stable_presence_models(presence_updated_bubble(stored_presence))
    except (RealtimeSessionNotFound, RealtimeSessionFenced, PresenceCapacityReached) as error:
        return await _realtime_lost(
            raw_token,
            services,
            account_id=identity.account_id,
            stage="presence",
            error=error,
        )

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
    if services.bubbles is None or services.poll_gate is None:
        raise RuntimeError("Stable Poll requires the Bubble bus and PollGate")

    gate_id = services.id_generator.new()
    try:
        acquired = await services.poll_gate.acquire(
            identity.account_id,
            realtime.fence,
            gate_id,
            expires_at=now + timedelta(seconds=services.settings.stable_poll_gate_seconds),
        )
    except Exception as error:
        if rate_limit("stable.poll.gate_failed", interval_seconds=5.0):
            log_event(
                "WARNING",
                "stable.poll.gate_failed",
                exception=error,
                outcome="failed",
                account_id=identity.account_id,
            )
        return _binary_response(b"")
    if not acquired:
        return _binary_response(b"")

    try:
        try:
            async with services.bubbles.subscribe(realtime.fence) as subscription:
                return await _poll_subscribed(
                    body,
                    context,
                    services,
                    subscription,
                    idle_ping=idle_ping,
                    started_ns=started_ns,
                )
        except (ProtocolError, ValueError) as error:
            log_event(
                "INFO",
                "stable.poll.protocol_rejected",
                exception=error,
                stage="dispatch",
                outcome="malformed",
                account_id=identity.account_id,
                error_type=type(error).__name__,
                input_bytes=len(body),
                duration_ms=duration_ms(started_ns),
            )
            return _protocol_failure("Malformed Bancho packet.")
        except (RealtimeSessionNotFound, RealtimeSessionFenced) as error:
            return await _realtime_lost(
                raw_token,
                services,
                account_id=identity.account_id,
                stage="bubble_poll",
                error=error,
            )
        except Exception as error:
            if rate_limit("stable.poll.subscription_failed", interval_seconds=5.0):
                log_event(
                    "WARNING",
                    "stable.poll.subscription_failed",
                    exception=error,
                    outcome="local_only",
                    account_id=identity.account_id,
                )
            return _binary_response(_render_local_response(context, services))
    finally:
        try:
            await services.poll_gate.release(identity.account_id, realtime.fence, gate_id)
        except Exception as error:
            if rate_limit("stable.poll.gate_release_failed", interval_seconds=5.0):
                log_event(
                    "WARNING",
                    "stable.poll.gate_release_failed",
                    exception=error,
                    outcome="ttl_cleanup",
                    account_id=identity.account_id,
                )


async def _poll_subscribed(
    body: bytes,
    context: StableRuntimeContext,
    services: StableServices,
    subscription: RealtimeBubbleSubscription,
    *,
    idle_ping: bool,
    started_ns: int,
) -> Response:
    local_bubbles = await dispatch_packets(body, context, services)
    if context.session_closed:
        return _binary_response(_render_local_response(context, services, local_bubbles))

    remote_bubbles: tuple[RealtimeBubble, ...]
    try:
        remote_bubbles = await subscription.drain(limit=services.settings.stable_bubble_batch_size)
    except Exception as error:
        _log_subscription_error(error, context.identity.account_id, stage="drain")
        remote_bubbles = ()

    waited = False
    if idle_ping and not local_bubbles and not context.stable_output and not remote_bubbles:
        waited = True
        try:
            first = await subscription.receive(timeout=services.settings.stable_bubble_wait_seconds)
            if first is not None:
                remaining = services.settings.stable_bubble_batch_size - 1
                additional = await subscription.drain(limit=remaining) if remaining else ()
                remote_bubbles = (first, *additional)
        except Exception as error:
            _log_subscription_error(error, context.identity.account_id, stage="wait")

    output = bytearray(_render_local_response(context, services, local_bubbles))
    remote_output = _BUBBLE_RENDERER.render_many(
        remote_bubbles,
        max_bytes=max(0, services.settings.stable_max_response_bytes - len(output)),
    )
    output.extend(remote_output)
    try:
        await subscription.acknowledge()
    except Exception as error:
        _log_subscription_error(error, context.identity.account_id, stage="acknowledge")
    if sampled((started_ns, "poll_summary"), services.settings.log_hot_path_sample_rate):
        log_event(
            "INFO",
            "stable.poll.completed",
            outcome="success",
            account_id=context.identity.account_id,
            input_bytes=len(body),
            local_output_bytes=len(context.stable_output),
            local_bubble_count=len(local_bubbles),
            remote_bubble_count=len(remote_bubbles),
            bubble_waited=waited,
            response_bytes=len(output),
            duration_ms=duration_ms(started_ns),
        )
    return _binary_response(bytes(output))


def _render_local_response(
    context: StableRuntimeContext,
    services: StableServices,
    bubbles: Sequence[RealtimeBubble] | None = None,
) -> bytes:
    output = bytearray(
        _BUBBLE_RENDERER.render_many(
            context.local_bubbles if bubbles is None else bubbles,
            max_bytes=services.settings.stable_max_response_bytes,
        )
    )
    _extend_stable_output(
        output,
        bytes(context.stable_output),
        services.settings.stable_max_response_bytes,
    )
    return bytes(output)


def _extend_stable_output(output: bytearray, payload: bytes, maximum: int) -> None:
    for packet in PacketReader(payload, packet_enum=ServerPacket):
        wire = build_packet(packet.packet_id, packet.payload_view)
        if len(output) + len(wire) > maximum:
            break
        output.extend(wire)


def _log_subscription_error(error: Exception, account_id: int, *, stage: str) -> None:
    if rate_limit(f"stable.poll.subscription_failed:{stage}", interval_seconds=5.0):
        log_event(
            "WARNING",
            "stable.poll.subscription_failed",
            exception=error,
            stage=stage,
            outcome="local_only",
            account_id=account_id,
        )


async def _read_limited_body(request: Request, maximum: int) -> bytes:
    body = bytearray()
    async for chunk in request.stream():
        if len(body) + len(chunk) > maximum:
            raise ValueError("request body exceeds configured limit")
        body.extend(chunk)
    return bytes(body)


def _empty_stats(account_id: int) -> UserStats:
    return UserStats(account_id, 0, "", "", 0, 0, 0, 0, 0.0, 0, 0, 0, 0)


async def _empty_friends() -> tuple[FollowView, ...]:
    return ()


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


def _offline_message_text(created_at: datetime, content: str) -> str:
    timestamp = created_at.astimezone(UTC)
    return f"[{timestamp:%a %b %d @ %H:%M%p}] {content}"


def _is_idle_ping(body: bytes) -> bool:
    """Return whether the body is exactly one empty Stable Osu_Pong packet."""
    try:
        reader = PacketReader(body)
        packet = next(reader)
        if packet.packet_type is not ClientPacket.PING or packet.payload.remaining:
            return False
        try:
            next(reader)
        except StopIteration:
            return True
    except ProtocolError, ValueError, StopIteration:
        return False
    return False


async def _compensate_failed_login(
    raw_token: str,
    realtime: RealtimeSession | None,
    services: StableServices,
) -> None:
    if realtime is not None:
        try:
            await services.realtime.fence_session(
                realtime.session_id,
                expected_revision=realtime.revision,
            )
        except Exception as error:
            log_event(
                "ERROR",
                "stable.login.cleanup_failed",
                exception=error,
                operation="fence_realtime_session",
                error_code=getattr(error, "code", "cleanup_failed"),
                error_type=type(error).__name__,
            )
    try:
        await services.identity.close_client_session(raw_token, reason="bootstrap_failed")
    except Exception as error:
        log_event(
            "ERROR",
            "stable.login.cleanup_failed",
            exception=error,
            operation="close_durable_session",
            error_code=getattr(error, "code", "cleanup_failed"),
            error_type=type(error).__name__,
        )


async def _realtime_lost(
    raw_token: str,
    services: StableServices,
    *,
    account_id: int,
    stage: str,
    error: Exception | None = None,
) -> Response:
    log_event(
        "WARNING",
        "stable.poll.session_lost",
        stage=stage,
        outcome="reconnect",
        account_id=account_id,
        error_code=getattr(error, "code", "session_fence_mismatch"),
        error_type=type(error).__name__ if error is not None else "SessionFenceMismatch",
    )
    try:
        await services.identity.close_client_session(raw_token, reason="realtime_state_lost")
    except Exception as cleanup_error:
        log_event(
            "ERROR",
            "stable.poll.cleanup_failed",
            exception=cleanup_error,
            operation="close_durable_session",
            account_id=account_id,
            error_code=getattr(cleanup_error, "code", "cleanup_failed"),
            error_type=type(cleanup_error).__name__,
        )
    return _binary_response(notification("Session state was lost. Please reconnect.") + restart(0))


def _log_invalid_poll_session(stage: str, error: InvalidSession) -> None:
    if not rate_limit(f"stable.poll.invalid_session:{stage}", interval_seconds=5.0):
        return
    log_event(
        "INFO",
        "stable.poll.invalid_session",
        stage=stage,
        outcome="reconnect",
        error_code=error.code,
        error_type=type(error).__name__,
    )


def _protocol_failure(message: str) -> Response:
    return _binary_response(notification(message) + restart(0))


def _binary_response(payload: bytes, *, token: str | None = None) -> Response:
    headers = {"cho-token": token} if token is not None else None
    return Response(payload, media_type=_BINARY_MEDIA_TYPE, headers=headers)
