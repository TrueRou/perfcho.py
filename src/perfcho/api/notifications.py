"""Adapt lazer chat delivery onto a raw WebSocket notifications channel.

The lazer client consumes realtime chat via its ``INotificationsClient``
WebSocket, not the SignalR hubs. The client first fetches ``notification_endpoint``
from ``GET /api/v2/notifications``, connects to that WebSocket with a Bearer
token, sends a ``chat.start`` message, and then receives ``SocketMessage``
payloads (``{"event": ..., "data": ...}``) for ``chat.message.new``,
``chat.channel.join`` and ``chat.channel.part``.

This module only adapts transport; the canonical account-keyed Bubble bus is
the event source.
"""

from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from perfcho.api.signalr.auth import authenticate
from perfcho.infra.logging import log_event
from perfcho.modules.identity import InvalidAccessToken
from perfcho.modules.realtime import (
    ChannelMembershipAction,
    ChannelUpdatedBubble,
    ChatMessageBubble,
    RealtimeBubble,
)

if TYPE_CHECKING:
    from perfcho.infra.compose import StableServices

router = APIRouter()

_UNAUTHORIZED_CODE = 4401


def _message(bubble: ChatMessageBubble) -> dict[str, object]:
    sender = {"id": bubble.sender_account_id, "username": bubble.sender_name}
    return {
        "message_id": bubble.message_id,
        "channel_id": bubble.channel_id,
        "is_action": bubble.is_action,
        "timestamp": bubble.created_at.isoformat(),
        "content": bubble.content,
        "sender": sender,
        "sender_id": bubble.sender_account_id,
        "uuid": None,
    }


def _channel(bubble: ChannelUpdatedBubble) -> dict[str, object]:
    return {
        "channel_id": bubble.channel_id,
        "type": "PUBLIC",
        "name": bubble.name,
        "description": bubble.topic,
        "last_message_id": None,
        "last_read_id": None,
        "message_length_limit": None,
    }


def _render(bubble: RealtimeBubble) -> dict[str, object] | None:
    if isinstance(bubble, ChatMessageBubble):
        # Bot DMs and other transient messages without a channel are delivered
        # through the durable notification inbox, not the chat transport.
        if bubble.channel_id is None:
            return None
        return {
            "event": "chat.message.new",
            "data": {
                "messages": [_message(bubble)],
                "users": [{"id": bubble.sender_account_id, "username": bubble.sender_name}],
            },
        }
    if isinstance(bubble, ChannelUpdatedBubble):
        if bubble.membership_action is ChannelMembershipAction.JOINED:
            return {"event": "chat.channel.join", "data": _channel(bubble)}
        if bubble.membership_action is ChannelMembershipAction.LEFT:
            return {"event": "chat.channel.part", "data": _channel(bubble)}
    return None


async def _bridge(websocket: WebSocket, services: StableServices, account_id: int) -> None:
    """Forward account-keyed chat Bubbles to the connected WebSocket."""
    bubbles = services.bubbles
    if bubbles is None:
        await websocket.close(code=1011, reason="realtime transport unavailable")
        return
    try:
        async with bubbles.subscribe(account_id) as subscription:
            while True:
                bubble = await subscription.receive(timeout=30.0)
                if bubble is None:
                    continue
                try:
                    payload = _render(bubble)
                    if payload is not None:
                        await websocket.send_text(json.dumps(payload, separators=(",", ":")))
                except Exception as error:
                    log_event(
                        "WARNING",
                        "notifications.ws.render_failed",
                        exception=error,
                        account_id=account_id,
                        bubble_type=type(bubble).__name__,
                    )
                await subscription.acknowledge()
    except (asyncio.CancelledError, WebSocketDisconnect):
        pass
    except Exception as error:
        log_event(
            "WARNING",
            "notifications.ws.bridge_failed",
            exception=error,
            account_id=account_id,
        )


@router.websocket("/notifications/ws")
async def notifications_ws(websocket: WebSocket) -> None:
    """Serve one lazer notifications WebSocket connection."""
    services = websocket.state.stable_services if hasattr(websocket.state, "stable_services") else None
    if services is None:
        await websocket.close(code=1011, reason="services unavailable")
        return
    try:
        account = await authenticate(services, dict(websocket.headers))
    except InvalidAccessToken:
        await websocket.close(code=_UNAUTHORIZED_CODE, reason="Unauthorized.")
        return

    await websocket.accept()
    try:
        login = await websocket.receive_json()
    except WebSocketDisconnect:
        return
    if not isinstance(login, dict) or login.get("event") != "chat.start":
        await websocket.close(code=1008, reason="chat.start required")
        return

    await _bridge(websocket, services, account.account_id)
