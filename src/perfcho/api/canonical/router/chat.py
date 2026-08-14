"""Adapt osu!lazer chat endpoints onto the community service."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Form
from fastapi.responses import JSONResponse

from perfcho.api.canonical.dependencies import CanonicalAccountDependency, CanonicalServicesDependency
from perfcho.api.canonical.router._shared import error
from perfcho.modules.community import (
    ChannelAccessDenied,
    ChannelMembershipRequired,
    ChannelNotFound,
    ChannelSelector,
    ChannelView,
    MessageResult,
)

router = APIRouter()


@router.get("/chat/channels", response_model=None, tags=["Chat"])
async def list_channels(
    services: CanonicalServicesDependency,
    account: CanonicalAccountDependency,
) -> list[dict[str, object]] | JSONResponse:
    """Return public channels readable by the account."""
    if services.community is None:
        return error(503, "service_unavailable", "Community service is unavailable.")
    channels = await services.community.list_public_channels(account.account_id)
    return [_channel(channel) for channel in channels]


@router.get("/chat/channels/{channel_id}", response_model=None, tags=["Chat"])
async def get_channel(
    channel_id: int,
    services: CanonicalServicesDependency,
    account: CanonicalAccountDependency,
) -> dict[str, object] | JSONResponse:
    """Return one channel by ID."""
    if services.community is None:
        return error(503, "service_unavailable", "Community service is unavailable.")
    try:
        channel = await services.community.get_public_channel(
            account.account_id, ChannelSelector(channel_id=channel_id)
        )
    except ChannelNotFound:
        return error(404, "not_found", "Channel was not found.")
    return _channel(channel)


@router.get("/chat/channels/{channel_id}/messages", response_model=None, tags=["Chat"])
async def get_messages(
    channel_id: int,
    services: CanonicalServicesDependency,
    account: CanonicalAccountDependency,
) -> list[dict[str, object]] | JSONResponse:
    """Return recent channel messages (history query lands in M5)."""
    del channel_id, services, account
    return []


@router.post("/chat/channels/{channel_id}/messages", response_model=None, tags=["Chat"])
async def post_message(
    channel_id: int,
    services: CanonicalServicesDependency,
    account: CanonicalAccountDependency,
    message: Annotated[str, Form(max_length=1000)],
    is_action: Annotated[str, Form()] = "false",
    uuid_value: Annotated[str | None, Form(alias="uuid")] = None,
) -> dict[str, object] | JSONResponse:
    """Send one public-channel message."""
    if services.community is None:
        return error(503, "service_unavailable", "Community service is unavailable.")
    client_message_id = _parse_uuid(uuid_value)
    try:
        result = await services.community.send_public_message(
            account.account_id,
            ChannelSelector(channel_id=channel_id),
            client_message_id,
            message,
            is_action=is_action == "true",
        )
    except ChannelNotFound:
        return error(404, "not_found", "Channel was not found.")
    except (ChannelAccessDenied, ChannelMembershipRequired) as exc:
        return error(403, "forbidden", str(exc))
    return _message(result)


@router.put("/chat/channels/{channel_id}/users/{user_id}", response_model=None, tags=["Chat"])
async def join_channel(
    channel_id: int,
    user_id: int,
    services: CanonicalServicesDependency,
    account: CanonicalAccountDependency,
) -> dict[str, object] | JSONResponse:
    """Join a channel (only the caller may join themselves)."""
    if account.account_id != user_id:
        return error(403, "forbidden", "Cannot join a channel on behalf of another user.")
    if services.community is None:
        return error(503, "service_unavailable", "Community service is unavailable.")
    try:
        await services.community.join_channel(account.account_id, channel_id)
        channel = await services.community.get_public_channel(
            account.account_id, ChannelSelector(channel_id=channel_id)
        )
    except ChannelNotFound:
        return error(404, "not_found", "Channel was not found.")
    return _channel(channel)


@router.delete("/chat/channels/{channel_id}/users/{user_id}", response_model=None, tags=["Chat"])
async def leave_channel(
    channel_id: int,
    user_id: int,
    services: CanonicalServicesDependency,
    account: CanonicalAccountDependency,
) -> JSONResponse:
    """Leave a channel."""
    if account.account_id != user_id:
        return error(403, "forbidden", "Cannot leave a channel on behalf of another user.")
    if services.community is None:
        return error(503, "service_unavailable", "Community service is unavailable.")
    await services.community.leave_channel(account.account_id, channel_id)
    return JSONResponse(status_code=204, content=None)


@router.put("/chat/channels/{channel_id}/mark-as-read/{message_id}", response_model=None, tags=["Chat"])
async def mark_as_read(
    channel_id: int,
    message_id: int,
    services: CanonicalServicesDependency,
    account: CanonicalAccountDependency,
) -> JSONResponse:
    """Advance the account's read cursor for a channel."""
    if services.community is None:
        return error(503, "service_unavailable", "Community service is unavailable.")
    await services.community.mark_read(account.account_id, channel_id, message_id)
    return JSONResponse(status_code=204, content=None)


@router.post("/chat/new", response_model=None, tags=["Chat"])
async def new_private_message(
    services: CanonicalServicesDependency,
    account: CanonicalAccountDependency,
    target_id: Annotated[int, Form(gt=0)],
    message: Annotated[str, Form(max_length=1000)],
    is_action: Annotated[str, Form()] = "false",
    uuid_value: Annotated[str | None, Form(alias="uuid")] = None,
) -> dict[str, object] | JSONResponse:
    """Send a direct message, creating the conversation channel if needed."""
    if services.community is None:
        return error(503, "service_unavailable", "Community service is unavailable.")
    client_message_id = _parse_uuid(uuid_value)
    try:
        result = await services.community.send_direct_message(
            account.account_id,
            target_id,
            client_message_id,
            message,
            is_action=is_action == "true",
        )
    except (ChannelAccessDenied, ChannelMembershipRequired) as exc:
        return error(403, "forbidden", str(exc))
    return _message(result)


def _channel(channel: ChannelView) -> dict[str, object]:
    return {
        "channel_id": channel.channel_id,
        "type": "PUBLIC",
        "name": channel.name,
        "description": channel.topic,
        "moderated": False,
        "last_message_id": None,
        "users": [],
        "recent_messages": [],
    }


def _message(message: MessageResult) -> dict[str, object]:
    return {
        "message_id": message.message_id,
        "sender_id": message.sender_account_id,
        "channel_id": message.channel_id,
        "content": message.content,
        "is_action": message.is_action,
        "timestamp": message.created_at.isoformat(),
        "sender": {"id": message.sender_account_id, "username": ""},
    }


def _parse_uuid(value: str | None) -> uuid.UUID:
    if value is None:
        return uuid.uuid7()
    try:
        return uuid.UUID(value)
    except ValueError:
        return uuid.uuid7()
