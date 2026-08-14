"""Tests for the lazer notifications WebSocket transport rendering."""

from datetime import UTC, datetime

import pytest

from perfcho.api.notifications import _render
from perfcho.modules.realtime import (
    ChannelMembershipAction,
    ChannelUpdatedBubble,
    ChatMessageBubble,
)

NOW = datetime(2026, 8, 10, 12, tzinfo=UTC)


def _chat(*, channel_id: int = 3, sender_account_id: int = 42, sender_name: str = "alice") -> ChatMessageBubble:
    return ChatMessageBubble(
        message_id=9,
        channel_id=channel_id,
        channel_name="#lobby",
        sender_account_id=sender_account_id,
        sender_name=sender_name,
        content="hi",
        is_action=False,
        created_at=NOW,
        direct=False,
    )


def test_render_chat_message_new() -> None:
    payload = _render(_chat())
    assert payload is not None
    assert payload["event"] == "chat.message.new"
    data = payload["data"]
    assert data["messages"][0]["channel_id"] == 3
    assert data["messages"][0]["content"] == "hi"
    assert data["users"][0] == {"id": 42, "username": "alice"}


def test_render_channel_join_and_part() -> None:
    bubble = ChannelUpdatedBubble(
        3, "#lobby", "topic", 5, ChannelMembershipAction.JOINED
    )
    joined = _render(bubble)
    assert joined["event"] == "chat.channel.join"
    assert joined["data"]["channel_id"] == 3

    parted = _render(
        ChannelUpdatedBubble(3, "#lobby", "topic", 5, ChannelMembershipAction.LEFT)
    )
    assert parted["event"] == "chat.channel.part"


def test_render_drops_transient_message_without_channel() -> None:
    bubble = _chat(channel_id=3)
    object.__setattr__(bubble, "channel_id", None)
    assert _render(bubble) is None


def test_render_drops_channel_update_without_membership_action() -> None:
    bubble = ChannelUpdatedBubble(3, "#lobby", "topic", 5)
    assert _render(bubble) is None
