"""Project durable notifications into best-effort realtime delivery.

Persistent notifications (the lazer inbox fact source in ``community.notification``)
are fanned out to stable clients as transient direct messages from BanchoBot.
This consumer is intentionally best-effort: a delivery failure never blocks the
underlying notification projection checkpoint.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime
from typing import Protocol

from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.consumers.common import (
    advance_checkpoint,
    payload_integer,
    payload_string,
    require_event_context,
)
from perfcho.infra.db.models.events import OutboxEvent
from perfcho.infra.logging import log_event
from perfcho.modules.realtime.bubbles import ChatMessageBubble, RealtimeBubble
from perfcho.modules.realtime.models import PresenceSnapshot, SessionFence

CONSUMER_NAME = "notification-realtime-consumer.v1"
EVENT_TYPES = frozenset({"community.notification-created.v1"})

type ConsumerHandler = Callable[[AsyncSession, OutboxEvent, str], Awaitable[None]]


class PresenceResolver(Protocol):
    """Resolve online presence for one account."""

    async def get_presence(self, account_id: int, *, at: datetime) -> PresenceSnapshot | None:
        """Return the account's live presence snapshot, or None when offline."""


class BubblePublisher(Protocol):
    """Publish fenced bubbles to online sessions."""

    async def publish(self, fence: SessionFence, bubble: RealtimeBubble) -> int:
        """Publish one bubble to a session fence and return the stream count."""


def render_notification_text(kind: str, payload: Mapping[str, object]) -> str | None:
    """Render a human-readable delivery line, returning None to skip delivery."""
    if kind == "direct_message":
        # Real-person DMs are already delivered live; never echo them as bot DMs.
        return None
    if kind == "achievement_unlocked":
        return "You unlocked a new achievement!"
    if kind == "beatmapset_status_changed":
        status = payload.get("status")
        return f"Your beatmapset status changed to {status}." if status else "Your beatmapset status changed."
    if kind == "beatmapset_synchronized":
        return "Your beatmapset was updated."
    return None


def notification_realtime_handler(
    *,
    realtime: PresenceResolver,
    bubbles: BubblePublisher,
    bot_account_id: int,
    bot_name: str,
) -> ConsumerHandler:
    """Build a consumer handler bound to realtime delivery dependencies."""

    async def handler(session: AsyncSession, event: OutboxEvent, partition_key: str) -> None:
        notification_id = payload_integer(event.payload, "notification_id")
        kind = payload_string(event.payload, "kind")
        require_event_context(
            event,
            partition_key,
            aggregate_type="notification",
            aggregate_id=str(notification_id),
            expected_partition_key=f"notification:{notification_id}",
        )
        text = render_notification_text(kind, _mapping(event.payload.get("payload")))
        if text is None:
            await advance_checkpoint(session, event, consumer=CONSUMER_NAME, partition_key=partition_key)
            return

        recipients = _recipient_ids(event.payload.get("recipient_account_ids"))
        now = datetime.now(UTC)
        delivered = 0
        for account_id in recipients:
            presence = await realtime.get_presence(account_id, at=now)
            if presence is None:
                continue
            bubble = ChatMessageBubble(
                message_id=None,
                channel_id=None,
                channel_name=presence.identity.display_name,
                sender_account_id=bot_account_id,
                sender_name=bot_name,
                content=text,
                is_action=False,
                created_at=now,
                direct=True,
            )
            try:
                await bubbles.publish(presence.account_id, bubble)
                delivered += 1
            except Exception as error:
                log_event(
                    "WARNING",
                    "notification.realtime.delivery_failed",
                    exception=error,
                    notification_id=notification_id,
                    account_id=account_id,
                    kind=kind,
                )
        log_event(
            "INFO",
            "notification.realtime.delivered",
            notification_id=notification_id,
            kind=kind,
            recipient_count=len(recipients),
            delivered=delivered,
        )
        await advance_checkpoint(session, event, consumer=CONSUMER_NAME, partition_key=partition_key)

    return handler


def _mapping(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


async def unconfigured_consumer(session: AsyncSession, event: OutboxEvent, partition_key: str) -> None:
    """Reject execution when runtime notification delivery is not configured."""
    del session, event, partition_key
    raise RuntimeError("notification realtime delivery is not configured")


def _recipient_ids(value: object) -> tuple[int, ...]:
    if not isinstance(value, (list, tuple)):
        return ()
    result: list[int] = []
    for item in value:
        if isinstance(item, bool) or not isinstance(item, int) or item < 1:
            continue
        result.append(item)
    return tuple(dict.fromkeys(result))
