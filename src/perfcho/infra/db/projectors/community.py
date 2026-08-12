"""Project durable community events without duplicating realtime delivery."""

from datetime import UTC, datetime

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra.db.models.community import (
    Channel,
    ChannelMembership,
    ChannelReadProjection,
    DirectConversation,
    Message,
)
from perfcho.infra.db.models.events import OutboxEvent
from perfcho.infra.db.projectors.common import (
    advance_checkpoint,
    payload_boolean,
    payload_datetime,
    payload_integer,
    payload_optional_integer,
    payload_string,
    payload_uuid,
    project_activity,
    project_notification,
    require_accounts,
    require_event_context,
)

COMMUNITY_CONSUMER_NAME = "community-projector.v1"
MESSAGE_CONSUMER_NAME = "community-message-projector.v1"
COMMUNITY_EVENT_TYPES = frozenset(
    {
        "community.direct-conversation-created.v1",
        "community.channel-member-joined.v1",
        "community.channel-member-left.v1",
    }
)
MESSAGE_EVENT_TYPES = frozenset({"community.message-sent.v1"})


async def project_community_event(session: AsyncSession, event: OutboxEvent, partition_key: str) -> None:
    """Project direct-conversation and durable membership changes."""
    channel_id = payload_integer(event.payload, "channel_id")
    require_event_context(
        event,
        partition_key,
        aggregate_type="channel",
        aggregate_id=str(channel_id),
        expected_partition_key=f"channel:{channel_id}",
    )
    await _upsert_channel_projection(session, event, channel_id=channel_id)
    if event.event_type == "community.direct-conversation-created.v1":
        low_account_id = payload_integer(event.payload, "low_account_id")
        high_account_id = payload_integer(event.payload, "high_account_id")
        direct = await session.get(DirectConversation, channel_id)
        if (
            low_account_id >= high_account_id
            or direct is None
            or direct.low_account_id != low_account_id
            or direct.high_account_id != high_account_id
        ):
            raise RuntimeError("community event does not match the authoritative direct conversation")
    else:
        account_id = payload_integer(event.payload, "account_id")
        await require_accounts(session, (account_id,))
        joined = event.event_type == "community.channel-member-joined.v1"
        await project_activity(
            session,
            event,
            subject_account_id=account_id,
            actor_account_id=account_id,
            event_type="channel_member_joined" if joined else "channel_member_left",
            visibility="private",
            occurred_at=event.created_at,
            snapshot={"channel_id": channel_id},
        )
    await advance_checkpoint(session, event, projector=COMMUNITY_CONSUMER_NAME, partition_key=partition_key)


async def project_community_message(session: AsyncSession, event: OutboxEvent, partition_key: str) -> None:
    """Project channel recency and create durable direct-message notification intents."""
    message_id = payload_integer(event.payload, "message_id")
    channel_id = payload_integer(event.payload, "channel_id")
    sender_account_id = payload_integer(event.payload, "sender_account_id")
    recipient_account_id = payload_optional_integer(event.payload, "direct_recipient_account_id")
    client_message_id = payload_uuid(event.payload, "client_message_id")
    content = payload_string(event.payload, "content")
    is_action = payload_boolean(event.payload, "is_action")
    reply_to_id = payload_optional_integer(event.payload, "reply_to_id")
    created_at = payload_datetime(event.payload, "created_at")
    require_event_context(
        event,
        partition_key,
        aggregate_type="channel",
        aggregate_id=str(channel_id),
        expected_partition_key=f"channel:{channel_id}",
    )
    message = await session.get(Message, message_id)
    if (
        message is None
        or message.channel_id != channel_id
        or message.sender_account_id != sender_account_id
        or message.client_message_id != client_message_id
        or message.content != content
        or message.is_action != is_action
        or message.reply_to_id != reply_to_id
        or message.created_at != created_at
    ):
        raise RuntimeError("community event does not match the authoritative message")
    await _upsert_channel_projection(
        session,
        event,
        channel_id=channel_id,
    )

    direct = await session.get(DirectConversation, channel_id)
    if recipient_account_id is None:
        if direct is not None:
            raise RuntimeError("direct message event is missing its recipient")
    else:
        if (
            direct is None
            or sender_account_id not in (direct.low_account_id, direct.high_account_id)
            or recipient_account_id not in (direct.low_account_id, direct.high_account_id)
            or recipient_account_id == sender_account_id
        ):
            raise RuntimeError("direct message recipient does not match the authoritative conversation")
        await project_notification(
            session,
            event,
            actor_account_id=sender_account_id,
            kind="direct_message",
            category="chat",
            resource_type="message",
            resource_id=str(message_id),
            payload={
                "message_id": message_id,
                "channel_id": channel_id,
                "sender_account_id": sender_account_id,
                "is_action": is_action,
            },
            recipient_account_ids=(recipient_account_id,),
        )
    await advance_checkpoint(session, event, projector=MESSAGE_CONSUMER_NAME, partition_key=partition_key)


async def _upsert_channel_projection(
    session: AsyncSession,
    event: OutboxEvent,
    *,
    channel_id: int,
) -> None:
    channel = await session.get(Channel, channel_id)
    if channel is None:
        raise RuntimeError("community event references a missing channel")
    direct = await session.get(DirectConversation, channel_id)
    active_member_count = (
        2
        if direct is not None
        else await session.scalar(
            select(func.count(ChannelMembership.id)).where(
                ChannelMembership.channel_id == channel_id,
                ChannelMembership.left_at.is_(None),
            )
        )
    )
    latest_message = (
        await session.execute(
            select(Message.id, Message.created_at)
            .where(Message.channel_id == channel_id, Message.deleted_at.is_(None))
            .order_by(Message.id.desc())
            .limit(1)
        )
    ).one_or_none()
    latest_message_id = latest_message.id if latest_message is not None else None
    latest_message_at = latest_message.created_at if latest_message is not None else None
    values: dict[str, object] = {
        "channel_id": channel_id,
        "kind": channel.kind.value,
        "active_member_count": active_member_count or 0,
        "latest_message_id": latest_message_id,
        "latest_message_at": latest_message_at,
        "source_event_id": event.id,
        "source_position": event.position,
    }
    statement = insert(ChannelReadProjection).values(**values)
    updates = {
        "kind": channel.kind.value,
        "active_member_count": active_member_count or 0,
        "latest_message_id": latest_message_id,
        "latest_message_at": latest_message_at,
        "source_event_id": event.id,
        "source_position": event.position,
        "updated_at": datetime.now(UTC),
    }
    await session.execute(
        statement.on_conflict_do_update(
            index_elements=(ChannelReadProjection.channel_id,),
            set_=updates,
            where=statement.excluded.source_position > ChannelReadProjection.source_position,
        )
    )
