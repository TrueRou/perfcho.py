"""Project social relations and achievement unlocks."""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.consumers.common import (
    advance_checkpoint,
    payload_boolean,
    payload_datetime,
    payload_integer,
    payload_nonnegative_integer,
    payload_optional_integer,
    record_activity,
    record_notification,
    require_accounts,
    require_event_context,
)
from perfcho.infra.db.models.events import OutboxEvent
from perfcho.infra.db.models.social import AchievementUnlock

SOCIAL_CONSUMER_NAME = "social-consumer.v1"
ACHIEVEMENT_CONSUMER_NAME = "achievement-consumer.v1"
SOCIAL_EVENT_TYPES = frozenset(
    {
        "social.account-followed.v1",
        "social.account-unfollowed.v1",
        "social.account-blocked.v1",
        "social.account-unblocked.v1",
    }
)
ACHIEVEMENT_EVENT_TYPES = frozenset({"social.achievement-unlocked.v1"})


async def consume_social_event(session: AsyncSession, event: OutboxEvent, partition_key: str) -> None:
    """Project one account-pair relationship change into private activity."""
    actor_account_id = payload_integer(event.payload, "actor_account_id")
    target_account_id = payload_integer(event.payload, "target_account_id")
    low_account_id, high_account_id = sorted((actor_account_id, target_account_id))
    require_event_context(
        event,
        partition_key,
        aggregate_type="social_pair",
        aggregate_id=f"{low_account_id}:{high_account_id}",
        expected_partition_key=f"social-pair:{low_account_id}:{high_account_id}",
    )
    await require_accounts(session, (actor_account_id, target_account_id))
    snapshot: dict[str, object] = {"target_account_id": target_account_id}
    if event.event_type == "social.account-followed.v1":
        activity_type = "account_followed"
        occurred_at = payload_datetime(event.payload, "followed_at")
        snapshot["mutual"] = payload_boolean(event.payload, "mutual")
    elif event.event_type == "social.account-unfollowed.v1":
        activity_type = "account_unfollowed"
        occurred_at = event.created_at
    elif event.event_type == "social.account-blocked.v1":
        activity_type = "account_blocked"
        occurred_at = payload_datetime(event.payload, "blocked_at")
        snapshot["removed_follow_count"] = payload_nonnegative_integer(event.payload, "removed_follow_count")
    else:
        activity_type = "account_unblocked"
        occurred_at = event.created_at
    await record_activity(
        session,
        event,
        subject_account_id=actor_account_id,
        actor_account_id=actor_account_id,
        event_type=activity_type,
        visibility="private",
        occurred_at=occurred_at,
        snapshot=snapshot,
    )
    await advance_checkpoint(session, event, consumer=SOCIAL_CONSUMER_NAME, partition_key=partition_key)


async def consume_achievement_event(session: AsyncSession, event: OutboxEvent, partition_key: str) -> None:
    """Project an achievement unlock into activity and a durable notification."""
    account_id = payload_integer(event.payload, "account_id")
    achievement_id = payload_integer(event.payload, "achievement_id")
    definition_version = payload_integer(event.payload, "definition_version")
    score_id = payload_optional_integer(event.payload, "score_id")
    require_event_context(
        event,
        partition_key,
        aggregate_type="account",
        aggregate_id=str(account_id),
        expected_partition_key=f"account:{account_id}",
    )
    await require_accounts(session, (account_id,))
    unlock = (
        await session.execute(
            select(AchievementUnlock).where(
                AchievementUnlock.account_id == account_id,
                AchievementUnlock.achievement_id == achievement_id,
            )
        )
    ).scalar_one_or_none()
    if unlock is None or unlock.definition_version != definition_version or unlock.score_id != score_id:
        raise RuntimeError("achievement event does not match the authoritative unlock")
    unlocked_at = payload_datetime(event.payload, "unlocked_at")
    snapshot = {
        "achievement_id": achievement_id,
        "definition_version": definition_version,
        "score_id": score_id,
    }
    await record_activity(
        session,
        event,
        subject_account_id=account_id,
        actor_account_id=account_id,
        event_type="achievement_unlocked",
        visibility="public",
        occurred_at=unlocked_at,
        snapshot=snapshot,
    )
    await record_notification(
        session,
        event,
        actor_account_id=account_id,
        kind="achievement_unlocked",
        category="achievement",
        resource_type="achievement",
        resource_id=str(achievement_id),
        payload=snapshot,
        recipient_account_ids=(account_id,),
    )
    await advance_checkpoint(session, event, consumer=ACHIEVEMENT_CONSUMER_NAME, partition_key=partition_key)
