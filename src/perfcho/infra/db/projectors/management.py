"""Project sensitive management events into staff activity history."""

from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra.db.models.events import OutboxEvent
from perfcho.infra.db.projectors.common import (
    advance_checkpoint,
    payload_datetime,
    payload_integer,
    project_activity,
    require_accounts,
    require_event_context,
)

AUTHORIZATION_CONSUMER_NAME = "authorization-projector.v1"
MODERATION_CONSUMER_NAME = "moderation-projector.v1"

AUTHORIZATION_EVENT_TYPES = frozenset(
    {
        "authorization.role-granted.v1",
        "authorization.role-revoked.v1",
        "authorization.permission-granted.v1",
        "authorization.permission-revoked.v1",
        "authorization.entitlement-granted.v1",
        "authorization.entitlement-revoked.v1",
    }
)
MODERATION_EVENT_TYPES = frozenset(
    {
        "moderation.case.opened.v1",
        "moderation.case.entry_added.v1",
        "moderation.sanction.imposed.v1",
        "moderation.sanction.extended.v1",
        "moderation.sanction.revoked.v1",
    }
)


async def project_authorization_event(session: AsyncSession, event: OutboxEvent, partition_key: str) -> None:
    """Project one authorization management event as staff-only activity."""
    if event.event_type not in AUTHORIZATION_EVENT_TYPES:
        raise RuntimeError(f"unsupported authorization management event: {event.event_type}")
    subject_account_id = payload_integer(event.payload, "subject_account_id")
    require_event_context(
        event,
        partition_key,
        aggregate_type=event.aggregate_type,
        aggregate_id=event.aggregate_id,
        expected_partition_key=f"account:{subject_account_id}",
    )
    await _project(session, event, partition_key, AUTHORIZATION_CONSUMER_NAME)


async def project_moderation_event(session: AsyncSession, event: OutboxEvent, partition_key: str) -> None:
    """Project one moderation management event as staff-only activity."""
    if event.event_type not in MODERATION_EVENT_TYPES:
        raise RuntimeError(f"unsupported moderation management event: {event.event_type}")
    require_event_context(
        event,
        partition_key,
        aggregate_type=event.aggregate_type,
        aggregate_id=event.aggregate_id,
        expected_partition_key=f"{event.aggregate_type}:{event.aggregate_id}",
    )
    await _project(session, event, partition_key, MODERATION_CONSUMER_NAME)


async def _project(
    session: AsyncSession,
    event: OutboxEvent,
    partition_key: str,
    consumer_name: str,
) -> None:
    actor_account_id = payload_integer(event.payload, "actor_account_id")
    subject_account_id = payload_integer(event.payload, "subject_account_id")
    await require_accounts(session, (actor_account_id, subject_account_id))
    await project_activity(
        session,
        event,
        subject_account_id=subject_account_id,
        actor_account_id=actor_account_id,
        event_type=event.event_type.removesuffix(".v1").replace(".", "_").replace("-", "_"),
        visibility="staff",
        occurred_at=payload_datetime(event.payload, "occurred_at"),
        snapshot=event.payload,
    )
    await advance_checkpoint(session, event, projector=consumer_name, partition_key=partition_key)
