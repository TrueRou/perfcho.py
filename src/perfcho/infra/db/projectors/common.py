"""Provide strict payload parsing and idempotent projector writes."""

import uuid
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra.db.models.community import (
    Notification,
    NotificationDispatch,
    NotificationPreference,
    NotificationRecipient,
)
from perfcho.infra.db.models.core import Account
from perfcho.infra.db.models.events import ActivityEvent, OutboxEvent, ProjectionCheckpoint


def require_event_context(
    event: OutboxEvent,
    partition_key: str,
    *,
    aggregate_type: str,
    aggregate_id: str,
    expected_partition_key: str,
) -> None:
    """Reject an event whose envelope disagrees with its validated payload."""
    if event.schema_version != 1:
        raise RuntimeError(f"unsupported schema version for {event.event_type}: {event.schema_version}")
    if event.aggregate_type != aggregate_type or event.aggregate_id != aggregate_id:
        raise RuntimeError(f"event envelope does not match {event.event_type} payload")
    if partition_key != expected_partition_key:
        raise RuntimeError(f"event partition does not match {event.event_type} payload")


def payload_integer(payload: Mapping[str, object], key: str) -> int:
    """Read one required positive integer payload field."""
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise RuntimeError(f"event payload field {key} must be a positive integer")
    return value


def payload_nonnegative_integer(payload: Mapping[str, object], key: str) -> int:
    """Read one required nonnegative integer payload field."""
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise RuntimeError(f"event payload field {key} must be a nonnegative integer")
    return value


def payload_optional_integer(payload: Mapping[str, object], key: str) -> int | None:
    """Read one nullable positive integer payload field."""
    value = payload.get(key)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise RuntimeError(f"event payload field {key} must be a positive integer or null")
    return value


def payload_string(payload: Mapping[str, object], key: str) -> str:
    """Read one required nonempty string payload field."""
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise RuntimeError(f"event payload field {key} must be a nonempty string")
    return value


def payload_optional_string(payload: Mapping[str, object], key: str) -> str | None:
    """Read one nullable string payload field."""
    value = payload.get(key)
    if value is not None and not isinstance(value, str):
        raise RuntimeError(f"event payload field {key} must be a string or null")
    return value


def payload_boolean(payload: Mapping[str, object], key: str) -> bool:
    """Read one required boolean payload field."""
    value = payload.get(key)
    if not isinstance(value, bool):
        raise RuntimeError(f"event payload field {key} must be a boolean")
    return value


def payload_datetime(payload: Mapping[str, object], key: str) -> datetime:
    """Read one timezone-aware ISO datetime payload field."""
    raw = payload_string(payload, key)
    try:
        value = datetime.fromisoformat(raw)
    except ValueError as error:
        raise RuntimeError(f"event payload field {key} must be an ISO datetime") from error
    if value.tzinfo is None or value.utcoffset() is None:
        raise RuntimeError(f"event payload field {key} must be timezone-aware")
    return value


def payload_uuid(payload: Mapping[str, object], key: str) -> uuid.UUID:
    """Read one UUID string payload field."""
    raw = payload_string(payload, key)
    try:
        return uuid.UUID(raw)
    except ValueError as error:
        raise RuntimeError(f"event payload field {key} must be a UUID") from error


async def require_accounts(session: AsyncSession, account_ids: Sequence[int]) -> None:
    """Require every referenced account before inserting projection foreign keys."""
    unique_ids = frozenset(account_ids)
    count = await session.scalar(select(func.count(Account.id)).where(Account.id.in_(unique_ids)))
    if count != len(unique_ids):
        raise RuntimeError("event references one or more missing accounts")


async def project_activity(
    session: AsyncSession,
    event: OutboxEvent,
    *,
    subject_account_id: int,
    actor_account_id: int | None,
    event_type: str,
    visibility: str,
    occurred_at: datetime,
    snapshot: Mapping[str, object],
) -> None:
    """Upsert one user activity projection by immutable source event."""
    statement = insert(ActivityEvent).values(
        source_event_id=event.id,
        subject_account_id=subject_account_id,
        actor_account_id=actor_account_id,
        event_type=event_type,
        visibility=visibility,
        occurred_at=occurred_at,
        snapshot=dict(snapshot),
    )
    await session.execute(
        statement.on_conflict_do_update(
            index_elements=(ActivityEvent.source_event_id,),
            set_={
                "subject_account_id": subject_account_id,
                "actor_account_id": actor_account_id,
                "event_type": event_type,
                "visibility": visibility,
                "occurred_at": occurred_at,
                "snapshot": dict(snapshot),
            },
        )
    )


async def project_notification(
    session: AsyncSession,
    event: OutboxEvent,
    *,
    actor_account_id: int | None,
    kind: str,
    category: str,
    resource_type: str,
    resource_id: str,
    payload: Mapping[str, object],
    recipient_account_ids: Sequence[int],
) -> int:
    """Upsert a notification, recipients, and configured external dispatch intents."""
    recipients = tuple(dict.fromkeys(recipient_account_ids))
    if not recipients:
        raise RuntimeError("notification projection requires at least one recipient")
    await require_accounts(session, recipients)
    statement = insert(Notification).values(
        source_event_id=event.id,
        actor_account_id=actor_account_id,
        kind=kind,
        category=category,
        resource_type=resource_type,
        resource_id=resource_id,
        payload=dict(payload),
    )
    notification_id = await session.scalar(
        statement.on_conflict_do_update(
            index_elements=(Notification.source_event_id, Notification.kind),
            set_={
                "actor_account_id": actor_account_id,
                "category": category,
                "resource_type": resource_type,
                "resource_id": resource_id,
                "payload": dict(payload),
            },
        ).returning(Notification.id)
    )
    if notification_id is None:
        raise RuntimeError("notification projection did not return an identifier")

    now = datetime.now(UTC)
    for account_id in recipients:
        await session.execute(
            insert(NotificationRecipient)
            .values(notification_id=notification_id, account_id=account_id)
            .on_conflict_do_nothing(
                index_elements=(NotificationRecipient.notification_id, NotificationRecipient.account_id)
            )
        )
        preference = await session.get(
            NotificationPreference,
            {"account_id": account_id, "category": category},
        )
        if preference is None:
            continue
        channels = (
            ("email" if preference.email_enabled else None),
            ("push" if preference.push_enabled else None),
        )
        for channel in channels:
            if channel is None:
                continue
            await session.execute(
                insert(NotificationDispatch)
                .values(
                    notification_id=notification_id,
                    account_id=account_id,
                    channel=channel,
                    status="pending",
                    attempt_count=0,
                    next_attempt_at=now,
                )
                .on_conflict_do_nothing(
                    index_elements=(
                        NotificationDispatch.notification_id,
                        NotificationDispatch.account_id,
                        NotificationDispatch.channel,
                    )
                )
            )
    return notification_id


async def advance_checkpoint(
    session: AsyncSession,
    event: OutboxEvent,
    *,
    projector: str,
    partition_key: str,
) -> None:
    """Advance a projector partition watermark without allowing regression."""
    statement = insert(ProjectionCheckpoint).values(
        projector=projector,
        partition_key=partition_key,
        source_event_id=event.id,
        source_position=event.position,
    )
    await session.execute(
        statement.on_conflict_do_update(
            index_elements=(ProjectionCheckpoint.projector, ProjectionCheckpoint.partition_key),
            set_={
                "source_event_id": event.id,
                "source_position": event.position,
                "updated_at": datetime.now(UTC),
            },
            where=statement.excluded.source_position > ProjectionCheckpoint.source_position,
        )
    )
