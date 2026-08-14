"""Project content synchronization and status-change events into read models."""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra.db.models.content import Beatmapset
from perfcho.infra.db.models.events import OutboxEvent
from perfcho.infra.db.projectors.common import (
    advance_checkpoint,
    payload_datetime,
    payload_integer,
    payload_nonnegative_integer,
    payload_optional_integer,
    payload_optional_string,
    payload_string,
    project_activity,
    project_notification,
    require_event_context,
)

CONSUMER_NAME = "content-projector.v1"
EVENT_TYPES = frozenset({"content.beatmapset-synchronized.v1", "content.beatmapset-status-changed.v1"})


async def project_content_event(session: AsyncSession, event: OutboxEvent, partition_key: str) -> None:
    """Route one content event to its type-specific projection handler."""
    if event.event_type == "content.beatmapset-synchronized.v1":
        await _project_synchronized(session, event, partition_key)
    elif event.event_type == "content.beatmapset-status-changed.v1":
        await _project_status_changed(session, event, partition_key)
    else:
        raise RuntimeError(f"unsupported content event type: {event.event_type}")


async def _project_synchronized(session: AsyncSession, event: OutboxEvent, partition_key: str) -> None:
    """Notify a linked creator about the latest beatmapset synchronization."""
    beatmapset_id = payload_integer(event.payload, "beatmapset_id")
    external_beatmapset_id = payload_integer(event.payload, "external_beatmapset_id")
    created_revision_count = payload_nonnegative_integer(event.payload, "created_revision_count")
    unchanged_revision_count = payload_nonnegative_integer(event.payload, "unchanged_revision_count")
    removed_beatmap_count = payload_nonnegative_integer(event.payload, "removed_beatmap_count")
    source_updated_at = payload_datetime(event.payload, "source_updated_at")
    require_event_context(
        event,
        partition_key,
        aggregate_type="beatmapset",
        aggregate_id=str(beatmapset_id),
        expected_partition_key=f"beatmapset:{beatmapset_id}",
    )
    beatmapset = await session.scalar(select(Beatmapset).where(Beatmapset.id == beatmapset_id))
    if beatmapset is None or beatmapset.external_id != external_beatmapset_id:
        raise RuntimeError("content event does not match the authoritative beatmapset")
    changed = created_revision_count > 0 or removed_beatmap_count > 0
    if changed and beatmapset.creator_account_id is not None:
        snapshot = {
            "beatmapset_id": beatmapset_id,
            "external_beatmapset_id": external_beatmapset_id,
            "created_revision_count": created_revision_count,
            "unchanged_revision_count": unchanged_revision_count,
            "removed_beatmap_count": removed_beatmap_count,
        }
        await project_activity(
            session,
            event,
            subject_account_id=beatmapset.creator_account_id,
            actor_account_id=None,
            event_type="beatmapset_synchronized",
            visibility="public",
            occurred_at=source_updated_at,
            snapshot=snapshot,
        )
        await project_notification(
            session,
            event,
            actor_account_id=None,
            kind="beatmapset_synchronized",
            category="content",
            resource_type="beatmapset",
            resource_id=str(beatmapset_id),
            payload=snapshot,
            recipient_account_ids=(beatmapset.creator_account_id,),
        )
    await advance_checkpoint(session, event, projector=CONSUMER_NAME, partition_key=partition_key)


async def _project_status_changed(session: AsyncSession, event: OutboxEvent, partition_key: str) -> None:
    """Notify a linked creator about a ranking status transition."""
    beatmapset_id = payload_integer(event.payload, "beatmapset_id")
    external_beatmapset_id = payload_integer(event.payload, "external_beatmapset_id")
    previous_status = payload_optional_string(event.payload, "previous_status")
    status = payload_string(event.payload, "status")
    source = payload_string(event.payload, "source")
    actor_account_id = payload_optional_integer(event.payload, "actor_account_id")
    effective_at = payload_datetime(event.payload, "effective_at")
    require_event_context(
        event,
        partition_key,
        aggregate_type="beatmapset",
        aggregate_id=str(beatmapset_id),
        expected_partition_key=f"beatmapset:{beatmapset_id}",
    )
    beatmapset = await session.scalar(select(Beatmapset).where(Beatmapset.id == beatmapset_id))
    if beatmapset is None or beatmapset.external_id != external_beatmapset_id:
        raise RuntimeError("content event does not match the authoritative beatmapset")
    if beatmapset.creator_account_id is not None:
        snapshot = {
            "beatmapset_id": beatmapset_id,
            "external_beatmapset_id": external_beatmapset_id,
            "previous_status": previous_status,
            "status": status,
            "source": source,
        }
        await project_activity(
            session,
            event,
            subject_account_id=beatmapset.creator_account_id,
            actor_account_id=actor_account_id,
            event_type="beatmapset_status_changed",
            visibility="public",
            occurred_at=effective_at,
            snapshot=snapshot,
        )
        await project_notification(
            session,
            event,
            actor_account_id=actor_account_id,
            kind="beatmapset_status_changed",
            category="content",
            resource_type="beatmapset",
            resource_id=str(beatmapset_id),
            payload=snapshot,
            recipient_account_ids=(beatmapset.creator_account_id,),
        )
    await advance_checkpoint(session, event, projector=CONSUMER_NAME, partition_key=partition_key)
