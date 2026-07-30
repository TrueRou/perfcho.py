"""Project content synchronization events into operational read models."""

from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra.db.models.content import Beatmapset, BeatmapsetSyncProjection
from perfcho.infra.db.models.events import OutboxEvent
from perfcho.infra.db.projectors.common import (
    advance_checkpoint,
    payload_datetime,
    payload_integer,
    payload_nonnegative_integer,
    project_activity,
    project_notification,
    require_event_context,
)

CONSUMER_NAME = "content-projector.v1"
EVENT_TYPES = frozenset({"content.beatmapset-synchronized.v1"})


async def project_content_event(session: AsyncSession, event: OutboxEvent, partition_key: str) -> None:
    """Project the latest beatmapset synchronization and notify a linked creator."""
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
    statement = insert(BeatmapsetSyncProjection).values(
        beatmapset_id=beatmapset_id,
        external_beatmapset_id=external_beatmapset_id,
        created_revision_count=created_revision_count,
        unchanged_revision_count=unchanged_revision_count,
        removed_beatmap_count=removed_beatmap_count,
        source_updated_at=source_updated_at,
        source_event_id=event.id,
        source_position=event.position,
    )
    await session.execute(
        statement.on_conflict_do_update(
            index_elements=(BeatmapsetSyncProjection.beatmapset_id,),
            set_={
                "external_beatmapset_id": external_beatmapset_id,
                "created_revision_count": created_revision_count,
                "unchanged_revision_count": unchanged_revision_count,
                "removed_beatmap_count": removed_beatmap_count,
                "source_updated_at": source_updated_at,
                "source_event_id": event.id,
                "source_position": event.position,
                "updated_at": datetime.now(UTC),
            },
            where=statement.excluded.source_position > BeatmapsetSyncProjection.source_position,
        )
    )
    changed = created_revision_count > 0 or removed_beatmap_count > 0
    if changed and beatmapset.creator_account_id is not None:
        snapshot = {
            "beatmapset_id": beatmapset_id,
            "external_beatmapset_id": external_beatmapset_id,
            "created_revision_count": created_revision_count,
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
