import uuid
from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from perfcho.infra.database.base import DbBase
from perfcho.infra.database.mixins import BigIntIdentityMixin, CreatedAtMixin, TimestampMixin, Uuid7PrimaryKeyMixin


class OutboxEvent(Uuid7PrimaryKeyMixin, CreatedAtMixin, DbBase):
    """Stores domain events committed atomically with business state for reliable publication."""

    __tablename__ = "outbox_events"
    __table_args__ = (
        CheckConstraint("schema_version > 0 AND attempt_count >= 0", name="version_attempt_range"),
        Index("ix_outbox_events_pending", "published_at", "available_at"),
        Index("ix_outbox_events_aggregate", "aggregate_type", "aggregate_id", "created_at"),
        {"schema": "events"},
    )

    aggregate_type: Mapped[str] = mapped_column(String(64), nullable=False)
    aggregate_id: Mapped[str] = mapped_column(String(128), nullable=False)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False)
    payload: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    last_error: Mapped[str | None] = mapped_column(Text)


class ActivityEvent(BigIntIdentityMixin, CreatedAtMixin, DbBase):
    """Stores user-visible activity projected from authoritative domain events."""

    __tablename__ = "activity_events"
    __table_args__ = (
        UniqueConstraint("source_event_id"),
        Index("ix_activity_events_subject_created", "subject_account_id", "created_at"),
        Index("ix_activity_events_type_created", "event_type", "created_at"),
        {"schema": "events"},
    )

    source_event_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("events.outbox_events.id", ondelete="RESTRICT"), nullable=False
    )
    subject_account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="CASCADE"), nullable=False)
    actor_account_id: Mapped[int | None] = mapped_column(ForeignKey("core.accounts.id", ondelete="SET NULL"))
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    visibility: Mapped[str] = mapped_column(String(16), nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    snapshot: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False, default=dict, server_default="{}")


class ProjectionCheckpoint(TimestampMixin, DbBase):
    """Tracks the event-processing watermark of each asynchronous projector."""

    __tablename__ = "projection_checkpoints"
    __table_args__ = ({"schema": "events"},)

    projector: Mapped[str] = mapped_column(String(100), primary_key=True)
    partition_key: Mapped[str] = mapped_column(
        String(100), primary_key=True, default="default", server_default="default"
    )
    source_event_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("events.outbox_events.id", ondelete="SET NULL")
    )
    source_position: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
