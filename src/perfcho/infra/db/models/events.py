"""Map transactional outbox, activity, and projection progress facts."""

import uuid
from datetime import datetime

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Identity,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from perfcho.infra.db.base import DbBase
from perfcho.infra.db.enums import OutboxDeliveryStatus, enum_type
from perfcho.infra.db.mixins import BigIntIdentityMixin, CreatedAtMixin, TimestampMixin, Uuid7PrimaryKeyMixin


class OutboxEvent(Uuid7PrimaryKeyMixin, CreatedAtMixin, DbBase):
    """Stores domain events committed atomically with business state for reliable publication."""

    __tablename__ = "outbox_event"
    __table_args__ = (
        CheckConstraint("schema_version > 0", name="positive_schema_version"),
        UniqueConstraint("position"),
        UniqueConstraint("id", "position", name="uq_outbox_event_id_position"),
        Index("ix_outbox_event_aggregate", "aggregate_type", "aggregate_id", "created_at"),
        Index("ix_outbox_event_trace_id", "trace_id", postgresql_where=text("trace_id IS NOT NULL")),
        {"schema": "event"},
    )

    position: Mapped[int] = mapped_column(BigInteger, Identity(always=True), nullable=False)
    aggregate_type: Mapped[str] = mapped_column(String(64), nullable=False)
    aggregate_id: Mapped[str] = mapped_column(String(128), nullable=False)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False)
    payload: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    trace_id: Mapped[uuid.UUID | None] = mapped_column()


class OutboxDelivery(TimestampMixin, DbBase):
    """Tracks durable delivery of one outbox event to one versioned consumer."""

    __tablename__ = "outbox_delivery"
    __table_args__ = (
        CheckConstraint("attempt_count >= 0 AND enqueue_count >= 0", name="nonnegative_attempt_counts"),
        CheckConstraint(
            "status != 'running' OR "
            "(lease_owner IS NOT NULL AND delivery_token IS NOT NULL AND lease_expires_at IS NOT NULL)",
            name="running_lease_required",
        ),
        CheckConstraint(
            "status = 'running' OR (lease_owner IS NULL AND delivery_token IS NULL AND lease_expires_at IS NULL)",
            name="nonrunning_lease_forbidden",
        ),
        CheckConstraint("status != 'succeeded' OR completed_at IS NOT NULL", name="succeeded_completion_required"),
        CheckConstraint("status = 'succeeded' OR completed_at IS NULL", name="non_succeeded_completion_forbidden"),
        CheckConstraint("status != 'dead' OR dead_lettered_at IS NOT NULL", name="dead_letter_required"),
        CheckConstraint("status = 'dead' OR dead_lettered_at IS NULL", name="non_dead_letter_forbidden"),
        ForeignKeyConstraint(
            ["event_id", "source_position"],
            ["event.outbox_event.id", "event.outbox_event.position"],
            name="fk_outbox_delivery_event_position",
            ondelete="CASCADE",
        ),
        Index(
            "ix_outbox_delivery_due",
            "available_at",
            "lease_expires_at",
            "source_position",
            postgresql_where=text("status IN ('pending', 'running')"),
        ),
        Index("ix_outbox_delivery_broker_task", "broker_task_id"),
        Index(
            "ix_outbox_delivery_partition",
            "consumer",
            "partition_key",
            "source_position",
            postgresql_where=text("status != 'succeeded'"),
        ),
        Index("ix_outbox_delivery_consumer_position", "consumer", "source_position"),
        {"schema": "event"},
    )

    event_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    consumer: Mapped[str] = mapped_column(String(100), primary_key=True)
    source_position: Mapped[int] = mapped_column(BigInteger, nullable=False)
    partition_key: Mapped[str] = mapped_column(String(100), nullable=False, default="default", server_default="default")
    status: Mapped[OutboxDeliveryStatus] = mapped_column(
        enum_type(OutboxDeliveryStatus, "outbox_delivery_status", 16),
        nullable=False,
        default=OutboxDeliveryStatus.PENDING,
        server_default=OutboxDeliveryStatus.PENDING.value,
    )
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    enqueued_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    dead_lettered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    enqueue_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    lease_owner: Mapped[str | None] = mapped_column(String(128))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    delivery_token: Mapped[uuid.UUID | None] = mapped_column()
    broker_task_id: Mapped[str | None] = mapped_column(String(128))
    last_error: Mapped[str | None] = mapped_column(Text)


class ActivityEvent(BigIntIdentityMixin, CreatedAtMixin, DbBase):
    """Stores user-visible activity projected from authoritative domain events."""

    __tablename__ = "activity_event"
    __table_args__ = (
        UniqueConstraint("source_event_id"),
        Index("ix_activity_event_subject_created", "subject_account_id", "created_at"),
        Index("ix_activity_event_type_created", "event_type", "created_at"),
        {"schema": "event"},
    )

    source_event_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("event.outbox_event.id", ondelete="RESTRICT"), nullable=False
    )
    subject_account_id: Mapped[int] = mapped_column(ForeignKey("core.account.id", ondelete="CASCADE"), nullable=False)
    actor_account_id: Mapped[int | None] = mapped_column(ForeignKey("core.account.id", ondelete="SET NULL"))
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    visibility: Mapped[str] = mapped_column(String(16), nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    snapshot: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False, default=dict, server_default="{}")


class ProjectionCheckpoint(TimestampMixin, DbBase):
    """Tracks the event-processing watermark of each asynchronous projector."""

    __tablename__ = "projection_checkpoint"
    __table_args__ = ({"schema": "event"},)

    projector: Mapped[str] = mapped_column(String(100), primary_key=True)
    partition_key: Mapped[str] = mapped_column(
        String(100), primary_key=True, default="default", server_default="default"
    )
    source_event_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("event.outbox_event.id", ondelete="SET NULL")
    )
    source_position: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default="0")
