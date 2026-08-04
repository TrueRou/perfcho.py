"""Map structured settings and resumable maintenance state."""

from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, Index, LargeBinary, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from perfcho.infra.db.base import DbBase
from perfcho.infra.db.mixins import CreatedAtMixin, TimestampMixin


class MaintenanceState(TimestampMixin, DbBase):
    """Stores resumable maintenance task state and short-lived leases."""

    __tablename__ = "maintenance_states"
    __table_args__ = (
        CheckConstraint("lease_expires_at IS NULL OR lease_owner IS NOT NULL", name="lease_owner_required"),
        {"schema": "system"},
    )

    task: Mapped[str] = mapped_column(String(100), primary_key=True)
    state: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False, default=dict, server_default="{}")
    lease_owner: Mapped[str | None] = mapped_column(String(128))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class CommandReceipt(CreatedAtMixin, DbBase):
    """Stores bounded command idempotency claims and non-secret result references."""

    __tablename__ = "command_receipts"
    __table_args__ = (
        CheckConstraint("octet_length(request_digest) = 32", name="request_digest_length"),
        CheckConstraint("expires_at > created_at", name="valid_period"),
        Index("ix_command_receipts_expiry", "expires_at"),
        Index("ix_command_receipts_resource", "resource_type", "resource_id"),
        {"schema": "system"},
    )

    scope: Mapped[str] = mapped_column(String(100), primary_key=True)
    idempotency_key: Mapped[str] = mapped_column(String(160), primary_key=True)
    request_digest: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    resource_type: Mapped[str | None] = mapped_column(String(64))
    resource_id: Mapped[str | None] = mapped_column(String(128))
    result_snapshot: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False, default=dict, server_default="{}")
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
