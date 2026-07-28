"""Map structured settings and resumable maintenance state."""

from datetime import datetime

from sqlalchemy import Boolean, CheckConstraint, DateTime, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from perfcho.infra.db.base import DbBase
from perfcho.infra.db.mixins import TimestampMixin


class ServerSetting(TimestampMixin, DbBase):
    """Stores auditable structured server runtime settings."""

    __tablename__ = "server_settings"
    __table_args__ = ({"schema": "system"},)

    key: Mapped[str] = mapped_column(String(128), primary_key=True)
    value: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    secret: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")


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
