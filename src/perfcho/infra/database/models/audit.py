import uuid

from sqlalchemy import BigInteger, Index, String, Text
from sqlalchemy.dialects.postgresql import INET, JSONB
from sqlalchemy.orm import Mapped, mapped_column

from perfcho.infra.database.base import DbBase
from perfcho.infra.database.mixins import BigIntIdentityMixin, CreatedAtMixin


class AuditEvent(BigIntIdentityMixin, CreatedAtMixin, DbBase):
    """Stores immutable audit records for sensitive administrative and security actions."""

    __tablename__ = "audit_events"
    __table_args__ = (
        Index("ix_audit_events_actor_created", "actor_account_id", "created_at"),
        Index("ix_audit_events_target_created", "target_type", "target_id", "created_at"),
        Index("ix_audit_events_request", "request_id"),
        {"schema": "audit"},
    )

    actor_account_id: Mapped[int | None] = mapped_column(BigInteger)
    actor_node_id: Mapped[uuid.UUID | None] = mapped_column(nullable=True)
    action: Mapped[str] = mapped_column(String(100), nullable=False)
    target_type: Mapped[str] = mapped_column(String(64), nullable=False)
    target_id: Mapped[str] = mapped_column(String(128), nullable=False)
    request_id: Mapped[uuid.UUID | None] = mapped_column(nullable=True)
    ip_address: Mapped[str | None] = mapped_column(INET)
    reason: Mapped[str | None] = mapped_column(Text)
    before: Mapped[dict[str, object] | None] = mapped_column(JSONB)
    after: Mapped[dict[str, object] | None] = mapped_column(JSONB)
    metadata_json: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False, default=dict, server_default="{}")
