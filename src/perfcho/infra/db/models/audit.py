"""Map immutable sensitive-operation audit facts."""

import uuid

from sqlalchemy import BigInteger, Index, String, Text
from sqlalchemy.dialects.postgresql import INET, JSONB
from sqlalchemy.orm import Mapped, mapped_column

from perfcho.infra.db.base import DbBase
from perfcho.infra.db.mixins import BigIntIdentityMixin, CreatedAtMixin


class AuditEvent(BigIntIdentityMixin, CreatedAtMixin, DbBase):
    """Stores immutable audit records for sensitive administrative and security actions."""

    __tablename__ = "audit_event"
    __table_args__ = (
        Index("ix_audit_event_actor_created", "actor_account_id", "created_at"),
        Index("ix_audit_event_target_created", "target_type", "target_id", "created_at"),
        Index("ix_audit_event_request", "request_id"),
        {"schema": "audit"},
    )

    actor_account_id: Mapped[int | None] = mapped_column(BigInteger)
    action: Mapped[str] = mapped_column(String(100), nullable=False)
    target_type: Mapped[str] = mapped_column(String(64), nullable=False)
    target_id: Mapped[str] = mapped_column(String(128), nullable=False)
    request_id: Mapped[uuid.UUID | None] = mapped_column(nullable=True)
    ip_address: Mapped[str | None] = mapped_column(INET)
    reason: Mapped[str | None] = mapped_column(Text)
    before: Mapped[dict[str, object] | None] = mapped_column(JSONB)
    after: Mapped[dict[str, object] | None] = mapped_column(JSONB)
    metadata_json: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False, default=dict, server_default="{}")
