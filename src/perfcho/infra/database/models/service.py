import uuid
from datetime import datetime

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    LargeBinary,
    String,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from perfcho.infra.database.base import DbBase
from perfcho.infra.database.enums import ServiceTrustTier, enum_type
from perfcho.infra.database.mixins import CreatedAtMixin, Uuid7PrimaryKeyMixin


class Service(Uuid7PrimaryKeyMixin, CreatedAtMixin, DbBase):
    """Defines service identities, ownership, capabilities, and trust tiers."""

    __tablename__ = "services"
    __table_args__ = (
        CheckConstraint("char_length(name) > 0", name="name_nonempty"),
        UniqueConstraint("name_key"),
        Index("ix_services_status_trust", "status", "trust_tier"),
        {"schema": "service"},
    )

    name: Mapped[str] = mapped_column(String(100), nullable=False)
    name_key: Mapped[str] = mapped_column(String(100), nullable=False)
    owner_account_id: Mapped[int | None] = mapped_column(
        ForeignKey("core.accounts.id", ondelete="SET NULL"), index=True
    )
    trust_tier: Mapped[ServiceTrustTier] = mapped_column(
        enum_type(ServiceTrustTier, "service_trust_tier", 16), nullable=False
    )
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="active", server_default="active")
    capabilities: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list, server_default="[]")


class ServiceNode(Uuid7PrimaryKeyMixin, CreatedAtMixin, DbBase):
    """Represents a trusted or edge runtime node belonging to a service."""

    __tablename__ = "service_nodes"
    __table_args__ = (
        UniqueConstraint("service_id", "external_name"),
        Index("ix_service_nodes_service_status", "service_id", "status"),
        Index("ix_service_nodes_last_seen", "last_seen_at"),
        {"schema": "service"},
    )

    service_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("service.services.id", ondelete="RESTRICT"), nullable=False
    )
    external_name: Mapped[str] = mapped_column(String(100), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="active", server_default="active")
    region: Mapped[str | None] = mapped_column(String(32))
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ServiceNodeKey(DbBase):
    """Stores versioned signing public keys and validity periods for service nodes."""

    __tablename__ = "service_node_keys"
    __table_args__ = (
        CheckConstraint("key_version > 0", name="positive_key_version"),
        CheckConstraint("valid_until IS NULL OR valid_until > valid_from", name="valid_period"),
        UniqueConstraint("fingerprint"),
        Index("ix_service_node_keys_validity", "node_id", "valid_from", "valid_until"),
        {"schema": "service"},
    )

    node_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("service.service_nodes.id", ondelete="CASCADE"), primary_key=True
    )
    key_version: Mapped[int] = mapped_column(Integer, primary_key=True)
    algorithm: Mapped[str] = mapped_column(String(32), nullable=False, default="Ed25519", server_default="Ed25519")
    public_key: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    fingerprint: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    valid_from: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    valid_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class NodeCommandReceipt(Uuid7PrimaryKeyMixin, CreatedAtMixin, DbBase):
    """Records signed node commands, idempotency keys, versions, and outcomes."""

    __tablename__ = "node_command_receipts"

    node_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    key_version: Mapped[int] = mapped_column(nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    aggregate_type: Mapped[str] = mapped_column(String(64), nullable=False)
    aggregate_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    expected_version: Mapped[int] = mapped_column(Integer, nullable=False)
    request_hash: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    signature: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    response: Mapped[dict[str, object] | None] = mapped_column(JSONB)
    error_code: Mapped[str | None] = mapped_column(String(64))

    __table_args__ = (
        CheckConstraint("expected_version >= 0", name="nonnegative_version"),
        ForeignKeyConstraint(
            ["node_id", "key_version"],
            ["service.service_node_keys.node_id", "service.service_node_keys.key_version"],
            ondelete="RESTRICT",
        ),
        UniqueConstraint("node_id", "idempotency_key"),
        Index("ix_node_command_receipts_aggregate", "aggregate_type", "aggregate_id", "created_at"),
        Index("ix_node_command_receipts_status_created", "status", "created_at"),
        {"schema": "service"},
    )
