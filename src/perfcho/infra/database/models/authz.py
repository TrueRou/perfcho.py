from datetime import datetime

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from perfcho.infra.database.base import DbBase
from perfcho.infra.database.enums import GrantEffect, enum_type
from perfcho.infra.database.mixins import CreatedAtMixin, Uuid7PrimaryKeyMixin


class Permission(DbBase):
    """Defines independently grantable capabilities within the service."""

    __tablename__ = "permissions"
    __table_args__ = (UniqueConstraint("code"), {"schema": "authz"})

    id: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    code: Mapped[str] = mapped_column(String(100), nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False)


class Role(DbBase):
    """Defines named bundles of permissions that can be assigned to accounts."""

    __tablename__ = "roles"
    __table_args__ = (UniqueConstraint("code"), {"schema": "authz"})

    id: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    code: Mapped[str] = mapped_column(String(64), nullable=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    priority: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=1000, server_default="1000")
    system: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")


class RolePermission(DbBase):
    """Associates roles with the permissions they provide."""

    __tablename__ = "role_permissions"
    __table_args__ = ({"schema": "authz"},)

    role_id: Mapped[int] = mapped_column(ForeignKey("authz.roles.id", ondelete="CASCADE"), primary_key=True)
    permission_id: Mapped[int] = mapped_column(ForeignKey("authz.permissions.id", ondelete="CASCADE"), primary_key=True)


class AccountRoleGrant(Uuid7PrimaryKeyMixin, CreatedAtMixin, DbBase):
    """Tracks role assignments, validity periods, and revocations for accounts."""

    __tablename__ = "account_role_grants"
    __table_args__ = (
        CheckConstraint("ends_at IS NULL OR ends_at > starts_at", name="valid_period"),
        Index("ix_account_role_grants_account_active", "account_id", "starts_at", "ends_at"),
        Index("ix_account_role_grants_role_active", "role_id", "starts_at", "ends_at"),
        {"schema": "authz"},
    )

    account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="RESTRICT"), nullable=False)
    role_id: Mapped[int] = mapped_column(ForeignKey("authz.roles.id", ondelete="RESTRICT"), nullable=False)
    granted_by_id: Mapped[int | None] = mapped_column(ForeignKey("core.accounts.id", ondelete="SET NULL"))
    starts_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    ends_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    reason: Mapped[str | None] = mapped_column(String(255))
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    revoked_by_id: Mapped[int | None] = mapped_column(ForeignKey("core.accounts.id", ondelete="SET NULL"))


class AccountPermissionGrant(Uuid7PrimaryKeyMixin, CreatedAtMixin, DbBase):
    """Tracks direct allow or deny permission exceptions granted to accounts."""

    __tablename__ = "account_permission_grants"
    __table_args__ = (
        CheckConstraint("ends_at IS NULL OR ends_at > starts_at", name="valid_period"),
        Index("ix_account_permission_grants_account_active", "account_id", "starts_at", "ends_at"),
        {"schema": "authz"},
    )

    account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="RESTRICT"), nullable=False)
    permission_id: Mapped[int] = mapped_column(ForeignKey("authz.permissions.id", ondelete="RESTRICT"), nullable=False)
    effect: Mapped[GrantEffect] = mapped_column(enum_type(GrantEffect, "grant_effect", 8), nullable=False)
    granted_by_id: Mapped[int | None] = mapped_column(ForeignKey("core.accounts.id", ondelete="SET NULL"))
    starts_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    ends_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    reason: Mapped[str | None] = mapped_column(String(255))
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class Entitlement(DbBase):
    """Defines non-authorization benefits such as supporter or premium access."""

    __tablename__ = "entitlements"
    __table_args__ = (UniqueConstraint("code"), {"schema": "authz"})

    id: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    code: Mapped[str] = mapped_column(String(64), nullable=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    description: Mapped[str | None] = mapped_column(Text)


class AccountEntitlementGrant(Uuid7PrimaryKeyMixin, CreatedAtMixin, DbBase):
    """Tracks time-bounded product entitlements granted to accounts."""

    __tablename__ = "account_entitlement_grants"
    __table_args__ = (
        CheckConstraint("ends_at IS NULL OR ends_at > starts_at", name="valid_period"),
        Index("ix_account_entitlement_grants_account_active", "account_id", "starts_at", "ends_at"),
        {"schema": "authz"},
    )

    account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="RESTRICT"), nullable=False)
    entitlement_id: Mapped[int] = mapped_column(
        ForeignKey("authz.entitlements.id", ondelete="RESTRICT"), nullable=False
    )
    granted_by_id: Mapped[int | None] = mapped_column(ForeignKey("core.accounts.id", ondelete="SET NULL"))
    starts_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    ends_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    source: Mapped[str] = mapped_column(String(64), nullable=False)
    reference_id: Mapped[str | None] = mapped_column(String(128))
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
