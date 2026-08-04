"""Define protocol-neutral authorization management commands and results."""

import uuid
from dataclasses import dataclass
from datetime import datetime

from perfcho.modules.common.models import CommandMeta


@dataclass(frozen=True, slots=True)
class GrantRole:
    """Grant one catalog role to an account for an optional period."""

    meta: CommandMeta
    account_id: int
    role_code: str
    starts_at: datetime
    ends_at: datetime | None = None
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class RevokeRole:
    """Revoke one role grant."""

    meta: CommandMeta
    grant_id: uuid.UUID
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class GrantPermission:
    """Grant or deny one direct permission for an optional period."""

    meta: CommandMeta
    account_id: int
    permission_code: str
    effect: str
    starts_at: datetime
    ends_at: datetime | None = None
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class RevokePermission:
    """Revoke one direct permission grant."""

    meta: CommandMeta
    grant_id: uuid.UUID
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class GrantEntitlement:
    """Grant one product entitlement for an optional period."""

    meta: CommandMeta
    account_id: int
    entitlement_code: str
    starts_at: datetime
    source: str
    ends_at: datetime | None = None
    reference_id: str | None = None


@dataclass(frozen=True, slots=True)
class RevokeEntitlement:
    """Revoke one entitlement grant."""

    meta: CommandMeta
    grant_id: uuid.UUID


@dataclass(frozen=True, slots=True)
class AuthorizationGrant:
    """Return the durable identity of an authorization grant."""

    grant_id: uuid.UUID
    account_id: int
    kind: str
    code: str
    effect: str | None
    starts_at: datetime
    ends_at: datetime | None
    revoked_at: datetime | None
