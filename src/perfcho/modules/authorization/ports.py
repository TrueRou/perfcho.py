"""Define persistence ports consumed by authorization services."""

import uuid
from datetime import datetime
from typing import Protocol, runtime_checkable

from perfcho.modules.authorization.commands import AuthorizationGrant
from perfcho.modules.authorization.models import EffectiveAuthorization


@runtime_checkable
class AuthorizationRepository(Protocol):
    """Load protocol-neutral effective authorization from authoritative grants."""

    async def get_effective(self, account_id: int, *, at: datetime) -> EffectiveAuthorization:
        """Return grants effective for an account at the supplied instant."""
        ...


class AuthorizationManagementRepository(AuthorizationRepository, Protocol):
    """Persist authorization grants without exposing storage entities."""

    async def account_exists(self, account_id: int) -> bool:
        """Return whether an authoritative account exists."""
        ...

    async def grant_role(
        self,
        *,
        account_id: int,
        role_code: str,
        starts_at: datetime,
        ends_at: datetime | None,
        granted_by_id: int,
        reason: str | None,
    ) -> AuthorizationGrant:
        """Create a non-overlapping active role grant."""
        ...

    async def revoke_role(
        self,
        grant_id: uuid.UUID,
        *,
        revoked_by_id: int,
        revoked_at: datetime,
        reason: str | None,
    ) -> AuthorizationGrant:
        """Revoke one active role grant."""
        ...

    async def grant_permission(
        self,
        *,
        account_id: int,
        permission_code: str,
        effect: str,
        starts_at: datetime,
        ends_at: datetime | None,
        granted_by_id: int,
        reason: str | None,
    ) -> AuthorizationGrant:
        """Create a non-overlapping direct permission grant."""
        ...

    async def revoke_permission(
        self,
        grant_id: uuid.UUID,
        *,
        revoked_at: datetime,
    ) -> AuthorizationGrant:
        """Revoke one active direct permission grant."""
        ...

    async def grant_entitlement(
        self,
        *,
        account_id: int,
        entitlement_code: str,
        starts_at: datetime,
        ends_at: datetime | None,
        granted_by_id: int,
        source: str,
        reference_id: str | None,
    ) -> AuthorizationGrant:
        """Create a non-overlapping entitlement grant."""
        ...

    async def revoke_entitlement(
        self,
        grant_id: uuid.UUID,
        *,
        revoked_at: datetime,
    ) -> AuthorizationGrant:
        """Revoke one active entitlement grant."""
        ...
