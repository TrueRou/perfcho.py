"""Load and mutate authorization through SQLAlchemy-backed canonical grants."""

import uuid
from datetime import datetime

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra.db.enums import GrantEffect
from perfcho.infra.db.models.authz import (
    AccountEntitlementGrant,
    AccountPermissionGrant,
    AccountRoleGrant,
    Entitlement,
    Permission,
    Role,
    RolePermission,
)
from perfcho.infra.db.models.core import Account
from perfcho.modules.authorization.commands import AuthorizationGrant
from perfcho.modules.authorization.models import EffectiveAuthorization
from perfcho.modules.common.errors import ResourceConflict, ResourceNotFound


class SqlAlchemyAuthorizationRepository:
    """Query active grants without exposing persistence entities to callers."""

    def __init__(self, session: AsyncSession) -> None:
        """Bind a request-owned asynchronous database session."""
        self._session = session

    async def get_effective(self, account_id: int, *, at: datetime) -> EffectiveAuthorization:
        """Resolve active grants with direct denies overriding every allow source."""
        role_statement = (
            select(Role.code, Permission.code)
            .select_from(AccountRoleGrant)
            .join(Role, Role.id == AccountRoleGrant.role_id)
            .outerjoin(RolePermission, RolePermission.role_id == Role.id)
            .outerjoin(Permission, Permission.id == RolePermission.permission_id)
            .where(
                AccountRoleGrant.account_id == account_id,
                AccountRoleGrant.revoked_at.is_(None),
                AccountRoleGrant.starts_at <= at,
                or_(AccountRoleGrant.ends_at.is_(None), AccountRoleGrant.ends_at > at),
            )
        )
        role_rows = (await self._session.execute(role_statement)).all()
        role_codes = {role_code for role_code, _ in role_rows}
        role_permission_codes = {permission_code for _, permission_code in role_rows if permission_code is not None}

        permission_statement = (
            select(Permission.code, AccountPermissionGrant.effect)
            .select_from(AccountPermissionGrant)
            .join(Permission, Permission.id == AccountPermissionGrant.permission_id)
            .where(
                AccountPermissionGrant.account_id == account_id,
                AccountPermissionGrant.revoked_at.is_(None),
                AccountPermissionGrant.starts_at <= at,
                or_(AccountPermissionGrant.ends_at.is_(None), AccountPermissionGrant.ends_at > at),
            )
        )
        permission_rows = (await self._session.execute(permission_statement)).all()
        directly_allowed = {
            permission_code for permission_code, effect in permission_rows if effect == GrantEffect.ALLOW
        }
        directly_denied = {permission_code for permission_code, effect in permission_rows if effect == GrantEffect.DENY}

        entitlement_statement = (
            select(Entitlement.code)
            .select_from(AccountEntitlementGrant)
            .join(Entitlement, Entitlement.id == AccountEntitlementGrant.entitlement_id)
            .where(
                AccountEntitlementGrant.account_id == account_id,
                AccountEntitlementGrant.revoked_at.is_(None),
                AccountEntitlementGrant.starts_at <= at,
                or_(AccountEntitlementGrant.ends_at.is_(None), AccountEntitlementGrant.ends_at > at),
            )
        )
        entitlement_codes = set((await self._session.scalars(entitlement_statement)).all())

        return EffectiveAuthorization(
            account_id=account_id,
            evaluated_at=at,
            permission_codes=frozenset((role_permission_codes | directly_allowed) - directly_denied),
            role_codes=frozenset(role_codes),
            entitlement_codes=frozenset(entitlement_codes),
        )

    async def account_exists(self, account_id: int) -> bool:
        """Return whether an account exists and lock it for a management write."""
        account = await self._session.scalar(select(Account.id).where(Account.id == account_id).with_for_update())
        return account is not None

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
        """Insert a role grant after rejecting an overlapping active grant."""
        role_id = await self._catalog_id(Role, role_code)
        await self._lock_account(account_id)
        statement = select(AccountRoleGrant.id).where(
            AccountRoleGrant.account_id == account_id,
            AccountRoleGrant.role_id == role_id,
            AccountRoleGrant.revoked_at.is_(None),
            or_(AccountRoleGrant.ends_at.is_(None), AccountRoleGrant.ends_at > starts_at),
        )
        if ends_at is not None:
            statement = statement.where(AccountRoleGrant.starts_at < ends_at)
        existing = await self._session.scalar(statement)
        if existing is not None:
            raise ResourceConflict("an overlapping role grant already exists")
        persisted = AccountRoleGrant(
            account_id=account_id,
            role_id=role_id,
            granted_by_id=granted_by_id,
            starts_at=starts_at,
            ends_at=ends_at,
            reason=reason,
        )
        self._session.add(persisted)
        await self._session.flush()
        return AuthorizationGrant(persisted.id, account_id, "role", role_code, None, starts_at, ends_at, None)

    async def revoke_role(
        self,
        grant_id: uuid.UUID,
        *,
        revoked_by_id: int,
        revoked_at: datetime,
        reason: str | None,
    ) -> AuthorizationGrant:
        """Mark one active role grant revoked."""
        persisted = await self._session.get(AccountRoleGrant, grant_id, with_for_update=True)
        if persisted is None:
            raise ResourceNotFound("role grant does not exist")
        if persisted.revoked_at is not None:
            raise ResourceConflict("role grant is already revoked")
        persisted.revoked_at = revoked_at
        persisted.revoked_by_id = revoked_by_id
        if reason:
            persisted.reason = reason
        role_code = await self._session.scalar(select(Role.code).where(Role.id == persisted.role_id))
        assert role_code is not None
        return AuthorizationGrant(
            persisted.id,
            persisted.account_id,
            "role",
            role_code,
            None,
            persisted.starts_at,
            persisted.ends_at,
            persisted.revoked_at,
        )

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
        """Insert a direct permission grant after rejecting overlap."""
        permission_id = await self._catalog_id(Permission, permission_code)
        await self._lock_account(account_id)
        statement = select(AccountPermissionGrant.id).where(
            AccountPermissionGrant.account_id == account_id,
            AccountPermissionGrant.permission_id == permission_id,
            AccountPermissionGrant.revoked_at.is_(None),
            or_(AccountPermissionGrant.ends_at.is_(None), AccountPermissionGrant.ends_at > starts_at),
        )
        if ends_at is not None:
            statement = statement.where(AccountPermissionGrant.starts_at < ends_at)
        existing = await self._session.scalar(statement)
        if existing is not None:
            raise ResourceConflict("an overlapping permission grant already exists")
        persisted = AccountPermissionGrant(
            account_id=account_id,
            permission_id=permission_id,
            effect=effect,
            granted_by_id=granted_by_id,
            starts_at=starts_at,
            ends_at=ends_at,
            reason=reason,
        )
        self._session.add(persisted)
        await self._session.flush()
        return AuthorizationGrant(
            persisted.id, account_id, "permission", permission_code, effect, starts_at, ends_at, None
        )

    async def revoke_permission(self, grant_id: uuid.UUID, *, revoked_at: datetime) -> AuthorizationGrant:
        """Mark one active direct permission grant revoked."""
        persisted = await self._session.get(AccountPermissionGrant, grant_id, with_for_update=True)
        if persisted is None:
            raise ResourceNotFound("permission grant does not exist")
        if persisted.revoked_at is not None:
            raise ResourceConflict("permission grant is already revoked")
        persisted.revoked_at = revoked_at
        permission_code = await self._session.scalar(
            select(Permission.code).where(Permission.id == persisted.permission_id)
        )
        assert permission_code is not None
        return AuthorizationGrant(
            persisted.id,
            persisted.account_id,
            "permission",
            permission_code,
            persisted.effect.value,
            persisted.starts_at,
            persisted.ends_at,
            persisted.revoked_at,
        )

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
        """Insert an entitlement grant after rejecting overlap."""
        entitlement_id = await self._catalog_id(Entitlement, entitlement_code)
        await self._lock_account(account_id)
        statement = select(AccountEntitlementGrant.id).where(
            AccountEntitlementGrant.account_id == account_id,
            AccountEntitlementGrant.entitlement_id == entitlement_id,
            AccountEntitlementGrant.revoked_at.is_(None),
            or_(AccountEntitlementGrant.ends_at.is_(None), AccountEntitlementGrant.ends_at > starts_at),
        )
        if ends_at is not None:
            statement = statement.where(AccountEntitlementGrant.starts_at < ends_at)
        existing = await self._session.scalar(statement)
        if existing is not None:
            raise ResourceConflict("an overlapping entitlement grant already exists")
        persisted = AccountEntitlementGrant(
            account_id=account_id,
            entitlement_id=entitlement_id,
            granted_by_id=granted_by_id,
            starts_at=starts_at,
            ends_at=ends_at,
            source=source,
            reference_id=reference_id,
        )
        self._session.add(persisted)
        await self._session.flush()
        return AuthorizationGrant(
            persisted.id, account_id, "entitlement", entitlement_code, None, starts_at, ends_at, None
        )

    async def revoke_entitlement(self, grant_id: uuid.UUID, *, revoked_at: datetime) -> AuthorizationGrant:
        """Mark one active entitlement grant revoked."""
        persisted = await self._session.get(AccountEntitlementGrant, grant_id, with_for_update=True)
        if persisted is None:
            raise ResourceNotFound("entitlement grant does not exist")
        if persisted.revoked_at is not None:
            raise ResourceConflict("entitlement grant is already revoked")
        persisted.revoked_at = revoked_at
        entitlement_code = await self._session.scalar(
            select(Entitlement.code).where(Entitlement.id == persisted.entitlement_id)
        )
        assert entitlement_code is not None
        return AuthorizationGrant(
            persisted.id,
            persisted.account_id,
            "entitlement",
            entitlement_code,
            None,
            persisted.starts_at,
            persisted.ends_at,
            persisted.revoked_at,
        )

    async def _lock_account(self, account_id: int) -> None:
        if await self._session.scalar(select(Account.id).where(Account.id == account_id).with_for_update()) is None:
            raise ResourceNotFound("account does not exist")

    async def _catalog_id(self, model: type[Role] | type[Permission] | type[Entitlement], code: str) -> int:
        if not code.strip():
            raise ResourceNotFound("authorization catalog entry does not exist")
        catalog_id = await self._session.scalar(select(model.id).where(model.code == code))
        if catalog_id is None:
            raise ResourceNotFound("authorization catalog entry does not exist")
        return catalog_id
