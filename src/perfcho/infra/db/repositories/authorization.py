"""Load effective authorization from SQLAlchemy-backed canonical grants."""

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
from perfcho.modules.authorization.models import EffectiveAuthorization


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
