"""Expose canonical authorization commands, queries, and projections."""

from perfcho.modules.authorization.commands import (
    AuthorizationGrant,
    GrantEntitlement,
    GrantPermission,
    GrantRole,
    RevokeEntitlement,
    RevokePermission,
    RevokeRole,
)
from perfcho.modules.authorization.management import AuthorizationManagementService, AuthorizationService
from perfcho.modules.authorization.models import EffectiveAuthorization
from perfcho.modules.authorization.ports import AuthorizationManagementRepository, AuthorizationRepository
from perfcho.modules.authorization.services import AuthorizationQueryService
from perfcho.modules.authorization.stable import StablePrivilege, project_stable_privileges

__all__ = (
    "AuthorizationQueryService",
    "AuthorizationGrant",
    "AuthorizationManagementRepository",
    "AuthorizationManagementService",
    "AuthorizationRepository",
    "AuthorizationService",
    "EffectiveAuthorization",
    "GrantEntitlement",
    "GrantPermission",
    "GrantRole",
    "RevokeEntitlement",
    "RevokePermission",
    "RevokeRole",
    "StablePrivilege",
    "project_stable_privileges",
)
