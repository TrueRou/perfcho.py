"""Expose canonical authorization queries and Stable projections."""

from perfcho.modules.authorization.models import EffectiveAuthorization
from perfcho.modules.authorization.ports import AuthorizationRepository
from perfcho.modules.authorization.services import AuthorizationQueryService
from perfcho.modules.authorization.stable import StablePrivilege, project_stable_privileges

__all__ = (
    "AuthorizationQueryService",
    "AuthorizationRepository",
    "EffectiveAuthorization",
    "StablePrivilege",
    "project_stable_privileges",
)
