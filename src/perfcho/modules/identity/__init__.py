"""Expose canonical Stable identity lifecycle operations."""

from perfcho.modules.identity.errors import (
    InvalidCredentials,
    InvalidStableSession,
    StableLoginRejected,
    StableSessionAlreadyActive,
)
from perfcho.modules.identity.models import CredentialSnapshot, ResolvedStableSession, StableLogin, StableSessionResult
from perfcho.modules.identity.ports import IdentityRepository
from perfcho.modules.identity.services import IdentityService

__all__ = (
    "CredentialSnapshot",
    "IdentityRepository",
    "IdentityService",
    "InvalidCredentials",
    "InvalidStableSession",
    "ResolvedStableSession",
    "StableLogin",
    "StableLoginRejected",
    "StableSessionAlreadyActive",
    "StableSessionResult",
)
