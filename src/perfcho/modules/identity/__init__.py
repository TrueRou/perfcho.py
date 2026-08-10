"""Expose canonical Stable identity lifecycle operations."""

from perfcho.modules.identity.errors import (
    InvalidAccessToken,
    InvalidCredentials,
    InvalidOAuthClient,
    InvalidOAuthGrant,
    InvalidStableSession,
    StableLoginRejected,
    StableSessionAlreadyActive,
)
from perfcho.modules.identity.models import (
    AuthenticatedAccount,
    CredentialSnapshot,
    OAuthTokenResult,
    PasswordGrant,
    RefreshGrant,
    ResolvedStableSession,
    StableLogin,
    StableSessionResult,
    StableWebPrincipal,
)
from perfcho.modules.identity.ports import IdentityRepository
from perfcho.modules.identity.services import IdentityService

__all__ = (
    "AuthenticatedAccount",
    "CredentialSnapshot",
    "IdentityRepository",
    "IdentityService",
    "InvalidAccessToken",
    "InvalidCredentials",
    "InvalidOAuthClient",
    "InvalidOAuthGrant",
    "InvalidStableSession",
    "OAuthTokenResult",
    "PasswordGrant",
    "RefreshGrant",
    "ResolvedStableSession",
    "StableLogin",
    "StableLoginRejected",
    "StableSessionAlreadyActive",
    "StableSessionResult",
    "StableWebPrincipal",
)
