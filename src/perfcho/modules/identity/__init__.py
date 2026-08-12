"""Expose canonical identity lifecycle operations."""

from perfcho.modules.identity.errors import (
    InvalidAccessToken,
    InvalidCredentials,
    InvalidOAuthClient,
    InvalidOAuthGrant,
    InvalidSession,
    SessionAlreadyActive,
)
from perfcho.modules.identity.models import (
    AuthenticateClientSession,
    AuthenticatedAccount,
    ClientSessionResult,
    CredentialSnapshot,
    OAuthTokenResult,
    OnlineCredentialPrincipal,
    PasswordGrant,
    RefreshGrant,
    ResolvedClientSession,
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
    "InvalidSession",
    "OAuthTokenResult",
    "PasswordGrant",
    "RefreshGrant",
    "AuthenticateClientSession",
    "ClientSessionResult",
    "OnlineCredentialPrincipal",
    "ResolvedClientSession",
    "SessionAlreadyActive",
)
