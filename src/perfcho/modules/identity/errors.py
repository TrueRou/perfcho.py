"""Define protocol-neutral identity lifecycle errors."""

from perfcho.modules.common.errors import AuthenticationFailed, ResourceConflict


class InvalidCredentials(AuthenticationFailed):
    """Reject every failed identifier and password lookup identically."""

    code = "invalid_credentials"


class SessionAlreadyActive(ResourceConflict):
    """Prevent a second direct client session for one account."""

    code = "session_already_active"


class InvalidSession(AuthenticationFailed):
    """Reject a missing, expired, closed, or revoked bearer session."""

    code = "invalid_session"


class InvalidOAuthClient(AuthenticationFailed):
    """Reject an unknown, inactive, or mismatched OAuth client."""

    code = "invalid_oauth_client"


class InvalidOAuthGrant(AuthenticationFailed):
    """Reject invalid password or refresh-token grants without proof details."""

    code = "invalid_oauth_grant"


class InvalidAccessToken(AuthenticationFailed):
    """Reject an inactive OAuth access token or its owning session."""

    code = "invalid_access_token"
