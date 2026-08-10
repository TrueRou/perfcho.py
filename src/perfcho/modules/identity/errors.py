"""Define protocol-neutral identity lifecycle errors."""

from perfcho.modules.common.errors import AuthenticationFailed, InputRejected, ResourceConflict


class InvalidCredentials(AuthenticationFailed):
    """Reject every failed identifier and password lookup identically."""

    code = "invalid_credentials"


class StableLoginRejected(InputRejected):
    """Reject malformed non-credential Stable login evidence."""

    code = "stable_login_rejected"


class StableSessionAlreadyActive(ResourceConflict):
    """Prevent a second normal Stable session for one account."""

    code = "stable_session_already_active"


class InvalidStableSession(AuthenticationFailed):
    """Reject a missing, expired, closed, or revoked Stable bearer session."""

    code = "invalid_stable_session"


class InvalidOAuthClient(AuthenticationFailed):
    """Reject an unknown, inactive, or mismatched OAuth client."""

    code = "invalid_oauth_client"


class InvalidOAuthGrant(AuthenticationFailed):
    """Reject invalid password or refresh-token grants without proof details."""

    code = "invalid_oauth_grant"


class InvalidAccessToken(AuthenticationFailed):
    """Reject an inactive OAuth access token or its owning session."""

    code = "invalid_access_token"
