"""Define errors shared by protocol-neutral application services."""


class ApplicationError(Exception):
    """Base an expected application failure on a durable machine-readable code."""

    code = "application_error"

    def __init__(self, message: str = "") -> None:
        """Create an error with a protocol-neutral public message."""
        super().__init__(message or self.code)


class InputRejected(ApplicationError):
    """Reject input that is syntactically valid but invalid for the operation."""

    code = "input_rejected"


class ResourceNotFound(ApplicationError):
    """Indicate that the requested authoritative resource does not exist."""

    code = "resource_not_found"


class ResourceConflict(ApplicationError):
    """Indicate that current authoritative state conflicts with an operation."""

    code = "resource_conflict"


class IdempotencyConflict(ResourceConflict):
    """Indicate reuse of an idempotency key for a different request."""

    code = "idempotency_conflict"


class ConcurrentModification(ResourceConflict):
    """Indicate that an aggregate changed after the caller observed it."""

    code = "concurrent_modification"


class AuthenticationFailed(ApplicationError):
    """Reject invalid authentication without exposing which proof failed."""

    code = "authentication_failed"


class SessionExpired(AuthenticationFailed):
    """Indicate that an authentication session is no longer current."""

    code = "session_expired"


class SessionRevoked(AuthenticationFailed):
    """Indicate that an authentication session was explicitly revoked."""

    code = "session_revoked"


class AuthorizationDenied(ApplicationError):
    """Reject an operation the authenticated actor may not perform."""

    code = "authorization_denied"


class AccountUnavailable(ApplicationError):
    """Indicate that account lifecycle or sanctions prevent an operation."""

    code = "account_unavailable"


class RateLimitExceeded(ApplicationError):
    """Reject work above the configured bounded admission rate."""

    code = "rate_limit_exceeded"


class DependencyUnavailable(ApplicationError):
    """Indicate a temporary failure in required infrastructure."""

    code = "dependency_unavailable"


class ProjectionUnavailable(ApplicationError):
    """Indicate that a required read model is not currently available."""

    code = "projection_unavailable"


class ObjectUnavailable(ApplicationError):
    """Indicate that a referenced object-store asset is not ready."""

    code = "object_unavailable"
