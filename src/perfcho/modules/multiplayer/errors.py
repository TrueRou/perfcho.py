"""Define protocol-independent multiplayer failures."""

from perfcho.modules.common.errors import ApplicationError


class MultiplayerError(ApplicationError):
    """Base class for multiplayer application failures."""

    code = "multiplayer_error"


class MatchNotFound(MultiplayerError):
    """Report an absent, closed, or expired room session."""

    code = "match_not_found"


class MatchPasswordRejected(MultiplayerError):
    """Reject an incorrect room password without disclosing room secrets."""

    code = "match_password_rejected"


class MatchPermissionDenied(MultiplayerError):
    """Reject a mutation that requires the current host."""

    code = "match_permission_denied"


class MatchFull(MultiplayerError):
    """Reject admission after all playable slots are occupied."""

    code = "match_full"


class MatchAlreadyJoined(MultiplayerError):
    """Reject joining a second active room."""

    code = "match_already_joined"


class MatchConcurrencyConflict(MultiplayerError):
    """Reject a state write based on a stale aggregate revision."""

    code = "match_concurrency_conflict"


class MatchStateRejected(MultiplayerError):
    """Reject a transition that is invalid for the current match state."""

    code = "match_state_rejected"


class MatchProjectionUnavailable(MultiplayerError):
    """Report that an ephemeral mutation must wait for projection recovery."""

    code = "match_projection_unavailable"
