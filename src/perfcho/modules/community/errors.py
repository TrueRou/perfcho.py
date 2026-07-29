"""Define protocol-neutral channel and messaging failures."""

from perfcho.modules.common.errors import AuthorizationDenied, InputRejected, ResourceConflict, ResourceNotFound


class CommunityInputRejected(InputRejected):
    """Reject semantically invalid channel or message input."""

    code = "community_input_rejected"


class ChannelNotFound(ResourceNotFound):
    """Indicate that a visible channel does not exist."""

    code = "channel_not_found"


class MessageNotFound(ResourceNotFound):
    """Indicate that a message does not belong to the requested channel."""

    code = "message_not_found"


class ChannelAccessDenied(AuthorizationDenied):
    """Reject a channel operation not authorized by canonical policy."""

    code = "channel_access_denied"


class DirectMessageBlocked(AuthorizationDenied):
    """Reject a direct message while either participant blocks the other."""

    code = "direct_message_blocked"


class PrivateMessageRejected(AuthorizationDenied):
    """Reject a direct message under the recipient's private-message policy."""

    code = "private_message_rejected"


class AccountSilenced(AuthorizationDenied):
    """Reject message sending under an active silence policy decision."""

    code = "account_silenced"


class MessageIdempotencyConflict(ResourceConflict):
    """Indicate reuse of a client message UUID for a different message."""

    code = "message_idempotency_conflict"


class MembershipRejected(ResourceConflict):
    """Reject durable membership for a channel where it is not applicable."""

    code = "membership_rejected"
