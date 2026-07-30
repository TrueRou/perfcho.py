"""Define protocol-neutral channel and messaging failures."""

from datetime import datetime

from perfcho.modules.common.errors import (
    AuthorizationDenied,
    DependencyUnavailable,
    InputRejected,
    ResourceConflict,
    ResourceNotFound,
)


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


class _MessageSilenced(AuthorizationDenied):
    """Carry protocol-neutral sanction timing for message denial adapters."""

    def __init__(
        self,
        message: str,
        *,
        account_id: int,
        ends_at: datetime | None,
        remaining_seconds: int | None,
        channel_id: int | None,
    ) -> None:
        super().__init__(message)
        self.account_id = account_id
        self.ends_at = ends_at
        self.remaining_seconds = remaining_seconds
        self.channel_id = channel_id


class AccountSilenced(_MessageSilenced):
    """Reject message sending while the sender has an active silence."""

    code = "account_silenced"


class TargetAccountSilenced(_MessageSilenced):
    """Reject a direct message while its recipient has an active silence."""

    code = "target_account_silenced"


class ChannelMembershipUnavailable(DependencyUnavailable):
    """Indicate that authoritative active channel membership cannot be queried."""

    code = "channel_membership_unavailable"


class MessageIdempotencyConflict(ResourceConflict):
    """Indicate reuse of a client message UUID for a different message."""

    code = "message_idempotency_conflict"


class MembershipRejected(ResourceConflict):
    """Reject durable membership for a channel where it is not applicable."""

    code = "membership_rejected"
