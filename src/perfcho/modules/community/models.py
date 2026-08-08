"""Define immutable Stable-facing channel and canonical messaging values."""

import uuid
from dataclasses import dataclass
from datetime import datetime


def _require_positive_id(name: str, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{name} must be a positive integer")


def _require_aware(name: str, value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")


@dataclass(frozen=True, slots=True)
class ChannelRecord:
    """Carry channel facts and account-specific relationships from persistence."""

    channel_id: int
    kind: str
    stable_name: str | None
    description: str | None
    owner_account_id: int | None
    team_id: int | None
    read_permission_code: str | None
    write_permission_code: str | None
    manage_permission_code: str | None
    auto_join: bool
    message_length_limit: int
    archived: bool
    direct_low_account_id: int | None = None
    direct_high_account_id: int | None = None
    active_member: bool = False
    active_team_member: bool = False

    def __post_init__(self) -> None:
        """Validate the channel identity and message bound."""
        _require_positive_id("channel_id", self.channel_id)
        if self.message_length_limit < 1:
            raise ValueError("message_length_limit must be positive")

    @property
    def direct(self) -> bool:
        """Return whether this channel is a direct-conversation specialization."""
        return self.direct_low_account_id is not None

    def includes_direct_account(self, account_id: int) -> bool:
        """Return whether an account is a direct-conversation participant."""
        return account_id in (self.direct_low_account_id, self.direct_high_account_id)


@dataclass(frozen=True, slots=True)
class StableChannel:
    """Describe a public channel using fields directly consumable by Stable adapters."""

    channel_id: int
    name: str
    topic: str
    auto_join: bool
    message_length_limit: int
    can_write: bool
    can_manage: bool

    def __post_init__(self) -> None:
        """Validate the Stable channel identity."""
        _require_positive_id("channel_id", self.channel_id)
        if not self.name.startswith("#"):
            raise ValueError("Stable channel names must start with #")


@dataclass(frozen=True, slots=True)
class ChannelPermissions:
    """Describe canonical read, write, and manage decisions for one account."""

    channel_id: int
    account_id: int
    can_read: bool
    can_write: bool
    can_manage: bool

    def __post_init__(self) -> None:
        """Validate both identifiers."""
        _require_positive_id("channel_id", self.channel_id)
        _require_positive_id("account_id", self.account_id)


@dataclass(frozen=True, slots=True)
class DirectConversationResult:
    """Return a durable direct-conversation channel and creation state."""

    channel_id: int
    low_account_id: int
    high_account_id: int
    message_length_limit: int
    created: bool

    def __post_init__(self) -> None:
        """Require a canonical account order."""
        _require_positive_id("channel_id", self.channel_id)
        if self.low_account_id >= self.high_account_id:
            raise ValueError("direct-conversation accounts must be ordered")


@dataclass(frozen=True, slots=True)
class DirectMessageContext:
    """Describe authoritative relationship and recipient PM policy for a pair."""

    existing_account_ids: frozenset[int]
    recipient_policy: str | None
    low_account_id: int
    high_account_id: int
    low_follows_high: bool
    high_follows_low: bool
    low_blocks_high: bool
    high_blocks_low: bool

    @property
    def mutual_friends(self) -> bool:
        """Return whether both account directions contain follows."""
        return self.low_follows_high and self.high_follows_low

    @property
    def blocked(self) -> bool:
        """Return whether either account direction contains a block."""
        return self.low_blocks_high or self.high_blocks_low

    def follows(self, actor_account_id: int, target_account_id: int) -> bool:
        """Return one directional follow from the canonical pair facts."""
        if (actor_account_id, target_account_id) == (self.low_account_id, self.high_account_id):
            return self.low_follows_high
        if (actor_account_id, target_account_id) == (self.high_account_id, self.low_account_id):
            return self.high_follows_low
        raise ValueError("accounts do not belong to the direct-message context")


@dataclass(frozen=True, slots=True)
class MessageResult:
    """Return one durable public or direct message independently of protocol packets."""

    message_id: int
    channel_id: int
    sender_account_id: int
    client_message_id: uuid.UUID
    content: str
    is_action: bool
    reply_to_id: int | None
    created_at: datetime
    direct_recipient_account_id: int | None = None
    created: bool = True
    resolved_channel: StableChannel | None = None

    def __post_init__(self) -> None:
        """Validate identifiers and creation time."""
        _require_positive_id("message_id", self.message_id)
        _require_positive_id("channel_id", self.channel_id)
        _require_positive_id("sender_account_id", self.sender_account_id)
        if self.reply_to_id is not None:
            _require_positive_id("reply_to_id", self.reply_to_id)
        if self.direct_recipient_account_id is not None:
            _require_positive_id("direct_recipient_account_id", self.direct_recipient_account_id)
        _require_aware("created_at", self.created_at)


@dataclass(frozen=True, slots=True)
class ReadCursorResult:
    """Return the monotonic read cursor after an advance attempt."""

    channel_id: int
    account_id: int
    last_read_message_id: int
    advanced: bool

    def __post_init__(self) -> None:
        """Validate cursor identifiers."""
        _require_positive_id("channel_id", self.channel_id)
        _require_positive_id("account_id", self.account_id)
        _require_positive_id("last_read_message_id", self.last_read_message_id)


@dataclass(frozen=True, slots=True)
class OfflineDirectMessage:
    """Describe one unread durable direct message for Stable offline delivery."""

    message_id: int
    channel_id: int
    sender_account_id: int
    sender_name: str
    client_message_id: uuid.UUID
    content: str
    is_action: bool
    created_at: datetime

    def __post_init__(self) -> None:
        """Validate message identifiers and creation time."""
        _require_positive_id("message_id", self.message_id)
        _require_positive_id("channel_id", self.channel_id)
        _require_positive_id("sender_account_id", self.sender_account_id)
        _require_aware("created_at", self.created_at)


@dataclass(frozen=True, slots=True)
class ConversationReadCursor:
    """Identify the latest delivered message read in one direct conversation."""

    channel_id: int
    message_id: int

    def __post_init__(self) -> None:
        """Validate both cursor identifiers."""
        _require_positive_id("channel_id", self.channel_id)
        _require_positive_id("message_id", self.message_id)


@dataclass(frozen=True, slots=True)
class OfflineDirectMessagePage:
    """Return one keyset page and the cursor needed to continue without starvation."""

    messages: tuple[OfflineDirectMessage, ...]
    next_after_message_id: int | None

    def __post_init__(self) -> None:
        """Require ascending messages and a continuation cursor at the page boundary."""
        message_ids = tuple(message.message_id for message in self.messages)
        if any(current >= following for current, following in zip(message_ids, message_ids[1:], strict=False)):
            raise ValueError("offline direct messages must be in ascending message order")
        if self.next_after_message_id is not None:
            _require_positive_id("next_after_message_id", self.next_after_message_id)
            if not message_ids or self.next_after_message_id != message_ids[-1]:
                raise ValueError("next cursor must identify the final message in the page")

    @property
    def read_cursors(self) -> tuple[ConversationReadCursor, ...]:
        """Collapse delivered messages to one highest cursor per conversation."""
        latest_by_channel: dict[int, int] = {}
        for message in self.messages:
            latest_by_channel[message.channel_id] = message.message_id
        return tuple(
            ConversationReadCursor(channel_id, message_id)
            for channel_id, message_id in sorted(latest_by_channel.items())
        )


@dataclass(frozen=True, slots=True)
class ChannelMembershipResult:
    """Return whether a channel membership operation changed durable history."""

    channel_id: int
    account_id: int
    joined: bool
    durable: bool
    changed: bool

    def __post_init__(self) -> None:
        """Validate membership identifiers."""
        _require_positive_id("channel_id", self.channel_id)
        _require_positive_id("account_id", self.account_id)


@dataclass(frozen=True, slots=True)
class ActiveSilence:
    """Describe an active global or channel-scoped message prohibition."""

    account_id: int
    reason: str
    ends_at: datetime | None
    channel_id: int | None = None

    def __post_init__(self) -> None:
        """Validate silence scope and optional expiry."""
        _require_positive_id("account_id", self.account_id)
        if self.channel_id is not None:
            _require_positive_id("channel_id", self.channel_id)
        if self.ends_at is not None:
            _require_aware("ends_at", self.ends_at)
