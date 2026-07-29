"""Define transaction-bound ports consumed by community services."""

import uuid
from datetime import datetime
from typing import Protocol

from perfcho.modules.authorization.ports import AuthorizationRepository
from perfcho.modules.common.ports import OutboxWriter, UnitOfWork
from perfcho.modules.community.models import (
    ActiveSilence,
    ChannelRecord,
    DirectConversationResult,
    DirectMessageContext,
    MessageResult,
    OfflineDirectMessage,
    ReadCursorResult,
)


class CommunityUnitOfWork(UnitOfWork, Protocol):
    """Expose the transaction resource used to bind community adapters."""

    @property
    def session(self) -> object:
        """Return the active transaction resource."""
        ...


class ActiveSilencePolicy(Protocol):
    """Resolve authoritative active message sanctions at one instant."""

    async def get_active_silence(
        self,
        account_id: int,
        *,
        channel_id: int,
        at: datetime,
    ) -> ActiveSilence | None:
        """Return a global or matching channel silence when active."""
        ...


class CommunityRepository(Protocol):
    """Persist and query channels and messages without exposing ORM entities."""

    async def acquire_pair_lock(self, first_account_id: int, second_account_id: int) -> None:
        """Serialize direct-conversation work for an ordered account pair."""
        ...

    async def list_public_channels(self, account_id: int) -> tuple[ChannelRecord, ...]:
        """List active public channels with permission codes in one query."""
        ...

    async def get_public_channel_by_stable_name(self, stable_name: str, account_id: int) -> ChannelRecord | None:
        """Resolve one active public channel by its Stable-facing name."""
        ...

    async def get_channel(self, channel_id: int, account_id: int) -> ChannelRecord | None:
        """Load one channel and account-specific access relationships in one query."""
        ...

    async def get_direct_message_context(
        self,
        sender_account_id: int,
        recipient_account_id: int,
    ) -> DirectMessageContext:
        """Batch-load account existence, follows, blocks, and recipient PM policy."""
        ...

    async def get_or_create_direct_conversation(
        self,
        low_account_id: int,
        high_account_id: int,
        *,
        now: datetime,
    ) -> DirectConversationResult:
        """Return or create the unique durable direct-conversation channel."""
        ...

    async def get_message_by_client_id(
        self,
        sender_account_id: int,
        client_message_id: uuid.UUID,
    ) -> MessageResult | None:
        """Resolve a sender's globally idempotent client message UUID."""
        ...

    async def insert_message(
        self,
        *,
        channel_id: int,
        sender_account_id: int,
        client_message_id: uuid.UUID,
        content: str,
        is_action: bool,
        reply_to_id: int | None,
        now: datetime,
    ) -> MessageResult:
        """Insert a message or return the conflicting sender-UUID row."""
        ...

    async def message_belongs_to_channel(self, channel_id: int, message_id: int) -> bool:
        """Return whether a message belongs to a channel."""
        ...

    async def advance_read_cursor(
        self,
        channel_id: int,
        account_id: int,
        message_id: int,
        *,
        now: datetime,
    ) -> ReadCursorResult:
        """Advance a per-user channel cursor monotonically."""
        ...

    async def list_unread_direct_messages(
        self,
        account_id: int,
        *,
        limit: int,
    ) -> tuple[OfflineDirectMessage, ...]:
        """List unread incoming direct messages in ascending message order."""
        ...

    async def set_private_message_policy(self, account_id: int, policy: str, *, now: datetime) -> str:
        """Persist an account private-message policy and return it."""
        ...

    async def join_membership(self, channel_id: int, account_id: int, *, now: datetime) -> bool:
        """Create or reopen a durable channel membership."""
        ...

    async def leave_membership(self, channel_id: int, account_id: int, *, now: datetime) -> bool:
        """Close an active durable channel membership."""
        ...


class CommunityRepositoryFactory(Protocol):
    """Bind a community repository to the current transaction resource."""

    def __call__(self, session: object) -> CommunityRepository:
        """Return a transaction-bound repository."""
        ...


class AuthorizationRepositoryFactory(Protocol):
    """Bind the canonical authorization port to the current transaction."""

    def __call__(self, session: object) -> AuthorizationRepository:
        """Return a transaction-bound authorization repository."""
        ...


class ActiveSilencePolicyFactory(Protocol):
    """Bind active-silence policy evaluation to the current transaction."""

    def __call__(self, session: object) -> ActiveSilencePolicy:
        """Return a transaction-bound silence policy."""
        ...


class CommunityOutboxWriterFactory(Protocol):
    """Bind an outbox writer to the current community transaction."""

    def __call__(self, session: object) -> OutboxWriter:
        """Return a transaction-bound outbox writer."""
        ...
