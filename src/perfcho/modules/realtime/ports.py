"""Define the infrastructure-neutral repository for ephemeral realtime state."""

import uuid
from datetime import datetime
from typing import Protocol, runtime_checkable

from perfcho.modules.realtime.models import (
    MailboxBatch,
    MailboxPacket,
    PresenceSnapshot,
    RealtimeSession,
    SessionFence,
    SpectatorAttachment,
    SpectatorFramePublish,
    SpectatorFrameWindow,
    SpectatorRelation,
)


@runtime_checkable
class RealtimeRepository(Protocol):
    """Coordinate fenced sessions, presence, delivery, and spectating state."""

    async def open_session(
        self,
        *,
        session_id: uuid.UUID,
        account_id: int,
        expires_at: datetime,
        durable_expires_at: datetime,
    ) -> RealtimeSession:
        """Open a new revision and fence any prior lifecycle for the session."""
        ...

    async def resolve_session(self, session_id: uuid.UUID, *, at: datetime) -> RealtimeSession:
        """Resolve a live session or raise RealtimeSessionNotFound."""
        ...

    async def heartbeat_session(
        self,
        session_id: uuid.UUID,
        *,
        expected_revision: int,
        expires_at: datetime,
    ) -> RealtimeSession:
        """Extend a session only when its fence revision remains current."""
        ...

    async def fence_session(self, session_id: uuid.UUID, *, expected_revision: int) -> None:
        """Expire a session and its owned state only when its revision is current."""
        ...

    async def set_presence(
        self,
        snapshot: PresenceSnapshot,
        *,
        session_id: uuid.UUID,
        capacity: int | None = None,
    ) -> None:
        """Replace presence, optionally claiming a slot in a bounded online index."""
        ...

    async def get_presence(self, account_id: int, *, at: datetime) -> PresenceSnapshot | None:
        """Return a live presence snapshot when one exists."""
        ...

    async def list_presences(self, *, at: datetime, limit: int) -> tuple[PresenceSnapshot, ...]:
        """Return a bounded online presence snapshot ordered by account ID."""
        ...

    async def clear_presence(self, account_id: int, *, expected_fence: SessionFence) -> bool:
        """Remove presence only when its full stored session epoch matches."""
        ...

    async def set_presence_filter(
        self,
        account_id: int,
        *,
        session_id: uuid.UUID,
        expected_revision: int,
        value: int,
    ) -> None:
        """Persist the current fenced session's Stable presence filter."""
        ...

    async def get_presence_filter(self, account_id: int) -> int:
        """Return the Stable presence filter, defaulting to no subscription."""
        ...

    async def set_away_message(
        self,
        account_id: int,
        *,
        session_id: uuid.UUID,
        expected_revision: int,
        message: str,
    ) -> None:
        """Persist a bounded away message for the current fenced session."""
        ...

    async def get_away_message(self, account_id: int) -> str:
        """Return an online account's current away message."""
        ...

    async def join_channel(
        self,
        channel_id: int,
        *,
        session_id: uuid.UUID,
        expected_revision: int,
    ) -> None:
        """Add the current fenced session to a channel membership set."""
        ...

    async def leave_channel(
        self,
        channel_id: int,
        *,
        session_id: uuid.UUID,
        expected_revision: int,
    ) -> None:
        """Remove the current fenced session from a channel membership set."""
        ...

    async def list_channel_members(self, channel_id: int) -> frozenset[int]:
        """Return the immutable account IDs currently joined to a channel."""
        ...

    async def enqueue_mailbox(
        self,
        account_id: int,
        payload: bytes,
        *,
        recipient_fence: SessionFence,
        expires_at: datetime,
    ) -> MailboxPacket:
        """Append an immutable packet or raise MailboxOverflow at the bound."""
        ...

    async def lease_mailbox(
        self,
        account_id: int,
        *,
        recipient_fence: SessionFence,
        lease_id: uuid.UUID,
        limit: int,
        expires_at: datetime,
    ) -> MailboxBatch:
        """Acquire an exclusive bounded poll lease and return ordered packets."""
        ...

    async def ack_mailbox(
        self,
        account_id: int,
        *,
        recipient_fence: SessionFence,
        lease_id: uuid.UUID,
        through_sequence: int,
    ) -> None:
        """Delete leased packets through an acknowledged sequence."""
        ...

    async def release_mailbox(
        self,
        account_id: int,
        *,
        recipient_fence: SessionFence,
        lease_id: uuid.UUID,
    ) -> None:
        """Release a poll lease without acknowledging its packets."""
        ...

    async def attach_spectator(
        self,
        host_account_id: int,
        spectator_account_id: int,
        *,
        relation_id: uuid.UUID,
        host_fence: SessionFence,
        spectator_fence: SessionFence,
        expires_at: datetime,
        history_limit: int,
    ) -> SpectatorAttachment:
        """Atomically attach and return the history-to-live handoff snapshot."""
        ...

    async def detach_spectator(
        self,
        host_account_id: int,
        spectator_account_id: int,
        *,
        relation_id: uuid.UUID,
        expected_revision: int,
        host_fence: SessionFence,
        spectator_fence: SessionFence,
    ) -> bool:
        """Remove only the exact relation and return whether it was detached."""
        ...

    async def get_spectator_relation(
        self,
        spectator_account_id: int,
        *,
        spectator_fence: SessionFence,
        at: datetime,
    ) -> SpectatorRelation | None:
        """Resolve one live spectator relation."""
        ...

    async def list_spectators(
        self,
        host_account_id: int,
        *,
        host_fence: SessionFence,
        at: datetime,
    ) -> tuple[SpectatorRelation, ...]:
        """Return live, fully fenced relations for one host."""
        ...

    async def publish_spectator_frame(
        self,
        host_account_id: int,
        *,
        host_fence: SessionFence,
        sequence: int,
        payload: bytes,
        expires_at: datetime,
    ) -> SpectatorFramePublish:
        """Roll history and atomically queue one live frame to fenced viewers."""
        ...

    async def read_spectator_frames(
        self,
        host_account_id: int,
        *,
        host_fence: SessionFence,
        after_cursor: int | None,
        limit: int,
        at: datetime,
    ) -> SpectatorFrameWindow:
        """Read a latest window or frames strictly after an internal cursor."""
        ...
