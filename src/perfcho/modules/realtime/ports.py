"""Define the infrastructure-neutral repository for ephemeral realtime state."""

import uuid
from collections.abc import Sequence
from contextlib import AbstractAsyncContextManager
from datetime import datetime
from typing import Protocol, runtime_checkable

from perfcho.modules.realtime.bubbles import RealtimeBubble, SpectatorFrameBubble
from perfcho.modules.realtime.models import (
    PresenceSnapshot,
    PresenceSubscription,
    RealtimeSession,
    SessionFence,
    SpectatorAttachment,
    SpectatorFramePublish,
    SpectatorFrameWindow,
    SpectatorRelation,
)


@runtime_checkable
class RealtimeStateRepository(Protocol):
    """Coordinate fenced sessions, presence, channels, and spectating state."""

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

    async def set_presence_subscription(
        self,
        account_id: int,
        *,
        session_id: uuid.UUID,
        expected_revision: int,
        subscription: PresenceSubscription,
    ) -> None:
        """Persist the current fenced session's presence subscription."""
        ...

    async def get_presence_subscription(self, account_id: int) -> PresenceSubscription:
        """Return the current subscription, defaulting to no updates."""
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
        frame: SpectatorFrameBubble,
        reset_history: bool,
        expires_at: datetime,
    ) -> SpectatorFramePublish:
        """Advance canonical history and return currently valid spectator targets."""
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


@runtime_checkable
class RealtimeBubbleSubscription(Protocol):
    """Consume best-effort Bubbles for one session fence."""

    async def receive(self, *, timeout: float) -> RealtimeBubble | None:
        """Wait for one valid Bubble or return None at timeout."""
        ...

    async def drain(self, *, limit: int) -> tuple[RealtimeBubble, ...]:
        """Return up to limit currently buffered valid Bubbles."""
        ...

    async def aclose(self) -> None:
        """Release the dedicated subscription connection."""
        ...


@runtime_checkable
class RealtimeBubbleBus(Protocol):
    """Publish and subscribe to ephemeral session-scoped Bubbles."""

    async def publish(self, recipient_fence: SessionFence, bubble: RealtimeBubble) -> int:
        """Publish one Bubble and return the subscriber count."""
        ...

    async def publish_many(self, recipient_fences: Sequence[SessionFence], bubble: RealtimeBubble) -> int:
        """Publish one Bubble to many exact session epochs and return total subscribers."""
        ...

    def subscribe(self, recipient_fence: SessionFence) -> AbstractAsyncContextManager[RealtimeBubbleSubscription]:
        """Open a subscription confirmed before entering its context."""
        ...


@runtime_checkable
class RealtimePollGate(Protocol):
    """Prevent concurrent Polls from consuming the same session channel."""

    async def acquire(
        self,
        account_id: int,
        recipient_fence: SessionFence,
        gate_id: uuid.UUID,
        *,
        expires_at: datetime,
    ) -> bool:
        """Acquire a short gate only for the current session fence."""
        ...

    async def release(self, account_id: int, recipient_fence: SessionFence, gate_id: uuid.UUID) -> None:
        """Release the gate only when all owner components match."""
        ...
