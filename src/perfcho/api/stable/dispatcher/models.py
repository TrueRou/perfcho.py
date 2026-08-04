"""Runtime models shared by Stable packet dispatch adapters."""

from dataclasses import dataclass, field
from typing import Protocol

from perfcho.modules.common import ClientContext
from perfcho.modules.identity import ResolvedStableSession
from perfcho.modules.realtime import RealtimeSession
from perfcho.modules.realtime.stable.models import UserPresence, UserStats


@dataclass(slots=True)
class StableRuntimeContext:
    """Carry current wire projections while dispatching one poll."""

    identity: ResolvedStableSession
    realtime: RealtimeSession
    presence: UserPresence
    stats: UserStats
    client: ClientContext | None = None
    raw_token: str | None = field(default=None, repr=False)
    session_closed: bool = False


class MultiplayerRuntimeContext(Protocol):
    """Describe the Stable session fields needed by the multiplayer adapter."""

    @property
    def identity(self) -> ResolvedStableSession:
        """Return the authenticated Stable identity."""
        ...

    @property
    def realtime(self) -> RealtimeSession:
        """Return the current fenced realtime session."""
        ...

    @property
    def client(self) -> ClientContext | None:
        """Return normalized request client evidence when available."""
        ...


__all__ = ("MultiplayerRuntimeContext", "StableRuntimeContext")
