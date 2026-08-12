"""Runtime models shared by Stable packet dispatch adapters."""

from dataclasses import dataclass, field
from typing import Protocol

from perfcho.api.stable.realtime.models import UserPresence, UserStats
from perfcho.modules.common import ClientContext
from perfcho.modules.identity import ResolvedClientSession
from perfcho.modules.realtime import RealtimeBubble, RealtimeSession


@dataclass(slots=True)
class StableRuntimeContext:
    """Carry current wire projections while dispatching one poll."""

    identity: ResolvedClientSession
    realtime: RealtimeSession
    presence: UserPresence
    stats: UserStats
    client: ClientContext | None = None
    raw_token: str | None = field(default=None, repr=False)
    session_closed: bool = False
    local_bubbles: list[RealtimeBubble] = field(default_factory=list, repr=False)
    stable_output: bytearray = field(default_factory=bytearray, repr=False)


class MultiplayerRuntimeContext(Protocol):
    """Describe the Stable session fields needed by the multiplayer adapter."""

    @property
    def identity(self) -> ResolvedClientSession:
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

    @property
    def local_bubbles(self) -> list[RealtimeBubble]:
        """Return the request-local Bubble collection."""
        ...


__all__ = ("MultiplayerRuntimeContext", "StableRuntimeContext")
