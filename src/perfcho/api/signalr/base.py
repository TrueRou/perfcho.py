"""Base lazer hub: authenticate the connection and bridge user events.

Every osu!lazer SignalR connection is authenticated with the client's OAuth
access token (sent as a Bearer header by ``HubClientConnector``). The hub
resolves that token to an account, publishes the ``account_id`` into the
connection's ``user_id`` so ``clients.user(...)`` can target it, and subscribes
the connection to the account's cross-worker event stream.
"""

import asyncio
from inspect import isawaitable
from typing import TYPE_CHECKING

from aiosignalr.server import Hub

from perfcho.api.signalr.auth import authenticate
from perfcho.infra.logging import log_event
from perfcho.modules.identity import InvalidAccessToken
from perfcho.modules.realtime import RealtimeBubble

if TYPE_CHECKING:
    from perfcho.infra.compose import StableServices

_UNAUTHORIZED_CODE = 4401


class PerfchoHub(Hub):
    """Base class authenticating a connection and routing cross-worker events."""

    def __init__(self) -> None:
        """Initialize per-connection state."""
        self.account_id: int | None = None
        self._bridge_task: asyncio.Task[None] | None = None
        self._connection = None

    # -- lifecycle ---------------------------------------------------------

    async def on_connected(self) -> None:
        """Authenticate the connection or close it before any invocation."""
        services = self._services()
        if services is None:
            await self._close(_UNAUTHORIZED_CODE, "Services unavailable.")
            return
        try:
            account = await authenticate(services, self.context.http_headers)
        except InvalidAccessToken:
            await self._close(_UNAUTHORIZED_CODE, "Unauthorized.")
            return
        self.account_id = account.account_id
        self.context.user_id = str(account.account_id)
        self._bridge_task = asyncio.create_task(self._bridge_loop())

    async def on_disconnected(self, exception: BaseException | None) -> None:
        """Stop the event bridge when the connection closes."""
        del exception
        task = self._bridge_task
        self._bridge_task = None
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    # -- cross-worker event bridge ----------------------------------------

    async def _bridge_loop(self) -> None:
        """Forward account-keyed events to this connection until it closes."""
        services = self._services()
        bubbles = services.bubbles if services is not None else None
        account_id = self.account_id
        if bubbles is None or account_id is None:
            return
        try:
            async with bubbles.subscribe(account_id) as subscription:
                while True:
                    bubble = await subscription.receive(timeout=30.0)
                    if bubble is None:
                        continue
                    try:
                        await self.handle_bubble(bubble)
                    except Exception as error:
                        log_event(
                            "WARNING",
                            "signalr.bubble.dispatch_failed",
                            exception=error,
                            hub=type(self).__name__,
                            bubble_type=type(bubble).__name__,
                        )
                    await subscription.acknowledge()
        except asyncio.CancelledError:
            pass
        except Exception as error:
            log_event(
                "WARNING",
                "signalr.bridge.failed",
                exception=error,
                hub=type(self).__name__,
                account_id=account_id,
            )

    async def handle_bubble(self, bubble: RealtimeBubble) -> None:
        """Translate one account event into hub-specific client invocations."""
        del bubble

    # -- helpers -----------------------------------------------------------

    def _services(self) -> StableServices | None:
        state = getattr(self.context, "state", None)
        if not isinstance(state, dict):
            return None
        return state.get("stable_services")

    async def _close(self, code: int, reason: str) -> None:
        close = getattr(self, "close", None)
        if not callable(close):
            raise RuntimeError("SignalR hub does not provide a close operation")
        result = close(code, reason)
        if not isawaitable(result):
            raise RuntimeError("SignalR hub close operation is not asynchronous")
        await result

    async def _caller(self, method: str, *args: object) -> None:
        await self.clients.caller.send(method, *args)
