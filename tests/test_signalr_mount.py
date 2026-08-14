"""Tests for the osu!lazer SignalR hub authentication and event bridge."""

import asyncio
import uuid
from datetime import UTC, datetime
from typing import cast

import pytest
from aiosignalr.server import HubContext

from perfcho.api.signalr.base import PerfchoHub
from perfcho.modules.identity import AuthenticatedAccount, InvalidAccessToken
from perfcho.modules.realtime import NotificationBubble, RealtimeBubble

NOW = datetime(2026, 8, 10, 12, tzinfo=UTC)


class _FakeIdentity:
    def __init__(self) -> None:
        self.invalid = False
        self.tokens: list[str] = []

    async def authenticate_access_token(self, token: str) -> AuthenticatedAccount:
        self.tokens.append(token)
        if self.invalid:
            raise InvalidAccessToken("invalid")
        return AuthenticatedAccount(
            account_id=42,
            current_name="Alice",
            account_type="user",
            country_code="JP",
            registered_at=NOW,
            last_seen_at=NOW,
            session_id=uuid.uuid7(),
            scope_codes=("public", "identify", "lazer"),
        )


class _FakeSubscription:
    def __init__(self, bubbles: list[RealtimeBubble]) -> None:
        self._bubbles = list(bubbles)
        self.acked = 0

    async def receive(self, *, timeout: float) -> RealtimeBubble | None:
        if self._bubbles:
            return self._bubbles.pop(0)
        await asyncio.sleep(0.001)
        return None

    async def acknowledge(self) -> None:
        self.acked += 1

    async def aclose(self) -> None:
        pass


class _FakeUserEvents:
    def __init__(self, bubbles: list[RealtimeBubble] = ()) -> None:
        self.subscription = _FakeSubscription(list(bubbles))

    def subscribe(self, account_id: int):
        class _Ctx:
            async def __aenter__(self):
                return self.parent.subscription

            async def __aexit__(self, *args):
                return None

        ctx = _Ctx()
        ctx.parent = self
        return ctx


class _SimpleServices:
    def __init__(self, identity, user_events) -> None:
        self.identity = identity
        self.user_events = user_events


class _RecordingHub(PerfchoHub):
    def __init__(self) -> None:
        super().__init__()
        self.received: list[RealtimeBubble] = []
        self.closed: list[tuple[int, str]] = []

    async def handle_bubble(self, bubble: RealtimeBubble) -> None:
        self.received.append(bubble)

    async def close(self, code: int = 1000, reason: str = "") -> None:
        self.closed.append((code, reason))
        await super().close(code, reason)


def _hub(identity, user_events, headers=None) -> tuple[_RecordingHub, HubContext]:
    hub = _RecordingHub()
    hub.context = HubContext(
        "conn-1",
        dict(headers or {"authorization": "Bearer access-value"}),
        {},
        state={"stable_services": _SimpleServices(identity, user_events)},
    )
    return hub, hub.context


@pytest.mark.asyncio
async def test_connected_authenticates_and_binds_user_id() -> None:
    identity = _FakeIdentity()
    events = _FakeUserEvents()
    hub, context = _hub(identity, events)

    await hub.on_connected()

    assert context.user_id == "42"
    assert hub.account_id == 42
    assert identity.tokens == ["access-value"]
    assert hub._bridge_task is not None
    await hub.on_disconnected(None)


@pytest.mark.asyncio
async def test_connected_rejects_invalid_token() -> None:
    identity = _FakeIdentity()
    identity.invalid = True
    events = _FakeUserEvents()
    hub, context = _hub(identity, events)

    await hub.on_connected()

    assert context.user_id is None
    assert hub.account_id is None
    assert hub.closed == [(4401, "Unauthorized.")]


@pytest.mark.asyncio
async def test_connected_rejects_missing_services() -> None:
    hub = _RecordingHub()
    hub.context = HubContext("conn-1", {}, {}, state={})
    await hub.on_connected()
    assert hub.closed == [(4401, "Services unavailable.")]


@pytest.mark.asyncio
async def test_bridge_forwards_account_events_to_hub() -> None:
    identity = _FakeIdentity()
    bubble = NotificationBubble("hello")
    events = _FakeUserEvents([bubble])
    hub, _ = _hub(identity, events)

    await hub.on_connected()
    for _ in range(20):
        if hub.received:
            break
        await asyncio.sleep(0.005)

    assert hub.received == [bubble]
    await hub.on_disconnected(None)


def test_build_signalr_apps_mounts_three_hubs() -> None:
    from perfcho.api.signalr.hubs import build_signalr_apps

    apps = build_signalr_apps()
    assert set(apps) == {
        "/signalr/spectator",
        "/signalr/multiplayer",
        "/signalr/metadata",
    }


def test_create_app_exposes_signalr_routes() -> None:
    from perfcho.main import create_app

    app = create_app()
    mounted = {route.path for route in app.routes if getattr(route, "path", "").startswith("/signalr")}
    assert mounted == {"/signalr/spectator", "/signalr/multiplayer", "/signalr/metadata"}
