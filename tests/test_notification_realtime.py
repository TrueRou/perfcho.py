"""Tests for durable-notification realtime delivery (BanchoBot DM)."""

from datetime import UTC, datetime

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.consumers import notification
from perfcho.consumers.notification import _recipient_ids, render_notification_text

NOW = datetime(2026, 8, 10, 12, tzinfo=UTC)


def test_render_notification_text_known_kinds() -> None:
    assert render_notification_text("achievement_unlocked", {}) == "You unlocked a new achievement!"
    assert (
        render_notification_text("beatmapset_status_changed", {"status": "ranked"})
        == "Your beatmapset status changed to ranked."
    )
    assert render_notification_text("beatmapset_synchronized", {}) == "Your beatmapset was updated."


def test_render_notification_text_skips_direct_message_and_unknown() -> None:
    assert render_notification_text("direct_message", {}) is None
    assert render_notification_text("something_new", {}) is None


def test_recipient_ids_dedupes_and_validates() -> None:
    assert _recipient_ids([1, 2, 1, 3, -1, "x", True]) == (1, 2, 3)
    assert _recipient_ids(None) == ()
    assert _recipient_ids("not-a-list") == ()


class _FakePresence:
    def __init__(self, account_id: int) -> None:
        self.account_id = account_id
        self.fence = ("fence", account_id)
        self.identity = type("I", (), {"display_name": f"user{account_id}"})()


class _FakeRealtime:
    def __init__(self, online: set[int]) -> None:
        self.online = online
        self.queries: list[int] = []

    async def get_presence(self, account_id: int, *, at: datetime) -> _FakePresence | None:
        self.queries.append(account_id)
        return _FakePresence(account_id) if account_id in self.online else None


class _FakeBubbles:
    def __init__(self) -> None:
        self.published: list[tuple[int, object]] = []

    async def publish(self, account_id: int, bubble: object) -> int:
        self.published.append((account_id, bubble))
        return 1


class _FakeSession:
    def __init__(self) -> None:
        self.advanced: tuple[str, str] | None = None


def _event(notification_id: int, kind: str, recipients: list[int], payload: dict | None = None) -> object:
    body = {
        "notification_id": notification_id,
        "recipient_account_ids": recipients,
        "kind": kind,
        "category": "chat",
        "resource_type": None,
        "resource_id": None,
        "payload": payload or {},
    }

    class _Event:
        schema_version = 1
        aggregate_type = "notification"
        aggregate_id = str(notification_id)
        payload = body

    return _Event()


@pytest.fixture
def handler_env(monkeypatch: pytest.MonkeyPatch) -> tuple[_FakeRealtime, _FakeBubbles, object]:
    realtime = _FakeRealtime({7})
    bubbles = _FakeBubbles()
    handler = notification.notification_realtime_handler(
        realtime=realtime, bubbles=bubbles, bot_account_id=1, bot_name="BanchoBot"
    )

    async def fake_advance(session: AsyncSession, event: object, *, consumer: str, partition_key: str) -> None:
        del event
        session.advanced = (consumer, partition_key)

    monkeypatch.setattr(notification, "advance_checkpoint", fake_advance)
    return realtime, bubbles, handler


@pytest.mark.asyncio
async def test_handler_delivers_to_online_recipients(handler_env: tuple) -> None:
    realtime, bubbles, handler = handler_env
    session = _FakeSession()

    await handler(session, _event(100, "achievement_unlocked", [7, 8]), "notification:100")

    assert realtime.queries == [7, 8]
    assert len(bubbles.published) == 1
    account_id, bubble = bubbles.published[0]
    assert account_id == 7
    assert bubble.sender_account_id == 1
    assert bubble.sender_name == "BanchoBot"
    assert bubble.content == "You unlocked a new achievement!"
    assert bubble.direct is True
    assert session.advanced == ("notification-realtime-consumer.v1", "notification:100")


@pytest.mark.asyncio
async def test_handler_skips_direct_message_kind(handler_env: tuple) -> None:
    realtime, bubbles, handler = handler_env
    session = _FakeSession()

    await handler(session, _event(101, "direct_message", [7]), "notification:101")

    assert realtime.queries == []
    assert bubbles.published == []
    assert session.advanced == ("notification-realtime-consumer.v1", "notification:101")


@pytest.mark.asyncio
async def test_handler_skips_offline_recipients(handler_env: tuple) -> None:
    realtime, bubbles, handler = handler_env
    session = _FakeSession()

    await handler(session, _event(102, "achievement_unlocked", [99]), "notification:102")

    assert realtime.queries == [99]
    assert bubbles.published == []
    assert session.advanced == ("notification-realtime-consumer.v1", "notification:102")
