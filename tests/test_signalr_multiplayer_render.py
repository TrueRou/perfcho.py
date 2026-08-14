"""Tests for the lazer multiplayer hub signal rendering."""

import pytest

from perfcho.api.signalr.hubs import MultiplayerHub
from perfcho.modules.realtime import (
    MultiplayerInvitationState,
    MultiplayerSignalBubble,
    MultiplayerSignalKind,
)


class _FakeCaller:
    def __init__(self) -> None:
        self.sent: list[tuple[str, tuple[object, ...]]] = []

    async def send(self, method: str, *args: object) -> None:
        self.sent.append((method, args))


class _FakeClients:
    def __init__(self, caller: _FakeCaller) -> None:
        self.caller = caller


class _RecordingMultiplayerHub(MultiplayerHub):
    def __init__(self) -> None:
        super().__init__()
        self._fake_caller = _FakeCaller()
        self._fake_clients = _FakeClients(self._fake_caller)

    @property
    def clients(self) -> _FakeClients:
        return self._fake_clients


def _hub() -> _RecordingMultiplayerHub:
    return _RecordingMultiplayerHub()


@pytest.mark.asyncio
async def test_host_transferred_signal_renders_host_changed() -> None:
    hub = _hub()
    await hub.handle_bubble(
        MultiplayerSignalBubble(MultiplayerSignalKind.HOST_TRANSFERRED, 7, actor_account_id=42)
    )
    assert hub._fake_caller.sent == [("HostChanged", (42,))]


@pytest.mark.asyncio
async def test_invited_signal_renders_invited_with_admission() -> None:
    hub = _hub()
    invitation = MultiplayerInvitationState(1, "alice", "bob", "room", "adm.token")
    await hub.handle_bubble(
        MultiplayerSignalBubble(MultiplayerSignalKind.INVITED, 7, actor_account_id=1, invitation=invitation)
    )
    assert hub._fake_caller.sent == [("Invited", (1, 7, "adm.token"))]


@pytest.mark.asyncio
async def test_unmapped_signal_is_dropped() -> None:
    hub = _hub()
    await hub.handle_bubble(
        MultiplayerSignalBubble(MultiplayerSignalKind.PARTICIPANT_LOADING_COMPLETED, 7)
    )
    assert hub._fake_caller.sent == []
