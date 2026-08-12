import uuid
from collections.abc import Callable
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta
from typing import cast

import pytest

from perfcho.modules.common.errors import InputRejected, ResourceConflict, ResourceNotFound
from perfcho.modules.realtime import (
    MAX_REVISION,
    MAX_SEQUENCE,
    CanonicalReplayFrame,
    CanonicalScoreFrame,
    InvalidFrame,
    PlayerActivity,
    PlayerStatistics,
    PresenceIdentity,
    PresenceSnapshot,
    RealtimeSession,
    RealtimeSessionFenced,
    RealtimeSessionNotFound,
    RealtimeStateRepository,
    SessionFence,
    SpectatorAttachment,
    SpectatorFrame,
    SpectatorFrameAction,
    SpectatorFramePublish,
    SpectatorFrameWindow,
    SpectatorHostOffline,
    SpectatorRecipient,
    SpectatorRelation,
)

INSTANT = datetime(2026, 7, 29, 12, 0, tzinfo=UTC)
EXPIRY = INSTANT + timedelta(minutes=5)
HOST_FENCE = SessionFence(uuid.uuid7(), 2)
SPECTATOR_FENCE = SessionFence(uuid.uuid7(), 3)


def presence(revision: int = 1, expiry: datetime = EXPIRY) -> PresenceSnapshot:
    return PresenceSnapshot(
        42,
        revision,
        PresenceIdentity("player", "JP", 9, frozenset({"account.login"})),
        PlayerActivity("idle"),
        PlayerStatistics(),
        expiry,
        uuid.uuid7(),
    )


def relation(revision: int = 1, expiry: datetime = EXPIRY) -> SpectatorRelation:
    return SpectatorRelation(
        42,
        43,
        uuid.uuid7(),
        revision,
        HOST_FENCE,
        SPECTATOR_FENCE,
        expiry,
    )


def frame(cursor: int = 1, sequence: int = 0) -> SpectatorFrame:
    return SpectatorFrame(
        cursor,
        42,
        sequence,
        SpectatorFrameAction.UPDATE,
        (CanonicalReplayFrame(10, 1.0, 2.0, 1, 0),),
        CanonicalScoreFrame(10, 1, 1, 0, 0, 0, 0, 0, 300, 1, 1, True, 255, 0, False),
        0,
    )


def test_realtime_values_are_frozen_and_slotted() -> None:
    session = RealtimeSession(uuid.uuid7(), 42, 1, EXPIRY)

    assert not hasattr(session, "__dict__")
    with pytest.raises(FrozenInstanceError):
        session.__setattr__("revision", 2)
    assert session.fence == SessionFence(session.session_id, 1)


@pytest.mark.parametrize("account_id", [0, -1, True])
def test_realtime_session_requires_a_positive_account_id(account_id: int) -> None:
    with pytest.raises(ValueError, match="account_id must be a positive integer"):
        RealtimeSession(uuid.uuid7(), account_id, 0, EXPIRY)


@pytest.mark.parametrize("revision", [-1, MAX_REVISION + 1, True])
def test_revisions_are_bounded(revision: int) -> None:
    with pytest.raises(ValueError, match="revision must be between"):
        RealtimeSession(uuid.uuid7(), 42, revision, EXPIRY)
    with pytest.raises(ValueError, match="revision must be between"):
        presence(revision)
    with pytest.raises(ValueError, match="revision must be between"):
        relation(revision)


@pytest.mark.parametrize(
    "factory",
    [
        lambda expiry: RealtimeSession(uuid.uuid7(), 42, 0, expiry),
        lambda expiry: presence(0, expiry),
        lambda expiry: relation(expiry=expiry),
    ],
)
def test_expiries_must_be_timezone_aware(factory: Callable[[datetime], object]) -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        factory(datetime(2026, 7, 29, 12, 5))


def test_canonical_presence_collections_are_defensively_frozen() -> None:
    privileges = {"account.login"}
    mods = ["HD"]
    snapshot = PresenceSnapshot(
        42,
        1,
        PresenceIdentity("player", "JP", 9, cast(frozenset[str], privileges)),
        PlayerActivity("playing", mods=cast(tuple[str, ...], mods)),
        PlayerStatistics(),
        EXPIRY,
        uuid.uuid7(),
    )
    privileges.clear()
    mods.clear()

    assert snapshot.identity.privileges == frozenset({"account.login"})
    assert snapshot.activity.mods == ("HD",)


def test_spectator_frame_uses_a_protocol_neutral_bounded_sequence() -> None:
    assert frame(sequence=0).sequence == 0
    assert frame(sequence=MAX_SEQUENCE).sequence == MAX_SEQUENCE
    with pytest.raises(ValueError, match="sequence must be between"):
        frame(sequence=MAX_SEQUENCE + 1)


def test_frame_windows_and_attachment_results_are_frozen() -> None:
    replay_frame = frame(cursor=7)
    window = SpectatorFrameWindow((replay_frame,), 7, 7, False)
    attached = SpectatorAttachment(relation(), window)
    recipients = (
        SpectatorRecipient(43, SPECTATOR_FENCE, EXPIRY),
        SpectatorRecipient(44, SessionFence(uuid.uuid7(), 1), EXPIRY),
    )
    published = SpectatorFramePublish(replay_frame, recipients)

    assert attached.history.frames == (replay_frame,)
    assert published.recipients == recipients
    with pytest.raises(ValueError, match="both be present"):
        SpectatorFrameWindow((), None, 1, False)


@pytest.mark.parametrize(
    ("host_account_id", "spectator_account_id", "message"),
    [(0, 2, "host_account_id"), (1, 0, "spectator_account_id"), (1, 1, "itself")],
)
def test_spectator_relation_requires_distinct_positive_accounts(
    host_account_id: int,
    spectator_account_id: int,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        SpectatorRelation(
            host_account_id,
            spectator_account_id,
            uuid.uuid7(),
            0,
            HOST_FENCE,
            SPECTATOR_FENCE,
            EXPIRY,
        )


def test_repository_protocol_covers_the_realtime_state_lifecycle() -> None:
    operations = {
        "open_session",
        "resolve_session",
        "heartbeat_session",
        "fence_session",
        "set_presence",
        "get_presence",
        "clear_presence",
        "join_channel",
        "leave_channel",
        "list_channel_members",
        "attach_spectator",
        "detach_spectator",
        "publish_spectator_frame",
        "read_spectator_frames",
    }

    assert getattr(RealtimeStateRepository, "_is_protocol", False)
    assert operations <= RealtimeStateRepository.__dict__.keys()


def test_realtime_errors_have_typed_categories_and_stable_codes() -> None:
    expected = (
        (RealtimeSessionNotFound, ResourceNotFound, "realtime_session_not_found"),
        (RealtimeSessionFenced, ResourceConflict, "realtime_session_fenced"),
        (SpectatorHostOffline, ResourceConflict, "spectator_host_offline"),
        (InvalidFrame, InputRejected, "invalid_frame"),
    )

    for error_type, base_type, code in expected:
        error = error_type()
        assert isinstance(error, base_type)
        assert error.code == code
        assert str(error) == code
