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
    InvalidFrame,
    MailboxBatch,
    MailboxOverflow,
    MailboxPacket,
    PollLeaseConflict,
    PresenceSnapshot,
    RealtimeRepository,
    RealtimeSession,
    RealtimeSessionFenced,
    RealtimeSessionNotFound,
    SpectatorHostOffline,
    SpectatorRelation,
)

INSTANT = datetime(2026, 7, 29, 12, 0, tzinfo=UTC)
EXPIRY = INSTANT + timedelta(minutes=5)


def test_realtime_values_are_frozen_and_slotted() -> None:
    session = RealtimeSession(uuid.uuid7(), 42, 1, EXPIRY)

    assert not hasattr(session, "__dict__")
    with pytest.raises(FrozenInstanceError):
        session.__setattr__("revision", 2)


@pytest.mark.parametrize("account_id", [0, -1, True])
def test_realtime_session_requires_a_positive_account_id(account_id: int) -> None:
    with pytest.raises(ValueError, match="account_id must be a positive integer"):
        RealtimeSession(uuid.uuid7(), account_id, 0, EXPIRY)


@pytest.mark.parametrize("revision", [-1, MAX_REVISION + 1, True])
def test_revisions_are_bounded(revision: int) -> None:
    with pytest.raises(ValueError, match="revision must be between"):
        RealtimeSession(uuid.uuid7(), 42, revision, EXPIRY)
    with pytest.raises(ValueError, match="revision must be between"):
        PresenceSnapshot(42, revision, b"presence", EXPIRY)
    with pytest.raises(ValueError, match="revision must be between"):
        SpectatorRelation(42, 43, revision, EXPIRY)


@pytest.mark.parametrize(
    "factory",
    [
        lambda expiry: RealtimeSession(uuid.uuid7(), 42, 0, expiry),
        lambda expiry: PresenceSnapshot(42, 0, b"presence", expiry),
        lambda expiry: MailboxBatch(uuid.uuid7(), (), expiry),
        lambda expiry: SpectatorRelation(42, 43, 0, expiry),
    ],
)
def test_expiries_must_be_timezone_aware(factory: Callable[[datetime], object]) -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        factory(datetime(2026, 7, 29, 12, 5))


def test_payloads_and_packet_collections_are_defensively_frozen() -> None:
    presence_payload = bytearray(b"presence")
    packet_payload = bytearray(b"packet")
    presence = PresenceSnapshot(42, 1, cast(bytes, presence_payload), EXPIRY)
    packet = MailboxPacket(1, cast(bytes, memoryview(packet_payload)))
    packets = [packet]
    batch = MailboxBatch(uuid.uuid7(), cast(tuple[MailboxPacket, ...], packets), EXPIRY)

    presence_payload[:] = b"modified"
    packet_payload[:] = b"change"
    packets.clear()

    assert presence.payload == b"presence"
    assert type(presence.payload) is bytes
    assert packet.payload == b"packet"
    assert type(packet.payload) is bytes
    assert batch.packets == (packet,)


@pytest.mark.parametrize("sequence", [-1, MAX_SEQUENCE + 1, True])
def test_mailbox_sequence_is_bounded(sequence: int) -> None:
    with pytest.raises(ValueError, match="sequence must be between"):
        MailboxPacket(sequence, b"packet")


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
        SpectatorRelation(host_account_id, spectator_account_id, 0, EXPIRY)


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
        "enqueue_mailbox",
        "lease_mailbox",
        "ack_mailbox",
        "release_mailbox",
        "attach_spectator",
        "detach_spectator",
        "publish_spectator_frame",
        "read_spectator_frames",
    }

    assert getattr(RealtimeRepository, "_is_protocol", False)
    assert operations <= RealtimeRepository.__dict__.keys()


def test_realtime_errors_have_typed_categories_and_stable_codes() -> None:
    expected = (
        (RealtimeSessionNotFound, ResourceNotFound, "realtime_session_not_found"),
        (RealtimeSessionFenced, ResourceConflict, "realtime_session_fenced"),
        (PollLeaseConflict, ResourceConflict, "poll_lease_conflict"),
        (MailboxOverflow, ResourceConflict, "mailbox_overflow"),
        (SpectatorHostOffline, ResourceConflict, "spectator_host_offline"),
        (InvalidFrame, InputRejected, "invalid_frame"),
    )

    for error_type, base_type, code in expected:
        error = error_type()
        assert isinstance(error, base_type)
        assert error.code == code
        assert str(error) == code
