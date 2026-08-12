import uuid
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime

import msgpack
import pytest

from perfcho.infra.redis.bubbles import decode_bubble, encode_bubble
from perfcho.modules.multiplayer import SlotStatus, TeamMode, WinCondition
from perfcho.modules.realtime import (
    CanonicalReplayFrame,
    CanonicalScoreFrame,
    ChannelMembershipAction,
    ChannelUpdatedBubble,
    ChatMessageBubble,
    MultiplayerRoomAction,
    MultiplayerRoomBubble,
    MultiplayerRoomSnapshot,
    MultiplayerScoreState,
    MultiplayerSignalBubble,
    MultiplayerSignalKind,
    MultiplayerSlotSnapshot,
    NotificationBubble,
    PlayerActivity,
    PlayerStatistics,
    PresenceIdentity,
    PresenceSnapshot,
    PresenceUpdatedBubble,
    SessionControlAction,
    SessionControlBubble,
    SpectatorAction,
    SpectatorFrameAction,
    SpectatorFrameBubble,
    SpectatorLifecycleBubble,
    UserLogoutBubble,
    presence_updated_bubble,
)
from perfcho.modules.scoring import CanonicalMod, Ruleset, ScoreboardVariant

INSTANT = datetime(2026, 8, 11, 12, 30, 45, 123000, tzinfo=UTC)

BUBBLES = (
    PresenceUpdatedBubble(
        42,
        "player",
        "JP",
        9,
        frozenset({"supporter", "player"}),
        PlayerActivity("playing", "map", 12, "checksum", "standard", ("HD", "DT")),
        PlayerStatistics(1000, 98.5, 20, 2000, 7, 123.4),
        139.7,
        35.6,
    ),
    UserLogoutBubble(42),
    ChatMessageBubble(9, 3, "general", 42, "player", "hello", False, INSTANT, False),
    ChannelUpdatedBubble(3, "general", "General", 20, ChannelMembershipAction.JOINED),
    MultiplayerRoomBubble(
        MultiplayerRoomAction.UPDATED,
        MultiplayerRoomSnapshot(
            7,
            3,
            1,
            42,
            False,
            "room",
            "map",
            12,
            b"m" * 16,
            Ruleset.OSU,
            ScoreboardVariant.VANILLA,
            TeamMode.HEAD_TO_HEAD,
            WinCondition.SCORE,
            (CanonicalMod("HD"),),
            False,
            0,
            (MultiplayerSlotSnapshot(0, SlotStatus.READY, 42),),
        ),
    ),
    MultiplayerSignalBubble(
        MultiplayerSignalKind.SCORE_UPDATED,
        7,
        42,
        0,
        MultiplayerScoreState(42, 100, 0, 1, 2, 3, 4, 5, 6, 999, 123, 12, False, 200, 0, True),
    ),
    SpectatorLifecycleBubble(SpectatorAction.ATTACHED_TO_HOST, 42, 43),
    SpectatorFrameBubble(
        42,
        3,
        SpectatorFrameAction.UPDATE,
        (CanonicalReplayFrame(123, 7.0, -2.0, 3, 1),),
        CanonicalScoreFrame(123, 0, 1, 2, 3, 4, 5, 6, 1000, 20, 10, False, 200, 0, False),
        1,
    ),
    NotificationBubble("notice"),
    SessionControlBubble(SessionControlAction.RECONNECT, 250),
)


@pytest.mark.parametrize("bubble", BUBBLES)
def test_all_bubbles_are_frozen_slotted_and_round_trip(bubble: object) -> None:
    assert not hasattr(bubble, "__dict__")
    with pytest.raises(FrozenInstanceError):
        bubble.__setattr__("unexpected", True)
    assert decode_bubble(encode_bubble(bubble)) == bubble


def test_codec_uses_explicit_messagepack_v1_envelope() -> None:
    payload = encode_bubble(BUBBLES[2])
    envelope = msgpack.unpackb(payload, raw=False)

    assert envelope["v"] == 1
    assert envelope["kind"] == "chat.message"
    assert envelope["body"]["created_at"] == 1786451445123
    assert decode_bubble(payload) == BUBBLES[2]


@pytest.mark.parametrize(
    "payload",
    [
        b"not-messagepack",
        msgpack.packb({"v": 2, "kind": "notification", "body": {"message": "x"}}),
        msgpack.packb({"v": 1, "kind": "unknown", "body": {}}),
        msgpack.packb({"v": 1, "kind": "notification", "body": {"message": "x", "extra": True}}),
        msgpack.packb({"v": 1, "kind": "notification", "body": {"message": 42}}),
        msgpack.packb([1, 2, 3]),
    ],
)
def test_codec_rejects_malformed_unknown_or_non_whitelisted_payloads(payload: bytes) -> None:
    assert decode_bubble(payload) is None


def test_codec_rejects_malformed_nested_model_data() -> None:
    envelope = msgpack.unpackb(encode_bubble(BUBBLES[4]), raw=False)
    envelope["body"]["room"]["mods"] = [None]

    assert decode_bubble(msgpack.packb(envelope, use_bin_type=True)) is None


def test_bubble_types_do_not_expose_raw_wire_payloads() -> None:
    for bubble in BUBBLES:
        assert "payload" not in getattr(bubble, "__dataclass_fields__", {})
        assert all(
            not isinstance(value, bytes) for value in (getattr(bubble, field) for field in bubble.__dataclass_fields__)
        )


def test_codec_rejects_local_multiplayer_admission_credentials() -> None:
    room = BUBBLES[4].room
    local_join = MultiplayerRoomBubble(MultiplayerRoomAction.JOINED, room, "secret")

    with pytest.raises(ValueError, match="cannot be encoded"):
        encode_bubble(local_join)
    payload = msgpack.packb(
        {
            "v": 1,
            "kind": "multiplayer.room",
            "body": {
                "action": "joined",
                "room": msgpack.unpackb(encode_bubble(BUBBLES[4]), raw=False)["body"]["room"],
                "local_admission_credential": "secret",
            },
        },
        use_bin_type=True,
    )
    assert decode_bubble(payload) is None


def test_models_preserve_canonical_collections() -> None:
    presence = BUBBLES[0]
    spectator = BUBBLES[7]

    assert isinstance(presence.privileges, frozenset)
    assert isinstance(presence.activity.mods, tuple)
    assert isinstance(spectator.frames, tuple)
    assert spectator.frames[0].position_x == 7.0


def test_session_ids_remain_transport_metadata_not_bubble_body() -> None:
    marker = uuid.uuid7().bytes
    assert marker not in encode_bubble(NotificationBubble("safe"))


def test_presence_bubble_factory_copies_complete_canonical_snapshot() -> None:
    snapshot = PresenceSnapshot(
        42,
        3,
        PresenceIdentity("player", "JP", 9, frozenset({"account.login"}), 139.7, 35.6),
        PlayerActivity("playing", "map", 12, "checksum", "osu", ("HD",)),
        PlayerStatistics(1000, 0.985, 20, 2000, 7, 123),
        INSTANT,
        uuid.uuid7(),
    )

    assert presence_updated_bubble(snapshot) == PresenceUpdatedBubble(
        42,
        "player",
        "JP",
        9,
        frozenset({"account.login"}),
        snapshot.activity,
        snapshot.statistics,
        139.7,
        35.6,
    )
