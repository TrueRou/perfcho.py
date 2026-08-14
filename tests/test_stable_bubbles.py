from datetime import UTC, datetime
from unittest.mock import patch

from perfcho.api.stable.authorization import StablePrivilege
from perfcho.api.stable.bubbles import StableBubbleRenderer
from perfcho.api.stable.canonize.scoring import LEGACY_MOD_BITS
from perfcho.api.stable.realtime import PacketReader, ServerPacket
from perfcho.modules.multiplayer import SlotStatus, TeamMode, WinCondition
from perfcho.modules.realtime import (
    CanonicalReplayFrame,
    CanonicalScoreFrame,
    ChannelMembershipAction,
    ChannelUpdatedBubble,
    ChatMessageBubble,
    MultiplayerInvitationState,
    MultiplayerRoomAction,
    MultiplayerRoomBubble,
    MultiplayerRoomSnapshot,
    MultiplayerScoreState,
    MultiplayerSignalBubble,
    MultiplayerSignalKind,
    MultiplayerSlotSnapshot,
    PlayerActivity,
    PlayerStatistics,
    PresenceUpdatedBubble,
    SessionControlAction,
    SessionControlBubble,
    SpectatorAction,
    SpectatorFrameAction,
    SpectatorFrameBubble,
    SpectatorLifecycleBubble,
    ToastBubble,
    UserLogoutBubble,
)
from perfcho.modules.scoring import Ruleset, ScoreboardVariant


def presence_bubble() -> PresenceUpdatedBubble:
    return PresenceUpdatedBubble(
        account_id=42,
        display_name="player",
        country_code="JP",
        utc_offset=9,
        privileges=frozenset({"account.login", "moderation.enforce", "supporter", "administrator", "admin.access"}),
        activity=PlayerActivity("playing", "map", 12, "a" * 32, "taiko", ("HD", "NC")),
        statistics=PlayerStatistics(1_000, 0.985, 20, 2_000, 7, 123),
        longitude=139.7,
        latitude=35.6,
    )


def test_renderer_presence_packet_contract_is_complete() -> None:
    packets = list(PacketReader(StableBubbleRenderer().render(presence_bubble()), packet_enum=ServerPacket))

    assert [packet.packet_type for packet in packets] == [ServerPacket.USER_PRESENCE, ServerPacket.USER_STATS]
    presence = packets[0].payload.read_user_presence()
    stats = packets[1].payload.read_user_stats()
    assert (presence.user_id, presence.username, presence.country_code, presence.mode, presence.global_rank) == (
        42,
        "player",
        111,
        1,
        7,
    )
    assert presence.privileges == int(
        StablePrivilege.PLAYER
        | StablePrivilege.MODERATOR
        | StablePrivilege.SUPPORTER
        | StablePrivilege.OWNER
        | StablePrivilege.DEVELOPER
    )
    assert (stats.action, stats.mode, stats.beatmap_id, stats.performance) == (2, 1, 12, 123)
    assert stats.mods == LEGACY_MOD_BITS["HD"] | LEGACY_MOD_BITS["NC"] | LEGACY_MOD_BITS["DT"]


def test_renderer_supports_logout_chat_channel_notification_and_session_control() -> None:
    renderer = StableBubbleRenderer()
    bubbles = (
        UserLogoutBubble(42),
        ChatMessageBubble(9, 3, "#general", 42, "player", "waves", True, datetime.now(UTC), False),
        ChannelUpdatedBubble(3, "general", "General", 20, ChannelMembershipAction.JOINED),
        ToastBubble("notice"),
        SessionControlBubble(SessionControlAction.RECONNECT, 250),
    )
    packets = list(PacketReader(b"".join(renderer.render(bubble) for bubble in bubbles), packet_enum=ServerPacket))

    assert [packet.packet_type for packet in packets] == [
        ServerPacket.USER_LOGOUT,
        ServerPacket.SEND_MESSAGE,
        ServerPacket.CHANNEL_JOIN_SUCCESS,
        ServerPacket.CHANNEL_INFO,
        ServerPacket.NOTIFICATION,
        ServerPacket.RESTART,
    ]
    assert packets[1].payload.read_message().text == "\x01ACTION waves\x01"
    assert packets[2].payload.read_string() == "#general"
    assert packets[3].payload.read_channel().player_count == 20
    assert packets[-1].payload.read_i32() == 250


def test_renderer_budget_drops_oversized_bubble_without_deferring_it() -> None:
    renderer = StableBubbleRenderer()
    first = renderer.render(ToastBubble("first"))
    third = renderer.render(UserLogoutBubble(42))

    rendered = renderer.render_many(
        (ToastBubble("first"), ToastBubble("x" * 200), UserLogoutBubble(42)),
        max_bytes=len(first) + len(third),
    )

    assert rendered == first + third


def test_renderer_drops_one_failed_bubble_and_continues() -> None:
    renderer = StableBubbleRenderer()
    expected = renderer.render(UserLogoutBubble(42))

    with (
        patch.object(renderer, "render", side_effect=[ValueError("broken projection"), expected]),
        patch("perfcho.api.stable.bubbles.log_event") as log_event,
        patch("perfcho.api.stable.bubbles.rate_limit", return_value=True),
    ):
        rendered = renderer.render_many((ToastBubble("broken"), UserLogoutBubble(42)), max_bytes=100)

    assert rendered == expected
    log_event.assert_called_once()
    assert log_event.call_args.args[:2] == ("WARNING", "stable.bubble.render_failed")


def multiplayer_snapshot() -> MultiplayerRoomSnapshot:
    return MultiplayerRoomSnapshot(
        7,
        1,
        16,
        42,
        False,
        "room",
        "",
        0,
        None,
        Ruleset.OSU,
        ScoreboardVariant.VANILLA,
        TeamMode.HEAD_TO_HEAD,
        WinCondition.SCORE,
        slots=(MultiplayerSlotSnapshot(0, SlotStatus.NOT_READY, 42),)
        + tuple(MultiplayerSlotSnapshot(position, SlotStatus.OPEN) for position in range(1, 16)),
    )


def test_renderer_supports_multiplayer_room_update() -> None:
    bubble = MultiplayerRoomBubble(MultiplayerRoomAction.UPDATED, multiplayer_snapshot())

    packet = next(PacketReader(StableBubbleRenderer().render(bubble), packet_enum=ServerPacket))
    assert packet.packet_type is ServerPacket.UPDATE_MATCH


def test_renderer_covers_multiplayer_room_lifecycle_and_local_join_password() -> None:
    renderer = StableBubbleRenderer()
    snapshot = multiplayer_snapshot()
    cases = (
        (MultiplayerRoomBubble(MultiplayerRoomAction.CREATED, snapshot), (ServerPacket.NEW_MATCH,)),
        (MultiplayerRoomBubble(MultiplayerRoomAction.UPDATED, snapshot), (ServerPacket.UPDATE_MATCH,)),
        (MultiplayerRoomBubble(MultiplayerRoomAction.DISPOSED, snapshot), (ServerPacket.DISPOSE_MATCH,)),
        (
            MultiplayerRoomBubble(MultiplayerRoomAction.JOINED, snapshot, "secret"),
            (ServerPacket.CHANNEL_KICK, ServerPacket.CHANNEL_JOIN_SUCCESS, ServerPacket.MATCH_JOIN_SUCCESS),
        ),
        (MultiplayerRoomBubble(MultiplayerRoomAction.ROUND_STARTED, snapshot), (ServerPacket.MATCH_START,)),
        (MultiplayerRoomBubble(MultiplayerRoomAction.ROUND_COMPLETED, snapshot), (ServerPacket.MATCH_COMPLETE,)),
        (MultiplayerRoomBubble(MultiplayerRoomAction.ROUND_ABORTED, snapshot), (ServerPacket.MATCH_ABORT,)),
        (MultiplayerRoomBubble(MultiplayerRoomAction.LEFT, snapshot), (ServerPacket.CHANNEL_KICK,)),
        (
            MultiplayerRoomBubble(MultiplayerRoomAction.KICKED, snapshot),
            (ServerPacket.CHANNEL_KICK, ServerPacket.MATCH_JOIN_FAIL),
        ),
    )

    for bubble, expected in cases:
        packets = list(PacketReader(renderer.render(bubble), packet_enum=ServerPacket))
        assert tuple(packet.packet_type for packet in packets) == expected

    joined = list(PacketReader(renderer.render(cases[3][0]), packet_enum=ServerPacket))[-1]
    assert joined.payload.read_multiplayer_match().password == "secret"
    updated = next(PacketReader(renderer.render(cases[1][0]), packet_enum=ServerPacket))
    assert updated.payload.read_multiplayer_match().password == ""


def test_renderer_covers_multiplayer_round_signals_losslessly() -> None:
    renderer = StableBubbleRenderer()
    score = MultiplayerScoreState(42, 100, 3, 1, 2, 3, 4, 5, 6, 999, 123, 12, False, 200, 7, True, 0.4, 0.6)
    cases = (
        (
            MultiplayerSignalBubble(MultiplayerSignalKind.PARTICIPANT_LOADING_COMPLETED, 7),
            ServerPacket.MATCH_ALL_PLAYERS_LOADED,
        ),
        (
            MultiplayerSignalBubble(MultiplayerSignalKind.FAILED, 7, actor_account_id=42, slot_position=3),
            ServerPacket.MATCH_PLAYER_FAILED,
        ),
        (
            MultiplayerSignalBubble(MultiplayerSignalKind.SKIPPED, 7, actor_account_id=42),
            ServerPacket.MATCH_PLAYER_SKIPPED,
        ),
        (MultiplayerSignalBubble(MultiplayerSignalKind.ALL_PLAYERS_SKIPPED, 7), ServerPacket.MATCH_SKIP),
        (
            MultiplayerSignalBubble(MultiplayerSignalKind.HOST_TRANSFERRED, 7, actor_account_id=42),
            ServerPacket.MATCH_TRANSFER_HOST,
        ),
        (
            MultiplayerSignalBubble(MultiplayerSignalKind.SCORE_UPDATED, 7, actor_account_id=42, score=score),
            ServerPacket.MATCH_SCORE_UPDATE,
        ),
        (
            MultiplayerSignalBubble(
                MultiplayerSignalKind.INVITED,
                7,
                actor_account_id=42,
                invitation=MultiplayerInvitationState(42, "host", "target", "room", "token"),
            ),
            ServerPacket.MATCH_INVITE,
        ),
        (MultiplayerSignalBubble(MultiplayerSignalKind.JOIN_FAILED, None), ServerPacket.MATCH_JOIN_FAIL),
    )

    packets = [next(PacketReader(renderer.render(bubble), packet_enum=ServerPacket)) for bubble, _ in cases]
    assert [packet.packet_type for packet in packets] == [expected for _, expected in cases]
    assert packets[1].payload.read_i32() == 3
    assert packets[2].payload.read_i32() == 42
    frame = packets[5].payload.read_score_frame()
    assert (frame.time, frame.frame_id, frame.total_score, frame.tag_byte, frame.combo_portion) == (100, 3, 999, 7, 0.4)
    invitation = packets[6].payload.read_message()
    assert invitation.recipient == "target" and "osump://7/token room" in invitation.text


def test_renderer_covers_all_spectator_events_and_frame_packet() -> None:
    renderer = StableBubbleRenderer()
    lifecycle = (
        (SpectatorAction.ATTACHED_TO_HOST, ServerPacket.SPECTATOR_JOINED),
        (SpectatorAction.DETACHED_FROM_HOST, ServerPacket.SPECTATOR_LEFT),
        (SpectatorAction.FELLOW_ATTACHED, ServerPacket.FELLOW_SPECTATOR_JOINED),
        (SpectatorAction.FELLOW_DETACHED, ServerPacket.FELLOW_SPECTATOR_LEFT),
        (SpectatorAction.PLAYBACK_UNAVAILABLE, ServerPacket.SPECTATOR_CANT_SPECTATE),
    )
    packets = [
        next(PacketReader(renderer.render(SpectatorLifecycleBubble(action, 42, 43)), packet_enum=ServerPacket))
        for action, _ in lifecycle
    ]
    assert [packet.packet_type for packet in packets] == [expected for _, expected in lifecycle]
    assert [packet.payload.read_i32() for packet in packets] == [43] * len(lifecycle)

    frame = SpectatorFrameBubble(
        42,
        7,
        SpectatorFrameAction.UPDATE,
        (CanonicalReplayFrame(10, 1.5, 2.5, 3, 4),),
        CanonicalScoreFrame(10, 1, 2, 3, 4, 5, 6, 7, 800, 9, 8, False, 200, 0, False),
        -1,
    )
    packet = next(PacketReader(renderer.render(frame), packet_enum=ServerPacket))
    assert packet.packet_type is ServerPacket.SPECTATE_FRAMES
    bundle = packet.payload.read_replay_frame_bundle()
    assert bundle.sequence == 7
    assert bundle.frames[0].button_state == 3
    assert bundle.score_frame.total_score == 800
