import math
import struct
from dataclasses import replace

import pytest

from perfcho.modules.realtime.stable import (
    BodyTooLargeError,
    Channel,
    ClientPacket,
    ClientStatus,
    CodecLimits,
    FrameCountExceededError,
    InvalidStringEncodingError,
    InvalidStringMarkerError,
    InvalidStructureError,
    ListTooLargeError,
    MalformedULEB128Error,
    Message,
    MultiplayerMatch,
    PacketCountExceededError,
    PacketReader,
    PacketTooLargeError,
    PacketWriter,
    ProtocolStateError,
    ReplayAction,
    ReplayFrame,
    ReplayFrameBundle,
    ScoreFrame,
    ServerPacket,
    StringTooLargeError,
    TrailingDataError,
    TruncatedHeaderError,
    TruncatedPayloadError,
    UserPresence,
    UserStats,
    build_packet,
    channel_info,
    login_reply,
    notification,
    pong,
    send_message,
    spectate_frames,
    update_match,
    user_presence,
    user_stats,
)

CLIENT_PACKET_INVENTORY = {
    "CHANGE_ACTION": 0,
    "SEND_PUBLIC_MESSAGE": 1,
    "LOGOUT": 2,
    "REQUEST_STATUS_UPDATE": 3,
    "PING": 4,
    "START_SPECTATING": 16,
    "STOP_SPECTATING": 17,
    "SPECTATE_FRAMES": 18,
    "ERROR_REPORT": 20,
    "CANT_SPECTATE": 21,
    "SEND_PRIVATE_MESSAGE": 25,
    "PART_LOBBY": 29,
    "JOIN_LOBBY": 30,
    "CREATE_MATCH": 31,
    "JOIN_MATCH": 32,
    "PART_MATCH": 33,
    "MATCH_CHANGE_SLOT": 38,
    "MATCH_READY": 39,
    "MATCH_LOCK": 40,
    "MATCH_CHANGE_SETTINGS": 41,
    "MATCH_START": 44,
    "MATCH_SCORE_UPDATE": 47,
    "MATCH_COMPLETE": 49,
    "MATCH_CHANGE_MODS": 51,
    "MATCH_LOAD_COMPLETE": 52,
    "MATCH_NO_BEATMAP": 54,
    "MATCH_NOT_READY": 55,
    "MATCH_FAILED": 56,
    "MATCH_HAS_BEATMAP": 59,
    "MATCH_SKIP_REQUEST": 60,
    "CHANNEL_JOIN": 63,
    "BEATMAP_INFO_REQUEST": 68,
    "MATCH_TRANSFER_HOST": 70,
    "FRIEND_ADD": 73,
    "FRIEND_REMOVE": 74,
    "MATCH_CHANGE_TEAM": 77,
    "CHANNEL_PART": 78,
    "RECEIVE_UPDATES": 79,
    "SET_AWAY_MESSAGE": 82,
    "IRC_ONLY": 84,
    "USER_STATS_REQUEST": 85,
    "MATCH_INVITE": 87,
    "MATCH_CHANGE_PASSWORD": 90,
    "TOURNAMENT_MATCH_INFO_REQUEST": 93,
    "USER_PRESENCE_REQUEST": 97,
    "USER_PRESENCE_REQUEST_ALL": 98,
    "TOGGLE_BLOCK_NON_FRIEND_DMS": 99,
    "TOURNAMENT_JOIN_MATCH_CHANNEL": 108,
    "TOURNAMENT_LEAVE_MATCH_CHANNEL": 109,
}

SERVER_PACKET_INVENTORY = {
    "USER_ID": 5,
    "SEND_MESSAGE": 7,
    "PONG": 8,
    "HANDLE_IRC_CHANGE_USERNAME": 9,
    "HANDLE_IRC_QUIT": 10,
    "USER_STATS": 11,
    "USER_LOGOUT": 12,
    "SPECTATOR_JOINED": 13,
    "SPECTATOR_LEFT": 14,
    "SPECTATE_FRAMES": 15,
    "VERSION_UPDATE": 19,
    "SPECTATOR_CANT_SPECTATE": 22,
    "GET_ATTENTION": 23,
    "NOTIFICATION": 24,
    "UPDATE_MATCH": 26,
    "NEW_MATCH": 27,
    "DISPOSE_MATCH": 28,
    "TOGGLE_BLOCK_NON_FRIEND_DMS": 34,
    "MATCH_JOIN_SUCCESS": 36,
    "MATCH_JOIN_FAIL": 37,
    "FELLOW_SPECTATOR_JOINED": 42,
    "FELLOW_SPECTATOR_LEFT": 43,
    "ALL_PLAYERS_LOADED": 45,
    "MATCH_START": 46,
    "MATCH_SCORE_UPDATE": 48,
    "MATCH_TRANSFER_HOST": 50,
    "MATCH_ALL_PLAYERS_LOADED": 53,
    "MATCH_PLAYER_FAILED": 57,
    "MATCH_COMPLETE": 58,
    "MATCH_SKIP": 61,
    "UNAUTHORIZED": 62,
    "CHANNEL_JOIN_SUCCESS": 64,
    "CHANNEL_INFO": 65,
    "CHANNEL_KICK": 66,
    "CHANNEL_AUTO_JOIN": 67,
    "BEATMAP_INFO_REPLY": 69,
    "PRIVILEGES": 71,
    "FRIENDS_LIST": 72,
    "PROTOCOL_VERSION": 75,
    "MAIN_MENU_ICON": 76,
    "MONITOR": 80,
    "MATCH_PLAYER_SKIPPED": 81,
    "USER_PRESENCE": 83,
    "RESTART": 86,
    "MATCH_INVITE": 88,
    "CHANNEL_INFO_END": 89,
    "MATCH_CHANGE_PASSWORD": 91,
    "SILENCE_END": 92,
    "USER_SILENCED": 94,
    "USER_PRESENCE_SINGLE": 95,
    "USER_PRESENCE_BUNDLE": 96,
    "USER_DM_BLOCKED": 100,
    "TARGET_IS_SILENCED": 101,
    "VERSION_UPDATE_FORCED": 102,
    "SWITCH_SERVER": 103,
    "ACCOUNT_RESTRICTED": 104,
    "RTX": 105,
    "MATCH_ABORT": 106,
    "SWITCH_TOURNAMENT_SERVER": 107,
}


def payload_writer(*, limits: CodecLimits | None = None) -> PacketWriter:
    return PacketWriter(limits=limits or CodecLimits())


def test_packet_enum_inventory_is_complete_and_numeric() -> None:
    assert {packet.name: packet.value for packet in ClientPacket} == CLIENT_PACKET_INVENTORY
    assert {packet.name: packet.value for packet in ServerPacket} == SERVER_PACKET_INVENTORY
    assert isinstance(ClientPacket.PING, int)
    assert isinstance(ServerPacket.PONG, int)


def test_exact_core_packet_bytes() -> None:
    assert pong() == b"\x08\x00\x00\x00\x00\x00\x00"
    assert login_reply(42) == b"\x05\x00\x00\x04\x00\x00\x00\x2a\x00\x00\x00"
    assert notification("hi") == b"\x18\x00\x00\x04\x00\x00\x00\x0b\x02hi"
    assert build_packet(0x1234, b"abc") == b"\x34\x12\x00\x03\x00\x00\x00abc"


def test_unicode_message_uses_utf8_byte_lengths_and_round_trips() -> None:
    message = Message(sender="", text="hé", recipient="#c", sender_id=7)
    encoded = send_message(message)
    expected = b"\x07\x00\x00\x0e\x00\x00\x00\x00\x0b\x03h\xc3\xa9\x0b\x02#c\x07\x00\x00\x00"

    assert encoded == expected
    packet = next(PacketReader(encoded, packet_enum=ServerPacket))
    assert packet.packet_type is ServerPacket.SEND_MESSAGE
    assert packet.payload.read_message() == message
    packet.payload.require_exhausted()


def test_concatenated_packets_have_exact_child_slices_and_cannot_desynchronize() -> None:
    original = bytearray(
        build_packet(ClientPacket.CHANGE_ACTION, b"\x01\x02")
        + build_packet(65535, b"unknown")
        + build_packet(ClientPacket.PING),
    )
    reader = PacketReader(memoryview(original))

    first = next(reader)
    assert first.packet_type is ClientPacket.CHANGE_ACTION
    assert first.payload.read_u8() == 1
    with pytest.raises(TruncatedPayloadError):
        first.payload.read_i32()
    assert first.payload_view.readonly
    assert first.payload_view.tobytes() == b"\x01\x02"

    unknown = next(reader)
    assert unknown.packet_id == 65535
    assert unknown.packet_type is None
    assert not unknown.known
    assert unknown.payload_view.tobytes() == b"unknown"

    last = next(reader)
    assert last.packet_type is ClientPacket.PING
    assert last.payload.remaining == 0
    with pytest.raises(StopIteration):
        next(reader)


def test_server_packet_stream_uses_configured_enum() -> None:
    packets = list(PacketReader(login_reply(1) + pong(), packet_enum=ServerPacket))
    assert [packet.packet_type for packet in packets] == [ServerPacket.USER_ID, ServerPacket.PONG]
    assert packets[0].payload.read_i32() == 1


def test_primitive_boundaries_have_exact_little_endian_encoding() -> None:
    writer = payload_writer()
    writer.write_i8(-128)
    writer.write_u8(255)
    writer.write_i16(-32768)
    writer.write_u16(65535)
    writer.write_i32(-(2**31))
    writer.write_u32(2**32 - 1)
    writer.write_i64(-(2**63))
    writer.write_u64(2**64 - 1)
    writer.write_f16(1.5)
    writer.write_f32(-2.5)
    writer.write_f64(math.pi)
    writer.write_bool(False)
    writer.write_bool(True)
    encoded = writer.to_bytes()

    assert encoded == struct.pack(
        "<bBhHiIqQefd??",
        -128,
        255,
        -32768,
        65535,
        -(2**31),
        2**32 - 1,
        -(2**63),
        2**64 - 1,
        1.5,
        -2.5,
        math.pi,
        False,
        True,
    )
    reader = PacketReader(encoded)
    assert reader.read_i8() == -128
    assert reader.read_u8() == 255
    assert reader.read_i16() == -32768
    assert reader.read_u16() == 65535
    assert reader.read_i32() == -(2**31)
    assert reader.read_u32() == 2**32 - 1
    assert reader.read_i64() == -(2**63)
    assert reader.read_u64() == 2**64 - 1
    assert reader.read_f16() == 1.5
    assert reader.read_f32() == -2.5
    assert reader.read_f64() == math.pi
    assert reader.read_bool() is False
    assert reader.read_bool() is True
    reader.require_exhausted()


def test_invalid_primitive_values_are_controlled_protocol_errors() -> None:
    with pytest.raises(InvalidStructureError, match="boolean"):
        PacketReader(b"\x02").read_bool()

    writer = payload_writer()
    with pytest.raises(InvalidStructureError, match="does not fit"):
        writer.write_u8(256)
    assert writer.to_bytes() == b""

    with pytest.raises(InvalidStructureError, match="must be bool"):
        writer.write_bool(1)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (0, b"\x00"),
        (1, b"\x01"),
        (127, b"\x7f"),
        (128, b"\x80\x01"),
        (16383, b"\xff\x7f"),
        (16384, b"\x80\x80\x01"),
        (2**32 - 1, b"\xff\xff\xff\xff\x0f"),
    ],
)
def test_uleb128_boundaries(value: int, expected: bytes) -> None:
    writer = payload_writer()
    writer.write_uleb128(value)
    assert writer.to_bytes() == expected
    reader = PacketReader(expected)
    assert reader.read_uleb128() == value
    reader.require_exhausted()


@pytest.mark.parametrize(
    "encoded",
    [
        b"",
        b"\x80",
        b"\x80\x00",
        b"\x81\x00",
        b"\x80\x80\x80\x80\x80",
        b"\xff\xff\xff\xff\x10",
    ],
)
def test_malformed_uleb128_is_rejected(encoded: bytes) -> None:
    with pytest.raises(MalformedULEB128Error):
        PacketReader(encoded).read_uleb128()


def test_string_markers_lengths_unicode_and_boundaries() -> None:
    writer = payload_writer()
    writer.write_string("")
    writer.write_string("a" * 127)
    writer.write_string("b" * 128)
    writer.write_string("日本語")
    encoded = writer.to_bytes()

    assert encoded.startswith(b"\x00\x0b\x7f" + b"a" * 127 + b"\x0b\x80\x01" + b"b" * 128)
    reader = PacketReader(encoded)
    assert reader.read_string() == ""
    assert reader.read_string() == "a" * 127
    assert reader.read_string() == "b" * 128
    assert reader.read_string() == "日本語"
    reader.require_exhausted()


def test_invalid_string_marker_utf8_and_surrogates_are_controlled() -> None:
    with pytest.raises(InvalidStringMarkerError):
        PacketReader(b"\x01").read_string()
    with pytest.raises(InvalidStringEncodingError):
        PacketReader(b"\x0b\x01\xff").read_string()
    with pytest.raises(InvalidStringEncodingError):
        payload_writer().write_string("\ud800")


def test_i32_lists_support_u16_and_u32_lengths_with_exact_bytes() -> None:
    values = (-(2**31), 0, 2**31 - 1)
    writer = payload_writer()
    writer.write_i32_list_u16(values)
    writer.write_i32_list_u32(values)
    encoded = writer.to_bytes()
    items = struct.pack("<iii", *values)

    assert encoded == b"\x03\x00" + items + b"\x03\x00\x00\x00" + items
    reader = PacketReader(encoded)
    assert reader.read_i32_list_u16() == values
    assert reader.read_i32_list_u32() == values
    reader.require_exhausted()


def test_message_channel_and_client_status_structures_round_trip() -> None:
    message = Message("alice", "hello", "#osu", 12)
    channel = Channel("#osu", "general", 65535)
    status = ClientStatus(2, "playing", "abc", 2**32 - 1, 3, -1)
    writer = payload_writer()
    writer.write_message(message)
    writer.write_channel(channel)
    writer.write_client_status(status)

    reader = PacketReader(writer.to_bytes())
    assert reader.read_message() == message
    assert reader.read_channel() == channel
    assert reader.read_client_status() == status
    reader.require_exhausted()


@pytest.mark.parametrize(
    ("action", "mode"),
    [(-1, 0), (14, 0), (0, -1), (0, 4)],
)
def test_client_status_rejects_unknown_action_and_mode(action: int, mode: int) -> None:
    with pytest.raises(ValueError):
        ClientStatus(action, "", "", 0, mode, 0)


def test_presence_and_stats_builders_round_trip() -> None:
    presence = UserPresence(
        user_id=17,
        username="player",
        utc_offset=-5,
        country_code=38,
        privileges=5,
        mode=3,
        longitude=-73.5,
        latitude=45.5,
        global_rank=100,
    )
    stats = UserStats(
        user_id=17,
        action=2,
        info_text="playing",
        beatmap_md5="abc",
        mods=64,
        mode=3,
        beatmap_id=9,
        ranked_score=10,
        accuracy=0.9875,
        play_count=11,
        total_score=12,
        global_rank=13,
        performance=65535,
    )

    presence_packet = next(PacketReader(user_presence(presence), packet_enum=ServerPacket))
    decoded_presence = presence_packet.payload.read_user_presence()
    assert decoded_presence == presence

    stats_packet = next(PacketReader(user_stats(stats), packet_enum=ServerPacket))
    decoded_stats = stats_packet.payload.read_user_stats()
    assert decoded_stats.user_id == stats.user_id
    assert decoded_stats.info_text == stats.info_text
    assert decoded_stats.accuracy == pytest.approx(stats.accuracy)
    assert decoded_stats == replace(stats, accuracy=decoded_stats.accuracy)


def test_channel_builder_has_exact_u16_player_count() -> None:
    channel = Channel("#osu", "topic", 513)
    encoded = channel_info(channel)
    expected_payload = b"\x0b\x04#osu\x0b\x05topic\x01\x02"
    assert encoded == b"\x41\x00\x00" + struct.pack("<I", len(expected_payload)) + expected_payload


def test_multiplayer_match_round_trip_and_hidden_password_marker() -> None:
    statuses = (4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    match = MultiplayerMatch(
        match_id=3,
        in_progress=True,
        mods=64,
        name="room",
        password="secret",
        beatmap_name="map",
        beatmap_id=8,
        beatmap_md5="hash",
        slot_statuses=statuses,
        slot_teams=(1,) + (0,) * 15,
        slot_user_ids=(99,) + (None,) * 15,
        host_id=99,
        mode=1,
        win_condition=2,
        team_type=3,
        freemods=True,
        slot_mods=tuple(range(16)),
        seed=-4,
    )
    writer = payload_writer()
    writer.write_multiplayer_match(match)
    encoded = writer.to_bytes()
    reader = PacketReader(encoded)
    assert reader.read_multiplayer_match() == match
    reader.require_exhausted()

    hidden = update_match(match, send_password=False)
    packet = next(PacketReader(hidden, packet_enum=ServerPacket))
    hidden_match = packet.payload.read_multiplayer_match()
    assert hidden_match.password == ""
    assert b"\x0b\x00" in packet.payload_view.tobytes()


def test_default_multiplayer_match_has_exact_60_byte_wire_layout() -> None:
    writer = payload_writer()
    writer.write_multiplayer_match(MultiplayerMatch())
    expected = struct.pack("<h?bi", 0, False, 0, 0)
    expected += b"\x00\x00\x00" + struct.pack("<i", 0) + b"\x00"
    expected += bytes(32) + struct.pack("<iBBB?i", 0, 0, 0, 0, False, 0)
    assert len(expected) == 60
    assert writer.to_bytes() == expected


def test_multiplayer_slot_occupancy_is_validated() -> None:
    match = MultiplayerMatch(slot_statuses=(4,) + (0,) * 15)
    with pytest.raises(InvalidStructureError, match="occupancy"):
        payload_writer().write_multiplayer_match(match)

    wrong_slot_count = MultiplayerMatch(slot_statuses=(0,))
    with pytest.raises(InvalidStructureError, match="exactly 16"):
        payload_writer().write_multiplayer_match(wrong_slot_count)


def test_score_v2_frame_and_replay_bundle_round_trip_with_exact_raw_view() -> None:
    score = ScoreFrame(
        time=123,
        frame_id=1,
        count_300=2,
        count_100=3,
        count_50=4,
        count_geki=5,
        count_katu=6,
        count_miss=7,
        total_score=8,
        max_combo=9,
        current_combo=10,
        perfect=True,
        current_hp=11,
        tag_byte=12,
        score_v2=True,
        combo_portion=13.5,
        bonus_portion=14.5,
    )
    bundle = ReplayFrameBundle(
        frames=(ReplayFrame(1, 2, 3.5, 4.5, 5),),
        score_frame=score,
        action=ReplayAction.COMPLETION,
        extra=-1,
        sequence=42,
        raw_data=memoryview(b"ignored on encode"),
    )
    writer = payload_writer()
    writer.write_replay_frame_bundle(bundle)
    encoded = writer.to_bytes()

    reader = PacketReader(bytearray(encoded))
    decoded = reader.read_replay_frame_bundle()
    assert decoded.frames == bundle.frames
    assert decoded.score_frame == score
    assert decoded.action == bundle.action
    assert decoded.extra == bundle.extra
    assert decoded.sequence == bundle.sequence
    assert decoded.raw_data.readonly
    assert decoded.raw_data.tobytes() == encoded
    reader.require_exhausted()


def test_replay_bundle_rejects_actions_outside_stable_inventory() -> None:
    score = ScoreFrame(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, False, 0, 0, False)
    with pytest.raises(ValueError, match="between 0 and 8"):
        ReplayFrameBundle((), score, 9, 0, 0, memoryview(b""))  # type: ignore[arg-type]

    valid = ReplayFrameBundle((), score, ReplayAction.STANDARD, 0, 0, memoryview(b""))
    writer = payload_writer()
    writer.write_replay_frame_bundle(valid)
    encoded = bytearray(writer.to_bytes())
    encoded[6] = 9
    with pytest.raises(InvalidStructureError, match="replay action"):
        PacketReader(encoded).read_replay_frame_bundle()


def test_score_frame_rejects_invalid_boolean_bytes_and_inconsistent_v2_fields() -> None:
    invalid_perfect = bytes(25) + b"\x02" + bytes(3)
    with pytest.raises(InvalidStructureError, match="perfect boolean"):
        PacketReader(invalid_perfect).read_score_frame()

    invalid_score_v2 = bytes(28) + b"\x02"
    with pytest.raises(InvalidStructureError, match="ScoreV2 boolean"):
        PacketReader(invalid_score_v2).read_score_frame()

    missing_portions = ScoreFrame(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, False, 0, 0, True)
    writer = payload_writer()
    with pytest.raises(InvalidStructureError, match="require combo"):
        writer.write_score_frame(missing_portions)
    assert writer.to_bytes() == b""


def test_spectator_builder_preserves_frame_bytes_exactly() -> None:
    raw_frames = b"\x01\x02\x03"
    assert spectate_frames(raw_frames) == b"\x0f\x00\x00\x03\x00\x00\x00\x01\x02\x03"


@pytest.mark.parametrize("data", [b"\x00", b"\x00" * 6])
def test_truncated_packet_header_is_controlled(data: bytes) -> None:
    with pytest.raises(TruncatedHeaderError):
        next(PacketReader(data))


def test_truncated_packet_and_field_payloads_are_controlled() -> None:
    with pytest.raises(TruncatedPayloadError, match="declares 5"):
        next(PacketReader(struct.pack("<HxI", ClientPacket.PING, 5) + b"abcd"))

    packet = next(PacketReader(build_packet(ClientPacket.PING, b"abc")))
    with pytest.raises(TruncatedPayloadError, match="requires 4"):
        packet.payload.read_i32()

    with pytest.raises(TruncatedPayloadError):
        PacketReader(b"\x0b\x03ab").read_string()


def test_body_packet_string_list_packet_count_and_frame_bounds() -> None:
    with pytest.raises(BodyTooLargeError):
        PacketReader(b"123", limits=CodecLimits(max_body_size=2))

    oversized_header = struct.pack("<HxI", ClientPacket.PING, 5) + b"12345"
    with pytest.raises(PacketTooLargeError):
        next(PacketReader(oversized_header, limits=CodecLimits(max_packet_size=4)))

    with pytest.raises(StringTooLargeError):
        PacketReader(b"\x0b\x03abc", limits=CodecLimits(max_string_size=2)).read_string()

    with pytest.raises(ListTooLargeError):
        PacketReader(b"\x03\x00" + bytes(12), limits=CodecLimits(max_list_length=2)).read_i32_list_u16()
    with pytest.raises(ListTooLargeError):
        PacketReader(b"\x02\x00" + bytes(8)).read_i32_list_u16(max_length=1)

    stream = build_packet(ClientPacket.PING) * 2
    packet_reader = PacketReader(stream, limits=CodecLimits(max_packet_count=1))
    assert next(packet_reader).packet_type is ClientPacket.PING
    with pytest.raises(PacketCountExceededError):
        next(packet_reader)

    frame_header = struct.pack("<iH", 0, 2)
    with pytest.raises(FrameCountExceededError):
        PacketReader(frame_header, limits=CodecLimits(max_frame_count=1)).read_replay_frame_bundle()


def test_writer_enforces_body_packet_string_list_packet_count_and_frame_bounds() -> None:
    with pytest.raises(BodyTooLargeError):
        build_packet(ClientPacket.PING, limits=CodecLimits(max_body_size=6))

    with pytest.raises(PacketTooLargeError):
        build_packet(ClientPacket.PING, b"123", limits=CodecLimits(max_packet_size=2))

    with pytest.raises(StringTooLargeError):
        payload_writer(limits=CodecLimits(max_string_size=2)).write_string("abc")

    with pytest.raises(ListTooLargeError):
        payload_writer(limits=CodecLimits(max_list_length=1)).write_i32_list_u32((1, 2))

    writer = payload_writer(limits=CodecLimits(max_packet_count=1))
    writer.write_packet(ClientPacket.PING)
    with pytest.raises(PacketCountExceededError):
        writer.write_packet(ClientPacket.PING)

    score = ScoreFrame(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, False, 0, 0, False)
    bundle = ReplayFrameBundle(
        frames=(ReplayFrame(0, 0, 0.0, 0.0, 0),),
        score_frame=score,
        action=ReplayAction.STANDARD,
        extra=0,
        sequence=0,
        raw_data=memoryview(b""),
    )
    with pytest.raises(FrameCountExceededError):
        payload_writer(limits=CodecLimits(max_frame_count=0)).write_replay_frame_bundle(bundle)


def test_trailing_payload_and_writer_state_are_explicit() -> None:
    with pytest.raises(TrailingDataError):
        PacketReader(b"x").require_exhausted()

    writer = payload_writer()
    writer.begin_packet(ClientPacket.PING)
    with pytest.raises(ProtocolStateError):
        writer.begin_packet(ClientPacket.PING)
    with pytest.raises(ProtocolStateError):
        writer.to_bytes()
    writer.cancel_packet()
    assert writer.to_bytes() == b""


def test_packet_context_rolls_back_failed_payload() -> None:
    writer = payload_writer()
    with pytest.raises(RuntimeError), writer.packet(ClientPacket.PING):
        writer.write_raw(b"not retained")
        raise RuntimeError("fail")

    writer.write_packet(ClientPacket.PING)
    assert writer.to_bytes() == pong().replace(b"\x08", b"\x04", 1)
