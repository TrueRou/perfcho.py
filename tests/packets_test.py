from __future__ import annotations

import pytest

from perfcho.modules.realtime.stable import builders as packets
from perfcho.modules.realtime.stable.models import Channel, Message, ScoreFrame, UserPresence, UserStats


@pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        (0, b"\x05\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00"),
        (2_147_483_647, b"\x05\x00\x00\x04\x00\x00\x00\xff\xff\xff\x7f"),
    ],
)
def test_write_user_id(test_input: int, expected: bytes) -> None:
    assert packets.login_reply(test_input) == expected


@pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        (
            Message(sender="cmyui", text="woah woah crazy!!", recipient="jacobian", sender_id=32),
            b"\x07\x00\x00(\x00\x00\x00\x0b\x05cmyui\x0b\x11woah woah crazy!!\x0b\x08jacobian \x00\x00\x00",
        ),
        (
            Message(sender="", text="", recipient="", sender_id=0),
            b"\x07\x00\x00\x07\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
        ),
    ],
)
def test_write_send_message(test_input: Message, expected: bytes) -> None:
    assert packets.send_message(test_input) == expected


def test_write_pong() -> None:
    assert packets.pong() == b"\x08\x00\x00\x00\x00\x00\x00"


@pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        (("cmyui", "abcgamer321"), b"\t\x00\x00\x16\x00\x00\x00\x0b\x14cmyui>>>>abcgamer321"),
        (("", ""), b"\t\x00\x00\x06\x00\x00\x00\x0b\x04>>>>"),
    ],
)
def test_write_change_username(test_input: tuple[str, str], expected: bytes) -> None:
    assert packets.change_username(*test_input) == expected


@pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        (
            UserStats(
                user_id=1001,
                action=2,
                info_text="gaming",
                beatmap_md5="60b725f10c9c85c70d97880dfe8191b3",
                mods=64,
                mode=0,
                beatmap_id=1723723,
                ranked_score=1_238_917_112,
                accuracy=0.9232,
                play_count=3821,
                total_score=3_812_428_392,
                global_rank=42,
                performance=8291,
            ),
            (
                b"\x0b\x00\x00V\x00\x00\x00\xe9\x03\x00\x00\x02\x0b\x06gaming\x0b 60b725f10c9c85c70d97880dfe8191b3"
                b"@\x00\x00\x00\x00KM\x1a\x00\xf8_\xd8I\x00\x00\x00\x00\xd6Vl?\xed\x0e\x00\x00"
                b"h\n=\xe3\x00\x00\x00\x00*\x00\x00\x00c "
            ),
        ),
        (
            UserStats(0, 0, "", "", 0, 0, 0, 0, 0.0, 0, 0, 0, 0),
            b"\x0b\x00\x00.\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
        ),
    ],
)
def test_write_user_stats(test_input: UserStats, expected: bytes) -> None:
    assert packets.user_stats(test_input) == expected


@pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        (0, b"\x0c\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00"),
        (2_147_483_647, b"\x0c\x00\x00\x05\x00\x00\x00\xff\xff\xff\x7f\x00"),
    ],
)
def test_write_logout(test_input: int, expected: bytes) -> None:
    assert packets.user_logout(test_input) == expected


@pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        (0, b"\x0d\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00"),
        (2_147_483_647, b"\x0d\x00\x00\x04\x00\x00\x00\xff\xff\xff\x7f"),
    ],
)
def test_write_spectator_joined(test_input: int, expected: bytes) -> None:
    assert packets.spectator_joined(test_input) == expected


@pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        (0, b"\x0e\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00"),
        (2_147_483_647, b"\x0e\x00\x00\x04\x00\x00\x00\xff\xff\xff\x7f"),
    ],
)
def test_write_spectator_left(test_input: int, expected: bytes) -> None:
    assert packets.spectator_left(test_input) == expected


@pytest.mark.xfail(reason="need to implement proper writing")
@pytest.mark.parametrize(("test_input", "expected"), [({}, b"")])
def test_write_spectate_frames(test_input: object, expected: bytes) -> None:
    assert packets.spectate_frames(test_input) == expected  # type: ignore[arg-type]


def test_write_version_update() -> None:
    assert packets.version_update() == b"\x13\x00\x00\x00\x00\x00\x00"


@pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        (0, b"\x16\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00"),
        (2_147_483_647, b"\x16\x00\x00\x04\x00\x00\x00\xff\xff\xff\x7f"),
    ],
)
def test_write_spectator_cant_spectate(test_input: int, expected: bytes) -> None:
    assert packets.spectator_cant_spectate(test_input) == expected


def test_write_get_attention() -> None:
    assert packets.get_attention() == b"\x17\x00\x00\x00\x00\x00\x00"


@pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        ("waowww", b"\x18\x00\x00\x08\x00\x00\x00\x0b\x06waowww"),
        ("", b"\x18\x00\x00\x01\x00\x00\x00\x00"),
    ],
)
def test_write_notification(test_input: str, expected: bytes) -> None:
    assert packets.notification(test_input) == expected


@pytest.mark.xfail(reason="need to remove bancho.py match object")
@pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        ({"m": None, "send_pw": False}, b""),
        ({"m": None, "send_pw": True}, b""),
    ],
)
def test_write_update_match(test_input: dict[str, object], expected: bytes) -> None:
    assert packets.update_match(**test_input) == expected  # type: ignore[arg-type]


@pytest.mark.xfail(reason="need to remove bancho.py match object")
@pytest.mark.parametrize(("test_input", "expected"), [({}, b""), ({}, b"")])
def test_write_new_match(test_input: object, expected: bytes) -> None:
    assert packets.new_match(test_input) == expected  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        (0, b"\x1c\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00"),
        (2_147_483_647, b"\x1c\x00\x00\x04\x00\x00\x00\xff\xff\xff\x7f"),
    ],
)
def test_write_dispose_match(test_input: int, expected: bytes) -> None:
    assert packets.dispose_match(test_input) == expected


def test_write_toggle_block_non_friend_pm() -> None:
    assert packets.toggle_block_non_friend_dms() == b'"\x00\x00\x00\x00\x00\x00'


@pytest.mark.xfail(reason="need to remove bancho.py match object")
@pytest.mark.parametrize(("test_input", "expected"), [({}, b""), ({}, b"")])
def test_write_match_join_success(test_input: object, expected: bytes) -> None:
    assert packets.match_join_success(test_input) == expected  # type: ignore[arg-type]


def test_write_match_join_fail() -> None:
    assert packets.match_join_fail() == b"%\x00\x00\x00\x00\x00\x00"


@pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        (0, b"*\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00"),
        (2_147_483_647, b"*\x00\x00\x04\x00\x00\x00\xff\xff\xff\x7f"),
    ],
)
def test_write_fellow_spectator_joined(test_input: int, expected: bytes) -> None:
    assert packets.fellow_spectator_joined(test_input) == expected


@pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        (0, b"+\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00"),
        (2_147_483_647, b"+\x00\x00\x04\x00\x00\x00\xff\xff\xff\x7f"),
    ],
)
def test_write_fellow_spectator_left(test_input: int, expected: bytes) -> None:
    assert packets.fellow_spectator_left(test_input) == expected


@pytest.mark.xfail(reason="need to remove bancho.py match object")
@pytest.mark.parametrize(("test_input", "expected"), [({}, b""), ({}, b"")])
def test_write_match_start(test_input: object, expected: bytes) -> None:
    assert packets.match_start(test_input) == expected  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        (
            ScoreFrame(
                time=38242,
                frame_id=28,
                count_300=320,
                count_100=48,
                count_50=2,
                count_geki=32,
                count_katu=8,
                count_miss=3,
                total_score=492_392,
                current_combo=39,
                max_combo=122,
                perfect=False,
                current_hp=245,
                tag_byte=0,
                score_v2=False,
            ),
            (
                b"0\x00\x00\x1d\x00\x00\x00b\x95\x00\x00\x1c@\x010\x00\x02\x00 \x00\x08\x00\x03\x00"
                b"h\x83\x07\x00z\x00'\x00\x00\xf5\x00\x00"
            ),
        ),
        (
            ScoreFrame(
                time=0,
                frame_id=0,
                count_300=0,
                count_100=0,
                count_50=0,
                count_geki=0,
                count_katu=0,
                count_miss=0,
                total_score=0,
                current_combo=0,
                max_combo=0,
                perfect=False,
                current_hp=0,
                tag_byte=0,
                score_v2=False,
            ),
            b"0\x00\x00\x1d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
        ),
    ],
)
def test_write_match_score_update(test_input: ScoreFrame, expected: bytes) -> None:
    assert packets.match_score_update(test_input) == expected


def test_write_match_transfer_host() -> None:
    assert packets.match_transfer_host() == b"2\x00\x00\x00\x00\x00\x00"


def test_write_match_all_players_loaded() -> None:
    assert packets.match_all_players_loaded() == b"5\x00\x00\x00\x00\x00\x00"


@pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        (0, b"9\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00"),
        (2_147_483_647, b"9\x00\x00\x04\x00\x00\x00\xff\xff\xff\x7f"),
    ],
)
def test_write_match_player_failed(test_input: int, expected: bytes) -> None:
    assert packets.match_player_failed(test_input) == expected


def test_write_match_complete() -> None:
    assert packets.match_complete() == b":\x00\x00\x00\x00\x00\x00"


def test_write_match_skip() -> None:
    assert packets.match_skip() == b"=\x00\x00\x00\x00\x00\x00"


@pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        ("#osu", b"@\x00\x00\x06\x00\x00\x00\x0b\x04#osu"),
        ("", b"@\x00\x00\x01\x00\x00\x00\x00"),
    ],
)
def test_write_channel_join(test_input: str, expected: bytes) -> None:
    assert packets.channel_join(test_input) == expected


@pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        (Channel("#osu", "le topique", 123), b"A\x00\x00\x14\x00\x00\x00\x0b\x04#osu\x0b\nle topique{\x00"),
        (Channel("", "", 0), b"A\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00"),
    ],
)
def test_write_channel_info(test_input: Channel, expected: bytes) -> None:
    assert packets.channel_info(test_input) == expected


@pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        ("#osu", b"B\x00\x00\x06\x00\x00\x00\x0b\x04#osu"),
        ("", b"B\x00\x00\x01\x00\x00\x00\x00"),
    ],
)
def test_write_channel_kick(test_input: str, expected: bytes) -> None:
    assert packets.channel_kick(test_input) == expected


@pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        (Channel("#osu", "le topique", 123), b"C\x00\x00\x14\x00\x00\x00\x0b\x04#osu\x0b\nle topique{\x00"),
        (Channel("", "", 0), b"C\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00"),
    ],
)
def test_write_channel_auto_join(test_input: Channel, expected: bytes) -> None:
    assert packets.channel_auto_join(test_input) == expected


@pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        (0, b"G\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00"),
        (2_147_483_647, b"G\x00\x00\x04\x00\x00\x00\xff\xff\xff\x7f"),
    ],
)
def test_write_bancho_privileges(test_input: int, expected: bytes) -> None:
    assert packets.privileges(test_input) == expected


@pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        ([1, 4, 1001], b"H\x00\x00\x0e\x00\x00\x00\x03\x00\x01\x00\x00\x00\x04\x00\x00\x00\xe9\x03\x00\x00"),
        ([], b"H\x00\x00\x02\x00\x00\x00\x00\x00"),
    ],
)
def test_write_friends_list(test_input: list[int], expected: bytes) -> None:
    assert packets.friends_list(test_input) == expected


@pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        (0, b"K\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00"),
        (2_147_483_647, b"K\x00\x00\x04\x00\x00\x00\xff\xff\xff\x7f"),
    ],
)
def test_write_protocol_version(test_input: int, expected: bytes) -> None:
    assert packets.protocol_version(test_input) == expected


@pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        (
            ("https://icon-url.ca/a.png", "https://onclick-url.ca/a.png"),
            b"L\x00\x008\x00\x00\x00\x0b6https://icon-url.ca/a.png|https://onclick-url.ca/a.png",
        ),
        (("", ""), b"L\x00\x00\x03\x00\x00\x00\x0b\x01|"),
    ],
)
def test_write_main_menu_icon(test_input: tuple[str, str], expected: bytes) -> None:
    assert packets.main_menu_icon(*test_input) == expected


def test_write_monitor() -> None:
    assert packets.monitor() == b"P\x00\x00\x00\x00\x00\x00"


@pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        (0, b"Q\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00"),
        (2_147_483_647, b"Q\x00\x00\x04\x00\x00\x00\xff\xff\xff\x7f"),
    ],
)
def test_write_match_player_skipped(test_input: int, expected: bytes) -> None:
    assert packets.match_player_skipped(test_input) == expected


@pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        (
            UserPresence(
                user_id=1001,
                username="cmyui",
                utc_offset=-5,
                country_code=38,
                privileges=31,
                mode=0,
                longitude=43.768,
                latitude=-79.522,
                global_rank=42,
            ),
            b"S\x00\x00\x1a\x00\x00\x00\xe9\x03\x00\x00\x0b\x05cmyui\x13&\x1fo\x12/BD\x0b\x9f\xc2*\x00\x00\x00",
        ),
        (
            UserPresence(0, "", 0, 0, 0, 0, 0.0, 0.0, 0),
            b"S\x00\x00\x14\x00\x00\x00\x00\x00\x00\x00\x00\x18\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
        ),
    ],
)
def test_write_user_presence(test_input: UserPresence, expected: bytes) -> None:
    assert packets.user_presence(test_input) == expected


@pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        (0, b"V\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00"),
        (2_147_483_647, b"V\x00\x00\x04\x00\x00\x00\xff\xff\xff\x7f"),
    ],
)
def test_write_restart_server(test_input: int, expected: bytes) -> None:
    assert packets.restart(test_input) == expected


@pytest.mark.xfail(reason="need to remove bancho.py match object")
@pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        ({"p": None, "t_name": "cover"}, b""),
        ({"p": None, "t_name": "cover"}, b""),
    ],
)
def test_write_match_invite(test_input: dict[str, object], expected: bytes) -> None:
    assert packets.match_invite(**test_input) == expected  # type: ignore[arg-type]


def test_channel_info_end() -> None:
    assert packets.channel_info_end() == b"Y\x00\x00\x00\x00\x00\x00"


@pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        ("newpassword", b"[\x00\x00\r\x00\x00\x00\x0b\x0bnewpassword"),
        ("", b"[\x00\x00\x01\x00\x00\x00\x00"),
    ],
)
def test_write_match_change_password(test_input: str, expected: bytes) -> None:
    assert packets.match_change_password(test_input) == expected


@pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        (0, b"\\\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00"),
        (2_147_483_647, b"\\\x00\x00\x04\x00\x00\x00\xff\xff\xff\x7f"),
    ],
)
def test_write_silence_end(test_input: int, expected: bytes) -> None:
    assert packets.silence_end(test_input) == expected


@pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        (0, b"^\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00"),
        (2_147_483_647, b"^\x00\x00\x04\x00\x00\x00\xff\xff\xff\x7f"),
    ],
)
def test_write_user_silenced(test_input: int, expected: bytes) -> None:
    assert packets.user_silenced(test_input) == expected


@pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        (0, b"_\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00"),
        (2_147_483_647, b"_\x00\x00\x04\x00\x00\x00\xff\xff\xff\x7f"),
    ],
)
def test_write_user_presence_single(test_input: int, expected: bytes) -> None:
    assert packets.user_presence_single(test_input) == expected


@pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        ([1, 4, 1001], b"`\x00\x00\x0e\x00\x00\x00\x03\x00\x01\x00\x00\x00\x04\x00\x00\x00\xe9\x03\x00\x00"),
        ([], b"`\x00\x00\x02\x00\x00\x00\x00\x00"),
    ],
)
def test_write_user_presence_bundle(test_input: list[int], expected: bytes) -> None:
    assert packets.user_presence_bundle(test_input) == expected


@pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        ("cover", b"d\x00\x00\r\x00\x00\x00\x00\x00\x0b\x05cover\x00\x00\x00\x00"),
        ("", b"d\x00\x00\x07\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"),
    ],
)
def test_write_user_dm_blocked(test_input: str, expected: bytes) -> None:
    assert packets.user_dm_blocked(test_input) == expected


@pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        ("cover", b"e\x00\x00\r\x00\x00\x00\x00\x00\x0b\x05cover\x00\x00\x00\x00"),
        ("", b"e\x00\x00\x07\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"),
    ],
)
def test_write_target_silenced(test_input: str, expected: bytes) -> None:
    assert packets.target_is_silenced(test_input) == expected


def test_write_version_update_forced() -> None:
    assert packets.version_update(forced=True) == b"f\x00\x00\x00\x00\x00\x00"


@pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        (0, b"g\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00"),
        (2_147_483_647, b"g\x00\x00\x04\x00\x00\x00\xff\xff\xff\x7f"),
    ],
)
def test_write_switch_server(test_input: int, expected: bytes) -> None:
    assert packets.switch_server(test_input) == expected


def test_write_account_restricted() -> None:
    assert packets.account_restricted() == b"h\x00\x00\x00\x00\x00\x00"


@pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        ("yoyoo rip rtx", b"i\x00\x00\x0f\x00\x00\x00\x0b\ryoyoo rip rtx"),
        ("", b"i\x00\x00\x01\x00\x00\x00\x00"),
    ],
)
def test_write_rtx(test_input: str, expected: bytes) -> None:
    assert packets.rtx(test_input) == expected


def test_write_match_abort() -> None:
    assert packets.match_abort() == b"j\x00\x00\x00\x00\x00\x00"


@pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        ("61.91.139.24", b"k\x00\x00\x0e\x00\x00\x00\x0b\x0c61.91.139.24"),
        ("", b"k\x00\x00\x01\x00\x00\x00\x00"),
    ],
)
def test_write_switch_tournament_server(test_input: str, expected: bytes) -> None:
    assert packets.switch_tournament_server(test_input) == expected
