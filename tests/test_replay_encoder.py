"""Tests for lazer replay reconstruction encoding."""

import lzma
import struct
from datetime import UTC, datetime

from perfcho.modules.realtime.models import CanonicalReplayFrame
from perfcho.modules.scoring.models import Ruleset
from perfcho.modules.scoring.replay_encoding import encode_replay, replay_digest

NOW = datetime(2026, 8, 10, 12, tzinfo=UTC)


def _uleb(data: bytes, offset: int) -> tuple[int, int]:
    value = 0
    shift = 0
    while True:
        byte = data[offset]
        offset += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value, offset
        shift += 7


def _read_string(data: bytes, offset: int) -> tuple[str, int]:
    assert data[offset] == 0x0B, "expected SerializationWriter StringType prefix"
    offset += 1
    length, offset = _uleb(data, offset)
    return data[offset : offset + length].decode(), offset + length


def test_encode_replay_header_layout() -> None:
    frames = (
        CanonicalReplayFrame(0, 100.0, 200.0, 5, 0),
        CanonicalReplayFrame(16, 101.0, 201.0, 1, 0),
    )
    replay = encode_replay(
        ruleset=Ruleset.OSU,
        username="Alice",
        beatmap_md5=bytes.fromhex("00112233445566778899aabbccddeeff"),
        hits={"great": 100, "ok": 5, "miss": 1},
        total_score=12345,
        max_combo=80,
        perfect=False,
        mods=("HD", "DT"),
        ended_at=NOW,
        frames=frames,
    )

    assert replay[0] == 0  # osu ruleset id
    assert struct.unpack("<i", replay[1:5])[0] == 30000019  # LATEST_VERSION

    offset = 5
    md5, offset = _read_string(replay, offset)
    username, offset = _read_string(replay, offset)
    replay_hash, offset = _read_string(replay, offset)
    assert md5 == "00112233445566778899aabbccddeeff"
    assert username == "Alice"
    assert len(replay_hash) == 32

    # hit counts: 300/100/50/geki/katu/miss as unsigned shorts.
    counts = struct.unpack("<6H", replay[offset : offset + 12])
    offset += 12
    assert counts == (100, 5, 0, 0, 0, 1)

    assert struct.unpack("<i", replay[offset : offset + 4])[0] == 12345
    offset += 4
    assert struct.unpack("<H", replay[offset : offset + 2])[0] == 80


def test_encode_replay_decompresses_frame_string() -> None:
    frames = (CanonicalReplayFrame(0, 100.0, 200.0, 5, 0), CanonicalReplayFrame(16, 101.0, 201.0, 1, 0))
    replay = encode_replay(
        ruleset=Ruleset.OSU,
        username="Alice",
        beatmap_md5=bytes(16),
        hits={"great": 10},
        total_score=1,
        max_combo=10,
        perfect=False,
        mods=(),
        ended_at=NOW,
        frames=frames,
    )

    # Parse the header: mode + version, three strings, six hit counts, total
    # score, max combo, perfect flag, mods int, HP graph string, DateTime ticks.
    offset = 5
    _, offset = _read_string(replay, offset)  # md5
    _, offset = _read_string(replay, offset)  # username
    _, offset = _read_string(replay, offset)  # replay hash
    offset += 12  # six u16 hit counts
    offset += 4  # total score i32
    offset += 2  # max combo u16
    offset += 1  # perfect bool
    offset += 4  # mods i32
    _, offset = _read_string(replay, offset)  # HP graph (empty)
    offset += 8  # DateTime ticks i64

    # Remaining is: replay data byte array (i32 length + LZMA), legacy id (i64),
    # mod-specific data byte array (i32 length + bytes).
    replay_len = struct.unpack("<i", replay[offset : offset + 4])[0]
    offset += 4
    replay_blob = replay[offset : offset + replay_len]

    raw = lzma.decompress(replay_blob, format=lzma.FORMAT_ALONE)
    content = raw.decode()
    assert "0|100.0|200.0|5" in content
    assert "-12345|0|0|0" in content


def test_replay_digest_is_stable() -> None:
    args = dict(
        ruleset=Ruleset.OSU,
        username="Alice",
        beatmap_md5=bytes(16),
        hits={"great": 1},
        total_score=1,
        max_combo=1,
        perfect=False,
        mods=(),
        ended_at=NOW,
        frames=(),
    )
    assert replay_digest(encode_replay(**args)) == replay_digest(encode_replay(**args))
