"""Encode canonical replay frames into the legacy osu! replay (.osr) format.

The lazer client submits solo scores without replay bytes; the server is
responsible for reconstructing the replay from the spectator frame stream and
persisting it as a stable-compatible ``.osr`` object.
"""

from __future__ import annotations

import hashlib
import lzma
import struct
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from perfcho.modules.realtime.models import CanonicalReplayFrame
from perfcho.modules.scoring.models import Ruleset

if TYPE_CHECKING:
    pass

_LATEST_VERSION = 30000019
_END_MARKER = "-12345|0|0|0"

_RULESET_IDS = {Ruleset.OSU: 0, Ruleset.TAIKO: 1, Ruleset.FRUITS: 2, Ruleset.MANIA: 3}

_LEGACY_MOD_BITS: dict[str, int] = {
    "NF": 1 << 0,
    "EZ": 1 << 1,
    "TD": 1 << 2,
    "HD": 1 << 3,
    "HR": 1 << 4,
    "SD": 1 << 5,
    "DT": 1 << 6,
    "RX": 1 << 7,
    "HT": 1 << 8,
    "NC": 1 << 9,
    "FL": 1 << 10,
    "AT": 1 << 11,
    "SO": 1 << 12,
    "AP": 1 << 13,
    "PF": 1 << 14,
    "K4": 1 << 15,
    "K5": 1 << 16,
    "K6": 1 << 17,
    "K7": 1 << 18,
    "K8": 1 << 19,
    "FI": 1 << 20,
    "RD": 1 << 21,
    "CN": 1 << 22,
    "TP": 1 << 23,
    "K9": 1 << 24,
    "CO": 1 << 25,
    "K1": 1 << 26,
    "K3": 1 << 27,
    "K2": 1 << 28,
    "V2": 1 << 29,
    "MR": 1 << 30,
}


def encode_replay(
    *,
    ruleset: Ruleset,
    username: str,
    beatmap_md5: bytes,
    hits: dict[str, int],
    total_score: int,
    max_combo: int,
    perfect: bool,
    mods: tuple[str, ...],
    ended_at: datetime,
    frames: tuple[CanonicalReplayFrame, ...],
) -> bytes:
    """Encode one complete .osr replay from canonical facts and frames."""
    replay_data = _lzma_compress(_replay_string(frames))
    return _encode_header(
        ruleset=ruleset,
        username=username,
        beatmap_md5=beatmap_md5,
        hits=hits,
        total_score=total_score,
        max_combo=max_combo,
        perfect=perfect,
        mods=mods,
        ended_at=ended_at,
        replay_data=replay_data,
    )


def _encode_header(
    *,
    ruleset: Ruleset,
    username: str,
    beatmap_md5: bytes,
    hits: dict[str, int],
    total_score: int,
    max_combo: int,
    perfect: bool,
    mods: tuple[str, ...],
    ended_at: datetime,
    replay_data: bytes,
) -> bytes:
    out = bytearray()
    out.append(_RULESET_IDS.get(ruleset, 0))
    out += struct.pack("<i", _LATEST_VERSION)
    out += _string(beatmap_md5.hex())
    out += _string(username)
    replay_hash = hashlib.md5(f"lazer-{username}-{ended_at.isoformat()}".encode(), usedforsecurity=False).hexdigest()
    out += _string(replay_hash)
    out += struct.pack("<H", _hit(hits, "great", "count300"))
    out += struct.pack("<H", _hit(hits, "ok", "count100"))
    out += struct.pack("<H", _hit(hits, "meh", "count50"))
    out += struct.pack("<H", _hit(hits, "geki", "countGeki"))
    out += struct.pack("<H", _hit(hits, "katu", "countKatu"))
    out += struct.pack("<H", _hit(hits, "miss", "countMiss"))
    out += struct.pack("<i", total_score)
    out += struct.pack("<H", max_combo)
    out += b"\x01" if perfect else b"\x00"
    out += struct.pack("<i", _legacy_mod_bits(mods))
    out += _string("")  # HP graph
    ticks = _datetime_ticks(ended_at)
    out += struct.pack("<q", ticks)
    out += _byte_array(replay_data)
    out += struct.pack("<q", 0)  # legacy online ID
    out += _byte_array(b"")  # mod-specific data
    return bytes(out)


def _replay_string(frames: tuple[CanonicalReplayFrame, ...]) -> str:
    if not frames:
        return _END_MARKER
    parts: list[str] = []
    last_time = 0
    for frame in frames:
        delta = max(0, frame.timestamp_ms - last_time)
        last_time = frame.timestamp_ms
        parts.append(f"{delta}|{frame.position_x}|{frame.position_y}|{frame.input_state}")
    parts.append(_END_MARKER)
    return ",".join(parts) + ","


def _lzma_compress(content: str) -> bytes:
    raw = content.encode()
    # Python's FORMAT_ALONE already emits the 13-byte LZMA-alone header
    # (properties + 8-byte uncompressed size), matching the lazer encoder.
    return lzma.compress(raw, format=lzma.FORMAT_ALONE)


def _string(value: str) -> bytes:
    raw = value.encode()
    out = bytearray()
    out.append(0x0B)  # SerializationWriter StringType
    out += _uleb128(len(raw))
    out += raw
    return bytes(out)


def _byte_array(value: bytes) -> bytes:
    out = bytearray()
    out += struct.pack("<i", len(value))
    out += value
    return bytes(out)


def _uleb128(value: int) -> bytes:
    if value == 0:
        return b"\x00"
    out = bytearray()
    while value != 0:
        byte = value & 0x7F
        value >>= 7
        if value != 0:
            byte |= 0x80
        out.append(byte)
    return bytes(out)


def _legacy_mod_bits(mods: tuple[str, ...]) -> int:
    bits = 0
    for mod in mods:
        bits |= _LEGACY_MOD_BITS.get(mod, 0)
    if "NC" in mods:
        bits |= _LEGACY_MOD_BITS["DT"]
    if "PF" in mods:
        bits |= _LEGACY_MOD_BITS["SD"]
    return bits


def _hit(hits: dict[str, int], *keys: str) -> int:
    for key in keys:
        if key in hits:
            return int(hits[key])
    return 0


def _datetime_ticks(value: datetime) -> int:
    if value.tzinfo is None or value.utcoffset() is None:
        value = value.replace(tzinfo=UTC)
    utc = value.astimezone(UTC)
    # .NET DateTime ticks: 100-nanosecond intervals since 0001-01-01.
    epoch = datetime(1, 1, 1, tzinfo=UTC)
    delta = utc - epoch
    return delta.days * 86_400_000_000_0 + delta.seconds * 10_000_000 + delta.microseconds * 10


def replay_digest(replay: bytes) -> bytes:
    """Return the content-addressable SHA-256 of a replay object."""
    return hashlib.sha256(replay).digest()


__all__ = ("encode_replay", "replay_digest")
