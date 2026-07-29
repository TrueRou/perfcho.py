"""Decrypt and normalize the legacy Stable modular score payload."""

from __future__ import annotations

import binascii
import re
from base64 import b64decode
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from py3rijndael import Pkcs7Padding, RijndaelCbc

from perfcho.modules.scoring import (
    CanonicalMod,
    ClientFamily,
    HitStatistic,
    PlayAttemptSubmission,
    Ruleset,
    ScoreAttestation,
    ScoreboardVariant,
    ScoreGrade,
    ScoreOutcome,
    ScoreSubmission,
)
from perfcho.modules.scoring.mods import LEGACY_MOD_BITS

_OSU_VERSION = re.compile(r"^\d{8}$")
_INTEGER = re.compile(r"^-?\d{1,20}$")
_RULESETS = {0: Ruleset.OSU, 1: Ruleset.TAIKO, 2: Ruleset.FRUITS, 3: Ruleset.MANIA}
_KNOWN_MOD_MASK = sum(LEGACY_MOD_BITS.values())


@dataclass(frozen=True, slots=True)
class ParsedStableScore:
    """Carry normalized Stable gameplay facts and identity evidence."""

    beatmap_md5: bytes
    username: str
    ruleset: Ruleset
    variant: ScoreboardVariant
    mods: tuple[CanonicalMod, ...]
    attempt: PlayAttemptSubmission
    score: ScoreSubmission
    attestation: ScoreAttestation
    client_hash: str


def decrypt_stable_score(
    *,
    score_data_b64: str,
    client_hash_b64: str,
    iv_b64: str,
    osu_version: str,
    exited: bool,
    fail_time_ms: int,
    score_time_ms: int,
    supported_build: str,
) -> ParsedStableScore:
    """Decrypt one bounded Rijndael payload and normalize its canonical score facts."""
    if not _OSU_VERSION.fullmatch(osu_version):
        raise ValueError("osu version must contain eight digits")
    if osu_version not in supported_build:
        raise ValueError("score was submitted by an unsupported Stable build")
    if fail_time_ms < 0 or score_time_ms < 0:
        raise ValueError("Stable score timing values must not be negative")
    if max(len(score_data_b64), len(client_hash_b64), len(iv_b64)) > 16_384:
        raise ValueError("encrypted Stable score fields are too large")
    try:
        iv = b64decode(iv_b64, validate=True)
        score_data = b64decode(score_data_b64, validate=True)
        encrypted_client_hash = b64decode(client_hash_b64, validate=True)
        cipher = RijndaelCbc(
            key=f"osu!-scoreburgr---------{osu_version}".encode(),
            iv=iv,
            padding=Pkcs7Padding(32),
            block_size=32,
        )
        fields = cipher.decrypt(score_data).decode("utf-8").split(":")
        client_hash = cipher.decrypt(encrypted_client_hash).decode("utf-8")
    except (binascii.Error, IndexError, UnicodeError, ValueError) as error:
        raise ValueError("Stable score encryption is invalid") from error
    if len(fields) != 18 or not client_hash or len(client_hash) > 512:
        raise ValueError("decrypted Stable score payload is invalid")

    beatmap_md5 = _hex_digest(fields[0], 16, "beatmap MD5")
    username = fields[1][:-1] if fields[1].endswith(" ") else fields[1]
    if not username or len(username) > 64:
        raise ValueError("Stable score username is invalid")
    online_checksum = _hex_digest(fields[2], 16, "online checksum")
    n300, n100, n50, ngeki, nkatu, nmiss = (_integer(field) for field in fields[3:9])
    total_score = _integer(fields[9])
    max_combo = _integer(fields[10])
    perfect = _boolean(fields[11])
    try:
        grade = ScoreGrade(fields[12].upper())
    except ValueError as error:
        raise ValueError("Stable score grade is invalid") from error
    legacy_mods = _integer(fields[13])
    passed = _boolean(fields[14])
    mode = _integer(fields[15])
    if mode not in _RULESETS:
        raise ValueError("Stable score ruleset is invalid")
    ruleset = _RULESETS[mode]
    mods, variant = parse_legacy_mods(legacy_mods)
    try:
        ended_at = datetime.strptime(fields[16], "%y%m%d%H%M%S").replace(tzinfo=UTC)
    except ValueError as error:
        raise ValueError("Stable score timestamp is invalid") from error
    client_flags = fields[17].count(" ") & ~4
    outcome = ScoreOutcome.PASSED if passed else ScoreOutcome.ABANDONED if exited else ScoreOutcome.FAILED
    progress = _progress(outcome, fail_time_ms, score_time_ms)
    hits = _hit_statistics(ruleset, n300, n100, n50, ngeki, nkatu, nmiss)
    accuracy = _accuracy(ruleset, hits, mods)
    return ParsedStableScore(
        beatmap_md5=beatmap_md5,
        username=username,
        ruleset=ruleset,
        variant=variant,
        mods=mods,
        attempt=PlayAttemptSubmission(
            idempotency_key=f"stable:{online_checksum.hex()}",
            started_at=ended_at - timedelta(milliseconds=score_time_ms),
            ended_at=ended_at,
            progress=progress,
            client_metadata={
                "fail_time_ms": fail_time_ms,
                "score_time_ms": score_time_ms,
                "legacy_mod_bits": legacy_mods,
            },
        ),
        score=ScoreSubmission(
            total_score=total_score,
            classic_score=total_score,
            accuracy=accuracy,
            max_combo=max_combo,
            grade=grade,
            outcome=outcome,
            perfect=perfect,
            hits=hits,
            client_flags=client_flags,
            online_checksum=online_checksum,
        ),
        attestation=ScoreAttestation(
            client_family=ClientFamily.STABLE,
            client_version=supported_build,
            verification_state="pending",
            client_flags=client_flags,
            evidence={"osu_version": osu_version},
        ),
        client_hash=client_hash,
    )


def _integer(value: str) -> int:
    if not _INTEGER.fullmatch(value):
        raise ValueError("Stable score integer field is invalid")
    result = int(value)
    if result < 0:
        raise ValueError("Stable score integer field must not be negative")
    return result


def _boolean(value: str) -> bool:
    if value == "True":
        return True
    if value == "False":
        return False
    raise ValueError("Stable score boolean field is invalid")


def _hex_digest(value: str, size: int, field_name: str) -> bytes:
    if len(value) != size * 2:
        raise ValueError(f"Stable score {field_name} is invalid")
    try:
        result = bytes.fromhex(value)
    except ValueError as error:
        raise ValueError(f"Stable score {field_name} is invalid") from error
    if len(result) != size:
        raise ValueError(f"Stable score {field_name} is invalid")
    return result


def parse_legacy_mods(legacy_bits: int) -> tuple[tuple[CanonicalMod, ...], ScoreboardVariant]:
    """Convert bounded Stable mod bits into canonical mods and a scoreboard variant."""
    if legacy_bits & ~_KNOWN_MOD_MASK:
        raise ValueError("Stable score contains unknown mod bits")
    if legacy_bits & LEGACY_MOD_BITS["RX"] and legacy_bits & LEGACY_MOD_BITS["AP"]:
        raise ValueError("Stable score cannot combine relax and autopilot")
    variant = (
        ScoreboardVariant.AUTOPILOT
        if legacy_bits & LEGACY_MOD_BITS["AP"]
        else ScoreboardVariant.RELAX
        if legacy_bits & LEGACY_MOD_BITS["RX"]
        else ScoreboardVariant.VANILLA
    )
    mods: list[CanonicalMod] = []
    for acronym, bit in LEGACY_MOD_BITS.items():
        if not legacy_bits & bit:
            continue
        if acronym in {"RX", "AP"}:
            mods.append(CanonicalMod(acronym))
        elif (acronym == "DT" and legacy_bits & LEGACY_MOD_BITS["NC"]) or (
            acronym == "SD" and legacy_bits & LEGACY_MOD_BITS["PF"]
        ):
            continue
        else:
            mods.append(CanonicalMod(acronym))
    return tuple(mods), variant


def _progress(outcome: ScoreOutcome, fail_time_ms: int, score_time_ms: int) -> Decimal:
    if outcome is ScoreOutcome.PASSED:
        return Decimal(1)
    if score_time_ms <= 0:
        return Decimal(0)
    return min(Decimal("0.9999999"), Decimal(fail_time_ms) / Decimal(score_time_ms))


def _hit_statistics(
    ruleset: Ruleset,
    n300: int,
    n100: int,
    n50: int,
    ngeki: int,
    nkatu: int,
    nmiss: int,
) -> tuple[HitStatistic, ...]:
    if ruleset is Ruleset.OSU:
        values = (("great", n300), ("ok", n100), ("meh", n50), ("miss", nmiss))
    elif ruleset is Ruleset.TAIKO:
        values = (("great", n300), ("ok", n100), ("miss", nmiss))
    elif ruleset is Ruleset.FRUITS:
        values = (
            ("great", n300),
            ("large_tick_hit", n100),
            ("small_tick_hit", n50),
            ("small_tick_miss", nkatu),
            ("large_tick_miss", ngeki),
            ("miss", nmiss),
        )
    else:
        values = (
            ("perfect", ngeki),
            ("great", n300),
            ("good", nkatu),
            ("ok", n100),
            ("meh", n50),
            ("miss", nmiss),
        )
    return tuple(HitStatistic(name, value) for name, value in values)


def _accuracy(
    ruleset: Ruleset,
    hits: tuple[HitStatistic, ...],
    mods: tuple[CanonicalMod, ...],
) -> Decimal:
    values = {hit.hit_result: hit.actual for hit in hits}
    total = Decimal(sum(values.values()))
    if total == 0:
        return Decimal(0)
    if ruleset is Ruleset.OSU:
        return Decimal(300 * values["great"] + 100 * values["ok"] + 50 * values["meh"]) / (300 * total)
    if ruleset is Ruleset.TAIKO:
        return (Decimal(values["great"]) + Decimal("0.5") * values["ok"]) / total
    if ruleset is Ruleset.FRUITS:
        caught = values["great"] + values["large_tick_hit"] + values["small_tick_hit"]
        return Decimal(caught) / total
    if any(mod.acronym == "SV2" for mod in mods):
        numerator = (
            305 * values["perfect"]
            + 300 * values["great"]
            + 200 * values["good"]
            + 100 * values["ok"]
            + 50 * values["meh"]
        )
        return Decimal(numerator) / (305 * total)
    numerator = (
        300 * (values["perfect"] + values["great"]) + 200 * values["good"] + 100 * values["ok"] + 50 * values["meh"]
    )
    return Decimal(numerator) / (300 * total)
