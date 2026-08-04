"""Decrypt and normalize the legacy Stable modular score payload."""

from __future__ import annotations

import binascii
import hashlib
import hmac
import re
import struct
from base64 import b64decode
from collections.abc import AsyncGenerator
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from fastapi import Request
from starlette.datastructures import FormData, UploadFile
from starlette.formparsers import MultiPartException, MultiPartParser

from perfcho.infra.security.rijndael import Rijndael256Cbc
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
from perfcho.modules.scoring.mods import parse_legacy_mods

_OSU_VERSION = re.compile(r"^\d{8}$")
_INTEGER = re.compile(r"^-?\d{1,20}$")
_RULESETS = {0: Ruleset.OSU, 1: Ruleset.TAIKO, 2: Ruleset.FRUITS, 3: Ruleset.MANIA}
_RULESET_IDS = {ruleset: identifier for identifier, ruleset in _RULESETS.items()}
_MAX_ELAPSED_MS = 7 * 24 * 60 * 60 * 1000
_MAX_INT32 = 2_147_483_647
_MAX_INT64 = 9_223_372_036_854_775_807
_MIN_REPLAY_BYTES = 24
_MAX_SUBMISSION_AGE = timedelta(days=30)
_MAX_CLOCK_SKEW = timedelta(minutes=5)


@dataclass(frozen=True, slots=True)
class StableScoreSubmissionForm:
    """Carry validated Stable multipart fields and the uploaded replay."""

    encrypted_score: str
    replay: UploadFile
    exited: bool
    fail_time_ms: int
    score_time_ms: int
    password_token: str
    osu_version: str
    encrypted_client_hash: str
    iv: str
    updated_beatmap_hash: str
    storyboard_hash: str | None
    unique_ids: str


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
    legacy_mod_bits: int


async def parse_stable_submission_form(request: Request, maximum: int) -> StableScoreSubmissionForm:
    """Parse one bounded Stable multipart score submission."""
    content_length = request.headers.get("content-length")
    if content_length is not None and (
        not content_length.isascii() or not content_length.isdigit() or int(content_length) > maximum
    ):
        raise MultiPartException("Stable score submission is too large")
    if request.headers.get("content-type", "").partition(";")[0].strip().casefold() != "multipart/form-data":
        raise MultiPartException("Stable score submission must be multipart")
    parser = MultiPartParser(
        request.headers,
        _limited_multipart_stream(request, maximum),
        max_files=2,
        max_fields=32,
        max_part_size=maximum,
    )
    form = await parser.parse()
    return _parse_submission_form(form)


async def read_stable_replay(upload: UploadFile, maximum: int) -> bytes:
    """Read one bounded replay with the minimum Stable replay structure."""
    content = await upload.read(maximum + 1)
    if len(content) > maximum:
        raise ValueError("Stable replay exceeds the configured limit")
    if len(content) < _MIN_REPLAY_BYTES:
        raise ValueError("Stable replay does not contain its minimum structure")
    return content


def validate_stable_submission_evidence(
    form: StableScoreSubmissionForm,
    parsed: ParsedStableScore,
) -> None:
    """Validate Stable multipart evidence that is not inside the encrypted score."""
    updated_beatmap_hash = _md5_bytes(form.updated_beatmap_hash, "bmk")
    if not hmac.compare_digest(updated_beatmap_hash, parsed.beatmap_md5):
        raise ValueError("Stable updated beatmap hash does not match the score")
    if form.storyboard_hash is not None:
        _md5_bytes(form.storyboard_hash, "sbk")
    identifiers = form.unique_ids.split("|")
    if len(identifiers) != 2 or any(
        not value or len(value) > 1024 or not all(character.isprintable() for character in value)
        for value in identifiers
    ):
        raise ValueError("Stable unique client identifiers are invalid")


def validate_stable_submission_time(parsed: ParsedStableScore, received_at: datetime) -> None:
    """Reject Stable timestamps outside the adapter's accepted transport window."""
    if not received_at - _MAX_SUBMISSION_AGE <= parsed.attempt.ended_at <= received_at + _MAX_CLOCK_SKEW:
        raise ValueError("Stable score timestamp is outside the accepted submission window")


def stable_submission_digest(form: StableScoreSubmissionForm, replay_digest: bytes) -> bytes:
    """Hash all Stable score submission facts with unambiguous field framing."""
    fields = (
        ("score", form.encrypted_score.encode()),
        ("replay_sha256", replay_digest),
        ("x", str(int(form.exited)).encode()),
        ("ft", str(form.fail_time_ms).encode()),
        ("st", str(form.score_time_ms).encode()),
        ("pass", form.password_token.encode()),
        ("osuver", form.osu_version.encode()),
        ("s", form.encrypted_client_hash.encode()),
        ("iv", form.iv.encode()),
        ("bmk", form.updated_beatmap_hash.encode()),
        ("sbk", (form.storyboard_hash or "").encode()),
        ("c1", form.unique_ids.encode()),
    )
    digest = hashlib.sha256()
    for name, value in fields:
        encoded_name = name.encode()
        digest.update(struct.pack(">H", len(encoded_name)))
        digest.update(encoded_name)
        digest.update(struct.pack(">Q", len(value)))
        digest.update(value)
    return digest.digest()


def normalize_stable_attestation(
    form: StableScoreSubmissionForm,
    parsed: ParsedStableScore,
) -> ScoreAttestation:
    """Add verified multipart evidence to the canonical Stable attestation."""
    online_checksum = parsed.score.online_checksum
    if online_checksum is None:
        raise ValueError("Stable online checksum is missing")
    return replace(
        parsed.attestation,
        checksum=online_checksum,
        client_integrity_digest=hashlib.sha256(parsed.client_hash.encode()).digest(),
        evidence={
            **dict(parsed.attestation.evidence),
            "online_checksum": "verified",
            "updated_beatmap_hash": "verified",
            "storyboard_hash": "format_valid_authoritative_match_pending"
            if form.storyboard_hash is not None
            else "not_supplied",
            "client_hash": "format_valid_authoritative_session_match_pending",
            "unique_ids": "format_valid_authoritative_session_match_pending",
            "unique_ids_digest": hashlib.sha256(form.unique_ids.encode()).hexdigest(),
        },
    )


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
    if not supported_build.startswith(f"b{osu_version}"):
        raise ValueError("score was submitted by an unsupported Stable build")
    if fail_time_ms < 0 or score_time_ms < 0:
        raise ValueError("Stable score timing values must not be negative")
    if fail_time_ms > _MAX_ELAPSED_MS or score_time_ms > _MAX_ELAPSED_MS:
        raise ValueError("Stable score timing values are too large")
    if max(len(score_data_b64), len(client_hash_b64), len(iv_b64)) > 16_384:
        raise ValueError("encrypted Stable score fields are too large")
    try:
        iv = b64decode(iv_b64, validate=True)
        score_data = b64decode(score_data_b64, validate=True)
        encrypted_client_hash = b64decode(client_hash_b64, validate=True)
        cipher = Rijndael256Cbc(
            key=f"osu!-scoreburgr---------{osu_version}".encode(),
            iv=iv,
        )
        fields = cipher.decrypt(score_data).decode("utf-8").split(":")
        client_hash = cipher.decrypt(encrypted_client_hash).decode("utf-8")
    except (binascii.Error, IndexError, UnicodeError, ValueError) as error:
        raise ValueError("Stable score encryption is invalid") from error
    if len(fields) != 19 or not _valid_client_value(client_hash, maximum=512):
        raise ValueError("decrypted Stable score payload is invalid")

    beatmap_md5 = _hex_digest(fields[0], 16, "beatmap MD5")
    username = fields[1][:-1] if fields[1].endswith(" ") else fields[1]
    if not username or len(username) > 64:
        raise ValueError("Stable score username is invalid")
    online_checksum = _hex_digest(fields[2], 16, "online checksum")
    n300, n100, n50, ngeki, nkatu, nmiss = (_integer(field, maximum=_MAX_INT32) for field in fields[3:9])
    total_score = _integer(fields[9], maximum=_MAX_INT64)
    max_combo = _integer(fields[10], maximum=_MAX_INT32)
    perfect = _boolean(fields[11])
    try:
        grade = ScoreGrade(fields[12].upper())
    except ValueError as error:
        raise ValueError("Stable score grade is invalid") from error
    legacy_mods = _integer(fields[13], maximum=_MAX_INT32)
    passed = _boolean(fields[14])
    mode = _integer(fields[15], maximum=max(_RULESETS))
    if mode not in _RULESETS:
        raise ValueError("Stable score ruleset is invalid")
    ruleset = _RULESETS[mode]
    mods, variant = parse_legacy_mods(legacy_mods)
    try:
        ended_at = datetime.strptime(fields[16], "%y%m%d%H%M%S").replace(tzinfo=UTC)
    except ValueError as error:
        raise ValueError("Stable score timestamp is invalid") from error
    if fields[17] != osu_version:
        raise ValueError("Stable score client version marker is invalid")
    client_flags = _integer(fields[18], maximum=_MAX_INT32)
    if client_flags < 0:
        raise ValueError("Stable score client flags are invalid")
    outcome = ScoreOutcome.PASSED if passed else ScoreOutcome.ABANDONED if exited else ScoreOutcome.FAILED
    if passed and (exited or score_time_ms == 0 or fail_time_ms != 0):
        raise ValueError("passed Stable score timing fields are inconsistent")
    if not passed and fail_time_ms == 0:
        raise ValueError("failed Stable score timing fields are inconsistent")
    progress = _progress(outcome, fail_time_ms, score_time_ms)
    elapsed_ms = score_time_ms if outcome is ScoreOutcome.PASSED else fail_time_ms
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
            started_at=ended_at - timedelta(milliseconds=elapsed_ms),
            ended_at=ended_at,
            progress=progress,
            client_metadata={
                "fail_time_ms": fail_time_ms,
                "score_time_ms": score_time_ms,
                "legacy_mod_bits": legacy_mods,
                "client_flags": client_flags,
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
        legacy_mod_bits=legacy_mods,
    )


def stable_online_checksum(
    parsed: ParsedStableScore,
    *,
    osu_version: str,
    client_hash: str,
    storyboard_hash: str | None,
    username: str | None = None,
) -> bytes:
    """Recompute the Stable online checksum using bancho.py's field order."""
    values = {statistic.hit_result: statistic.actual for statistic in parsed.score.hits}
    n300, n100, n50, ngeki, nkatu, nmiss = _legacy_hit_counts(parsed.ruleset, values)
    score = parsed.score
    payload = (
        f"chickenmcnuggets{n100 + n300}o15{n50}{ngeki}smustard{nkatu}{nmiss}uu"
        f"{parsed.beatmap_md5.hex()}{score.max_combo}{score.perfect}{username or parsed.username}{score.total_score}"
        f"{score.grade.value}{parsed.legacy_mod_bits}Q{score.outcome is ScoreOutcome.PASSED}"
        f"{_RULESET_IDS[parsed.ruleset]}{osu_version}{parsed.attempt.ended_at:%y%m%d%H%M%S}"
        f"{client_hash}{storyboard_hash or ''}"
    )
    return hashlib.md5(payload.encode(), usedforsecurity=False).digest()


def verify_stable_online_checksum(
    parsed: ParsedStableScore,
    *,
    osu_version: str,
    storyboard_hash: str | None,
    username: str | None = None,
) -> None:
    """Reject a Stable online checksum mismatch with constant-time comparison."""
    supplied = parsed.score.online_checksum
    if supplied is None:
        raise ValueError("Stable online checksum is missing")
    if storyboard_hash is None:
        return
    expected = stable_online_checksum(
        parsed,
        osu_version=osu_version,
        client_hash=parsed.client_hash,
        storyboard_hash=storyboard_hash,
        username=username,
    )
    if not hmac.compare_digest(supplied, expected):
        raise ValueError("Stable online checksum does not match the submitted score")


async def _limited_multipart_stream(request: Request, maximum: int) -> AsyncGenerator[bytes]:
    total = 0
    async for chunk in request.stream():
        total += len(chunk)
        if total > maximum:
            raise MultiPartException("Stable score submission is too large")
        yield chunk


def _parse_submission_form(form: FormData) -> StableScoreSubmissionForm:
    score_parts = form.getlist("score")
    encrypted_score = next((part for part in score_parts if isinstance(part, str)), None)
    replay = next((part for part in score_parts if isinstance(part, UploadFile)), None)
    if encrypted_score is None or replay is None or len(score_parts) != 2:
        raise ValueError("Stable score multipart fields are invalid")
    return StableScoreSubmissionForm(
        encrypted_score=encrypted_score,
        replay=replay,
        exited=_form_boolean(form, "x"),
        fail_time_ms=_form_integer(form, "ft", maximum=_MAX_ELAPSED_MS),
        score_time_ms=_form_integer(form, "st", maximum=_MAX_ELAPSED_MS),
        password_token=_form_text(form, "pass", maximum=32),
        osu_version=_form_text(form, "osuver", maximum=16),
        encrypted_client_hash=_form_text(form, "s", maximum=16_384),
        iv=_form_text(form, "iv", maximum=1024),
        updated_beatmap_hash=_form_text(form, "bmk", maximum=128),
        storyboard_hash=_optional_form_text(form, "sbk", maximum=128),
        unique_ids=_form_text(form, "c1", maximum=2048),
    )


def _form_text(form: FormData, key: str, *, maximum: int) -> str:
    value = form.get(key)
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise ValueError(f"Stable score field {key} is invalid")
    return value


def _optional_form_text(form: FormData, key: str, *, maximum: int) -> str | None:
    value = form.get(key)
    if value is None or value == "":
        return None
    if not isinstance(value, str) or len(value) > maximum:
        raise ValueError(f"Stable score field {key} is invalid")
    return value


def _form_integer(form: FormData, key: str, *, maximum: int) -> int:
    value = _form_text(form, key, maximum=20)
    if not value.isascii() or not value.isdigit():
        raise ValueError(f"Stable score field {key} is invalid")
    result = int(value)
    if result > maximum:
        raise ValueError(f"Stable score field {key} is outside its supported range")
    return result


def _form_boolean(form: FormData, key: str) -> bool:
    value = _form_text(form, key, maximum=5)
    if value in {"1", "True"}:
        return True
    if value in {"0", "False"}:
        return False
    raise ValueError(f"Stable score field {key} is invalid")


def _md5_bytes(value: str, field_name: str) -> bytes:
    if len(value) != 32:
        raise ValueError(f"Stable score field {field_name} is not an MD5")
    try:
        digest = bytes.fromhex(value)
    except ValueError as error:
        raise ValueError(f"Stable score field {field_name} is not an MD5") from error
    if len(digest) != 16:
        raise ValueError(f"Stable score field {field_name} is not an MD5")
    return digest


def _integer(value: str, *, maximum: int = _MAX_INT64) -> int:
    if not _INTEGER.fullmatch(value):
        raise ValueError("Stable score integer field is invalid")
    result = int(value)
    if result < 0 or result > maximum:
        raise ValueError("Stable score integer field is outside its supported range")
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


def _valid_client_value(value: str, *, maximum: int) -> bool:
    return bool(value) and len(value) <= maximum and all(character.isprintable() for character in value)


def _legacy_hit_counts(ruleset: Ruleset, values: dict[str, int]) -> tuple[int, int, int, int, int, int]:
    if ruleset is Ruleset.OSU:
        return values["great"], values["ok"], values["meh"], 0, 0, values["miss"]
    if ruleset is Ruleset.TAIKO:
        return values["great"], values["ok"], 0, 0, 0, values["miss"]
    if ruleset is Ruleset.FRUITS:
        return (
            values["great"],
            values["large_tick_hit"],
            values["small_tick_hit"],
            values["large_tick_miss"],
            values["small_tick_miss"],
            values["miss"],
        )
    return (
        values["great"],
        values["ok"],
        values["meh"],
        values["perfect"],
        values["good"],
        values["miss"],
    )


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
