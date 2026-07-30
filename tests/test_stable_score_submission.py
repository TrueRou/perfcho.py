import hashlib
from base64 import b64encode
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from py3rijndael import Pkcs7Padding, RijndaelCbc

from perfcho.api.stable.score_submission import (
    ParsedStableScore,
    decrypt_stable_score,
    stable_online_checksum,
    verify_stable_online_checksum,
)
from perfcho.modules.scoring import Ruleset, ScoreboardVariant, ScoreGrade, ScoreOutcome
from perfcho.modules.scoring.mods import LEGACY_MOD_BITS

OSU_VERSION = "20260711"
SUPPORTED_BUILD = "b20260711.1"
IV = b"i" * 32


def encrypted_fields(fields: list[str], client_hash: str = "client-hash") -> tuple[str, str, str]:
    cipher = RijndaelCbc(
        key=f"osu!-scoreburgr---------{OSU_VERSION}".encode(),
        iv=IV,
        padding=Pkcs7Padding(32),
        block_size=32,
    )
    score = b64encode(cipher.encrypt(":".join(fields).encode())).decode()
    encrypted_client_hash = b64encode(cipher.encrypt(client_hash.encode())).decode()
    return score, encrypted_client_hash, b64encode(IV).decode()


def score_fields(*, mods: int = 0, mode: int = 0) -> list[str]:
    return [
        "a" * 32,
        "player ",
        "b" * 32,
        "10",
        "0",
        "0",
        "0",
        "0",
        "0",
        "1000000",
        "10",
        "True",
        "X",
        str(mods),
        "True",
        str(mode),
        "260729123000",
        "b20260711.1    ",
    ]


def decrypt(fields: list[str]) -> ParsedStableScore:
    score, client_hash, iv = encrypted_fields(fields)
    return decrypt_stable_score(
        score_data_b64=score,
        client_hash_b64=client_hash,
        iv_b64=iv,
        osu_version=OSU_VERSION,
        exited=False,
        fail_time_ms=0,
        score_time_ms=60_000,
        supported_build=SUPPORTED_BUILD,
    )


def expected_checksum(fields: list[str], client_hash: str = "client-hash", storyboard_hash: str = "") -> bytes:
    username = fields[1][:-1] if fields[1].endswith(" ") else fields[1]
    payload = (
        f"chickenmcnuggets{int(fields[3]) + int(fields[4])}o15{fields[5]}{fields[6]}smustard"
        f"{fields[7]}{fields[8]}uu{fields[0]}{fields[10]}{fields[11]}{username}{fields[9]}"
        f"{fields[12]}{fields[13]}Q{fields[14]}{fields[15]}{OSU_VERSION}{fields[16]}"
        f"{client_hash}{storyboard_hash}"
    )
    return hashlib.md5(payload.encode(), usedforsecurity=False).digest()


def test_stable_score_decryption_normalizes_identity_hits_and_timing() -> None:
    result = decrypt(score_fields())

    assert result.username == "player"
    assert result.beatmap_md5 == b"\xaa" * 16
    assert result.ruleset is Ruleset.OSU
    assert result.variant is ScoreboardVariant.VANILLA
    assert result.score.grade is ScoreGrade.X
    assert result.score.outcome is ScoreOutcome.PASSED
    assert result.score.accuracy == Decimal(1)
    assert result.score.client_flags == 0
    assert result.attempt.started_at == datetime(2026, 7, 29, 12, 29, tzinfo=UTC)
    assert result.attempt.ended_at - result.attempt.started_at == timedelta(minutes=1)
    assert result.attestation.verification_state == "pending"
    assert result.client_hash == "client-hash"


def test_stable_score_decryption_maps_assistance_and_composite_mod_bits() -> None:
    bits = LEGACY_MOD_BITS["RX"] | LEGACY_MOD_BITS["NC"] | LEGACY_MOD_BITS["DT"]
    result = decrypt(score_fields(mods=bits))

    assert result.variant is ScoreboardVariant.RELAX
    assert {mod.acronym for mod in result.mods} == {"RX", "NC"}


def test_stable_score_decryption_rejects_wrong_build_and_malformed_payload() -> None:
    score, client_hash, iv = encrypted_fields(score_fields())
    with pytest.raises(ValueError, match="unsupported"):
        decrypt_stable_score(
            score_data_b64=score,
            client_hash_b64=client_hash,
            iv_b64=iv,
            osu_version=OSU_VERSION,
            exited=False,
            fail_time_ms=0,
            score_time_ms=1,
            supported_build="b20250101",
        )


def test_stable_online_checksum_matches_bancho_formula_and_rejects_mismatch() -> None:
    fields = score_fields()
    expected = expected_checksum(fields)
    fields[2] = expected.hex()
    parsed = decrypt(fields)

    assert (
        stable_online_checksum(
            parsed,
            osu_version=OSU_VERSION,
            client_hash=parsed.client_hash,
            storyboard_hash=None,
        )
        == expected
    )
    verify_stable_online_checksum(parsed, osu_version=OSU_VERSION, storyboard_hash=None)

    storyboard_hash = "c" * 32
    with pytest.raises(ValueError, match="checksum"):
        verify_stable_online_checksum(parsed, osu_version=OSU_VERSION, storyboard_hash=storyboard_hash)


def test_stable_score_decryption_rejects_unbounded_time_and_client_marker() -> None:
    score, client_hash, iv = encrypted_fields(score_fields())
    with pytest.raises(ValueError, match="too large"):
        decrypt_stable_score(
            score_data_b64=score,
            client_hash_b64=client_hash,
            iv_b64=iv,
            osu_version=OSU_VERSION,
            exited=False,
            fail_time_ms=0,
            score_time_ms=10**20,
            supported_build=SUPPORTED_BUILD,
        )

    fields = score_fields()
    fields[17] = "b20260711.2"
    with pytest.raises(ValueError, match="build marker"):
        decrypt(fields)
    with pytest.raises(ValueError, match="encryption"):
        decrypt_stable_score(
            score_data_b64="not-base64",
            client_hash_b64=client_hash,
            iv_b64=iv,
            osu_version=OSU_VERSION,
            exited=False,
            fail_time_ms=0,
            score_time_ms=1,
            supported_build=SUPPORTED_BUILD,
        )
