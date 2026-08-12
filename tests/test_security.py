import base64
import hashlib
import hmac
import re
from dataclasses import FrozenInstanceError

import bcrypt
import pytest
from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError

from perfcho.infra.security import (
    Argon2Policy,
    PasswordHash,
    PasswordPepper,
    PasswordVerificationStatus,
    digest_device_component,
    digest_opaque_token,
    generate_urlsafe_token,
    hash_password,
    hmac_sha256_digest,
    normalize_email,
    normalize_name,
    preverify_password,
    validate_password_preverification,
    verify_dummy_password,
    verify_hmac_sha256_digest,
    verify_legacy_bcrypt_md5,
    verify_password,
)


def set_runtime_attribute(target: object, name: str, value: object) -> None:
    """Exercise an immutable object's assignment boundary at runtime."""
    setattr(target, name, value)


TEST_POLICY = Argon2Policy(time_cost=1, memory_cost_kib=32, parallelism=1)


def test_stable_name_normalization_uses_nfkc_casefold_and_unicode_whitespace() -> None:
    assert normalize_name("ＴｅＳＴ\u2003Straße") == "test_strasse"
    assert normalize_name("Player Name") == normalize_name("player\tname")


@pytest.mark.parametrize(
    "name",
    [
        "x",
        "a" * 16,
        "player/name",
        "player_name two",
        "[]-_",
    ],
)
def test_stable_name_normalization_rejects_invalid_names(name: str) -> None:
    with pytest.raises(ValueError):
        normalize_name(name)


def test_email_normalization_is_conservative() -> None:
    assert normalize_email(" User.Name+Filter@Example.COM ") == "user.name+filter@example.com"
    assert normalize_email("first.last@example.com") != normalize_email("firstlast@example.com")
    assert normalize_email("user+one@example.com") != normalize_email("user@example.com")


@pytest.mark.parametrize("email", ["", "missing-at.example", "two@@example.com", "user @example.com", "@example.com"])
def test_email_normalization_rejects_malformed_addresses(email: str) -> None:
    with pytest.raises(ValueError):
        normalize_email(email)


def test_password_preverification_requires_lowercase_md5_shape() -> None:
    token = "0123456789abcdef0123456789abcdef"
    assert validate_password_preverification(token) == token

    for malformed in (token.upper(), token[:-1], f"{token}0", "g" * 32, f" {token}"):
        with pytest.raises(ValueError):
            validate_password_preverification(malformed)


def test_lazer_password_uses_stable_preverification_representation() -> None:
    assert preverify_password("password") == "5f4dcc3b5aa765d61d8327deb882cf99"


def test_argon2id_password_hash_uses_appended_versioned_pepper() -> None:
    token = preverify_password("correct horse battery staple")
    pepper = PasswordPepper(version=7, secret=b"test-password-pepper")
    password_hash = hash_password(token, pepper=pepper, policy=TEST_POLICY)

    assert password_hash.verifier.startswith("$argon2id$")
    assert password_hash.pepper_version == 7
    assert verify_password(token, password_hash, pepper=pepper, policy=TEST_POLICY).verified

    with pytest.raises(VerifyMismatchError):
        PasswordHasher().verify(password_hash.verifier, token)


def test_legacy_bcrypt_verifies_the_stable_md5_token_and_treats_malformed_hashes_as_mismatch() -> None:
    token = preverify_password("password")
    verifier = bcrypt.hashpw(token.encode("ascii"), bcrypt.gensalt(rounds=4)).decode("ascii")

    assert verify_legacy_bcrypt_md5(token, verifier).verified
    for candidate_token, candidate_verifier in (
        (preverify_password("wrong"), verifier),
        (token.upper(), verifier),
        (token, "not-a-bcrypt-hash"),
        (token, "\N{SNOWMAN}"),
    ):
        result = verify_legacy_bcrypt_md5(candidate_token, candidate_verifier)
        assert result.status is PasswordVerificationStatus.MISMATCH
        assert not result.verified


def test_password_verification_rejects_wrong_input_pepper_version_and_malformed_hash() -> None:
    token = preverify_password("password")
    pepper = PasswordPepper(version=2, secret=b"current-pepper")
    password_hash = hash_password(token, pepper=pepper, policy=TEST_POLICY)

    wrong_password = verify_password(
        preverify_password("not-password"),
        password_hash,
        pepper=pepper,
        policy=TEST_POLICY,
    )
    wrong_pepper = verify_password(
        token,
        password_hash,
        pepper=PasswordPepper(version=2, secret=b"wrong-pepper"),
        policy=TEST_POLICY,
    )
    wrong_version = verify_password(
        token,
        password_hash,
        pepper=PasswordPepper(version=3, secret=b"current-pepper"),
        policy=TEST_POLICY,
    )
    malformed = verify_password(
        token,
        PasswordHash(verifier="not-an-argon-hash", pepper_version=2),
        pepper=pepper,
        policy=TEST_POLICY,
    )

    assert {wrong_password.status, wrong_pepper.status, wrong_version.status, malformed.status} == {
        PasswordVerificationStatus.MISMATCH
    }
    assert not wrong_password.verified


def test_password_verification_reports_policy_rehash_and_results_are_frozen() -> None:
    token = preverify_password("password")
    pepper = PasswordPepper(version=1, secret=b"pepper")
    password_hash = hash_password(token, pepper=pepper, policy=TEST_POLICY)
    stronger_policy = Argon2Policy(time_cost=2, memory_cost_kib=32, parallelism=1)

    result = verify_password(token, password_hash, pepper=pepper, policy=stronger_policy)

    assert result.verified
    assert result.needs_rehash
    with pytest.raises(FrozenInstanceError):
        set_runtime_attribute(result, "needs_rehash", False)


def test_dummy_password_verification_spends_current_policy_without_authenticating() -> None:
    result = verify_dummy_password(
        pepper=PasswordPepper(version=1, secret=b"dummy-test-pepper"),
        policy=TEST_POLICY,
    )

    assert result.status is PasswordVerificationStatus.MISMATCH
    assert not result.verified


@pytest.mark.parametrize(
    "policy",
    [
        Argon2Policy(time_cost=1, memory_cost_kib=32, parallelism=1),
    ],
)
def test_argon2_policy_is_explicit(policy: Argon2Policy) -> None:
    assert policy == TEST_POLICY
    with pytest.raises(ValueError):
        Argon2Policy(time_cost=0, memory_cost_kib=32, parallelism=1)
    with pytest.raises(ValueError):
        Argon2Policy(time_cost=1, memory_cost_kib=7, parallelism=1)


def test_hmac_sha256_digest_matches_standard_and_verifies_in_constant_time() -> None:
    key = b"token-hmac-key"
    value = "opaque-token"
    expected = hmac.digest(key, value.encode(), hashlib.sha256)

    assert hmac_sha256_digest(value, key=key) == expected
    assert digest_opaque_token(value, key=key) == expected
    assert digest_device_component(value, key=key) == expected
    assert len(expected) == 32
    assert verify_hmac_sha256_digest(value, expected, key=key)
    assert not verify_hmac_sha256_digest(f"{value}-changed", expected, key=key)


def test_hmac_digest_requires_an_injected_key() -> None:
    with pytest.raises(ValueError, match="key"):
        hmac_sha256_digest("value", key=b"")


def test_urlsafe_tokens_are_unique_unpadded_and_have_requested_entropy() -> None:
    tokens = {generate_urlsafe_token(32) for _ in range(20)}

    assert len(tokens) == 20
    for token in tokens:
        assert re.fullmatch(r"[A-Za-z0-9_-]+", token)
        decoded = base64.urlsafe_b64decode(token + "=" * (-len(token) % 4))
        assert len(decoded) == 32

    with pytest.raises(ValueError, match="16 bytes"):
        generate_urlsafe_token(15)
