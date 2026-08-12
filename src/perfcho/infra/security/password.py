"""Create and verify the canonical password preverification representation."""

import hashlib
import re
from dataclasses import dataclass, field
from enum import StrEnum
from functools import lru_cache

import bcrypt
from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerificationError
from argon2.low_level import Type

from perfcho.infra.logging import log_event

_PASSWORD_PREVERIFICATION = re.compile(r"[0-9a-f]{32}")
_DUMMY_PASSWORD_TOKEN = "0" * 32
_DUMMY_PASSWORD_MISMATCH = "1" * 32


@dataclass(frozen=True, slots=True)
class Argon2Policy:
    """Define injected Argon2id cost and output parameters."""

    time_cost: int
    memory_cost_kib: int
    parallelism: int
    hash_length: int = 32
    salt_length: int = 16

    def __post_init__(self) -> None:
        """Reject parameters that cannot provide a valid Argon2 configuration."""
        if self.time_cost < 1:
            raise ValueError("Argon2 time_cost must be positive")
        if self.parallelism < 1:
            raise ValueError("Argon2 parallelism must be positive")
        if self.memory_cost_kib < 8 * self.parallelism:
            raise ValueError("Argon2 memory_cost_kib must be at least eight times parallelism")
        if self.hash_length < 16:
            raise ValueError("Argon2 hash_length must be at least 16 bytes")
        if self.salt_length < 16:
            raise ValueError("Argon2 salt_length must be at least 16 bytes")


@dataclass(frozen=True, slots=True)
class PasswordPepper:
    """Pair a secret password pepper with its persisted positive version."""

    version: int
    secret: bytes = field(repr=False)

    def __post_init__(self) -> None:
        """Ensure callers provide an explicit usable pepper."""
        if self.version < 1:
            raise ValueError("password pepper version must be positive")
        if not self.secret:
            raise ValueError("password pepper must not be empty")


@dataclass(frozen=True, slots=True)
class PasswordHash:
    """Carry an Argon2 verifier and the pepper version required to verify it."""

    verifier: str
    pepper_version: int


class PasswordVerificationStatus(StrEnum):
    """Describe the protocol-independent outcome of password verification."""

    MATCH = "match"
    MISMATCH = "mismatch"


@dataclass(frozen=True, slots=True)
class PasswordVerification:
    """Return password validity and whether current Argon2 costs require rehashing."""

    status: PasswordVerificationStatus
    needs_rehash: bool = False

    @property
    def verified(self) -> bool:
        """Return whether the supplied password matched the verifier."""
        return self.status is PasswordVerificationStatus.MATCH


def validate_password_preverification(token: str) -> str:
    """Validate the canonical lowercase hexadecimal password representation."""
    if _PASSWORD_PREVERIFICATION.fullmatch(token) is None:
        raise ValueError("password preverification must be exactly 32 lowercase hexadecimal characters")
    return token


def preverify_password(plaintext: str) -> str:
    """Convert plaintext into the canonical password preverification representation."""
    return hashlib.md5(plaintext.encode("utf-8"), usedforsecurity=False).hexdigest()


def hash_password(preverification: str, *, pepper: PasswordPepper, policy: Argon2Policy) -> PasswordHash:
    """Hash a validated preverification token with an appended, versioned pepper."""
    token = validate_password_preverification(preverification)
    verifier = _make_hasher(policy).hash(_append_pepper(token, pepper))
    return PasswordHash(verifier=verifier, pepper_version=pepper.version)


def verify_password(
    preverification: str,
    password_hash: PasswordHash,
    *,
    pepper: PasswordPepper,
    policy: Argon2Policy,
) -> PasswordVerification:
    """Verify a preverification token without exposing malformed credentials as errors."""
    try:
        token = validate_password_preverification(preverification)
    except ValueError:
        return PasswordVerification(PasswordVerificationStatus.MISMATCH)

    if password_hash.pepper_version != pepper.version:
        return PasswordVerification(PasswordVerificationStatus.MISMATCH)

    hasher = _make_hasher(policy)
    try:
        hasher.verify(password_hash.verifier, _append_pepper(token, pepper))
    except InvalidHashError as error:
        log_event("ERROR", "identity.password.invalid_verifier", exception=error)
        return PasswordVerification(PasswordVerificationStatus.MISMATCH)
    except VerificationError:
        return PasswordVerification(PasswordVerificationStatus.MISMATCH)

    return PasswordVerification(
        PasswordVerificationStatus.MATCH,
        needs_rehash=hasher.check_needs_rehash(password_hash.verifier),
    )


def verify_legacy_bcrypt_md5(preverification: str, verifier: str) -> PasswordVerification:
    """Verify a legacy bancho.py bcrypt hash of a Stable MD5 password token."""
    try:
        token = validate_password_preverification(preverification)
    except TypeError, UnicodeError, ValueError:
        return PasswordVerification(PasswordVerificationStatus.MISMATCH)
    try:
        matched = bcrypt.checkpw(token.encode("ascii"), verifier.encode("ascii"))
    except (TypeError, UnicodeError) as error:
        log_event("ERROR", "identity.password.invalid_legacy_verifier", exception=error)
        return PasswordVerification(PasswordVerificationStatus.MISMATCH)
    except ValueError as error:
        log_event("ERROR", "identity.password.invalid_legacy_verifier", exception=error)
        return PasswordVerification(PasswordVerificationStatus.MISMATCH)
    return PasswordVerification(
        PasswordVerificationStatus.MATCH if matched else PasswordVerificationStatus.MISMATCH,
    )


def verify_dummy_password(*, pepper: PasswordPepper, policy: Argon2Policy) -> PasswordVerification:
    """Spend one current-policy Argon2 verification without authenticating an account."""
    verification = verify_password(
        _DUMMY_PASSWORD_MISMATCH,
        _dummy_password_hash(pepper, policy),
        pepper=pepper,
        policy=policy,
    )
    if verification.verified:
        raise RuntimeError("dummy password verifier unexpectedly matched")
    return verification


@lru_cache(maxsize=16)
def _dummy_password_hash(pepper: PasswordPepper, policy: Argon2Policy) -> PasswordHash:
    return hash_password(_DUMMY_PASSWORD_TOKEN, pepper=pepper, policy=policy)


def _append_pepper(token: str, pepper: PasswordPepper) -> bytes:
    return token.encode("ascii") + pepper.secret


def _make_hasher(policy: Argon2Policy) -> PasswordHasher:
    return PasswordHasher(
        time_cost=policy.time_cost,
        memory_cost=policy.memory_cost_kib,
        parallelism=policy.parallelism,
        hash_len=policy.hash_length,
        salt_len=policy.salt_length,
        type=Type.ID,
    )
