"""Provide protocol-independent normalization and security primitives."""

from perfcho.infra.security.password import (
    Argon2Policy,
    PasswordHash,
    PasswordPepper,
    PasswordVerification,
    PasswordVerificationStatus,
    hash_password,
    preverify_lazer_password,
    validate_stable_password_token,
    verify_password,
)
from perfcho.infra.security.tokens import (
    digest_device_component,
    digest_opaque_token,
    generate_urlsafe_token,
    hmac_sha256_digest,
    verify_hmac_sha256_digest,
)
from perfcho.modules.common.normalization import normalize_email, normalize_stable_name

__all__ = (
    "Argon2Policy",
    "PasswordHash",
    "PasswordPepper",
    "PasswordVerification",
    "PasswordVerificationStatus",
    "digest_device_component",
    "digest_opaque_token",
    "generate_urlsafe_token",
    "hash_password",
    "hmac_sha256_digest",
    "normalize_email",
    "normalize_stable_name",
    "preverify_lazer_password",
    "validate_stable_password_token",
    "verify_hmac_sha256_digest",
    "verify_password",
)
