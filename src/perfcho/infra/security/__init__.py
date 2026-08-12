"""Provide protocol-independent normalization and security primitives."""

from perfcho.infra.security.password import (
    Argon2Policy,
    PasswordHash,
    PasswordPepper,
    PasswordVerification,
    PasswordVerificationStatus,
    hash_password,
    preverify_password,
    validate_password_preverification,
    verify_dummy_password,
    verify_legacy_bcrypt_md5,
    verify_password,
)
from perfcho.infra.security.tokens import (
    digest_device_component,
    digest_opaque_token,
    generate_urlsafe_token,
    hmac_sha256_digest,
    verify_hmac_sha256_digest,
)
from perfcho.modules.common.normalization import normalize_email, normalize_name

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
    "normalize_name",
    "preverify_password",
    "validate_password_preverification",
    "verify_dummy_password",
    "verify_hmac_sha256_digest",
    "verify_legacy_bcrypt_md5",
    "verify_password",
)
