"""Generate opaque secrets and compute server-keyed SHA-256 digests."""

import hashlib
import hmac
import secrets

_MINIMUM_TOKEN_ENTROPY_BYTES = 16


def hmac_sha256_digest(value: str | bytes, *, key: bytes) -> bytes:
    """Return a 32-byte HMAC-SHA-256 digest for a UTF-8 string or raw bytes."""
    if not key:
        raise ValueError("HMAC key must not be empty")
    encoded_value = value.encode("utf-8") if isinstance(value, str) else value
    return hmac.digest(key, encoded_value, hashlib.sha256)


def verify_hmac_sha256_digest(value: str | bytes, expected_digest: bytes, *, key: bytes) -> bool:
    """Compare a keyed digest in constant time."""
    actual_digest = hmac_sha256_digest(value, key=key)
    return hmac.compare_digest(actual_digest, expected_digest)


def digest_opaque_token(token: str, *, key: bytes) -> bytes:
    """Digest an opaque token for persistence instead of storing its bearer value."""
    return hmac_sha256_digest(token, key=key)


def digest_device_component(component: str | bytes, *, key: bytes) -> bytes:
    """Digest one normalized device identifier component for persistence."""
    return hmac_sha256_digest(component, key=key)


def generate_urlsafe_token(entropy_bytes: int = 32) -> str:
    """Generate an unpadded URL-safe token with at least 128 bits of entropy."""
    if entropy_bytes < _MINIMUM_TOKEN_ENTROPY_BYTES:
        raise ValueError("URL-safe tokens require at least 16 bytes of entropy")
    return secrets.token_urlsafe(entropy_bytes)
