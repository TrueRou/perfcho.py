"""Cache short-lived Stable Web password verification proofs in Redis."""

import hmac
import uuid

from redis.asyncio import Redis


class RedisStableWebVerificationCache:
    """Store no password material, only session-bound HMAC digests with a short TTL."""

    _VALUE_SIZE = 16 + 32 + 32

    def __init__(self, redis: Redis, *, prefix: str, ttl_seconds: int) -> None:
        """Bind a binary Redis client, versioned namespace, and bounded lifetime."""
        if not isinstance(redis, Redis):
            raise TypeError("redis must be a redis.asyncio.Redis instance")
        if isinstance(ttl_seconds, bool) or not 1 <= ttl_seconds <= 300:
            raise ValueError("Stable Web verification cache TTL must be between 1 and 300 seconds")
        self._redis = redis
        self._prefix = prefix.rstrip(":")
        self._ttl_seconds = ttl_seconds

    def _key(self, account_id: int) -> str:
        if isinstance(account_id, bool) or account_id < 1:
            raise ValueError("account_id must be positive")
        return f"{self._prefix}:v2:identity:web-verification:{account_id}"

    async def matches(
        self,
        *,
        account_id: int,
        session_id: uuid.UUID,
        password_proof: bytes,
        credential_fingerprint: bytes,
    ) -> bool:
        """Compare all cached fields without exposing digest timing differences."""
        value = await self._redis.get(self._key(account_id))
        if not isinstance(value, bytes) or len(value) != self._VALUE_SIZE:
            return False
        return (
            hmac.compare_digest(value[:16], session_id.bytes)
            and hmac.compare_digest(value[16:48], password_proof)
            and hmac.compare_digest(value[48:], credential_fingerprint)
        )

    async def store(
        self,
        *,
        account_id: int,
        session_id: uuid.UUID,
        password_proof: bytes,
        credential_fingerprint: bytes,
    ) -> None:
        """Store one fixed-width proof under the configured short TTL."""
        if len(password_proof) != 32 or len(credential_fingerprint) != 32:
            raise ValueError("Stable Web verification digests must be 32 bytes")
        await self._redis.set(
            self._key(account_id),
            session_id.bytes + password_proof + credential_fingerprint,
            ex=self._ttl_seconds,
        )
