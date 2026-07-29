"""Define transaction-bound ports consumed by Stable identity operations."""

import uuid
from datetime import datetime
from typing import Protocol

from perfcho.modules.common.ports import OutboxWriter, UnitOfWork
from perfcho.modules.identity.models import CredentialSnapshot, OpenStableSession, ResolvedStableSession


class IdentityUnitOfWork(UnitOfWork, Protocol):
    """Expose the caller-owned transaction resource to identity adapters."""

    @property
    def session(self) -> object:
        """Return the active transaction resource."""
        ...


class IdentityRepository(Protocol):
    """Persist Stable identity facts without exposing ORM entities."""

    async def find_credential(self, identifier_kind: str, identifier_key: str) -> CredentialSnapshot | None:
        """Look up a scalar credential snapshot by ID, current name, or active email."""
        ...

    async def get_current_credential(self, account_id: int) -> CredentialSnapshot | None:
        """Re-read current account, auth version, name, and password facts."""
        ...

    async def acquire_stable_session_lock(self, account_id: int) -> None:
        """Serialize normal Stable session transitions for one account."""
        ...

    async def find_open_stable_session(self, account_id: int) -> OpenStableSession | None:
        """Return the account's unclosed normal Stable session, including stale rows."""
        ...

    async def get_or_create_device(
        self,
        *,
        proposed_device_id: uuid.UUID,
        fingerprint_hmac: bytes,
        component_hmacs: tuple[tuple[str, bytes], ...],
        account_id: int,
        platform: str | None,
        now: datetime,
    ) -> uuid.UUID:
        """Upsert HMAC-only device and account-device facts."""
        ...

    async def create_stable_session(
        self,
        *,
        session_id: uuid.UUID,
        token_id: uuid.UUID,
        token_jti: uuid.UUID,
        account_id: int,
        device_id: uuid.UUID,
        client_version: str,
        client_variant: str | None,
        ip_address: str,
        user_agent: str | None,
        token_digest: bytes,
        token_prefix: str,
        now: datetime,
        expires_at: datetime,
    ) -> None:
        """Create a normal Stable session and digest-only bearer token."""
        ...

    async def append_auth_attempt(
        self,
        *,
        account_id: int | None,
        session_id: uuid.UUID | None,
        device_id: uuid.UUID | None,
        identifier_hmac: bytes,
        ip_address: str,
        client_version: str | None,
        result: str,
        failure_reason: str | None,
        context: dict[str, object],
        now: datetime,
    ) -> None:
        """Append non-secret success or failure authentication evidence."""
        ...

    async def resolve_stable_session(self, token_digest: bytes, *, at: datetime) -> ResolvedStableSession | None:
        """Resolve a digest only when token, session, account, and name are active."""
        ...

    async def get_stable_session_account_id(self, session_id: uuid.UUID) -> int | None:
        """Return the owning account for a normal Stable session."""
        ...

    async def close_stable_session(
        self,
        session_id: uuid.UUID,
        *,
        now: datetime,
        reason: str,
        revoke: bool,
    ) -> int | None:
        """Close or revoke one active session and revoke all of its tokens."""
        ...


class IdentityRepositoryFactory(Protocol):
    """Bind an identity repository to a unit of work transaction."""

    def __call__(self, session: object) -> IdentityRepository:
        """Return a repository that never owns the transaction."""
        ...


class IdentityOutboxWriterFactory(Protocol):
    """Bind an outbox writer to a unit of work transaction."""

    def __call__(self, session: object) -> OutboxWriter:
        """Return an outbox writer that never owns the transaction."""
        ...
