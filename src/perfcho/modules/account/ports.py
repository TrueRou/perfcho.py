"""Define transaction-bound ports consumed by account registration."""

from datetime import datetime
from typing import Protocol

from perfcho.modules.account.models import RegistrationClaim, RegistrationRecord, RegistrationResult
from perfcho.modules.common.ports import OutboxWriter, UnitOfWork


class AccountUnitOfWork(UnitOfWork, Protocol):
    """Expose the caller-owned transaction resource to bound adapters."""

    @property
    def session(self) -> object:
        """Return the active transaction resource."""
        ...


class AccountRepository(Protocol):
    """Persist account registrations without exposing ORM entities."""

    async def claim_registration(
        self,
        *,
        idempotency_key: str,
        request_digest: bytes,
        now: datetime,
        expires_at: datetime,
    ) -> RegistrationClaim:
        """Claim the command receipt or return its exact completed result."""
        ...

    async def acquire_identifier_locks(self, name_key: str, email_key: str) -> None:
        """Serialize claims for the normalized name and email in stable order."""
        ...

    async def name_exists(self, name_key: str) -> bool:
        """Return whether a current account owns the normalized name."""
        ...

    async def email_exists(self, email_key: str) -> bool:
        """Return whether an active account email owns the normalized address."""
        ...

    async def create_account(self, record: RegistrationRecord) -> RegistrationResult:
        """Create the complete user account graph and its default role grant."""
        ...

    async def complete_registration(self, idempotency_key: str, result: RegistrationResult) -> None:
        """Attach the registration result to its command receipt."""
        ...


class AccountRepositoryFactory(Protocol):
    """Bind an account repository to a unit of work's transaction resource."""

    def __call__(self, session: object) -> AccountRepository:
        """Return a repository that never owns the transaction."""
        ...


class AccountOutboxWriterFactory(Protocol):
    """Bind an outbox writer to a unit of work's transaction resource."""

    def __call__(self, session: object) -> OutboxWriter:
        """Return an outbox writer that never owns the transaction."""
        ...
