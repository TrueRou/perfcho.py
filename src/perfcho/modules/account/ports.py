"""Define transaction-bound ports consumed by account registration."""

from datetime import datetime
from typing import Protocol

from perfcho.modules.account.models import PublicAccountView, RegistrationClaim, RegistrationRecord, RegistrationResult
from perfcho.modules.common.ports import UnitOfWork


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

    async def get_public_account(
        self, *, account_id: int | None = None, name_key: str | None = None
    ) -> PublicAccountView | None:
        """Return a public account selected by ID or normalized current name."""
        ...


class AccountRepositoryFactory(Protocol):
    """Bind an account repository to a unit of work's transaction resource."""

    def __call__(self, session: object) -> AccountRepository:
        """Return a repository that never owns the transaction."""
        ...
