"""Define transaction-bound moderation persistence ports."""

import uuid
from datetime import datetime
from typing import Protocol

from perfcho.modules.moderation.commands import CaseEntry, ModerationCase, SanctionRecord


class ModerationRepository(Protocol):
    """Persist moderation facts without exposing ORM entities."""

    async def account_exists(self, account_id: int) -> bool:
        """Return whether an account exists."""
        ...

    async def open_case(
        self,
        *,
        subject_account_id: int,
        opened_by_id: int,
        summary: str,
        severity: int,
    ) -> ModerationCase:
        """Create one open case."""
        ...

    async def get_case(self, case_id: uuid.UUID, *, for_update: bool = False) -> ModerationCase | None:
        """Load one case, optionally locking it for a command."""
        ...

    async def add_case_entry(
        self,
        *,
        case_id: uuid.UUID,
        author_account_id: int,
        kind: str,
        visibility: str,
        content: str,
        evidence: dict[str, object],
    ) -> CaseEntry:
        """Append one entry to a case."""
        ...

    async def impose_sanction(
        self,
        *,
        case_id: uuid.UUID,
        subject_account_id: int,
        kind: str,
        channel_id: int | None,
        team_id: int | None,
        starts_at: datetime,
        ends_at: datetime | None,
        reason: str,
        imposed_by_id: int,
    ) -> SanctionRecord:
        """Create a sanction and its initial sanction event."""
        ...

    async def get_sanction(self, sanction_id: uuid.UUID, *, for_update: bool = False) -> SanctionRecord | None:
        """Load one sanction, optionally locking it for a command."""
        ...

    async def extend_sanction(
        self,
        sanction_id: uuid.UUID,
        *,
        ends_at: datetime,
        actor_account_id: int,
        reason: str | None,
    ) -> SanctionRecord:
        """Extend a sanction and append a sanction event."""
        ...

    async def revoke_sanction(
        self,
        sanction_id: uuid.UUID,
        *,
        revoked_at: datetime,
        revoked_by_id: int,
        reason: str,
    ) -> SanctionRecord:
        """Revoke a sanction and append a sanction event."""
        ...


class ModerationRepositoryFactory(Protocol):
    """Bind moderation persistence to a transaction resource."""

    def __call__(self, session: object) -> ModerationRepository:
        """Return a transaction-bound moderation repository."""
        ...
