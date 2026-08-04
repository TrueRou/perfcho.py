"""Persist moderation facts through a caller-owned SQLAlchemy session."""

import uuid
from datetime import datetime

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra.db.enums import SanctionKind
from perfcho.infra.db.models.community import Channel
from perfcho.infra.db.models.core import Account
from perfcho.infra.db.models.moderation import CaseEntry, ModerationCase, Sanction, SanctionEvent
from perfcho.infra.db.models.social import Team
from perfcho.modules.common.errors import ResourceConflict, ResourceNotFound
from perfcho.modules.moderation.commands import CaseEntry as CaseEntryRecord
from perfcho.modules.moderation.commands import ModerationCase as ModerationCaseRecord
from perfcho.modules.moderation.commands import SanctionRecord


class SqlAlchemyModerationRepository:
    """Adapt moderation persistence without leaking mapped entities."""

    def __init__(self, session: AsyncSession) -> None:
        """Bind the repository to one active transaction."""
        self._session = session

    async def account_exists(self, account_id: int) -> bool:
        """Return whether an account exists and lock it for a management write."""
        return (
            await self._session.scalar(select(Account.id).where(Account.id == account_id).with_for_update()) is not None
        )

    async def open_case(
        self,
        *,
        subject_account_id: int,
        opened_by_id: int,
        summary: str,
        severity: int,
    ) -> ModerationCaseRecord:
        """Insert one open moderation case."""
        persisted = ModerationCase(
            subject_account_id=subject_account_id,
            opened_by_id=opened_by_id,
            status="open",
            summary=summary,
            severity=severity,
        )
        self._session.add(persisted)
        await self._session.flush()
        return _case_record(persisted)

    async def get_case(self, case_id: uuid.UUID, *, for_update: bool = False) -> ModerationCaseRecord | None:
        """Load one case, optionally locking it."""
        statement = select(ModerationCase).where(ModerationCase.id == case_id)
        if for_update:
            statement = statement.with_for_update()
        persisted = await self._session.scalar(statement)
        return _case_record(persisted) if persisted is not None else None

    async def add_case_entry(
        self,
        *,
        case_id: uuid.UUID,
        author_account_id: int,
        kind: str,
        visibility: str,
        content: str,
        evidence: dict[str, object],
    ) -> CaseEntryRecord:
        """Append one immutable entry to a moderation case."""
        persisted = CaseEntry(
            case_id=case_id,
            author_account_id=author_account_id,
            kind=kind,
            visibility=visibility,
            content=content,
            evidence=evidence,
        )
        self._session.add(persisted)
        await self._session.flush()
        return CaseEntryRecord(
            persisted.id,
            persisted.case_id,
            author_account_id,
            persisted.kind,
            persisted.visibility,
            persisted.content,
        )

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
        """Insert a sanction and its initial history event."""
        await self._require_scope(channel_id, team_id)
        statement = select(Sanction.id).where(
            Sanction.subject_account_id == subject_account_id,
            Sanction.kind == SanctionKind(kind),
            Sanction.channel_id.is_(None) if channel_id is None else Sanction.channel_id == channel_id,
            Sanction.team_id.is_(None) if team_id is None else Sanction.team_id == team_id,
            Sanction.revoked_at.is_(None),
            or_(Sanction.ends_at.is_(None), Sanction.ends_at > starts_at),
        )
        if ends_at is not None:
            statement = statement.where(Sanction.starts_at < ends_at)
        if await self._session.scalar(statement) is not None:
            raise ResourceConflict("an overlapping sanction already exists for this scope")
        persisted = Sanction(
            case_id=case_id,
            subject_account_id=subject_account_id,
            kind=SanctionKind(kind),
            channel_id=channel_id,
            team_id=team_id,
            starts_at=starts_at,
            ends_at=ends_at,
            reason=reason,
            imposed_by_id=imposed_by_id,
        )
        self._session.add(persisted)
        await self._session.flush()
        self._session.add(
            SanctionEvent(
                sanction_id=persisted.id,
                actor_account_id=imposed_by_id,
                action="imposed",
                reason=reason,
                details={"starts_at": starts_at.isoformat(), "ends_at": ends_at.isoformat() if ends_at else None},
            )
        )
        return _sanction_record(persisted)

    async def get_sanction(self, sanction_id: uuid.UUID, *, for_update: bool = False) -> SanctionRecord | None:
        """Load one sanction, optionally locking it."""
        statement = select(Sanction).where(Sanction.id == sanction_id)
        if for_update:
            statement = statement.with_for_update()
        persisted = await self._session.scalar(statement)
        return _sanction_record(persisted) if persisted is not None else None

    async def extend_sanction(
        self,
        sanction_id: uuid.UUID,
        *,
        ends_at: datetime,
        actor_account_id: int,
        reason: str | None,
    ) -> SanctionRecord:
        """Extend a locked sanction and append its history event."""
        persisted = await self._session.get(Sanction, sanction_id, with_for_update=True)
        if persisted is None:
            raise ResourceNotFound("sanction does not exist")
        if persisted.revoked_at is not None or persisted.ends_at is None or ends_at <= persisted.ends_at:
            raise ResourceConflict("sanction cannot be extended")
        previous_end = persisted.ends_at
        persisted.ends_at = ends_at
        self._session.add(
            SanctionEvent(
                sanction_id=sanction_id,
                actor_account_id=actor_account_id,
                action="extended",
                reason=reason,
                details={"previous_ends_at": previous_end.isoformat(), "ends_at": ends_at.isoformat()},
            )
        )
        return _sanction_record(persisted)

    async def revoke_sanction(
        self,
        sanction_id: uuid.UUID,
        *,
        revoked_at: datetime,
        revoked_by_id: int,
        reason: str,
    ) -> SanctionRecord:
        """Revoke one sanction and append its history event."""
        persisted = await self._session.get(Sanction, sanction_id, with_for_update=True)
        if persisted is None:
            raise ResourceNotFound("sanction does not exist")
        if persisted.revoked_at is not None:
            raise ResourceConflict("sanction is already revoked")
        persisted.revoked_at = revoked_at
        persisted.revoked_by_id = revoked_by_id
        self._session.add(
            SanctionEvent(
                sanction_id=sanction_id,
                actor_account_id=revoked_by_id,
                action="revoked",
                reason=reason,
                details={"revoked_at": revoked_at.isoformat()},
            )
        )
        return _sanction_record(persisted)

    async def _require_scope(self, channel_id: int | None, team_id: int | None) -> None:
        if channel_id is not None and await self._session.get(Channel, channel_id) is None:
            raise ResourceNotFound("sanction channel scope does not exist")
        if team_id is not None and await self._session.get(Team, team_id) is None:
            raise ResourceNotFound("sanction team scope does not exist")


def _case_record(persisted: ModerationCase) -> ModerationCaseRecord:
    return ModerationCaseRecord(
        persisted.id,
        persisted.subject_account_id,
        persisted.status,
        persisted.summary,
        persisted.severity,
    )


def _sanction_record(persisted: Sanction) -> SanctionRecord:
    return SanctionRecord(
        persisted.id,
        persisted.case_id,
        persisted.subject_account_id,
        persisted.kind.value,
        persisted.channel_id,
        persisted.team_id,
        persisted.starts_at,
        persisted.ends_at,
        persisted.reason,
        persisted.revoked_at,
    )
