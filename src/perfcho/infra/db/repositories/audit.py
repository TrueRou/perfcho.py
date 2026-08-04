"""Persist protocol-neutral audit facts through SQLAlchemy."""

from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra.db.models.audit import AuditEvent
from perfcho.modules.audit.value import AuditEventValue


class SqlAlchemyAuditWriter:
    """Adapt audit facts to the caller-owned SQLAlchemy transaction."""

    def __init__(self, session: AsyncSession) -> None:
        """Bind the writer to one active session."""
        self._session = session

    async def append(self, event: AuditEventValue) -> int:
        """Insert one immutable audit event and return its database id."""
        persisted = AuditEvent(
            actor_account_id=event.actor_account_id,
            action=event.action,
            target_type=event.target_type,
            target_id=event.target_id,
            request_id=event.request_id,
            ip_address=event.ip_address,
            reason=event.reason,
            before=dict(event.before) if event.before is not None else None,
            after=dict(event.after) if event.after is not None else None,
            metadata_json=dict(event.metadata),
        )
        self._session.add(persisted)
        await self._session.flush()
        return persisted.id
