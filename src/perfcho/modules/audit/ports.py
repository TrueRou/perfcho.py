"""Define transaction-bound audit persistence ports."""

from typing import Protocol

from perfcho.modules.audit.value import AuditEventValue


class AuditWriter(Protocol):
    """Append immutable audit facts inside the caller-owned transaction."""

    async def append(self, event: AuditEventValue) -> int:
        """Persist an audit event without committing the transaction."""
        ...


class AuditWriterFactory(Protocol):
    """Bind audit writes to one transaction resource."""

    def __call__(self, session: object) -> AuditWriter:
        """Return a transaction-bound audit writer."""
        ...
