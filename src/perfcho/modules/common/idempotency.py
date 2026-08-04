"""Define protocol-neutral command receipt values and ports."""

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from perfcho.modules.common.models import JsonValue


@dataclass(frozen=True, slots=True)
class CommandClaim:
    """Describe a new command claim or an exact completed replay."""

    replayed: bool
    resource_type: str | None
    resource_id: str | None
    result_snapshot: dict[str, JsonValue]


class CommandReceiptStore(Protocol):
    """Claim and complete command receipts inside a caller transaction."""

    async def claim(
        self,
        *,
        scope: str,
        idempotency_key: str,
        request_digest: bytes,
        now: datetime,
        expires_at: datetime,
    ) -> CommandClaim:
        """Claim a key or return the prior exact result."""
        ...

    async def complete(
        self,
        *,
        scope: str,
        idempotency_key: str,
        resource_type: str,
        resource_id: str,
        result_snapshot: dict[str, JsonValue],
    ) -> None:
        """Attach a result snapshot to a new command claim."""
        ...


class CommandReceiptStoreFactory(Protocol):
    """Bind command receipts to one transaction resource."""

    def __call__(self, session: object) -> CommandReceiptStore:
        """Return a transaction-bound receipt store."""
        ...
