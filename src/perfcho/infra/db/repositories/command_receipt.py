"""Adapt PostgreSQL command receipts to the protocol-neutral port."""

from datetime import datetime
from typing import cast

from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra.db.idempotency import CommandReceiptRepository, ReceiptClaimState
from perfcho.modules.common.idempotency import CommandClaim
from perfcho.modules.common.models import JsonValue


class SqlAlchemyCommandReceiptStore:
    """Expose existing command receipt persistence through a module-owned value."""

    def __init__(self, session: AsyncSession) -> None:
        """Bind receipt operations to one caller transaction."""
        self._repository = CommandReceiptRepository(session)

    async def claim(
        self,
        *,
        scope: str,
        idempotency_key: str,
        request_digest: bytes,
        now: datetime,
        expires_at: datetime,
    ) -> CommandClaim:
        """Claim a command key or map its exact replay result."""
        claim = await self._repository.claim(
            scope=scope,
            idempotency_key=idempotency_key,
            request_digest=request_digest,
            now=now,
            expires_at=expires_at,
        )
        return CommandClaim(
            claim.state is ReceiptClaimState.REPLAY,
            claim.resource_type,
            claim.resource_id,
            cast(dict[str, JsonValue], claim.result_snapshot),
        )

    async def complete(
        self,
        *,
        scope: str,
        idempotency_key: str,
        resource_type: str,
        resource_id: str,
        result_snapshot: dict[str, JsonValue],
    ) -> None:
        """Persist one non-secret management result snapshot."""
        await self._repository.complete(
            scope=scope,
            idempotency_key=idempotency_key,
            resource_type=resource_type,
            resource_id=resource_id,
            result_snapshot=cast(dict[str, object], result_snapshot),
        )
