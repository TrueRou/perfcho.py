"""Implement PostgreSQL-backed application command idempotency receipts."""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra.db.models.system import CommandReceipt
from perfcho.modules.common.errors import IdempotencyConflict


class ReceiptClaimState(StrEnum):
    """Describe whether a command claim is new or an exact replay."""

    NEW = "new"
    REPLAY = "replay"


@dataclass(frozen=True, slots=True)
class ReceiptClaim:
    """Return immutable command receipt state to an application service."""

    state: ReceiptClaimState
    resource_type: str | None
    resource_id: str | None
    result_snapshot: dict[str, object]


class CommandReceiptRepository:
    """Claim and complete idempotency keys inside the caller transaction."""

    def __init__(self, session: AsyncSession) -> None:
        """Bind receipt operations to the caller-owned transaction."""
        self._session = session

    async def claim(
        self,
        *,
        scope: str,
        idempotency_key: str,
        request_digest: bytes,
        now: datetime,
        expires_at: datetime,
    ) -> ReceiptClaim:
        """Create a claim or return the existing result for an exact replay."""
        if not scope or not idempotency_key:
            raise ValueError("scope and idempotency_key must not be empty")
        if len(request_digest) != 32:
            raise ValueError("request_digest must contain a SHA-256 digest")
        if expires_at <= now:
            raise ValueError("expires_at must be in the future")

        statement = (
            insert(CommandReceipt)
            .values(
                scope=scope,
                idempotency_key=idempotency_key,
                request_digest=request_digest,
                result_snapshot={},
                expires_at=expires_at,
            )
            .on_conflict_do_nothing(index_elements=("scope", "idempotency_key"))
            .returning(CommandReceipt.scope)
        )
        if (await self._session.execute(statement)).scalar_one_or_none() is not None:
            return ReceiptClaim(ReceiptClaimState.NEW, None, None, {})

        receipt = (
            await self._session.execute(
                select(CommandReceipt)
                .where(
                    CommandReceipt.scope == scope,
                    CommandReceipt.idempotency_key == idempotency_key,
                )
                .with_for_update()
            )
        ).scalar_one()
        if receipt.expires_at <= now:
            receipt.request_digest = request_digest
            receipt.resource_type = None
            receipt.resource_id = None
            receipt.result_snapshot = {}
            receipt.expires_at = expires_at
            return ReceiptClaim(ReceiptClaimState.NEW, None, None, {})
        if receipt.request_digest != request_digest:
            raise IdempotencyConflict("idempotency key was already used for a different request")
        return ReceiptClaim(
            ReceiptClaimState.REPLAY,
            receipt.resource_type,
            receipt.resource_id,
            dict(receipt.result_snapshot),
        )

    async def complete(
        self,
        *,
        scope: str,
        idempotency_key: str,
        resource_type: str,
        resource_id: str,
        result_snapshot: dict[str, object] | None = None,
    ) -> None:
        """Attach a non-secret result reference to a claimed receipt."""
        receipt = await self._session.get(
            CommandReceipt,
            {"scope": scope, "idempotency_key": idempotency_key},
            with_for_update=True,
        )
        if receipt is None:
            raise RuntimeError("command receipt must be claimed before completion")
        receipt.resource_type = resource_type
        receipt.resource_id = resource_id
        receipt.result_snapshot = result_snapshot or {}
