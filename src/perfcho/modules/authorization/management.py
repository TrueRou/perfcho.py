"""Provide the authorization management application service."""

import uuid
from datetime import datetime, timedelta
from typing import Protocol

from perfcho.infra.cache.backend import CacheBackend
from perfcho.modules.audit import AuditEventValue, AuditWriterFactory
from perfcho.modules.authorization.commands import (
    AuthorizationGrant,
    GrantEntitlement,
    GrantPermission,
    GrantRole,
    RevokeEntitlement,
    RevokePermission,
    RevokeRole,
)
from perfcho.modules.authorization.ports import AuthorizationManagementRepository
from perfcho.modules.common import (
    AuthorizationDenied,
    Clock,
    CommandMeta,
    InputRejected,
    OutboxWriterFactory,
    PendingEvent,
    ResourceNotFound,
    UnitOfWork,
    UnitOfWorkFactory,
)
from perfcho.modules.common.errors import AuthenticationFailed
from perfcho.modules.common.idempotency import CommandClaim, CommandReceiptStoreFactory
from perfcho.modules.common.models import JsonValue

_AUTHORIZATION_CONSUMERS = ("authorization-consumer.v1",)
_EFFECTS = frozenset({"allow", "deny"})

type _GrantCommand = GrantRole | GrantPermission | GrantEntitlement
type _RevokeCommand = RevokeRole | RevokePermission | RevokeEntitlement


class _ManagementRepositoryFactory(Protocol):
    def __call__(self, session: object) -> AuthorizationManagementRepository: ...


class AuthorizationManagementService:
    """Apply authorization changes atomically with audit and outbox facts."""

    def __init__(
        self,
        uow_factory: UnitOfWorkFactory,
        repository_factory: _ManagementRepositoryFactory,
        audit_writer_factory: AuditWriterFactory,
        outbox_writer_factory: OutboxWriterFactory,
        clock: Clock,
        cache: CacheBackend,
        receipt_store_factory: CommandReceiptStoreFactory | None = None,
    ) -> None:
        """Bind transaction, persistence, audit, event, and clock ports."""
        self._uow_factory = uow_factory
        self._repository_factory = repository_factory
        self._audit_writer_factory = audit_writer_factory
        self._outbox_writer_factory = outbox_writer_factory
        self._clock = clock
        self._receipt_store_factory = receipt_store_factory
        self._cache = cache

    async def grant_role(self, command: GrantRole) -> AuthorizationGrant:
        """Grant a role after validating actor, subject, and period."""
        _validate_code(command.role_code, "role code")
        _validate_period(command.starts_at, command.ends_at)
        return await self._grant(command, "role")

    async def revoke_role(self, command: RevokeRole) -> AuthorizationGrant:
        """Revoke a role grant exactly once."""
        return await self._revoke(command, "role")

    async def grant_permission(self, command: GrantPermission) -> AuthorizationGrant:
        """Grant or deny a direct permission after validating its effect."""
        _validate_code(command.permission_code, "permission code")
        if not isinstance(command.effect, str) or command.effect not in _EFFECTS:
            raise InputRejected("permission effect must be allow or deny")
        _validate_period(command.starts_at, command.ends_at)
        return await self._grant(command, "permission")

    async def revoke_permission(self, command: RevokePermission) -> AuthorizationGrant:
        """Revoke a direct permission grant exactly once."""
        return await self._revoke(command, "permission")

    async def grant_entitlement(self, command: GrantEntitlement) -> AuthorizationGrant:
        """Grant a product entitlement after validating source and period."""
        _validate_code(command.entitlement_code, "entitlement code")
        _validate_period(command.starts_at, command.ends_at)
        if not isinstance(command.source, str) or not command.source.strip():
            raise InputRejected("entitlement source must not be empty")
        return await self._grant(command, "entitlement")

    async def revoke_entitlement(self, command: RevokeEntitlement) -> AuthorizationGrant:
        """Revoke an entitlement grant exactly once."""
        return await self._revoke(command, "entitlement")

    async def _grant(self, command: _GrantCommand, kind: str) -> AuthorizationGrant:
        actor_id = _actor_id(command.meta)
        _validate_account_id(command.account_id)
        now = self._clock.now()
        async with self._uow_factory() as uow:
            session = _session(uow)
            repository = self._repository_factory(session)
            await _require_account(repository, actor_id)
            await self._require_manager(repository, actor_id, now)
            await _require_account(repository, command.account_id)
            claim = await self._claim(session, command.meta, f"authorization.{kind}.grant", now)
            if claim is not None and claim.replayed:
                result = _grant_from_snapshot(claim)
                await uow.commit()
                return result
            if isinstance(command, GrantRole):
                result = await repository.grant_role(
                    account_id=command.account_id,
                    role_code=command.role_code,
                    starts_at=command.starts_at,
                    ends_at=command.ends_at,
                    granted_by_id=actor_id,
                    reason=command.reason,
                )
            elif isinstance(command, GrantPermission):
                result = await repository.grant_permission(
                    account_id=command.account_id,
                    permission_code=command.permission_code,
                    effect=command.effect,
                    starts_at=command.starts_at,
                    ends_at=command.ends_at,
                    granted_by_id=actor_id,
                    reason=command.reason,
                )
            else:
                result = await repository.grant_entitlement(
                    account_id=command.account_id,
                    entitlement_code=command.entitlement_code,
                    starts_at=command.starts_at,
                    ends_at=command.ends_at,
                    granted_by_id=actor_id,
                    source=command.source,
                    reference_id=command.reference_id,
                )
            await self._record(session, command, result, actor_id, kind, action=f"authorization.{kind}.granted")
            await self._complete(session, command.meta, f"authorization.{kind}.grant", result)
            await uow.commit()
            await self._cache.delete(self._cache.key("authorization", "effective", str(result.account_id)))
            return result

    async def _revoke(self, command: _RevokeCommand, kind: str) -> AuthorizationGrant:
        actor_id = _actor_id(command.meta)
        _validate_uuid(command.grant_id)
        now = self._clock.now()
        async with self._uow_factory() as uow:
            session = _session(uow)
            repository = self._repository_factory(session)
            await _require_account(repository, actor_id)
            await self._require_manager(repository, actor_id, now)
            claim = await self._claim(session, command.meta, f"authorization.{kind}.revoke", now)
            if claim is not None and claim.replayed:
                result = _grant_from_snapshot(claim)
                await uow.commit()
                return result
            if isinstance(command, RevokeRole):
                result = await repository.revoke_role(
                    command.grant_id,
                    revoked_by_id=actor_id,
                    revoked_at=now,
                    reason=command.reason,
                )
            elif isinstance(command, RevokePermission):
                result = await repository.revoke_permission(command.grant_id, revoked_at=now)
            else:
                result = await repository.revoke_entitlement(command.grant_id, revoked_at=now)
            await self._record(session, command, result, actor_id, kind, action=f"authorization.{kind}.revoked")
            await self._complete(session, command.meta, f"authorization.{kind}.revoke", result)
            await uow.commit()
            await self._cache.delete(self._cache.key("authorization", "effective", str(result.account_id)))
            return result

    async def _require_manager(
        self,
        repository: AuthorizationManagementRepository,
        actor_id: int,
        now: datetime,
    ) -> None:
        authorization = await repository.get_effective(actor_id, at=now)
        if "admin.access" not in authorization.permission_codes:
            raise AuthorizationDenied("admin.access is required")

    async def _claim(self, session: object, meta: CommandMeta, scope: str, now: datetime) -> CommandClaim | None:
        if self._receipt_store_factory is None:
            return None
        return await self._receipt_store_factory(session).claim(
            scope=scope,
            idempotency_key=meta.idempotency_key,
            request_digest=meta.request_digest,
            now=now,
            expires_at=now + timedelta(days=1),
        )

    async def _complete(
        self,
        session: object,
        meta: CommandMeta,
        scope: str,
        result: AuthorizationGrant,
    ) -> None:
        if self._receipt_store_factory is not None:
            await self._receipt_store_factory(session).complete(
                scope=scope,
                idempotency_key=meta.idempotency_key,
                resource_type=f"authorization_{result.kind}_grant",
                resource_id=str(result.grant_id),
                result_snapshot=_grant_state(result),
            )

    async def _record(
        self,
        session: object,
        command: _GrantCommand | _RevokeCommand,
        result: AuthorizationGrant,
        actor_id: int,
        kind: str,
        *,
        action: str,
    ) -> None:
        state = _grant_state(result)
        revoked = action.endswith("revoked")
        before = {**state, "revoked_at": None} if revoked else None
        await self._audit_writer_factory(session).append(
            AuditEventValue(
                actor_account_id=actor_id,
                action=action,
                target_type=f"authorization_{kind}_grant",
                target_id=str(result.grant_id),
                request_id=command.meta.request_id,
                ip_address=command.meta.client.ip_address,
                reason=getattr(command, "reason", None),
                before=before,
                after=state,
                metadata={"subject_account_id": result.account_id, "code": result.code},
            )
        )
        event_action = "revoked" if revoked else "granted"
        payload = {
            **state,
            "actor_account_id": actor_id,
            "subject_account_id": result.account_id,
            "occurred_at": command.meta.received_at.isoformat(),
        }
        await self._outbox_writer_factory(session).append(
            PendingEvent(
                aggregate_type=f"authorization_{kind}_grant",
                aggregate_id=str(result.grant_id),
                event_type=f"authorization.{kind}-{event_action}.v1",
                schema_version=1,
                payload=payload,
                consumers=_AUTHORIZATION_CONSUMERS,
                partition_key=f"account:{result.account_id}",
            )
        )


def _validate_period(starts_at: datetime, ends_at: datetime | None) -> None:
    if not isinstance(starts_at, datetime) or starts_at.tzinfo is None or starts_at.utcoffset() is None:
        raise InputRejected("authorization timestamps must be timezone-aware")
    if ends_at is not None and (
        not isinstance(ends_at, datetime)
        or ends_at.tzinfo is None
        or ends_at.utcoffset() is None
        or ends_at <= starts_at
    ):
        raise InputRejected("authorization end must be after start")


def _validate_code(value: str, name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise InputRejected(f"{name} must not be empty")


def _validate_account_id(account_id: int) -> None:
    if isinstance(account_id, bool) or not isinstance(account_id, int) or account_id <= 0:
        raise InputRejected("account id must be positive")


def _actor_id(meta: object) -> int:
    actor = getattr(meta, "actor", None)
    account_id = getattr(actor, "account_id", None)
    if isinstance(account_id, bool) or not isinstance(account_id, int) or account_id <= 0:
        raise AuthenticationFailed("an authenticated actor is required")
    return account_id


def _validate_uuid(value: object) -> None:
    if not isinstance(value, uuid.UUID):
        raise InputRejected("grant id must be a UUID")


def _session(uow: UnitOfWork) -> object:
    session = getattr(uow, "session", None)
    if session is None:
        raise RuntimeError("management UoW must expose a session")
    return session


async def _require_account(repository: AuthorizationManagementRepository, account_id: int) -> None:
    if not await repository.account_exists(account_id):
        raise ResourceNotFound("account does not exist")


def _grant_state(result: AuthorizationGrant) -> dict[str, JsonValue]:
    return {
        "grant_id": str(result.grant_id),
        "account_id": result.account_id,
        "kind": result.kind,
        "code": result.code,
        "effect": result.effect,
        "starts_at": result.starts_at.isoformat(),
        "ends_at": result.ends_at.isoformat() if result.ends_at is not None else None,
        "revoked_at": result.revoked_at.isoformat() if result.revoked_at is not None else None,
    }


def _grant_from_snapshot(claim: CommandClaim) -> AuthorizationGrant:
    snapshot = claim.result_snapshot
    try:
        return AuthorizationGrant(
            uuid.UUID(str(snapshot["grant_id"])),
            int(str(snapshot["account_id"])),
            str(snapshot["kind"]),
            str(snapshot["code"]),
            str(snapshot["effect"]) if snapshot["effect"] is not None else None,
            datetime.fromisoformat(str(snapshot["starts_at"])),
            datetime.fromisoformat(str(snapshot["ends_at"])) if snapshot["ends_at"] is not None else None,
            datetime.fromisoformat(str(snapshot["revoked_at"])) if snapshot["revoked_at"] is not None else None,
        )
    except (KeyError, TypeError, ValueError) as error:
        raise RuntimeError("command receipt contains an invalid authorization result") from error


AuthorizationService = AuthorizationManagementService
