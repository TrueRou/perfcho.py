"""Provide the moderation application service."""

import uuid
from datetime import datetime, timedelta
from typing import Protocol

from perfcho.modules.audit import AuditEventValue, AuditWriterFactory
from perfcho.modules.authorization.ports import AuthorizationRepository
from perfcho.modules.common import (
    AuthorizationDenied,
    Clock,
    InputRejected,
    OutboxWriterFactory,
    PendingEvent,
    ResourceNotFound,
    UnitOfWork,
    UnitOfWorkFactory,
)
from perfcho.modules.common.errors import AuthenticationFailed, ResourceConflict
from perfcho.modules.common.idempotency import CommandClaim, CommandReceiptStoreFactory
from perfcho.modules.common.models import CommandMeta, JsonValue
from perfcho.modules.moderation.commands import (
    AddCaseEntry,
    CaseEntry,
    ExtendSanction,
    ImposeSanction,
    ModerationCase,
    OpenCase,
    RevokeSanction,
    SanctionRecord,
)
from perfcho.modules.moderation.ports import ModerationRepository, ModerationRepositoryFactory

_MODERATION_CONSUMERS = ("moderation-consumer.v1",)
_SANCTION_KINDS = frozenset({"restriction", "silence", "channel_mute", "tournament_ban", "leaderboard_freeze"})
_VISIBILITIES = frozenset({"staff", "subject", "public"})


class _AuthorizationRepositoryFactory(Protocol):
    def __call__(self, session: object) -> AuthorizationRepository: ...


class ModerationService:
    """Apply moderation changes atomically with audit and outbox facts."""

    def __init__(
        self,
        uow_factory: UnitOfWorkFactory,
        repository_factory: ModerationRepositoryFactory,
        authorization_repository_factory: _AuthorizationRepositoryFactory,
        audit_writer_factory: AuditWriterFactory,
        outbox_writer_factory: OutboxWriterFactory,
        clock: Clock,
        receipt_store_factory: CommandReceiptStoreFactory | None = None,
    ) -> None:
        """Bind transaction-bound repositories and application infrastructure."""
        self._uow_factory = uow_factory
        self._repository_factory = repository_factory
        self._authorization_repository_factory = authorization_repository_factory
        self._audit_writer_factory = audit_writer_factory
        self._outbox_writer_factory = outbox_writer_factory
        self._clock = clock
        self._receipt_store_factory = receipt_store_factory

    async def open_case(self, command: OpenCase) -> ModerationCase:
        """Open a case after validating actor, subject, summary, and severity."""
        actor_id = _actor_id(command.meta)
        _account_id(command.subject_account_id)
        if not isinstance(command.summary, str) or not command.summary.strip() or len(command.summary) > 255:
            raise InputRejected("case summary must contain 1..255 characters")
        if isinstance(command.severity, bool) or not 0 <= command.severity <= 100:
            raise InputRejected("case severity must be between 0 and 100")
        async with self._uow_factory() as uow:
            session = _session(uow)
            repository = self._repository_factory(session)
            await _require_account(repository, actor_id)
            await self._require_moderator(session, actor_id)
            await _require_account(repository, command.subject_account_id)
            claim = await self._claim(session, command.meta, "moderation.case.open", self._clock.now())
            if claim is not None and claim.replayed:
                result = _case_from_snapshot(claim)
                await uow.commit()
                return result
            result = await repository.open_case(
                subject_account_id=command.subject_account_id,
                opened_by_id=actor_id,
                summary=command.summary,
                severity=command.severity,
            )
            await self._record(
                session, command.meta, actor_id, "case.opened", "case", str(result.case_id), _case_state(result)
            )
            await self._complete(session, command.meta, "moderation.case.open", result, _case_state(result))
            await uow.commit()
            return result

    async def add_case_entry(self, command: AddCaseEntry) -> CaseEntry:
        """Append an entry only while its case remains open."""
        actor_id = _actor_id(command.meta)
        _validate_uuid(command.case_id)
        if (
            not isinstance(command.kind, str)
            or not command.kind.strip()
            or not isinstance(command.content, str)
            or not command.content.strip()
            or not isinstance(command.visibility, str)
            or command.visibility not in _VISIBILITIES
        ):
            raise InputRejected("case entry kind, content, or visibility is invalid")
        async with self._uow_factory() as uow:
            session = _session(uow)
            repository = self._repository_factory(session)
            await _require_account(repository, actor_id)
            await self._require_moderator(session, actor_id)
            claim = await self._claim(session, command.meta, "moderation.case.entry", self._clock.now())
            if claim is not None and claim.replayed:
                result = _entry_from_snapshot(claim)
                await uow.commit()
                return result
            case = await repository.get_case(command.case_id, for_update=True)
            if case is None:
                raise ResourceNotFound("case does not exist")
            if case.status != "open":
                raise ResourceConflict("case is not open")
            result = await repository.add_case_entry(
                case_id=command.case_id,
                author_account_id=actor_id,
                kind=command.kind,
                visibility=command.visibility,
                content=command.content,
                evidence=dict(command.evidence),
            )
            await self._record(
                session,
                command.meta,
                actor_id,
                "case.entry_added",
                "case_entry",
                str(result.entry_id),
                _entry_state(result),
            )
            await self._complete(session, command.meta, "moderation.case.entry", result, _entry_state(result))
            await uow.commit()
            return result

    async def impose_sanction(self, command: ImposeSanction) -> SanctionRecord:
        """Impose a scoped sanction for an open case subject."""
        actor_id = _actor_id(command.meta)
        _validate_uuid(command.case_id)
        _validate_period(command.starts_at, command.ends_at)
        _validate_scope(command.channel_id, command.team_id)
        if (
            not isinstance(command.kind, str)
            or command.kind not in _SANCTION_KINDS
            or not isinstance(command.reason, str)
            or not command.reason.strip()
        ):
            raise InputRejected("sanction kind or reason is invalid")
        async with self._uow_factory() as uow:
            session = _session(uow)
            repository = self._repository_factory(session)
            await _require_account(repository, actor_id)
            await self._require_moderator(session, actor_id)
            await _require_account(repository, command.subject_account_id)
            claim = await self._claim(session, command.meta, "moderation.sanction.impose", self._clock.now())
            if claim is not None and claim.replayed:
                result = _sanction_from_snapshot(claim)
                await uow.commit()
                return result
            case = await repository.get_case(command.case_id, for_update=True)
            if case is None:
                raise ResourceNotFound("case does not exist")
            if case.status != "open":
                raise ResourceConflict("case is not open")
            if case.subject_account_id != command.subject_account_id:
                raise InputRejected("sanction subject must match case subject")
            result = await repository.impose_sanction(
                case_id=command.case_id,
                subject_account_id=command.subject_account_id,
                kind=command.kind,
                channel_id=command.channel_id,
                team_id=command.team_id,
                starts_at=command.starts_at,
                ends_at=command.ends_at,
                reason=command.reason,
                imposed_by_id=actor_id,
            )
            await self._record(
                session,
                command.meta,
                actor_id,
                "sanction.imposed",
                "sanction",
                str(result.sanction_id),
                _sanction_state(result),
            )
            await self._complete(session, command.meta, "moderation.sanction.impose", result, _sanction_state(result))
            await uow.commit()
            return result

    async def extend_sanction(self, command: ExtendSanction) -> SanctionRecord:
        """Extend an existing sanction without shortening or reviving it."""
        actor_id = _actor_id(command.meta)
        _validate_uuid(command.sanction_id)
        if (
            not isinstance(command.ends_at, datetime)
            or command.ends_at.tzinfo is None
            or command.ends_at.utcoffset() is None
        ):
            raise InputRejected("sanction end must be timezone-aware")
        async with self._uow_factory() as uow:
            session = _session(uow)
            repository = self._repository_factory(session)
            await _require_account(repository, actor_id)
            await self._require_moderator(session, actor_id)
            claim = await self._claim(session, command.meta, "moderation.sanction.extend", self._clock.now())
            if claim is not None and claim.replayed:
                result = _sanction_from_snapshot(claim)
                await uow.commit()
                return result
            sanction = await repository.get_sanction(command.sanction_id, for_update=True)
            if sanction is None:
                raise ResourceNotFound("sanction does not exist")
            if sanction.revoked_at is not None or sanction.ends_at is None or command.ends_at <= sanction.ends_at:
                raise ResourceConflict("sanction cannot be extended")
            result = await repository.extend_sanction(
                command.sanction_id,
                ends_at=command.ends_at,
                actor_account_id=actor_id,
                reason=command.reason,
            )
            await self._record(
                session,
                command.meta,
                actor_id,
                "sanction.extended",
                "sanction",
                str(result.sanction_id),
                _sanction_state(result),
            )
            await self._complete(session, command.meta, "moderation.sanction.extend", result, _sanction_state(result))
            await uow.commit()
            return result

    async def revoke_sanction(self, command: RevokeSanction) -> SanctionRecord:
        """Revoke an active sanction exactly once."""
        actor_id = _actor_id(command.meta)
        _validate_uuid(command.sanction_id)
        if not isinstance(command.reason, str) or not command.reason.strip():
            raise InputRejected("revocation reason must not be empty")
        now = self._clock.now()
        async with self._uow_factory() as uow:
            session = _session(uow)
            repository = self._repository_factory(session)
            await _require_account(repository, actor_id)
            await self._require_moderator(session, actor_id)
            claim = await self._claim(session, command.meta, "moderation.sanction.revoke", now)
            if claim is not None and claim.replayed:
                result = _sanction_from_snapshot(claim)
                await uow.commit()
                return result
            result = await repository.revoke_sanction(
                command.sanction_id,
                revoked_at=now,
                revoked_by_id=actor_id,
                reason=command.reason,
            )
            await self._record(
                session,
                command.meta,
                actor_id,
                "sanction.revoked",
                "sanction",
                str(result.sanction_id),
                _sanction_state(result),
            )
            await self._complete(session, command.meta, "moderation.sanction.revoke", result, _sanction_state(result))
            await uow.commit()
            return result

    async def _require_moderator(self, session: object, actor_id: int) -> None:
        repository = self._authorization_repository_factory(session)
        authorization = await repository.get_effective(actor_id, at=self._clock.now())
        if "moderation.enforce" not in authorization.permission_codes:
            raise AuthorizationDenied("moderation.enforce is required")

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
        result: object,
        snapshot: dict[str, JsonValue],
    ) -> None:
        if self._receipt_store_factory is not None:
            resource_id = next(str(snapshot[key]) for key in ("case_id", "entry_id", "sanction_id") if key in snapshot)
            await self._receipt_store_factory(session).complete(
                scope=scope,
                idempotency_key=meta.idempotency_key,
                resource_type=type(result).__name__,
                resource_id=resource_id,
                result_snapshot=snapshot,
            )

    async def _record(
        self,
        session: object,
        meta: CommandMeta,
        actor_id: int,
        action: str,
        target_type: str,
        target_id: str,
        state: dict[str, JsonValue],
    ) -> None:
        reason = state.get("reason")
        await self._audit_writer_factory(session).append(
            AuditEventValue(
                actor_account_id=actor_id,
                action=f"moderation.{action}",
                target_type=target_type,
                target_id=target_id,
                request_id=meta.request_id,
                ip_address=meta.client.ip_address,
                reason=reason if isinstance(reason, str) else None,
                after=state,
            )
        )
        subject_account_id = state.get("subject_account_id")
        payload = {
            **state,
            "actor_account_id": actor_id,
            "subject_account_id": subject_account_id if isinstance(subject_account_id, int) else actor_id,
            "occurred_at": meta.received_at.isoformat(),
        }
        await self._outbox_writer_factory(session).append(
            PendingEvent(
                aggregate_type=target_type,
                aggregate_id=target_id,
                event_type=f"moderation.{action}.v1",
                schema_version=1,
                payload=payload,
                consumers=_MODERATION_CONSUMERS,
                partition_key=f"{target_type}:{target_id}",
            )
        )


def _actor_id(meta: object) -> int:
    actor_id = getattr(getattr(meta, "actor", None), "account_id", None)
    if isinstance(actor_id, bool) or not isinstance(actor_id, int) or actor_id <= 0:
        raise AuthenticationFailed("an authenticated actor is required")
    return actor_id


def _account_id(value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise InputRejected("account id must be positive")


def _validate_uuid(value: object) -> None:
    if not isinstance(value, uuid.UUID):
        raise InputRejected("resource id must be a UUID")


def _validate_period(starts_at: datetime, ends_at: datetime | None) -> None:
    if not isinstance(starts_at, datetime) or starts_at.tzinfo is None or starts_at.utcoffset() is None:
        raise InputRejected("sanction timestamps must be timezone-aware")
    if ends_at is not None and (
        not isinstance(ends_at, datetime)
        or ends_at.tzinfo is None
        or ends_at.utcoffset() is None
        or ends_at <= starts_at
    ):
        raise InputRejected("sanction end must be after start")


def _validate_scope(channel_id: int | None, team_id: int | None) -> None:
    if channel_id is not None:
        _account_id(channel_id)
    if team_id is not None:
        _account_id(team_id)
    if channel_id is not None and team_id is not None:
        raise InputRejected("sanction scope must target at most one channel or team")


def _session(uow: UnitOfWork) -> object:
    session = getattr(uow, "session", None)
    if session is None:
        raise RuntimeError("management UoW must expose a session")
    return session


async def _require_account(repository: ModerationRepository, account_id: int) -> None:
    if not await repository.account_exists(account_id):
        raise ResourceNotFound("account does not exist")


def _case_state(value: ModerationCase) -> dict[str, JsonValue]:
    return {
        "case_id": str(value.case_id),
        "subject_account_id": value.subject_account_id,
        "status": value.status,
        "summary": value.summary,
        "severity": value.severity,
    }


def _entry_state(value: CaseEntry) -> dict[str, JsonValue]:
    return {
        "entry_id": value.entry_id,
        "case_id": str(value.case_id),
        "author_account_id": value.author_account_id,
        "kind": value.kind,
        "visibility": value.visibility,
        "content": value.content,
    }


def _sanction_state(value: SanctionRecord) -> dict[str, JsonValue]:
    return {
        "sanction_id": str(value.sanction_id),
        "case_id": str(value.case_id),
        "subject_account_id": value.subject_account_id,
        "kind": value.kind,
        "channel_id": value.channel_id,
        "team_id": value.team_id,
        "starts_at": value.starts_at.isoformat(),
        "ends_at": value.ends_at.isoformat() if value.ends_at else None,
        "reason": value.reason,
        "revoked_at": value.revoked_at.isoformat() if value.revoked_at else None,
    }


def _case_from_snapshot(claim: CommandClaim) -> ModerationCase:
    snapshot = claim.result_snapshot
    try:
        return ModerationCase(
            uuid.UUID(str(snapshot["case_id"])),
            int(str(snapshot["subject_account_id"])),
            str(snapshot["status"]),
            str(snapshot["summary"]),
            int(str(snapshot["severity"])),
        )
    except (KeyError, TypeError, ValueError) as error:
        raise RuntimeError("command receipt contains an invalid case result") from error


def _entry_from_snapshot(claim: CommandClaim) -> CaseEntry:
    snapshot = claim.result_snapshot
    try:
        return CaseEntry(
            int(str(snapshot["entry_id"])),
            uuid.UUID(str(snapshot["case_id"])),
            int(str(snapshot["author_account_id"])),
            str(snapshot["kind"]),
            str(snapshot["visibility"]),
            str(snapshot["content"]),
        )
    except (KeyError, TypeError, ValueError) as error:
        raise RuntimeError("command receipt contains an invalid case entry result") from error


def _sanction_from_snapshot(claim: CommandClaim) -> SanctionRecord:
    snapshot = claim.result_snapshot
    try:
        return SanctionRecord(
            uuid.UUID(str(snapshot["sanction_id"])),
            uuid.UUID(str(snapshot["case_id"])),
            int(str(snapshot["subject_account_id"])),
            str(snapshot["kind"]),
            int(str(snapshot["channel_id"])) if snapshot["channel_id"] is not None else None,
            int(str(snapshot["team_id"])) if snapshot["team_id"] is not None else None,
            datetime.fromisoformat(str(snapshot["starts_at"])),
            datetime.fromisoformat(str(snapshot["ends_at"])) if snapshot["ends_at"] is not None else None,
            str(snapshot["reason"]),
            datetime.fromisoformat(str(snapshot["revoked_at"])) if snapshot["revoked_at"] is not None else None,
        )
    except (KeyError, TypeError, ValueError) as error:
        raise RuntimeError("command receipt contains an invalid sanction result") from error
