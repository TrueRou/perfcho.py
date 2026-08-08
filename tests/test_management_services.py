"""Unit-test management command validation and transaction side effects."""

import uuid
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import cast

import pytest

from perfcho.infra.cache import MemoryCache
from perfcho.modules.audit import AuditEventValue
from perfcho.modules.authorization import (
    AuthorizationGrant,
    AuthorizationManagementService,
    GrantRole,
)
from perfcho.modules.authorization.models import EffectiveAuthorization
from perfcho.modules.authorization.ports import AuthorizationManagementRepository, AuthorizationRepository
from perfcho.modules.common import Actor, ClientContext, CommandMeta
from perfcho.modules.common.errors import AuthenticationFailed, IdempotencyConflict, InputRejected, ResourceConflict
from perfcho.modules.common.idempotency import CommandClaim, CommandReceiptStore
from perfcho.modules.moderation import ImposeSanction, ModerationCase, ModerationService, OpenCase
from perfcho.modules.moderation.commands import SanctionRecord
from perfcho.modules.moderation.ports import ModerationRepository

NOW = datetime(2026, 8, 3, 12, tzinfo=UTC)


class FakeClock:
    def now(self) -> datetime:
        return NOW


@dataclass
class FakeUow:
    session: object = object()
    committed: bool = False

    async def __aenter__(self) -> FakeUow:
        return self

    async def __aexit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        return None

    async def commit(self) -> None:
        self.committed = True


class FakeAudit:
    def __init__(self) -> None:
        self.events: list[AuditEventValue] = []

    async def append(self, event: AuditEventValue) -> int:
        self.events.append(event)
        return len(self.events)


class FakeOutbox:
    def __init__(self) -> None:
        self.events: list[object] = []

    async def append(self, event: object) -> uuid.UUID:
        self.events.append(event)
        return uuid.uuid7()


class FakeReceipts:
    def __init__(self) -> None:
        self.claims: dict[tuple[str, str], tuple[bytes, dict[str, object] | None]] = {}

    async def claim(self, **values: object) -> CommandClaim:
        key = (cast(str, values["scope"]), cast(str, values["idempotency_key"]))
        digest = cast(bytes, values["request_digest"])
        existing = self.claims.get(key)
        if existing is None:
            self.claims[key] = (digest, None)
            return CommandClaim(False, None, None, {})
        if existing[0] != digest:
            raise IdempotencyConflict("digest conflict")
        snapshot = existing[1]
        return CommandClaim(snapshot is not None, None, None, cast(dict, snapshot or {}))

    async def complete(self, **values: object) -> None:
        key = (cast(str, values["scope"]), cast(str, values["idempotency_key"]))
        digest, _ = self.claims[key]
        self.claims[key] = (digest, cast(dict[str, object], values["result_snapshot"]))


class FakeAuthorizationRepository:
    def __init__(self) -> None:
        self.audit = EffectiveAuthorization(
            1, NOW, frozenset({"admin.access", "moderation.enforce"}), frozenset(), frozenset()
        )
        self.grants: list[AuthorizationGrant] = []
        self.accounts = {1, 2}

    async def account_exists(self, account_id: int) -> bool:
        return account_id in self.accounts

    async def get_effective(self, account_id: int, *, at: datetime) -> EffectiveAuthorization:
        del at
        if account_id == 1:
            return self.audit
        return EffectiveAuthorization(account_id, NOW, frozenset(), frozenset(), frozenset())

    async def grant_role(self, **values: object) -> AuthorizationGrant:
        account_id = cast(int, values["account_id"])
        if any(grant.account_id == account_id for grant in self.grants):
            raise ResourceConflict("duplicate")
        result = AuthorizationGrant(
            uuid.uuid7(),
            account_id,
            "role",
            cast(str, values["role_code"]),
            None,
            cast(datetime, values["starts_at"]),
            cast(datetime | None, values["ends_at"]),
            None,
        )
        self.grants.append(result)
        return result


class FakeModerationRepository:
    def __init__(self) -> None:
        self.accounts = {1, 2}
        self.case: ModerationCase | None = None
        self.sanction: SanctionRecord | None = None

    async def account_exists(self, account_id: int) -> bool:
        return account_id in self.accounts

    async def open_case(self, **values: object) -> ModerationCase:
        self.case = ModerationCase(
            uuid.uuid7(),
            cast(int, values["subject_account_id"]),
            "open",
            cast(str, values["summary"]),
            cast(int, values["severity"]),
        )
        return self.case

    async def get_case(self, case_id: uuid.UUID, *, for_update: bool = False) -> ModerationCase | None:
        del for_update
        return self.case if self.case is not None and self.case.case_id == case_id else None

    async def impose_sanction(self, **values: object) -> SanctionRecord:
        self.sanction = SanctionRecord(
            uuid.uuid7(),
            cast(uuid.UUID, values["case_id"]),
            cast(int, values["subject_account_id"]),
            cast(str, values["kind"]),
            cast(int | None, values["channel_id"]),
            cast(int | None, values["team_id"]),
            cast(datetime, values["starts_at"]),
            cast(datetime | None, values["ends_at"]),
            cast(str, values["reason"]),
            None,
        )
        return self.sanction


class FakeAuthorizationPolicy:
    async def get_effective(self, account_id: int, *, at: datetime) -> EffectiveAuthorization:
        return EffectiveAuthorization(account_id, at, frozenset({"moderation.enforce"}), frozenset(), frozenset())


def meta(actor_id: int | None = 1) -> CommandMeta:
    return CommandMeta(
        uuid.uuid7(),
        "management-test",
        b"x" * 32,
        Actor(actor_id, None) if actor_id is not None else None,
        ClientContext("api", "test", None, "127.0.0.1"),
        NOW,
    )


def authorization_service(
    repository: FakeAuthorizationRepository,
    receipts: FakeReceipts | None = None,
) -> tuple[AuthorizationManagementService, FakeUow, FakeAudit, FakeOutbox]:
    uow = FakeUow()
    audit = FakeAudit()
    outbox = FakeOutbox()
    service = AuthorizationManagementService(
        lambda: uow,
        lambda session: cast(AuthorizationManagementRepository, repository),
        lambda session: audit,
        lambda session: outbox,
        FakeClock(),
        MemoryCache(),
        (lambda session: cast(CommandReceiptStore, receipts)) if receipts is not None else None,
    )
    return service, uow, audit, outbox


@pytest.mark.asyncio
async def test_authorization_grant_writes_audit_and_outbox_in_one_uow() -> None:
    service, uow, audit, outbox = authorization_service(FakeAuthorizationRepository())

    result = await service.grant_role(GrantRole(meta(), 2, "moderator", NOW, NOW + timedelta(days=1), "review"))

    assert result.kind == "role"
    assert uow.committed
    assert len(audit.events) == 1
    assert len(outbox.events) == 1


@pytest.mark.asyncio
async def test_management_rejects_missing_actor_and_invalid_period() -> None:
    service, _, _, _ = authorization_service(FakeAuthorizationRepository())

    with pytest.raises(AuthenticationFailed, match="authenticated actor"):
        await service.grant_role(GrantRole(meta(None), 2, "moderator", NOW))
    with pytest.raises(InputRejected):
        await service.grant_role(GrantRole(meta(), 2, "moderator", NOW, NOW))


@pytest.mark.asyncio
async def test_authorization_exact_replay_and_digest_conflict() -> None:
    receipts = FakeReceipts()
    service, _, audit, outbox = authorization_service(FakeAuthorizationRepository(), receipts)
    command = GrantRole(meta(), 2, "moderator", NOW, reason="review")

    first = await service.grant_role(command)
    replayed = await service.grant_role(command)

    assert replayed == first
    assert len(audit.events) == len(outbox.events) == 1
    conflicting = replace(command, meta=replace(command.meta, request_digest=b"y" * 32))
    with pytest.raises(IdempotencyConflict):
        await service.grant_role(conflicting)


@pytest.mark.asyncio
async def test_moderation_rejects_subject_scope_conflicts_before_writing() -> None:
    repository = FakeModerationRepository()
    audit = FakeAudit()
    outbox = FakeOutbox()
    uow = FakeUow()
    service = ModerationService(
        lambda: uow,
        lambda session: cast(ModerationRepository, repository),
        lambda session: cast(AuthorizationRepository, FakeAuthorizationPolicy()),
        lambda session: audit,
        lambda session: outbox,
        FakeClock(),
    )
    opened = await service.open_case(OpenCase(meta(), 2, "review", 10))

    with pytest.raises(InputRejected, match="subject"):
        await service.impose_sanction(
            ImposeSanction(meta(), opened.case_id, 1, "silence", NOW, NOW + timedelta(hours=1), "reason")
        )
    with pytest.raises(InputRejected, match="scope"):
        await service.impose_sanction(
            ImposeSanction(
                meta(),
                opened.case_id,
                2,
                "silence",
                NOW,
                NOW + timedelta(hours=1),
                "reason",
                channel_id=1,
                team_id=2,
            )
        )
    assert repository.sanction is None
    assert len(audit.events) == 1
    assert len(outbox.events) == 1
