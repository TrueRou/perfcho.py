import hashlib
import uuid
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime
from types import TracebackType
from typing import cast
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra.db import engine as infra_db
from perfcho.infra.db.enums import AccountStatus, AccountType
from perfcho.infra.db.models.authz import AccountRoleGrant, Role
from perfcho.infra.db.models.core import Account, AccountEmail, AccountName, UserPreference, UserProfile
from perfcho.infra.db.models.events import OutboxEvent
from perfcho.infra.db.models.iam import PasswordCredential
from perfcho.infra.db.models.system import CommandReceipt
from perfcho.infra.db.repositories.account import SqlAlchemyAccountRepository
from perfcho.infra.db.repositories.outbox import SqlAlchemyOutboxWriter
from perfcho.infra.db.uow import SqlAlchemyUnitOfWorkFactory
from perfcho.infra.security.password import Argon2Policy, PasswordHash, PasswordPepper
from perfcho.modules.account import (
    AccountService,
    EmailUnavailable,
    NameUnavailable,
    RegisterAccount,
    RegistrationRejected,
    RegistrationResult,
)
from perfcho.modules.account.models import RegistrationClaim, RegistrationRecord
from perfcho.modules.common import ClientContext, CommandMeta
from perfcho.modules.common.models import PendingEvent


class FixedClock:
    def __init__(self, instant: datetime) -> None:
        self.instant = instant

    def now(self) -> datetime:
        return self.instant


class FakeUnitOfWork:
    def __init__(self, calls: list[str]) -> None:
        self.session = object()
        self.calls = calls
        self.committed = False

    async def __aenter__(self) -> FakeUnitOfWork:
        self.calls.append("enter")
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.calls.append("exit")

    async def commit(self) -> None:
        self.calls.append("commit")
        self.committed = True


class FakeAccountRepository:
    def __init__(self, calls: list[str], result: RegistrationResult) -> None:
        self.calls = calls
        self.result = result
        self.claim = RegistrationClaim()
        self.name_taken = False
        self.email_taken = False
        self.record: RegistrationRecord | None = None

    async def claim_registration(self, **kwargs: object) -> RegistrationClaim:
        self.calls.append("claim")
        return self.claim

    async def acquire_identifier_locks(self, name_key: str, email_key: str) -> None:
        self.calls.append(f"locks:{name_key}:{email_key}")

    async def name_exists(self, name_key: str) -> bool:
        self.calls.append(f"name:{name_key}")
        return self.name_taken

    async def email_exists(self, email_key: str) -> bool:
        self.calls.append(f"email:{email_key}")
        return self.email_taken

    async def create_account(self, record: RegistrationRecord) -> RegistrationResult:
        self.calls.append("create")
        self.record = record
        return self.result

    async def complete_registration(self, idempotency_key: str, result: RegistrationResult) -> None:
        self.calls.append("complete")


class FakeOutboxWriter:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls
        self.events: list[PendingEvent] = []

    async def append(self, event: PendingEvent) -> uuid.UUID:
        self.calls.append("outbox")
        self.events.append(event)
        return uuid.uuid7()


def _command(*, activate_immediately: bool = True, password: str = "a" * 32) -> RegisterAccount:
    instant = datetime(2026, 7, 28, 12, 30, tzinfo=UTC)
    return RegisterAccount(
        meta=CommandMeta(
            request_id=uuid.uuid7(),
            idempotency_key="registration:test",
            request_digest=hashlib.sha256(b"registration").digest(),
            actor=None,
            client=ClientContext("stable", "b20260728", None, "127.0.0.1"),
            received_at=instant,
        ),
        display_name="Ａlice",
        email=" Alice@Example.COM ",
        password_preverification=password,
        activate_immediately=activate_immediately,
    )


def _service(
    monkeypatch: pytest.MonkeyPatch,
    repository: FakeAccountRepository,
    outbox: FakeOutboxWriter,
    calls: list[str],
) -> tuple[AccountService, list[FakeUnitOfWork]]:
    units: list[FakeUnitOfWork] = []

    def hash_before_transaction(*args: object, **kwargs: object) -> PasswordHash:
        calls.append("hash")
        return PasswordHash("$argon2id$test", 2)

    def create_uow() -> FakeUnitOfWork:
        calls.append("uow")
        unit = FakeUnitOfWork(calls)
        units.append(unit)
        return unit

    monkeypatch.setattr("perfcho.modules.account.services.hash_password", hash_before_transaction)
    return (
        AccountService(
            uow_factory=create_uow,
            repository_factory=lambda session: repository,
            outbox_writer_factory=lambda session: outbox,
            password_pepper=PasswordPepper(2, b"pepper"),
            argon2_policy=Argon2Policy(1, 8, 1),
            clock=FixedClock(datetime(2026, 7, 28, 12, 30, tzinfo=UTC)),
        ),
        units,
    )


def test_registration_values_are_frozen_and_slotted() -> None:
    command = _command()
    result = RegistrationResult(42, "Alice", "alice@example.com", "active")

    assert command.__slots__
    assert result.__slots__
    assert result.active
    with pytest.raises(FrozenInstanceError):
        command.__setattr__("email", "other@example.com")
    with pytest.raises(ValueError, match="active or pending"):
        RegistrationResult(42, "Alice", "alice@example.com", "locked")


@pytest.mark.asyncio
async def test_registration_normalizes_hashes_before_transaction_and_commits_all_writes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    result = RegistrationResult(42, "Alice", "alice@example.com", "active")
    repository = FakeAccountRepository(calls, result)
    outbox = FakeOutboxWriter(calls)
    service, units = _service(monkeypatch, repository, outbox, calls)

    assert await service.register(_command()) == result

    assert calls == [
        "hash",
        "uow",
        "enter",
        "claim",
        "locks:alice:alice@example.com",
        "name:alice",
        "email:alice@example.com",
        "create",
        "outbox",
        "complete",
        "commit",
        "exit",
    ]
    assert units[0].committed
    assert repository.record == RegistrationRecord(
        display_name="Alice",
        name_key="alice",
        email="alice@example.com",
        email_key="alice@example.com",
        password_verifier="$argon2id$test",
        pepper_version=2,
        status="active",
        registered_at=datetime(2026, 7, 28, 12, 30, tzinfo=UTC),
    )
    event = outbox.events[0]
    assert event.event_type == "account.registered.v1"
    assert event.partition_key == "account:42"
    assert "email" not in event.payload


@pytest.mark.asyncio
async def test_exact_replay_returns_receipt_without_locks_or_writes(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    prior = RegistrationResult(42, "Alice", "alice@example.com", "active")
    repository = FakeAccountRepository(calls, prior)
    repository.claim = RegistrationClaim(prior)
    outbox = FakeOutboxWriter(calls)
    service, units = _service(monkeypatch, repository, outbox, calls)

    assert await service.register(_command()) is prior
    assert calls == ["hash", "uow", "enter", "claim", "commit", "exit"]
    assert units[0].committed
    assert outbox.events == []


@pytest.mark.asyncio
async def test_invalid_password_is_rejected_before_hashing_or_transaction(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    result = RegistrationResult(42, "Alice", "alice@example.com", "active")
    repository = FakeAccountRepository(calls, result)
    outbox = FakeOutboxWriter(calls)
    service, units = _service(monkeypatch, repository, outbox, calls)

    with pytest.raises(RegistrationRejected, match="32 lowercase"):
        await service.register(_command(password="A" * 32))

    assert calls == []
    assert units == []


@pytest.mark.asyncio
@pytest.mark.parametrize(("conflict", "error_type"), (("name", NameUnavailable), ("email", EmailUnavailable)))
async def test_registration_maps_identifier_conflicts_without_committing(
    monkeypatch: pytest.MonkeyPatch,
    conflict: str,
    error_type: type[Exception],
) -> None:
    calls: list[str] = []
    result = RegistrationResult(42, "Alice", "alice@example.com", "pending")
    repository = FakeAccountRepository(calls, result)
    setattr(repository, f"{conflict}_taken", True)
    outbox = FakeOutboxWriter(calls)
    service, units = _service(monkeypatch, repository, outbox, calls)

    with pytest.raises(error_type):
        await service.register(_command(activate_immediately=False))

    assert not units[0].committed
    assert "create" not in calls
    assert "outbox" not in calls


@pytest.mark.asyncio
async def test_sqlalchemy_repository_builds_complete_user_graph_without_committing() -> None:
    instant = datetime(2026, 7, 28, 12, 30, tzinfo=UTC)
    session = MagicMock(spec=AsyncSession)
    session.scalar = AsyncMock(return_value=1)
    session.flush = AsyncMock()

    async def assign_account_id() -> None:
        if session.flush.await_count == 1:
            account = session.add.call_args.args[0]
            account.id = 42

    session.flush.side_effect = assign_account_id
    repository = SqlAlchemyAccountRepository(session)
    result = await repository.create_account(
        RegistrationRecord(
            display_name="Alice",
            name_key="alice",
            email="alice@example.com",
            email_key="alice@example.com",
            password_verifier="$argon2id$test",
            pepper_version=2,
            status="pending",
            registered_at=instant,
        )
    )

    assert result == RegistrationResult(42, "Alice", "alice@example.com", "pending")
    account = session.add.call_args.args[0]
    assert isinstance(account, Account)
    assert account.type is AccountType.USER
    assert account.status is AccountStatus.PENDING
    records = session.add_all.call_args.args[0]
    assert {type(record) for record in records} == {
        AccountName,
        AccountEmail,
        UserProfile,
        UserPreference,
        PasswordCredential,
        AccountRoleGrant,
    }
    role_grant = next(record for record in records if isinstance(record, AccountRoleGrant))
    assert role_grant.role_id == 1
    session.commit.assert_not_called()


class ConstraintViolation(Exception):
    def __init__(self, constraint_name: str) -> None:
        super().__init__(constraint_name)
        self.constraint_name = constraint_name


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("constraint_name", "error_type"),
    (
        ("uq_account_names_current_key", NameUnavailable),
        ("uq_account_emails_active_key", EmailUnavailable),
    ),
)
async def test_sqlalchemy_repository_maps_identifier_integrity_errors(
    constraint_name: str,
    error_type: type[Exception],
) -> None:
    session = MagicMock(spec=AsyncSession)
    session.scalar = AsyncMock(return_value=1)
    session.flush = AsyncMock()

    async def flush_or_conflict() -> None:
        if session.flush.await_count == 1:
            account = session.add.call_args.args[0]
            account.id = 42
            return
        raise IntegrityError("insert", {}, ConstraintViolation(constraint_name))

    session.flush.side_effect = flush_or_conflict
    repository = SqlAlchemyAccountRepository(session)
    record = RegistrationRecord(
        display_name="Alice",
        name_key="alice",
        email="alice@example.com",
        email_key="alice@example.com",
        password_verifier="$argon2id$test",
        pepper_version=2,
        status="pending",
        registered_at=datetime(2026, 7, 28, 12, 30, tzinfo=UTC),
    )

    with pytest.raises(error_type):
        await repository.create_account(record)

    session.commit.assert_not_called()


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_registration_is_atomic_replayable_and_grants_only_user_role(postgres_database_url: str) -> None:
    db_engine = await infra_db.create_engine()
    session_factory = infra_db.create_session_factory(db_engine)
    try:
        instant = datetime.now(UTC)
        service = AccountService(
            uow_factory=SqlAlchemyUnitOfWorkFactory(session_factory),
            repository_factory=lambda session: SqlAlchemyAccountRepository(cast(AsyncSession, session)),
            outbox_writer_factory=lambda session: SqlAlchemyOutboxWriter(cast(AsyncSession, session)),
            password_pepper=PasswordPepper(1, b"integration-test-pepper"),
            argon2_policy=Argon2Policy(1, 8, 1),
            clock=FixedClock(instant),
        )
        base_command = _command()
        command = replace(base_command, meta=replace(base_command.meta, received_at=instant))

        result = await service.register(command)
        assert await service.register(command) == result

        async with session_factory() as session:
            account = await session.get(Account, result.account_id)
            assert account is not None
            assert account.type is AccountType.USER
            assert account.status is AccountStatus.ACTIVE
            assert await session.get(UserProfile, result.account_id) is not None
            assert await session.get(UserPreference, result.account_id) is not None
            credential = await session.get(PasswordCredential, result.account_id)
            assert credential is not None and credential.verifier.startswith("$argon2id$")
            email = await session.scalar(select(AccountEmail).where(AccountEmail.account_id == result.account_id))
            assert email is not None and email.is_primary and email.verified_at is not None

            role_codes = set(
                await session.scalars(
                    select(Role.code)
                    .join(AccountRoleGrant, AccountRoleGrant.role_id == Role.id)
                    .where(AccountRoleGrant.account_id == result.account_id)
                )
            )
            assert role_codes == {"user"}
            assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 1
            assert await session.scalar(select(func.count()).select_from(CommandReceipt)) == 1
    finally:
        await db_engine.dispose()
