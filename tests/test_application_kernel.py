import hashlib
import uuid
from datetime import UTC, datetime, timedelta
from typing import cast
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import LargeBinary, Table
from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra.db.locks import acquire_transaction_locks, advisory_lock_key
from perfcho.infra.db.models.system import CommandReceipt
from perfcho.infra.db.uow import SqlAlchemyUnitOfWork
from perfcho.modules.common import Actor, ClientContext, CommandMeta, PendingEvent


def test_command_context_and_event_are_immutable_and_validated() -> None:
    now = datetime.now(UTC)
    context = CommandMeta(
        request_id=uuid.uuid7(),
        idempotency_key="stable:1",
        request_digest=hashlib.sha256(b"request").digest(),
        actor=Actor(account_id=3, auth_session_id=None),
        client=ClientContext("stable", "b20260711.1", None, "127.0.0.1"),
        received_at=now,
    )
    assert context.actor is not None and context.actor.account_id == 3

    event = PendingEvent("account", "3", "account.updated.v1", 1, {}, ("projection.v1",), "account:3")
    assert event.partition_key == "account:3"
    with pytest.raises(ValueError, match="consumers"):
        PendingEvent("account", "3", "bad", 1, {}, (), "account:3")


def test_command_receipt_metadata_contract() -> None:
    table = cast(Table, CommandReceipt.__table__)
    assert tuple(column.name for column in table.primary_key.columns) == ("scope", "idempotency_key")
    assert cast(LargeBinary, table.c.request_digest.type).length == 32
    assert {index.name for index in table.indexes} >= {
        "ix_command_receipt_expiry",
        "ix_command_receipt_resource",
    }


def test_advisory_lock_keys_are_stable_signed_and_namespaced() -> None:
    first = advisory_lock_key("account", 3, "name")
    assert first == advisory_lock_key("account", 3, "name")
    assert first != advisory_lock_key("identity", 3, "name")
    assert -(2**63) <= first < 2**63


@pytest.mark.asyncio
async def test_multiple_advisory_locks_are_sorted_and_deduplicated() -> None:
    session = MagicMock(spec=AsyncSession)
    session.execute = AsyncMock()
    result = await acquire_transaction_locks(session, (3, -2, 3, 1))
    assert result == (-2, 1, 3)
    assert [call.args[1]["lock_key"] for call in session.execute.await_args_list] == [-2, 1, 3]


@pytest.mark.asyncio
async def test_unit_of_work_rolls_back_without_explicit_commit() -> None:
    session = AsyncMock(spec=AsyncSession)
    factory = MagicMock(return_value=session)
    uow = SqlAlchemyUnitOfWork(factory)
    async with uow:
        assert uow.session is session
    session.rollback.assert_awaited_once()
    session.commit.assert_not_awaited()
    session.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_unit_of_work_commits_only_when_requested() -> None:
    session = AsyncMock(spec=AsyncSession)
    factory = MagicMock(return_value=session)
    uow = SqlAlchemyUnitOfWork(factory)
    async with uow:
        await uow.commit()
    session.commit.assert_awaited_once()
    session.rollback.assert_not_awaited()
    with pytest.raises(RuntimeError, match="not active"):
        _ = uow.session


def test_command_receipt_expiry_example_is_timezone_aware() -> None:
    now = datetime.now(UTC)
    assert now + timedelta(hours=1) > now
