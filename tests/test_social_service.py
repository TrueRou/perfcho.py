import uuid
from datetime import UTC, datetime
from types import TracebackType
from typing import cast
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra.db.repositories.social import SqlAlchemySocialRepository
from perfcho.modules.common.models import PendingEvent
from perfcho.modules.common.ports import OutboxWriter
from perfcho.modules.social import SocialAccountNotFound, SocialInteractionBlocked, SocialService
from perfcho.modules.social.models import FollowRecord, PairRelationship
from perfcho.modules.social.ports import SocialRepository

INSTANT = datetime(2026, 7, 29, 12, 30, tzinfo=UTC)


class FixedClock:
    def now(self) -> datetime:
        return INSTANT


class FakeUnitOfWork:
    def __init__(self) -> None:
        self.session = object()
        self.committed = False

    async def __aenter__(self) -> FakeUnitOfWork:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        return None

    async def commit(self) -> None:
        self.committed = True


class FakeOutboxWriter:
    def __init__(self) -> None:
        self.events: list[PendingEvent] = []

    async def append(self, event: PendingEvent) -> uuid.UUID:
        self.events.append(event)
        return uuid.uuid7()


class FakeSocialRepository:
    def __init__(self, pair: PairRelationship) -> None:
        self.pair = pair
        self.existing = frozenset({10, 20})
        self.deleted_blocks: list[tuple[int, int]] = []
        self.upserted_follows: list[tuple[int, int]] = []
        self.pair_queries = 0
        self.blocking_ids = frozenset[int]()
        self.blocking_queries: list[tuple[int, tuple[int, ...]]] = []

    async def acquire_pair_lock(self, first_account_id: int, second_account_id: int) -> None:
        return None

    async def existing_account_ids(self, account_ids: tuple[int, ...]) -> frozenset[int]:
        return self.existing

    async def get_pair_relationship(self, first_account_id: int, second_account_id: int) -> PairRelationship:
        self.pair_queries += 1
        return self.pair

    async def delete_block(self, actor_account_id: int, target_account_id: int) -> bool:
        self.deleted_blocks.append((actor_account_id, target_account_id))
        return True

    async def get_follow(self, actor_account_id: int, target_account_id: int) -> None:
        return None

    async def upsert_follow(
        self,
        actor_account_id: int,
        target_account_id: int,
        *,
        remark: str | None,
        now: datetime,
    ) -> FollowRecord:
        self.upserted_follows.append((actor_account_id, target_account_id))
        return FollowRecord(actor_account_id, target_account_id, remark, now)

    async def list_blocking_account_ids(
        self,
        target_account_id: int,
        actor_account_ids: tuple[int, ...],
    ) -> frozenset[int]:
        self.blocking_queries.append((target_account_id, actor_account_ids))
        return self.blocking_ids


def _service(
    repository: FakeSocialRepository,
) -> tuple[SocialService, FakeOutboxWriter, list[FakeUnitOfWork]]:
    outbox = FakeOutboxWriter()
    units: list[FakeUnitOfWork] = []

    def create_uow() -> FakeUnitOfWork:
        unit = FakeUnitOfWork()
        units.append(unit)
        return unit

    return (
        SocialService(
            create_uow,
            lambda session: cast(SocialRepository, repository),
            lambda session: cast(OutboxWriter, outbox),
            FixedClock(),
        ),
        outbox,
        units,
    )


def _pair(*, actor_blocks_target: bool = False, target_blocks_actor: bool = False) -> PairRelationship:
    return PairRelationship(
        low_account_id=10,
        high_account_id=20,
        low_follows_high=False,
        high_follows_low=False,
        low_blocks_high=target_blocks_actor,
        high_blocks_low=actor_blocks_target,
    )


@pytest.mark.asyncio
async def test_stable_friend_add_removes_only_the_actors_own_block_then_follows() -> None:
    repository = FakeSocialRepository(_pair(actor_blocks_target=True))
    service, outbox, units = _service(repository)

    result = await service.follow(20, 10)

    assert result == FollowRecord(20, 10, None, INSTANT)
    assert repository.deleted_blocks == [(20, 10)]
    assert repository.upserted_follows == [(20, 10)]
    assert [event.event_type for event in outbox.events] == [
        "social.account-unblocked.v1",
        "social.account-followed.v1",
    ]
    assert units[0].committed


@pytest.mark.asyncio
async def test_friend_add_never_removes_the_targets_block() -> None:
    repository = FakeSocialRepository(_pair(actor_blocks_target=True, target_blocks_actor=True))
    service, outbox, units = _service(repository)

    with pytest.raises(SocialInteractionBlocked):
        await service.follow(20, 10)

    assert repository.deleted_blocks == []
    assert repository.upserted_follows == []
    assert outbox.events == []
    assert not units[0].committed


@pytest.mark.asyncio
async def test_friend_add_maps_a_missing_target_to_social_account_not_found_before_relation_queries() -> None:
    repository = FakeSocialRepository(_pair())
    repository.existing = frozenset({20})
    service, _, units = _service(repository)

    with pytest.raises(SocialAccountNotFound):
        await service.follow(20, 10)

    assert repository.pair_queries == 0
    assert repository.upserted_follows == []
    assert not units[0].committed


@pytest.mark.asyncio
async def test_message_recipient_filter_batches_recipient_blocks_and_preserves_order() -> None:
    repository = FakeSocialRepository(_pair())
    repository.blocking_ids = frozenset({10, 12})
    service, _, _ = _service(repository)

    result = await service.filter_message_recipients(20, (10, 11, 10, 12, 13))

    assert result == (11, 13)
    assert repository.blocking_queries == [(20, (10, 11, 12, 13))]


@pytest.mark.asyncio
async def test_sqlalchemy_recipient_block_filter_uses_one_batch_query() -> None:
    session = MagicMock(spec=AsyncSession)
    session.scalars = AsyncMock(return_value=[10, 12])
    repository = SqlAlchemySocialRepository(session)

    assert await repository.list_blocking_account_ids(20, (10, 11, 12)) == frozenset({10, 12})

    session.scalars.assert_awaited_once()
    statement = session.scalars.await_args.args[0]
    sql = str(statement)
    parameters = statement.compile().params
    assert "social.blocks.target_account_id" in sql
    assert "social.blocks.actor_account_id IN" in sql
    assert 20 in parameters.values()
    assert [10, 11, 12] in parameters.values()
