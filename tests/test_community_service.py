import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace, TracebackType
from typing import cast
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import Table, UniqueConstraint
from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra.db.enums import SanctionKind
from perfcho.infra.db.models.community import Message
from perfcho.infra.db.repositories.community import SqlAlchemyActiveSilencePolicy, SqlAlchemyCommunityRepository
from perfcho.modules.authorization import EffectiveAuthorization
from perfcho.modules.authorization.services import AuthorizationQueryService
from perfcho.modules.common.models import PendingEvent
from perfcho.modules.community import (
    ActiveSilence,
    ChannelAccessDenied,
    CommunityService,
    ConversationReadCursor,
    PrivateMessageRejected,
    ReadCursorResult,
    TargetAccountSilenced,
)
from perfcho.modules.community.models import (
    ChannelRecord,
    DirectConversationResult,
    DirectMessageContext,
    MessageResult,
    OfflineDirectMessage,
)
from perfcho.modules.community.ports import CommunityRepository
from tests.cache_support import MemoryCache

INSTANT = datetime(2026, 7, 29, 12, 30, tzinfo=UTC)


class FixedClock:
    def now(self) -> datetime:
        return INSTANT


class RowsResult:
    def __init__(self, rows: list[object]) -> None:
        self.rows = rows

    def all(self) -> list[object]:
        return self.rows


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


class FakeAuthorizationRepository:
    def __init__(self, permissions: frozenset[str]) -> None:
        self.permissions = permissions

    async def get_effective(self, account_id: int, *, at: datetime) -> EffectiveAuthorization:
        return EffectiveAuthorization(account_id, at, self.permissions, frozenset(), frozenset())


class FakeOutboxWriter:
    def __init__(self) -> None:
        self.events: list[PendingEvent] = []

    async def append(self, event: PendingEvent) -> uuid.UUID:
        self.events.append(event)
        return uuid.uuid7()


class FakeSilencePolicy:
    def __init__(self) -> None:
        self.global_silences: dict[int, ActiveSilence] = {}
        self.calls: list[tuple[int, int | None, datetime]] = []

    async def get_active_silence(
        self,
        account_id: int,
        *,
        channel_id: int | None,
        at: datetime,
    ) -> ActiveSilence | None:
        self.calls.append((account_id, channel_id, at))
        return self.global_silences.get(account_id)


class FakeActiveMemberships:
    def __init__(self, *, member: bool = True, count: int = 0) -> None:
        self.member = member
        self.count = count
        self.member_calls: list[tuple[int, int, datetime]] = []
        self.count_calls: list[tuple[int, datetime]] = []

    async def is_active_member(self, channel_id: int, account_id: int, *, at: datetime) -> bool:
        self.member_calls.append((channel_id, account_id, at))
        return self.member

    async def count_active_members(self, channel_id: int, *, at: datetime) -> int:
        self.count_calls.append((channel_id, at))
        return self.count


class FakeCommunityRepository:
    def __init__(self) -> None:
        self.channel = _public_channel()
        self.direct_context = _direct_context(recipient_follows_sender=True)
        self.conversation = DirectConversationResult(11, 10, 20, 2000, created=False)
        self.messages_by_client_id: dict[tuple[int, uuid.UUID], MessageResult] = {}
        self.insert_calls = 0
        self.conversation_calls = 0
        self.offline_messages: tuple[OfflineDirectMessage, ...] = ()
        self.offline_calls: list[tuple[int | None, int]] = []
        self.read_positions: dict[tuple[int, int], int] = {}
        self.valid_cursors = True
        self.conversation_cursor: ConversationReadCursor | None = ConversationReadCursor(11, 205)

    async def acquire_pair_lock(self, first_account_id: int, second_account_id: int) -> None:
        return None

    async def get_public_channel_by_stable_name(
        self,
        stable_name: str,
        account_id: int,
    ) -> ChannelRecord | None:
        return self.channel if stable_name == self.channel.stable_name else None

    async def get_channel(self, channel_id: int, account_id: int) -> ChannelRecord | None:
        return self.channel if channel_id == self.channel.channel_id else None

    async def get_direct_message_context(
        self,
        sender_account_id: int,
        recipient_account_id: int,
    ) -> DirectMessageContext:
        return self.direct_context

    async def get_or_create_direct_conversation(
        self,
        low_account_id: int,
        high_account_id: int,
        *,
        now: datetime,
    ) -> DirectConversationResult:
        self.conversation_calls += 1
        return self.conversation

    async def get_message_by_client_id(
        self,
        sender_account_id: int,
        client_message_id: uuid.UUID,
    ) -> MessageResult | None:
        return self.messages_by_client_id.get((sender_account_id, client_message_id))

    async def insert_message(
        self,
        *,
        channel_id: int,
        sender_account_id: int,
        client_message_id: uuid.UUID,
        content: str,
        is_action: bool,
        reply_to_id: int | None,
        now: datetime,
    ) -> MessageResult:
        self.insert_calls += 1
        result = MessageResult(
            1000 + self.insert_calls,
            channel_id,
            sender_account_id,
            client_message_id,
            content,
            is_action,
            reply_to_id,
            now,
        )
        self.messages_by_client_id[(sender_account_id, client_message_id)] = result
        return result

    async def message_belongs_to_channel(self, channel_id: int, message_id: int) -> bool:
        return True

    async def list_unread_direct_messages(
        self,
        account_id: int,
        *,
        after_message_id: int | None,
        limit: int,
    ) -> tuple[OfflineDirectMessage, ...]:
        self.offline_calls.append((after_message_id, limit))
        after = after_message_id or 0
        return tuple(message for message in self.offline_messages if message.message_id > after)[:limit]

    async def list_valid_direct_read_cursors(
        self,
        account_id: int,
        cursors: tuple[ConversationReadCursor, ...],
    ) -> frozenset[ConversationReadCursor]:
        return frozenset(cursors) if self.valid_cursors else frozenset()

    async def get_direct_conversation_read_cursor(
        self,
        account_id: int,
        other_account_id: int,
    ) -> ConversationReadCursor | None:
        assert (account_id, other_account_id) == (20, 30)
        return self.conversation_cursor

    async def advance_read_cursor(
        self,
        channel_id: int,
        account_id: int,
        message_id: int,
        *,
        now: datetime,
    ) -> ReadCursorResult:
        del now
        key = (account_id, channel_id)
        previous = self.read_positions.get(key, 0)
        current = max(previous, message_id)
        self.read_positions[key] = current
        return ReadCursorResult(channel_id, account_id, current, current > previous)

    async def advance_read_cursors(
        self,
        account_id: int,
        cursors: tuple[ConversationReadCursor, ...],
        *,
        now: datetime,
    ) -> tuple[ReadCursorResult, ...]:
        results: list[ReadCursorResult] = []
        for cursor in cursors:
            key = (account_id, cursor.channel_id)
            previous = self.read_positions.get(key, 0)
            current = max(previous, cursor.message_id)
            self.read_positions[key] = current
            results.append(ReadCursorResult(cursor.channel_id, account_id, current, current > previous))
        return tuple(results)


def _public_channel() -> ChannelRecord:
    return ChannelRecord(
        channel_id=7,
        kind="public",
        stable_name="#general",
        description="General",
        owner_account_id=None,
        team_id=None,
        read_permission_code="chat.read",
        write_permission_code="chat.write",
        manage_permission_code=None,
        auto_join=True,
        message_length_limit=2000,
        archived=False,
    )


def _direct_context(*, recipient_follows_sender: bool, sender_follows_recipient: bool = False) -> DirectMessageContext:
    return DirectMessageContext(
        existing_account_ids=frozenset({10, 20}),
        recipient_policy="friends",
        low_account_id=10,
        high_account_id=20,
        low_follows_high=recipient_follows_sender,
        high_follows_low=sender_follows_recipient,
        low_blocks_high=False,
        high_blocks_low=False,
    )


def _offline_message(message_id: int, channel_id: int) -> OfflineDirectMessage:
    return OfflineDirectMessage(
        message_id=message_id,
        channel_id=channel_id,
        sender_account_id=30,
        sender_name="Sender",
        client_message_id=uuid.UUID(int=message_id),
        content=f"message {message_id}",
        is_action=False,
        created_at=INSTANT,
    )


def _service(
    repository: FakeCommunityRepository,
    *,
    permissions: frozenset[str] = frozenset({"chat.read", "chat.write"}),
    silence: FakeSilencePolicy | None = None,
    memberships: FakeActiveMemberships | None = None,
) -> tuple[CommunityService, FakeOutboxWriter, list[FakeUnitOfWork]]:
    outbox = FakeOutboxWriter()
    units: list[FakeUnitOfWork] = []
    authorization = FakeAuthorizationRepository(permissions)
    silence_policy = silence or FakeSilencePolicy()
    active_memberships = memberships or FakeActiveMemberships()

    def create_uow() -> FakeUnitOfWork:
        unit = FakeUnitOfWork()
        units.append(unit)
        return unit

    return (
        CommunityService(
            create_uow,
            lambda session: cast(CommunityRepository, repository),
            AuthorizationQueryService(authorization, FixedClock(), MemoryCache()),
            lambda session: silence_policy,
            lambda session: outbox,
            FixedClock(),
            active_memberships,
        ),
        outbox,
        units,
    )


@pytest.mark.asyncio
async def test_friends_only_direct_messages_require_recipient_following_sender_not_mutual_friendship() -> None:
    repository = FakeCommunityRepository()
    service, _, _ = _service(repository)

    result = await service.send_direct_message(20, 10, uuid.uuid7(), "accepted")

    assert result.direct_recipient_account_id == 10
    assert repository.insert_calls == 1

    repository.direct_context = _direct_context(
        recipient_follows_sender=False,
        sender_follows_recipient=True,
    )
    with pytest.raises(PrivateMessageRejected):
        await service.send_direct_message(20, 10, uuid.uuid7(), "rejected")

    assert repository.insert_calls == 1


@pytest.mark.asyncio
async def test_direct_message_target_silence_is_distinct_and_carries_remaining_time() -> None:
    repository = FakeCommunityRepository()
    silence = FakeSilencePolicy()
    silence.global_silences[10] = ActiveSilence(10, "active silence", INSTANT + timedelta(seconds=90))
    service, _, units = _service(repository, silence=silence)

    with pytest.raises(TargetAccountSilenced) as raised:
        await service.send_direct_message(20, 10, uuid.uuid7(), "not delivered")

    assert raised.value.code == "target_account_silenced"
    assert raised.value.account_id == 10
    assert raised.value.ends_at == INSTANT + timedelta(seconds=90)
    assert raised.value.remaining_seconds == 90
    assert raised.value.channel_id is None
    assert (10, None, INSTANT) in silence.calls
    assert repository.conversation_calls == 0
    assert repository.insert_calls == 0
    assert not units[0].committed


@pytest.mark.asyncio
async def test_public_message_requires_authoritative_active_membership_even_with_write_permission() -> None:
    repository = FakeCommunityRepository()
    memberships = FakeActiveMemberships(member=False)
    service, outbox, units = _service(repository, memberships=memberships)

    with pytest.raises(ChannelAccessDenied, match="active channel member"):
        await service.send_public_message(20, "#general", uuid.uuid7(), "not joined")

    assert memberships.member_calls == [(7, 20, INSTANT)]
    assert repository.insert_calls == 0
    assert outbox.events == []
    assert not units[0].committed


@pytest.mark.asyncio
async def test_public_message_replay_with_same_deterministic_uuid_persists_and_emits_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = FakeCommunityRepository()
    memberships = FakeActiveMemberships(member=True)
    service, outbox, units = _service(repository, memberships=memberships)
    client_message_id = uuid.UUID("1b0bd5bf-d560-5f6d-a90d-cf55a8c81e89")
    logged: list[tuple[str, str, dict[str, object]]] = []
    monkeypatch.setattr(
        "perfcho.modules.community.services.log_event",
        lambda level, event, **fields: logged.append((level, event, fields)),
    )

    created = await service.send_public_message(20, "#general", client_message_id, "hello")
    replayed = await service.send_public_message(20, "#general", client_message_id, "hello")

    assert created.created
    assert not replayed.created
    assert replayed.message_id == created.message_id
    assert repository.insert_calls == 1
    assert memberships.member_calls == [(7, 20, INSTANT)]
    assert [event.event_type for event in outbox.events] == ["community.message-sent.v1"]
    assert all(unit.committed for unit in units)
    assert [(level, event, fields["replayed"]) for level, event, fields in logged] == [
        ("DEBUG", "community.message.committed", False),
        ("DEBUG", "community.message.committed", True),
    ]
    assert all(fields["content_length"] == 5 for _, _, fields in logged)
    assert all(not {"content", "channel_name", "sender_name"} & fields.keys() for _, _, fields in logged)


@pytest.mark.asyncio
async def test_offline_direct_messages_paginate_past_100_and_expose_conversation_read_cursors() -> None:
    repository = FakeCommunityRepository()
    repository.offline_messages = tuple(_offline_message(index, 11 if index % 2 else 12) for index in range(1, 206))
    service, _, _ = _service(repository)

    first = await service.list_unread_offline_direct_message_page(20)
    second = await service.list_unread_offline_direct_message_page(20, after_message_id=first.next_after_message_id)
    third = await service.list_unread_offline_direct_message_page(20, after_message_id=second.next_after_message_id)

    assert tuple(message.message_id for message in first.messages) == tuple(range(1, 101))
    assert tuple(message.message_id for message in second.messages) == tuple(range(101, 201))
    assert tuple(message.message_id for message in third.messages) == tuple(range(201, 206))
    assert (first.next_after_message_id, second.next_after_message_id, third.next_after_message_id) == (100, 200, None)
    assert first.read_cursors == (ConversationReadCursor(11, 99), ConversationReadCursor(12, 100))
    assert repository.offline_calls == [(None, 101), (100, 101), (200, 101)]


@pytest.mark.asyncio
async def test_stable_direct_message_read_cursors_batch_by_conversation_and_never_regress() -> None:
    repository = FakeCommunityRepository()
    service, _, units = _service(repository)

    first = await service.mark_direct_messages_read(
        20,
        (
            ConversationReadCursor(11, 90),
            ConversationReadCursor(12, 80),
            ConversationReadCursor(11, 100),
        ),
    )
    replay = await service.mark_direct_messages_read(20, (ConversationReadCursor(11, 95),))

    assert first == (
        ReadCursorResult(11, 20, 100, True),
        ReadCursorResult(12, 20, 80, True),
    )
    assert replay == (ReadCursorResult(11, 20, 100, False),)
    assert all(unit.committed for unit in units)


@pytest.mark.asyncio
async def test_mark_direct_conversation_read_uses_peer_latest_message_and_never_regresses() -> None:
    repository = FakeCommunityRepository()
    service, _, units = _service(repository)

    first = await service.mark_direct_conversation_read(20, 30)
    repository.conversation_cursor = ConversationReadCursor(11, 200)
    replay = await service.mark_direct_conversation_read(20, 30)

    assert first == ReadCursorResult(11, 20, 205, True)
    assert replay == ReadCursorResult(11, 20, 205, False)
    assert all(unit.committed for unit in units)


@pytest.mark.asyncio
async def test_global_silence_query_returns_ceil_remaining_seconds_and_permanent_bound() -> None:
    repository = FakeCommunityRepository()
    silence = FakeSilencePolicy()
    service, _, _ = _service(repository, silence=silence)

    silence.global_silences[20] = ActiveSilence(20, "timed", INSTANT + timedelta(milliseconds=90_001))
    assert await service.get_global_silence_remaining_seconds(20) == 91

    silence.global_silences[20] = ActiveSilence(20, "permanent", None)
    assert await service.get_global_silence_remaining_seconds(20) == 2**31 - 1


@pytest.mark.asyncio
async def test_channel_member_count_uses_the_same_authoritative_membership_query_as_public_send() -> None:
    repository = FakeCommunityRepository()
    memberships = FakeActiveMemberships(count=7)
    service, _, _ = _service(repository, memberships=memberships)

    assert await service.get_channel_member_count(20, 7) == 7
    assert memberships.count_calls == [(7, INSTANT)]


@pytest.mark.asyncio
async def test_global_target_silence_query_does_not_treat_channel_mute_as_global() -> None:
    class EmptyResult:
        def one_or_none(self) -> None:
            return None

    session = MagicMock(spec=AsyncSession)
    session.execute = AsyncMock(return_value=EmptyResult())
    policy = SqlAlchemyActiveSilencePolicy(session)

    assert await policy.get_active_silence(10, channel_id=None, at=INSTANT) is None

    statement = session.execute.await_args.args[0]
    parameters = statement.compile().params.values()
    assert SanctionKind.SILENCE in parameters
    assert SanctionKind.CHANNEL_MUTE not in parameters


@pytest.mark.asyncio
async def test_sqlalchemy_offline_message_query_uses_keyset_cursor_and_requested_batch_size() -> None:
    session = MagicMock(spec=AsyncSession)
    session.execute = AsyncMock(return_value=RowsResult([]))
    repository = SqlAlchemyCommunityRepository(session)

    assert await repository.list_unread_direct_messages(20, after_message_id=100, limit=101) == ()

    session.execute.assert_awaited_once()
    statement = session.execute.await_args.args[0]
    sql = str(statement)
    parameters = statement.compile().params
    assert sql.count("community.messages.id >") == 2
    assert 100 in parameters.values()
    assert 101 in parameters.values()


@pytest.mark.asyncio
async def test_sqlalchemy_direct_read_cursor_batch_upserts_once_and_returns_monotonic_positions() -> None:
    session = MagicMock(spec=AsyncSession)
    session.execute = AsyncMock(
        side_effect=(
            RowsResult([SimpleNamespace(channel_id=11, last_read_message_id=100)]),
            RowsResult(
                [
                    SimpleNamespace(channel_id=11, last_read_message_id=100),
                    SimpleNamespace(channel_id=12, last_read_message_id=90),
                ]
            ),
        )
    )
    repository = SqlAlchemyCommunityRepository(session)

    result = await repository.advance_read_cursors(
        20,
        (ConversationReadCursor(11, 100), ConversationReadCursor(12, 80)),
        now=INSTANT,
    )

    assert result == (
        ReadCursorResult(11, 20, 100, True),
        ReadCursorResult(12, 20, 90, False),
    )
    assert session.execute.await_count == 2
    upsert = session.execute.await_args_list[0].args[0]
    assert str(upsert).startswith("INSERT INTO community.channel_user_states")
    assert "ON CONFLICT" in str(upsert)


def test_message_persistence_keeps_sender_scoped_client_uuid_unique() -> None:
    unique_columns = {
        tuple(column.name for column in constraint.columns)
        for constraint in cast(Table, Message.__table__).constraints
        if isinstance(constraint, UniqueConstraint)
    }

    assert ("sender_account_id", "client_message_id") in unique_columns
