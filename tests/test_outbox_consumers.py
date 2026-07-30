import uuid
from dataclasses import replace
from datetime import UTC, datetime

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra.db import engine as infra_db
from perfcho.infra.db.enums import AccountStatus, AccountType, BeatmapStatus, ChannelKind, OutboxDeliveryStatus
from perfcho.infra.db.models.community import (
    Channel,
    ChannelReadProjection,
    DirectConversation,
    Message,
    Notification,
    NotificationDispatch,
    NotificationPreference,
    NotificationRecipient,
)
from perfcho.infra.db.models.content import Beatmapset, BeatmapsetSyncProjection
from perfcho.infra.db.models.core import Account
from perfcho.infra.db.models.events import ActivityEvent, OutboxDelivery, OutboxEvent, ProjectionCheckpoint
from perfcho.infra.db.models.social import AchievementDefinition, AchievementUnlock
from perfcho.infra.db.projectors.catalog import (
    DEFAULT_CONSUMER_CATALOG,
    ConsumerCatalog,
    ConsumerRegistration,
)
from perfcho.infra.db.relays.outbox_delivery import (
    OutboxDeliveryProcessor,
    SqlAlchemyOutboxDeliveryRelayStore,
)
from perfcho.infra.db.repositories.outbox import append_outbox_event
from perfcho.infra.settings import settings
from perfcho.modules.common.models import JsonValue, PendingEvent

NOW = datetime(2026, 7, 29, 12, 30, tzinfo=UTC)
ACTOR_ACCOUNT_ID = 2001
RECIPIENT_ACCOUNT_ID = 2002
EXPECTED_CONSUMERS = {
    "account-projector.v1": {"account.registered.v1"},
    "identity-projector.v1": {"identity.session-opened.v1", "identity.session-closed.v1"},
    "content-projector.v1": {"content.beatmapset-synchronized.v1"},
    "social-projector.v1": {
        "social.account-followed.v1",
        "social.account-unfollowed.v1",
        "social.account-blocked.v1",
        "social.account-unblocked.v1",
    },
    "achievement-projector.v1": {"social.achievement-unlocked.v1"},
    "community-projector.v1": {
        "community.direct-conversation-created.v1",
        "community.channel-member-joined.v1",
        "community.channel-member-left.v1",
    },
    "community-message-projector.v1": {"community.message-sent.v1"},
    "ranking-projector.v1": {"score.accepted.v1", "score.performance-calculated.v1"},
}

TEST_CONSUMER = "tests-projector.v1"
TEST_EVENT_TYPE = "tests.event.v1"


async def _complete_test_event(session: AsyncSession, event: OutboxEvent, partition_key: str) -> None:
    del session, event, partition_key


async def _fail_test_event(session: AsyncSession, event: OutboxEvent, partition_key: str) -> None:
    del session, event, partition_key
    raise RuntimeError("projector failed")


async def write_outbox_event(
    session: AsyncSession,
    *,
    aggregate_type: str,
    aggregate_id: str,
    event_type: str,
    schema_version: int,
    payload: dict[str, JsonValue],
    consumers: tuple[str, ...],
    partition_key: str,
) -> OutboxEvent:
    return await append_outbox_event(
        session,
        PendingEvent(
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            event_type=event_type,
            schema_version=schema_version,
            payload=payload,
            consumers=consumers,
            partition_key=partition_key,
        ),
    )


def test_outbox_task_registers_every_declared_consumer() -> None:
    assert set(DEFAULT_CONSUMER_CATALOG) == set(EXPECTED_CONSUMERS)
    for name, event_types in EXPECTED_CONSUMERS.items():
        assert DEFAULT_CONSUMER_CATALOG[name].event_types == event_types


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_outbox_dead_delivery_blocks_later_partition_events(postgres_database_url: str) -> None:
    del postgres_database_url
    db_engine = await infra_db.create_engine()
    session_factory = infra_db.create_session_factory(db_engine)
    store = SqlAlchemyOutboxDeliveryRelayStore(
        session_factory,
        batch_size=10,
        lease_seconds=30,
        max_attempts=1,
        max_retry_seconds=30,
    )
    processor = OutboxDeliveryProcessor(
        session_factory,
        ConsumerCatalog((ConsumerRegistration(TEST_CONSUMER, frozenset({TEST_EVENT_TYPE}), _fail_test_event),)),
        max_attempts=1,
        max_retry_seconds=30,
    )
    try:
        async with session_factory.begin() as session:
            first = await append_outbox_event(
                session,
                PendingEvent("test", "1", TEST_EVENT_TYPE, 1, {}, (TEST_CONSUMER,), "test:partition"),
            )
            second = await append_outbox_event(
                session,
                PendingEvent("test", "2", TEST_EVENT_TYPE, 1, {}, (TEST_CONSUMER,), "test:partition"),
            )

        claims = await store.claim("tests:dead-owner")
        assert [claim.event_id for claim in claims] == [first.id]
        with pytest.raises(RuntimeError, match="projector failed"):
            await processor.execute(claims[0])
        assert await store.claim("tests:next-owner") == ()

        async with session_factory() as session:
            first_delivery = await session.get(
                OutboxDelivery,
                {"event_id": first.id, "consumer": TEST_CONSUMER},
            )
            second_delivery = await session.get(
                OutboxDelivery,
                {"event_id": second.id, "consumer": TEST_CONSUMER},
            )
            assert first_delivery is not None and first_delivery.status is OutboxDeliveryStatus.DEAD
            assert second_delivery is not None and second_delivery.status is OutboxDeliveryStatus.PENDING
    finally:
        await db_engine.dispose()


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_outbox_stale_token_and_enqueue_failure_do_not_consume_attempt(
    postgres_database_url: str,
) -> None:
    del postgres_database_url
    db_engine = await infra_db.create_engine()
    session_factory = infra_db.create_session_factory(db_engine)
    store = SqlAlchemyOutboxDeliveryRelayStore(
        session_factory,
        batch_size=1,
        lease_seconds=30,
        max_attempts=3,
        max_retry_seconds=30,
    )
    processor = OutboxDeliveryProcessor(
        session_factory,
        ConsumerCatalog((ConsumerRegistration(TEST_CONSUMER, frozenset({TEST_EVENT_TYPE}), _complete_test_event),)),
        max_attempts=3,
        max_retry_seconds=30,
    )
    try:
        async with session_factory.begin() as session:
            event = await append_outbox_event(
                session,
                PendingEvent("test", "1", TEST_EVENT_TYPE, 1, {}, (TEST_CONSUMER,), "test:1"),
            )

        reference = (await store.claim("tests:enqueue-owner"))[0]
        await processor.execute(replace(reference, delivery_token=uuid.uuid4()))
        await store.mark_enqueue_failed(reference, "tests:enqueue-owner", RuntimeError("Redis unavailable"))

        async with session_factory() as session:
            delivery = await session.get(
                OutboxDelivery,
                {"event_id": event.id, "consumer": TEST_CONSUMER},
            )
            assert delivery is not None
            assert delivery.status is OutboxDeliveryStatus.PENDING
            assert delivery.attempt_count == 0
            assert delivery.enqueue_count == 1
            assert delivery.delivery_token is None
            assert delivery.last_error == "RuntimeError: Redis unavailable"
    finally:
        await db_engine.dispose()


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_outbox_consumers_project_idempotently_and_rollback_invalid_payload(
    postgres_database_url: str,
) -> None:
    del postgres_database_url
    db_engine = await infra_db.create_engine()
    session_factory = infra_db.create_session_factory(db_engine)
    relay_store = SqlAlchemyOutboxDeliveryRelayStore(
        session_factory,
        batch_size=settings.outbox_delivery_batch_size,
        lease_seconds=settings.outbox_delivery_lease_seconds,
        max_attempts=settings.outbox_delivery_max_attempts,
        max_retry_seconds=settings.outbox_delivery_max_retry_seconds,
    )
    processor = OutboxDeliveryProcessor(
        session_factory,
        DEFAULT_CONSUMER_CATALOG,
        max_attempts=settings.outbox_delivery_max_attempts,
        max_retry_seconds=settings.outbox_delivery_max_retry_seconds,
    )
    try:
        async with session_factory.begin() as session:
            session.add_all(
                (
                    Account(
                        id=ACTOR_ACCOUNT_ID,
                        type=AccountType.USER,
                        status=AccountStatus.ACTIVE,
                        country_code="JP",
                        registered_at=NOW,
                        activated_at=NOW,
                    ),
                    Account(
                        id=RECIPIENT_ACCOUNT_ID,
                        type=AccountType.USER,
                        status=AccountStatus.ACTIVE,
                        country_code="US",
                        registered_at=NOW,
                        activated_at=NOW,
                    ),
                )
            )
            await session.flush()
            beatmapset = Beatmapset(
                source_id=1,
                external_id=777,
                creator_account_id=ACTOR_ACCOUNT_ID,
                creator_name="Mapper",
                artist="Artist",
                title="Title",
                status=BeatmapStatus.RANKED,
                available=True,
            )
            achievement = AchievementDefinition(
                id=10,
                slug="test-achievement",
                evaluator_code="tests.always",
                evaluator_version=1,
                parameters={},
                active=True,
            )
            channel = Channel(kind=ChannelKind.PRIVATE, auto_join=False, message_length_limit=2000)
            session.add_all((beatmapset, achievement, channel))
            await session.flush()
            session.add_all(
                (
                    AchievementUnlock(
                        account_id=ACTOR_ACCOUNT_ID,
                        achievement_id=achievement.id,
                        definition_version=1,
                        score_id=None,
                        snapshot={},
                        created_at=NOW,
                    ),
                    DirectConversation(
                        channel_id=channel.id,
                        low_account_id=ACTOR_ACCOUNT_ID,
                        high_account_id=RECIPIENT_ACCOUNT_ID,
                    ),
                    NotificationPreference(
                        account_id=RECIPIENT_ACCOUNT_ID,
                        category="chat",
                        realtime_enabled=True,
                        email_enabled=True,
                        push_enabled=False,
                        digest_frequency="none",
                    ),
                )
            )
            message = Message(
                channel_id=channel.id,
                sender_account_id=ACTOR_ACCOUNT_ID,
                client_message_id=uuid.uuid7(),
                content="hello",
                is_action=False,
                created_at=NOW,
            )
            session.add(message)
            await session.flush()

            identity_session_id = uuid.uuid7()
            events = (
                await write_outbox_event(
                    session,
                    aggregate_type="account",
                    aggregate_id=str(ACTOR_ACCOUNT_ID),
                    event_type="account.registered.v1",
                    schema_version=1,
                    payload={
                        "account_id": ACTOR_ACCOUNT_ID,
                        "display_name": "Mapper",
                        "status": "active",
                        "registered_at": NOW.isoformat(),
                        "request_id": str(uuid.uuid7()),
                    },
                    consumers=("account-projector.v1",),
                    partition_key=f"account:{ACTOR_ACCOUNT_ID}",
                ),
                await write_outbox_event(
                    session,
                    aggregate_type="identity_session",
                    aggregate_id=str(identity_session_id),
                    event_type="identity.session-opened.v1",
                    schema_version=1,
                    payload={
                        "account_id": ACTOR_ACCOUNT_ID,
                        "session_id": str(identity_session_id),
                        "device_id": str(uuid.uuid7()),
                        "client_family": "stable",
                        "client_version": "b20260729",
                        "client_variant": None,
                        "opened_at": NOW.isoformat(),
                        "expires_at": NOW.replace(hour=23).isoformat(),
                        "request_id": str(uuid.uuid7()),
                    },
                    consumers=("identity-projector.v1",),
                    partition_key=f"account:{ACTOR_ACCOUNT_ID}",
                ),
                await write_outbox_event(
                    session,
                    aggregate_type="beatmapset",
                    aggregate_id=str(beatmapset.id),
                    event_type="content.beatmapset-synchronized.v1",
                    schema_version=1,
                    payload={
                        "beatmapset_id": beatmapset.id,
                        "external_beatmapset_id": 777,
                        "created_revision_count": 1,
                        "unchanged_revision_count": 0,
                        "removed_beatmap_count": 0,
                        "source_updated_at": NOW.isoformat(),
                    },
                    consumers=("content-projector.v1",),
                    partition_key=f"beatmapset:{beatmapset.id}",
                ),
                await write_outbox_event(
                    session,
                    aggregate_type="social_pair",
                    aggregate_id=f"{ACTOR_ACCOUNT_ID}:{RECIPIENT_ACCOUNT_ID}",
                    event_type="social.account-followed.v1",
                    schema_version=1,
                    payload={
                        "actor_account_id": ACTOR_ACCOUNT_ID,
                        "target_account_id": RECIPIENT_ACCOUNT_ID,
                        "mutual": False,
                        "followed_at": NOW.isoformat(),
                    },
                    consumers=("social-projector.v1",),
                    partition_key=f"social-pair:{ACTOR_ACCOUNT_ID}:{RECIPIENT_ACCOUNT_ID}",
                ),
                await write_outbox_event(
                    session,
                    aggregate_type="account",
                    aggregate_id=str(ACTOR_ACCOUNT_ID),
                    event_type="social.achievement-unlocked.v1",
                    schema_version=1,
                    payload={
                        "account_id": ACTOR_ACCOUNT_ID,
                        "achievement_id": 10,
                        "definition_version": 1,
                        "score_id": None,
                        "unlocked_at": NOW.isoformat(),
                    },
                    consumers=("achievement-projector.v1",),
                    partition_key=f"account:{ACTOR_ACCOUNT_ID}",
                ),
                await write_outbox_event(
                    session,
                    aggregate_type="channel",
                    aggregate_id=str(channel.id),
                    event_type="community.direct-conversation-created.v1",
                    schema_version=1,
                    payload={
                        "channel_id": channel.id,
                        "low_account_id": ACTOR_ACCOUNT_ID,
                        "high_account_id": RECIPIENT_ACCOUNT_ID,
                    },
                    consumers=("community-projector.v1",),
                    partition_key=f"channel:{channel.id}",
                ),
                await write_outbox_event(
                    session,
                    aggregate_type="channel",
                    aggregate_id=str(channel.id),
                    event_type="community.message-sent.v1",
                    schema_version=1,
                    payload={
                        "message_id": message.id,
                        "channel_id": channel.id,
                        "sender_account_id": ACTOR_ACCOUNT_ID,
                        "direct_recipient_account_id": RECIPIENT_ACCOUNT_ID,
                        "client_message_id": str(message.client_message_id),
                        "content": message.content,
                        "is_action": False,
                        "reply_to_id": None,
                        "created_at": NOW.isoformat(),
                    },
                    consumers=("community-message-projector.v1",),
                    partition_key=f"channel:{channel.id}",
                ),
            )
            event_consumers = (
                "account-projector.v1",
                "identity-projector.v1",
                "content-projector.v1",
                "social-projector.v1",
                "achievement-projector.v1",
                "community-projector.v1",
                "community-message-projector.v1",
            )

        claims = await relay_store.claim("tests:consumer-owner")
        assert {reference.event_id for reference in claims} == {event.id for event in events}
        for reference in claims:
            await processor.execute(reference)

        async with session_factory.begin() as session:
            for event, consumer in zip(events, event_consumers, strict=True):
                delivery = await session.get(
                    OutboxDelivery,
                    {"event_id": event.id, "consumer": consumer},
                )
                assert delivery is not None and delivery.completed_at is not None
                registration = DEFAULT_CONSUMER_CATALOG[delivery.consumer]
                await registration.handler(session, event, delivery.partition_key)

        async with session_factory() as session:
            assert await session.scalar(select(func.count()).select_from(ActivityEvent)) == 5
            assert await session.scalar(select(func.count()).select_from(Notification)) == 3
            assert await session.scalar(select(func.count()).select_from(NotificationRecipient)) == 3
            assert await session.scalar(select(func.count()).select_from(NotificationDispatch)) == 1
            assert await session.scalar(select(func.count()).select_from(ProjectionCheckpoint)) == 7
            sync_projection = await session.get(BeatmapsetSyncProjection, beatmapset.id)
            channel_projection = await session.get(ChannelReadProjection, channel.id)
            assert sync_projection is not None
            assert sync_projection.source_event_id == events[2].id
            assert channel_projection is not None
            assert channel_projection.latest_message_id == message.id
            assert channel_projection.active_member_count == 2

        async with session_factory.begin() as session:
            invalid = await write_outbox_event(
                session,
                aggregate_type="account",
                aggregate_id=str(ACTOR_ACCOUNT_ID),
                event_type="account.registered.v1",
                schema_version=1,
                payload={"account_id": "invalid"},
                consumers=("account-projector.v1",),
                partition_key=f"account:{ACTOR_ACCOUNT_ID}",
            )
        invalid_claim = await relay_store.claim("tests:invalid-owner")
        assert [reference.event_id for reference in invalid_claim] == [invalid.id]
        with pytest.raises(RuntimeError, match="account_id"):
            await processor.execute(invalid_claim[0])
        async with session_factory() as session:
            delivery = await session.get(
                OutboxDelivery,
                {"event_id": invalid.id, "consumer": "account-projector.v1"},
            )
            assert delivery is not None
            assert delivery.attempt_count == 1
            assert delivery.completed_at is None
            assert (
                await session.scalar(
                    select(func.count()).select_from(ActivityEvent).where(ActivityEvent.source_event_id == invalid.id)
                )
                == 0
            )
    finally:
        await db_engine.dispose()
