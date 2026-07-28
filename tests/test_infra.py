import uuid
from datetime import UTC, datetime
from unittest.mock import MagicMock
from urllib.parse import urlparse

import pytest
from sqlalchemy.ext.asyncio import AsyncSession
from taskiq_redis import RedisStreamBroker

from perfcho.infra.db.models.events import OutboxDelivery
from perfcho.infra.outbox import write_outbox_event
from perfcho.infra.settings import settings
from perfcho.infra.taskiq import broker
from perfcho.tasks.outbox import dispatch_outbox_delivery


def test_redis_state_and_taskiq_use_separate_logical_databases() -> None:
    assert urlparse(settings.redis_state_url).path == "/0"
    assert urlparse(settings.taskiq_broker_url).path == "/1"
    assert settings.redis_state_prefix


def test_taskiq_uses_stream_broker_without_result_storage() -> None:
    assert isinstance(broker, RedisStreamBroker)
    assert broker.queue_name == settings.taskiq_queue_name
    assert broker.consumer_group_name == settings.taskiq_consumer_group
    assert broker.consumer_id == "0-0"
    assert broker.maxlen == settings.taskiq_stream_max_length
    assert dispatch_outbox_delivery.task_name in broker.get_all_tasks()


@pytest.mark.asyncio
async def test_outbox_writer_creates_explicit_consumer_delivery() -> None:
    event_type = "tests.runtime-event.v1"
    consumer_name = "tests.runtime-consumer.v1"

    session = MagicMock(spec=AsyncSession)
    event_id = uuid.uuid7()

    async def assign_event_id() -> None:
        event = session.add.call_args_list[0].args[0]
        event.id = event_id

    session.flush.side_effect = assign_event_id
    available_at = datetime.now(UTC)
    event = await write_outbox_event(
        session,
        aggregate_type="test",
        aggregate_id="1",
        event_type=event_type,
        schema_version=1,
        payload={"value": 1},
        consumers=(consumer_name,),
        available_at=available_at,
        partition_key="test:1",
    )

    added = [call.args[0] for call in session.add.call_args_list]
    assert event is added[0]
    assert isinstance(added[1], OutboxDelivery)
    assert added[1].event_id == event_id
    assert added[1].consumer == consumer_name
    assert added[1].partition_key == "test:1"
    session.execute.assert_awaited_once()
    session.flush.assert_awaited_once()


@pytest.mark.asyncio
async def test_outbox_writer_rejects_events_without_consumers() -> None:
    session = MagicMock(spec=AsyncSession)

    with pytest.raises(ValueError, match="at least one consumer"):
        await write_outbox_event(
            session,
            aggregate_type="test",
            aggregate_id="1",
            event_type="tests.unrouted-event.v1",
            schema_version=1,
            payload={},
            consumers=(),
        )

    session.add.assert_not_called()
    session.flush.assert_not_awaited()
