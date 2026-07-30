import uuid
from datetime import UTC, datetime
from unittest.mock import MagicMock
from urllib.parse import urlparse

import pytest
from pydantic import ValidationError
from sqlalchemy import inspect
from sqlalchemy.ext.asyncio import AsyncSession
from taskiq import TaskiqEvents
from taskiq_redis import RedisStreamBroker

from perfcho.infra.db import DbBase
from perfcho.infra.db import engine as infra_db
from perfcho.infra.db.models.events import OutboxDelivery
from perfcho.infra.db.repositories.outbox import append_outbox_event
from perfcho.infra.settings import Settings, settings
from perfcho.modules.common.models import PendingEvent
from perfcho.tasks.outbox_delivery import dispatch_outbox_delivery
from perfcho.tasks.performance_calculation import calculate_performance
from perfcho.worker import broker, worker_shutdown, worker_startup


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
    assert calculate_performance.task_name in broker.get_all_tasks()
    assert broker.event_handlers[TaskiqEvents.WORKER_STARTUP] == [worker_startup]
    assert broker.event_handlers[TaskiqEvents.WORKER_SHUTDOWN] == [worker_shutdown]


def test_settings_reject_performance_timing_shorter_than_http_window() -> None:
    with pytest.raises(ValidationError, match="lease must exceed"):
        Settings(
            performance_http_timeout_seconds=60,
            performance_calculation_lease_seconds=89,
        )
    with pytest.raises(ValidationError, match="URL expiry must exceed"):
        Settings(
            performance_http_timeout_seconds=60,
            performance_beatmap_url_expiry_seconds=89,
        )


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_engine_creates_all_mapped_tables(postgres_database_url: str) -> None:
    expected_tables = {(table.schema, table.name) for table in DbBase.metadata.sorted_tables}
    assert expected_tables

    for _ in range(2):
        db_engine = await infra_db.create_engine()
        try:
            async with db_engine.connect() as connection:
                existing_tables = await connection.run_sync(
                    lambda sync_connection: {
                        (schema, table_name)
                        for schema in {table_schema for table_schema, _ in expected_tables}
                        for table_name in inspect(sync_connection).get_table_names(schema=schema)
                    }
                )
        finally:
            await db_engine.dispose()

        assert expected_tables <= existing_tables


@pytest.mark.asyncio
async def test_outbox_writer_creates_explicit_consumer_delivery() -> None:
    event_type = "tests.runtime-event.v1"
    consumer_name = "tests.runtime-consumer.v1"

    session = MagicMock(spec=AsyncSession)
    event_id = uuid.uuid7()

    async def assign_event_id() -> None:
        event = session.add.call_args_list[0].args[0]
        event.id = event_id
        event.position = 1

    session.flush.side_effect = assign_event_id
    available_at = datetime.now(UTC)
    session.scalar.return_value = available_at
    event = await append_outbox_event(
        session,
        PendingEvent(
            aggregate_type="test",
            aggregate_id="1",
            event_type=event_type,
            schema_version=1,
            payload={"value": 1},
            consumers=(consumer_name,),
            partition_key="test:1",
        ),
    )

    added = [call.args[0] for call in session.add.call_args_list]
    assert event is added[0]
    assert isinstance(added[1], OutboxDelivery)
    assert added[1].event_id == event_id
    assert added[1].consumer == consumer_name
    assert added[1].partition_key == "test:1"
    assert added[1].available_at == available_at
    assert added[1].source_position == 1
    session.execute.assert_awaited_once()
    session.flush.assert_awaited_once()


@pytest.mark.asyncio
async def test_outbox_writer_rejects_events_without_consumers() -> None:
    session = MagicMock(spec=AsyncSession)

    with pytest.raises(ValueError, match="non-empty and unique"):
        PendingEvent(
            aggregate_type="test",
            aggregate_id="1",
            event_type="tests.unrouted-event.v1",
            schema_version=1,
            payload={},
            consumers=(),
            partition_key="test:1",
        )

    session.add.assert_not_called()
    session.flush.assert_not_awaited()
