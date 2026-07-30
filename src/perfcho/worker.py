"""Compose the Taskiq worker process and durable relay loops."""

import asyncio
import os
import socket
import uuid
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any, Protocol, cast

import httpx
from loguru import logger
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from taskiq import TaskiqEvents, TaskiqState

from perfcho.infra.db import engine as infra_db
from perfcho.infra.db.projectors.catalog import DEFAULT_CONSUMER_CATALOG
from perfcho.infra.db.relays.outbox_delivery import (
    OutboxDeliveryProcessor,
    OutboxDeliveryReference,
    SqlAlchemyOutboxDeliveryRelayStore,
)
from perfcho.infra.db.relays.performance_job import (
    PerformanceJobReference,
    SqlAlchemyPerformanceJobRelayStore,
)
from perfcho.infra.db.repositories.outbox import SqlAlchemyOutboxWriter
from perfcho.infra.db.repositories.performance.job import SqlAlchemyPerformanceJobRepository
from perfcho.infra.db.uow import SqlAlchemyUnitOfWorkFactory
from perfcho.infra.settings import settings
from perfcho.infra.storage import S3ObjectStorage
from perfcho.infra.taskiq import broker
from perfcho.infra.upstream.calculator import HttpPerformanceCalculator
from perfcho.modules.performance.services import PerformanceCalculationService
from perfcho.tasks.outbox_delivery import dispatch_outbox_delivery
from perfcho.tasks.performance_calculation import calculate_performance


class _RelayStore[Reference](Protocol):
    """Persist claims and enqueue outcomes for one work family."""

    async def claim(self, owner: str) -> Sequence[Reference]: ...

    async def mark_enqueued(self, reference: Reference, owner: str, broker_task_id: str) -> None: ...

    async def mark_enqueue_failed(self, reference: Reference, owner: str, error: Exception) -> None: ...

    async def release(self, reference: Reference, owner: str) -> None: ...


type _Enqueue[Reference] = Callable[[Reference], Awaitable[str]]


@dataclass(frozen=True, slots=True)
class _WorkerResources:
    """Own process resources that require ordered shutdown."""

    db_engine: AsyncEngine
    http_client: httpx.AsyncClient
    relay_tasks: tuple[asyncio.Task[None], ...]


@broker.on_event(TaskiqEvents.WORKER_STARTUP)
async def worker_startup(state: TaskiqState) -> None:
    """Create all worker-owned resources and independent relay loops."""
    db_engine = await infra_db.create_engine()
    session_factory = infra_db.create_session_factory(db_engine)
    http_client = httpx.AsyncClient(timeout=settings.performance_http_timeout_seconds)
    try:
        outbox_store = SqlAlchemyOutboxDeliveryRelayStore(
            session_factory,
            batch_size=settings.outbox_delivery_batch_size,
            lease_seconds=settings.outbox_delivery_lease_seconds,
            max_attempts=settings.outbox_delivery_max_attempts,
            max_retry_seconds=settings.outbox_delivery_max_retry_seconds,
        )
        unknown_consumers = await outbox_store.unknown_consumers(DEFAULT_CONSUMER_CATALOG)
        if unknown_consumers:
            names = ", ".join(sorted(unknown_consumers))
            raise RuntimeError(f"Unregistered outbox consumers have unfinished deliveries: {names}")

        performance_store = SqlAlchemyPerformanceJobRelayStore(
            session_factory,
            batch_size=settings.performance_calculation_batch_size,
            lease_seconds=settings.performance_calculation_lease_seconds,
            max_attempts=settings.performance_calculation_max_attempts,
            max_retry_seconds=settings.performance_calculation_max_retry_seconds,
        )
        performance_service = PerformanceCalculationService(
            SqlAlchemyUnitOfWorkFactory(session_factory),
            lambda session: SqlAlchemyPerformanceJobRepository(
                cast(AsyncSession, session),
                execution_lease_seconds=settings.performance_calculation_lease_seconds,
            ),
            lambda session: SqlAlchemyOutboxWriter(cast(AsyncSession, session)),
            HttpPerformanceCalculator(http_client, settings.performance_calculator_urls),
            S3ObjectStorage.from_settings(settings),
            max_attempts=settings.performance_calculation_max_attempts,
            beatmap_url_expiry_seconds=settings.performance_beatmap_url_expiry_seconds,
            max_retry_seconds=settings.performance_calculation_max_retry_seconds,
        )
        owner = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4()}"
        state.outbox_delivery_processor = OutboxDeliveryProcessor(
            session_factory,
            DEFAULT_CONSUMER_CATALOG,
            max_attempts=settings.outbox_delivery_max_attempts,
            max_retry_seconds=settings.outbox_delivery_max_retry_seconds,
        )
        state.performance_calculation_service = performance_service
        relay_tasks = (
            asyncio.create_task(
                _run_relay_loop(
                    "outbox-delivery",
                    outbox_store,
                    owner,
                    _enqueue_outbox,
                    poll_interval=settings.durable_relay_poll_interval_seconds,
                ),
                name="perfcho-outbox-relay",
            ),
            asyncio.create_task(
                _run_relay_loop(
                    "performance-job",
                    performance_store,
                    owner,
                    _enqueue_performance,
                    poll_interval=settings.durable_relay_poll_interval_seconds,
                ),
                name="perfcho-performance-relay",
            ),
        )
        state.worker_resources = _WorkerResources(
            db_engine=db_engine,
            http_client=http_client,
            relay_tasks=relay_tasks,
        )
    except BaseException:
        await http_client.aclose()
        await db_engine.dispose()
        raise


@broker.on_event(TaskiqEvents.WORKER_SHUTDOWN)
async def worker_shutdown(state: TaskiqState) -> None:
    """Stop relays before closing shared HTTP and database resources."""
    resources = cast(_WorkerResources | None, getattr(state, "worker_resources", None))
    if resources is None:
        return
    for task in resources.relay_tasks:
        task.cancel()
    if resources.relay_tasks:
        await asyncio.gather(*resources.relay_tasks, return_exceptions=True)
    await resources.http_client.aclose()
    await resources.db_engine.dispose()


async def _relay_once[Reference](
    store: _RelayStore[Reference],
    owner: str,
    enqueue: _Enqueue[Reference],
) -> int:
    """Claim and publish one batch while preserving uncertain enqueue leases."""
    references = tuple(await store.claim(owner))
    for index, reference in enumerate(references):
        try:
            broker_task_id = await enqueue(reference)
        except asyncio.CancelledError:
            for unattempted in references[index + 1 :]:
                await store.release(unattempted, owner)
            raise
        except Exception as error:
            await store.mark_enqueue_failed(reference, owner, error)
        else:
            await store.mark_enqueued(reference, owner, broker_task_id)
    return len(references)


async def _run_relay_loop[Reference](
    name: str,
    store: _RelayStore[Reference],
    owner: str,
    enqueue: _Enqueue[Reference],
    *,
    poll_interval: float,
) -> None:
    """Run one independently supervised work-family relay."""
    if poll_interval <= 0:
        raise ValueError("poll_interval must be positive")
    while True:
        try:
            claimed = await _relay_once(store, owner, enqueue)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("{} relay iteration failed", name)
            claimed = 0
        if claimed == 0:
            await asyncio.sleep(poll_interval)


async def _enqueue_outbox(reference: OutboxDeliveryReference) -> str:
    task = await cast(Any, dispatch_outbox_delivery).kiq(
        str(reference.event_id),
        reference.consumer,
        str(reference.delivery_token),
    )
    return str(task.task_id)


async def _enqueue_performance(reference: PerformanceJobReference) -> str:
    task = await cast(Any, calculate_performance).kiq(
        str(reference.job_id),
        str(reference.lease_token),
    )
    return str(task.task_id)
