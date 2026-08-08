"""Compose the Taskiq worker process and durable relay loops."""

import asyncio
import os
import socket
import uuid
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from time import monotonic_ns
from typing import Any, NotRequired, Protocol, TypedDict, cast

import httpx
from sqlalchemy.ext.asyncio import AsyncEngine
from taskiq import TaskiqEvents, TaskiqState

from perfcho.infra import logging
from perfcho.infra.db import engine as infra_db
from perfcho.infra.db.maintenance import RankSnapshotMaintenance
from perfcho.infra.db.projectors.catalog import build_consumer_catalog
from perfcho.infra.db.projectors.performance import PerformanceProjector
from perfcho.infra.db.relays.outbox_delivery import (
    OutboxDeliveryProcessor,
    OutboxDeliveryReference,
    SqlAlchemyOutboxDeliveryRelayStore,
)
from perfcho.infra.settings import settings
from perfcho.infra.storage import S3ObjectStorage
from perfcho.infra.upstream.calculator import HttpPerformanceCalculator

logging.init_logger("worker")

from perfcho.infra.taskiq import broker  # noqa: E402
from perfcho.tasks.outbox_delivery import dispatch_outbox_delivery  # noqa: E402


class _RelayStore[Reference](Protocol):
    """Persist claims and enqueue outcomes for one work family."""

    async def claim(self, owner: str) -> Sequence[Reference]: ...

    async def record_enqueue_outcomes(
        self,
        outcomes: Sequence[tuple[Reference, str | Exception]],
        owner: str,
    ) -> None: ...

    async def release(self, references: Sequence[Reference], owner: str) -> None: ...


type _Enqueue[Reference] = Callable[[Reference], Awaitable[str]]


@dataclass(frozen=True, slots=True)
class _WorkerResources:
    """Own process resources that require ordered shutdown."""

    db_engine: AsyncEngine
    http_client: httpx.AsyncClient
    relay_tasks: tuple[asyncio.Task[None], ...]
    started_ns: int


@dataclass(frozen=True, slots=True)
class _RelayBatchResult:
    """Summarize one committed relay batch for bounded aggregate logging."""

    claimed: int
    enqueued: int
    enqueue_failed: int


class _ReferenceLogFields(TypedDict):
    """Constrain generic relay context to non-fencing durable identifiers."""

    event_id: NotRequired[str]
    consumer: NotRequired[str]


@broker.on_event(TaskiqEvents.WORKER_STARTUP)
async def worker_startup(state: TaskiqState) -> None:
    """Create all worker-owned resources and independent relay loops."""
    started_ns = monotonic_ns()
    state.worker_started_ns = started_ns
    state.worker_lifecycle_stopped = False
    logging.log_event("INFO", "runtime.worker.starting")
    db_engine: AsyncEngine | None = None
    http_client: httpx.AsyncClient | None = None
    relay_tasks: tuple[asyncio.Task[None], ...] = ()
    try:
        db_engine = await infra_db.create_engine()
        session_factory = infra_db.create_session_factory(db_engine)
        http_client = httpx.AsyncClient(timeout=settings.performance_http_timeout_seconds)
        outbox_store = SqlAlchemyOutboxDeliveryRelayStore(
            session_factory,
            batch_size=settings.outbox_delivery_batch_size,
            lease_seconds=settings.outbox_delivery_lease_seconds,
            max_attempts=settings.outbox_delivery_max_attempts,
            max_retry_seconds=settings.outbox_delivery_max_retry_seconds,
        )
        consumer_catalog = build_consumer_catalog(
            PerformanceProjector(
                HttpPerformanceCalculator(http_client, settings.performance_calculator_urls),
                S3ObjectStorage.from_settings(settings),
                beatmap_url_expiry_seconds=settings.performance_beatmap_url_expiry_seconds,
            )
        )
        unknown_consumers = await outbox_store.unknown_consumers(consumer_catalog)
        if unknown_consumers:
            names = ", ".join(sorted(unknown_consumers))
            raise RuntimeError(f"Unregistered outbox consumers have unfinished deliveries: {names}")

        owner = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4()}"
        state.outbox_delivery_processor = OutboxDeliveryProcessor(
            session_factory,
            consumer_catalog,
            max_attempts=settings.outbox_delivery_max_attempts,
            max_retry_seconds=settings.outbox_delivery_max_retry_seconds,
        )
        outbox_relay_task = asyncio.create_task(
            _run_relay_loop(
                "outbox-delivery",
                outbox_store,
                owner,
                _enqueue_outbox,
                poll_interval=settings.durable_relay_poll_interval_seconds,
                enqueue_concurrency=settings.durable_relay_enqueue_concurrency,
            ),
            name="perfcho-outbox-relay",
        )
        relay_tasks = (outbox_relay_task,)
        rank_snapshot_task = asyncio.create_task(
            _run_rank_snapshot_loop(
                RankSnapshotMaintenance(session_factory),
                poll_interval=settings.rank_snapshot_poll_interval_seconds,
            ),
            name="perfcho-rank-snapshot-maintenance",
        )
        relay_tasks = (outbox_relay_task, rank_snapshot_task)
        state.worker_resources = _WorkerResources(
            db_engine=db_engine,
            http_client=http_client,
            relay_tasks=relay_tasks,
            started_ns=started_ns,
        )
        logging.log_event("INFO", "runtime.worker.ready", duration_ms=logging.duration_ms(started_ns))
    except BaseException as error:
        logging.log_event(
            "ERROR",
            "runtime.worker.startup_failed",
            exception=error,
            error_type=type(error).__name__,
            duration_ms=logging.duration_ms(started_ns),
        )
        logging.log_event("INFO", "runtime.worker.stopping")
        await _cleanup_worker_resources(relay_tasks, http_client, db_engine)
        state.worker_lifecycle_stopped = True
        logging.log_event("INFO", "runtime.worker.stopped", duration_ms=logging.duration_ms(started_ns))
        raise


@broker.on_event(TaskiqEvents.WORKER_SHUTDOWN)
async def worker_shutdown(state: TaskiqState) -> None:
    """Stop relays before closing shared HTTP and database resources."""
    if bool(getattr(state, "worker_lifecycle_stopped", False)):
        return
    resources = cast(_WorkerResources | None, getattr(state, "worker_resources", None))
    started_ns = resources.started_ns if resources is not None else monotonic_ns()
    logging.log_event("INFO", "runtime.worker.stopping")
    if resources is not None:
        await _cleanup_worker_resources(
            resources.relay_tasks,
            resources.http_client,
            resources.db_engine,
        )
    state.worker_lifecycle_stopped = True
    logging.log_event("INFO", "runtime.worker.stopped", duration_ms=logging.duration_ms(started_ns))


async def _cleanup_worker_resources(
    relay_tasks: tuple[asyncio.Task[None], ...],
    http_client: httpx.AsyncClient | None,
    db_engine: AsyncEngine | None,
) -> None:
    """Close every initialized worker resource even when an earlier close fails."""
    for task in relay_tasks:
        task.cancel()
    if relay_tasks:
        try:
            outcomes = await asyncio.gather(*relay_tasks, return_exceptions=True)
        except Exception as error:
            logging.log_event(
                "ERROR",
                "runtime.worker.resource_close_failed",
                exception=error,
                resource="relay",
                error_type=type(error).__name__,
            )
        else:
            for task, outcome in zip(relay_tasks, outcomes, strict=True):
                if isinstance(outcome, BaseException) and not isinstance(outcome, asyncio.CancelledError):
                    logging.log_event(
                        "ERROR",
                        "runtime.worker.resource_close_failed",
                        exception=outcome,
                        resource="relay",
                        relay_task=task.get_name(),
                        error_type=type(outcome).__name__,
                    )
    if http_client is not None:
        try:
            await http_client.aclose()
        except Exception as error:
            logging.log_event(
                "ERROR",
                "runtime.worker.resource_close_failed",
                exception=error,
                resource="http",
                error_type=type(error).__name__,
            )
    if db_engine is not None:
        try:
            await db_engine.dispose()
        except Exception as error:
            logging.log_event(
                "ERROR",
                "runtime.worker.resource_close_failed",
                exception=error,
                resource="postgres",
                error_type=type(error).__name__,
            )


async def _relay_once[Reference](
    store: _RelayStore[Reference],
    owner: str,
    enqueue: _Enqueue[Reference],
    *,
    relay: str = "unknown",
    enqueue_concurrency: int = 1,
) -> _RelayBatchResult:
    """Claim and publish one batch while preserving uncertain enqueue leases."""
    if enqueue_concurrency < 1:
        raise ValueError("enqueue_concurrency must be positive")
    references = tuple(await store.claim(owner))
    enqueued = 0
    enqueue_failed = 0
    outcomes: list[tuple[Reference, str | Exception]] = []

    async def enqueue_one(reference: Reference) -> str | Exception:
        try:
            return await enqueue(reference)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            return error

    async def preserve_cancelled_tail(unattempted: Sequence[Reference]) -> None:
        if outcomes:
            try:
                await store.record_enqueue_outcomes(outcomes, owner)
            except Exception as error:
                logging.log_event(
                    "ERROR",
                    "runtime.worker.relay.outcome_persist_failed",
                    exception=error,
                    relay=relay,
                    error_type=type(error).__name__,
                )
        if unattempted:
            try:
                await store.release(unattempted, owner)
            except Exception as error:
                logging.log_event(
                    "ERROR",
                    "runtime.worker.relay.release_failed",
                    exception=error,
                    relay=relay,
                    error_type=type(error).__name__,
                    release_count=len(unattempted),
                )

    for offset in range(0, len(references), enqueue_concurrency):
        chunk = references[offset : offset + enqueue_concurrency]
        tasks = tuple(asyncio.create_task(enqueue_one(reference)) for reference in chunk)
        try:
            chunk_results = await asyncio.gather(*tasks, return_exceptions=True)
        except asyncio.CancelledError:
            for task in tasks:
                task.cancel()
            settled = await asyncio.gather(*tasks, return_exceptions=True)
            for reference, outcome in zip(chunk, settled, strict=True):
                if isinstance(outcome, asyncio.CancelledError):
                    continue
                if isinstance(outcome, BaseException) and not isinstance(outcome, Exception):
                    continue
                outcomes.append((reference, cast(str | Exception, outcome)))
            unattempted = references[offset + len(chunk) :]
            await preserve_cancelled_tail(unattempted)
            raise
        cancelled = False
        for reference, outcome in zip(chunk, chunk_results, strict=True):
            if isinstance(outcome, asyncio.CancelledError):
                cancelled = True
                continue
            if isinstance(outcome, Exception):
                outcomes.append((reference, outcome))
                enqueue_failed += 1
                logging.log_event(
                    "WARNING",
                    "runtime.worker.relay.enqueue_failed",
                    exception=outcome,
                    relay=relay,
                    error_type=type(outcome).__name__,
                    **_reference_fields(reference),
                )
            elif isinstance(outcome, BaseException):
                raise outcome
            else:
                outcomes.append((reference, outcome))
                enqueued += 1
        if cancelled:
            await preserve_cancelled_tail(references[offset + len(chunk) :])
            raise asyncio.CancelledError
    if outcomes:
        await store.record_enqueue_outcomes(outcomes, owner)
    return _RelayBatchResult(len(references), enqueued, enqueue_failed)


async def _run_relay_loop[Reference](
    name: str,
    store: _RelayStore[Reference],
    owner: str,
    enqueue: _Enqueue[Reference],
    *,
    poll_interval: float,
    enqueue_concurrency: int = 1,
) -> None:
    """Run one independently supervised work-family relay."""
    if poll_interval <= 0:
        raise ValueError("poll_interval must be positive")
    started_ns = monotonic_ns()
    logging.log_event("INFO", "runtime.worker.relay.started", relay=name)
    try:
        while True:
            iteration_started_ns = monotonic_ns()
            try:
                result = await _relay_once(
                    store,
                    owner,
                    enqueue,
                    relay=name,
                    enqueue_concurrency=enqueue_concurrency,
                )
            except asyncio.CancelledError:
                raise
            except Exception as error:
                if logging.rate_limit(f"worker-relay:{name}:iteration-failed"):
                    logging.log_event(
                        "ERROR",
                        "runtime.worker.relay.iteration_failed",
                        exception=error,
                        relay=name,
                        error_type=type(error).__name__,
                    )
                result = _RelayBatchResult(0, 0, 0)
            if result.claimed == 0:
                await asyncio.sleep(poll_interval)
            else:
                logging.log_event(
                    "DEBUG",
                    "runtime.worker.relay.batch",
                    relay=name,
                    claimed=result.claimed,
                    enqueued=result.enqueued,
                    enqueue_failed=result.enqueue_failed,
                    duration_ms=logging.duration_ms(iteration_started_ns),
                )
    finally:
        logging.log_event(
            "INFO",
            "runtime.worker.relay.stopped",
            relay=name,
            duration_ms=logging.duration_ms(started_ns),
        )


async def _run_rank_snapshot_loop(
    maintenance: RankSnapshotMaintenance,
    *,
    poll_interval: float,
) -> None:
    """Poll durable state and materialize one complete rank snapshot per day."""
    started_ns = monotonic_ns()
    logging.log_event("INFO", "runtime.worker.maintenance.started", maintenance="rank-snapshot")
    try:
        while True:
            iteration_started_ns = monotonic_ns()
            try:
                completed = await maintenance.run_due()
            except asyncio.CancelledError:
                raise
            except Exception as error:
                if logging.rate_limit("worker-maintenance:rank-snapshot:iteration-failed"):
                    logging.log_event(
                        "ERROR",
                        "runtime.worker.maintenance.iteration_failed",
                        exception=error,
                        maintenance="rank-snapshot",
                        error_type=type(error).__name__,
                    )
            else:
                if completed:
                    logging.log_event(
                        "INFO",
                        "runtime.worker.maintenance.completed",
                        maintenance="rank-snapshot",
                        duration_ms=logging.duration_ms(iteration_started_ns),
                    )
            await asyncio.sleep(poll_interval)
    finally:
        logging.log_event(
            "INFO",
            "runtime.worker.maintenance.stopped",
            maintenance="rank-snapshot",
            duration_ms=logging.duration_ms(started_ns),
        )


def _reference_fields(reference: object) -> _ReferenceLogFields:
    """Return only non-fencing identifiers approved for relay logs."""
    if isinstance(reference, OutboxDeliveryReference):
        return {"event_id": str(reference.event_id), "consumer": reference.consumer}
    return {}


async def _enqueue_outbox(reference: OutboxDeliveryReference) -> str:
    task = await cast(Any, dispatch_outbox_delivery).kiq(
        str(reference.event_id),
        reference.consumer,
        str(reference.delivery_token),
    )
    return str(task.task_id)
