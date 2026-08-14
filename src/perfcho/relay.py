"""Run the durable outbox relay as an independent process."""

import asyncio
import contextlib
import os
import socket
import uuid
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from time import monotonic_ns
from typing import Any, NotRequired, Protocol, TypedDict, cast

import asyncpg  # type: ignore[import-untyped]
from sqlalchemy.engine import make_url

from perfcho.infra import logging
from perfcho.infra.db import engine as infra_db
from perfcho.infra.db.repositories.outbox import OUTBOX_NOTIFY_CHANNEL
from perfcho.infra.db.repositories.outbox_delivery import (
    OutboxDeliveryReference,
    SqlAlchemyOutboxDeliveryRepository,
)
from perfcho.infra.settings import settings
from perfcho.infra.taskiq import broker
from perfcho.worker import dispatch_outbox_delivery

logging.init_logger("relay")


class RelayStore[Reference](Protocol):
    """Persist claims and enqueue outcomes for one durable work family."""

    async def claim(self, owner: str) -> Sequence[Reference]:
        """Claim a bounded batch of due references."""
        ...

    async def record_enqueue_outcomes(
        self,
        outcomes: Sequence[tuple[Reference, str | Exception]],
        owner: str,
    ) -> None:
        """Persist successful or failed enqueue outcomes."""
        ...

    async def release(self, references: Sequence[Reference], owner: str) -> None:
        """Release claims that were not attempted before cancellation."""
        ...


type Enqueue[Reference] = Callable[[Reference], Awaitable[str]]


@dataclass(frozen=True, slots=True)
class RelayBatchResult:
    """Summarize one committed relay batch for bounded aggregate logging."""

    claimed: int
    enqueued: int
    enqueue_failed: int


class ReferenceLogFields(TypedDict):
    """Constrain generic relay context to non-fencing durable identifiers."""

    event_id: NotRequired[str]
    consumer: NotRequired[str]
    trace_id: NotRequired[str]


async def run_relay() -> None:
    """Create relay resources and supervise notification and delivery loops."""
    db_engine = await infra_db.create_engine(settings)
    session_factory = infra_db.create_session_factory(db_engine)
    owner = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4()}"
    wakeup = asyncio.Event()
    wakeup.set()
    notify_task: asyncio.Task[None] | None = None
    broker_started = False
    try:
        await broker.startup()
        broker_started = True
        repository = SqlAlchemyOutboxDeliveryRepository(
            session_factory,
            batch_size=settings.outbox_delivery_batch_size,
            lease_seconds=settings.outbox_delivery_lease_seconds,
            max_attempts=settings.outbox_delivery_max_attempts,
            max_retry_seconds=settings.outbox_delivery_max_retry_seconds,
        )
        notify_task = asyncio.create_task(_listen_for_outbox(wakeup), name="perfcho-outbox-notify-listener")
        await _run_relay_loop(
            "outbox-delivery",
            repository,
            owner,
            _enqueue_outbox,
            wakeup=wakeup,
            poll_interval=settings.durable_relay_poll_interval_seconds,
            debounce_seconds=settings.durable_relay_debounce_seconds,
            enqueue_concurrency=settings.durable_relay_enqueue_concurrency,
        )
    finally:
        if notify_task is not None:
            notify_task.cancel()
            await asyncio.gather(notify_task, return_exceptions=True)
        if broker_started:
            await broker.shutdown()
        await db_engine.dispose()


async def _listen_for_outbox(wakeup: asyncio.Event) -> None:
    """Listen for transactional outbox hints and reconnect after disconnects."""
    database_url = make_url(settings.database_url).set(drivername="postgresql")
    dsn = database_url.render_as_string(hide_password=False)
    while True:
        connection: Any = None
        terminated = asyncio.Event()

        def on_notify(*_: object) -> None:
            wakeup.set()

        def on_termination(*_: object, event: asyncio.Event = terminated) -> None:
            event.set()

        try:
            connection = await asyncpg.connect(dsn)
            await connection.add_listener(OUTBOX_NOTIFY_CHANNEL, on_notify)
            connection.add_termination_listener(on_termination)
            logging.log_event("INFO", "runtime.relay.notify.connected", channel=OUTBOX_NOTIFY_CHANNEL)
            await terminated.wait()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            logging.log_event(
                "ERROR",
                "runtime.relay.notify.failed",
                exception=error,
                error_type=type(error).__name__,
            )
        finally:
            if connection is not None and not connection.is_closed():
                with contextlib.suppress(Exception):
                    await connection.remove_listener(OUTBOX_NOTIFY_CHANNEL, on_notify)
                with contextlib.suppress(Exception):
                    await connection.close()
        await asyncio.sleep(1)


async def _relay_once[Reference](
    store: RelayStore[Reference],
    owner: str,
    enqueue: Enqueue[Reference],
    *,
    relay: str = "unknown",
    enqueue_concurrency: int = 1,
) -> RelayBatchResult:
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
                    "runtime.relay.outcome_persist_failed",
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
                    "runtime.relay.release_failed",
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
            await preserve_cancelled_tail(references[offset + len(chunk) :])
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
                    "runtime.relay.enqueue_failed",
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
    return RelayBatchResult(len(references), enqueued, enqueue_failed)


async def _run_relay_loop[Reference](
    name: str,
    store: RelayStore[Reference],
    owner: str,
    enqueue: Enqueue[Reference],
    *,
    wakeup: asyncio.Event,
    poll_interval: float,
    debounce_seconds: float,
    enqueue_concurrency: int = 1,
) -> None:
    """Drain durable work after coalesced hints, with timed polling fallback."""
    if min(poll_interval, debounce_seconds) <= 0:
        raise ValueError("Relay timing values must be positive")
    started_ns = monotonic_ns()
    logging.log_event("INFO", "runtime.relay.started", relay=name)
    try:
        while True:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(wakeup.wait(), timeout=poll_interval)
            wakeup.clear()
            await asyncio.sleep(debounce_seconds)
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
                    if logging.rate_limit(f"relay:{name}:iteration-failed"):
                        logging.log_event(
                            "ERROR",
                            "runtime.relay.iteration_failed",
                            exception=error,
                            relay=name,
                            error_type=type(error).__name__,
                        )
                    result = RelayBatchResult(0, 0, 0)
                if result.claimed == 0:
                    break
                logging.log_event(
                    "DEBUG",
                    "runtime.relay.batch",
                    relay=name,
                    claimed=result.claimed,
                    enqueued=result.enqueued,
                    enqueue_failed=result.enqueue_failed,
                    duration_ms=logging.duration_ms(iteration_started_ns),
                )
    finally:
        logging.log_event(
            "INFO",
            "runtime.relay.stopped",
            relay=name,
            duration_ms=logging.duration_ms(started_ns),
        )


async def _enqueue_outbox(reference: OutboxDeliveryReference) -> str:
    """Enqueue one fenced delivery into the Taskiq Redis Stream."""
    task = await cast(Any, dispatch_outbox_delivery).kiq(
        str(reference.event_id),
        reference.consumer,
        str(reference.delivery_token),
        reference.trace_id.hex if reference.trace_id is not None else None,
        reference.event_type,
        reference.event_created_at.isoformat() if reference.event_created_at is not None else None,
    )
    return str(task.task_id)


def _reference_fields(reference: object) -> ReferenceLogFields:
    """Return only non-fencing identifiers approved for relay logs."""
    if isinstance(reference, OutboxDeliveryReference):
        fields: ReferenceLogFields = {"event_id": str(reference.event_id), "consumer": reference.consumer}
        if reference.trace_id is not None:
            fields["trace_id"] = reference.trace_id.hex
        return fields
    return {}


if __name__ == "__main__":
    asyncio.run(run_relay())
