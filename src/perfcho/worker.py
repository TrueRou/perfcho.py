"""Compose the Taskiq worker process and execute durable deliveries."""

from contextlib import suppress
from datetime import UTC, datetime, timedelta
from time import monotonic_ns
from typing import Annotated, Literal, TypedDict, cast

import httpx
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from taskiq import Context, TaskiqDepends, TaskiqEvents, TaskiqState

from perfcho.infra import logging
from perfcho.infra.cache import RedisCache
from perfcho.infra.db import engine as infra_db
from perfcho.infra.db.base import DbSessionFactory
from perfcho.infra.db.enums import OutboxDeliveryStatus
from perfcho.infra.db.models.events import OutboxDelivery, OutboxEvent
from perfcho.infra.db.projectors import notification, ranking
from perfcho.infra.db.projectors.catalog import ConsumerCatalog, build_consumer_catalog
from perfcho.infra.db.projectors.performance import PerformanceProjector
from perfcho.infra.db.repositories.outbox_delivery import (
    OutboxDeliveryReference,
    SqlAlchemyOutboxDeliveryRepository,
    _clear_delivery_lease,
    _database_now,
    _error_message,
    _retry_delay,
)
from perfcho.infra.redis import engine as infra_redis
from perfcho.infra.redis.bubbles import RedisRealtimeBubbleBus
from perfcho.infra.redis.realtime import RedisRealtimeStateRepository
from perfcho.infra.settings import settings
from perfcho.infra.storage import S3ObjectStorage
from perfcho.infra.tracing import trace_context
from perfcho.infra.upstream.calculator import HttpPerformanceCalculator

logging.init_logger("worker")

from perfcho.infra.logging import set_relay_delay_ms, set_relay_event_type  # noqa: E402
from perfcho.infra.taskiq import broker  # noqa: E402

_HANDLED_FAILURE_ATTRIBUTE = "_perfcho_outbox_failure_handled"

type DeliveryFailureOutcome = tuple[Literal["retry", "dead"], int]


class _WorkerResources:
    """Own process resources that require ordered shutdown."""

    def __init__(
        self,
        db_engine: AsyncEngine,
        http_client: httpx.AsyncClient,
        cache: RedisCache,
        started_ns: int,
        state_redis: Redis | None = None,
        bubble_redis: Redis | None = None,
    ) -> None:
        """Store worker-owned resources."""
        self.db_engine = db_engine
        self.http_client = http_client
        self.cache = cache
        self.started_ns = started_ns
        self.state_redis = state_redis
        self.bubble_redis = bubble_redis


class OutboxEventLogFields(TypedDict, total=False):
    """Describe the outbox event context emitted by delivery logs."""

    event_type: str
    aggregate_type: str
    aggregate_id: str
    schema_version: int
    source_position: int
    payload_fields: tuple[str, ...]
    outbox_payload: dict[str, object]


class OutboxDeliveryProcessor:
    """Execute one fenced projector transaction and persist bounded failures."""

    def __init__(
        self,
        session_factory: DbSessionFactory,
        consumer_catalog: ConsumerCatalog,
        *,
        max_attempts: int,
        max_retry_seconds: int,
    ) -> None:
        """Bind delivery execution to PostgreSQL, consumers, and retry policy."""
        if min(max_attempts, max_retry_seconds) < 1:
            raise ValueError("Outbox processor limits must be positive")
        self._session_factory = session_factory
        self._consumer_catalog = consumer_catalog
        self._max_attempts = max_attempts
        self._max_retry_seconds = max_retry_seconds

    async def execute(self, reference: OutboxDeliveryReference) -> None:
        """Run one delivery if its lease is current and unexpired at task start."""
        started_ns = monotonic_ns()
        outcome: Literal["stale", "succeeded"] = "stale"
        attempt_count: int | None = None
        event_fields: OutboxEventLogFields = {}
        failure_event_fields: OutboxEventLogFields = {}
        event: OutboxEvent | None = None
        try:
            async with self._session_factory.begin() as session:
                delivery = await _locked_delivery(session, reference)
                if delivery is None:
                    raise LookupError(f"Outbox delivery does not exist: {reference.event_id}/{reference.consumer}")
                event = await session.get(OutboxEvent, reference.event_id)
                if event is not None:
                    event_fields = _outbox_event_fields(event)
                    failure_event_fields = _outbox_event_fields(event, include_payload=True)
                if delivery.status not in {OutboxDeliveryStatus.SUCCEEDED, OutboxDeliveryStatus.DEAD}:
                    now = await _database_now(session)
                    if (
                        delivery.status is OutboxDeliveryStatus.RUNNING
                        and delivery.delivery_token == reference.delivery_token
                        and delivery.lease_expires_at is not None
                        and delivery.lease_expires_at > now
                    ):
                        registration = self._consumer_catalog.get(reference.consumer)
                        if registration is None:
                            raise LookupError(f"Outbox consumer is not registered: {reference.consumer}")
                        if event is None:
                            raise LookupError(f"Outbox event does not exist: {reference.event_id}")
                        if event.event_type not in registration.event_types:
                            raise LookupError(
                                f"Outbox consumer {reference.consumer} does not accept event type {event.event_type}"
                            )

                        await registration.handler(session, event, delivery.partition_key)
                        delivery.attempt_count += 1
                        delivery.status = OutboxDeliveryStatus.SUCCEEDED
                        delivery.completed_at = now
                        _clear_delivery_lease(delivery)
                        delivery.last_error = None
                        outcome = "succeeded"
                        attempt_count = delivery.attempt_count
        except Exception as error:
            failure = await self._record_failure(reference, error)
            if failure is None:
                raise
            _mark_outbox_failure_handled(error)
            failure_outcome, failure_attempt_count = failure
            logging.log_event(
                "ERROR" if failure_outcome == "dead" else "WARNING",
                f"outbox.delivery.{failure_outcome}",
                exception=error,
                event_id=str(reference.event_id),
                consumer=reference.consumer,
                attempt_count=failure_attempt_count,
                error_type=type(error).__name__,
                duration_ms=logging.duration_ms(started_ns),
                **failure_event_fields,
            )
            raise
        if outcome == "succeeded":
            assert attempt_count is not None
            logging.log_event(
                "INFO",
                "outbox.delivery.succeeded",
                event_id=str(reference.event_id),
                consumer=reference.consumer,
                attempt_count=attempt_count,
                duration_ms=logging.duration_ms(started_ns),
                **event_fields,
            )
        else:
            logging.log_event(
                "DEBUG",
                "outbox.delivery.stale",
                event_id=str(reference.event_id),
                consumer=reference.consumer,
                duration_ms=logging.duration_ms(started_ns),
                **event_fields,
            )

    async def _record_failure(
        self,
        reference: OutboxDeliveryReference,
        error: Exception,
    ) -> DeliveryFailureOutcome | None:
        outcome: DeliveryFailureOutcome | None = None
        async with self._session_factory.begin() as session:
            delivery = await _locked_delivery(session, reference)
            if (
                delivery is not None
                and delivery.status is OutboxDeliveryStatus.RUNNING
                and delivery.delivery_token == reference.delivery_token
            ):
                now = await _database_now(session)
                delivery.attempt_count += 1
                delivery.enqueued_at = None
                delivery.broker_task_id = None
                delivery.last_error = _error_message(error)
                if delivery.attempt_count >= self._max_attempts:
                    delivery.status = OutboxDeliveryStatus.DEAD
                    delivery.dead_lettered_at = now
                    outcome = ("dead", delivery.attempt_count)
                else:
                    delivery.status = OutboxDeliveryStatus.PENDING
                    delivery.available_at = now + _retry_delay(
                        delivery.attempt_count,
                        self._max_retry_seconds,
                    )
                    outcome = ("retry", delivery.attempt_count)
                _clear_delivery_lease(delivery)
        return outcome


def is_handled_outbox_failure(error: Exception) -> bool:
    """Return whether the processor already logged a durable failure outcome."""
    return bool(getattr(error, _HANDLED_FAILURE_ATTRIBUTE, False))


def _mark_outbox_failure_handled(error: Exception) -> None:
    with suppress(AttributeError, TypeError):
        setattr(error, _HANDLED_FAILURE_ATTRIBUTE, True)


async def _locked_delivery(
    session: AsyncSession,
    reference: OutboxDeliveryReference,
) -> OutboxDelivery | None:
    return await session.get(
        OutboxDelivery,
        {"event_id": reference.event_id, "consumer": reference.consumer},
        with_for_update=True,
    )


def _outbox_event_fields(
    event: OutboxEvent,
    *,
    include_payload: bool = False,
) -> OutboxEventLogFields:
    """Return event identity, adding the complete payload only for failure diagnostics."""
    fields: OutboxEventLogFields = {
        "event_type": event.event_type,
        "aggregate_type": event.aggregate_type,
        "aggregate_id": event.aggregate_id,
        "schema_version": event.schema_version,
        "source_position": event.position,
        "payload_fields": tuple(sorted(event.payload)),
    }
    if include_payload:
        fields["outbox_payload"] = event.payload
    return fields


@broker.task(task_name="perfcho.outbox.dispatch")
async def dispatch_outbox_delivery(
    event_id: str,
    consumer: str,
    delivery_token: str,
    trace_id: str | None,
    event_type: str | None,
    event_created_at: str | None,
    context: Annotated[Context, TaskiqDepends()],
) -> None:
    """Pass one broker message to the worker-composed delivery processor."""
    set_relay_event_type(event_type)
    set_relay_delay_ms(_consumer_delay_ms(event_created_at))
    try:
        with trace_context(trace_id):
            await _dispatch_outbox_delivery(event_id, consumer, delivery_token, context)
    finally:
        set_relay_event_type(None)
        set_relay_delay_ms(None)


@broker.on_event(TaskiqEvents.WORKER_STARTUP)
async def worker_startup(state: TaskiqState) -> None:
    """Create resources used by Taskiq delivery consumers."""
    started_ns = monotonic_ns()
    state.worker_started_ns = started_ns
    state.worker_lifecycle_stopped = False
    logging.log_event("INFO", "runtime.worker.starting")
    db_engine: AsyncEngine | None = None
    http_client: httpx.AsyncClient | None = None
    cache: RedisCache | None = None
    state_redis: Redis | None = None
    bubble_redis: Redis | None = None
    try:
        db_engine = await infra_db.create_engine(settings)
        session_factory = infra_db.create_session_factory(db_engine)
        http_client = httpx.AsyncClient(timeout=settings.performance_http_timeout_seconds)
        cache_redis = await infra_redis.create_cache_redis(settings)
        cache = RedisCache(cache_redis, prefix=settings.redis_cache_prefix)
        state_redis = await infra_redis.create_state_redis(settings)
        bubble_redis = await infra_redis.create_bubble_redis(settings)
        outbox_repository = SqlAlchemyOutboxDeliveryRepository(
            session_factory,
            batch_size=settings.outbox_delivery_batch_size,
            lease_seconds=settings.outbox_delivery_lease_seconds,
            max_attempts=settings.outbox_delivery_max_attempts,
            max_retry_seconds=settings.outbox_delivery_max_retry_seconds,
        )

        async def invalidate_leaderboard(beatmap_id: int) -> None:
            await cache.increment(cache.key("scoring", "leaderboard-generation", str(beatmap_id)))

        async def ranking_handler(session: AsyncSession, event: OutboxEvent, partition_key: str) -> None:
            await ranking.project_accepted_score(session, event, partition_key, invalidate_leaderboard)

        realtime = RedisRealtimeStateRepository(
            state_redis,
            prefix=settings.redis_state_prefix,
            session_ttl=timedelta(seconds=settings.redis_session_ttl_seconds),
            presence_ttl=timedelta(seconds=settings.redis_presence_ttl_seconds),
            max_frame_count=settings.redis_spectator_max_frames,
            max_frame_bytes=settings.redis_spectator_max_bytes,
            max_spectators_per_host=settings.redis_spectator_max_viewers,
        )
        bubble_bus = RedisRealtimeBubbleBus(
            bubble_redis,
            prefix=settings.redis_state_prefix,
            max_entries=settings.redis_bubble_max_entries,
            ttl_seconds=settings.redis_bubble_ttl_seconds,
        )
        notification_handler = notification.notification_realtime_handler(
            realtime=realtime,
            bubbles=bubble_bus,
            bot_account_id=settings.bot_account_id,
            bot_name=settings.bot_name,
        )

        consumer_catalog = build_consumer_catalog(
            PerformanceProjector(
                HttpPerformanceCalculator(http_client, settings.performance_calculator_urls),
                S3ObjectStorage.from_settings(settings),
                beatmap_url_expiry_seconds=settings.performance_beatmap_url_expiry_seconds,
            ),
            ranking_handler=ranking_handler,
            notification_realtime_handler=notification_handler,
        )
        unknown_consumers = await outbox_repository.unknown_consumers(consumer_catalog)
        if unknown_consumers:
            names = ", ".join(sorted(unknown_consumers))
            raise RuntimeError(f"Unregistered outbox consumers have unfinished deliveries: {names}")

        state.outbox_delivery_processor = OutboxDeliveryProcessor(
            session_factory,
            consumer_catalog,
            max_attempts=settings.outbox_delivery_max_attempts,
            max_retry_seconds=settings.outbox_delivery_max_retry_seconds,
        )
        state.worker_resources = _WorkerResources(
            db_engine, http_client, cache, started_ns, state_redis, bubble_redis
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
        await _cleanup_worker_resources(http_client, db_engine, cache, state_redis, bubble_redis)
        state.worker_lifecycle_stopped = True
        logging.log_event("INFO", "runtime.worker.stopped", duration_ms=logging.duration_ms(started_ns))
        raise


@broker.on_event(TaskiqEvents.WORKER_SHUTDOWN)
async def worker_shutdown(state: TaskiqState) -> None:
    """Close worker-owned resources after Taskiq stops receiving work."""
    if bool(getattr(state, "worker_lifecycle_stopped", False)):
        return
    resources = cast(_WorkerResources | None, getattr(state, "worker_resources", None))
    started_ns = resources.started_ns if resources is not None else monotonic_ns()
    logging.log_event("INFO", "runtime.worker.stopping")
    if resources is not None:
        await _cleanup_worker_resources(
            resources.http_client, resources.db_engine, resources.cache, resources.state_redis, resources.bubble_redis
        )
    state.worker_lifecycle_stopped = True
    logging.log_event("INFO", "runtime.worker.stopped", duration_ms=logging.duration_ms(started_ns))


async def _cleanup_worker_resources(
    http_client: httpx.AsyncClient | None,
    db_engine: AsyncEngine | None,
    cache: RedisCache | None = None,
    state_redis: Redis | None = None,
    bubble_redis: Redis | None = None,
) -> None:
    """Close every initialized worker resource even when an earlier close fails."""
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
    if cache is not None:
        try:
            await cache.aclose()
        except Exception as error:
            logging.log_event(
                "ERROR",
                "runtime.worker.resource_close_failed",
                exception=error,
                resource="cache",
                error_type=type(error).__name__,
            )
    for resource, client in (("state", state_redis), ("bubble", bubble_redis)):
        if client is not None:
            try:
                await client.aclose()
            except Exception as error:
                logging.log_event(
                    "ERROR",
                    "runtime.worker.resource_close_failed",
                    exception=error,
                    resource=resource,
                    error_type=type(error).__name__,
                )


async def _dispatch_outbox_delivery(
    event_id: str,
    consumer: str,
    delivery_token: str,
    context: Context,
) -> None:
    """Execute one delivery inside the caller's trace context."""
    import uuid

    parsed_event_id: uuid.UUID | None = None
    try:
        parsed_event_id = uuid.UUID(event_id)
        parsed_delivery_token = uuid.UUID(delivery_token)
    except (AttributeError, TypeError, ValueError) as error:
        logging.log_event(
            "ERROR",
            "task.outbox_delivery.malformed_payload",
            exception=error,
            event_id=str(parsed_event_id) if parsed_event_id is not None else None,
            consumer=consumer,
            error_type=type(error).__name__,
        )
        raise

    try:
        processor = cast(OutboxDeliveryProcessor, context.state.outbox_delivery_processor)
        await processor.execute(
            OutboxDeliveryReference(
                event_id=parsed_event_id,
                consumer=consumer,
                delivery_token=parsed_delivery_token,
            )
        )
    except Exception as error:
        if not is_handled_outbox_failure(error):
            logging.log_event(
                "ERROR",
                "task.outbox_delivery.failed",
                exception=error,
                event_id=str(parsed_event_id),
                consumer=consumer,
                error_type=type(error).__name__,
            )
        raise


def _consumer_delay_ms(event_created_at: str | None) -> float | None:
    """Return elapsed milliseconds from durable event creation to consumer start."""
    if event_created_at is None:
        return None
    try:
        created_at = datetime.fromisoformat(event_created_at)
    except ValueError:
        return None
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=UTC)
    return max(0.0, round((datetime.now(UTC) - created_at).total_seconds() * 1000, 3))
