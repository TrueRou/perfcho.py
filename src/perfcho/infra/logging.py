"""Configure allow-listed process logging and bounded hot-path emission."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import sys
import time
import traceback
import uuid
from collections import OrderedDict
from collections.abc import Iterable
from contextvars import ContextVar, Token
from datetime import date, datetime
from enum import Enum
from threading import Lock
from typing import TYPE_CHECKING, Literal, TextIO, cast, override

from loguru import logger

from perfcho.infra.settings import settings

if TYPE_CHECKING:
    from loguru import Message, Record

type ProcessRole = Literal["api", "worker", "migration", "test"]

_HUMAN_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{extra[process_identity]}</cyan> | "
    "<level>{message}</level> | {extra[human_extra]}\n{exception}"
)
_HUMAN_HIDDEN_EXTRA_FIELDS = frozenset(
    {
        "event",
        "library",
        "event_schema",
        "service",
        "process_role",
        "process_identity",
        "pid",
        "human_extra",
        "correlation_id",
        "exception",
    }
)
_BASE_FIELDS = frozenset({"event_schema", "service", "process_role", "pid", "event"})
_NOISY_WORKER_LIBRARY_LOGS = frozenset({("taskiq.redis_broker", "Starting fetching new messages")})
_relay_task_name: ContextVar[str | None] = ContextVar("perfcho_relay_task_name", default=None)
_SAFE_EVENT_FIELDS = frozenset(
    {
        "account_id",
        "achievement_id",
        "action",
        "aggregate_id",
        "aggregate_type",
        "actor_account_id",
        "attempt",
        "attempt_count",
        "attempt_id",
        "attempts",
        "away_message_length",
        "batches_committed",
        "beatmap_id",
        "beatmap_revision_id",
        "beatmapset_id",
        "calculator",
        "changed",
        "channel_count",
        "channel_id",
        "check",
        "checked",
        "checkpoint_status",
        "checks",
        "claimed",
        "client_message_id",
        "command",
        "completed_phases",
        "consumer",
        "content_length",
        "correlation_id",
        "created_revision_count",
        "credential_upgraded",
        "deferred_mailbox_packet_count",
        "delay_ms",
        "delivered_count",
        "delivery_failure_count",
        "detail_type",
        "device_id",
        "diagnostics",
        "direct_recipient_account_id",
        "durable_session_outcome",
        "duration_ms",
        "enqueue_failed",
        "enqueued",
        "endpoint_label",
        "error_code",
        "error_count",
        "error_type",
        "errors",
        "event_id",
        "event_type",
        "exists",
        "external_beatmapset_id",
        "failed_checks",
        "failure",
        "failure_count",
        "failures",
        "favourited",
        "fellow_count",
        "frame_bytes",
        "friend_count",
        "high_account_id",
        "history_frame_count",
        "host_account_id",
        "input_bytes",
        "invocation_id",
        "item_count",
        "job_id",
        "joined",
        "leased_packet_count",
        "library",
        "local_output_bytes",
        "low_account_id",
        "mailbox_stage",
        "max_overflow",
        "maximum_attempts",
        "media_type",
        "message_id",
        "message_kind",
        "message_length",
        "migration_id",
        "mod_set_id",
        "msg",
        "multiplayer_outcome",
        "object_kind",
        "offline_message_count",
        "online_presence_count",
        "operation",
        "outcome",
        "output_bytes",
        "outbox_payload",
        "packet_count",
        "packet_histogram",
        "payload_fields",
        "participant_count",
        "phase",
        "policy",
        "pool_size",
        "presence_broadcast_failure_count",
        "protocol",
        "provider_code",
        "public_id",
        "rating",
        "realtime_outcome",
        "reason",
        "recipient_count",
        "relay",
        "relay_task",
        "release_version",
        "removed_beatmap_count",
        "removed_follow_count",
        "replayed",
        "reply_to_id",
        "request_id",
        "resource",
        "response_bytes",
        "retryable",
        "returned_mailbox_packet_count",
        "room_id",
        "round_id",
        "route",
        "rows_committed",
        "ruleset",
        "schema_version",
        "schema_count",
        "scope",
        "score_id",
        "scoreboard_id",
        "sender_account_id",
        "session_id",
        "size_bytes",
        "slow",
        "source_event_id",
        "source_position",
        "spectator_account_id",
        "stage",
        "state_revision",
        "status",
        "status_code",
        "table_count",
        "target_account_id",
        "tls",
        "unchanged_revision_count",
        "verification",
        "version",
    }
)
_RATE_LIMIT_CAPACITY = 256
_rate_limit_lock = Lock()
_rate_limit_deadlines: OrderedDict[str, float] = OrderedDict()


class _JsonSink:
    """Write an allow-listed JSON envelope with complete exception details."""

    def __init__(self, stream: TextIO) -> None:
        self._stream: TextIO = stream

    def __call__(self, message: Message) -> None:
        record = message.record
        extra = cast(dict[str, object], record["extra"])
        payload = {
            "timestamp": record["time"].isoformat(),
            "level": record["level"].name,
            **{key: _json_value(value) for key, value in extra.items()},
        }
        _ = self._stream.write(json.dumps(payload, ensure_ascii=True, separators=(",", ":")) + "\n")
        self._stream.flush()


def _format_human(record: Record) -> str:
    extra = cast(dict[str, object], record["extra"])
    process_role = extra["process_role"]
    extra["process_identity"] = f"{process_role}[{extra['pid']}]" if process_role == "worker" else process_role
    extra["human_extra"] = {key: value for key, value in extra.items() if key not in _HUMAN_HIDDEN_EXTRA_FIELDS}
    return f"{_HUMAN_FORMAT}\n"


class InterceptHandler(logging.Handler):
    """Forward allow-listed standard-library record metadata and message."""

    def __init__(self, process_role: ProcessRole) -> None:
        """Configure role-specific library log filtering."""
        super().__init__()
        self._process_role: ProcessRole = process_role

    @override
    def emit(self, record: logging.LogRecord) -> None:
        """Translate one library record into a structured event."""
        message = record.getMessage()
        if self._process_role == "worker" and (record.name, message) in _NOISY_WORKER_LIBRARY_LOGS:
            return
        if self._process_role == "worker" and record.name == "taskiq.worker" and message.startswith("Executing task "):
            return
        if (
            self._process_role == "worker"
            and record.name == "taskiq.redis_broker"
            and message.startswith("Received message: ")
        ):
            return
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno
        exception = record.exc_info[1] if record.exc_info is not None else None
        relay_task = current_relay_task()
        fields: dict[str, object] = {"library": record.name, "msg": message}
        if relay_task is not None:
            fields["relay_task"] = relay_task
        log_event(
            level,
            relay_task or "library.log",
            exception=exception,
            **fields,
        )


def init_logger(process_role: ProcessRole, *, stream: TextIO | None = None) -> None:
    """Configure one process-wide sink and standard-library interception."""
    _ = logger.remove()
    _ = logger.configure(
        extra={
            "event_schema": 1,
            "service": "perfcho",
            "process_role": process_role,
            "pid": os.getpid(),
            "event": "application.log",
        },
        patcher=_sanitize_record,
    )
    output = stream or (sys.stderr if process_role == "migration" else sys.stdout)
    if settings.log_format == "json":
        _ = logger.add(
            _JsonSink(output),
            level=settings.log_level,
            enqueue=True,
            backtrace=True,
            diagnose=False,
        )
    else:
        _ = logger.add(
            output,
            level=settings.log_level,
            format=_format_human,
            colorize=output.isatty(),
            enqueue=True,
            backtrace=True,
            diagnose=False,
        )

    intercept_handler = InterceptHandler(process_role)
    logging.basicConfig(handlers=[intercept_handler], level=settings.log_level, force=True)
    for name in ("uvicorn", "uvicorn.error", "taskiq"):
        child = logging.getLogger(name)
        child.handlers = []
        child.propagate = True
    access_logger = logging.getLogger("uvicorn.access")
    access_logger.handlers = []
    access_logger.propagate = False
    access_logger.disabled = True
    for name in ("httpx", "httpcore", "botocore", "aiobotocore", "boto3", "redis", "sqlalchemy.engine"):
        logging.getLogger(name).setLevel(logging.WARNING)


def log_event(level: str | int, event: str, *, exception: BaseException | None = None, **fields: object) -> None:
    """Emit one named event after dropping fields outside the logging contract."""
    approved = {key: value for key, value in fields.items() if key in _SAFE_EVENT_FIELDS}
    relay_task = current_relay_task()
    if relay_task is not None:
        approved.setdefault("relay_task", relay_task)
    if exception is not None:
        _ = approved.setdefault("error_type", type(exception).__name__)
    _ = logger.bind(event=event, **approved).opt(exception=exception).log(level, event)


def set_relay_task(task_name: str | None) -> Token[str | None]:
    """Bind the concrete Taskiq task name to the current execution context."""
    return _relay_task_name.set(task_name)


def reset_relay_task(token: Token[str | None]) -> None:
    """Restore the relay task context after one Taskiq task finishes."""
    _relay_task_name.reset(token)


def clear_relay_task() -> None:
    """Clear the current Taskiq task context after worker execution."""
    _ = _relay_task_name.set(None)


def current_relay_task() -> str | None:
    """Return the Taskiq task name bound to the current execution context."""
    return _relay_task_name.get()


def sampled(sample_key: object, rate: float) -> bool:
    """Return a deterministic sampling decision without global random state."""
    if rate <= 0:
        return False
    if rate >= 1:
        return True
    digest = hashlib.blake2b(str(sample_key).encode(), digest_size=8, person=b"perfcho-log").digest()
    value = int.from_bytes(digest) / float(1 << 64)
    return value < rate


def rate_limit(key: str, *, interval_seconds: float = 30.0) -> bool:
    """Allow one event per bounded process-local key and time interval."""
    now = time.monotonic()
    with _rate_limit_lock:
        deadline = _rate_limit_deadlines.get(key, 0.0)
        if deadline > now:
            return False
        _rate_limit_deadlines[key] = now + interval_seconds
        _rate_limit_deadlines.move_to_end(key)
        while len(_rate_limit_deadlines) > _RATE_LIMIT_CAPACITY:
            _ = _rate_limit_deadlines.popitem(last=False)
    return True


def duration_ms(started_ns: int) -> float:
    """Return elapsed monotonic milliseconds rounded for log output."""
    return round((time.monotonic_ns() - started_ns) / 1_000_000, 3)


def _sanitize_record(record: Record) -> None:
    """Remove contextual fields not explicitly approved by the event schema."""
    extra = cast(dict[str, object], record["extra"])
    approved = {key: value for key, value in extra.items() if key in _BASE_FIELDS or key in _SAFE_EVENT_FIELDS}
    exception = record["exception"]
    if exception is not None and exception.type is not None and exception.value is not None:
        approved["exception"] = {
            "type": exception.type.__name__,
            "message": str(exception.value),
            "traceback": "".join(traceback.format_exception(exception.type, exception.value, exception.traceback)),
        }
    approved["pid"] = record["process"].id
    record["extra"] = approved


def _json_value(value: object) -> object:
    """Convert approved values without stringifying arbitrary application objects."""
    if value is None or isinstance(value, str | int | float | bool):
        return value
    if isinstance(value, Enum):
        return _json_value(cast(object, value.value))
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in cast(dict[object, object], value).items()}
    if isinstance(value, tuple | list | set | frozenset):
        return [_json_value(item) for item in cast(Iterable[object], value)]
    return f"<{type(value).__name__}>"
