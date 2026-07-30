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
from datetime import date, datetime
from enum import Enum
from pathlib import Path
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
    "<level>{message}</level> | {extra[human_extra]}"
)
_HUMAN_HIDDEN_EXTRA_FIELDS = frozenset(
    {"event", "library", "event_schema", "service", "process_role", "process_identity", "pid", "human_extra"}
)
_BASE_FIELDS = frozenset({"event_schema", "service", "process_role", "pid", "event"})
_NOISY_WORKER_LIBRARY_LOGS = frozenset({("taskiq.redis_broker", "Starting fetching new messages")})
_SAFE_EVENT_FIELDS = frozenset(
    {
        "account_id",
        "achievement_id",
        "action",
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
        "exception_frames",
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
        "packet_count",
        "packet_histogram",
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
        "schema_count",
        "scope",
        "score_id",
        "scoreboard_id",
        "sender_account_id",
        "session_id",
        "size_bytes",
        "slow",
        "source_event_id",
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
    """Write a minimal JSON envelope without Loguru's source and exception fields."""

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
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno
        exception = record.exc_info[1] if record.exc_info is not None else None
        log_event(level, "library.log", exception=exception, library=record.name, msg=message)


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
            backtrace=False,
            diagnose=False,
        )
    else:
        _ = logger.add(
            output,
            level=settings.log_level,
            format=_format_human,
            colorize=output.isatty(),
            enqueue=True,
            backtrace=False,
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
    if exception is not None:
        _ = approved.setdefault("error_type", type(exception).__name__)
        approved["exception_frames"] = _exception_frames(exception)
    _ = logger.bind(event=event, **approved).log(level, event)


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
    approved["pid"] = record["process"].id
    record["extra"] = approved


def _exception_frames(exception: BaseException) -> tuple[dict[str, object], ...]:
    """Return bounded source locations without exception values or code text."""
    extracted = traceback.extract_tb(exception.__traceback__)[-8:]
    return tuple(
        {
            "file": Path(frame.filename).name,
            "function": frame.name,
            "line": frame.lineno,
        }
        for frame in extracted
    )


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
