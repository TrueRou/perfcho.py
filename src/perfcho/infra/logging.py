"""Configure process logging, direct Loki delivery, and bounded hot-path emission."""

from __future__ import annotations

import hashlib
import logging
import os
import sys
import time
from collections import OrderedDict
from contextvars import ContextVar, Token
from threading import Lock
from typing import TYPE_CHECKING, Literal, TextIO, cast, override

from loguru import logger
from loki_logger_handler.formatters.loguru_formatter import LoguruFormatter
from loki_logger_handler.loki_logger_handler import LokiLoggerHandler

from perfcho.infra.settings import settings
from perfcho.infra.tracing import current_trace_id

if TYPE_CHECKING:
    from loguru import Record

type ProcessRole = Literal["api", "worker", "migration", "test"]

_HUMAN_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{extra[process_role]}[{extra[pid]}]</cyan> | "
    "<magenta>{extra[trace_id]}</magenta> | "
    "<level>{message}</level>{extra[human_suffix]}\n{exception}"
)
_LOKI_METADATA_KEYS = (
    "trace_id",
    "process",
    "thread",
    "function",
    "module",
    "name",
    "level",
    "event_schema",
    "service",
    "process_role",
    "pid",
)
_NOISY_WORKER_LIBRARY_LOGS = frozenset({("taskiq.redis_broker", "Starting fetching new messages")})
_relay_task_name: ContextVar[str | None] = ContextVar("perfcho_relay_task_name", default=None)
_relay_event_type: ContextVar[str | None] = ContextVar("perfcho_relay_event_type", default=None)
_relay_delay_ms: ContextVar[float | None] = ContextVar("perfcho_relay_delay_ms", default=None)
_RATE_LIMIT_CAPACITY = 256
_rate_limit_lock = Lock()
_rate_limit_deadlines: OrderedDict[str, float] = OrderedDict()


class _StructuredLoguruFormatter(LoguruFormatter):
    """Keep the third-party formatter's structured metadata mapping writable."""

    def format(self, record: Record) -> tuple[dict[str, object], dict[str, object]]:
        formatted, metadata = super().format(record)
        structured = cast(dict[str, object], formatted)
        structured.pop("human_suffix", None)
        return structured, cast(dict[str, object], metadata or {})


class InterceptHandler(logging.Handler):
    """Forward standard-library logger names, messages, and exceptions."""

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
        if (
            self._process_role == "worker"
            and record.name.startswith("taskiq.")
            and message.startswith("Executing task ")
        ):
            return
        if (
            self._process_role == "worker"
            and record.name == "taskiq.redis_broker"
            and message.startswith("Received message: ")
        ):
            return
        level: str | int
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno
        exception = record.exc_info[1] if record.exc_info is not None else None
        relay_task = current_relay_task()
        fields: dict[str, object] = {"library": record.name}
        if relay_task is not None:
            fields["relay_task"] = relay_task
        relay_event_type = current_relay_event_type()
        if relay_event_type is not None:
            fields["event_type"] = relay_event_type
        relay_delay_ms = current_relay_delay_ms()
        if relay_delay_ms is not None:
            fields["delay_ms"] = relay_delay_ms
        if exception is not None:
            fields["error_type"] = type(exception).__name__
        _ = logger.bind(event=relay_task or "library.log", **fields).opt(exception=exception).log(level, message)


def init_logger(process_role: ProcessRole, *, stream: TextIO | None = None) -> None:
    """Configure one process-wide sink and standard-library interception."""
    logger.remove()
    _ = logger.configure(
        extra={
            "event_schema": 1,
            "service": "perfcho",
            "process_role": process_role,
            "pid": os.getpid(),
            "event": "application.log",
            "trace_id": "-",
            "human_suffix": "",
        },
        patcher=_bind_trace_id,
    )
    output = stream or (sys.stderr if process_role == "migration" else sys.stdout)
    _ = logger.add(
        output,
        level=settings.log_level,
        format=_HUMAN_FORMAT,
        colorize=output.isatty(),
        enqueue=True,
        backtrace=True,
        diagnose=False,
    )
    if settings.loki_url is not None:
        loki_handler = LokiLoggerHandler(
            url=settings.loki_url,
            labels={
                "application": "perfcho",
                "environment": settings.loki_environment,
                "process_role": process_role,
            },
            label_keys={"level"},
            timeout=settings.loki_flush_interval_seconds,
            compressed=True,
            default_formatter=_StructuredLoguruFormatter(),
            enable_structured_loki_metadata=True,
            loki_metadata_keys=_LOKI_METADATA_KEYS,
        )
        _ = logger.add(
            loki_handler.write,
            level=settings.log_level,
            enqueue=False,
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
    """Emit one named event with all caller-provided fields."""
    relay_task = current_relay_task()
    if relay_task is not None:
        fields.setdefault("relay_task", relay_task)
    relay_event_type = current_relay_event_type()
    if relay_event_type is not None:
        fields.setdefault("event_type", relay_event_type)
    relay_delay_ms = current_relay_delay_ms()
    if relay_delay_ms is not None:
        fields.setdefault("delay_ms", relay_delay_ms)
    if exception is not None:
        _ = fields.setdefault("error_type", type(exception).__name__)
    _ = logger.bind(event=event, **fields).opt(exception=exception).log(level, event)


def _bind_trace_id(record: Record) -> None:
    """Attach active trace and derive the concise human-only suffix."""
    extra = record["extra"]
    if isinstance(extra, dict):
        trace_id = current_trace_id()
        if trace_id is not None:
            extra["trace_id"] = trace_id
        else:
            extra.setdefault("trace_id", "-")
        suffix: list[str] = []
        if extra.get("process_role") == "worker" and (event_type := extra.get("event_type")) is not None:
            suffix.append(f"event_type={event_type}")
        if (duration := extra.get("duration_ms")) is not None:
            suffix.append(f"duration={duration}ms")
        if extra.get("process_role") == "worker" and (delay := extra.get("delay_ms")) is not None:
            suffix.append(f"delay={delay}ms")
        extra["human_suffix"] = "".join(f" | {part}" for part in suffix)


def set_relay_task(task_name: str | None) -> Token[str | None]:
    """Bind the concrete Taskiq task name to the current execution context."""
    return _relay_task_name.set(task_name)


def reset_relay_task(token: Token[str | None]) -> None:
    """Restore the relay task context after one Taskiq task finishes."""
    _relay_task_name.reset(token)


def clear_relay_task() -> None:
    """Clear the current Taskiq task context after worker execution."""
    _ = _relay_task_name.set(None)
    _ = _relay_event_type.set(None)
    _ = _relay_delay_ms.set(None)


def current_relay_task() -> str | None:
    """Return the Taskiq task name bound to the current execution context."""
    return _relay_task_name.get()


def set_relay_event_type(event_type: str | None) -> None:
    """Bind the durable event type consumed by the current relay task."""
    _ = _relay_event_type.set(event_type)


def current_relay_event_type() -> str | None:
    """Return the durable event type bound to the current relay task."""
    return _relay_event_type.get()


def set_relay_delay_ms(delay_ms: float | None) -> None:
    """Bind the event-to-consumer delay for the current relay task."""
    _ = _relay_delay_ms.set(delay_ms)


def current_relay_delay_ms() -> float | None:
    """Return the event-to-consumer delay bound to the current relay task."""
    return _relay_delay_ms.get()


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
