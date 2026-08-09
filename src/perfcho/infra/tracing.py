"""Create and scope trace identifiers across synchronous and asynchronous work."""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar, Token

_trace_id: ContextVar[str | None] = ContextVar("perfcho_trace_id", default=None)


def new_trace_id() -> str:
    """Return one W3C-compatible 128-bit trace identifier."""
    return uuid.uuid4().hex


def trace_id_from_traceparent(traceparent: str | None) -> str | None:
    """Extract a valid W3C trace identifier from a traceparent header."""
    if traceparent is None:
        return None
    parts = traceparent.split("-")
    if len(parts) != 4:
        return None
    version, trace_id, parent_id, flags = parts
    if version == "ff" or len(version) != 2 or len(trace_id) != 32 or len(parent_id) != 16 or len(flags) != 2:
        return None
    try:
        int(version, 16)
        int(trace_id, 16)
        int(parent_id, 16)
        int(flags, 16)
    except ValueError:
        return None
    if trace_id == "0" * 32 or parent_id == "0" * 16:
        return None
    return trace_id.lower()


def current_trace_id() -> str | None:
    """Return the trace identifier bound to the current execution context."""
    return _trace_id.get()


def set_trace_id(trace_id: str | None) -> Token[str | None]:
    """Bind a trace identifier and return the token needed to restore context."""
    return _trace_id.set(trace_id)


def reset_trace_id(token: Token[str | None]) -> None:
    """Restore the trace context represented by a previous token."""
    _trace_id.reset(token)


@contextmanager
def trace_context(trace_id: str | None) -> Iterator[None]:
    """Scope a trace identifier around one unit of work."""
    token = set_trace_id(trace_id)
    try:
        yield
    finally:
        reset_trace_id(token)
