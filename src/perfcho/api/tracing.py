"""Bind one trace context to each HTTP request."""

from starlette.datastructures import Headers, MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from perfcho.infra.tracing import new_trace_id, reset_trace_id, set_trace_id, trace_id_from_traceparent


class TraceContextMiddleware:
    """Propagate an upstream trace or create a server trace for an HTTP request."""

    def __init__(self, app: ASGIApp) -> None:
        """Wrap one ASGI application."""
        self._app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Bind trace context for the complete request and response lifecycle."""
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return

        trace_id = trace_id_from_traceparent(Headers(scope=scope).get("traceparent")) or new_trace_id()
        token = set_trace_id(trace_id)

        async def send_with_trace(message: Message) -> None:
            if message["type"] == "http.response.start":
                MutableHeaders(scope=message)["x-trace-id"] = trace_id
            await send(message)

        try:
            await self._app(scope, receive, send_with_trace)
        finally:
            reset_trace_id(token)
