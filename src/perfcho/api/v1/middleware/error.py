"""Translate framework exceptions and emit safe request lifecycle events."""

from __future__ import annotations

from time import monotonic_ns
from uuid import UUID, uuid4

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from loguru import logger
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from perfcho.api.v1.response import ResponseHandler
from perfcho.infra import logging
from perfcho.infra.settings import settings

unexpected_error_response = ResponseHandler.error("An unexpected error occurred on the server.").model_dump()


class ExceptionHandlerMiddleware:
    """Catch unexpected failures and observe complete HTTP response streams."""

    def __init__(self, app: ASGIApp) -> None:
        """Wrap one ASGI application."""
        self._app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Observe one HTTP exchange and pass through non-HTTP scopes."""
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return

        started_ns = monotonic_ns()
        correlation_id = _correlation_id(scope)
        sampling_id = str(uuid4())
        scope.setdefault("state", {})["correlation_id"] = correlation_id
        protocol, initial_operation = _request_kind(scope)
        status_code = 500
        response_started = False
        input_bytes = 0
        output_bytes = 0

        async def observed_receive() -> Message:
            nonlocal input_bytes
            message = await receive()
            if message["type"] == "http.request":
                input_bytes += len(message.get("body", b""))
            return message

        async def observed_send(message: Message) -> None:
            nonlocal output_bytes, response_started, status_code
            if message["type"] == "http.response.start":
                response_started = True
                status_code = int(message["status"])
                headers: list[tuple[bytes, bytes]] = list(message.get("headers", ()))
                headers.append((b"x-request-id", correlation_id.encode("ascii")))
                message = {**message, "headers": headers}
            elif message["type"] == "http.response.body":
                output_bytes += len(message.get("body", b""))
            await send(message)
            if message["type"] == "http.response.body" and not message.get("more_body", False):
                _log_completion(
                    scope,
                    correlation_id=correlation_id,
                    sampling_id=sampling_id,
                    protocol=protocol,
                    initial_operation=initial_operation,
                    status_code=status_code,
                    input_bytes=input_bytes,
                    output_bytes=output_bytes,
                    started_ns=started_ns,
                )

        with logger.contextualize(correlation_id=correlation_id):
            try:
                await self._app(scope, observed_receive, observed_send)
            except Exception as error:
                logging.log_event(
                    "ERROR",
                    "http.request.unhandled",
                    exception=error,
                    protocol=protocol,
                    operation=_operation(scope, initial_operation),
                    route=_route_template(scope),
                    correlation_id=correlation_id,
                    status_code=status_code if response_started else 500,
                )
                if response_started:
                    raise
                response = JSONResponse(status_code=500, content=unexpected_error_response)
                await response(scope, observed_receive, observed_send)


def add_middleware(asgi_app: FastAPI) -> None:
    """Attach request observation and unexpected-error translation."""
    asgi_app.add_middleware(ExceptionHandlerMiddleware)


def add_exception_handler(asgi_app: FastAPI) -> None:
    """Register validation and explicit HTTP exception handlers."""

    @asgi_app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
        errors = exc.errors()
        if errors:
            error = errors[0]
            field = " -> ".join(str(loc) for loc in error.get("loc", []))
            msg = f"Request validation failed: {field} - {error.get('msg', 'unknown error')}"
        else:
            msg = "Request validation failed"

        route = _route_template(request.scope)
        if logging.rate_limit(f"http-input-rejected:{route}", interval_seconds=5):
            logging.log_event(
                "INFO",
                "http.request.input_rejected",
                exception=exc,
                protocol=_request_kind(request.scope)[0],
                route=route,
                status_code=422,
                error_type="RequestValidationError",
                error_count=len(errors),
            )
        return JSONResponse(status_code=422, content=ResponseHandler.error(msg, 422).model_dump())

    @asgi_app.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
        route = _route_template(request.scope)
        if exc.status_code >= 500 or logging.rate_limit(f"http-rejected:{route}:{exc.status_code}", interval_seconds=5):
            logging.log_event(
                "INFO" if exc.status_code < 500 else "ERROR",
                "http.request.rejected",
                exception=exc,
                protocol=_request_kind(request.scope)[0],
                route=route,
                status_code=exc.status_code,
                error_type="HTTPException",
                detail_type=type(exc.detail).__name__,
            )
        return JSONResponse(
            status_code=exc.status_code,
            content=ResponseHandler.error(exc.detail, exc.status_code).model_dump(),
        )


def _correlation_id(scope: Scope) -> str:
    """Accept only UUID request IDs so arbitrary header data is never logged."""
    for name, value in scope.get("headers", ()):
        if name.lower() != b"x-request-id":
            continue
        try:
            return str(UUID(value.decode("ascii")))
        except UnicodeDecodeError, ValueError:
            break
    return str(uuid4())


def _route_template(scope: Scope) -> str:
    """Return only the framework-owned route template."""
    route = scope.get("route")
    path = getattr(route, "path", None)
    return path if isinstance(path, str) else "<unmatched>"


def _request_kind(scope: Scope) -> tuple[str, str]:
    """Classify protocol traffic without returning raw path components."""
    path = str(scope.get("path", ""))
    method = str(scope.get("method", "GET"))
    if path == "/" and method == "POST":
        has_token = any(name.lower() == b"osu-token" for name, _ in scope.get("headers", ()))
        return "stable-bancho", "poll" if has_token else "login"
    if path.startswith("/web/") or path.startswith("/d/"):
        return "stable-web", "request"
    return "http", method.lower()


def _operation(scope: Scope, fallback: str) -> str:
    route = scope.get("route")
    name = getattr(route, "name", None)
    return name if isinstance(name, str) and name.isidentifier() else fallback


def _log_completion(
    scope: Scope,
    *,
    correlation_id: str,
    sampling_id: str,
    protocol: str,
    initial_operation: str,
    status_code: int,
    input_bytes: int,
    output_bytes: int,
    started_ns: int,
) -> None:
    elapsed_ms = logging.duration_ms(started_ns)
    if not _should_log_completion(sampling_id, protocol, status_code, elapsed_ms):
        return
    logging.log_event(
        "WARNING" if elapsed_ms >= settings.log_slow_request_ms else "INFO",
        "http.request.completed",
        protocol=protocol,
        operation=_operation(scope, initial_operation),
        route=_route_template(scope),
        correlation_id=correlation_id,
        status_code=status_code,
        outcome="success" if status_code < 400 else "rejected" if status_code < 500 else "failed",
        input_bytes=input_bytes,
        output_bytes=output_bytes,
        duration_ms=elapsed_ms,
        slow=elapsed_ms >= settings.log_slow_request_ms,
    )


def _should_log_completion(sampling_id: str, protocol: str, status_code: int, elapsed_ms: float) -> bool:
    """Keep expected traffic bounded while retaining server failures and slow requests."""
    if status_code >= 500 or elapsed_ms >= settings.log_slow_request_ms:
        return True
    if protocol == "stable-bancho":
        rate = settings.log_stable_poll_sample_rate
    else:
        rate = settings.log_http_success_sample_rate
    if status_code >= 400:
        rate = max(rate, 0.1)
    return logging.sampled(sampling_id, rate)
