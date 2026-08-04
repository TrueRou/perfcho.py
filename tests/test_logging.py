import json
import logging
import os
import sys
from collections.abc import AsyncIterator
from io import StringIO

import httpx
import pytest
from loguru import logger
from starlette.responses import StreamingResponse

from perfcho.api.v1.middleware.error import ExceptionHandlerMiddleware
from perfcho.infra.logging import init_logger, log_event, reset_relay_task, set_relay_task
from perfcho.infra.settings import settings


def test_human_output_hides_event_and_library_from_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    stream = StringIO()
    monkeypatch.setattr(settings, "log_format", "human")

    try:
        init_logger("test", stream=stream)
        logger.bind(event="test.event", library="test.library", request_id="request-1").info("message")
        logger.complete()
    finally:
        logger.remove()
        logger.configure(extra={}, patcher=None)
        logger.add(sys.stderr)

    output = stream.getvalue()
    assert "message |" in output
    assert "'request_id': 'request-1'" in output
    assert "test.event" not in output
    assert "test.library" not in output


def test_human_output_keeps_standard_log_message_in_msg(monkeypatch: pytest.MonkeyPatch) -> None:
    stream = StringIO()
    monkeypatch.setattr(settings, "log_format", "human")

    try:
        init_logger("test", stream=stream)
        logging.getLogger("test.library").warning("normal log message")
        logger.complete()
    finally:
        logger.remove()
        logger.configure(extra={}, patcher=None)
        logger.add(sys.stderr)

    output = stream.getvalue()
    assert "library.log | {'msg': 'normal log message'}" in output
    assert "test.library" not in output


def test_worker_human_output_includes_pid_and_suppresses_empty_fetch_poll(monkeypatch: pytest.MonkeyPatch) -> None:
    stream = StringIO()
    monkeypatch.setattr(settings, "log_format", "human")
    monkeypatch.setattr(settings, "log_level", "DEBUG")

    try:
        init_logger("worker", stream=stream)
        log_event("INFO", "worker.test")
        taskiq_logger = logging.getLogger("taskiq.redis_broker")
        taskiq_logger.debug("Starting fetching new messages")
        taskiq_logger.debug("Redis broker detail")
        logger.complete()
    finally:
        logger.remove()
        logger.configure(extra={}, patcher=None)
        logger.add(sys.stderr)

    output = stream.getvalue()
    assert f"| worker[{os.getpid()}] | worker.test | {{}}" in output
    assert "Starting fetching new messages" not in output
    assert "library.log | {'msg': 'Redis broker detail'}" in output


def test_worker_task_execution_log_does_not_expose_task_id(monkeypatch: pytest.MonkeyPatch) -> None:
    stream = StringIO()
    monkeypatch.setattr(settings, "log_format", "human")
    monkeypatch.setattr(settings, "log_level", "DEBUG")
    token = set_relay_task("perfcho.outbox.dispatch")

    try:
        init_logger("worker", stream=stream)
        logging.getLogger("taskiq.worker").info(
            "Executing task perfcho.outbox.dispatch with ID: sensitive-task-id",
        )
        logging.getLogger("taskiq.redis_broker").debug("Received message: sensitive-task-id")
        logger.complete()
    finally:
        reset_relay_task(token)
        logger.remove()
        logger.configure(extra={}, patcher=None)
        logger.add(sys.stderr)

    output = stream.getvalue()
    assert "Executing task perfcho.outbox.dispatch" not in output
    assert "sensitive-task-id" not in output


def test_json_output_keeps_event_and_library_in_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    stream = StringIO()
    monkeypatch.setattr(settings, "log_format", "json")

    try:
        init_logger("test", stream=stream)
        logger.bind(event="test.event", library="test.library", request_id="request-1").info("message")
        logger.complete()
    finally:
        logger.remove()
        logger.configure(extra={}, patcher=None)
        logger.add(sys.stderr)

    payload = json.loads(stream.getvalue())
    assert payload["event"] == "test.event"
    assert payload["library"] == "test.library"
    assert payload["request_id"] == "request-1"


def test_json_event_is_allow_listed_and_includes_full_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    stream = StringIO()
    monkeypatch.setattr(settings, "log_format", "json")
    try:
        init_logger("test", stream=stream)
        try:
            raise RuntimeError("database connection failed after 30 seconds")
        except RuntimeError as error:
            log_event(
                "ERROR",
                "test.failure",
                exception=error,
                account_id=42,
                secret="should-not-appear",
                message="should-not-appear",
            )
        logger.complete()
    finally:
        logger.remove()
        logger.configure(extra={}, patcher=None)
        logger.add(sys.stderr)

    output = stream.getvalue()
    payload = json.loads(output)
    assert payload["event"] == "test.failure"
    assert payload["service"] == "perfcho"
    assert payload["process_role"] == "test"
    assert payload["account_id"] == 42
    assert payload["error_type"] == "RuntimeError"
    assert payload["exception"]["type"] == "RuntimeError"
    assert payload["exception"]["message"] == "database connection failed after 30 seconds"
    assert "raise RuntimeError" in payload["exception"]["traceback"]
    assert "database connection failed after 30 seconds" in payload["exception"]["traceback"]
    assert "should-not-appear" not in output


def test_human_event_includes_full_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    stream = StringIO()
    monkeypatch.setattr(settings, "log_format", "human")
    try:
        init_logger("test", stream=stream)
        try:
            raise ValueError("invalid calculation response")
        except ValueError as error:
            log_event("ERROR", "test.failure", exception=error)
        logger.complete()
    finally:
        logger.remove()
        logger.configure(extra={}, patcher=None)
        logger.add(sys.stderr)

    output = stream.getvalue()
    assert "Traceback (most recent call last)" in output
    assert 'raise ValueError("invalid calculation response")' in output
    assert "ValueError: invalid calculation response" in output


@pytest.mark.asyncio
async def test_http_completion_is_emitted_after_stream_finishes(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[str] = []

    def capture(level: str | int, event: str, **fields: object) -> None:
        del level, fields
        events.append(event)

    import perfcho.api.v1.middleware.error as error_module

    monkeypatch.setattr(error_module.logging, "log_event", capture)
    monkeypatch.setattr(error_module.settings, "log_http_success_sample_rate", 1.0)

    async def body() -> AsyncIterator[bytes]:
        yield b"first"
        yield b"second"

    async def app(scope: dict[str, object], receive: object, send: object) -> None:
        await StreamingResponse(body())(scope, receive, send)  # type: ignore[arg-type]

    wrapped = ExceptionHandlerMiddleware(app)  # type: ignore[arg-type]
    transport = httpx.ASGITransport(app=wrapped)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/safe")

    assert response.status_code == 200
    assert response.content == b"firstsecond"
    assert events[-1] == "http.request.completed"
