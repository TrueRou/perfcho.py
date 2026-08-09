from __future__ import annotations

import logging
import os
import sys
from io import StringIO
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest
from loguru import logger

from perfcho.infra import logging as logging_module
from perfcho.infra.logging import (
    init_logger,
    log_event,
    reset_relay_task,
    set_relay_delay_ms,
    set_relay_event_type,
    set_relay_task,
)
from perfcho.infra.settings import Settings, settings
from perfcho.infra.tracing import trace_context

if TYPE_CHECKING:
    from loguru import Message


def _restore_logger() -> None:
    logger.remove()
    logger.configure(extra={}, patcher=None)
    logger.add(sys.stderr)


def test_console_is_human_with_trace_and_without_structured_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    stream = StringIO()
    monkeypatch.setattr(settings, "loki_url", None)

    try:
        init_logger("test", stream=stream)
        with trace_context("0123456789abcdef0123456789abcdef"):
            log_event(
                "INFO",
                "test.event",
                request_id="request-1",
                secret="visible-secret",
                duration_ms=12.5,
            )
        logger.complete()
    finally:
        _restore_logger()

    output = stream.getvalue()
    assert "| test[" in output
    assert "0123456789abcdef0123456789abcdef | test.event" in output
    assert output.rstrip().endswith("test.event | duration=12.5ms")
    assert "request_id" not in output
    assert "visible-secret" not in output
    assert not output.lstrip().startswith("{")


def test_log_format_environment_setting_is_ignored() -> None:
    configured = Settings.model_validate({"LOG_FORMAT": "json"})

    assert not hasattr(configured, "log_format")


def test_human_output_keeps_standard_log_message(monkeypatch: pytest.MonkeyPatch) -> None:
    stream = StringIO()
    monkeypatch.setattr(settings, "loki_url", None)

    try:
        init_logger("test", stream=stream)
        logging.getLogger("test.library").warning("normal log message")
        logger.complete()
    finally:
        _restore_logger()

    output = stream.getvalue()
    assert "- | normal log message" in output
    assert "library.log" not in output
    assert "test.library" not in output


def test_worker_human_output_includes_pid_and_suppresses_empty_fetch_poll(monkeypatch: pytest.MonkeyPatch) -> None:
    stream = StringIO()
    monkeypatch.setattr(settings, "loki_url", None)
    monkeypatch.setattr(settings, "log_level", "DEBUG")

    try:
        init_logger("worker", stream=stream)
        set_relay_event_type("score.accepted.v1")
        set_relay_delay_ms(1004.25)
        log_event("INFO", "worker.test", duration_ms=5.5)
        taskiq_logger = logging.getLogger("taskiq.redis_broker")
        taskiq_logger.debug("Starting fetching new messages")
        taskiq_logger.debug("Redis broker detail")
        logger.complete()
    finally:
        set_relay_event_type(None)
        set_relay_delay_ms(None)
        _restore_logger()

    output = stream.getvalue()
    assert f"| worker[{os.getpid()}] | - | worker.test" in output
    assert "worker.test | event_type=score.accepted.v1 | duration=5.5ms | delay=1004.25ms" in output
    assert "Starting fetching new messages" not in output
    assert "Redis broker detail" in output


def test_worker_task_execution_noise_remains_suppressed(monkeypatch: pytest.MonkeyPatch) -> None:
    stream = StringIO()
    monkeypatch.setattr(settings, "loki_url", None)
    monkeypatch.setattr(settings, "log_level", "DEBUG")
    token = set_relay_task("perfcho.outbox.dispatch")

    try:
        init_logger("worker", stream=stream)
        logging.getLogger("taskiq.receiver.receiver").info(
            "Executing task perfcho.outbox.dispatch with ID: task-id",
        )
        logging.getLogger("taskiq.redis_broker").debug("Received message: task-id")
        logger.complete()
    finally:
        reset_relay_task(token)
        _restore_logger()

    output = stream.getvalue()
    assert "Executing task perfcho.outbox.dispatch" not in output
    assert "Received message: task-id" not in output


def test_loki_sink_receives_complete_structured_record(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeLokiHandler:
        def __init__(self) -> None:
            self.messages: list[Message] = []

        def write(self, message: Message) -> None:
            self.messages.append(message)

    stream = StringIO()
    handler = FakeLokiHandler()
    handler_type = MagicMock(return_value=handler)
    monkeypatch.setattr(settings, "loki_url", "http://loki:3100/loki/api/v1/push")
    monkeypatch.setattr(settings, "loki_environment", "test")
    monkeypatch.setattr(logging_module, "LokiLoggerHandler", handler_type)

    try:
        init_logger("test", stream=stream)
        with trace_context("0123456789abcdef0123456789abcdef"):
            log_event("INFO", "test.loki", request_id="request-1", secret="visible-secret")
        logger.complete()
    finally:
        _restore_logger()

    handler_type.assert_called_once()
    assert handler_type.call_args.kwargs["labels"] == {
        "application": "perfcho",
        "environment": "test",
        "process_role": "test",
    }
    assert handler_type.call_args.kwargs["enable_structured_loki_metadata"] is True
    assert "trace_id" in handler_type.call_args.kwargs["loki_metadata_keys"]
    assert "module" in handler_type.call_args.kwargs["loki_metadata_keys"]
    message = handler.messages[0]
    assert message.record["extra"]["event"] == "test.loki"
    assert message.record["extra"]["request_id"] == "request-1"
    assert message.record["extra"]["secret"] == "visible-secret"
    assert message.record["extra"]["trace_id"] == "0123456789abcdef0123456789abcdef"
    assert "test.loki" in stream.getvalue()


def test_human_event_includes_full_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    stream = StringIO()
    monkeypatch.setattr(settings, "loki_url", None)

    try:
        init_logger("test", stream=stream)
        try:
            raise ValueError("invalid calculation response")
        except ValueError as error:
            log_event("ERROR", "test.failure", exception=error)
        logger.complete()
    finally:
        _restore_logger()

    output = stream.getvalue()
    assert "Traceback (most recent call last)" in output
    assert 'raise ValueError("invalid calculation response")' in output
    assert "ValueError: invalid calculation response" in output
