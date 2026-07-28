"""Bridge standard-library and Uvicorn logs into structured Loguru output."""

import logging
import sys
from collections.abc import Callable
from typing import TYPE_CHECKING

from loguru import logger

from perfcho.infra.settings import settings

if TYPE_CHECKING:
    from loguru import Record


def source(name: str = "", function: str = "") -> Callable[[Record], None]:
    """Return a Loguru patcher that overrides the displayed source."""

    def _source(record: Record) -> None:
        record["name"] = name
        record["function"] = function
        record["line"] = ""  # type: ignore

    return _source


class InterceptHandler(logging.Handler):
    """Forward standard-library log records to Loguru."""

    def emit(self, record: logging.LogRecord) -> None:
        """Translate and emit one standard-library log record."""
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        frame, depth = logging.currentframe(), 2
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        logger.patch(source()).opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


def init_logger() -> None:
    """Configure process logging and intercept Uvicorn loggers."""
    logger.remove()

    log_level = settings.log_level.upper()
    logger.add(sys.stdout, backtrace=False, diagnose=False, colorize=True, enqueue=True, level=log_level)

    loggers = (logging.getLogger(name) for name in logging.root.manager.loggerDict if name.startswith("uvicorn."))
    for uvicorn_logger in loggers:
        uvicorn_logger.handlers = []

    intercept_handler = InterceptHandler()
    logging.getLogger("uvicorn").handlers = [intercept_handler]
