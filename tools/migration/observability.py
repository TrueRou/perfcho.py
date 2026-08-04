"""Emit allow-listed structured events for migration operations."""

from __future__ import annotations

import time
from types import TracebackType
from typing import TypedDict

from perfcho.infra.logging import duration_ms, log_event, rate_limit
from tools.migration.models import MigrationRuntime
from tools.migration.report import MigrationReport


class MigrationEventFields(TypedDict):
    """Identifiers shared by migration events."""

    invocation_id: str
    migration_id: str


def event_fields(report: MigrationReport) -> MigrationEventFields:
    """Return identifiers approved for every migration event."""
    return {
        "invocation_id": report.invocation_id,
        "migration_id": report.migration_id,
    }


class PhaseObserver:
    """Track one phase without exposing source rows, cursors, or diagnostics."""

    def __init__(self, runtime: MigrationRuntime, phase: str) -> None:
        """Bind a phase to the invocation report."""
        self._report = runtime.report
        self._phase = phase
        self._started_ns = 0
        self._batches_committed = 0
        self._rows_committed = 0
        self._skipped = False

    def __enter__(self) -> PhaseObserver:
        """Start phase timing and emit its lifecycle event."""
        self._started_ns = time.monotonic_ns()
        log_event("INFO", "migration.phase.started", phase=self._phase, **event_fields(self._report))
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Emit the terminal phase event without suppressing failures."""
        del exc_type, traceback
        if exc_value is not None:
            log_event(
                "WARNING" if isinstance(exc_value, KeyboardInterrupt) else "ERROR",
                "migration.phase.failed",
                exception=exc_value,
                phase=self._phase,
                error_type=type(exc_value).__name__,
                duration_ms=duration_ms(self._started_ns),
                batches_committed=self._batches_committed,
                rows_committed=self._rows_committed,
                **event_fields(self._report),
            )
        elif not self._skipped:
            log_event(
                "INFO",
                "migration.phase.completed",
                phase=self._phase,
                duration_ms=duration_ms(self._started_ns),
                batches_committed=self._batches_committed,
                rows_committed=self._rows_committed,
                **event_fields(self._report),
            )

    def skipped(self) -> None:
        """Record a phase omitted because its checkpoint is already complete."""
        self._skipped = True
        log_event(
            "INFO",
            "migration.phase.skipped",
            phase=self._phase,
            reason="checkpoint_complete",
            duration_ms=duration_ms(self._started_ns),
            **event_fields(self._report),
        )

    def batch_committed(self, row_count: int) -> None:
        """Emit every commit at DEBUG and bounded aggregate progress at INFO."""
        self._batches_committed += 1
        self._rows_committed += row_count
        fields = {
            "phase": self._phase,
            "batch_rows": row_count,
            "batches_committed": self._batches_committed,
            "rows_committed": self._rows_committed,
            "duration_ms": duration_ms(self._started_ns),
            **event_fields(self._report),
        }
        log_event("DEBUG", "migration.batch.committed", **fields)
        progress_allowed = rate_limit(
            f"migration-progress:{self._report.invocation_id}:{self._phase}",
            interval_seconds=30.0,
        )
        if self._batches_committed == 1 or progress_allowed:
            log_event("INFO", "migration.phase.progress", **fields)


class VerificationObserver:
    """Summarize centralized verification checks without row-level logging."""

    def __init__(self, report: MigrationReport, verification: str) -> None:
        """Bind a verification scope to the invocation report."""
        self._report = report
        self._verification = verification
        self._started_ns = 0
        self._checks = 0
        self._failed_checks = 0

    def __enter__(self) -> VerificationObserver:
        """Start verification timing and emit its lifecycle event."""
        self._started_ns = time.monotonic_ns()
        log_event(
            "INFO",
            "migration.verification.started",
            verification=self._verification,
            **event_fields(self._report),
        )
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Emit the terminal verification event without suppressing failures."""
        del exc_type, traceback
        if exc_value is not None:
            log_event(
                "WARNING" if isinstance(exc_value, KeyboardInterrupt) else "ERROR",
                "migration.verification.failed",
                exception=exc_value,
                verification=self._verification,
                error_type=type(exc_value).__name__,
                checks=self._checks,
                failed_checks=self._failed_checks,
                duration_ms=duration_ms(self._started_ns),
                **event_fields(self._report),
            )
            return
        log_event(
            "INFO",
            "migration.verification.completed",
            verification=self._verification,
            checks=self._checks,
            failed_checks=self._failed_checks,
            duration_ms=duration_ms(self._started_ns),
            **event_fields(self._report),
        )

    def check(self, check: str, *, failures: int = 0, checked: int | None = None) -> None:
        """Emit one aggregate check result using only bounded counts."""
        self._checks += 1
        self._failed_checks += int(failures > 0)
        fields = event_fields(self._report)
        log_event(
            "WARNING" if failures else "INFO",
            "migration.verification.check_completed",
            verification=self._verification,
            check=check,
            status="failed" if failures else "completed",
            failures=failures,
            checked=checked,
            **fields,
        )
