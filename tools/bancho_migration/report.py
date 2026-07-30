"""Collect and persist machine-readable migration diagnostics."""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from tools.bancho_migration.models import Diagnostic, DiagnosticSeverity, MigrationStatus


@dataclass(frozen=True, slots=True)
class ReportSnapshot:
    """Capture report mutations that must agree with a database transaction."""

    counters: dict[str, dict[str, int]]
    diagnostics: tuple[Diagnostic, ...]
    diagnostic_counts: dict[str, int]


@dataclass(slots=True)
class MigrationReport:
    """Accumulate phase counters and bounded diagnostics for one run."""

    migration_id: str
    source_fingerprint: str | None = None
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    completed_at: datetime | None = None
    counters: dict[str, dict[str, int]] = field(default_factory=dict)
    diagnostics: list[Diagnostic] = field(default_factory=list)
    diagnostic_limit: int = 10_000
    invocation_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    status: MigrationStatus = MigrationStatus.RUNNING
    diagnostic_counts: dict[str, int] = field(
        default_factory=lambda: {severity.value: 0 for severity in DiagnosticSeverity}
    )

    def increment(self, phase: str, outcome: str, count: int = 1) -> None:
        """Increment a named phase outcome counter."""
        phase_counts = self.counters.setdefault(phase, {})
        phase_counts[outcome] = phase_counts.get(outcome, 0) + count

    def add(
        self,
        severity: DiagnosticSeverity,
        code: str,
        message: str,
        *,
        entity: str | None = None,
        source_id: object | None = None,
        details: dict[str, object] | None = None,
    ) -> None:
        """Append one diagnostic while bounding report memory usage."""
        self.diagnostic_counts[severity.value] = self.diagnostic_counts.get(severity.value, 0) + 1
        if len(self.diagnostics) >= self.diagnostic_limit:
            self.increment("report", "diagnostics_dropped")
            return
        self.diagnostics.append(
            Diagnostic(
                severity=severity,
                code=code,
                message=message,
                entity=entity,
                source_id=str(source_id) if source_id is not None else None,
                details=details or {},
            )
        )

    @property
    def has_errors(self) -> bool:
        """Return whether any fatal preflight or row diagnostic exists."""
        return self.diagnostic_counts.get(DiagnosticSeverity.ERROR.value, 0) > 0

    def snapshot(self) -> ReportSnapshot:
        """Return a copy suitable for restoring after transaction rollback."""
        return ReportSnapshot(
            counters={phase: dict(outcomes) for phase, outcomes in self.counters.items()},
            diagnostics=tuple(self.diagnostics),
            diagnostic_counts=dict(self.diagnostic_counts),
        )

    def restore(self, snapshot: ReportSnapshot) -> None:
        """Restore counters and diagnostics to their pre-transaction values."""
        self.counters = {phase: dict(outcomes) for phase, outcomes in snapshot.counters.items()}
        self.diagnostics = list(snapshot.diagnostics)
        self.diagnostic_counts = dict(snapshot.diagnostic_counts)

    def finish(self, status: MigrationStatus | None = None) -> None:
        """Mark the report complete using an aware UTC timestamp and outcome."""
        self.status = status or (MigrationStatus.FAILED if self.has_errors else MigrationStatus.COMPLETED)
        self.completed_at = datetime.now(UTC)

    def write(self, path: Path) -> None:
        """Atomically replace the JSON report at the requested path."""
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(f"{path.suffix}.tmp")
        payload = asdict(self)
        payload["status"] = self.status.value
        payload["started_at"] = self.started_at.isoformat()
        payload["completed_at"] = self.completed_at.isoformat() if self.completed_at is not None else None
        payload["diagnostics"] = [{**asdict(item), "severity": item.severity.value} for item in self.diagnostics]
        encoded = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(encoded)
        temporary.replace(path)
        path.chmod(0o600)
