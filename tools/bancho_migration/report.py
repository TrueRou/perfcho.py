"""Collect and persist machine-readable migration diagnostics."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from tools.bancho_migration.models import Diagnostic, DiagnosticSeverity


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
        return any(item.severity is DiagnosticSeverity.ERROR for item in self.diagnostics)

    def finish(self) -> None:
        """Mark the report complete using an aware UTC timestamp."""
        self.completed_at = datetime.now(UTC)

    def write(self, path: Path) -> None:
        """Atomically replace the JSON report at the requested path."""
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(f"{path.suffix}.tmp")
        payload = asdict(self)
        payload["started_at"] = self.started_at.isoformat()
        payload["completed_at"] = self.completed_at.isoformat() if self.completed_at is not None else None
        payload["diagnostics"] = [{**asdict(item), "severity": item.severity.value} for item in self.diagnostics]
        temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
        temporary.replace(path)
