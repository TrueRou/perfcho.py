"""JSON and Markdown output for benchmark results."""

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def write_report(path: Path, title: str, rows: list[dict[str, Any]]) -> None:
    """Write machine-readable JSON beside a concise Markdown report."""
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"title": title, "generated_at": datetime.now(UTC).isoformat(), "results": rows}
    path.with_suffix(".json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    lines = [
        f"# {title}",
        "",
        f"Generated: `{payload['generated_at']}`",
        "",
        "| Scenario | C | QPS | P50 | P95 | P99 | Errors |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    lines.extend(
        f"| {row['name']} | {row['concurrency']} | {row['qps']:.1f} | "
        f"{row['p50_ms']:.2f} ms | {row['p95_ms']:.2f} ms | "
        f"{row['p99_ms']:.2f} ms | {row['errors']} |"
        for row in rows
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
