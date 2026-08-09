"""Latency aggregation with no dependency on a particular load generator."""

from dataclasses import asdict
from math import ceil
from statistics import mean
from typing import Any

from .models import BenchmarkResult


def percentile(values: tuple[float, ...], fraction: float) -> float:
    """Return the nearest-rank percentile in milliseconds."""
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, max(0, ceil(len(ordered) * fraction) - 1))]


def summarize(result: BenchmarkResult) -> dict[str, Any]:
    """Convert raw measurements into stable report fields."""
    latencies = result.latencies_ms
    return {
        **asdict(result),
        "latencies_ms": None,
        "qps": result.requests / result.elapsed_seconds if result.elapsed_seconds else 0.0,
        "error_rate": result.errors / result.requests if result.requests else 0.0,
        "p50_ms": percentile(latencies, 0.50),
        "p90_ms": percentile(latencies, 0.90),
        "p95_ms": percentile(latencies, 0.95),
        "p99_ms": percentile(latencies, 0.99),
        "min_ms": min(latencies, default=0.0),
        "max_ms": max(latencies, default=0.0),
        "mean_ms": mean(latencies) if latencies else 0.0,
    }
