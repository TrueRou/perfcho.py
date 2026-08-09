import asyncio
import json
from pathlib import Path

import pytest

from tools.benchmark.metrics import percentile, summarize
from tools.benchmark.models import BenchmarkConfig, BenchmarkResult
from tools.benchmark.report import write_report
from tools.benchmark.runner import run_benchmark


class CountingScenario:
    name = "counting"

    def __init__(self) -> None:
        self.calls = 0

    async def setup(self) -> None:
        return None

    async def run(self) -> None:
        self.calls += 1
        await asyncio.sleep(0)
        if self.calls % 5 == 0:
            raise RuntimeError("synthetic failure")

    async def teardown(self) -> None:
        return None


def test_percentile_and_summary_are_stable() -> None:
    result = BenchmarkResult("test", 2, 4, 1, 1.0, (1.0, 2.0, 3.0))

    assert percentile(result.latencies_ms, 0.50) == 2.0
    summary = summarize(result)
    assert summary["p95_ms"] == 3.0
    assert summary["error_rate"] == 0.25


@pytest.mark.asyncio
async def test_runner_counts_errors_without_stopping_workers() -> None:
    scenario = CountingScenario()

    result = await run_benchmark(scenario, BenchmarkConfig(0, 0.02, 3))

    assert result.requests > 0
    assert result.errors > 0
    assert result.requests == len(result.latencies_ms) + result.errors


def test_report_writes_json_and_markdown(tmp_path: Path) -> None:
    result = BenchmarkResult("test", 1, 1, 0, 1.0, (2.0,))

    write_report(tmp_path / "report.md", "Performance", [summarize(result)])

    payload = json.loads((tmp_path / "report.json").read_text())
    assert payload["results"][0]["name"] == "test"
    assert "| test |" in (tmp_path / "report.md").read_text()
