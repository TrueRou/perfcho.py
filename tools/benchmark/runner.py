"""Run bounded concurrent benchmark workloads."""

import asyncio
from time import monotonic, monotonic_ns

from .models import BenchmarkConfig, BenchmarkResult, BenchmarkScenario


async def run_benchmark(scenario: BenchmarkScenario, config: BenchmarkConfig) -> BenchmarkResult:
    """Warm up and run a scenario until the configured deadline."""
    if config.concurrency < 1 or config.duration_seconds <= 0 or config.warmup_seconds < 0:
        raise ValueError("benchmark timing and concurrency values are invalid")
    await scenario.setup()
    try:
        await _run_phase(scenario, config.concurrency, config.warmup_seconds, collect=False)
        started = monotonic()
        latencies, errors = await _run_phase(scenario, config.concurrency, config.duration_seconds, collect=True)
        elapsed = monotonic() - started
        return BenchmarkResult(
            scenario.name,
            config.concurrency,
            len(latencies) + errors,
            errors,
            elapsed,
            tuple(latencies),
        )
    finally:
        await scenario.teardown()


async def _run_phase(
    scenario: BenchmarkScenario,
    concurrency: int,
    duration_seconds: float,
    *,
    collect: bool,
) -> tuple[list[float], int]:
    if duration_seconds == 0:
        return [], 0
    deadline = monotonic() + duration_seconds
    latencies: list[float] = []
    errors = 0
    lock = asyncio.Lock()

    async def worker() -> None:
        nonlocal errors
        while monotonic() < deadline:
            started_ns = monotonic_ns()
            try:
                await scenario.run()
            except Exception:
                async with lock:
                    errors += 1
            else:
                if collect:
                    latency = (monotonic_ns() - started_ns) / 1_000_000
                    async with lock:
                        latencies.append(latency)

    async with asyncio.TaskGroup() as group:
        for _ in range(concurrency):
            group.create_task(worker())
    return latencies, errors
