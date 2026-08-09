"""Small domain-neutral contracts used by benchmarks."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, slots=True)
class BenchmarkConfig:
    """Describe one load phase."""

    warmup_seconds: float
    duration_seconds: float
    concurrency: int


@dataclass(frozen=True, slots=True)
class BenchmarkResult:
    """Aggregated latency and throughput for one scenario."""

    name: str
    concurrency: int
    requests: int
    errors: int
    elapsed_seconds: float
    latencies_ms: tuple[float, ...]


class BenchmarkScenario(Protocol):
    """Provide an independently setup and executable workload."""

    name: str

    async def setup(self) -> None:
        """Prepare resources required by the workload."""
        ...

    async def run(self) -> object:
        """Execute one request and return an arbitrary result."""
        ...

    async def teardown(self) -> None:
        """Release scenario-owned resources."""
        ...


ScenarioFactory = Callable[[BenchmarkConfig], BenchmarkScenario | Awaitable[BenchmarkScenario]]
