"""Provide infrastructure adapters for deferred score calculation."""

from perfcho.modules.scoring.models import PerformanceCalculationInput, PerformanceResult


class DeferredPerformanceCalculator:
    """Defer PP until a worker can read the beatmap object outside the request transaction."""

    async def calculate(self, calculation: PerformanceCalculationInput) -> PerformanceResult | None:
        """Return no synchronous result while preserving the explicit calculator Port."""
        del calculation
        return None
