"""Call versioned external Performance calculators through HTTP."""

import json
from collections.abc import Mapping
from decimal import Decimal, DecimalException
from time import monotonic_ns
from typing import cast

import httpx

from perfcho.infra.logging import duration_ms, log_event
from perfcho.modules.common.models import JsonValue
from perfcho.modules.performance.errors import PerformanceCalculationError
from perfcho.modules.performance.models import (
    DifficultyCalculationResult,
    PerformanceCalculationInput,
    PerformanceResult,
)


class HttpPerformanceCalculator:
    """Route immutable calculation input to its Formula-owned calculator."""

    def __init__(self, client: httpx.AsyncClient, calculator_urls: Mapping[str, str]) -> None:
        """Bind a shared HTTP client and calculator-code endpoint registry."""
        self._client = client
        self._calculator_urls = {code: url.rstrip("/") for code, url in calculator_urls.items() if url.strip()}

    async def calculate(
        self,
        calculation: PerformanceCalculationInput,
        *,
        beatmap_url: str,
    ) -> PerformanceResult:
        """Send one pure request containing a short-lived immutable Beatmap URL."""
        started_ns = monotonic_ns()
        base_url = self._calculator_urls.get(calculation.calculator)
        if base_url is None:
            log_event(
                "ERROR",
                "calculator.request.rejected",
                job_id=str(calculation.job_id),
                calculator=calculation.calculator,
                release_version=calculation.release_version,
                reason="endpoint_not_configured",
            )
            raise PerformanceCalculationError(
                f"calculator endpoint is not configured: {calculation.calculator}",
                retryable=False,
            )
        metadata = calculation.digest_payload()
        metadata.update(
            {
                "job_id": str(calculation.job_id),
                "score_id": calculation.score_id,
                "input_digest": calculation.input_digest.hex(),
                "beatmap_url": beatmap_url,
            }
        )
        try:
            response = await self._client.post(
                f"{base_url}/v1/performance/calculate",
                files={
                    "metadata": (
                        "metadata.json",
                        json.dumps(metadata, sort_keys=True, separators=(",", ":")),
                        "application/json",
                    )
                },
            )
        except httpx.HTTPError as error:
            log_event(
                "WARNING",
                "calculator.request.failed",
                exception=error,
                job_id=str(calculation.job_id),
                calculator=calculation.calculator,
                release_version=calculation.release_version,
                error_type=type(error).__name__,
                retryable=True,
                duration_ms=duration_ms(started_ns),
            )
            raise PerformanceCalculationError("calculator request failed", retryable=True) from error
        if response.status_code >= 400:
            retryable = response.status_code == 429 or response.status_code >= 500
            log_event(
                "WARNING" if retryable else "ERROR",
                "calculator.request.failed",
                job_id=str(calculation.job_id),
                calculator=calculation.calculator,
                release_version=calculation.release_version,
                status_code=response.status_code,
                retryable=retryable,
                duration_ms=duration_ms(started_ns),
            )
            raise PerformanceCalculationError(
                f"calculator returned HTTP {response.status_code}",
                retryable=retryable,
            )
        try:
            payload = response.json()
            if not isinstance(payload, dict):
                raise TypeError("response must be a JSON object")
            _expect(payload, "schema_version", 1)
            _expect(payload, "calculator", calculation.calculator)
            _expect(payload, "release_version", calculation.release_version)
            _expect(payload, "difficulty_release_version", calculation.difficulty_release_version)
            _expect(payload, "input_digest", calculation.input_digest.hex())
            difficulty = _mapping(payload.get("difficulty"), "difficulty")
            performance = _mapping(payload.get("performance"), "performance")
            attributes = _mapping(difficulty.get("attributes", {}), "difficulty.attributes")
            breakdown = _mapping(performance.get("breakdown", {}), "performance.breakdown")
            result = PerformanceResult(
                pp=Decimal(_text(performance.get("pp"), "performance.pp")),
                difficulty=DifficultyCalculationResult(
                    star_rating=Decimal(_text(difficulty.get("star_rating"), "difficulty.star_rating")),
                    max_combo=_integer(difficulty.get("max_combo"), "difficulty.max_combo"),
                    attributes=attributes,
                ),
                breakdown=breakdown,
            )
        except (KeyError, TypeError, ValueError, DecimalException) as error:
            log_event(
                "ERROR",
                "calculator.response.invalid",
                exception=error,
                job_id=str(calculation.job_id),
                calculator=calculation.calculator,
                release_version=calculation.release_version,
                error_type=type(error).__name__,
                duration_ms=duration_ms(started_ns),
            )
            raise PerformanceCalculationError("calculator returned an invalid response", retryable=False) from error
        log_event(
            "DEBUG",
            "calculator.request.completed",
            job_id=str(calculation.job_id),
            calculator=calculation.calculator,
            release_version=calculation.release_version,
            status_code=response.status_code,
            duration_ms=duration_ms(started_ns),
        )
        return result


def _mapping(value: object, name: str) -> dict[str, JsonValue]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise TypeError(f"{name} must be a JSON object")
    return cast(dict[str, JsonValue], value)


def _text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise TypeError(f"{name} must be a non-empty string")
    return value


def _integer(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    return value


def _expect(payload: Mapping[str, object], key: str, expected: object) -> None:
    if payload.get(key) != expected:
        raise ValueError(f"calculator response {key} does not match the requested release")
