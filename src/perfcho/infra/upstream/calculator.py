"""Call versioned external Performance calculators through HTTP."""

import json
from collections.abc import Mapping
from decimal import Decimal, DecimalException
from typing import cast

import httpx

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
        base_url = self._calculator_urls.get(calculation.calculator)
        if base_url is None:
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
            raise PerformanceCalculationError("calculator request failed", retryable=True) from error
        if response.status_code >= 400:
            retryable = response.status_code == 429 or response.status_code >= 500
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
            _expect(payload, "artifact_digest", calculation.artifact_digest.hex())
            _expect(payload, "difficulty_release_version", calculation.difficulty_release_version)
            _expect(payload, "difficulty_artifact_digest", calculation.difficulty_artifact_digest.hex())
            _expect(payload, "input_digest", calculation.input_digest.hex())
            difficulty = _mapping(payload.get("difficulty"), "difficulty")
            performance = _mapping(payload.get("performance"), "performance")
            attributes = _mapping(difficulty.get("attributes", {}), "difficulty.attributes")
            breakdown = _mapping(performance.get("breakdown", {}), "performance.breakdown")
            return PerformanceResult(
                pp=Decimal(_text(performance.get("pp"), "performance.pp")),
                difficulty=DifficultyCalculationResult(
                    star_rating=Decimal(_text(difficulty.get("star_rating"), "difficulty.star_rating")),
                    max_combo=_integer(difficulty.get("max_combo"), "difficulty.max_combo"),
                    attributes=attributes,
                ),
                breakdown=breakdown,
            )
        except (KeyError, TypeError, ValueError, DecimalException) as error:
            raise PerformanceCalculationError("calculator returned an invalid response", retryable=False) from error


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
