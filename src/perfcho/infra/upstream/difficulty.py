"""Call the versioned external difficulty-only calculator through HTTP."""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal, DecimalException
from time import monotonic_ns
from typing import cast

import httpx
import orjson

from perfcho.infra.logging import duration_ms, log_event
from perfcho.modules.common.models import JsonValue
from perfcho.modules.performance.errors import PerformanceCalculationError
from perfcho.modules.performance.models import DifficultyCalculationResult, DifficultyRequest


class HttpDifficultyCalculator:
    """Route one immutable difficulty request to its Formula-owned calculator."""

    def __init__(self, client: httpx.AsyncClient, calculator_urls: Mapping[str, str]) -> None:
        """Bind a shared HTTP client and calculator-code endpoint registry."""
        self._client = client
        self._calculator_urls = {code: url.rstrip("/") for code, url in calculator_urls.items() if url.strip()}

    async def calculate(self, request: DifficultyRequest, *, beatmap_url: str) -> DifficultyCalculationResult:
        """Send one pure request containing a short-lived immutable Beatmap URL."""
        started_ns = monotonic_ns()
        base_url = self._calculator_urls.get(request.calculator)
        if base_url is None:
            raise PerformanceCalculationError(
                f"calculator endpoint is not configured: {request.calculator}",
                retryable=False,
            )
        metadata: dict[str, object] = {
            "schema_version": 1,
            "beatmap_revision_id": request.beatmap_revision_id,
            "beatmap_sha256": request.beatmap_sha256.hex(),
            "beatmap_url": beatmap_url,
            "ruleset": request.ruleset.value,
            "difficulty_formula_code": request.difficulty_formula_code,
            "difficulty_release_version": request.difficulty_release_version,
            "mods": [mod.as_json() for mod in request.mods],
        }
        try:
            response = await self._client.post(
                f"{base_url}/v1/difficulty/attributes",
                files={
                    "metadata": (
                        "metadata.json",
                        orjson.dumps(metadata, option=orjson.OPT_SORT_KEYS),
                        "application/json",
                    )
                },
            )
        except httpx.HTTPError as error:
            log_event(
                "WARNING",
                "calculator.difficulty.request_failed",
                exception=error,
                beatmap_revision_id=request.beatmap_revision_id,
                calculator=request.calculator,
                retryable=True,
                duration_ms=duration_ms(started_ns),
            )
            raise PerformanceCalculationError("difficulty calculator request failed", retryable=True) from error
        if response.status_code >= 400:
            retryable = response.status_code == 429 or response.status_code >= 500
            raise PerformanceCalculationError(
                f"difficulty calculator returned HTTP {response.status_code}",
                retryable=retryable,
            )
        try:
            payload = response.json()
            if not isinstance(payload, dict):
                raise TypeError("response must be a JSON object")
            attributes = _mapping(payload.get("attributes"), "attributes")
            result = DifficultyCalculationResult(
                star_rating=Decimal(_text(attributes.get("star_rating"), "attributes.star_rating")),
                max_combo=_integer(attributes.get("max_combo"), "attributes.max_combo"),
                attributes=_attributes_mapping(attributes),
            )
        except (KeyError, TypeError, ValueError, DecimalException) as error:
            log_event(
                "ERROR",
                "calculator.difficulty.response_invalid",
                exception=error,
                beatmap_revision_id=request.beatmap_revision_id,
                calculator=request.calculator,
                error_type=type(error).__name__,
                duration_ms=duration_ms(started_ns),
            )
            raise PerformanceCalculationError(
                "difficulty calculator returned an invalid response", retryable=False
            ) from error
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


def _attributes_mapping(attributes: dict[str, JsonValue]) -> dict[str, JsonValue]:
    """Strip the scalar star_rating/max_combo keys to expose only skill attributes."""
    result = dict(attributes)
    result.pop("star_rating", None)
    result.pop("max_combo", None)
    return result
