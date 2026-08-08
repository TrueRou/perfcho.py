from decimal import Decimal
from typing import Any

import httpx
import pytest

from perfcho.infra.upstream.calculator import HttpPerformanceCalculator
from perfcho.modules.performance.errors import PerformanceCalculationError
from perfcho.modules.performance.models import (
    PerformanceCalculationInput,
)
from perfcho.modules.scoring.models import (
    ClientFamily,
    HitStatistic,
    Ruleset,
    ScoreboardInfo,
    ScoreboardVariant,
    ScoreGrade,
    ScoreOutcome,
    ScoreSubmission,
)


def test_performance_input_has_no_job_identity() -> None:
    calculation = _calculation()
    assert not hasattr(calculation, "job_id")
    assert not hasattr(calculation, "attempt_count")


@pytest.mark.asyncio
async def test_http_calculator_sends_score_projection_metadata_and_verifies_release() -> None:
    calculation = _calculation()
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = request.content
        return httpx.Response(
            200,
            json={
                "schema_version": 1,
                "calculator": "perfcho-pp",
                "release_version": "2026.07.1",
                "difficulty_release_version": "2026.07.1",
                "input_digest": (b"i" * 32).hex(),
                "difficulty": {"star_rating": "6.543219", "max_combo": 1234, "attributes": {"aim": 3.2}},
                "performance": {"pp": "321.123456", "breakdown": {"aim": 200}},
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await HttpPerformanceCalculator(client, {"perfcho-pp": "http://calculator.test/"}).calculate(
            calculation,
            beatmap_url="http://s3.test/map.osu",
        )

    assert b"job_id" not in captured["body"]
    assert b'"score_id":1' in captured["body"]
    assert result.pp == Decimal("321.12346")
    assert result.difficulty.star_rating == Decimal("6.54322")


@pytest.mark.asyncio
async def test_http_calculator_rejects_missing_formula_endpoint_without_fallback() -> None:
    async with httpx.AsyncClient() as client:
        calculator = HttpPerformanceCalculator(client, {"perfcho-rust": "http://rust.test"})
        with pytest.raises(PerformanceCalculationError, match="not configured") as caught:
            await calculator.calculate(_calculation(), beatmap_url="http://s3.test/map.osu")
    assert not caught.value.retryable


def _calculation() -> PerformanceCalculationInput:
    return PerformanceCalculationInput(
        score_id=1,
        account_id=42,
        formula_id=__import__("uuid").uuid7(),
        formula_code="official",
        calculator="perfcho-pp",
        release_id=__import__("uuid").uuid7(),
        release_version="2026.07.1",
        release_configuration={},
        difficulty_formula_id=__import__("uuid").uuid7(),
        difficulty_formula_code="official-difficulty",
        difficulty_release_id=__import__("uuid").uuid7(),
        difficulty_release_version="2026.07.1",
        difficulty_release_configuration={},
        input_digest=b"i" * 32,
        beatmap_revision_id=1,
        beatmap_sha256=b"b" * 32,
        beatmap_storage_key="beatmaps/test.osu",
        scoreboard=ScoreboardInfo(1, "osu", Ruleset.OSU, ScoreboardVariant.VANILLA),
        mod_set_id=1,
        mods=(),
        client_family=ClientFamily.LAZER,
        score=ScoreSubmission(
            total_score=1_000_000,
            classic_score=1_000_000,
            accuracy=Decimal("0.98"),
            max_combo=1000,
            grade=ScoreGrade.S,
            outcome=ScoreOutcome.PASSED,
            perfect=False,
            hits=(HitStatistic("great", 500), HitStatistic("miss", 1)),
        ),
    )
