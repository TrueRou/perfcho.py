from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from hashlib import sha256
from types import TracebackType
from typing import cast

import httpx
import pytest
from sqlalchemy import Index, UniqueConstraint

from perfcho.infra.db.models.scoring import (
    CalculationFormula,
    CalculationFormulaScoreboard,
    CalculationRelease,
    PerformanceCalculationJob,
    RankingPolicy,
    ScorePerformance,
)
from perfcho.infra.scoring import HttpPerformanceCalculator
from perfcho.modules.common import Clock, ObjectStorage, PendingEvent, StoredObject
from perfcho.modules.scoring.errors import PerformanceCalculationError
from perfcho.modules.scoring.models import (
    ClientFamily,
    DifficultyCalculationResult,
    HitStatistic,
    PerformanceCalculationInput,
    PerformanceCompletion,
    PerformanceResult,
    Ruleset,
    ScoreboardInfo,
    ScoreboardVariant,
    ScoreGrade,
    ScoreOutcome,
    ScoreSubmission,
)
from perfcho.modules.scoring.ports import PerformanceCalculationRepository, PerformanceCalculator
from perfcho.modules.scoring.services import PerformanceCalculationService

NOW = datetime(2026, 7, 29, 12, 30, tzinfo=UTC)


def test_multi_formula_tables_preserve_release_owned_results() -> None:
    assert tuple(ScorePerformance.__table__.primary_key.columns.keys()) == ("score_id", "release_id")
    assert tuple(PerformanceCalculationJob.__table__.primary_key.columns.keys()) == ("id",)
    assert any(
        isinstance(constraint, UniqueConstraint) and tuple(constraint.columns.keys()) == ("score_id", "release_id")
        for constraint in PerformanceCalculationJob.__table__.constraints
    )
    assert tuple(CalculationFormulaScoreboard.__table__.primary_key.columns.keys()) == (
        "formula_id",
        "scoreboard_id",
    )
    assert any(
        isinstance(constraint, UniqueConstraint)
        and tuple(constraint.columns.keys()) == ("formula_id", "ruleset", "version")
        for constraint in CalculationRelease.__table__.constraints
    )
    assert "calculator" in CalculationFormula.__table__.columns
    assert "difficulty_attribute_id" in ScorePerformance.__table__.columns
    assert "input_digest" in ScorePerformance.__table__.columns
    assert "output_digest" in ScorePerformance.__table__.columns


def test_formula_and_ranking_active_indexes_allow_parallel_systems() -> None:
    release_indexes = {index.name: index for index in CalculationRelease.__table__.indexes}
    assert _index_columns(release_indexes["uq_calculation_releases_active_formula_ruleset"]) == (
        "formula_id",
        "ruleset",
    )
    policy_indexes = {index.name: index for index in RankingPolicy.__table__.indexes}
    assert _index_columns(policy_indexes["uq_ranking_policies_active_code"]) == ("code",)
    assert _index_columns(policy_indexes["uq_ranking_policies_default_scoreboard"]) == ("scoreboard_id",)


@pytest.mark.asyncio
async def test_http_calculator_routes_by_formula_calculator_and_verifies_release() -> None:
    calculation = _calculation()

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url == httpx.URL("http://calculator.test/v1/performance/calculate")
        assert request.headers["content-type"].startswith("multipart/form-data")
        return httpx.Response(
            200,
            json={
                "schema_version": 1,
                "calculator": "osu-lazer-dotnet",
                "release_version": "2026.07.1",
                "artifact_digest": (b"a" * 32).hex(),
                "difficulty_release_version": "2026.07.1-difficulty",
                "difficulty_artifact_digest": (b"d" * 32).hex(),
                "input_digest": (b"i" * 32).hex(),
                "difficulty": {
                    "star_rating": "6.543219",
                    "max_combo": 1234,
                    "attributes": {"aim": 3.2},
                },
                "performance": {"pp": "321.123456", "breakdown": {"aim": 200}},
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await HttpPerformanceCalculator(
            client,
            {"osu-lazer-dotnet": "http://calculator.test/"},
        ).calculate(calculation, b"osu file")

    assert result.pp == Decimal("321.12346")
    assert result.difficulty.star_rating == Decimal("6.54322")
    assert result.difficulty.max_combo == 1234


@pytest.mark.asyncio
async def test_http_calculator_rejects_missing_formula_endpoint_without_fallback() -> None:
    async with httpx.AsyncClient() as client:
        calculator = HttpPerformanceCalculator(client, {"perfcho-rust": "http://rust.test"})
        with pytest.raises(PerformanceCalculationError, match="not configured") as caught:
            await calculator.calculate(_calculation(), b"osu file")
    assert not caught.value.retryable


@pytest.mark.asyncio
async def test_calculation_service_keeps_external_io_between_short_transactions() -> None:
    calls: list[str] = []
    calculation = replace(_calculation(), beatmap_sha256=sha256(b"osu file").digest())
    result = PerformanceResult(
        Decimal("321.12345"),
        DifficultyCalculationResult(Decimal("6.54321"), 1234),
    )
    repository = _FakeCalculationRepository(calls, calculation)
    outbox = _FakeOutbox(calls)
    service = PerformanceCalculationService(
        lambda: _FakeUnitOfWork(calls),
        lambda session: cast(PerformanceCalculationRepository, repository),
        lambda session: outbox,
        cast(PerformanceCalculator, _FakeCalculator(calls, result)),
        cast(ObjectStorage, _FakeStorage(calls, b"osu file")),
        cast(Clock, _FixedClock()),
        max_attempts=3,
        max_beatmap_bytes=1024,
        max_retry_seconds=30,
    )

    await service.execute(calculation.job_id, uuid.uuid4())

    assert calls == [
        "enter",
        "start",
        "commit",
        "exit",
        "storage-open",
        "storage-read",
        "calculator",
        "enter",
        "complete",
        "outbox",
        "commit",
        "exit",
    ]
    assert outbox.events[0].event_type == "score.performance-calculated.v1"
    assert outbox.events[0].payload["formula_code"] == "official"


@pytest.mark.asyncio
async def test_calculation_service_dead_letters_nonretryable_engine_errors() -> None:
    calls: list[str] = []
    calculation = replace(_calculation(), beatmap_sha256=sha256(b"osu file").digest())
    repository = _FakeCalculationRepository(calls, calculation)
    service = PerformanceCalculationService(
        lambda: _FakeUnitOfWork(calls),
        lambda session: cast(PerformanceCalculationRepository, repository),
        lambda session: _FakeOutbox(calls),
        cast(
            PerformanceCalculator,
            _FakeCalculator(calls, PerformanceCalculationError("unsupported mods", retryable=False)),
        ),
        cast(ObjectStorage, _FakeStorage(calls, b"osu file")),
        cast(Clock, _FixedClock()),
        max_attempts=3,
        max_beatmap_bytes=1024,
        max_retry_seconds=30,
    )

    await service.execute(calculation.job_id, uuid.uuid4())

    assert repository.failed is not None
    assert repository.failed["dead"] is True
    assert repository.failed["consume_attempt"] is False


def _calculation() -> PerformanceCalculationInput:
    return PerformanceCalculationInput(
        job_id=uuid.uuid7(),
        score_id=1,
        attempt_count=1,
        formula_id=uuid.uuid7(),
        formula_code="official",
        calculator="osu-lazer-dotnet",
        release_id=uuid.uuid7(),
        release_version="2026.07.1",
        artifact_digest=b"a" * 32,
        release_configuration={},
        difficulty_formula_id=uuid.uuid7(),
        difficulty_formula_code="official-difficulty",
        difficulty_release_id=uuid.uuid7(),
        difficulty_release_version="2026.07.1-difficulty",
        difficulty_artifact_digest=b"d" * 32,
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


def _index_columns(index: Index) -> tuple[str, ...]:
    return tuple(column.name for column in index.columns)


class _FixedClock:
    def now(self) -> datetime:
        return NOW


class _FakeUnitOfWork:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls
        self.session = object()

    async def __aenter__(self) -> _FakeUnitOfWork:
        self.calls.append("enter")
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc_value, traceback
        self.calls.append("exit")

    async def commit(self) -> None:
        self.calls.append("commit")


class _FakeCalculationRepository:
    def __init__(self, calls: list[str], calculation: PerformanceCalculationInput) -> None:
        self.calls = calls
        self.calculation = calculation
        self.failed: dict[str, object] | None = None

    async def start(self, *args: object, **kwargs: object) -> PerformanceCalculationInput:
        self.calls.append("start")
        return self.calculation

    async def complete(
        self,
        calculation: PerformanceCalculationInput,
        lease_token: uuid.UUID,
        result: PerformanceResult,
        *,
        output_digest: bytes,
        now: datetime,
    ) -> PerformanceCompletion:
        del lease_token, now
        self.calls.append("complete")
        return PerformanceCompletion(
            calculation.score_id,
            calculation.scoreboard.scoreboard_id,
            calculation.formula_id,
            calculation.formula_code,
            calculation.release_id,
            result.pp,
            output_digest,
        )

    async def fail(self, *args: object, **kwargs: object) -> None:
        self.calls.append("fail")
        self.failed = kwargs


class _FakeCalculator:
    def __init__(self, calls: list[str], result: PerformanceResult | Exception) -> None:
        self.calls = calls
        self.result = result

    async def calculate(self, calculation: PerformanceCalculationInput, beatmap_content: bytes) -> PerformanceResult:
        del calculation
        self.calls.append("calculator")
        assert beatmap_content == b"osu file"
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


class _FakeStream:
    def __init__(self, calls: list[str], content: bytes) -> None:
        self.calls = calls
        self.content = content
        self.metadata = StoredObject("beatmaps/test.osu", len(content), "text/plain", None)

    async def iter_chunks(self) -> AsyncIterator[bytes]:
        self.calls.append("storage-read")
        yield self.content


class _FakeStorage:
    def __init__(self, calls: list[str], content: bytes) -> None:
        self.calls = calls
        self.content = content

    @asynccontextmanager
    async def open(self, storage_key: str) -> AsyncIterator[_FakeStream]:
        assert storage_key == "beatmaps/test.osu"
        self.calls.append("storage-open")
        yield _FakeStream(self.calls, self.content)


class _FakeOutbox:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls
        self.events: list[PendingEvent] = []

    async def append(self, event: PendingEvent) -> uuid.UUID:
        self.calls.append("outbox")
        self.events.append(event)
        return uuid.uuid7()
