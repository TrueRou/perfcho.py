from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import TracebackType
from typing import Any, cast
from unittest.mock import MagicMock

import httpx
import pytest
from loguru import logger
from sqlalchemy import Index, UniqueConstraint
from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra.db.enums import CalculationJobStatus
from perfcho.infra.db.models.scoring import (
    CalculationFormula,
    CalculationFormulaScoreboard,
    CalculationRelease,
    PerformanceCalculationJob,
    RankingPolicy,
    ScorePerformance,
)
from perfcho.infra.db.repositories.performance.job import SqlAlchemyPerformanceJobRepository
from perfcho.infra.upstream.calculator import HttpPerformanceCalculator
from perfcho.modules.common import ObjectUrlProvider, PendingEvent
from perfcho.modules.performance.errors import PerformanceCalculationError
from perfcho.modules.performance.models import (
    DifficultyCalculationResult,
    PerformanceCalculationInput,
    PerformanceCompletion,
    PerformanceResult,
)
from perfcho.modules.performance.ports import PerformanceCalculationRepository, PerformanceCalculator
from perfcho.modules.performance.services import PerformanceCalculationService
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

NOW = datetime(2026, 7, 29, 12, 30, tzinfo=UTC)


def test_multi_formula_tables_preserve_release_owned_results() -> None:
    assert tuple(ScorePerformance.__table__.primary_key.columns.keys()) == ("score_id", "release_id")
    assert PerformanceCalculationJob.__tablename__ == "calculation_jobs"
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
    assert "configuration_digest" not in CalculationRelease.__table__.columns
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
async def test_performance_job_start_is_idempotent_for_one_lease_token() -> None:
    calculation = _calculation()
    lease_token = uuid.uuid4()
    job = PerformanceCalculationJob(
        id=calculation.job_id,
        score_id=calculation.score_id,
        release_id=calculation.release_id,
        status=CalculationJobStatus.RUNNING,
        available_at=NOW,
        attempt_count=1,
        enqueue_count=1,
        lease_owner="tests:owner",
        lease_token=lease_token,
        lease_expires_at=NOW + timedelta(minutes=5),
        attempt_started_at=NOW,
        input_digest=calculation.input_digest,
    )
    session = MagicMock(spec=AsyncSession)
    session.get.return_value = job
    session.scalar.return_value = NOW + timedelta(seconds=1)
    repository = SqlAlchemyPerformanceJobRepository(session, execution_lease_seconds=300)

    assert await repository.start(calculation.job_id, lease_token) is None
    assert job.attempt_count == 1
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_performance_completion_rejects_dimensions_outside_leased_job() -> None:
    calculation = _calculation()
    lease_token = uuid.uuid4()
    job = PerformanceCalculationJob(
        id=calculation.job_id,
        score_id=calculation.score_id + 1,
        release_id=calculation.release_id,
        status=CalculationJobStatus.RUNNING,
        available_at=NOW,
        attempt_count=1,
        enqueue_count=1,
        lease_owner="tests:owner",
        lease_token=lease_token,
        lease_expires_at=NOW + timedelta(minutes=5),
        attempt_started_at=NOW,
        input_digest=calculation.input_digest,
    )
    session = MagicMock(spec=AsyncSession)
    session.get.return_value = job
    session.scalar.return_value = NOW + timedelta(seconds=1)
    repository = SqlAlchemyPerformanceJobRepository(session, execution_lease_seconds=300)
    result = PerformanceResult(
        Decimal("321.12345"),
        DifficultyCalculationResult(Decimal("6.54321"), 1234),
    )

    with pytest.raises(PerformanceCalculationError, match="dimensions") as caught:
        await repository.complete(
            calculation,
            lease_token,
            result,
            output_digest=b"o" * 32,
        )
    assert not caught.value.retryable


@pytest.mark.asyncio
async def test_http_calculator_routes_by_formula_calculator_and_verifies_release() -> None:
    calculation = _calculation()

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url == httpx.URL("http://calculator.test/v1/performance/calculate")
        assert request.headers["content-type"].startswith("multipart/form-data")
        assert b"http://s3.test/map.osu" in request.content
        assert b'name="beatmap"' not in request.content
        return httpx.Response(
            200,
            json={
                "schema_version": 1,
                "calculator": "perfcho-pp",
                "release_version": "2026.07.1",
                "difficulty_release_version": "2026.07.1-difficulty",
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
            {"perfcho-pp": "http://calculator.test/"},
        ).calculate(
            calculation,
            beatmap_url="http://s3.test/map.osu",
        )

    assert result.pp == Decimal("321.12346")
    assert result.difficulty.star_rating == Decimal("6.54322")
    assert result.difficulty.max_combo == 1234


@pytest.mark.asyncio
async def test_http_calculator_rejects_missing_formula_endpoint_without_fallback() -> None:
    async with httpx.AsyncClient() as client:
        calculator = HttpPerformanceCalculator(client, {"perfcho-rust": "http://rust.test"})
        with pytest.raises(PerformanceCalculationError, match="not configured") as caught:
            await calculator.calculate(
                _calculation(),
                beatmap_url="http://s3.test/map.osu",
            )
    assert not caught.value.retryable


@pytest.mark.asyncio
async def test_http_calculator_treats_failed_dependency_as_client_error() -> None:
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(424))) as client:
        calculator = HttpPerformanceCalculator(client, {"perfcho-pp": "http://calculator.test"})
        with pytest.raises(PerformanceCalculationError, match="HTTP 424") as caught:
            await calculator.calculate(
                _calculation(),
                beatmap_url="http://s3.test/map.osu",
            )
    assert not caught.value.retryable


@pytest.mark.asyncio
async def test_calculation_service_keeps_external_io_between_short_transactions() -> None:
    calls: list[str] = []
    calculation = _calculation()
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
        cast(ObjectUrlProvider, _FakeUrlProvider(calls)),
        max_attempts=3,
        beatmap_url_expiry_seconds=600,
        max_retry_seconds=30,
    )

    records: list[dict[str, Any]] = []
    sink_id = logger.add(lambda message: records.append(cast(dict[str, Any], message.record)))
    try:
        await service.execute(calculation.job_id, uuid.uuid4())
    finally:
        logger.remove(sink_id)

    assert calls == [
        "enter",
        "start",
        "commit",
        "exit",
        "presign",
        "calculator-url",
        "enter",
        "complete",
        "outbox",
        "commit",
        "exit",
    ]
    assert outbox.events[0].event_type == "score.performance-calculated.v1"
    assert outbox.events[0].payload["formula_code"] == "official"
    outcomes = [record for record in records if record["extra"]["event"].startswith("performance.calculation.")]
    assert [record["extra"]["event"] for record in outcomes] == [
        "performance.calculation.started",
        "performance.calculation.succeeded",
    ]
    assert [record["extra"]["phase"] for record in outcomes] == ["start", "complete"]
    assert all(record["extra"]["job_id"] == str(calculation.job_id) for record in outcomes)
    assert all("duration_ms" in record["extra"] for record in outcomes)


@pytest.mark.asyncio
async def test_calculation_service_dead_letters_nonretryable_engine_errors() -> None:
    calls: list[str] = []
    calculation = _calculation()
    repository = _FakeCalculationRepository(calls, calculation)
    service = PerformanceCalculationService(
        lambda: _FakeUnitOfWork(calls),
        lambda session: cast(PerformanceCalculationRepository, repository),
        lambda session: _FakeOutbox(calls),
        cast(
            PerformanceCalculator,
            _FakeCalculator(calls, PerformanceCalculationError("unsupported mods", retryable=False)),
        ),
        cast(ObjectUrlProvider, _FakeUrlProvider(calls)),
        max_attempts=3,
        beatmap_url_expiry_seconds=600,
        max_retry_seconds=30,
    )

    records: list[dict[str, Any]] = []
    sink_id = logger.add(lambda message: records.append(cast(dict[str, Any], message.record)))
    try:
        await service.execute(calculation.job_id, uuid.uuid4())
    finally:
        logger.remove(sink_id)

    assert repository.failed is not None
    assert repository.failed["dead"] is True
    assert repository.failed["consume_attempt"] is False
    dead_event = next(record for record in records if record["extra"]["event"] == "performance.calculation.dead")
    assert dead_event["level"].name == "ERROR"
    assert dead_event["extra"]["phase"] == "calculate"
    assert dead_event["extra"]["error_type"] == "PerformanceCalculationError"
    assert dead_event["exception"].value.args == ("unsupported mods",)
    assert {
        "lease_token",
        "beatmap_url",
        "beatmap_storage_key",
        "score_id",
        "pp",
        "error",
    }.isdisjoint(dead_event["extra"])


@pytest.mark.asyncio
async def test_calculation_service_logs_retry_with_full_exception_details() -> None:
    calls: list[str] = []
    calculation = _calculation()
    repository = _FakeCalculationRepository(calls, calculation)
    service = PerformanceCalculationService(
        lambda: _FakeUnitOfWork(calls),
        lambda session: cast(PerformanceCalculationRepository, repository),
        lambda session: _FakeOutbox(calls),
        cast(PerformanceCalculator, _FakeCalculator(calls, RuntimeError("calculator secret detail"))),
        cast(ObjectUrlProvider, _FakeUrlProvider(calls)),
        max_attempts=3,
        beatmap_url_expiry_seconds=600,
        max_retry_seconds=30,
    )
    records: list[dict[str, Any]] = []
    sink_id = logger.add(lambda message: records.append(cast(dict[str, Any], message.record)))

    try:
        await service.execute(calculation.job_id, uuid.uuid4())
    finally:
        logger.remove(sink_id)

    assert repository.failed is not None and repository.failed["dead"] is False
    retry_event = next(record for record in records if record["extra"]["event"] == "performance.calculation.retry")
    assert retry_event["level"].name == "WARNING"
    assert retry_event["extra"]["phase"] == "calculate"
    assert retry_event["extra"]["error_type"] == "RuntimeError"
    assert retry_event["exception"].value.args == ("calculator secret detail",)
    assert {"lease_token", "beatmap_url", "beatmap_storage_key", "score_id", "error"}.isdisjoint(retry_event["extra"])


def _calculation() -> PerformanceCalculationInput:
    return PerformanceCalculationInput(
        job_id=uuid.uuid7(),
        score_id=1,
        account_id=42,
        attempt_count=1,
        formula_id=uuid.uuid7(),
        formula_code="official",
        calculator="perfcho-pp",
        release_id=uuid.uuid7(),
        release_version="2026.07.1",
        release_configuration={},
        difficulty_formula_id=uuid.uuid7(),
        difficulty_formula_code="official-difficulty",
        difficulty_release_id=uuid.uuid7(),
        difficulty_release_version="2026.07.1-difficulty",
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
    ) -> PerformanceCompletion:
        del lease_token
        self.calls.append("complete")
        return PerformanceCompletion(
            calculation.score_id,
            calculation.account_id,
            calculation.scoreboard.scoreboard_id,
            calculation.formula_id,
            calculation.formula_code,
            calculation.release_id,
            result.pp,
            output_digest,
        )

    async def fail(self, *args: object, **kwargs: object) -> bool:
        self.calls.append("fail")
        self.failed = kwargs
        return True


class _FakeCalculator:
    def __init__(self, calls: list[str], result: PerformanceResult | Exception) -> None:
        self.calls = calls
        self.result = result

    async def calculate(
        self,
        calculation: PerformanceCalculationInput,
        *,
        beatmap_url: str,
    ) -> PerformanceResult:
        del calculation
        self.calls.append("calculator-url")
        assert beatmap_url == "http://s3.test/map.osu"
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


class _FakeUrlProvider:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls

    async def presign_read(self, storage_key: str, *, expires_in_seconds: int) -> str:
        assert storage_key == "beatmaps/test.osu"
        assert expires_in_seconds == 600
        self.calls.append("presign")
        return "http://s3.test/map.osu"


class _FakeOutbox:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls
        self.events: list[PendingEvent] = []

    async def append(self, event: PendingEvent) -> uuid.UUID:
        self.calls.append("outbox")
        self.events.append(event)
        return uuid.uuid7()
