import hashlib
import uuid
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from types import TracebackType
from typing import cast

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra.db import engine as infra_db
from perfcho.infra.db.enums import BeatmapStatus as DbBeatmapStatus
from perfcho.infra.db.enums import CalculationJobStatus
from perfcho.infra.db.enums import CalculationKind as DbCalculationKind
from perfcho.infra.db.enums import Ruleset as DbRuleset
from perfcho.infra.db.models.content import Beatmap, BeatmapRevision, Beatmapset
from perfcho.infra.db.models.events import OutboxEvent
from perfcho.infra.db.models.scoring import (
    CalculationFormula,
    CalculationFormulaScoreboard,
    CalculationRelease,
    LeaderboardEntry,
    PerformanceCalculationJob,
    PlayAttempt,
    Replay,
    Score,
    ScoreHitStatistic,
)
from perfcho.infra.db.models.scoring import (
    ScoreAttestation as DbScoreAttestation,
)
from perfcho.infra.db.projectors.ranking import project_accepted_score
from perfcho.infra.db.relays.performance_job import SqlAlchemyPerformanceJobRelayStore
from perfcho.infra.db.repositories.outbox import SqlAlchemyOutboxWriter
from perfcho.infra.db.repositories.performance.scheduling import SqlAlchemyPerformanceJobScheduler
from perfcho.infra.db.repositories.scoring import (
    SqlAlchemyAccountSubmissionValidator,
    SqlAlchemyMultiplayerSubmissionValidator,
    SqlAlchemyScoringRepository,
)
from perfcho.infra.db.uow import SqlAlchemyUnitOfWorkFactory
from perfcho.modules.common import Actor, ClientContext, Clock, CommandMeta, IdGenerator, PendingEvent
from perfcho.modules.scoring import (
    AcceptedScoreResult,
    AcceptScore,
    BeatmapReference,
    CanonicalMod,
    ClientFamily,
    HitStatistic,
    PlayAttemptSubmission,
    Ruleset,
    ScoreAttestation,
    ScoreboardVariant,
    ScoreGrade,
    ScoreOutcome,
    ScoreRejected,
    ScoreSubmission,
    ScoringService,
    StagedReplayManifest,
)
from perfcho.modules.scoring.models import (
    AcceptanceClaim,
    AccountSubmissionContext,
    AttemptClaim,
    BeatmapRevisionInfo,
    ModSetInfo,
    NormalizedModSet,
    PlayAttemptRecord,
    ScoreAcceptanceRecord,
    ScoreboardInfo,
)
from perfcho.modules.scoring.ports import ScoringRepository
from perfcho.modules.scoring.validation import validate_score

NOW = datetime(2026, 7, 29, 12, 30, tzinfo=UTC)


class FixedClock:
    def now(self) -> datetime:
        return NOW


class FakeIds:
    def new(self) -> uuid.UUID:
        return uuid.uuid7()


class FakeUnitOfWork:
    def __init__(self, calls: list[str]) -> None:
        self.session = object()
        self.calls = calls
        self.committed = False

    async def __aenter__(self) -> FakeUnitOfWork:
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
        self.committed = True
        self.calls.append("commit")


class FakeRepository:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls
        self.record: ScoreAcceptanceRecord | None = None
        self.attempt_id = uuid.uuid7()

    async def claim_acceptance(self, **kwargs: object) -> AcceptanceClaim:
        self.calls.append("claim-acceptance")
        return AcceptanceClaim()

    async def resolve_current_revision(self, reference: BeatmapReference) -> BeatmapRevisionInfo:
        self.calls.append("revision")
        assert reference.md5 == b"m" * 16
        return BeatmapRevisionInfo(10, 20, Ruleset.OSU, "ranked", 10, 10)

    async def get_scoreboard(self, ruleset: Ruleset, variant: ScoreboardVariant) -> ScoreboardInfo:
        self.calls.append("scoreboard")
        return ScoreboardInfo(1, "osu", ruleset, variant)

    async def get_or_create_mod_set(self, scoreboard_id: int, normalized: NormalizedModSet) -> ModSetInfo:
        self.calls.append("mod-set")
        return ModSetInfo(30, scoreboard_id, normalized.canonical, normalized.canonical_digest, normalized.legacy_bits)

    async def claim_attempt(self, record: PlayAttemptRecord) -> AttemptClaim:
        self.calls.append("attempt")
        return AttemptClaim(self.attempt_id)

    async def insert_score(self, record: ScoreAcceptanceRecord) -> AcceptedScoreResult:
        self.calls.append("score")
        self.record = record
        return AcceptedScoreResult(
            record.attempt_id,
            40,
            record.revision.beatmap_id,
            record.revision.revision_id,
            record.scoreboard.scoreboard_id,
            record.mod_set.mod_set_id,
            record.score.outcome,
        )

    async def complete_acceptance(self, idempotency_key: str, result: AcceptedScoreResult) -> None:
        self.calls.append("complete")


class FakeAccountValidator:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls

    async def validate(self, account_id: int, *, at: datetime) -> AccountSubmissionContext:
        self.calls.append("account")
        assert account_id == 1 and at == NOW
        return AccountSubmissionContext(account_id, "JP")


class FakeMultiplayerValidator:
    async def validate(self, *args: object, **kwargs: object) -> None:
        raise AssertionError("single-player submission must not validate multiplayer")

    async def bind_score(self, *args: object, **kwargs: object) -> None:
        raise AssertionError("single-player submission must not bind multiplayer")


class FakeTaskScheduler:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls

    async def schedule(self, **kwargs: object) -> None:
        self.calls.append("schedule-performance")


class FakeOutbox:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls
        self.events: list[PendingEvent] = []

    async def append(self, event: PendingEvent) -> uuid.UUID:
        self.calls.append("outbox")
        self.events.append(event)
        return uuid.uuid7()


def command() -> AcceptScore:
    return AcceptScore(
        meta=CommandMeta(
            request_id=uuid.uuid7(),
            idempotency_key="score:test",
            request_digest=hashlib.sha256(b"score").digest(),
            actor=Actor(1, uuid.uuid7()),
            client=ClientContext("stable", "b20260711.1", None, "127.0.0.1"),
            received_at=NOW,
        ),
        beatmap=BeatmapReference(md5=b"m" * 16),
        ruleset=Ruleset.OSU,
        variant=ScoreboardVariant.VANILLA,
        mods=(),
        attempt=PlayAttemptSubmission("attempt:test", NOW, NOW, Decimal(1)),
        score=ScoreSubmission(
            total_score=1_000_000,
            classic_score=1_000_000,
            accuracy=Decimal(1),
            max_combo=10,
            grade=ScoreGrade.X,
            outcome=ScoreOutcome.PASSED,
            perfect=True,
            hits=(
                HitStatistic("great", 10),
                HitStatistic("ok", 0),
                HitStatistic("meh", 0),
                HitStatistic("miss", 0),
            ),
            online_checksum=b"o" * 16,
        ),
        replay=StagedReplayManifest("stable", b"r" * 32, 100, "replays/test.osr", "b20260711.1"),
        attestation=ScoreAttestation(
            ClientFamily.STABLE,
            "b20260711.1",
            "verified",
            checksum=b"a" * 32,
        ),
    )


@pytest.mark.asyncio
async def test_scoring_service_persists_validated_facts_and_event_in_one_transaction() -> None:
    calls: list[str] = []
    repository = FakeRepository(calls)
    account = FakeAccountValidator(calls)
    outbox = FakeOutbox(calls)
    units: list[FakeUnitOfWork] = []

    def uow_factory() -> FakeUnitOfWork:
        unit = FakeUnitOfWork(calls)
        units.append(unit)
        return unit

    service = ScoringService(
        uow_factory,
        lambda session: cast(ScoringRepository, repository),
        lambda session: outbox,
        lambda session: account,
        lambda session: FakeMultiplayerValidator(),
        lambda session: FakeTaskScheduler(calls),
        cast(Clock, FixedClock()),
        cast(IdGenerator, FakeIds()),
    )

    result = await service.accept(command())

    assert result.score_id == 40
    assert calls == [
        "enter",
        "claim-acceptance",
        "account",
        "revision",
        "scoreboard",
        "mod-set",
        "attempt",
        "score",
        "schedule-performance",
        "outbox",
        "complete",
        "commit",
        "exit",
    ]
    assert units[0].committed
    assert repository.record is not None
    assert repository.record.validated.total_hits == 10
    assert outbox.events[0].event_type == "score.accepted.v1"
    assert outbox.events[0].payload["country_code"] == "JP"
    assert "performance_release_ids" not in outbox.events[0].payload


def test_score_validation_enforces_object_count_and_vanilla_combo_bounds() -> None:
    submitted = command()
    revision = BeatmapRevisionInfo(10, 20, Ruleset.OSU, "ranked", 10, 10)

    with pytest.raises(ScoreRejected, match="hit count"):
        validate_score(
            Ruleset.OSU,
            (),
            submitted.attempt,
            replace(
                submitted.score,
                hits=(
                    HitStatistic("great", 9),
                    HitStatistic("ok", 0),
                    HitStatistic("meh", 0),
                    HitStatistic("miss", 0),
                ),
            ),
            revision,
        )
    with pytest.raises(ScoreRejected, match="combo exceeds"):
        validate_score(
            Ruleset.OSU,
            (),
            submitted.attempt,
            replace(submitted.score, max_combo=11, perfect=False),
            revision,
        )


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_postgres_scoring_acceptance_is_atomic_and_exactly_replayable(
    postgres_database_url: str,
) -> None:
    del postgres_database_url
    engine = await infra_db.create_engine()
    session_factory = infra_db.create_session_factory(engine)
    try:
        async with session_factory.begin() as session:
            beatmapset = Beatmapset(
                source_id=1,
                external_id=200,
                creator_name="Creator",
                artist="Artist",
                title="Title",
                status=DbBeatmapStatus.RANKED,
                available=True,
            )
            session.add(beatmapset)
            await session.flush()
            beatmap = Beatmap(
                beatmapset_id=beatmapset.id,
                source_id=1,
                external_id=100,
                ruleset=DbRuleset.OSU,
                difficulty_name="Test",
                status=DbBeatmapStatus.RANKED,
            )
            session.add(beatmap)
            await session.flush()
            session.add(
                BeatmapRevision(
                    beatmap_id=beatmap.id,
                    md5=b"m" * 16,
                    sha256=b"s" * 32,
                    file_name="Artist - Title (Creator) [Test].osu",
                    file_name_key="artist - title (creator) [test].osu",
                    source_updated_at=NOW,
                    total_length_ms=60_000,
                    drain_length_ms=50_000,
                    bpm=Decimal(180),
                    circle_size=Decimal(4),
                    overall_difficulty=Decimal(8),
                    approach_rate=Decimal(9),
                    health_drain=Decimal(6),
                    object_count=10,
                    circle_count=10,
                    slider_count=0,
                    spinner_count=0,
                    max_combo=10,
                    is_current=True,
                )
            )
            difficulty_formula = CalculationFormula(
                code="official-difficulty",
                name="Official Difficulty",
                kind=DbCalculationKind.DIFFICULTY,
                calculator="osu-lazer-dotnet",
                enabled=True,
            )
            performance_formula = CalculationFormula(
                code="official",
                name="Official Performance",
                kind=DbCalculationKind.PERFORMANCE,
                calculator="osu-lazer-dotnet",
                enabled=True,
            )
            session.add_all((difficulty_formula, performance_formula))
            await session.flush()
            difficulty_release = CalculationRelease(
                formula_id=difficulty_formula.id,
                ruleset=DbRuleset.OSU,
                version="2026.07.1-difficulty",
                artifact_digest=b"d" * 32,
                configuration={},
                configuration_digest=b"e" * 32,
                active=True,
            )
            session.add(difficulty_release)
            await session.flush()
            performance_release = CalculationRelease(
                formula_id=performance_formula.id,
                ruleset=DbRuleset.OSU,
                version="2026.07.1",
                artifact_digest=b"p" * 32,
                configuration={},
                configuration_digest=b"q" * 32,
                difficulty_release_id=difficulty_release.id,
                active=True,
            )
            session.add(performance_release)
            session.add(CalculationFormulaScoreboard(formula_id=performance_formula.id, scoreboard_id=1))

        uow_factory = SqlAlchemyUnitOfWorkFactory(session_factory)
        service = ScoringService(
            uow_factory,
            lambda session: SqlAlchemyScoringRepository(cast(AsyncSession, session)),
            lambda session: SqlAlchemyOutboxWriter(cast(AsyncSession, session)),
            lambda session: SqlAlchemyAccountSubmissionValidator(cast(AsyncSession, session)),
            lambda session: SqlAlchemyMultiplayerSubmissionValidator(cast(AsyncSession, session)),
            lambda session: SqlAlchemyPerformanceJobScheduler(cast(AsyncSession, session)),
            cast(Clock, FixedClock()),
            cast(IdGenerator, FakeIds()),
        )
        submitted = replace(
            command(),
            mods=(CanonicalMod("HD"),),
            score=replace(command().score, grade=ScoreGrade.XH),
        )
        first = await service.accept(submitted)
        replayed = await service.accept(submitted)
        shared_replay = replace(
            submitted,
            meta=replace(
                submitted.meta,
                request_id=uuid.uuid7(),
                idempotency_key="score:test:shared-replay",
                request_digest=hashlib.sha256(b"score:shared-replay").digest(),
            ),
            attempt=replace(submitted.attempt, idempotency_key="attempt:test:shared-replay"),
            mods=(CanonicalMod("HD", {"test_setting": 1}),),
            score=replace(submitted.score, online_checksum=b"p" * 16),
        )
        await service.accept(shared_replay)

        performance_relay = SqlAlchemyPerformanceJobRelayStore(
            session_factory,
            batch_size=10,
            lease_seconds=300,
            max_attempts=5,
            max_retry_seconds=300,
        )
        performance_claims = await performance_relay.claim("tests:performance-owner")
        assert len(performance_claims) == 2
        await performance_relay.mark_enqueue_failed(
            performance_claims[0],
            "tests:performance-owner",
            RuntimeError("Redis unavailable"),
        )
        await performance_relay.release(performance_claims[1], "tests:performance-owner")

        async with session_factory.begin() as session:
            events = (
                await session.scalars(
                    select(OutboxEvent)
                    .where(OutboxEvent.event_type == "score.accepted.v1")
                    .order_by(OutboxEvent.position)
                )
            ).all()
            for event in events:
                await project_accepted_score(session, event, "scoreboard:1")
            exact_mods = await SqlAlchemyScoringRepository(session).get_leaderboard(
                beatmap_id=first.beatmap_id,
                ruleset=Ruleset.OSU,
                variant=ScoreboardVariant.VANILLA,
                leaderboard_type=2,
                legacy_mod_bits=1 << 3,
                requester_account_id=1,
                friend_account_ids=(),
                limit=50,
            )

        assert replayed == first
        assert len(exact_mods.scores) == 1
        async with session_factory() as session:
            assert await session.scalar(select(func.count()).select_from(PlayAttempt)) == 2
            assert await session.scalar(select(func.count()).select_from(Score)) == 2
            assert await session.scalar(select(func.count()).select_from(ScoreHitStatistic)) == 8
            assert await session.scalar(select(func.count()).select_from(Replay)) == 2
            assert await session.scalar(select(func.count()).select_from(DbScoreAttestation)) == 2
            assert await session.scalar(select(func.count()).select_from(PerformanceCalculationJob)) == 2
            assert set(await session.scalars(select(PerformanceCalculationJob.release_id))) == {performance_release.id}
            assert set(await session.scalars(select(PerformanceCalculationJob.status))) == {
                CalculationJobStatus.PENDING
            }
            assert set(await session.scalars(select(PerformanceCalculationJob.attempt_count))) == {0}
            assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 2
            assert await session.scalar(select(func.count()).select_from(LeaderboardEntry)) == 3
    finally:
        await engine.dispose()
