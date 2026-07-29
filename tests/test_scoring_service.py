import hashlib
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from types import TracebackType
from typing import cast

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra.db import engine as infra_db
from perfcho.infra.db.enums import BeatmapStatus as DbBeatmapStatus
from perfcho.infra.db.enums import Ruleset as DbRuleset
from perfcho.infra.db.models.content import Beatmap, BeatmapRevision, Beatmapset
from perfcho.infra.db.models.events import OutboxEvent
from perfcho.infra.db.models.scoring import (
    LeaderboardEntry,
    PlayAttempt,
    Replay,
    Score,
    ScoreHitStatistic,
)
from perfcho.infra.db.models.scoring import (
    ScoreAttestation as DbScoreAttestation,
)
from perfcho.infra.db.projectors.ranking import project_accepted_score
from perfcho.infra.db.repositories.account import SqlAlchemyOutboxWriter
from perfcho.infra.db.repositories.scoring import (
    SqlAlchemyAccountSubmissionValidator,
    SqlAlchemyMultiplayerSubmissionValidator,
    SqlAlchemyScoringRepository,
)
from perfcho.infra.db.uow import SqlAlchemyUnitOfWorkFactory
from perfcho.infra.scoring import DeferredPerformanceCalculator
from perfcho.modules.common import Actor, ClientContext, Clock, CommandMeta, IdGenerator, PendingEvent
from perfcho.modules.scoring import (
    AcceptedScoreResult,
    AcceptScore,
    BeatmapReference,
    ClientFamily,
    HitStatistic,
    PlayAttemptSubmission,
    Ruleset,
    ScoreAttestation,
    ScoreboardVariant,
    ScoreGrade,
    ScoreOutcome,
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
    PerformanceCalculationInput,
    PerformanceResult,
    PlayAttemptRecord,
    ScoreAcceptanceRecord,
    ScoreboardInfo,
)
from perfcho.modules.scoring.ports import ScoringRepository

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
            record.performance,
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


class FakeCalculator:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls

    async def calculate(self, calculation: PerformanceCalculationInput) -> PerformanceResult | None:
        self.calls.append("calculate")
        assert calculation.revision.revision_id == 20
        return None


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
    calculator = FakeCalculator(calls)
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
        calculator,
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
        "calculate",
        "score",
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

        uow_factory = SqlAlchemyUnitOfWorkFactory(session_factory)
        service = ScoringService(
            uow_factory,
            lambda session: SqlAlchemyScoringRepository(cast(AsyncSession, session)),
            lambda session: SqlAlchemyOutboxWriter(cast(AsyncSession, session)),
            lambda session: SqlAlchemyAccountSubmissionValidator(cast(AsyncSession, session)),
            lambda session: SqlAlchemyMultiplayerSubmissionValidator(cast(AsyncSession, session)),
            DeferredPerformanceCalculator(),
            cast(Clock, FixedClock()),
            cast(IdGenerator, FakeIds()),
        )
        submitted = command()
        first = await service.accept(submitted)
        replayed = await service.accept(submitted)

        async with session_factory.begin() as session:
            event = (
                await session.scalars(select(OutboxEvent).where(OutboxEvent.event_type == "score.accepted.v1"))
            ).one()
            await project_accepted_score(session, event, "scoreboard:1")

        assert replayed == first
        async with session_factory() as session:
            assert await session.scalar(select(func.count()).select_from(PlayAttempt)) == 1
            assert await session.scalar(select(func.count()).select_from(Score)) == 1
            assert await session.scalar(select(func.count()).select_from(ScoreHitStatistic)) == 4
            assert await session.scalar(select(func.count()).select_from(Replay)) == 1
            assert await session.scalar(select(func.count()).select_from(DbScoreAttestation)) == 1
            assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 1
            assert await session.scalar(select(func.count()).select_from(LeaderboardEntry)) == 2
    finally:
        await engine.dispose()
