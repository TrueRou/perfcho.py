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
from perfcho.infra.db.enums import Ruleset as DbRuleset
from perfcho.infra.db.models.content import Beatmap, BeatmapRevision, Beatmapset
from perfcho.infra.db.models.events import OutboxEvent
from perfcho.infra.db.models.scoring import (
    BeatmapActivity,
    BeatmapDifficultyAttribute,
    BeatmapFailHistogram,
    CalculationFormula,
    CalculationRelease,
    LeaderboardEntry,
    PerformanceCalculationJob,
    PlayAttempt,
    RankingPolicy,
    Replay,
    ReplayViewEvent,
    Score,
    ScoreHitStatistic,
    ScorePerformance,
    UserBeatmapActivity,
    UserMonthlyActivity,
    UserPlayStat,
    UserRankedStat,
)
from perfcho.infra.db.models.scoring import (
    ScoreAttestation as DbScoreAttestation,
)
from perfcho.infra.db.models.social import AchievementDefinition, AchievementTranslation, AchievementUnlock
from perfcho.infra.db.projectors.ranking import project_accepted_score
from perfcho.infra.db.projectors.scoring_stats import project_scoring_stats
from perfcho.infra.db.relays.performance_job import SqlAlchemyPerformanceJobRelayStore
from perfcho.infra.db.repositories.outbox import SqlAlchemyOutboxWriter
from perfcho.infra.db.repositories.performance.scheduling import SqlAlchemyPerformanceJobScheduler
from perfcho.infra.db.repositories.scoring import (
    SqlAlchemyAccountSubmissionValidator,
    SqlAlchemyMultiplayerSubmissionValidator,
    SqlAlchemyScoringRepository,
)
from perfcho.infra.db.repositories.social import SqlAlchemySocialRepository
from perfcho.infra.db.uow import SqlAlchemyUnitOfWorkFactory
from perfcho.modules.common import Actor, ClientContext, Clock, CommandMeta, IdGenerator, PendingEvent
from perfcho.modules.scoring import (
    AcceptedScoreResult,
    AcceptScore,
    BeatmapReference,
    CanonicalMod,
    ClientFamily,
    HitStatistic,
    MultiplayerSubmissionContext,
    PlayAttemptSubmission,
    ReplayQueryService,
    ReplayService,
    Ruleset,
    ScoreAttestation,
    ScoreboardVariant,
    ScoreGrade,
    ScoreOutcome,
    ScoreRejected,
    ScoreSubmission,
    ScoringService,
    StagedReplayManifest,
    weighted_total_performance,
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
from perfcho.modules.social.achievements import default_achievement_evaluator_registry
from perfcho.modules.social.services import TransactionAchievementAwarder


def test_weighted_performance_uses_descending_personal_bests_and_bonus() -> None:
    assert weighted_total_performance(()) == 0
    assert weighted_total_performance((Decimal("100"),)) == 100
    assert weighted_total_performance((Decimal("100"), Decimal("50"))) == 148


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
        self.acceptance_claim = AcceptanceClaim()

    async def claim_acceptance(self, **kwargs: object) -> AcceptanceClaim:
        self.calls.append("claim-acceptance")
        return self.acceptance_claim

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


class FakeBoundMultiplayerValidator:
    async def validate(self, *args: object, **kwargs: object) -> None:
        return None

    async def bind_score(self, *args: object, **kwargs: object) -> None:
        return None


class FakeTaskScheduler:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls

    async def schedule(self, **kwargs: object) -> None:
        self.calls.append("schedule-performance")


class FakeAchievementAwarder:
    def __init__(self, calls: list[str], error: Exception | None = None) -> None:
        self.calls = calls
        self.error = error

    async def award_for_score(self, *args: object, **kwargs: object) -> tuple[()]:
        self.calls.append("achievements")
        if self.error is not None:
            raise self.error
        return ()


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
async def test_scoring_service_persists_validated_facts_and_event_in_one_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
        lambda session: FakeAchievementAwarder(calls),
        cast(Clock, FixedClock()),
        cast(IdGenerator, FakeIds()),
    )
    logged: list[tuple[str, str, dict[str, object]]] = []
    monkeypatch.setattr(
        "perfcho.modules.scoring.services.log_event",
        lambda level, event, **fields: logged.append((level, event, fields)),
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
        "achievements",
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
    assert outbox.events[0].consumers == ("ranking-projector.v1", "scoring-stats-projector.v1")
    assert outbox.events[0].payload["country_code"] == "JP"
    assert "performance_release_ids" not in outbox.events[0].payload
    assert logged[0][:2] == ("INFO", "scoring.score.accepted")
    assert logged[0][2]["score_id"] == 40
    assert not {"replay", "checksum", "facts", "request_digest"} & logged[0][2].keys()


@pytest.mark.asyncio
async def test_scoring_service_does_not_commit_when_achievement_awarding_fails() -> None:
    calls: list[str] = []
    units: list[FakeUnitOfWork] = []

    def uow_factory() -> FakeUnitOfWork:
        unit = FakeUnitOfWork(calls)
        units.append(unit)
        return unit

    service = ScoringService(
        uow_factory,
        lambda session: cast(ScoringRepository, FakeRepository(calls)),
        lambda session: FakeOutbox(calls),
        lambda session: FakeAccountValidator(calls),
        lambda session: FakeMultiplayerValidator(),
        lambda session: FakeTaskScheduler(calls),
        lambda session: FakeAchievementAwarder(calls, RuntimeError("achievement storage failed")),
        cast(Clock, FixedClock()),
        cast(IdGenerator, FakeIds()),
    )

    with pytest.raises(RuntimeError, match="achievement storage failed"):
        await service.accept(command())

    assert calls[-2:] == ["achievements", "exit"]
    assert "schedule-performance" not in calls
    assert "complete" not in calls
    assert not units[0].committed


@pytest.mark.asyncio
async def test_multiplayer_score_routes_results_projector_in_acceptance_transaction() -> None:
    calls: list[str] = []
    repository = FakeRepository(calls)
    outbox = FakeOutbox(calls)
    service = ScoringService(
        lambda: FakeUnitOfWork(calls),
        lambda session: cast(ScoringRepository, repository),
        lambda session: outbox,
        lambda session: FakeAccountValidator(calls),
        lambda session: FakeBoundMultiplayerValidator(),
        lambda session: FakeTaskScheduler(calls),
        lambda session: FakeAchievementAwarder(calls),
        cast(Clock, FixedClock()),
        cast(IdGenerator, FakeIds()),
    )

    await service.accept(replace(command(), multiplayer=MultiplayerSubmissionContext(uuid.uuid7(), b"m" * 32)))

    assert outbox.events[0].consumers == (
        "ranking-projector.v1",
        "scoring-stats-projector.v1",
        "multiplayer-results-projector.v1",
    )


@pytest.mark.asyncio
async def test_scoring_replay_logs_only_stable_ids_at_debug(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    repository = FakeRepository(calls)
    prior = AcceptedScoreResult(repository.attempt_id, 40, 10, 20, 1, 30, ScoreOutcome.PASSED)
    repository.acceptance_claim = AcceptanceClaim(prior)
    logged: list[tuple[str, str, dict[str, object]]] = []
    monkeypatch.setattr(
        "perfcho.modules.scoring.services.log_event",
        lambda level, event, **fields: logged.append((level, event, fields)),
    )

    def uow_factory() -> FakeUnitOfWork:
        return FakeUnitOfWork(calls)

    service = ScoringService(
        uow_factory,
        lambda session: cast(ScoringRepository, repository),
        lambda session: FakeOutbox(calls),
        lambda session: FakeAccountValidator(calls),
        lambda session: FakeMultiplayerValidator(),
        lambda session: FakeTaskScheduler(calls),
        lambda session: FakeAchievementAwarder(calls),
        cast(Clock, FixedClock()),
        cast(IdGenerator, FakeIds()),
    )

    assert await service.accept(command()) is prior

    assert logged[0][:2] == ("DEBUG", "scoring.score.replayed")
    assert logged[0][2]["attempt_id"] == str(prior.attempt_id)
    assert not {"replay", "checksum", "facts", "request_digest"} & logged[0][2].keys()


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
            session.add(
                AchievementDefinition(
                    slug="score-test-million-20260803",
                    evaluator_code="score_total_at_least",
                    evaluator_version=1,
                    parameters={"minimum": 1_000_000},
                    ruleset=DbRuleset.OSU,
                    active=True,
                )
            )
            await session.flush()
            achievement = await session.scalar(
                select(AchievementDefinition).where(AchievementDefinition.slug == "score-test-million-20260803")
            )
            assert achievement is not None
            session.add(
                AchievementTranslation(
                    achievement_id=achievement.id,
                    locale="en",
                    name="Million Score",
                    description="Reach one million",
                )
            )
        async with session_factory() as session:
            performance_release = await session.scalar(
                select(CalculationRelease)
                .join(CalculationFormula, CalculationFormula.id == CalculationRelease.formula_id)
                .where(
                    CalculationFormula.code == "official",
                    CalculationRelease.ruleset == DbRuleset.OSU,
                    CalculationRelease.active.is_(True),
                )
            )
            assert performance_release is not None

        uow_factory = SqlAlchemyUnitOfWorkFactory(session_factory)
        service = ScoringService(
            uow_factory,
            lambda session: SqlAlchemyScoringRepository(cast(AsyncSession, session)),
            lambda session: SqlAlchemyOutboxWriter(cast(AsyncSession, session)),
            lambda session: SqlAlchemyAccountSubmissionValidator(cast(AsyncSession, session)),
            lambda session: SqlAlchemyMultiplayerSubmissionValidator(cast(AsyncSession, session)),
            lambda session: SqlAlchemyPerformanceJobScheduler(cast(AsyncSession, session)),
            lambda session: TransactionAchievementAwarder(
                SqlAlchemySocialRepository(cast(AsyncSession, session)),
                SqlAlchemyOutboxWriter(cast(AsyncSession, session)),
                default_achievement_evaluator_registry(),
            ),
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
        assert [unlock.slug for unlock in first.new_achievement_unlocks] == ["score-test-million-20260803"]
        assert replayed.new_achievement_unlocks == ()
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
        shared = await service.accept(shared_replay)

        performance_relay = SqlAlchemyPerformanceJobRelayStore(
            session_factory,
            batch_size=10,
            lease_seconds=300,
            max_attempts=5,
            max_retry_seconds=300,
        )
        performance_claims = await performance_relay.claim("tests:performance-owner")
        assert len(performance_claims) == 2
        await performance_relay.record_enqueue_outcomes(
            [(performance_claims[0], RuntimeError("Redis unavailable"))],
            "tests:performance-owner",
        )
        await performance_relay.release((performance_claims[1],), "tests:performance-owner")

        async with session_factory.begin() as session:
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(AchievementUnlock)
                    .join(AchievementDefinition, AchievementDefinition.id == AchievementUnlock.achievement_id)
                    .where(AchievementDefinition.slug == "score-test-million-20260803")
                )
                == 1
            )
            events = (
                await session.scalars(
                    select(OutboxEvent)
                    .where(OutboxEvent.event_type == "score.accepted.v1")
                    .order_by(OutboxEvent.position)
                )
            ).all()
            for event in events:
                await project_accepted_score(session, event, "account:1:scoreboard:1")
                await project_scoring_stats(session, event, "account:1:scoreboard:1")
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

        replay_query = ReplayQueryService(
            SqlAlchemyUnitOfWorkFactory(session_factory),
            lambda session: SqlAlchemyScoringRepository(cast(AsyncSession, session)),
        )
        replay_service = ReplayService(
            SqlAlchemyUnitOfWorkFactory(session_factory),
            lambda session: SqlAlchemyScoringRepository(cast(AsyncSession, session)),
            lambda session: SqlAlchemyOutboxWriter(cast(AsyncSession, session)),
        )
        replay_reference = await replay_query.get(first.score_id)
        replay_view_request = uuid.uuid7()
        await replay_service.record_view(
            request_id=replay_view_request,
            replay=replay_reference,
            viewer_account_id=None,
        )
        await replay_service.record_view(
            request_id=replay_view_request,
            replay=replay_reference,
            viewer_account_id=None,
        )
        async with session_factory.begin() as session:
            replay_events = (
                await session.scalars(select(OutboxEvent).where(OutboxEvent.event_type == "score.replay-viewed.v1"))
            ).all()
            assert len(replay_events) == 1
            await project_scoring_stats(session, replay_events[0], "account:1:scoreboard:1")

        assert replayed == replace(first, new_achievement_unlocks=())
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
            assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 4
            assert await session.scalar(select(func.count()).select_from(LeaderboardEntry)) == 3
            play_stat = await session.get(UserPlayStat, {"account_id": 1, "scoreboard_id": 1})
            policy_id = await session.scalar(
                select(RankingPolicy.id).where(RankingPolicy.scoreboard_id == 1, RankingPolicy.is_default.is_(True))
            )
            assert policy_id is not None
            ranked_stat = await session.get(UserRankedStat, {"account_id": 1, "policy_id": policy_id})
            assert play_stat is not None
            assert ranked_stat is not None
            assert (play_stat.play_count, play_stat.total_score, play_stat.total_hits) == (2, 2_000_000, 20)
            assert play_stat.replay_views == 1
            assert ranked_stat.ranked_score == 1_000_000
            assert ranked_stat.performance == Decimal(0)
            assert ranked_stat.accuracy == Decimal(1)
            assert ranked_stat.grade_counts == {"XH": 1, "X": 0, "SH": 0, "S": 0, "A": 0}
            monthly = (
                await session.scalars(
                    select(UserMonthlyActivity)
                    .where(UserMonthlyActivity.account_id == 1)
                    .order_by(UserMonthlyActivity.month)
                )
            ).all()
            user_beatmap = await session.get(
                UserBeatmapActivity,
                {"account_id": 1, "beatmap_id": first.beatmap_id, "scoreboard_id": 1},
            )
            beatmap_activity = await session.scalar(
                select(BeatmapActivity).where(BeatmapActivity.beatmap_id == first.beatmap_id)
            )
            assert [(item.play_count, item.replay_views) for item in monthly] == [(2, 0), (0, 1)]
            assert user_beatmap is not None and (user_beatmap.attempt_count, user_beatmap.pass_count) == (2, 2)
            assert beatmap_activity is not None and (beatmap_activity.attempt_count, beatmap_activity.pass_count) == (
                2,
                2,
            )
            assert await session.scalar(select(func.count()).select_from(ReplayViewEvent)) == 1

            first_score = await session.get(Score, first.score_id)
            shared_score = await session.get(Score, shared.score_id)
            assert first_score is not None and shared_score is not None
            first_difficulty = BeatmapDifficultyAttribute(
                beatmap_revision_id=first_score.beatmap_revision_id,
                scoreboard_id=first_score.scoreboard_id,
                mod_set_id=first_score.mod_set_id,
                release_id=performance_release.id,
                star_rating=Decimal("1"),
                max_combo=first_score.max_combo,
            )
            shared_difficulty = BeatmapDifficultyAttribute(
                beatmap_revision_id=shared_score.beatmap_revision_id,
                scoreboard_id=shared_score.scoreboard_id,
                mod_set_id=shared_score.mod_set_id,
                release_id=performance_release.id,
                star_rating=Decimal("1"),
                max_combo=shared_score.max_combo,
            )
            session.add_all((first_difficulty, shared_difficulty))
            await session.flush()
            session.add_all(
                (
                    ScorePerformance(
                        score_id=first.score_id,
                        release_id=performance_release.id,
                        difficulty_attribute_id=first_difficulty.id,
                        pp=Decimal("100"),
                        input_digest=b"f" * 32,
                        output_digest=b"p" * 32,
                    ),
                    ScorePerformance(
                        score_id=shared.score_id,
                        release_id=performance_release.id,
                        difficulty_attribute_id=shared_difficulty.id,
                        pp=Decimal("200"),
                        input_digest=b"i" * 32,
                        output_digest=b"o" * 32,
                    ),
                )
            )
            await SqlAlchemyOutboxWriter(session).append(
                PendingEvent(
                    aggregate_type="score",
                    aggregate_id=str(shared.score_id),
                    event_type="score.performance-calculated.v1",
                    schema_version=1,
                    payload={
                        "score_id": shared.score_id,
                        "account_id": 1,
                        "scoreboard_id": 1,
                        "formula_id": str(performance_release.formula_id),
                        "formula_code": "official",
                        "release_id": str(performance_release.id),
                        "pp": "200",
                        "output_digest": (b"o" * 32).hex(),
                    },
                    consumers=("ranking-projector.v1",),
                    partition_key="account:1:scoreboard:1",
                )
            )
            await session.flush()
            performance_event = await session.scalar(
                select(OutboxEvent).where(
                    OutboxEvent.event_type == "score.performance-calculated.v1",
                    OutboxEvent.aggregate_id == str(shared.score_id),
                )
            )
            assert performance_event is not None
            await project_accepted_score(session, performance_event, "account:1:scoreboard:1")
            ranked_stat = await session.get(UserRankedStat, {"account_id": 1, "policy_id": policy_id})
            assert ranked_stat is not None
            await session.refresh(ranked_stat)
            assert ranked_stat.performance == Decimal("200")
            account_stats = await SqlAlchemyScoringRepository(session).get_account_stats(
                1,
                Ruleset.OSU,
                ScoreboardVariant.VANILLA,
            )
            assert (account_stats.performance, account_stats.global_rank) == (200, 1)

        failed_base = command()
        failed_command = replace(
            failed_base,
            meta=replace(
                failed_base.meta,
                request_id=uuid.uuid7(),
                idempotency_key="score:test:failed",
                request_digest=hashlib.sha256(b"score:failed").digest(),
            ),
            attempt=replace(
                failed_base.attempt,
                idempotency_key="attempt:test:failed",
                progress=Decimal("0.42"),
            ),
            score=replace(
                failed_base.score,
                total_score=0,
                classic_score=0,
                accuracy=Decimal("0.8"),
                max_combo=4,
                grade=ScoreGrade.F,
                outcome=ScoreOutcome.FAILED,
                perfect=False,
                hits=(
                    HitStatistic("great", 4),
                    HitStatistic("ok", 0),
                    HitStatistic("meh", 0),
                    HitStatistic("miss", 1),
                ),
                online_checksum=b"f" * 16,
            ),
        )
        failed_result = await service.accept(failed_command)
        async with session_factory.begin() as session:
            failed_event = await session.scalar(
                select(OutboxEvent).where(
                    OutboxEvent.event_type == "score.accepted.v1",
                    OutboxEvent.aggregate_id == str(failed_result.score_id),
                )
            )
            assert failed_event is not None
            await project_accepted_score(session, failed_event, "account:1:scoreboard:1")
            await project_scoring_stats(session, failed_event, "account:1:scoreboard:1")
        async with session_factory() as session:
            histogram = await session.get(
                BeatmapFailHistogram,
                {"beatmap_id": first.beatmap_id, "scoreboard_id": 1},
            )
            play_stat = await session.get(UserPlayStat, {"account_id": 1, "scoreboard_id": 1})
            user_beatmap = await session.get(
                UserBeatmapActivity,
                {"account_id": 1, "beatmap_id": first.beatmap_id, "scoreboard_id": 1},
            )
            assert histogram is not None and histogram.failed[42] == 1 and sum(histogram.quit) == 0
            assert play_stat is not None and (play_stat.play_count, play_stat.total_hits) == (3, 25)
            assert user_beatmap is not None and (user_beatmap.attempt_count, user_beatmap.pass_count) == (3, 2)
    finally:
        await engine.dispose()
