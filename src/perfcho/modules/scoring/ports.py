"""Define transaction-bound ports consumed by canonical score acceptance."""

import uuid
from datetime import datetime
from typing import Protocol

from perfcho.modules.common.ports import OutboxWriter, UnitOfWork
from perfcho.modules.scoring.models import (
    AcceptanceClaim,
    AcceptedScoreResult,
    AccountStatsView,
    AccountSubmissionContext,
    AttemptClaim,
    BeatmapGradeView,
    BeatmapReference,
    BeatmapRevisionInfo,
    LeaderboardPage,
    ModSetInfo,
    MultiplayerSubmissionContext,
    NormalizedModSet,
    PerformanceCalculationInput,
    PerformanceCompletion,
    PerformanceResult,
    PlayAttemptRecord,
    PlayAttemptSubmission,
    ReplayReference,
    Ruleset,
    ScoreAcceptanceRecord,
    ScoreboardInfo,
    ScoreboardVariant,
    ScorePerformanceView,
)


class ScoringUnitOfWork(UnitOfWork, Protocol):
    """Expose the transaction resource used to bind scoring adapters."""

    @property
    def session(self) -> object:
        """Return the active transaction resource."""
        ...


class ScoringRepository(Protocol):
    """Persist score facts without returning ORM entities."""

    async def claim_acceptance(
        self,
        *,
        idempotency_key: str,
        request_digest: bytes,
        now: datetime,
        expires_at: datetime,
    ) -> AcceptanceClaim:
        """Claim command idempotency or return an exact completed result."""
        ...

    async def resolve_current_revision(self, reference: BeatmapReference) -> BeatmapRevisionInfo | None:
        """Resolve only a current immutable beatmap revision."""
        ...

    async def get_scoreboard(self, ruleset: Ruleset, variant: ScoreboardVariant) -> ScoreboardInfo | None:
        """Return the active scoreboard for canonical gameplay dimensions."""
        ...

    async def get_or_create_mod_set(self, scoreboard_id: int, normalized: NormalizedModSet) -> ModSetInfo:
        """Resolve canonical mod JSON by its deterministic digest."""
        ...

    async def claim_attempt(self, record: PlayAttemptRecord) -> AttemptClaim:
        """Claim independent play-attempt idempotency and dimensions."""
        ...

    async def insert_score(self, record: ScoreAcceptanceRecord) -> AcceptedScoreResult:
        """Insert score, hits, replay, and attestation facts."""
        ...

    async def schedule_performance_calculations(
        self,
        *,
        score_id: int,
        scoreboard_id: int,
        ruleset: Ruleset,
        now: datetime,
    ) -> tuple[uuid.UUID, ...]:
        """Create one durable job for every enabled active formula release."""
        ...

    async def complete_acceptance(self, idempotency_key: str, result: AcceptedScoreResult) -> None:
        """Attach the non-secret accepted result to its command receipt."""
        ...

    async def get_score_performances(self, score_id: int) -> tuple[ScorePerformanceView, ...]:
        """Return every persisted Formula release result for one score."""
        ...

    async def get_replay(self, score_id: int) -> ReplayReference | None:
        """Resolve one ready replay and its score ownership."""
        ...

    async def record_replay_view(
        self,
        *,
        request_id: uuid.UUID,
        score_id: int,
        score_owner_account_id: int,
        viewer_account_id: int | None,
    ) -> None:
        """Idempotently append one replay view fact."""
        ...

    async def get_leaderboard(
        self,
        *,
        beatmap_id: int,
        ruleset: Ruleset,
        variant: ScoreboardVariant,
        leaderboard_type: int,
        legacy_mod_bits: int,
        requester_account_id: int,
        friend_account_ids: tuple[int, ...],
        limit: int,
    ) -> LeaderboardPage:
        """Return one bounded Stable leaderboard projection."""
        ...

    async def get_account_stats(
        self,
        account_id: int,
        ruleset: Ruleset,
        variant: ScoreboardVariant,
    ) -> AccountStatsView:
        """Aggregate Stable-facing account statistics without calculating PP."""
        ...

    async def get_beatmap_grades(
        self,
        account_id: int,
        beatmap_ids: tuple[int, ...],
    ) -> tuple[BeatmapGradeView, ...]:
        """Return projected vanilla personal-best grades for logical beatmaps."""
        ...


class AccountSubmissionValidator(Protocol):
    """Validate authoritative account lifecycle and scoring sanctions."""

    async def validate(self, account_id: int, *, at: datetime) -> AccountSubmissionContext:
        """Return current account facts or raise an application error."""
        ...


class MultiplayerSubmissionValidator(Protocol):
    """Validate and consume authoritative multiplayer score opportunities."""

    async def validate(
        self,
        context: MultiplayerSubmissionContext,
        *,
        account_id: int,
        revision: BeatmapRevisionInfo,
        scoreboard: ScoreboardInfo,
        mod_set: ModSetInfo,
        attempt: PlayAttemptSubmission,
        at: datetime,
    ) -> None:
        """Lock and validate one unconsumed context against score dimensions."""
        ...

    async def bind_score(
        self,
        context: MultiplayerSubmissionContext,
        *,
        play_attempt_id: uuid.UUID,
        score_id: int,
        at: datetime,
    ) -> None:
        """Consume and bind the validated opportunity in the same transaction."""
        ...


class PerformanceCalculator(Protocol):
    """Calculate difficulty and PP through a versioned external engine."""

    async def calculate(self, calculation: PerformanceCalculationInput, beatmap_content: bytes) -> PerformanceResult:
        """Return deterministic output for one immutable calculation input."""
        ...


class PerformanceCalculationRepository(Protocol):
    """Coordinate short transaction phases around external calculation I/O."""

    async def start(
        self,
        job_id: uuid.UUID,
        lease_token: uuid.UUID,
        *,
        now: datetime,
    ) -> PerformanceCalculationInput | None:
        """Fence and materialize one leased immutable calculation input."""
        ...

    async def complete(
        self,
        calculation: PerformanceCalculationInput,
        lease_token: uuid.UUID,
        result: PerformanceResult,
        *,
        output_digest: bytes,
        now: datetime,
    ) -> PerformanceCompletion | None:
        """Persist one idempotent result if the caller still owns the lease."""
        ...

    async def fail(
        self,
        job_id: uuid.UUID,
        lease_token: uuid.UUID,
        *,
        error: str,
        retry_at: datetime,
        dead: bool,
        consume_attempt: bool,
        now: datetime,
    ) -> None:
        """Release or dead-letter a failed fenced job."""
        ...


class AnticheatAnalyzer(Protocol):
    """Reserve the future asynchronous anti-cheat boundary without an implementation."""

    async def analyze(self, score_id: int, replay_sha256: bytes) -> None:
        """Analyze an accepted score without applying sanctions directly."""
        ...


class ScoringRepositoryFactory(Protocol):
    """Bind a scoring repository to one caller-owned transaction."""

    def __call__(self, session: object) -> ScoringRepository:
        """Return a transaction-bound repository."""
        ...


class PerformanceCalculationRepositoryFactory(Protocol):
    """Bind calculation persistence to one caller-owned transaction."""

    def __call__(self, session: object) -> PerformanceCalculationRepository:
        """Return a transaction-bound calculation repository."""
        ...


class AccountSubmissionValidatorFactory(Protocol):
    """Bind account validation to the scoring transaction."""

    def __call__(self, session: object) -> AccountSubmissionValidator:
        """Return a transaction-bound account validator."""
        ...


class MultiplayerSubmissionValidatorFactory(Protocol):
    """Bind multiplayer validation to the scoring transaction."""

    def __call__(self, session: object) -> MultiplayerSubmissionValidator:
        """Return a transaction-bound multiplayer validator."""
        ...


class ScoringOutboxWriterFactory(Protocol):
    """Bind an outbox writer to the scoring transaction."""

    def __call__(self, session: object) -> OutboxWriter:
        """Return a transaction-bound outbox writer."""
        ...
