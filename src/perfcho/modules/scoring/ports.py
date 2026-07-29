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
    BeatmapReference,
    BeatmapRevisionInfo,
    LeaderboardPage,
    ModSetInfo,
    MultiplayerSubmissionContext,
    NormalizedModSet,
    PerformanceCalculationInput,
    PerformanceResult,
    PlayAttemptRecord,
    ReplayReference,
    Ruleset,
    ScoreAcceptanceRecord,
    ScoreboardInfo,
    ScoreboardVariant,
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
        """Insert score, hits, replay, attestation, and optional PP facts."""
        ...

    async def complete_acceptance(self, idempotency_key: str, result: AcceptedScoreResult) -> None:
        """Attach the non-secret accepted result to its command receipt."""
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
    """Calculate PP through an explicitly injected, versioned engine."""

    async def calculate(self, calculation: PerformanceCalculationInput) -> PerformanceResult | None:
        """Return a release-owned result, or defer calculation by returning none."""
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
