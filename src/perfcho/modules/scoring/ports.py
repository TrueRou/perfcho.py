"""Define transaction-bound ports consumed by canonical score acceptance."""

import uuid
from datetime import datetime
from typing import Protocol

from perfcho.modules.common.ports import UnitOfWork
from perfcho.modules.scoring.models import (
    AcceptanceClaim,
    AcceptedScoreResult,
    AccountStatsView,
    AccountSubmissionContext,
    AttemptClaim,
    BeatmapGradeView,
    BeatmapReference,
    BeatmapRevisionInfo,
    LeaderboardScope,
    LeaderboardScoreView,
    ModSetInfo,
    MultiplayerSubmissionContext,
    NormalizedModSet,
    PlayAttemptRecord,
    PlayAttemptSubmission,
    ReplayReference,
    Ruleset,
    ScoreAcceptanceRecord,
    ScoreboardInfo,
    ScoreboardVariant,
    SoloScoreToken,
)


class ScoringUnitOfWork(UnitOfWork, Protocol):
    """Expose the transaction resource used to bind scoring adapters."""

    @property
    def session(self) -> object:
        """Return the active transaction resource."""
        ...


class ScoringAcceptanceRepository(Protocol):
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
        """Insert score, hits, optional replay, and attestation facts."""
        ...

    async def complete_acceptance(self, idempotency_key: str, result: AcceptedScoreResult) -> None:
        """Attach the non-secret accepted result to its command receipt."""
        ...

    async def issue_solo_token(
        self,
        *,
        account_id: int,
        revision: BeatmapRevisionInfo,
        ruleset: Ruleset,
        started_at: datetime,
        expires_at: datetime,
    ) -> SoloScoreToken:
        """Create a short-lived Lazer solo score authorization."""
        ...

    async def claim_solo_token(
        self,
        token_id: int,
        *,
        account_id: int,
        beatmap_id: int,
        ruleset: Ruleset,
        at: datetime,
    ) -> SoloScoreToken:
        """Lock and validate a Lazer solo token for submission."""
        ...

    async def complete_solo_token(self, token_id: int, score_id: int, *, at: datetime) -> None:
        """Bind a successfully accepted score to its consumed token."""
        ...

    async def accepted_result_for_score(self, score_id: int) -> AcceptedScoreResult | None:
        """Resolve the canonical accepted result for one immutable score."""
        ...


# Kept as a source-compatible alias for tests and non-query acceptance adapters.
ScoringRepository = ScoringAcceptanceRepository


class ReplayRepository(Protocol):
    """Read replays and append replay-view facts."""

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
    ) -> bool:
        """Idempotently append one replay view fact and report whether it was new."""
        ...


class RankingRepository(Protocol):
    """Read protocol-neutral leaderboard projections."""

    async def get_public_leaderboard(
        self,
        *,
        beatmap_id: int,
        ruleset: Ruleset,
        variant: ScoreboardVariant,
        scope: LeaderboardScope,
        limit: int,
    ) -> tuple[LeaderboardScoreView, ...]:
        """Return the public rows for one leaderboard scope."""
        ...

    async def get_personal_leaderboard(
        self,
        *,
        beatmap_id: int,
        ruleset: Ruleset,
        variant: ScoreboardVariant,
        scope: LeaderboardScope,
        account_id: int,
    ) -> LeaderboardScoreView | None:
        """Return one account's best row for one leaderboard scope."""
        ...


class AccountStatisticsRepository(Protocol):
    """Read projected account statistics."""

    async def get_account_stats(
        self,
        account_id: int,
        ruleset: Ruleset,
        variant: ScoreboardVariant,
    ) -> AccountStatsView:
        """Aggregate Stable-facing account statistics without calculating PP."""
        ...


class BeatmapScoresRepository(Protocol):
    """Read projected scores for bounded beatmap batches."""

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


class AnticheatAnalyzer(Protocol):
    """Reserve the future asynchronous anti-cheat boundary without an implementation."""

    async def analyze(self, score_id: int, replay_sha256: bytes) -> None:
        """Analyze an accepted score without applying sanctions directly."""
        ...


class ScoringAcceptanceRepositoryFactory(Protocol):
    """Bind a scoring repository to one caller-owned transaction."""

    def __call__(self, session: object) -> ScoringAcceptanceRepository:
        """Return a transaction-bound repository."""
        ...


class ReplayRepositoryFactory(Protocol):
    """Bind replay persistence to a caller-owned transaction."""

    def __call__(self, session: object) -> ReplayRepository:
        """Return a replay repository bound to the session."""
        ...


class RankingRepositoryFactory(Protocol):
    """Bind ranking queries to a caller-owned transaction."""

    def __call__(self, session: object) -> RankingRepository:
        """Return a ranking repository bound to the session."""
        ...


class AccountStatisticsRepositoryFactory(Protocol):
    """Bind account statistics queries to a caller-owned transaction."""

    def __call__(self, session: object) -> AccountStatisticsRepository:
        """Return an account statistics repository bound to the session."""
        ...


class BeatmapScoresRepositoryFactory(Protocol):
    """Bind beatmap score queries to a caller-owned transaction."""

    def __call__(self, session: object) -> BeatmapScoresRepository:
        """Return a beatmap scores repository bound to the session."""
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
