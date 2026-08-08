"""Map play attempts, scores, replays, and ranking projections."""

import uuid
from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import (
    ARRAY,
    BigInteger,
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Identity,
    Index,
    Integer,
    LargeBinary,
    Numeric,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from perfcho.infra.db.base import DbBase
from perfcho.infra.db.enums import (
    AttemptStatus,
    CalculationKind,
    ClientFamily,
    Ruleset,
    ScoreboardVariant,
    ScoreGrade,
    ScoreOutcome,
    enum_type,
)
from perfcho.infra.db.mixins import BigIntIdentityMixin, CreatedAtMixin, TimestampMixin, Uuid7PrimaryKeyMixin


class Scoreboard(DbBase):
    """Defines ruleset and vanilla, relax, or autopilot ranking variants."""

    __tablename__ = "scoreboards"
    __table_args__ = (
        CheckConstraint(
            "variant = 'vanilla' OR ruleset IN ('osu', 'taiko', 'fruits')",
            name="variant_ruleset_compatibility",
        ),
        UniqueConstraint("ruleset", "variant"),
        UniqueConstraint("code"),
        {"schema": "scoring"},
    )

    id: Mapped[int] = mapped_column(SmallInteger, Identity(always=False), primary_key=True)
    code: Mapped[str] = mapped_column(String(32), nullable=False)
    ruleset: Mapped[Ruleset] = mapped_column(enum_type(Ruleset, "scoreboard_ruleset", 16), nullable=False)
    variant: Mapped[ScoreboardVariant] = mapped_column(
        enum_type(ScoreboardVariant, "scoreboard_variant", 16), nullable=False
    )
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="true")


class ModSet(BigIntIdentityMixin, CreatedAtMixin, DbBase):
    """Stores canonical Lazer mod settings together with Stable legacy bits."""

    __tablename__ = "mod_sets"
    __table_args__ = (
        CheckConstraint("legacy_bits >= 0", name="nonnegative_legacy_bits"),
        UniqueConstraint("id", "scoreboard_id", name="uq_mod_sets_id_scoreboard"),
        UniqueConstraint("scoreboard_id", "canonical_digest"),
        Index("ix_mod_sets_legacy", "scoreboard_id", "legacy_bits"),
        Index("ix_mod_sets_json", "canonical", postgresql_using="gin"),
        {"schema": "scoring"},
    )

    scoreboard_id: Mapped[int] = mapped_column(
        ForeignKey("scoring.scoreboards.id", ondelete="RESTRICT"), nullable=False
    )
    canonical: Mapped[list[dict[str, object]]] = mapped_column(JSONB, nullable=False)
    canonical_digest: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    legacy_bits: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default="0")


class ModPolicy(Uuid7PrimaryKeyMixin, CreatedAtMixin, DbBase):
    """Defines allowed, required, and forbidden mod rules for ranked contexts."""

    __tablename__ = "mod_policies"
    __table_args__ = (UniqueConstraint("digest"), {"schema": "scoring"})

    name: Mapped[str] = mapped_column(String(100), nullable=False)
    schema_version: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    rules: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    digest: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)


class CalculationFormula(Uuid7PrimaryKeyMixin, CreatedAtMixin, DbBase):
    """Names one user-visible calculation system and its calculator family."""

    __tablename__ = "calculation_formulas"
    __table_args__ = (
        UniqueConstraint("code"),
        Index("ix_calculation_formulas_kind_enabled", "kind", "enabled"),
        {"schema": "scoring"},
    )

    code: Mapped[str] = mapped_column(String(64), nullable=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    kind: Mapped[CalculationKind] = mapped_column(
        enum_type(CalculationKind, "calculation_formula_kind", 16), nullable=False
    )
    calculator: Mapped[str] = mapped_column(String(64), nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")


class CalculationFormulaScoreboard(DbBase):
    """Select the canonical scoreboards to which one Formula applies."""

    __tablename__ = "calculation_formula_scoreboards"
    __table_args__ = (
        Index("ix_calculation_formula_scoreboards_board", "scoreboard_id", "formula_id"),
        {"schema": "scoring"},
    )

    formula_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("scoring.calculation_formulas.id", ondelete="CASCADE"), primary_key=True
    )
    scoreboard_id: Mapped[int] = mapped_column(
        ForeignKey("scoring.scoreboards.id", ondelete="CASCADE"), primary_key=True
    )


class CalculationRelease(Uuid7PrimaryKeyMixin, CreatedAtMixin, DbBase):
    """Pins one formula to an immutable release configuration."""

    __tablename__ = "calculation_releases"
    __table_args__ = (
        UniqueConstraint("formula_id", "ruleset", "version", name="uq_calculation_releases_formula_version"),
        Index("ix_calculation_releases_active", "formula_id", "ruleset", "active"),
        Index(
            "uq_calculation_releases_active_formula_ruleset",
            "formula_id",
            "ruleset",
            unique=True,
            postgresql_where=text("active"),
        ),
        {"schema": "scoring"},
    )

    formula_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("scoring.calculation_formulas.id", ondelete="RESTRICT"), nullable=False
    )
    ruleset: Mapped[Ruleset] = mapped_column(enum_type(Ruleset, "calculation_ruleset", 16), nullable=False)
    version: Mapped[str] = mapped_column(String(64), nullable=False)
    configuration: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False, default=dict, server_default="{}")
    difficulty_release_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("scoring.calculation_releases.id", ondelete="RESTRICT")
    )
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")


class BeatmapDifficultyAttribute(BigIntIdentityMixin, CreatedAtMixin, DbBase):
    """Caches versioned difficulty attributes for a map revision and mod set."""

    __tablename__ = "beatmap_difficulty_attributes"
    __table_args__ = (
        CheckConstraint("star_rating >= 0 AND max_combo >= 0", name="nonnegative_values"),
        ForeignKeyConstraint(
            ["mod_set_id", "scoreboard_id"],
            ["scoring.mod_sets.id", "scoring.mod_sets.scoreboard_id"],
            name="fk_difficulty_attributes_mod_set_scoreboard",
            ondelete="RESTRICT",
        ),
        UniqueConstraint("beatmap_revision_id", "scoreboard_id", "mod_set_id", "release_id"),
        Index("ix_difficulty_attributes_revision", "beatmap_revision_id", "scoreboard_id"),
        {"schema": "scoring"},
    )

    beatmap_revision_id: Mapped[int] = mapped_column(
        ForeignKey("content.beatmap_revisions.id", ondelete="RESTRICT"), nullable=False
    )
    scoreboard_id: Mapped[int] = mapped_column(
        ForeignKey("scoring.scoreboards.id", ondelete="RESTRICT"), nullable=False
    )
    mod_set_id: Mapped[int] = mapped_column(nullable=False)
    release_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("scoring.calculation_releases.id", ondelete="RESTRICT"), nullable=False
    )
    star_rating: Mapped[Decimal] = mapped_column(Numeric(9, 5), nullable=False)
    max_combo: Mapped[int] = mapped_column(Integer, nullable=False)
    attributes: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False, default=dict, server_default="{}")


class PlayAttempt(Uuid7PrimaryKeyMixin, CreatedAtMixin, DbBase):
    """Records a normalized Stable or Lazer score submission attempt."""

    __tablename__ = "play_attempts"
    __table_args__ = (
        CheckConstraint("ended_at IS NULL OR ended_at >= started_at", name="valid_period"),
        CheckConstraint("progress BETWEEN 0 AND 1", name="progress_range"),
        ForeignKeyConstraint(
            ["beatmap_revision_id", "beatmap_id"],
            ["content.beatmap_revisions.id", "content.beatmap_revisions.beatmap_id"],
            name="fk_play_attempts_revision_beatmap",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["mod_set_id", "scoreboard_id"],
            ["scoring.mod_sets.id", "scoring.mod_sets.scoreboard_id"],
            name="fk_play_attempts_mod_set_scoreboard",
            ondelete="RESTRICT",
        ),
        UniqueConstraint("account_id", "protocol", "idempotency_key"),
        UniqueConstraint(
            "id",
            "account_id",
            "beatmap_id",
            "beatmap_revision_id",
            "scoreboard_id",
            "mod_set_id",
            name="uq_play_attempts_score_dimensions",
        ),
        Index("ix_play_attempts_account_started", "account_id", "started_at"),
        Index(
            "ix_play_attempts_account_map_started",
            "account_id",
            "beatmap_id",
            text("started_at DESC"),
            text("id DESC"),
        ),
        Index("ix_play_attempts_revision_started", "beatmap_revision_id", "started_at"),
        Index("ix_play_attempts_status_created", "status", "created_at"),
        {"schema": "scoring"},
    )

    account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="RESTRICT"), nullable=False)
    beatmap_id: Mapped[int] = mapped_column(ForeignKey("content.beatmaps.id", ondelete="RESTRICT"), nullable=False)
    beatmap_revision_id: Mapped[int] = mapped_column(nullable=False)
    scoreboard_id: Mapped[int] = mapped_column(
        ForeignKey("scoring.scoreboards.id", ondelete="RESTRICT"), nullable=False
    )
    mod_set_id: Mapped[int] = mapped_column(nullable=False)
    protocol: Mapped[ClientFamily] = mapped_column(enum_type(ClientFamily, "attempt_protocol", 16), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    status: Mapped[AttemptStatus] = mapped_column(enum_type(AttemptStatus, "play_attempt_status", 16), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    outcome: Mapped[ScoreOutcome | None] = mapped_column(enum_type(ScoreOutcome, "attempt_outcome", 16))
    progress: Mapped[Decimal] = mapped_column(Numeric(8, 7), nullable=False, default=0, server_default="0")
    client_metadata: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False, default=dict, server_default="{}")

    score: Mapped[Score | None] = relationship(back_populates="attempt", lazy="raise")


class PlayAttemptToken(Uuid7PrimaryKeyMixin, CreatedAtMixin, DbBase):
    """Stores single-use opaque tokens authorizing submission for a play attempt."""

    __tablename__ = "play_attempt_tokens"
    __table_args__ = (
        CheckConstraint("expires_at > created_at", name="valid_period"),
        CheckConstraint("consumed_at IS NULL OR consumed_at >= created_at", name="valid_consumed_at"),
        CheckConstraint("octet_length(token_digest) = 32", name="token_digest_length"),
        UniqueConstraint("token_digest"),
        Index(
            "uq_play_attempt_tokens_active_attempt",
            "attempt_id",
            unique=True,
            postgresql_where=text("consumed_at IS NULL AND revoked_at IS NULL"),
        ),
        Index("ix_play_attempt_tokens_expiry", "expires_at"),
        {"schema": "scoring"},
    )

    attempt_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("scoring.play_attempts.id", ondelete="CASCADE"), nullable=False
    )
    token_digest: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    token_prefix: Mapped[str] = mapped_column(String(16), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    consumed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class Score(BigIntIdentityMixin, CreatedAtMixin, DbBase):
    """Stores one verified and immutable normalized gameplay score."""

    __tablename__ = "scores"
    __table_args__ = (
        CheckConstraint("total_score >= 0 AND classic_score >= 0", name="nonnegative_scores"),
        CheckConstraint("accuracy BETWEEN 0 AND 1", name="accuracy_range"),
        CheckConstraint("max_combo >= 0", name="nonnegative_combo"),
        CheckConstraint("ended_at >= started_at", name="valid_period"),
        ForeignKeyConstraint(
            ["attempt_id", "account_id", "beatmap_id", "beatmap_revision_id", "scoreboard_id", "mod_set_id"],
            [
                "scoring.play_attempts.id",
                "scoring.play_attempts.account_id",
                "scoring.play_attempts.beatmap_id",
                "scoring.play_attempts.beatmap_revision_id",
                "scoring.play_attempts.scoreboard_id",
                "scoring.play_attempts.mod_set_id",
            ],
            name="fk_scores_attempt_dimensions",
            ondelete="RESTRICT",
        ),
        UniqueConstraint("attempt_id"),
        UniqueConstraint("online_checksum"),
        Index("ix_scores_account_board_ended", "account_id", "scoreboard_id", "ended_at"),
        Index(
            "ix_scores_account_map_ended",
            "account_id",
            "beatmap_id",
            text("ended_at DESC"),
            text("id DESC"),
        ),
        Index("ix_scores_revision_board", "beatmap_revision_id", "scoreboard_id", "id"),
        {"schema": "scoring"},
    )

    attempt_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="RESTRICT"), nullable=False)
    beatmap_id: Mapped[int] = mapped_column(ForeignKey("content.beatmaps.id", ondelete="RESTRICT"), nullable=False)
    beatmap_revision_id: Mapped[int] = mapped_column(
        ForeignKey("content.beatmap_revisions.id", ondelete="RESTRICT"), nullable=False
    )
    scoreboard_id: Mapped[int] = mapped_column(
        ForeignKey("scoring.scoreboards.id", ondelete="RESTRICT"), nullable=False
    )
    mod_set_id: Mapped[int] = mapped_column(ForeignKey("scoring.mod_sets.id", ondelete="RESTRICT"), nullable=False)
    total_score: Mapped[int] = mapped_column(BigInteger, nullable=False)
    classic_score: Mapped[int] = mapped_column(BigInteger, nullable=False)
    accuracy: Mapped[Decimal] = mapped_column(Numeric(10, 9), nullable=False)
    max_combo: Mapped[int] = mapped_column(Integer, nullable=False)
    grade: Mapped[ScoreGrade] = mapped_column(enum_type(ScoreGrade, "score_grade", 4), nullable=False)
    outcome: Mapped[ScoreOutcome] = mapped_column(enum_type(ScoreOutcome, "score_outcome", 16), nullable=False)
    perfect: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    client_flags: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default="0")
    online_checksum: Mapped[bytes | None] = mapped_column(LargeBinary(16))
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    ended_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    attempt: Mapped[PlayAttempt] = relationship(back_populates="score", lazy="raise")
    hit_statistics: Mapped[list[ScoreHitStatistic]] = relationship(back_populates="score", lazy="raise")
    performances: Mapped[list[ScorePerformance]] = relationship(back_populates="score", lazy="raise")
    replay: Mapped[Replay | None] = relationship(back_populates="score", lazy="raise")


class ScoreHitStatistic(DbBase):
    """Stores normalized Stable and Lazer hit results using extensible result names."""

    __tablename__ = "score_hit_statistics"
    __table_args__ = (
        CheckConstraint("actual >= 0", name="nonnegative_actual"),
        CheckConstraint("maximum IS NULL OR maximum >= actual", name="maximum_range"),
        {"schema": "scoring"},
    )

    score_id: Mapped[int] = mapped_column(ForeignKey("scoring.scores.id", ondelete="CASCADE"), primary_key=True)
    hit_result: Mapped[str] = mapped_column(String(32), primary_key=True)
    actual: Mapped[int] = mapped_column(Integer, nullable=False)
    maximum: Mapped[int | None] = mapped_column(Integer)

    score: Mapped[Score] = relationship(back_populates="hit_statistics", lazy="raise")


class ScorePerformance(CreatedAtMixin, DbBase):
    """Stores PP and breakdown values produced by a calculation release."""

    __tablename__ = "score_performances"
    __table_args__ = (
        CheckConstraint("pp >= 0", name="nonnegative_pp"),
        CheckConstraint("octet_length(input_digest) = 32", name="input_digest_length"),
        CheckConstraint("octet_length(output_digest) = 32", name="output_digest_length"),
        Index("ix_score_performances_release_pp", "release_id", "pp"),
        {"schema": "scoring"},
    )

    score_id: Mapped[int] = mapped_column(ForeignKey("scoring.scores.id", ondelete="CASCADE"), primary_key=True)
    release_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("scoring.calculation_releases.id", ondelete="RESTRICT"), primary_key=True
    )
    difficulty_attribute_id: Mapped[int] = mapped_column(
        ForeignKey("scoring.beatmap_difficulty_attributes.id", ondelete="RESTRICT"), nullable=False
    )
    pp: Mapped[Decimal] = mapped_column(Numeric(12, 5), nullable=False)
    breakdown: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False, default=dict, server_default="{}")
    input_digest: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    output_digest: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)

    score: Mapped[Score] = relationship(back_populates="performances", lazy="raise")


class RankingPolicy(Uuid7PrimaryKeyMixin, CreatedAtMixin, DbBase):
    """Defines versioned score selection, metric, and tie-breaking rules."""

    __tablename__ = "ranking_policies"
    __table_args__ = (
        UniqueConstraint("code", "version"),
        Index("ix_ranking_policies_active", "scoreboard_id", "active"),
        Index(
            "uq_ranking_policies_active_code",
            "code",
            unique=True,
            postgresql_where=text("active"),
        ),
        Index(
            "uq_ranking_policies_default_scoreboard",
            "scoreboard_id",
            unique=True,
            postgresql_where=text("active AND is_default"),
        ),
        {"schema": "scoring"},
    )

    code: Mapped[str] = mapped_column(String(64), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    scoreboard_id: Mapped[int] = mapped_column(
        ForeignKey("scoring.scoreboards.id", ondelete="RESTRICT"), nullable=False
    )
    metric: Mapped[str] = mapped_column(String(32), nullable=False)
    tie_breaker: Mapped[str] = mapped_column(String(32), nullable=False)
    mod_policy_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("scoring.mod_policies.id", ondelete="RESTRICT"), nullable=False
    )
    calculation_release_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("scoring.calculation_releases.id", ondelete="RESTRICT")
    )
    configuration: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False, default=dict, server_default="{}")
    is_default: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")


class ScoreEligibility(TimestampMixin, DbBase):
    """Projects whether a score is eligible, ineligible, or quarantined by a policy."""

    __tablename__ = "score_eligibility"
    __table_args__ = (
        Index("ix_score_eligibility_policy_state", "policy_id", "state", "score_id"),
        {"schema": "scoring"},
    )

    score_id: Mapped[int] = mapped_column(ForeignKey("scoring.scores.id", ondelete="CASCADE"), primary_key=True)
    policy_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("scoring.ranking_policies.id", ondelete="CASCADE"), primary_key=True
    )
    state: Mapped[str] = mapped_column(String(16), nullable=False)
    reason: Mapped[str | None] = mapped_column(String(100))
    input_version: Mapped[int] = mapped_column(Integer, nullable=False)


class LeaderboardEntry(BigIntIdentityMixin, TimestampMixin, DbBase):
    """Projects the best score per account for a ranking policy and map context."""

    __tablename__ = "leaderboard_entries"
    __table_args__ = (
        CheckConstraint("metric_value >= 0", name="nonnegative_metric"),
        CheckConstraint("country_code IS NULL OR country_code ~ '^[A-Z]{2}$'", name="country_code_format"),
        CheckConstraint(
            "(scope = 'exact_mods' AND filter_mod_set_id IS NOT NULL) OR "
            "(scope <> 'exact_mods' AND filter_mod_set_id IS NULL)",
            name="scope_mod_filter",
        ),
        Index(
            "uq_leaderboard_entries_context_user",
            "policy_id",
            "beatmap_id",
            "scope",
            "filter_mod_set_id",
            "account_id",
            unique=True,
            postgresql_nulls_not_distinct=True,
        ),
        Index(
            "ix_leaderboard_entries_rank",
            "policy_id",
            "beatmap_id",
            "scope",
            "filter_mod_set_id",
            text("metric_value DESC"),
            text("tie_break_value DESC"),
            text("score_id ASC"),
        ),
        Index(
            "ix_leaderboard_entries_country_rank",
            "policy_id",
            "beatmap_id",
            "scope",
            "filter_mod_set_id",
            "country_code",
            text("metric_value DESC"),
            text("tie_break_value DESC"),
            text("score_id ASC"),
            postgresql_where=text("country_code IS NOT NULL"),
        ),
        Index("ix_leaderboard_entries_account", "account_id", "policy_id", "beatmap_id"),
        Index("ix_leaderboard_entries_score", "score_id"),
        {"schema": "scoring"},
    )

    policy_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("scoring.ranking_policies.id", ondelete="CASCADE"), nullable=False
    )
    beatmap_id: Mapped[int] = mapped_column(ForeignKey("content.beatmaps.id", ondelete="CASCADE"), nullable=False)
    account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="CASCADE"), nullable=False)
    score_id: Mapped[int] = mapped_column(ForeignKey("scoring.scores.id", ondelete="RESTRICT"), nullable=False)
    country_code: Mapped[str | None] = mapped_column(String(2))
    scope: Mapped[str] = mapped_column(String(16), nullable=False, default="overall", server_default="overall")
    filter_mod_set_id: Mapped[int | None] = mapped_column(ForeignKey("scoring.mod_sets.id", ondelete="RESTRICT"))
    metric_value: Mapped[Decimal] = mapped_column(Numeric(30, 5), nullable=False)
    tie_break_value: Mapped[Decimal] = mapped_column(Numeric(30, 5), nullable=False)


class Replay(CreatedAtMixin, DbBase):
    """Stores the authoritative object-storage manifest and digest for a score replay."""

    __tablename__ = "replays"
    __table_args__ = (
        CheckConstraint("size_bytes >= 0", name="nonnegative_size"),
        Index("ix_replays_state_created", "state", "created_at"),
        {"schema": "scoring"},
    )

    score_id: Mapped[int] = mapped_column(ForeignKey("scoring.scores.id", ondelete="RESTRICT"), primary_key=True)
    format: Mapped[str] = mapped_column(String(32), nullable=False)
    sha256: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    storage_key: Mapped[str] = mapped_column(String(512), nullable=False)
    state: Mapped[str] = mapped_column(String(16), nullable=False, default="pending", server_default="pending")
    client_version: Mapped[str | None] = mapped_column(String(64))
    verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    score: Mapped[Score] = relationship(back_populates="replay", lazy="raise")


class ReplayViewEvent(BigIntIdentityMixin, CreatedAtMixin, DbBase):
    """Records replay views used to derive score and account statistics."""

    __tablename__ = "replay_view_events"
    __table_args__ = (
        UniqueConstraint("request_id"),
        Index("ix_replay_views_score_created", "score_id", "created_at"),
        Index("ix_replay_views_owner_created", "score_owner_account_id", "created_at"),
        {"schema": "scoring"},
    )

    request_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    score_id: Mapped[int] = mapped_column(ForeignKey("scoring.scores.id", ondelete="RESTRICT"), nullable=False)
    score_owner_account_id: Mapped[int] = mapped_column(
        ForeignKey("core.accounts.id", ondelete="RESTRICT"), nullable=False
    )
    viewer_account_id: Mapped[int | None] = mapped_column(ForeignKey("core.accounts.id", ondelete="SET NULL"))


class UserPlayStat(TimestampMixin, DbBase):
    """Projects cumulative factual play statistics for an account and scoreboard."""

    __tablename__ = "user_play_stats"
    __table_args__ = (
        CheckConstraint(
            "play_count >= 0 AND play_time_ms >= 0 AND total_score >= 0 AND total_hits >= 0 "
            "AND max_combo >= 0 AND replay_views >= 0",
            name="nonnegative_values",
        ),
        Index("ix_user_play_stats_score_rank", "scoreboard_id", "total_score", "account_id"),
        {"schema": "scoring"},
    )

    account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="CASCADE"), primary_key=True)
    scoreboard_id: Mapped[int] = mapped_column(
        ForeignKey("scoring.scoreboards.id", ondelete="CASCADE"), primary_key=True
    )
    play_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    play_time_ms: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default="0")
    total_score: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default="0")
    total_hits: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default="0")
    max_combo: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    replay_views: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default="0")
    source_event_id: Mapped[uuid.UUID | None] = mapped_column(nullable=True)


class UserRankedStat(TimestampMixin, DbBase):
    """Projects ranked score, performance, and accuracy for a ranking policy."""

    __tablename__ = "user_ranked_stats"
    __table_args__ = (
        CheckConstraint("ranked_score >= 0 AND performance >= 0", name="nonnegative_values"),
        CheckConstraint("accuracy BETWEEN 0 AND 1", name="accuracy_range"),
        Index("ix_user_ranked_stats_rank", "policy_id", "performance", "account_id"),
        Index("ix_user_ranked_stats_ranked_score", "policy_id", "ranked_score", "account_id"),
        {"schema": "scoring"},
    )

    account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="CASCADE"), primary_key=True)
    policy_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("scoring.ranking_policies.id", ondelete="CASCADE"), primary_key=True
    )
    ranked_score: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default="0")
    performance: Mapped[Decimal] = mapped_column(Numeric(14, 5), nullable=False, default=0, server_default="0")
    accuracy: Mapped[Decimal] = mapped_column(Numeric(10, 9), nullable=False, default=0, server_default="0")
    grade_counts: Mapped[dict[str, int]] = mapped_column(JSONB, nullable=False, default=dict, server_default="{}")
    source_event_id: Mapped[uuid.UUID | None] = mapped_column(nullable=True)


class UserMonthlyActivity(TimestampMixin, DbBase):
    """Projects monthly play time, play count, and replay views for an account."""

    __tablename__ = "user_monthly_activity"
    __table_args__ = (
        CheckConstraint("month = date_trunc('month', month)::date", name="month_start"),
        CheckConstraint("play_count >= 0 AND play_time_ms >= 0 AND replay_views >= 0", name="nonnegative_values"),
        {"schema": "scoring"},
    )

    account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="CASCADE"), primary_key=True)
    scoreboard_id: Mapped[int] = mapped_column(
        ForeignKey("scoring.scoreboards.id", ondelete="CASCADE"), primary_key=True
    )
    month: Mapped[date] = mapped_column(Date, primary_key=True)
    play_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    play_time_ms: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default="0")
    replay_views: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default="0")


class UserBeatmapActivity(TimestampMixin, DbBase):
    """Projects per-account play statistics for a beatmap and scoreboard."""

    __tablename__ = "user_beatmap_activity"
    __table_args__ = (
        CheckConstraint("attempt_count >= 0 AND pass_count >= 0 AND pass_count <= attempt_count", name="count_range"),
        Index("ix_user_beatmap_activity_most_played", "account_id", "scoreboard_id", "attempt_count"),
        {"schema": "scoring"},
    )

    account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="CASCADE"), primary_key=True)
    beatmap_id: Mapped[int] = mapped_column(ForeignKey("content.beatmaps.id", ondelete="CASCADE"), primary_key=True)
    scoreboard_id: Mapped[int] = mapped_column(
        ForeignKey("scoring.scoreboards.id", ondelete="CASCADE"), primary_key=True
    )
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    pass_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    last_played_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class BeatmapActivity(TimestampMixin, DbBase):
    """Projects public attempt and pass counts for a beatmap and scoreboard."""

    __tablename__ = "beatmap_activity"
    __table_args__ = (
        CheckConstraint("attempt_count >= 0 AND pass_count >= 0 AND pass_count <= attempt_count", name="count_range"),
        {"schema": "scoring"},
    )

    beatmap_id: Mapped[int] = mapped_column(ForeignKey("content.beatmaps.id", ondelete="CASCADE"), primary_key=True)
    scoreboard_id: Mapped[int] = mapped_column(
        ForeignKey("scoring.scoreboards.id", ondelete="CASCADE"), primary_key=True
    )
    attempt_count: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default="0")
    pass_count: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default="0")


class BeatmapFailHistogram(TimestampMixin, DbBase):
    """Projects failure and quit progress into percentile buckets."""

    __tablename__ = "beatmap_fail_histograms"
    __table_args__ = (
        CheckConstraint("array_length(failed, 1) = 100", name="failed_bucket_count"),
        CheckConstraint("array_length(quit, 1) = 100", name="quit_bucket_count"),
        {"schema": "scoring"},
    )

    beatmap_id: Mapped[int] = mapped_column(ForeignKey("content.beatmaps.id", ondelete="CASCADE"), primary_key=True)
    scoreboard_id: Mapped[int] = mapped_column(
        ForeignKey("scoring.scoreboards.id", ondelete="CASCADE"), primary_key=True
    )
    failed: Mapped[list[int]] = mapped_column(ARRAY(BigInteger), nullable=False)
    quit: Mapped[list[int]] = mapped_column(ARRAY(BigInteger), nullable=False)


class RankSnapshot(CreatedAtMixin, DbBase):
    """Stores daily global and country ranking snapshots for accounts."""

    __tablename__ = "rank_snapshots"
    __table_args__ = (
        CheckConstraint("global_rank > 0", name="positive_global_rank"),
        CheckConstraint("country_rank IS NULL OR country_rank > 0", name="positive_country_rank"),
        Index("ix_rank_snapshots_account_history", "account_id", "policy_id", "snapshot_date"),
        {"schema": "scoring"},
    )

    policy_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("scoring.ranking_policies.id", ondelete="CASCADE"), primary_key=True
    )
    snapshot_date: Mapped[date] = mapped_column(Date, primary_key=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="CASCADE"), primary_key=True)
    global_rank: Mapped[int] = mapped_column(Integer, nullable=False)
    country_rank: Mapped[int | None] = mapped_column(Integer)
    value: Mapped[Decimal] = mapped_column(Numeric(20, 5), nullable=False)


class ScoreAttestation(CreatedAtMixin, DbBase):
    """Stores client, checksum, integrity, and verification evidence for a score."""

    __tablename__ = "score_attestations"
    __table_args__ = (Index("ix_score_attestations_state", "verification_state", "created_at"), {"schema": "scoring"})

    score_id: Mapped[int] = mapped_column(ForeignKey("scoring.scores.id", ondelete="RESTRICT"), primary_key=True)
    client_family: Mapped[ClientFamily] = mapped_column(
        enum_type(ClientFamily, "attestation_client_family", 16), nullable=False
    )
    client_version: Mapped[str] = mapped_column(String(64), nullable=False)
    client_flags: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default="0")
    checksum: Mapped[bytes | None] = mapped_column(LargeBinary(32))
    client_integrity_digest: Mapped[bytes | None] = mapped_column(LargeBinary(32))
    verification_state: Mapped[str] = mapped_column(String(16), nullable=False)
    evidence: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False, default=dict, server_default="{}")
