import uuid
from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
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
from sqlalchemy.orm import Mapped, mapped_column, relationship

from perfcho.infra.db.base import DbBase
from perfcho.infra.db.enums import BeatmapStatus, Ruleset, enum_type
from perfcho.infra.db.mixins import BigIntIdentityMixin, CreatedAtMixin, TimestampMixin, Uuid7PrimaryKeyMixin


class ContentSource(DbBase):
    """Defines namespaces for official, private, and local beatmap sources."""

    __tablename__ = "sources"
    __table_args__ = (UniqueConstraint("code"), {"schema": "content"})

    id: Mapped[int] = mapped_column(SmallInteger, Identity(always=False), primary_key=True)
    code: Mapped[str] = mapped_column(String(32), nullable=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    base_url: Mapped[str | None] = mapped_column(String(512))
    official: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")


class Beatmapset(BigIntIdentityMixin, TimestampMixin, DbBase):
    """Stores beatmapset-level metadata, source identity, and ranking state."""

    __tablename__ = "beatmapsets"
    __table_args__ = (
        CheckConstraint("external_id > 0", name="positive_external_id"),
        UniqueConstraint("source_id", "external_id"),
        Index("ix_beatmapsets_creator", "creator_account_id", "id"),
        Index("ix_beatmapsets_status_ranked", "status", "ranked_at"),
        {"schema": "content"},
    )

    source_id: Mapped[int] = mapped_column(ForeignKey("content.sources.id", ondelete="RESTRICT"), nullable=False)
    external_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    creator_account_id: Mapped[int | None] = mapped_column(ForeignKey("core.accounts.id", ondelete="SET NULL"))
    creator_external_id: Mapped[int | None] = mapped_column(BigInteger)
    creator_name: Mapped[str] = mapped_column(String(64), nullable=False)
    artist: Mapped[str] = mapped_column(String(255), nullable=False)
    artist_unicode: Mapped[str | None] = mapped_column(String(255))
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    title_unicode: Mapped[str | None] = mapped_column(String(255))
    source_text: Mapped[str | None] = mapped_column(String(255))
    tags: Mapped[str] = mapped_column(Text, nullable=False, default="", server_default="")
    genre_id: Mapped[int | None] = mapped_column(SmallInteger)
    language_id: Mapped[int | None] = mapped_column(SmallInteger)
    description: Mapped[str | None] = mapped_column(Text)
    status: Mapped[BeatmapStatus] = mapped_column(
        enum_type(BeatmapStatus, "beatmapset_status", 16),
        nullable=False,
        default=BeatmapStatus.PENDING,
        server_default=BeatmapStatus.PENDING.value,
    )
    submitted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ranked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_source_update_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    available: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="true")
    nsfw: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")

    beatmaps: Mapped[list[Beatmap]] = relationship(back_populates="beatmapset", lazy="raise")


class BeatmapsetAsset(BigIntIdentityMixin, CreatedAtMixin, DbBase):
    """Associates beatmapsets with cover, preview, and related media assets."""

    __tablename__ = "beatmapset_assets"
    __table_args__ = (
        UniqueConstraint("beatmapset_id", "kind", "scale"),
        Index("ix_beatmapset_assets_asset", "asset_id"),
        {"schema": "content"},
    )

    beatmapset_id: Mapped[int] = mapped_column(ForeignKey("content.beatmapsets.id", ondelete="CASCADE"), nullable=False)
    asset_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("core.media_assets.id", ondelete="RESTRICT"), nullable=False)
    kind: Mapped[str] = mapped_column(String(32), nullable=False)
    scale: Mapped[str] = mapped_column(String(16), nullable=False, default="1x", server_default="1x")


class Beatmap(BigIntIdentityMixin, TimestampMixin, DbBase):
    """Represents a logical beatmap difficulty independently of its file revisions."""

    __tablename__ = "beatmaps"
    __table_args__ = (
        CheckConstraint("external_id > 0", name="positive_external_id"),
        UniqueConstraint("source_id", "external_id"),
        Index("ix_beatmaps_set_id", "beatmapset_id", "id"),
        Index("ix_beatmaps_status_ruleset", "status", "ruleset"),
        {"schema": "content"},
    )

    beatmapset_id: Mapped[int] = mapped_column(
        ForeignKey("content.beatmapsets.id", ondelete="RESTRICT"), nullable=False
    )
    source_id: Mapped[int] = mapped_column(ForeignKey("content.sources.id", ondelete="RESTRICT"), nullable=False)
    external_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    ruleset: Mapped[Ruleset] = mapped_column(enum_type(Ruleset, "beatmap_ruleset", 16), nullable=False)
    difficulty_name: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[BeatmapStatus] = mapped_column(
        enum_type(BeatmapStatus, "beatmap_status", 16),
        nullable=False,
        default=BeatmapStatus.PENDING,
        server_default=BeatmapStatus.PENDING.value,
    )
    status_locked: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    beatmapset: Mapped[Beatmapset] = relationship(back_populates="beatmaps", lazy="raise")
    revisions: Mapped[list[BeatmapRevision]] = relationship(back_populates="beatmap", lazy="raise")


class BeatmapOwner(DbBase):
    """Associates beatmaps with their creators and collaborating mappers."""

    __tablename__ = "beatmap_owners"
    __table_args__ = (Index("ix_beatmap_owners_account", "account_id", "beatmap_id"), {"schema": "content"})

    beatmap_id: Mapped[int] = mapped_column(ForeignKey("content.beatmaps.id", ondelete="CASCADE"), primary_key=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="RESTRICT"), primary_key=True)
    role: Mapped[str] = mapped_column(String(32), primary_key=True)


class BeatmapRevision(BigIntIdentityMixin, CreatedAtMixin, DbBase):
    """Stores immutable beatmap file revisions, hashes, and base difficulty attributes."""

    __tablename__ = "beatmap_revisions"
    __table_args__ = (
        CheckConstraint("total_length_ms >= 0", name="nonnegative_total_length"),
        CheckConstraint("drain_length_ms >= 0", name="nonnegative_drain_length"),
        CheckConstraint("bpm >= 0", name="nonnegative_bpm"),
        CheckConstraint("circle_size BETWEEN 0 AND 20", name="circle_size_range"),
        CheckConstraint("overall_difficulty BETWEEN 0 AND 20", name="overall_difficulty_range"),
        CheckConstraint("approach_rate BETWEEN 0 AND 20", name="approach_rate_range"),
        CheckConstraint("health_drain BETWEEN 0 AND 20", name="health_drain_range"),
        CheckConstraint("object_count >= 0 AND max_combo >= 0", name="nonnegative_counts"),
        UniqueConstraint("beatmap_id", "sha256"),
        Index("ix_beatmap_revisions_md5", "md5"),
        Index(
            "uq_beatmap_revisions_current",
            "beatmap_id",
            unique=True,
            postgresql_where=text("is_current"),
        ),
        {"schema": "content"},
    )

    beatmap_id: Mapped[int] = mapped_column(ForeignKey("content.beatmaps.id", ondelete="RESTRICT"), nullable=False)
    md5: Mapped[bytes] = mapped_column(LargeBinary(16), nullable=False)
    sha256: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    file_name: Mapped[str] = mapped_column(String(255), nullable=False)
    source_updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    total_length_ms: Mapped[int] = mapped_column(Integer, nullable=False)
    drain_length_ms: Mapped[int] = mapped_column(Integer, nullable=False)
    bpm: Mapped[Decimal] = mapped_column(Numeric(10, 3), nullable=False)
    circle_size: Mapped[Decimal] = mapped_column(Numeric(5, 2), nullable=False)
    overall_difficulty: Mapped[Decimal] = mapped_column(Numeric(5, 2), nullable=False)
    approach_rate: Mapped[Decimal] = mapped_column(Numeric(5, 2), nullable=False)
    health_drain: Mapped[Decimal] = mapped_column(Numeric(5, 2), nullable=False)
    object_count: Mapped[int] = mapped_column(Integer, nullable=False)
    circle_count: Mapped[int] = mapped_column(Integer, nullable=False)
    slider_count: Mapped[int] = mapped_column(Integer, nullable=False)
    spinner_count: Mapped[int] = mapped_column(Integer, nullable=False)
    max_combo: Mapped[int] = mapped_column(Integer, nullable=False)
    has_storyboard: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    has_video: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    is_current: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="true")

    beatmap: Mapped[Beatmap] = relationship(back_populates="revisions", lazy="raise")


class BeatmapStatusEvent(BigIntIdentityMixin, CreatedAtMixin, DbBase):
    """Records every beatmap ranking status transition and its source."""

    __tablename__ = "beatmap_status_events"
    __table_args__ = (
        Index("ix_beatmap_status_events_beatmap_effective", "beatmap_id", "effective_at"),
        {"schema": "content"},
    )

    beatmap_id: Mapped[int] = mapped_column(ForeignKey("content.beatmaps.id", ondelete="RESTRICT"), nullable=False)
    previous_status: Mapped[BeatmapStatus | None] = mapped_column(
        enum_type(BeatmapStatus, "previous_beatmap_status", 16)
    )
    status: Mapped[BeatmapStatus] = mapped_column(enum_type(BeatmapStatus, "new_beatmap_status", 16), nullable=False)
    actor_account_id: Mapped[int | None] = mapped_column(ForeignKey("core.accounts.id", ondelete="SET NULL"))
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    reason: Mapped[str | None] = mapped_column(String(255))
    effective_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class ContentSyncState(TimestampMixin, DbBase):
    """Tracks upstream beatmapset synchronization watermarks and retry state."""

    __tablename__ = "sync_states"
    __table_args__ = (
        CheckConstraint("unchanged_count >= 0 AND error_count >= 0", name="nonnegative_counts"),
        Index("ix_sync_states_next_check", "next_check_at"),
        {"schema": "content"},
    )

    beatmapset_id: Mapped[int] = mapped_column(
        ForeignKey("content.beatmapsets.id", ondelete="CASCADE"), primary_key=True
    )
    etag: Mapped[str | None] = mapped_column(String(255))
    last_modified: Mapped[str | None] = mapped_column(String(255))
    last_checked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    next_check_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    unchanged_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    error_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    last_error: Mapped[str | None] = mapped_column(Text)


class BeatmapsetFavourite(CreatedAtMixin, DbBase):
    """Records beatmapsets favourited by accounts."""

    __tablename__ = "beatmapset_favourites"
    __table_args__ = (
        Index("ix_beatmapset_favourites_set_created", "beatmapset_id", "created_at"),
        {"schema": "content"},
    )

    account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="RESTRICT"), primary_key=True)
    beatmapset_id: Mapped[int] = mapped_column(
        ForeignKey("content.beatmapsets.id", ondelete="RESTRICT"), primary_key=True
    )


class RatingVote(BigIntIdentityMixin, TimestampMixin, DbBase):
    """Records account ratings for beatmapsets or specific beatmap revisions."""

    __tablename__ = "rating_votes"
    __table_args__ = (
        CheckConstraint("rating BETWEEN 1 AND 10", name="rating_range"),
        CheckConstraint("num_nonnulls(beatmapset_id, beatmap_revision_id) = 1", name="single_target"),
        Index(
            "uq_rating_votes_set_account",
            "account_id",
            "beatmapset_id",
            unique=True,
            postgresql_where=text("beatmapset_id IS NOT NULL"),
        ),
        Index(
            "uq_rating_votes_revision_account",
            "account_id",
            "beatmap_revision_id",
            unique=True,
            postgresql_where=text("beatmap_revision_id IS NOT NULL"),
        ),
        Index("ix_rating_votes_set_rating", "beatmapset_id", "rating"),
        Index("ix_rating_votes_revision_rating", "beatmap_revision_id", "rating"),
        {"schema": "content"},
    )

    account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="RESTRICT"), nullable=False)
    beatmapset_id: Mapped[int | None] = mapped_column(ForeignKey("content.beatmapsets.id", ondelete="RESTRICT"))
    beatmap_revision_id: Mapped[int | None] = mapped_column(
        ForeignKey("content.beatmap_revisions.id", ondelete="RESTRICT")
    )
    rating: Mapped[int] = mapped_column(SmallInteger, nullable=False)


class TagDefinition(DbBase):
    """Defines tags used to classify and discover beatmaps."""

    __tablename__ = "tag_definitions"
    __table_args__ = (UniqueConstraint("slug"), {"schema": "content"})

    id: Mapped[int] = mapped_column(Integer, Identity(always=False), primary_key=True)
    slug: Mapped[str] = mapped_column(String(64), nullable=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    ruleset: Mapped[Ruleset | None] = mapped_column(enum_type(Ruleset, "tag_ruleset", 16))


class BeatmapTagVote(CreatedAtMixin, DbBase):
    """Records user votes assigning tags to beatmaps."""

    __tablename__ = "beatmap_tag_votes"
    __table_args__ = (Index("ix_beatmap_tag_votes_count", "beatmap_id", "tag_id"), {"schema": "content"})

    beatmap_id: Mapped[int] = mapped_column(ForeignKey("content.beatmaps.id", ondelete="CASCADE"), primary_key=True)
    tag_id: Mapped[int] = mapped_column(ForeignKey("content.tag_definitions.id", ondelete="CASCADE"), primary_key=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="RESTRICT"), primary_key=True)


class MapStatusRequest(Uuid7PrimaryKeyMixin, CreatedAtMixin, DbBase):
    """Tracks requested beatmap status changes and their resolution workflow."""

    __tablename__ = "map_status_requests"
    __table_args__ = (
        Index(
            "uq_map_status_requests_open",
            "beatmap_id",
            "requester_account_id",
            unique=True,
            postgresql_where=text("status = 'open'"),
        ),
        Index("ix_map_status_requests_queue", "status", "created_at"),
        {"schema": "content"},
    )

    beatmap_id: Mapped[int] = mapped_column(ForeignKey("content.beatmaps.id", ondelete="RESTRICT"), nullable=False)
    requester_account_id: Mapped[int] = mapped_column(
        ForeignKey("core.accounts.id", ondelete="RESTRICT"), nullable=False
    )
    requested_status: Mapped[BeatmapStatus] = mapped_column(
        enum_type(BeatmapStatus, "requested_beatmap_status", 16), nullable=False
    )
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="open", server_default="open")
    reason: Mapped[str | None] = mapped_column(Text)
    resolved_by_id: Mapped[int | None] = mapped_column(ForeignKey("core.accounts.id", ondelete="SET NULL"))
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    resolution: Mapped[str | None] = mapped_column(Text)


class Comment(BigIntIdentityMixin, CreatedAtMixin, DbBase):
    """Stores timeline comments targeting scores, beatmaps, or beatmapsets."""

    __tablename__ = "comments"
    __table_args__ = (
        CheckConstraint("num_nonnulls(score_id, beatmap_id, beatmapset_id) = 1", name="single_target"),
        CheckConstraint("position_ms IS NULL OR position_ms >= 0", name="nonnegative_position"),
        CheckConstraint("char_length(body) BETWEEN 1 AND 1000", name="body_length"),
        CheckConstraint("color IS NULL OR color ~ '^#[0-9A-Fa-f]{6}$'", name="color_format"),
        Index("ix_comments_score_position", "score_id", "position_ms", "id"),
        Index("ix_comments_beatmap_position", "beatmap_id", "position_ms", "id"),
        Index("ix_comments_set_position", "beatmapset_id", "position_ms", "id"),
        {"schema": "content"},
    )

    author_account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="RESTRICT"), nullable=False)
    score_id: Mapped[int | None] = mapped_column(ForeignKey("scoring.scores.id", ondelete="RESTRICT"))
    beatmap_id: Mapped[int | None] = mapped_column(ForeignKey("content.beatmaps.id", ondelete="RESTRICT"))
    beatmapset_id: Mapped[int | None] = mapped_column(ForeignKey("content.beatmapsets.id", ondelete="RESTRICT"))
    position_ms: Mapped[int | None] = mapped_column(Integer)
    body: Mapped[str] = mapped_column(Text, nullable=False)
    color: Mapped[str | None] = mapped_column(String(7))
    moderation_state: Mapped[str] = mapped_column(
        String(16), nullable=False, default="visible", server_default="visible"
    )
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
