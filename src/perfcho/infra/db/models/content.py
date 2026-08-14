"""Map immutable beatmap content and community metadata."""

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
from perfcho.infra.db.enums import BeatmapStatus, BeatmapStatusEventSource, Ruleset, enum_type
from perfcho.infra.db.mixins import BigIntIdentityMixin, CreatedAtMixin, TimestampMixin


class ContentSource(DbBase):
    """Defines namespaces for official, private, and local beatmap sources."""

    __tablename__ = "source"
    __table_args__ = (UniqueConstraint("code"), {"schema": "content"})

    id: Mapped[int] = mapped_column(SmallInteger, Identity(always=False), primary_key=True)
    code: Mapped[str] = mapped_column(String(32), nullable=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    base_url: Mapped[str | None] = mapped_column(String(512))
    official: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")


class Beatmapset(BigIntIdentityMixin, TimestampMixin, DbBase):
    """Stores beatmapset-level metadata, source identity, and ranking state."""

    __tablename__ = "beatmapset"
    __table_args__ = (
        CheckConstraint("external_id > 0", name="positive_external_id"),
        UniqueConstraint("source_id", "external_id"),
        Index("ix_beatmapset_creator", "creator_account_id", "id"),
        Index("ix_beatmapset_status_ranked", "status", "ranked_at"),
        {"schema": "content"},
    )

    source_id: Mapped[int] = mapped_column(ForeignKey("content.source.id", ondelete="RESTRICT"), nullable=False)
    external_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    creator_account_id: Mapped[int | None] = mapped_column(ForeignKey("core.account.id", ondelete="SET NULL"))
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
        enum_type(BeatmapStatus, "beatmap_status", 16),
        nullable=False,
        default=BeatmapStatus.PENDING,
        server_default=BeatmapStatus.PENDING.value,
    )
    source_status: Mapped[BeatmapStatus] = mapped_column(
        enum_type(BeatmapStatus, "source_beatmap_status", 16),
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

    __tablename__ = "beatmapset_asset"
    __table_args__ = (
        UniqueConstraint("beatmapset_id", "kind", "scale"),
        Index("ix_beatmapset_asset_asset", "asset_id"),
        {"schema": "content"},
    )

    beatmapset_id: Mapped[int] = mapped_column(ForeignKey("content.beatmapset.id", ondelete="CASCADE"), nullable=False)
    asset_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("core.media_asset.id", ondelete="RESTRICT"), nullable=False)
    kind: Mapped[str] = mapped_column(String(32), nullable=False)
    scale: Mapped[str] = mapped_column(String(16), nullable=False, default="1x", server_default="1x")


class Beatmap(BigIntIdentityMixin, TimestampMixin, DbBase):
    """Represents a logical beatmap difficulty independently of its file revisions."""

    __tablename__ = "beatmap"
    __table_args__ = (
        CheckConstraint("external_id > 0", name="positive_external_id"),
        UniqueConstraint("source_id", "external_id"),
        Index("ix_beatmap_set_id", "beatmapset_id", "id"),
        {"schema": "content"},
    )

    beatmapset_id: Mapped[int] = mapped_column(ForeignKey("content.beatmapset.id", ondelete="RESTRICT"), nullable=False)
    source_id: Mapped[int] = mapped_column(ForeignKey("content.source.id", ondelete="RESTRICT"), nullable=False)
    external_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    ruleset: Mapped[Ruleset] = mapped_column(enum_type(Ruleset, "beatmap_ruleset", 16), nullable=False)
    difficulty_name: Mapped[str] = mapped_column(String(255), nullable=False)
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    beatmapset: Mapped[Beatmapset] = relationship(back_populates="beatmaps", lazy="raise")
    revisions: Mapped[list[BeatmapRevision]] = relationship(back_populates="beatmap", lazy="raise")


class BeatmapOwner(DbBase):
    """Associates beatmaps with their creators and collaborating mappers."""

    __tablename__ = "beatmap_owner"
    __table_args__ = (Index("ix_beatmap_owner_account", "account_id", "beatmap_id"), {"schema": "content"})

    beatmap_id: Mapped[int] = mapped_column(ForeignKey("content.beatmap.id", ondelete="CASCADE"), primary_key=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("core.account.id", ondelete="RESTRICT"), primary_key=True)
    role: Mapped[str] = mapped_column(String(32), primary_key=True)


class BeatmapRevision(BigIntIdentityMixin, CreatedAtMixin, DbBase):
    """Stores immutable beatmap file revisions, hashes, and base difficulty attributes."""

    __tablename__ = "beatmap_revision"
    __table_args__ = (
        CheckConstraint("total_length_ms >= 0", name="nonnegative_total_length"),
        CheckConstraint("drain_length_ms >= 0", name="nonnegative_drain_length"),
        CheckConstraint("bpm >= 0", name="nonnegative_bpm"),
        CheckConstraint("circle_size BETWEEN 0 AND 20", name="circle_size_range"),
        CheckConstraint("overall_difficulty BETWEEN 0 AND 20", name="overall_difficulty_range"),
        CheckConstraint("approach_rate BETWEEN 0 AND 20", name="approach_rate_range"),
        CheckConstraint("health_drain BETWEEN 0 AND 20", name="health_drain_range"),
        CheckConstraint("object_count >= 0 AND max_combo >= 0", name="nonnegative_counts"),
        CheckConstraint("octet_length(md5) = 16", name="md5_length"),
        CheckConstraint("octet_length(sha256) = 32", name="sha256_length"),
        CheckConstraint("char_length(file_name) > 0", name="nonempty_file_name"),
        CheckConstraint("file_name = btrim(file_name)", name="trimmed_file_name"),
        CheckConstraint("file_name_key = lower(file_name)", name="normalized_file_name"),
        CheckConstraint("file_name !~ '[/\\\\]'", name="file_name_is_basename"),
        UniqueConstraint("id", "beatmap_id", name="uq_beatmap_revision_id_beatmap"),
        UniqueConstraint("beatmap_id", "sha256"),
        UniqueConstraint("md5", name="uq_beatmap_revision_md5"),
        Index("ix_beatmap_revision_file_name_key", "file_name_key", "beatmap_id"),
        Index(
            "uq_beatmap_revision_current",
            "beatmap_id",
            unique=True,
            postgresql_where=text("is_current"),
        ),
        {"schema": "content"},
    )

    beatmap_id: Mapped[int] = mapped_column(ForeignKey("content.beatmap.id", ondelete="RESTRICT"), nullable=False)
    file_asset_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("core.media_asset.id", ondelete="RESTRICT"), unique=True
    )
    md5: Mapped[bytes] = mapped_column(LargeBinary(16), nullable=False)
    sha256: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    file_name: Mapped[str] = mapped_column(String(255), nullable=False)
    file_name_key: Mapped[str] = mapped_column(String(255), nullable=False)
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


class BeatmapsetStatusEvent(BigIntIdentityMixin, CreatedAtMixin, DbBase):
    """Records every beatmapset ranking status transition and its source."""

    __tablename__ = "beatmapset_status_event"
    __table_args__ = (
        Index("ix_beatmapset_status_event_set_effective", "beatmapset_id", "effective_at"),
        {"schema": "content"},
    )

    beatmapset_id: Mapped[int] = mapped_column(ForeignKey("content.beatmapset.id", ondelete="RESTRICT"), nullable=False)
    previous_status: Mapped[BeatmapStatus | None] = mapped_column(
        enum_type(BeatmapStatus, "previous_beatmap_status", 16)
    )
    status: Mapped[BeatmapStatus] = mapped_column(enum_type(BeatmapStatus, "new_beatmap_status", 16), nullable=False)
    source: Mapped[BeatmapStatusEventSource] = mapped_column(
        enum_type(BeatmapStatusEventSource, "beatmap_status_event_source", 16), nullable=False
    )
    actor_account_id: Mapped[int | None] = mapped_column(ForeignKey("core.account.id", ondelete="SET NULL"))
    reason: Mapped[str | None] = mapped_column(String(255))
    effective_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class BeatmapsetSyncState(TimestampMixin, DbBase):
    """Tracks upstream beatmapset synchronization lease and retry state."""

    __tablename__ = "sync_state"
    __table_args__ = (
        CheckConstraint("error_count >= 0", name="nonnegative_error_count"),
        Index("ix_sync_state_next_check", "next_check_at"),
        {"schema": "content"},
    )

    beatmapset_id: Mapped[int] = mapped_column(
        ForeignKey("content.beatmapset.id", ondelete="CASCADE"), primary_key=True
    )
    last_checked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    next_check_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")


class BeatmapsetFavourite(CreatedAtMixin, DbBase):
    """Records beatmapsets favourited by accounts."""

    __tablename__ = "beatmapset_favourite"
    __table_args__ = (
        Index("ix_beatmapset_favourite_set_created", "beatmapset_id", "created_at"),
        {"schema": "content"},
    )

    account_id: Mapped[int] = mapped_column(ForeignKey("core.account.id", ondelete="RESTRICT"), primary_key=True)
    beatmapset_id: Mapped[int] = mapped_column(
        ForeignKey("content.beatmapset.id", ondelete="RESTRICT"), primary_key=True
    )


class RatingVote(BigIntIdentityMixin, TimestampMixin, DbBase):
    """Records account ratings for beatmapsets or logical beatmaps."""

    __tablename__ = "rating_vote"
    __table_args__ = (
        CheckConstraint("rating BETWEEN 1 AND 10", name="rating_range"),
        CheckConstraint("num_nonnulls(beatmapset_id, beatmap_id) = 1", name="single_target"),
        Index(
            "uq_rating_vote_set_account",
            "account_id",
            "beatmapset_id",
            unique=True,
            postgresql_where=text("beatmapset_id IS NOT NULL"),
        ),
        Index(
            "uq_rating_vote_beatmap_account",
            "account_id",
            "beatmap_id",
            unique=True,
            postgresql_where=text("beatmap_id IS NOT NULL"),
        ),
        Index("ix_rating_vote_set_rating", "beatmapset_id", "rating"),
        Index("ix_rating_vote_beatmap_rating", "beatmap_id", "rating"),
        {"schema": "content"},
    )

    account_id: Mapped[int] = mapped_column(ForeignKey("core.account.id", ondelete="RESTRICT"), nullable=False)
    beatmapset_id: Mapped[int | None] = mapped_column(ForeignKey("content.beatmapset.id", ondelete="RESTRICT"))
    beatmap_id: Mapped[int | None] = mapped_column(ForeignKey("content.beatmap.id", ondelete="RESTRICT"))
    rating: Mapped[int] = mapped_column(SmallInteger, nullable=False)


class TagDefinition(DbBase):
    """Defines tags used to classify and discover beatmaps."""

    __tablename__ = "tag_definition"
    __table_args__ = (UniqueConstraint("slug"), {"schema": "content"})

    id: Mapped[int] = mapped_column(Integer, Identity(always=False), primary_key=True)
    slug: Mapped[str] = mapped_column(String(64), nullable=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    ruleset: Mapped[Ruleset | None] = mapped_column(enum_type(Ruleset, "tag_ruleset", 16))


class BeatmapTagVote(CreatedAtMixin, DbBase):
    """Records user votes assigning tags to beatmaps."""

    __tablename__ = "beatmap_tag_vote"
    __table_args__ = (Index("ix_beatmap_tag_vote_count", "beatmap_id", "tag_id"), {"schema": "content"})

    beatmap_id: Mapped[int] = mapped_column(ForeignKey("content.beatmap.id", ondelete="CASCADE"), primary_key=True)
    tag_id: Mapped[int] = mapped_column(ForeignKey("content.tag_definition.id", ondelete="CASCADE"), primary_key=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("core.account.id", ondelete="RESTRICT"), primary_key=True)


class Comment(BigIntIdentityMixin, CreatedAtMixin, DbBase):
    """Stores timeline comments targeting scores, beatmaps, or beatmapsets."""

    __tablename__ = "comment"
    __table_args__ = (
        CheckConstraint("num_nonnulls(score_id, beatmap_id, beatmapset_id) = 1", name="single_target"),
        CheckConstraint("position_ms IS NULL OR position_ms >= 0", name="nonnegative_position"),
        CheckConstraint("char_length(body) BETWEEN 1 AND 1000", name="body_length"),
        CheckConstraint("color IS NULL OR color ~ '^#[0-9A-Fa-f]{6}$'", name="color_format"),
        Index("ix_comment_score_position", "score_id", "position_ms", "id"),
        Index("ix_comment_beatmap_position", "beatmap_id", "position_ms", "id"),
        Index("ix_comment_set_position", "beatmapset_id", "position_ms", "id"),
        {"schema": "content"},
    )

    author_account_id: Mapped[int] = mapped_column(ForeignKey("core.account.id", ondelete="RESTRICT"), nullable=False)
    score_id: Mapped[int | None] = mapped_column(ForeignKey("scoring.score.id", ondelete="RESTRICT"))
    beatmap_id: Mapped[int | None] = mapped_column(ForeignKey("content.beatmap.id", ondelete="RESTRICT"))
    beatmapset_id: Mapped[int | None] = mapped_column(ForeignKey("content.beatmapset.id", ondelete="RESTRICT"))
    position_ms: Mapped[int | None] = mapped_column(Integer)
    body: Mapped[str] = mapped_column(Text, nullable=False)
    color: Mapped[str | None] = mapped_column(String(7))
    moderation_state: Mapped[str] = mapped_column(
        String(16), nullable=False, default="visible", server_default="visible"
    )
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
