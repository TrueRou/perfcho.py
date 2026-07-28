import uuid
from datetime import datetime

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Identity,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from perfcho.infra.db.base import DbBase
from perfcho.infra.db.enums import Ruleset, TeamRole, enum_type
from perfcho.infra.db.mixins import BigIntIdentityMixin, CreatedAtMixin, TimestampMixin, Uuid7PrimaryKeyMixin


class Follow(CreatedAtMixin, DbBase):
    """Stores account follow relationships from which mutual friendship is derived."""

    __tablename__ = "follows"
    __table_args__ = (
        CheckConstraint("actor_account_id <> target_account_id", name="not_self"),
        Index("ix_follows_target", "target_account_id", "actor_account_id"),
        {"schema": "social"},
    )

    actor_account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="CASCADE"), primary_key=True)
    target_account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="CASCADE"), primary_key=True)
    remark: Mapped[str | None] = mapped_column(String(64))


class Block(CreatedAtMixin, DbBase):
    """Stores account-to-account block relationships."""

    __tablename__ = "blocks"
    __table_args__ = (
        CheckConstraint("actor_account_id <> target_account_id", name="not_self"),
        Index("ix_blocks_target", "target_account_id", "actor_account_id"),
        {"schema": "social"},
    )

    actor_account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="CASCADE"), primary_key=True)
    target_account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="CASCADE"), primary_key=True)
    reason: Mapped[str | None] = mapped_column(String(255))


class Team(TimestampMixin, DbBase):
    """Defines account teams with names, tags, branding, and a default ruleset."""

    __tablename__ = "teams"
    __table_args__ = (
        UniqueConstraint("name_key"),
        UniqueConstraint("tag_key"),
        Index("ix_teams_ruleset_archived", "ruleset", "archived_at"),
        {"schema": "social"},
    )

    id: Mapped[int] = mapped_column(Integer, Identity(always=False), primary_key=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    name_key: Mapped[str] = mapped_column(String(100), nullable=False)
    tag: Mapped[str] = mapped_column(String(8), nullable=False)
    tag_key: Mapped[str] = mapped_column(String(8), nullable=False)
    ruleset: Mapped[Ruleset] = mapped_column(enum_type(Ruleset, "team_ruleset", 16), nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    website: Mapped[str | None] = mapped_column(String(512))
    flag_asset_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("core.media_assets.id", ondelete="SET NULL"))
    archived_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class TeamMembership(BigIntIdentityMixin, CreatedAtMixin, DbBase):
    """Stores team membership roles and their complete history."""

    __tablename__ = "team_memberships"
    __table_args__ = (
        CheckConstraint("left_at IS NULL OR left_at > created_at", name="valid_period"),
        Index(
            "uq_team_memberships_current_account",
            "account_id",
            unique=True,
            postgresql_where=text("left_at IS NULL"),
        ),
        Index(
            "uq_team_memberships_current_owner",
            "team_id",
            unique=True,
            postgresql_where=text("left_at IS NULL AND role = 'owner'"),
        ),
        Index("ix_team_memberships_team_active", "team_id", "left_at", "account_id"),
        {"schema": "social"},
    )

    team_id: Mapped[int] = mapped_column(ForeignKey("social.teams.id", ondelete="RESTRICT"), nullable=False)
    account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="RESTRICT"), nullable=False)
    role: Mapped[TeamRole] = mapped_column(enum_type(TeamRole, "team_role", 16), nullable=False)
    left_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class TeamJoinRequest(Uuid7PrimaryKeyMixin, CreatedAtMixin, DbBase):
    """Tracks both applications and invitations for team membership."""

    __tablename__ = "team_join_requests"
    __table_args__ = (
        Index(
            "uq_team_join_requests_pending",
            "team_id",
            "candidate_account_id",
            unique=True,
            postgresql_where=text("status = 'pending'"),
        ),
        Index("ix_team_join_requests_candidate", "candidate_account_id", "status"),
        {"schema": "social"},
    )

    team_id: Mapped[int] = mapped_column(ForeignKey("social.teams.id", ondelete="RESTRICT"), nullable=False)
    candidate_account_id: Mapped[int] = mapped_column(
        ForeignKey("core.accounts.id", ondelete="RESTRICT"), nullable=False
    )
    requested_by_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="RESTRICT"), nullable=False)
    kind: Mapped[str] = mapped_column(String(16), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="pending", server_default="pending")
    decided_by_id: Mapped[int | None] = mapped_column(ForeignKey("core.accounts.id", ondelete="SET NULL"))
    decided_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class AchievementDefinition(TimestampMixin, DbBase):
    """Defines versioned achievement evaluators, parameters, and applicable rulesets."""

    __tablename__ = "achievement_definitions"
    __table_args__ = (UniqueConstraint("slug"), {"schema": "social"})

    id: Mapped[int] = mapped_column(Integer, Identity(always=False), primary_key=True)
    slug: Mapped[str] = mapped_column(String(100), nullable=False)
    evaluator_code: Mapped[str] = mapped_column(String(100), nullable=False)
    evaluator_version: Mapped[int] = mapped_column(Integer, nullable=False)
    parameters: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False, default=dict, server_default="{}")
    ruleset: Mapped[Ruleset | None] = mapped_column(enum_type(Ruleset, "achievement_ruleset", 16))
    icon_asset_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("core.media_assets.id", ondelete="SET NULL"))
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="true")


class AchievementTranslation(DbBase):
    """Stores localized names and descriptions for achievements."""

    __tablename__ = "achievement_translations"
    __table_args__ = ({"schema": "social"},)

    achievement_id: Mapped[int] = mapped_column(
        ForeignKey("social.achievement_definitions.id", ondelete="CASCADE"), primary_key=True
    )
    locale: Mapped[str] = mapped_column(String(16), primary_key=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False)


class AchievementUnlock(CreatedAtMixin, DbBase):
    """Records the idempotent first unlock of an achievement by an account."""

    __tablename__ = "achievement_unlocks"
    __table_args__ = (Index("ix_achievement_unlocks_account_created", "account_id", "created_at"), {"schema": "social"})

    account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="RESTRICT"), primary_key=True)
    achievement_id: Mapped[int] = mapped_column(
        ForeignKey("social.achievement_definitions.id", ondelete="RESTRICT"), primary_key=True
    )
    definition_version: Mapped[int] = mapped_column(Integer, nullable=False)
    score_id: Mapped[int | None] = mapped_column(ForeignKey("scoring.scores.id", ondelete="RESTRICT"))
    source_event_id: Mapped[uuid.UUID | None] = mapped_column(nullable=True, unique=True)
    snapshot: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False, default=dict, server_default="{}")
