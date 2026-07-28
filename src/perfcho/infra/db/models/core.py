"""Map canonical account identities, profiles, and media assets."""

import uuid
from datetime import datetime

from sqlalchemy import (
    ARRAY,
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Identity,
    Index,
    Integer,
    LargeBinary,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from perfcho.infra.db.base import DbBase
from perfcho.infra.db.enums import AccountStatus, AccountType, Ruleset, enum_type
from perfcho.infra.db.mixins import BigIntIdentityMixin, CreatedAtMixin, TimestampMixin, Uuid7PrimaryKeyMixin


class Account(DbBase):
    """Stores the shared Stable and Lazer account identity and lifecycle."""

    __tablename__ = "accounts"
    __table_args__ = (
        CheckConstraint("id BETWEEN 1 AND 2147483647", name="stable_id_range"),
        CheckConstraint("country_code IS NULL OR country_code ~ '^[A-Z]{2}$'", name="country_code_format"),
        Index("ix_accounts_status_id", "status", "id"),
        Index("ix_accounts_country_id", "country_code", "id"),
        {"schema": "core"},
    )

    id: Mapped[int] = mapped_column(Integer, Identity(always=False), primary_key=True)
    type: Mapped[AccountType] = mapped_column(enum_type(AccountType, "account_type", 16), nullable=False)
    status: Mapped[AccountStatus] = mapped_column(
        enum_type(AccountStatus, "account_status", 16),
        nullable=False,
        default=AccountStatus.PENDING,
        server_default=AccountStatus.PENDING.value,
    )
    country_code: Mapped[str | None] = mapped_column(String(2))
    registered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    activated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    first_stable_login_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    auth_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default="1")

    names: Mapped[list[AccountName]] = relationship(
        back_populates="account",
        foreign_keys="AccountName.account_id",
        lazy="raise",
    )
    emails: Mapped[list[AccountEmail]] = relationship(
        back_populates="account",
        foreign_keys="AccountEmail.account_id",
        lazy="raise",
    )
    profile: Mapped[UserProfile | None] = relationship(back_populates="account", lazy="raise")
    preference: Mapped[UserPreference | None] = relationship(back_populates="account", lazy="raise")


class AccountName(BigIntIdentityMixin, DbBase):
    """Stores display names, normalized keys, and the complete rename history."""

    __tablename__ = "account_names"
    __table_args__ = (
        CheckConstraint("char_length(display_name) BETWEEN 2 AND 15", name="display_name_length"),
        CheckConstraint("char_length(name_key) BETWEEN 2 AND 32", name="name_key_length"),
        CheckConstraint("ended_at IS NULL OR ended_at > started_at", name="valid_period"),
        Index("uq_account_names_current_key", "name_key", unique=True, postgresql_where=text("ended_at IS NULL")),
        Index(
            "uq_account_names_current_account",
            "account_id",
            unique=True,
            postgresql_where=text("ended_at IS NULL"),
        ),
        {"schema": "core"},
    )

    account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="RESTRICT"), nullable=False)
    display_name: Mapped[str] = mapped_column(String(15), nullable=False)
    name_key: Mapped[str] = mapped_column(String(32), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    changed_by_id: Mapped[int | None] = mapped_column(ForeignKey("core.accounts.id", ondelete="SET NULL"))

    account: Mapped[Account] = relationship(back_populates="names", foreign_keys=[account_id], lazy="raise")


class AccountEmail(Uuid7PrimaryKeyMixin, DbBase):
    """Stores verified email addresses, primary selection, and email change history."""

    __tablename__ = "account_emails"
    __table_args__ = (
        CheckConstraint("position('@' IN email) > 1", name="email_format"),
        CheckConstraint("retired_at IS NULL OR retired_at > added_at", name="valid_period"),
        Index("uq_account_emails_active_key", "email_key", unique=True, postgresql_where=text("retired_at IS NULL")),
        Index(
            "uq_account_emails_primary_account",
            "account_id",
            unique=True,
            postgresql_where=text("is_primary AND retired_at IS NULL"),
        ),
        {"schema": "core"},
    )

    account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="RESTRICT"), nullable=False)
    email: Mapped[str] = mapped_column(String(254), nullable=False)
    email_key: Mapped[str] = mapped_column(String(254), nullable=False)
    is_primary: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    added_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    retired_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    account: Mapped[Account] = relationship(back_populates="emails", foreign_keys=[account_id], lazy="raise")


class MediaAsset(Uuid7PrimaryKeyMixin, CreatedAtMixin, DbBase):
    """Stores object-storage metadata for avatars, covers, flags, and other media."""

    __tablename__ = "media_assets"
    __table_args__ = (
        CheckConstraint("size_bytes >= 0", name="nonnegative_size"),
        CheckConstraint("width IS NULL OR width > 0", name="positive_width"),
        CheckConstraint("height IS NULL OR height > 0", name="positive_height"),
        UniqueConstraint("storage_key"),
        UniqueConstraint("sha256"),
        {"schema": "core"},
    )

    owner_account_id: Mapped[int | None] = mapped_column(
        ForeignKey("core.accounts.id", ondelete="SET NULL"), index=True
    )
    storage_key: Mapped[str] = mapped_column(String(512), nullable=False)
    sha256: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    media_type: Mapped[str] = mapped_column(String(127), nullable=False)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    width: Mapped[int | None] = mapped_column(Integer)
    height: Mapped[int | None] = mapped_column(Integer)
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class UserProfile(DbBase):
    """Stores public profile details, media, and the default ruleset for an account."""

    __tablename__ = "user_profiles"
    __table_args__ = (
        CheckConstraint("accent_color IS NULL OR accent_color ~ '^#[0-9A-Fa-f]{6}$'", name="accent_color_format"),
        {"schema": "core"},
    )

    account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="CASCADE"), primary_key=True)
    avatar_asset_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("core.media_assets.id", ondelete="SET NULL"))
    cover_asset_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("core.media_assets.id", ondelete="SET NULL"))
    bio: Mapped[str | None] = mapped_column(Text)
    location: Mapped[str | None] = mapped_column(String(100))
    occupation: Mapped[str | None] = mapped_column(String(100))
    interests: Mapped[str | None] = mapped_column(String(255))
    website: Mapped[str | None] = mapped_column(String(512))
    social_links: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False, default=dict, server_default="{}")
    default_ruleset: Mapped[Ruleset] = mapped_column(
        enum_type(Ruleset, "ruleset", 16), nullable=False, default=Ruleset.OSU, server_default=Ruleset.OSU.value
    )
    play_style: Mapped[list[str]] = mapped_column(ARRAY(String(32)), nullable=False, default=list, server_default="{}")
    accent_color: Mapped[str | None] = mapped_column(String(7))

    account: Mapped[Account] = relationship(back_populates="profile", lazy="raise")


class UserPreference(TimestampMixin, DbBase):
    """Stores private client, privacy, and presentation preferences for an account."""

    __tablename__ = "user_preferences"
    __table_args__ = (
        CheckConstraint("master_volume BETWEEN 0 AND 1", name="master_volume_range"),
        CheckConstraint("music_volume BETWEEN 0 AND 1", name="music_volume_range"),
        CheckConstraint("effect_volume BETWEEN 0 AND 1", name="effect_volume_range"),
        {"schema": "core"},
    )

    account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="CASCADE"), primary_key=True)
    locale: Mapped[str] = mapped_column(String(16), nullable=False, default="en", server_default="en")
    timezone: Mapped[str] = mapped_column(String(64), nullable=False, default="UTC", server_default="UTC")
    theme: Mapped[str] = mapped_column(String(32), nullable=False, default="system", server_default="system")
    master_volume: Mapped[float] = mapped_column(nullable=False, default=1.0, server_default="1")
    music_volume: Mapped[float] = mapped_column(nullable=False, default=1.0, server_default="1")
    effect_volume: Mapped[float] = mapped_column(nullable=False, default=1.0, server_default="1")
    preferred_ranking_policy: Mapped[str | None] = mapped_column(String(64))
    private_message_policy: Mapped[str] = mapped_column(
        String(16), nullable=False, default="friends", server_default="friends"
    )
    invisible_online: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    profile_section_order: Mapped[list[str]] = mapped_column(
        ARRAY(String(32)), nullable=False, default=list, server_default="{}"
    )
    extra: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False, default=dict, server_default="{}")

    account: Mapped[Account] = relationship(back_populates="preference", lazy="raise")


class Badge(DbBase):
    """Defines honorary badges that can be displayed on account profiles."""

    __tablename__ = "badges"
    __table_args__ = (UniqueConstraint("slug"), {"schema": "core"})

    id: Mapped[int] = mapped_column(SmallInteger, Identity(always=False), primary_key=True)
    slug: Mapped[str] = mapped_column(String(64), nullable=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    image_asset_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("core.media_assets.id", ondelete="SET NULL"))


class AccountBadge(BigIntIdentityMixin, CreatedAtMixin, DbBase):
    """Records profile badges awarded to accounts independently of achievements."""

    __tablename__ = "account_badges"
    __table_args__ = (
        CheckConstraint("expires_at IS NULL OR expires_at > created_at", name="valid_period"),
        Index("ix_account_badges_account_created", "account_id", "created_at"),
        {"schema": "core"},
    )

    account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="RESTRICT"), nullable=False)
    badge_id: Mapped[int] = mapped_column(ForeignKey("core.badges.id", ondelete="RESTRICT"), nullable=False)
    awarded_by_id: Mapped[int | None] = mapped_column(ForeignKey("core.accounts.id", ondelete="SET NULL"))
    description: Mapped[str | None] = mapped_column(String(255))
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    display_order: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=0, server_default="0")
