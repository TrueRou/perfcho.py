"""Map channels, messages, read cursors, and notification facts."""

import uuid
from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Identity,
    Index,
    Integer,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from perfcho.infra.db.base import DbBase
from perfcho.infra.db.enums import ChannelKind, enum_type
from perfcho.infra.db.mixins import BigIntIdentityMixin, CreatedAtMixin, TimestampMixin


class Channel(TimestampMixin, DbBase):
    """Defines persistent public, direct, team, multiplayer, and system chat channels."""

    __tablename__ = "channels"
    __table_args__ = (
        CheckConstraint("message_length_limit BETWEEN 1 AND 10000", name="message_length_range"),
        UniqueConstraint("slug"),
        Index("ix_channels_kind_archived", "kind", "archived_at"),
        {"schema": "community"},
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    kind: Mapped[ChannelKind] = mapped_column(enum_type(ChannelKind, "channel_kind", 16), nullable=False)
    slug: Mapped[str | None] = mapped_column(String(100))
    name: Mapped[str | None] = mapped_column(String(100))
    description: Mapped[str | None] = mapped_column(String(255))
    owner_account_id: Mapped[int | None] = mapped_column(ForeignKey("core.accounts.id", ondelete="SET NULL"))
    team_id: Mapped[int | None] = mapped_column(ForeignKey("social.teams.id", ondelete="SET NULL"))
    read_permission_id: Mapped[int | None] = mapped_column(ForeignKey("authz.permissions.id", ondelete="SET NULL"))
    write_permission_id: Mapped[int | None] = mapped_column(ForeignKey("authz.permissions.id", ondelete="SET NULL"))
    manage_permission_id: Mapped[int | None] = mapped_column(ForeignKey("authz.permissions.id", ondelete="SET NULL"))
    auto_join: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    message_length_limit: Mapped[int] = mapped_column(Integer, nullable=False, default=2000, server_default="2000")
    archived_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    messages: Mapped[list[Message]] = relationship(
        back_populates="channel", foreign_keys="Message.channel_id", lazy="raise"
    )


class DirectConversation(DbBase):
    """Specializes a channel as the unique direct conversation between two accounts."""

    __tablename__ = "direct_conversations"
    __table_args__ = (
        CheckConstraint("low_account_id < high_account_id", name="ordered_accounts"),
        UniqueConstraint("low_account_id", "high_account_id"),
        Index("ix_direct_conversations_low_account", "low_account_id", "channel_id"),
        Index("ix_direct_conversations_high_account", "high_account_id", "channel_id"),
        {"schema": "community"},
    )

    channel_id: Mapped[int] = mapped_column(ForeignKey("community.channels.id", ondelete="CASCADE"), primary_key=True)
    low_account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="RESTRICT"), nullable=False)
    high_account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="RESTRICT"), nullable=False)


class ChannelMembership(BigIntIdentityMixin, CreatedAtMixin, DbBase):
    """Stores membership history for users in persistent group channels."""

    __tablename__ = "channel_memberships"
    __table_args__ = (
        CheckConstraint("left_at IS NULL OR left_at > created_at", name="valid_period"),
        Index(
            "uq_channel_memberships_current",
            "channel_id",
            "account_id",
            unique=True,
            postgresql_where=text("left_at IS NULL"),
        ),
        Index("ix_channel_memberships_account", "account_id", "left_at"),
        {"schema": "community"},
    )

    channel_id: Mapped[int] = mapped_column(ForeignKey("community.channels.id", ondelete="RESTRICT"), nullable=False)
    account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="RESTRICT"), nullable=False)
    role: Mapped[str] = mapped_column(String(16), nullable=False, default="member", server_default="member")
    left_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class Message(BigIntIdentityMixin, CreatedAtMixin, DbBase):
    """Stores durable chat messages sent to channels."""

    __tablename__ = "messages"
    __table_args__ = (
        CheckConstraint("char_length(content) BETWEEN 1 AND 10000", name="content_length"),
        ForeignKeyConstraint(
            ["channel_id", "reply_to_id"],
            ["community.messages.channel_id", "community.messages.id"],
            name="fk_messages_reply_same_channel",
            ondelete="RESTRICT",
        ),
        UniqueConstraint("channel_id", "id"),
        UniqueConstraint("sender_account_id", "client_message_id"),
        Index("ix_messages_channel_id_desc", "channel_id", text("id DESC")),
        Index("ix_messages_sender_created", "sender_account_id", "created_at"),
        {"schema": "community"},
    )

    channel_id: Mapped[int] = mapped_column(ForeignKey("community.channels.id", ondelete="RESTRICT"), nullable=False)
    sender_account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="RESTRICT"), nullable=False)
    client_message_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    reply_to_id: Mapped[int | None] = mapped_column(BigInteger)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    is_action: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    edited_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    channel: Mapped[Channel] = relationship(back_populates="messages", foreign_keys=[channel_id], lazy="raise")


class ChannelUserState(TimestampMixin, DbBase):
    """Stores per-user read cursors, mute settings, and notification state for channels."""

    __tablename__ = "channel_user_states"
    __table_args__ = (
        ForeignKeyConstraint(
            ["channel_id", "last_read_message_id"],
            ["community.messages.channel_id", "community.messages.id"],
            ondelete="RESTRICT",
        ),
        Index("ix_channel_user_states_account", "account_id", "hidden", "channel_id"),
        {"schema": "community"},
    )

    channel_id: Mapped[int] = mapped_column(ForeignKey("community.channels.id", ondelete="CASCADE"), primary_key=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="CASCADE"), primary_key=True)
    last_read_message_id: Mapped[int | None] = mapped_column(BigInteger)
    muted_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    hidden: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    notification_level: Mapped[str] = mapped_column(String(16), nullable=False, default="all", server_default="all")


class ChannelReadProjection(TimestampMixin, DbBase):
    """Projects channel membership size and latest durable message for ordered reads."""

    __tablename__ = "channel_read_projections"
    __table_args__ = (
        CheckConstraint("active_member_count >= 0", name="nonnegative_member_count"),
        CheckConstraint("source_position > 0", name="positive_source_position"),
        UniqueConstraint("source_event_id"),
        Index("ix_channel_read_projections_latest", "latest_message_at", "channel_id"),
        Index("ix_channel_read_projections_position", "source_position"),
        {"schema": "community"},
    )

    channel_id: Mapped[int] = mapped_column(ForeignKey("community.channels.id", ondelete="CASCADE"), primary_key=True)
    kind: Mapped[str] = mapped_column(String(16), nullable=False)
    active_member_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    latest_message_id: Mapped[int | None] = mapped_column(ForeignKey("community.messages.id", ondelete="SET NULL"))
    latest_message_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    source_event_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("events.outbox_events.id", ondelete="RESTRICT"), nullable=False
    )
    source_position: Mapped[int] = mapped_column(BigInteger, nullable=False)


class MessageRevision(BigIntIdentityMixin, CreatedAtMixin, DbBase):
    """Records message edits, retractions, and administrative deletion history."""

    __tablename__ = "message_revisions"
    __table_args__ = (
        Index("ix_message_revisions_message_created", "message_id", "created_at"),
        {"schema": "community"},
    )

    message_id: Mapped[int] = mapped_column(ForeignKey("community.messages.id", ondelete="RESTRICT"), nullable=False)
    actor_account_id: Mapped[int | None] = mapped_column(ForeignKey("core.accounts.id", ondelete="SET NULL"))
    action: Mapped[str] = mapped_column(String(16), nullable=False)
    previous_content: Mapped[str | None] = mapped_column(Text)
    reason: Mapped[str | None] = mapped_column(String(255))


class Notification(BigIntIdentityMixin, CreatedAtMixin, DbBase):
    """Stores a notification produced from a business event and its display snapshot."""

    __tablename__ = "notifications"
    __table_args__ = (
        UniqueConstraint("source_event_id", "kind"),
        Index("ix_notifications_kind_created", "kind", "created_at"),
        {"schema": "community"},
    )

    source_event_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    actor_account_id: Mapped[int | None] = mapped_column(ForeignKey("core.accounts.id", ondelete="SET NULL"))
    kind: Mapped[str] = mapped_column(String(64), nullable=False)
    category: Mapped[str] = mapped_column(String(32), nullable=False)
    resource_type: Mapped[str | None] = mapped_column(String(64))
    resource_id: Mapped[str | None] = mapped_column(String(128))
    payload: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False, default=dict, server_default="{}")


class NotificationRecipient(DbBase):
    """Tracks seen, read, and dismissed state for notification recipients."""

    __tablename__ = "notification_recipients"
    __table_args__ = (
        Index(
            "ix_notification_recipients_unread",
            "account_id",
            "notification_id",
            postgresql_where=text("read_at IS NULL"),
        ),
        {"schema": "community"},
    )

    notification_id: Mapped[int] = mapped_column(
        ForeignKey("community.notifications.id", ondelete="CASCADE"), primary_key=True
    )
    account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="CASCADE"), primary_key=True)
    seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    read_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    dismissed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class NotificationPreference(TimestampMixin, DbBase):
    """Stores delivery preferences for each notification category and account."""

    __tablename__ = "notification_preferences"
    __table_args__ = ({"schema": "community"},)

    account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="CASCADE"), primary_key=True)
    category: Mapped[str] = mapped_column(String(32), primary_key=True)
    realtime_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="true")
    email_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    push_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    digest_frequency: Mapped[str] = mapped_column(String(16), nullable=False, default="none", server_default="none")


class NotificationDispatch(BigIntIdentityMixin, CreatedAtMixin, DbBase):
    """Tracks external notification delivery attempts and retry state."""

    __tablename__ = "notification_dispatches"
    __table_args__ = (
        CheckConstraint("attempt_count >= 0", name="nonnegative_attempt_count"),
        UniqueConstraint("notification_id", "account_id", "channel"),
        Index("ix_notification_dispatches_status", "status", "next_attempt_at"),
        {"schema": "community"},
    )

    notification_id: Mapped[int] = mapped_column(
        ForeignKey("community.notifications.id", ondelete="CASCADE"), nullable=False
    )
    account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="CASCADE"), nullable=False)
    channel: Mapped[str] = mapped_column(String(16), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="pending", server_default="pending")
    attempt_count: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=0, server_default="0")
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    provider_message_id: Mapped[str | None] = mapped_column(String(255))
    last_error: Mapped[str | None] = mapped_column(Text)
