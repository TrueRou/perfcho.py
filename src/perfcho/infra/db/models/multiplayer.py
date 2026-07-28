"""Map authoritative multiplayer, tournament, and matchmaking facts."""

import uuid
from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import (
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
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from perfcho.infra.db.base import DbBase
from perfcho.infra.db.enums import (
    AttemptStatus,
    RoomStatus,
    SessionStatus,
    enum_type,
)
from perfcho.infra.db.mixins import BigIntIdentityMixin, CreatedAtMixin, TimestampMixin, Uuid7PrimaryKeyMixin


class Room(Uuid7PrimaryKeyMixin, TimestampMixin, DbBase):
    """Defines a persistent centrally hosted multiplayer room."""

    __tablename__ = "rooms"
    __table_args__ = (
        CheckConstraint("capacity BETWEEN 1 AND 1024", name="capacity_range"),
        CheckConstraint("ends_at IS NULL OR starts_at IS NULL OR ends_at > starts_at", name="valid_period"),
        UniqueConstraint("public_id"),
        Index("ix_rooms_category_status_start", "category", "status", "starts_at"),
        Index("ix_rooms_creator_created", "creator_account_id", "created_at"),
        {"schema": "multiplayer"},
    )

    public_id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), nullable=False)
    creator_account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="RESTRICT"), nullable=False)
    channel_id: Mapped[int | None] = mapped_column(ForeignKey("community.channels.id", ondelete="SET NULL"))
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    category: Mapped[str] = mapped_column(String(32), nullable=False)
    format: Mapped[str] = mapped_column(String(32), nullable=False)
    visibility: Mapped[str] = mapped_column(String(16), nullable=False)
    status: Mapped[RoomStatus] = mapped_column(
        enum_type(RoomStatus, "room_status", 16),
        nullable=False,
        default=RoomStatus.PENDING,
        server_default=RoomStatus.PENDING.value,
    )
    ranked: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    capacity: Mapped[int] = mapped_column(Integer, nullable=False)
    starts_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ends_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    configuration: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False, default=dict, server_default="{}")

    sessions: Mapped[list[MultiplayerSession]] = relationship(back_populates="room", lazy="raise")
    playlist_items: Mapped[list[PlaylistItem]] = relationship(back_populates="room", lazy="raise")


class MultiplayerSession(Uuid7PrimaryKeyMixin, CreatedAtMixin, DbBase):
    """Represents one real-time hosting lifecycle within a persistent room."""

    __tablename__ = "sessions"
    __table_args__ = (
        CheckConstraint("ordinal > 0 AND version >= 0", name="positive_ordinal_version"),
        CheckConstraint("ended_at IS NULL OR ended_at > created_at", name="valid_period"),
        UniqueConstraint("room_id", "ordinal"),
        UniqueConstraint("id", "room_id"),
        Index("ix_sessions_room_status", "room_id", "status"),
        Index("ix_sessions_host_status", "host_account_id", "status"),
        {"schema": "multiplayer"},
    )

    room_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("multiplayer.rooms.id", ondelete="RESTRICT"), nullable=False)
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    host_account_id: Mapped[int | None] = mapped_column(ForeignKey("core.accounts.id", ondelete="SET NULL"))
    protocol: Mapped[str] = mapped_column(String(16), nullable=False)
    team_mode: Mapped[str] = mapped_column(String(32), nullable=False)
    scoring_mode: Mapped[str] = mapped_column(String(32), nullable=False)
    status: Mapped[SessionStatus] = mapped_column(
        enum_type(SessionStatus, "multiplayer_session_status", 16),
        nullable=False,
        default=SessionStatus.PENDING,
        server_default=SessionStatus.PENDING.value,
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    room: Mapped[Room] = relationship(back_populates="sessions", lazy="raise")


class RoomParticipant(CreatedAtMixin, DbBase):
    """Tracks accounts admitted to or banned from a persistent room."""

    __tablename__ = "room_participants"
    __table_args__ = (
        UniqueConstraint("admission_jti"),
        Index("ix_room_participants_account_activity", "account_id", "last_activity_at"),
        Index("ix_room_participants_room_status", "room_id", "status"),
        {"schema": "multiplayer"},
    )

    room_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("multiplayer.rooms.id", ondelete="RESTRICT"), primary_key=True
    )
    account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="RESTRICT"), primary_key=True)
    admission_jti: Mapped[uuid.UUID] = mapped_column(nullable=False)
    participant_kind: Mapped[str] = mapped_column(String(16), nullable=False, default="player", server_default="player")
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="active", server_default="active")
    last_activity_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    banned_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class SessionPresence(Uuid7PrimaryKeyMixin, CreatedAtMixin, DbBase):
    """Stores account join and leave history for a hosted session."""

    __tablename__ = "session_presences"

    session_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    room_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    account_id: Mapped[int] = mapped_column(nullable=False)
    join_number: Mapped[int] = mapped_column(Integer, nullable=False)
    left_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    leave_reason: Mapped[str | None] = mapped_column(String(32))

    __table_args__ = (
        CheckConstraint("join_number > 0", name="positive_join_number"),
        CheckConstraint("left_at IS NULL OR left_at > created_at", name="valid_period"),
        ForeignKeyConstraint(
            ["room_id", "account_id"],
            ["multiplayer.room_participants.room_id", "multiplayer.room_participants.account_id"],
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["session_id", "room_id"],
            ["multiplayer.sessions.id", "multiplayer.sessions.room_id"],
            name="fk_session_presences_session_room",
            ondelete="RESTRICT",
        ),
        UniqueConstraint("session_id", "account_id", "join_number"),
        Index(
            "uq_session_presences_current",
            "session_id",
            "account_id",
            unique=True,
            postgresql_where=text("left_at IS NULL"),
        ),
        Index("ix_session_presences_account", "account_id", "created_at"),
        {"schema": "multiplayer"},
    )


class PlaylistItem(Uuid7PrimaryKeyMixin, TimestampMixin, DbBase):
    """Represents a versioned item in a multiplayer room playlist."""

    __tablename__ = "playlist_items"
    __table_args__ = (
        CheckConstraint("client_item_id > 0", name="positive_client_item_id"),
        UniqueConstraint("room_id", "client_item_id"),
        Index("ix_playlist_items_room_state", "room_id", "state", "client_item_id"),
        {"schema": "multiplayer"},
    )

    room_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("multiplayer.rooms.id", ondelete="RESTRICT"), nullable=False)
    client_item_id: Mapped[int] = mapped_column(Integer, nullable=False)
    state: Mapped[str] = mapped_column(String(16), nullable=False, default="active", server_default="active")
    position: Mapped[int] = mapped_column(Integer, nullable=False)

    room: Mapped[Room] = relationship(back_populates="playlist_items", lazy="raise")


class PlaylistRevision(Uuid7PrimaryKeyMixin, CreatedAtMixin, DbBase):
    """Stores immutable map, mod, and scoring configuration for a playlist item."""

    __tablename__ = "playlist_revisions"
    __table_args__ = (
        CheckConstraint("revision_number > 0", name="positive_revision"),
        UniqueConstraint("item_id", "revision_number", name="uq_playlist_revisions_item_revision"),
        UniqueConstraint("item_id", "configuration_digest", name="uq_playlist_revisions_item_digest"),
        Index(
            "uq_playlist_revisions_current",
            "item_id",
            unique=True,
            postgresql_where=text("is_current"),
        ),
        Index("ix_playlist_revisions_beatmap", "beatmap_revision_id"),
        {"schema": "multiplayer"},
    )

    item_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("multiplayer.playlist_items.id", ondelete="RESTRICT"), nullable=False
    )
    revision_number: Mapped[int] = mapped_column(Integer, nullable=False)
    owner_account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="RESTRICT"), nullable=False)
    beatmap_revision_id: Mapped[int] = mapped_column(
        ForeignKey("content.beatmap_revisions.id", ondelete="RESTRICT"), nullable=False
    )
    scoreboard_id: Mapped[int] = mapped_column(
        ForeignKey("scoring.scoreboards.id", ondelete="RESTRICT"), nullable=False
    )
    mod_policy_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("scoring.mod_policies.id", ondelete="RESTRICT"), nullable=False
    )
    required_mod_set_id: Mapped[int | None] = mapped_column(ForeignKey("scoring.mod_sets.id", ondelete="RESTRICT"))
    scoring_mode: Mapped[str] = mapped_column(String(32), nullable=False)
    configuration: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False, default=dict, server_default="{}")
    configuration_digest: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    is_current: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="true")


class TournamentPool(Uuid7PrimaryKeyMixin, TimestampMixin, DbBase):
    """Defines a versioned tournament map pool that sessions can load."""

    __tablename__ = "tournament_pools"
    __table_args__ = (
        UniqueConstraint("namespace", "name_key"),
        Index("ix_tournament_pools_creator_status", "creator_account_id", "status"),
        {"schema": "multiplayer"},
    )

    namespace: Mapped[str] = mapped_column(String(64), nullable=False, default="default", server_default="default")
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    name_key: Mapped[str] = mapped_column(String(100), nullable=False)
    creator_account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="RESTRICT"), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="draft", server_default="draft")


class TournamentPoolRevision(Uuid7PrimaryKeyMixin, CreatedAtMixin, DbBase):
    """Stores immutable published revisions of a tournament pool."""

    __tablename__ = "tournament_pool_revisions"
    __table_args__ = (
        CheckConstraint("revision_number > 0", name="positive_revision"),
        UniqueConstraint("pool_id", "revision_number", name="uq_tournament_pool_revisions_pool_revision"),
        UniqueConstraint("pool_id", "configuration_digest", name="uq_tournament_pool_revisions_pool_digest"),
        Index(
            "uq_tournament_pool_revisions_current",
            "pool_id",
            unique=True,
            postgresql_where=text("is_current"),
        ),
        {"schema": "multiplayer"},
    )

    pool_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("multiplayer.tournament_pools.id", ondelete="RESTRICT"), nullable=False
    )
    revision_number: Mapped[int] = mapped_column(Integer, nullable=False)
    created_by_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="RESTRICT"), nullable=False)
    configuration_digest: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    state: Mapped[str] = mapped_column(String(16), nullable=False, default="draft", server_default="draft")
    is_current: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")


class TournamentPoolItem(Uuid7PrimaryKeyMixin, DbBase):
    """Stores map, mod, bucket, and slot assignments in a pool revision."""

    __tablename__ = "tournament_pool_items"
    __table_args__ = (
        CheckConstraint("slot_number > 0", name="positive_slot"),
        UniqueConstraint(
            "revision_id",
            "mod_bucket",
            "slot_number",
            name="uq_tournament_pool_items_revision_bucket_slot",
        ),
        UniqueConstraint(
            "revision_id",
            "beatmap_revision_id",
            "mod_set_id",
            name="uq_tournament_pool_items_revision_map_mods",
        ),
        Index("ix_tournament_pool_items_beatmap", "beatmap_revision_id"),
        {"schema": "multiplayer"},
    )

    revision_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("multiplayer.tournament_pool_revisions.id", ondelete="CASCADE"), nullable=False
    )
    beatmap_revision_id: Mapped[int] = mapped_column(
        ForeignKey("content.beatmap_revisions.id", ondelete="RESTRICT"), nullable=False
    )
    scoreboard_id: Mapped[int] = mapped_column(
        ForeignKey("scoring.scoreboards.id", ondelete="RESTRICT"), nullable=False
    )
    mod_set_id: Mapped[int] = mapped_column(ForeignKey("scoring.mod_sets.id", ondelete="RESTRICT"), nullable=False)
    mod_bucket: Mapped[str] = mapped_column(String(16), nullable=False)
    slot_number: Mapped[int] = mapped_column(SmallInteger, nullable=False)


class Round(Uuid7PrimaryKeyMixin, CreatedAtMixin, DbBase):
    """Stores one synchronized play round with frozen configuration and lifecycle."""

    __tablename__ = "rounds"
    __table_args__ = (
        CheckConstraint("round_number > 0", name="positive_round_number"),
        CheckConstraint(
            "num_nonnulls(playlist_revision_id, tournament_pool_item_id) = 1",
            name="single_source",
        ),
        CheckConstraint("ended_at IS NULL OR started_at IS NULL OR ended_at > started_at", name="valid_period"),
        UniqueConstraint("session_id", "round_number"),
        Index("ix_rounds_session_status", "session_id", "status"),
        Index("ix_rounds_playlist_revision", "playlist_revision_id"),
        {"schema": "multiplayer"},
    )

    session_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("multiplayer.sessions.id", ondelete="RESTRICT"), nullable=False
    )
    round_number: Mapped[int] = mapped_column(Integer, nullable=False)
    playlist_revision_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("multiplayer.playlist_revisions.id", ondelete="RESTRICT")
    )
    tournament_pool_item_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("multiplayer.tournament_pool_items.id", ondelete="RESTRICT")
    )
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="pending", server_default="pending")
    configuration: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    configuration_digest: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class RoundParticipant(DbBase):
    """Freezes slot, team, and mod selections for participants in a round."""

    __tablename__ = "round_participants"
    __table_args__ = (
        CheckConstraint("slot_number IS NULL OR slot_number BETWEEN 0 AND 15", name="slot_range"),
        Index(
            "uq_round_participants_slot",
            "round_id",
            "slot_number",
            unique=True,
            postgresql_where=text("slot_number IS NOT NULL"),
        ),
        Index("ix_round_participants_account", "account_id", "round_id"),
        {"schema": "multiplayer"},
    )

    round_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("multiplayer.rounds.id", ondelete="RESTRICT"), primary_key=True
    )
    account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="RESTRICT"), primary_key=True)
    presence_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("multiplayer.session_presences.id", ondelete="SET NULL")
    )
    slot_number: Mapped[int | None] = mapped_column(SmallInteger)
    team_number: Mapped[int | None] = mapped_column(SmallInteger)
    mod_set_id: Mapped[int] = mapped_column(ForeignKey("scoring.mod_sets.id", ondelete="RESTRICT"), nullable=False)


class MultiplayerAttempt(Uuid7PrimaryKeyMixin, CreatedAtMixin, DbBase):
    """Authorizes one score opportunity for a round or playlist revision."""

    __tablename__ = "attempts"
    __table_args__ = (
        CheckConstraint("num_nonnulls(round_id, playlist_revision_id) = 1", name="single_context"),
        CheckConstraint("attempt_number > 0", name="positive_attempt_number"),
        CheckConstraint("expires_at > created_at", name="valid_period"),
        UniqueConstraint("token_digest"),
        UniqueConstraint("play_attempt_id"),
        UniqueConstraint("score_id"),
        Index(
            "uq_multiplayer_attempts_round_account",
            "round_id",
            "account_id",
            unique=True,
            postgresql_where=text("round_id IS NOT NULL"),
        ),
        Index("ix_multiplayer_attempts_playlist_account", "playlist_revision_id", "account_id", "attempt_number"),
        Index("ix_multiplayer_attempts_status_expiry", "status", "expires_at"),
        {"schema": "multiplayer"},
    )

    account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="RESTRICT"), nullable=False)
    round_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("multiplayer.rounds.id", ondelete="RESTRICT"))
    playlist_revision_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("multiplayer.playlist_revisions.id", ondelete="RESTRICT")
    )
    play_attempt_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("scoring.play_attempts.id", ondelete="RESTRICT")
    )
    score_id: Mapped[int | None] = mapped_column(ForeignKey("scoring.scores.id", ondelete="RESTRICT"))
    token_digest: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[AttemptStatus] = mapped_column(
        enum_type(AttemptStatus, "multiplayer_attempt_status", 16), nullable=False
    )
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    consumed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class MultiplayerEvent(BigIntIdentityMixin, CreatedAtMixin, DbBase):
    """Stores the ordered semantic event stream of a multiplayer room aggregate."""

    __tablename__ = "events"
    __table_args__ = (
        CheckConstraint("room_sequence > 0 AND aggregate_version >= 0", name="positive_versions"),
        UniqueConstraint("room_id", "room_sequence"),
        Index("ix_multiplayer_events_room_id_desc", "room_id", "id"),
        Index("ix_multiplayer_events_session_version", "session_id", "aggregate_version"),
        Index("ix_multiplayer_events_type_created", "event_type", "created_at"),
        {"schema": "multiplayer"},
    )

    room_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("multiplayer.rooms.id", ondelete="RESTRICT"), nullable=False)
    room_sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    session_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("multiplayer.sessions.id", ondelete="RESTRICT"))
    actor_account_id: Mapped[int | None] = mapped_column(ForeignKey("core.accounts.id", ondelete="SET NULL"))
    aggregate_version: Mapped[int] = mapped_column(Integer, nullable=False)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    visibility: Mapped[str] = mapped_column(String(16), nullable=False)
    payload: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False, default=dict, server_default="{}")


class RoundResult(Uuid7PrimaryKeyMixin, CreatedAtMixin, DbBase):
    """Stores trusted per-account or per-team results calculated for a round."""

    __tablename__ = "round_results"
    __table_args__ = (
        CheckConstraint("num_nonnulls(account_id, team_number) = 1", name="single_subject"),
        CheckConstraint("rank > 0 AND metric_value >= 0 AND points >= 0", name="positive_values"),
        Index(
            "uq_round_results_account",
            "round_id",
            "account_id",
            unique=True,
            postgresql_where=text("account_id IS NOT NULL"),
        ),
        Index(
            "uq_round_results_team",
            "round_id",
            "team_number",
            unique=True,
            postgresql_where=text("team_number IS NOT NULL"),
        ),
        Index("ix_round_results_rank", "round_id", "rank"),
        {"schema": "multiplayer"},
    )

    round_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("multiplayer.rounds.id", ondelete="RESTRICT"), nullable=False
    )
    account_id: Mapped[int | None] = mapped_column(ForeignKey("core.accounts.id", ondelete="RESTRICT"))
    team_number: Mapped[int | None] = mapped_column(SmallInteger)
    score_id: Mapped[int | None] = mapped_column(ForeignKey("scoring.scores.id", ondelete="RESTRICT"))
    rank: Mapped[int] = mapped_column(Integer, nullable=False)
    metric_value: Mapped[Decimal] = mapped_column(Numeric(20, 5), nullable=False)
    points: Mapped[Decimal] = mapped_column(Numeric(12, 4), nullable=False)
    result_digest: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)


class SessionStanding(TimestampMixin, DbBase):
    """Projects current account or team points within a multiplayer session."""

    __tablename__ = "session_standings"
    __table_args__ = (
        CheckConstraint("points >= 0 AND version >= 0", name="nonnegative_values"),
        Index("ix_session_standings_rank", "session_id", "points"),
        {"schema": "multiplayer"},
    )

    session_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("multiplayer.sessions.id", ondelete="CASCADE"), primary_key=True
    )
    subject_type: Mapped[str] = mapped_column(String(16), primary_key=True)
    subject_key: Mapped[str] = mapped_column(String(64), primary_key=True)
    points: Mapped[Decimal] = mapped_column(Numeric(14, 4), nullable=False, default=0, server_default="0")
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")


class SessionPoolBinding(Uuid7PrimaryKeyMixin, CreatedAtMixin, DbBase):
    """Tracks tournament pool revisions loaded into a multiplayer session."""

    __tablename__ = "session_pool_bindings"
    __table_args__ = (
        CheckConstraint("unloaded_at IS NULL OR unloaded_at > created_at", name="valid_period"),
        Index(
            "uq_session_pool_bindings_active",
            "session_id",
            unique=True,
            postgresql_where=text("unloaded_at IS NULL"),
        ),
        Index("ix_session_pool_bindings_revision", "pool_revision_id"),
        {"schema": "multiplayer"},
    )

    session_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("multiplayer.sessions.id", ondelete="RESTRICT"), nullable=False
    )
    pool_revision_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("multiplayer.tournament_pool_revisions.id", ondelete="RESTRICT"), nullable=False
    )
    actor_account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="RESTRICT"), nullable=False)
    unloaded_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class MatchmakingQueue(Uuid7PrimaryKeyMixin, TimestampMixin, DbBase):
    """Defines queue sizes, rating algorithms, scoreboards, and tournament pools."""

    __tablename__ = "matchmaking_queues"
    __table_args__ = (
        CheckConstraint("team_size > 0 AND min_group_size > 0 AND max_group_size >= min_group_size", name="size_range"),
        UniqueConstraint("scoreboard_id", "name_key"),
        Index("ix_matchmaking_queues_active_board", "active", "scoreboard_id"),
        {"schema": "multiplayer"},
    )

    name: Mapped[str] = mapped_column(String(100), nullable=False)
    name_key: Mapped[str] = mapped_column(String(100), nullable=False)
    scoreboard_id: Mapped[int] = mapped_column(
        ForeignKey("scoring.scoreboards.id", ondelete="RESTRICT"), nullable=False
    )
    pool_revision_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("multiplayer.tournament_pool_revisions.id", ondelete="SET NULL")
    )
    team_size: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    min_group_size: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    max_group_size: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    rating_algorithm: Mapped[str] = mapped_column(String(64), nullable=False)
    algorithm_version: Mapped[int] = mapped_column(Integer, nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")


class MatchmakingGroup(Uuid7PrimaryKeyMixin, CreatedAtMixin, DbBase):
    """Represents a solo player or party entering matchmaking together."""

    __tablename__ = "matchmaking_groups"
    __table_args__ = (
        Index("ix_matchmaking_groups_queue_status", "queue_id", "status", "created_at"),
        Index("ix_matchmaking_groups_owner", "owner_account_id", "status"),
        {"schema": "multiplayer"},
    )

    queue_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("multiplayer.matchmaking_queues.id", ondelete="RESTRICT"), nullable=False
    )
    owner_account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="RESTRICT"), nullable=False)
    kind: Mapped[str] = mapped_column(String(16), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="forming", server_default="forming")
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class MatchmakingGroupMember(CreatedAtMixin, DbBase):
    """Tracks accounts and seats within a matchmaking group."""

    __tablename__ = "matchmaking_group_members"
    __table_args__ = (
        CheckConstraint("seat_number >= 0", name="nonnegative_seat"),
        UniqueConstraint("group_id", "seat_number"),
        Index("ix_matchmaking_group_members_account", "account_id", "group_id"),
        {"schema": "multiplayer"},
    )

    group_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("multiplayer.matchmaking_groups.id", ondelete="CASCADE"), primary_key=True
    )
    account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="RESTRICT"), primary_key=True)
    seat_number: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    state: Mapped[str] = mapped_column(String(16), nullable=False, default="active", server_default="active")


class MatchmakingTicket(Uuid7PrimaryKeyMixin, CreatedAtMixin, DbBase):
    """Stores ordered queue entries for matchmaking groups."""

    __tablename__ = "matchmaking_tickets"
    __table_args__ = (
        CheckConstraint("expires_at > created_at", name="valid_period"),
        Index(
            "uq_matchmaking_tickets_queued_group",
            "group_id",
            unique=True,
            postgresql_where=text("state = 'queued'"),
        ),
        Index(
            "ix_matchmaking_tickets_queue_order",
            "queue_id",
            "priority",
            "created_at",
            postgresql_where=text("state = 'queued'"),
        ),
        {"schema": "multiplayer"},
    )

    queue_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("multiplayer.matchmaking_queues.id", ondelete="RESTRICT"), nullable=False
    )
    group_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("multiplayer.matchmaking_groups.id", ondelete="RESTRICT"), nullable=False
    )
    state: Mapped[str] = mapped_column(String(16), nullable=False, default="queued", server_default="queued")
    rating: Mapped[Decimal] = mapped_column(Numeric(12, 4), nullable=False)
    rating_deviation: Mapped[Decimal] = mapped_column(Numeric(12, 4), nullable=False)
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class MatchmakingAssignment(Uuid7PrimaryKeyMixin, CreatedAtMixin, DbBase):
    """Stores a matchmaking room assignment and its acceptance deadline."""

    __tablename__ = "matchmaking_assignments"
    __table_args__ = (
        CheckConstraint("accept_deadline > created_at", name="valid_period"),
        UniqueConstraint("session_id"),
        Index("ix_matchmaking_assignments_deadline", "status", "accept_deadline"),
        {"schema": "multiplayer"},
    )

    queue_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("multiplayer.matchmaking_queues.id", ondelete="RESTRICT"), nullable=False
    )
    room_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("multiplayer.rooms.id", ondelete="RESTRICT"), nullable=False)
    session_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("multiplayer.sessions.id", ondelete="RESTRICT"), nullable=False
    )
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="pending", server_default="pending")
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    accept_deadline: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    configuration_digest: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)


class MatchmakingAssignmentMember(DbBase):
    """Tracks team placement and acceptance state for matched accounts."""

    __tablename__ = "matchmaking_assignment_members"
    __table_args__ = (
        Index("ix_matchmaking_assignment_members_account", "account_id", "accept_state"),
        {"schema": "multiplayer"},
    )

    assignment_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("multiplayer.matchmaking_assignments.id", ondelete="CASCADE"), primary_key=True
    )
    account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="RESTRICT"), primary_key=True)
    group_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("multiplayer.matchmaking_groups.id", ondelete="RESTRICT"), nullable=False
    )
    team_number: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    accept_state: Mapped[str] = mapped_column(String(16), nullable=False, default="pending", server_default="pending")
    responded_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class MatchmakingRating(TimestampMixin, DbBase):
    """Projects the current seasonal rating of an account in a queue."""

    __tablename__ = "matchmaking_ratings"
    __table_args__ = (
        CheckConstraint(
            "rating > 0 AND deviation >= 0 AND volatility >= 0 AND placement_count >= 0 AND version >= 0",
            name="value_ranges",
        ),
        Index("ix_matchmaking_ratings_rank", "queue_id", "season_key", "rating", "account_id"),
        {"schema": "multiplayer"},
    )

    account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="CASCADE"), primary_key=True)
    queue_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("multiplayer.matchmaking_queues.id", ondelete="CASCADE"), primary_key=True
    )
    season_key: Mapped[str] = mapped_column(String(32), primary_key=True)
    rating: Mapped[Decimal] = mapped_column(Numeric(12, 4), nullable=False)
    deviation: Mapped[Decimal] = mapped_column(Numeric(12, 4), nullable=False)
    volatility: Mapped[Decimal] = mapped_column(Numeric(12, 6), nullable=False)
    placement_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")


class MatchmakingRatingChange(BigIntIdentityMixin, CreatedAtMixin, DbBase):
    """Stores immutable rating changes produced by completed assignments."""

    __tablename__ = "matchmaking_rating_changes"
    __table_args__ = (
        UniqueConstraint("assignment_id", "account_id", "algorithm_version"),
        Index("ix_matchmaking_rating_changes_account", "account_id", "created_at"),
        {"schema": "multiplayer"},
    )

    assignment_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("multiplayer.matchmaking_assignments.id", ondelete="RESTRICT"), nullable=False
    )
    account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="RESTRICT"), nullable=False)
    queue_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("multiplayer.matchmaking_queues.id", ondelete="RESTRICT"), nullable=False
    )
    season_key: Mapped[str] = mapped_column(String(32), nullable=False)
    algorithm_version: Mapped[int] = mapped_column(Integer, nullable=False)
    rating_before: Mapped[Decimal] = mapped_column(Numeric(12, 4), nullable=False)
    rating_after: Mapped[Decimal] = mapped_column(Numeric(12, 4), nullable=False)
    details: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False, default=dict, server_default="{}")


class DailyChallenge(Uuid7PrimaryKeyMixin, CreatedAtMixin, DbBase):
    """Defines a dated challenge with its map, scoreboard, and ranking policy."""

    __tablename__ = "daily_challenges"
    __table_args__ = (
        CheckConstraint("ends_at > starts_at", name="valid_period"),
        UniqueConstraint("challenge_date", "scoreboard_id"),
        UniqueConstraint("room_id"),
        Index("ix_daily_challenges_status_start", "status", "starts_at"),
        {"schema": "multiplayer"},
    )

    challenge_date: Mapped[date] = mapped_column(Date, nullable=False)
    room_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("multiplayer.rooms.id", ondelete="RESTRICT"), nullable=False)
    playlist_revision_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("multiplayer.playlist_revisions.id", ondelete="RESTRICT"), nullable=False
    )
    scoreboard_id: Mapped[int] = mapped_column(
        ForeignKey("scoring.scoreboards.id", ondelete="RESTRICT"), nullable=False
    )
    ranking_policy_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("scoring.ranking_policies.id", ondelete="RESTRICT"), nullable=False
    )
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="scheduled", server_default="scheduled")
    starts_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    ends_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finalized_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class DailyChallengeCompletion(CreatedAtMixin, DbBase):
    """Stores authoritative daily challenge completions and final ranks."""

    __tablename__ = "daily_challenge_completions"
    __table_args__ = (
        CheckConstraint("rank IS NULL OR rank > 0", name="positive_rank"),
        CheckConstraint("percentile IS NULL OR percentile BETWEEN 0 AND 1", name="percentile_range"),
        UniqueConstraint("attempt_id"),
        UniqueConstraint("score_id"),
        Index("ix_daily_challenge_completions_account", "account_id", "created_at"),
        Index("ix_daily_challenge_completions_rank", "challenge_id", "rank"),
        {"schema": "multiplayer"},
    )

    challenge_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("multiplayer.daily_challenges.id", ondelete="RESTRICT"), primary_key=True
    )
    account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="RESTRICT"), primary_key=True)
    attempt_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("multiplayer.attempts.id", ondelete="RESTRICT"), nullable=False
    )
    score_id: Mapped[int] = mapped_column(ForeignKey("scoring.scores.id", ondelete="RESTRICT"), nullable=False)
    rank: Mapped[int | None] = mapped_column(Integer)
    percentile: Mapped[Decimal | None] = mapped_column(Numeric(8, 7))


class PlaylistItemUserSummary(TimestampMixin, DbBase):
    """Projects attempts and best results per account and playlist item."""

    __tablename__ = "playlist_item_user_summaries"
    __table_args__ = (
        CheckConstraint("attempt_count >= 0 AND completion_count >= 0", name="nonnegative_counts"),
        Index("ix_playlist_item_summaries_rank", "playlist_item_id", "best_metric_value"),
        {"schema": "multiplayer"},
    )

    playlist_item_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("multiplayer.playlist_items.id", ondelete="CASCADE"), primary_key=True
    )
    account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="CASCADE"), primary_key=True)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    completion_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    best_score_id: Mapped[int | None] = mapped_column(ForeignKey("scoring.scores.id", ondelete="SET NULL"))
    best_metric_value: Mapped[Decimal | None] = mapped_column(Numeric(20, 5))


class RoomUserSummary(TimestampMixin, DbBase):
    """Projects cumulative room results and completion statistics per account."""

    __tablename__ = "room_user_summaries"
    __table_args__ = (
        CheckConstraint("attempt_count >= 0 AND completion_count >= 0 AND total_score >= 0", name="nonnegative_values"),
        Index("ix_room_user_summaries_rank", "room_id", "total_score", "account_id"),
        {"schema": "multiplayer"},
    )

    room_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("multiplayer.rooms.id", ondelete="CASCADE"), primary_key=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="CASCADE"), primary_key=True)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    completion_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    total_score: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default="0")
    total_performance: Mapped[Decimal] = mapped_column(Numeric(14, 5), nullable=False, default=0, server_default="0")
    average_accuracy: Mapped[Decimal] = mapped_column(Numeric(10, 9), nullable=False, default=0, server_default="0")


class DailyChallengeUserSummary(TimestampMixin, DbBase):
    """Projects completion counts, streaks, and high-rank totals for daily challenges."""

    __tablename__ = "daily_challenge_user_summaries"
    __table_args__ = (
        CheckConstraint(
            "completed_count >= 0 AND current_daily_streak >= 0 AND best_daily_streak >= 0 "
            "AND current_weekly_streak >= 0 AND best_weekly_streak >= 0 AND top_10_count >= 0 AND top_50_count >= 0",
            name="nonnegative_values",
        ),
        {"schema": "multiplayer"},
    )

    account_id: Mapped[int] = mapped_column(ForeignKey("core.accounts.id", ondelete="CASCADE"), primary_key=True)
    completed_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    current_daily_streak: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    best_daily_streak: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    current_weekly_streak: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    best_weekly_streak: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    top_10_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    top_50_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
