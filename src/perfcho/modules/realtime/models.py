"""Define immutable protocol-neutral realtime state values."""

import uuid
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from math import isfinite

MAX_REVISION = 2**63 - 1
MAX_SEQUENCE = 2**63 - 1


class PresenceSubscription(StrEnum):
    """Select which presence changes an online session receives."""

    NONE = "none"
    ALL = "all"
    FOLLOWED = "followed"


def _require_positive_id(name: str, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{name} must be a positive integer")


def _require_bounded_counter(name: str, value: int, maximum: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= maximum:
        raise ValueError(f"{name} must be between 0 and {maximum}")


def _require_aware(name: str, value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")


def _freeze_bytes(name: str, value: bytes) -> bytes:
    if not isinstance(value, bytes | bytearray | memoryview):
        raise TypeError(f"{name} must be bytes-like")
    return bytes(value)


def _require_text(name: str, value: str, *, maximum: int, required: bool = False) -> None:
    if not isinstance(value, str) or (required and not value) or len(value) > maximum:
        raise ValueError(f"{name} must be a string of at most {maximum} characters")


def _require_number(name: str, value: int | float) -> None:
    if isinstance(value, bool) or not isinstance(value, int | float) or not isfinite(value):
        raise ValueError(f"{name} must be a finite number")


@dataclass(frozen=True, slots=True)
class SessionFence:
    """Identify one exact realtime session epoch."""

    session_id: uuid.UUID
    revision: int

    def __post_init__(self) -> None:
        """Require a UUID and bounded revision."""
        if not isinstance(self.session_id, uuid.UUID):
            raise TypeError("session_id must be a UUID")
        _require_bounded_counter("revision", self.revision, MAX_REVISION)


@dataclass(frozen=True, slots=True)
class RealtimeSession:
    """Identify one fenced online lifecycle for an authenticated session."""

    session_id: uuid.UUID
    account_id: int
    revision: int
    expires_at: datetime

    def __post_init__(self) -> None:
        """Require a valid account, bounded fence revision, and TTL instant."""
        _require_positive_id("account_id", self.account_id)
        _require_bounded_counter("revision", self.revision, MAX_REVISION)
        _require_aware("expires_at", self.expires_at)

    @property
    def fence(self) -> SessionFence:
        """Return the immutable epoch token required by fenced operations."""
        return SessionFence(self.session_id, self.revision)


@dataclass(frozen=True, slots=True)
class PresenceIdentity:
    """Describe protocol-neutral identity fields shown in online presence."""

    display_name: str
    country_code: str | None
    utc_offset: int
    privileges: frozenset[str]
    longitude: float = 0.0
    latitude: float = 0.0

    def __post_init__(self) -> None:
        """Validate bounded display identity and freeze privilege codes."""
        _require_text("display_name", self.display_name, maximum=64, required=True)
        if self.country_code is not None:
            if len(self.country_code) != 2 or not self.country_code.isascii() or not self.country_code.isalpha():
                raise ValueError("country_code must be an ISO alpha-2 code or None")
            object.__setattr__(self, "country_code", self.country_code.upper())
        if (
            isinstance(self.utc_offset, bool)
            or not isinstance(self.utc_offset, int)
            or not -24 <= self.utc_offset <= 24
        ):
            raise ValueError("utc_offset must be between -24 and 24")
        privileges = frozenset(self.privileges)
        if len(privileges) > 64 or any(
            not isinstance(value, str) or not value or len(value) > 128 for value in privileges
        ):
            raise ValueError("privileges must contain at most 64 non-empty bounded strings")
        object.__setattr__(self, "privileges", privileges)
        _require_number("longitude", self.longitude)
        _require_number("latitude", self.latitude)


@dataclass(frozen=True, slots=True)
class PlayerActivity:
    """Describe a player's current protocol-neutral activity."""

    action: str
    info: str = ""
    beatmap_id: int | None = None
    beatmap_checksum: str | None = None
    ruleset: str = "osu"
    mods: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """Validate bounded activity fields and freeze canonical mods."""
        _require_text("action", self.action, maximum=32, required=True)
        _require_text("info", self.info, maximum=512)
        _require_text("ruleset", self.ruleset, maximum=32, required=True)
        if self.beatmap_checksum is not None:
            _require_text("beatmap_checksum", self.beatmap_checksum, maximum=128)
        if self.beatmap_id is not None and (
            isinstance(self.beatmap_id, bool)
            or not isinstance(self.beatmap_id, int)
            or not -(2**31) <= self.beatmap_id < 2**31
        ):
            raise ValueError("beatmap_id must fit a signed 32-bit integer or be None")
        mods = tuple(self.mods)
        if len(mods) > 64 or any(not isinstance(mod, str) or not mod or len(mod) > 16 for mod in mods):
            raise ValueError("mods must contain at most 64 non-empty bounded strings")
        object.__setattr__(self, "mods", mods)


@dataclass(frozen=True, slots=True)
class PlayerStatistics:
    """Describe protocol-neutral statistics shown with online presence."""

    ranked_score: int = 0
    accuracy: float = 0.0
    play_count: int = 0
    total_score: int = 0
    global_rank: int | None = None
    performance: float = 0.0

    def __post_init__(self) -> None:
        """Validate non-negative counters and finite numeric values."""
        for name in ("ranked_score", "play_count", "total_score"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        if self.global_rank is not None:
            _require_positive_id("global_rank", self.global_rank)
        _require_number("accuracy", self.accuracy)
        _require_number("performance", self.performance)


@dataclass(frozen=True, slots=True)
class PresenceSnapshot:
    """Store one complete canonical presence projection for an account epoch."""

    account_id: int
    revision: int
    identity: PresenceIdentity
    activity: PlayerActivity
    statistics: PlayerStatistics
    expires_at: datetime
    session_id: uuid.UUID

    def __post_init__(self) -> None:
        """Validate canonical state, owning fence, and TTL instant."""
        _require_positive_id("account_id", self.account_id)
        _require_bounded_counter("revision", self.revision, MAX_REVISION)
        if not isinstance(self.identity, PresenceIdentity):
            raise TypeError("identity must be a PresenceIdentity")
        if not isinstance(self.activity, PlayerActivity):
            raise TypeError("activity must be a PlayerActivity")
        if not isinstance(self.statistics, PlayerStatistics):
            raise TypeError("statistics must be a PlayerStatistics")
        _require_aware("expires_at", self.expires_at)
        if not isinstance(self.session_id, uuid.UUID):
            raise TypeError("session_id must be a UUID")

    @property
    def fence(self) -> SessionFence:
        """Return the owning epoch."""
        return SessionFence(self.session_id, self.revision)


@dataclass(frozen=True, slots=True)
class SpectatorRelation:
    """Describe one versioned, expiring spectator-to-host attachment."""

    host_account_id: int
    spectator_account_id: int
    relation_id: uuid.UUID
    revision: int
    host_fence: SessionFence
    spectator_fence: SessionFence
    expires_at: datetime

    def __post_init__(self) -> None:
        """Require distinct valid accounts, a bounded revision, and an aware expiry."""
        _require_positive_id("host_account_id", self.host_account_id)
        _require_positive_id("spectator_account_id", self.spectator_account_id)
        if self.host_account_id == self.spectator_account_id:
            raise ValueError("a spectator cannot attach to itself")
        if not isinstance(self.relation_id, uuid.UUID):
            raise TypeError("relation_id must be a UUID")
        _require_bounded_counter("revision", self.revision, MAX_REVISION)
        if not isinstance(self.host_fence, SessionFence):
            raise TypeError("host_fence must be a SessionFence")
        if not isinstance(self.spectator_fence, SessionFence):
            raise TypeError("spectator_fence must be a SessionFence")
        _require_aware("expires_at", self.expires_at)


class SpectatorFrameAction(StrEnum):
    """Describe a protocol-neutral replay stream transition."""

    UPDATE = "update"
    NEW_PLAY = "new_play"
    SKIP = "skip"
    COMPLETE = "complete"
    FAIL = "fail"
    PAUSE = "pause"
    RESUME = "resume"
    SELECT_PLAY = "select_play"
    SWITCH_HOST = "switch_host"


@dataclass(frozen=True, slots=True)
class CanonicalReplayFrame:
    """Represent one replay input sample without adapter wire types."""

    timestamp_ms: int
    position_x: float
    position_y: float
    input_state: int
    auxiliary_state: int

    def __post_init__(self) -> None:
        """Validate replay sample coordinates and input state."""
        if isinstance(self.timestamp_ms, bool) or not isinstance(self.timestamp_ms, int):
            raise ValueError("timestamp_ms must be an integer")
        _require_number("position_x", self.position_x)
        _require_number("position_y", self.position_y)
        _require_bounded_counter("input_state", self.input_state, MAX_SEQUENCE)
        _require_bounded_counter("auxiliary_state", self.auxiliary_state, MAX_SEQUENCE)


@dataclass(frozen=True, slots=True)
class CanonicalScoreFrame:
    """Represent complete score state accompanying replay samples."""

    elapsed_ms: int
    frame_index: int
    count_300: int
    count_100: int
    count_50: int
    count_geki: int
    count_katu: int
    count_miss: int
    total_score: int
    max_combo: int
    current_combo: int
    perfect: bool
    health: int
    tag: int
    score_v2: bool
    combo_portion: float | None = None
    bonus_portion: float | None = None

    def __post_init__(self) -> None:
        """Validate complete score counters and optional score portions."""
        if isinstance(self.elapsed_ms, bool) or not isinstance(self.elapsed_ms, int):
            raise ValueError("elapsed_ms must be an integer")
        for name in (
            "frame_index",
            "count_300",
            "count_100",
            "count_50",
            "count_geki",
            "count_katu",
            "count_miss",
            "total_score",
            "max_combo",
            "current_combo",
            "health",
            "tag",
        ):
            _require_bounded_counter(name, getattr(self, name), MAX_SEQUENCE)
        if not isinstance(self.perfect, bool) or not isinstance(self.score_v2, bool):
            raise TypeError("perfect and score_v2 must be booleans")
        if self.score_v2:
            if self.combo_portion is None or self.bonus_portion is None:
                raise ValueError("score_v2 state requires combo and bonus portions")
            _require_number("combo_portion", self.combo_portion)
            _require_number("bonus_portion", self.bonus_portion)
        elif self.combo_portion is not None or self.bonus_portion is not None:
            raise ValueError("non-score_v2 state cannot contain score portions")


@dataclass(frozen=True, slots=True)
class SpectatorFrame:
    """Carry one canonical host frame behind a monotonic history cursor."""

    cursor: int
    host_account_id: int
    sequence: int
    action: SpectatorFrameAction
    frames: tuple[CanonicalReplayFrame, ...]
    score: CanonicalScoreFrame
    extra: int

    def __post_init__(self) -> None:
        """Validate history metadata and canonical frame state."""
        _require_bounded_counter("cursor", self.cursor, MAX_SEQUENCE)
        _require_positive_id("host_account_id", self.host_account_id)
        _require_bounded_counter("sequence", self.sequence, MAX_SEQUENCE)
        if not isinstance(self.action, SpectatorFrameAction):
            raise TypeError("action must be a SpectatorFrameAction")
        frames = tuple(self.frames)
        if any(not isinstance(frame, CanonicalReplayFrame) for frame in frames):
            raise TypeError("frames must contain only CanonicalReplayFrame values")
        object.__setattr__(self, "frames", frames)
        if not isinstance(self.score, CanonicalScoreFrame):
            raise TypeError("score must be a CanonicalScoreFrame")
        if isinstance(self.extra, bool) or not isinstance(self.extra, int):
            raise ValueError("extra must be an integer")


@dataclass(frozen=True, slots=True)
class SpectatorRecipient:
    """Identify one currently valid spectator session target."""

    account_id: int
    fence: SessionFence
    expires_at: datetime

    def __post_init__(self) -> None:
        """Validate the target account, fence, and relation expiry."""
        _require_positive_id("account_id", self.account_id)
        if not isinstance(self.fence, SessionFence):
            raise TypeError("fence must be a SessionFence")
        _require_aware("expires_at", self.expires_at)


@dataclass(frozen=True, slots=True)
class SpectatorFrameWindow:
    """Describe a bounded frame snapshot and its retained cursor range."""

    frames: tuple[SpectatorFrame, ...]
    oldest_cursor: int | None
    latest_cursor: int | None
    truncated: bool

    def __post_init__(self) -> None:
        """Freeze frames and validate snapshot cursor metadata."""
        frames = tuple(self.frames)
        if any(not isinstance(frame, SpectatorFrame) for frame in frames):
            raise TypeError("frames must contain only SpectatorFrame values")
        object.__setattr__(self, "frames", frames)
        for name, value in (("oldest_cursor", self.oldest_cursor), ("latest_cursor", self.latest_cursor)):
            if value is not None:
                _require_bounded_counter(name, value, MAX_SEQUENCE)
        if (self.oldest_cursor is None) != (self.latest_cursor is None):
            raise ValueError("frame window cursors must both be present or absent")
        if (
            self.oldest_cursor is not None
            and self.latest_cursor is not None
            and self.oldest_cursor > self.latest_cursor
        ):
            raise ValueError("oldest_cursor must not exceed latest_cursor")


@dataclass(frozen=True, slots=True)
class SpectatorAttachment:
    """Return an atomic relation and history-to-live handoff snapshot."""

    relation: SpectatorRelation
    history: SpectatorFrameWindow

    def __post_init__(self) -> None:
        """Require typed immutable attachment components."""
        if not isinstance(self.relation, SpectatorRelation):
            raise TypeError("relation must be a SpectatorRelation")
        if not isinstance(self.history, SpectatorFrameWindow):
            raise TypeError("history must be a SpectatorFrameWindow")


@dataclass(frozen=True, slots=True)
class SpectatorFramePublish:
    """Report one accepted frame and state-validated live targets."""

    frame: SpectatorFrame
    recipients: tuple[SpectatorRecipient, ...]

    def __post_init__(self) -> None:
        """Freeze and validate the delivery result."""
        if not isinstance(self.frame, SpectatorFrame):
            raise TypeError("frame must be a SpectatorFrame")
        recipients = tuple(self.recipients)
        if any(not isinstance(recipient, SpectatorRecipient) for recipient in recipients):
            raise TypeError("recipients must contain only SpectatorRecipient values")
        object.__setattr__(self, "recipients", recipients)
