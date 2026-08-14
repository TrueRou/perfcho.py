"""Protocol-neutral, best-effort realtime events."""

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum

from perfcho.modules.multiplayer.models import RoomState, SlotStatus, TeamMode, WinCondition
from perfcho.modules.realtime.models import (
    CanonicalReplayFrame,
    CanonicalScoreFrame,
    PlayerActivity,
    PlayerStatistics,
    PresenceSnapshot,
    SpectatorFrameAction,
)
from perfcho.modules.scoring.models import CanonicalMod, Ruleset, ScoreboardVariant


def _positive(name: str, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{name} must be a positive integer")


def _non_negative(name: str, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")


def _integer(name: str, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")


def _text(name: str, value: str, *, required: bool = False) -> None:
    if not isinstance(value, str) or (required and not value):
        raise ValueError(f"{name} must be a string")


def _number(name: str, value: int | float) -> None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"{name} must be numeric")


@dataclass(frozen=True, slots=True)
class PresenceUpdatedBubble:
    """Announce a complete canonical presence projection."""

    account_id: int
    display_name: str
    country_code: str | None
    utc_offset: int
    privileges: frozenset[str]
    activity: PlayerActivity
    statistics: PlayerStatistics
    longitude: float = 0.0
    latitude: float = 0.0

    def __post_init__(self) -> None:
        """Validate the account and freeze privileges."""
        _positive("account_id", self.account_id)
        object.__setattr__(self, "privileges", frozenset(self.privileges))
        _text("display_name", self.display_name, required=True)
        if self.country_code is not None:
            _text("country_code", self.country_code, required=True)
        if isinstance(self.utc_offset, bool) or not isinstance(self.utc_offset, int):
            raise ValueError("utc_offset must be an integer")
        if any(not isinstance(privilege, str) or not privilege for privilege in self.privileges):
            raise ValueError("privileges must contain non-empty strings")
        if not isinstance(self.activity, PlayerActivity) or not isinstance(self.statistics, PlayerStatistics):
            raise TypeError("presence activity and statistics must be canonical values")
        _number("longitude", self.longitude)
        _number("latitude", self.latitude)


def presence_updated_bubble(snapshot: PresenceSnapshot) -> PresenceUpdatedBubble:
    """Create a transport event from one complete canonical presence snapshot."""
    if not isinstance(snapshot, PresenceSnapshot):
        raise TypeError("snapshot must be a PresenceSnapshot")
    identity = snapshot.identity
    return PresenceUpdatedBubble(
        account_id=snapshot.account_id,
        display_name=identity.display_name,
        country_code=identity.country_code,
        utc_offset=identity.utc_offset,
        privileges=identity.privileges,
        activity=snapshot.activity,
        statistics=snapshot.statistics,
        longitude=identity.longitude,
        latitude=identity.latitude,
    )


@dataclass(frozen=True, slots=True)
class UserLogoutBubble:
    """Announce that an account left its online session."""

    account_id: int

    def __post_init__(self) -> None:
        """Require a valid account."""
        _positive("account_id", self.account_id)


@dataclass(frozen=True, slots=True)
class ChatMessageBubble:
    """Carry a persisted or transient chat message."""

    message_id: int | None
    channel_id: int | None
    channel_name: str
    sender_account_id: int
    sender_name: str
    content: str
    is_action: bool
    created_at: datetime
    direct: bool

    def __post_init__(self) -> None:
        """Require a valid sender and aware creation instant."""
        _positive("sender_account_id", self.sender_account_id)
        if self.created_at.tzinfo is None or self.created_at.utcoffset() is None:
            raise ValueError("created_at must be timezone-aware")
        if self.message_id is not None:
            _positive("message_id", self.message_id)
        if self.channel_id is not None:
            _positive("channel_id", self.channel_id)
        for name in ("channel_name", "sender_name", "content"):
            _text(name, getattr(self, name), required=name != "content")
        if not isinstance(self.is_action, bool) or not isinstance(self.direct, bool):
            raise ValueError("chat flags must be booleans")


class ChannelMembershipAction(StrEnum):
    """Describe an optional recipient membership transition."""

    JOINED = "joined"
    LEFT = "left"


@dataclass(frozen=True, slots=True)
class ChannelUpdatedBubble:
    """Announce channel metadata and membership count changes."""

    channel_id: int
    name: str
    topic: str
    member_count: int
    membership_action: ChannelMembershipAction | None = None

    def __post_init__(self) -> None:
        """Require valid channel identity and member count."""
        _positive("channel_id", self.channel_id)
        _non_negative("member_count", self.member_count)
        _text("name", self.name, required=True)
        _text("topic", self.topic)
        if self.membership_action is not None and not isinstance(self.membership_action, ChannelMembershipAction):
            raise TypeError("membership_action must be a ChannelMembershipAction")


class MultiplayerRoomAction(StrEnum):
    """Describe a multiplayer room lifecycle transition."""

    CREATED = "created"
    UPDATED = "updated"
    DISPOSED = "disposed"
    JOINED = "joined"
    ROUND_STARTED = "round_started"
    ROUND_COMPLETED = "round_completed"
    ROUND_ABORTED = "round_aborted"
    LEFT = "left"
    KICKED = "kicked"


@dataclass(frozen=True, slots=True)
class MultiplayerSlotSnapshot:
    """Carry one canonical multiplayer slot without adapter bit fields."""

    position: int
    status: SlotStatus
    account_id: int | None = None
    team: int = 0
    mods: tuple[CanonicalMod, ...] = ()
    loaded: bool = False
    skipped: bool = False
    failed: bool = False

    def __post_init__(self) -> None:
        """Validate canonical slot values and freeze mods."""
        _non_negative("position", self.position)
        if not isinstance(self.status, SlotStatus):
            raise TypeError("status must be a SlotStatus")
        if self.account_id is not None:
            _positive("account_id", self.account_id)
        if self.team not in {0, 1, 2}:
            raise ValueError("team must be neutral, red, or blue")
        object.__setattr__(self, "mods", tuple(self.mods))
        if any(not isinstance(mod, CanonicalMod) for mod in self.mods):
            raise TypeError("mods must contain CanonicalMod values")
        if not all(isinstance(flag, bool) for flag in (self.loaded, self.skipped, self.failed)):
            raise TypeError("slot lifecycle flags must be booleans")


@dataclass(frozen=True, slots=True)
class MultiplayerRoomSnapshot:
    """Carry a transport-safe canonical room projection."""

    room_public_id: int
    state_revision: int
    capacity: int
    host_account_id: int
    in_progress: bool
    name: str
    beatmap_name: str
    external_beatmap_id: int
    beatmap_md5: bytes | None
    ruleset: Ruleset
    variant: ScoreboardVariant
    team_mode: TeamMode
    win_condition: WinCondition
    mods: tuple[CanonicalMod, ...] = ()
    free_mods: bool = False
    seed: int = 0
    slots: tuple[MultiplayerSlotSnapshot, ...] = ()
    password_protected: bool = False
    round_id: uuid.UUID | None = None
    round_participant_account_ids: tuple[int, ...] = ()

    def __post_init__(self) -> None:
        """Validate the room and freeze nested collections."""
        _positive("room_public_id", self.room_public_id)
        _non_negative("state_revision", self.state_revision)
        _positive("capacity", self.capacity)
        _positive("host_account_id", self.host_account_id)
        object.__setattr__(self, "mods", tuple(self.mods))
        object.__setattr__(self, "slots", tuple(self.slots))
        object.__setattr__(self, "round_participant_account_ids", tuple(self.round_participant_account_ids))
        for name in ("name", "beatmap_name"):
            _text(name, getattr(self, name), required=name == "name")
        if self.external_beatmap_id < -1:
            raise ValueError("external_beatmap_id must be -1 or greater")
        if self.beatmap_md5 is not None and len(self.beatmap_md5) != 16:
            raise ValueError("beatmap_md5 must contain 16 bytes")
        if not isinstance(self.ruleset, Ruleset) or not isinstance(self.variant, ScoreboardVariant):
            raise TypeError("ruleset and variant must be canonical scoring values")
        if not isinstance(self.team_mode, TeamMode) or not isinstance(self.win_condition, WinCondition):
            raise TypeError("team mode and win condition must be canonical multiplayer values")
        if any(not isinstance(mod, CanonicalMod) for mod in self.mods):
            raise TypeError("mods must contain CanonicalMod values")
        if len(self.slots) != self.capacity or tuple(slot.position for slot in self.slots) != tuple(
            range(self.capacity)
        ):
            raise ValueError("slots must be complete, ordered, and match capacity")
        if any(not isinstance(slot, MultiplayerSlotSnapshot) for slot in self.slots):
            raise TypeError("slots must contain MultiplayerSlotSnapshot values")
        if not all(isinstance(flag, bool) for flag in (self.in_progress, self.free_mods, self.password_protected)):
            raise TypeError("room flags must be booleans")
        if self.in_progress != (self.round_id is not None):
            raise ValueError("in-progress snapshot requires exactly one round ID")
        if any(account_id < 1 for account_id in self.round_participant_account_ids):
            raise ValueError("round participant account IDs must be positive")


def multiplayer_room_snapshot(state: RoomState) -> MultiplayerRoomSnapshot:
    """Copy one domain room state into a transport-safe canonical snapshot."""
    settings = state.room.settings
    return MultiplayerRoomSnapshot(
        room_public_id=state.room.public_id,
        state_revision=state.state_revision,
        capacity=state.room.capacity,
        host_account_id=state.room.host_account_id,
        in_progress=state.in_progress,
        name=settings.name,
        beatmap_name=settings.beatmap_name,
        external_beatmap_id=settings.external_beatmap_id,
        beatmap_md5=settings.beatmap_md5,
        ruleset=settings.ruleset,
        variant=settings.variant,
        team_mode=settings.team_mode,
        win_condition=settings.win_condition,
        mods=settings.mods,
        free_mods=settings.free_mods,
        seed=settings.seed,
        slots=tuple(
            MultiplayerSlotSnapshot(
                slot.position,
                slot.status,
                slot.account_id,
                slot.team,
                slot.mods,
                slot.loaded,
                slot.skipped,
                slot.failed,
            )
            for slot in state.slots
        ),
        password_protected=state.room.password_protected,
        round_id=state.round_id,
        round_participant_account_ids=state.round_participant_account_ids,
    )


@dataclass(frozen=True, slots=True)
class MultiplayerRoomBubble:
    """Announce a complete multiplayer room state change."""

    action: MultiplayerRoomAction
    room: MultiplayerRoomSnapshot
    local_admission_credential: str | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        """Require canonical room action and state values."""
        if not isinstance(self.action, MultiplayerRoomAction) or not isinstance(self.room, MultiplayerRoomSnapshot):
            raise TypeError("multiplayer room bubble requires canonical action and room values")
        if self.local_admission_credential is not None:
            _text("local_admission_credential", self.local_admission_credential)
            if self.action is not MultiplayerRoomAction.JOINED:
                raise ValueError("admission credentials are only valid for local join responses")


class MultiplayerSignalKind(StrEnum):
    """Identify a lightweight multiplayer event."""

    PARTICIPANT_LOADING_COMPLETED = "participant_loading_completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    ALL_PLAYERS_SKIPPED = "all_players_skipped"
    HOST_TRANSFERRED = "host_transferred"
    SCORE_UPDATED = "score_updated"
    INVITED = "invited"
    JOIN_FAILED = "join_failed"


@dataclass(frozen=True, slots=True)
class MultiplayerScoreState:
    """Describe one lossless canonical multiplayer gameplay score frame."""

    account_id: int
    elapsed_milliseconds: int
    slot_position: int
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
    current_health: int
    tag: int
    score_v2: bool
    combo_portion: float | None = None
    bonus_portion: float | None = None

    def __post_init__(self) -> None:
        """Validate canonical score counters."""
        _positive("account_id", self.account_id)
        for name in (
            "elapsed_milliseconds",
            "slot_position",
            "count_300",
            "count_100",
            "count_50",
            "count_geki",
            "count_katu",
            "count_miss",
            "total_score",
            "max_combo",
            "current_combo",
            "current_health",
            "tag",
        ):
            _non_negative(name, getattr(self, name))
        if not isinstance(self.perfect, bool) or not isinstance(self.score_v2, bool):
            raise TypeError("score flags must be booleans")
        for name in ("combo_portion", "bonus_portion"):
            value = getattr(self, name)
            if value is not None:
                _number(name, value)


@dataclass(frozen=True, slots=True)
class MultiplayerInvitationState:
    """Carry a protocol-neutral room invitation for one recipient."""

    sender_account_id: int
    sender_name: str
    recipient_name: str
    room_name: str
    admission_token: str = field(repr=False)

    def __post_init__(self) -> None:
        """Validate invitation identities and bounded text."""
        _positive("sender_account_id", self.sender_account_id)
        for name in ("sender_name", "recipient_name", "room_name", "admission_token"):
            _text(name, getattr(self, name), required=True)


@dataclass(frozen=True, slots=True)
class MultiplayerSignalBubble:
    """Carry a lightweight multiplayer event."""

    kind: MultiplayerSignalKind
    room_public_id: int | None
    actor_account_id: int | None = None
    slot_position: int | None = None
    score: MultiplayerScoreState | None = None
    invitation: MultiplayerInvitationState | None = None

    def __post_init__(self) -> None:
        """Require valid room and optional actor identities."""
        if self.room_public_id is not None:
            _positive("room_public_id", self.room_public_id)
        if self.actor_account_id is not None:
            _positive("actor_account_id", self.actor_account_id)
        if self.slot_position is not None:
            _non_negative("slot_position", self.slot_position)
        if not isinstance(self.kind, MultiplayerSignalKind):
            raise TypeError("kind must be a MultiplayerSignalKind")
        if self.score is not None and not isinstance(self.score, MultiplayerScoreState):
            raise TypeError("score must be a MultiplayerScoreState")
        if self.invitation is not None and not isinstance(self.invitation, MultiplayerInvitationState):
            raise TypeError("invitation must be a MultiplayerInvitationState")
        if self.kind is MultiplayerSignalKind.SCORE_UPDATED and self.score is None:
            raise ValueError("score-updated signal requires score state")
        if self.kind is MultiplayerSignalKind.INVITED and self.invitation is None:
            raise ValueError("invited signal requires invitation state")
        if self.kind is not MultiplayerSignalKind.JOIN_FAILED and self.room_public_id is None:
            raise ValueError("multiplayer signal requires a room public ID")


class SpectatorAction(StrEnum):
    """Describe a spectator event from one recipient's perspective."""

    ATTACHED_TO_HOST = "attached_to_host"
    DETACHED_FROM_HOST = "detached_from_host"
    FELLOW_ATTACHED = "fellow_attached"
    FELLOW_DETACHED = "fellow_detached"
    PLAYBACK_UNAVAILABLE = "playback_unavailable"


@dataclass(frozen=True, slots=True)
class SpectatorLifecycleBubble:
    """Announce a spectator relation transition."""

    action: SpectatorAction
    host_account_id: int
    spectator_account_id: int

    def __post_init__(self) -> None:
        """Require distinct valid relation accounts."""
        _positive("host_account_id", self.host_account_id)
        _positive("spectator_account_id", self.spectator_account_id)
        if self.host_account_id == self.spectator_account_id:
            raise ValueError("a spectator cannot attach to itself")
        if not isinstance(self.action, SpectatorAction):
            raise TypeError("action must be a SpectatorAction")


@dataclass(frozen=True, slots=True)
class SpectatorFrameBubble:
    """Carry canonical replay samples for live spectators."""

    host_account_id: int
    sequence: int
    action: SpectatorFrameAction
    frames: tuple[CanonicalReplayFrame, ...]
    score: CanonicalScoreFrame
    extra: int

    def __post_init__(self) -> None:
        """Validate the host and freeze replay samples."""
        _positive("host_account_id", self.host_account_id)
        object.__setattr__(self, "frames", tuple(self.frames))
        _non_negative("sequence", self.sequence)
        if not isinstance(self.action, SpectatorFrameAction):
            raise TypeError("action must be a SpectatorFrameAction")
        if any(not isinstance(frame, CanonicalReplayFrame) for frame in self.frames):
            raise TypeError("frames must contain CanonicalReplayFrame values")
        if not isinstance(self.score, CanonicalScoreFrame):
            raise TypeError("score must be a CanonicalScoreFrame")
        if isinstance(self.extra, bool) or not isinstance(self.extra, int):
            raise ValueError("extra must be an integer")


@dataclass(frozen=True, slots=True)
class ToastBubble:
    """Carry a transient user-facing toast."""

    message: str

    def __post_init__(self) -> None:
        """Require toast text."""
        _text("message", self.message)


class SessionControlAction(StrEnum):
    """Identify a transient session instruction."""

    RECONNECT = "reconnect"
    CLOSE = "close"


@dataclass(frozen=True, slots=True)
class SessionControlBubble:
    """Carry a transient session instruction."""

    action: SessionControlAction
    retry_after_ms: int = 0

    def __post_init__(self) -> None:
        """Reject negative retry delays."""
        _non_negative("retry_after_ms", self.retry_after_ms)
        if not isinstance(self.action, SessionControlAction):
            raise TypeError("action must be a SessionControlAction")


type RealtimeBubble = (
    PresenceUpdatedBubble
    | UserLogoutBubble
    | ChatMessageBubble
    | ChannelUpdatedBubble
    | MultiplayerRoomBubble
    | MultiplayerSignalBubble
    | SpectatorLifecycleBubble
    | SpectatorFrameBubble
    | ToastBubble
    | SessionControlBubble
)
