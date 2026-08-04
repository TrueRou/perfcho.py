"""Define immutable protocol-neutral multiplayer commands and snapshots."""

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum

from perfcho.modules.common.models import CommandMeta
from perfcho.modules.scoring.models import CanonicalMod, Ruleset, ScoreboardVariant

MAX_ROOM_CAPACITY = 1024
MAX_STABLE_PUBLIC_ID = 32767


class TeamMode(StrEnum):
    """Describe how participants cooperate or compete in one room."""

    HEAD_TO_HEAD = "head_to_head"
    TAG_COOP = "tag_coop"
    TEAM_VS = "team_vs"
    TAG_TEAM_VS = "tag_team_vs"


class WinCondition(StrEnum):
    """Describe the metric used to order multiplayer results."""

    SCORE = "score"
    ACCURACY = "accuracy"
    COMBO = "combo"
    SCORE_V2 = "score_v2"


class SlotStatus(StrEnum):
    """Describe protocol-neutral ephemeral participant readiness."""

    OPEN = "open"
    LOCKED = "locked"
    NOT_READY = "not_ready"
    READY = "ready"
    NO_BEATMAP = "no_beatmap"
    PLAYING = "playing"
    COMPLETE = "complete"


class ProjectionStatus(StrEnum):
    """Describe whether a room snapshot is backed by the live Redis projection."""

    LIVE = "live"
    DURABLE_RECOVERY = "durable_recovery"


class MultiplayerMutationKind(StrEnum):
    """Describe a committed multiplayer change for protocol adapters."""

    SETTINGS_UPDATED = "settings_updated"
    HOST_CHANGED = "host_changed"
    PASSWORD_CHANGED = "password_changed"
    PARTICIPANT_KICKED = "participant_kicked"
    ROUND_STARTED = "round_started"
    ROUND_COMPLETED = "round_completed"
    ROUND_ABORTED = "round_aborted"


def _positive(name: str, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{name} must be a positive integer")


def _aware(name: str, value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")


@dataclass(frozen=True, slots=True)
class RoomSettings:
    """Carry shared room configuration independently of a client wire format."""

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

    def __post_init__(self) -> None:
        """Validate bounded text, map identity, checksum, and immutable mods."""
        name = self.name.strip()
        if not name or len(name) > 255:
            raise ValueError("room name must contain at most 255 characters")
        if len(self.beatmap_name) > 255:
            raise ValueError("beatmap_name must contain at most 255 characters")
        if self.external_beatmap_id < -1:
            raise ValueError("external_beatmap_id must be -1 or greater")
        if self.beatmap_md5 is not None and len(self.beatmap_md5) != 16:
            raise ValueError("beatmap_md5 must contain 16 bytes")
        mods = tuple(self.mods)
        if any(not isinstance(mod, CanonicalMod) for mod in mods):
            raise TypeError("mods must contain only CanonicalMod values")
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "mods", mods)


@dataclass(frozen=True, slots=True)
class RoomSlot:
    """Describe one bounded realtime slot projection."""

    position: int
    status: SlotStatus
    account_id: int | None = None
    team: int = 0
    mods: tuple[CanonicalMod, ...] = ()
    loaded: bool = False
    skipped: bool = False
    failed: bool = False

    def __post_init__(self) -> None:
        """Require a valid position and coherent occupied state."""
        if isinstance(self.position, bool) or not 0 <= self.position < MAX_ROOM_CAPACITY:
            raise ValueError("slot position is outside the room capacity bound")
        if self.account_id is not None:
            _positive("account_id", self.account_id)
        occupied = self.status not in {SlotStatus.OPEN, SlotStatus.LOCKED}
        if occupied != (self.account_id is not None):
            raise ValueError("occupied slot status and account_id are inconsistent")
        if self.team not in {0, 1, 2}:
            raise ValueError("slot team must be neutral, red, or blue")
        mods = tuple(self.mods)
        if any(not isinstance(mod, CanonicalMod) for mod in mods):
            raise TypeError("slot mods must contain only CanonicalMod values")
        object.__setattr__(self, "mods", mods)


@dataclass(frozen=True, slots=True)
class RoomRecord:
    """Carry durable room and current hosting-session facts."""

    room_id: uuid.UUID
    public_id: int
    session_id: uuid.UUID
    version: int
    creator_account_id: int
    host_account_id: int
    capacity: int
    settings: RoomSettings
    requires_password: bool = False
    password_salt: str | None = field(default=None, repr=False)
    password_verifier: str | None = field(default=None, repr=False)
    public_id_epoch: int = 1

    def __post_init__(self) -> None:
        """Validate identifiers, version, capacity, and password fields."""
        _positive("public_id", self.public_id)
        _positive("creator_account_id", self.creator_account_id)
        _positive("host_account_id", self.host_account_id)
        _positive("public_id_epoch", self.public_id_epoch)
        if self.version < 0:
            raise ValueError("version must be non-negative")
        if not 1 <= self.capacity <= MAX_ROOM_CAPACITY:
            raise ValueError("capacity is outside the supported range")
        if (self.password_salt is None) != (self.password_verifier is None):
            raise ValueError("room password fields must both be present or absent")
        if self.password_verifier is not None and not self.requires_password:
            raise ValueError("stored room password requires its public protection marker")

    @property
    def password_protected(self) -> bool:
        """Return whether joining requires a password proof."""
        return self.requires_password


@dataclass(frozen=True, slots=True)
class RoomState:
    """Combine durable room identity with an expiring realtime projection."""

    room: RoomRecord
    state_revision: int
    slots: tuple[RoomSlot, ...]
    in_progress: bool
    expires_at: datetime
    round_id: uuid.UUID | None = None
    round_participant_account_ids: tuple[int, ...] = ()
    projection_status: ProjectionStatus = ProjectionStatus.LIVE

    def __post_init__(self) -> None:
        """Require complete ordered slots and a timezone-aware expiry."""
        if self.state_revision < 0:
            raise ValueError("state_revision must be non-negative")
        slots = tuple(self.slots)
        if len(slots) != self.room.capacity:
            raise ValueError("slots must match room capacity")
        if tuple(slot.position for slot in slots) != tuple(range(self.room.capacity)):
            raise ValueError("slots must be ordered and contiguous")
        account_ids = [slot.account_id for slot in slots if slot.account_id is not None]
        if len(account_ids) != len(set(account_ids)):
            raise ValueError("an account cannot occupy multiple slots")
        if self.room.host_account_id not in account_ids:
            raise ValueError("the room host must occupy a slot")
        round_accounts = tuple(self.round_participant_account_ids)
        if len(round_accounts) != len(set(round_accounts)) or any(account_id < 1 for account_id in round_accounts):
            raise ValueError("round participant account IDs must be unique and positive")
        if self.in_progress != (self.round_id is not None):
            raise ValueError("in-progress room state requires exactly one active round")
        if not set(round_accounts) <= set(account_ids):
            raise ValueError("round participants must occupy the room projection")
        _aware("expires_at", self.expires_at)
        object.__setattr__(self, "slots", slots)
        object.__setattr__(self, "round_participant_account_ids", round_accounts)

    def slot_for(self, account_id: int) -> RoomSlot | None:
        """Return the slot occupied by an account."""
        return next((slot for slot in self.slots if slot.account_id == account_id), None)


@dataclass(frozen=True, slots=True)
class MultiplayerMutationResult:
    """Return a committed room change without choosing a protocol response."""

    kind: MultiplayerMutationKind
    state: RoomState
    target_account_id: int | None = None
    round_participant_account_ids: tuple[int, ...] = ()
    replayed: bool = False

    def __post_init__(self) -> None:
        """Freeze recipient facts and validate optional target identity."""
        if self.target_account_id is not None:
            _positive("target_account_id", self.target_account_id)
        account_ids = tuple(self.round_participant_account_ids)
        if len(account_ids) != len(set(account_ids)) or any(account_id < 1 for account_id in account_ids):
            raise ValueError("round participant account IDs must be unique and positive")
        object.__setattr__(self, "round_participant_account_ids", account_ids)


@dataclass(frozen=True, slots=True)
class RoundParticipantSelection:
    """Freeze one participant's slot, team, and personal mods at round start."""

    account_id: int
    slot_position: int
    team: int
    mods: tuple[CanonicalMod, ...] = ()

    def __post_init__(self) -> None:
        """Validate the account, Stable slot, team, and immutable mods."""
        _positive("account_id", self.account_id)
        if not 0 <= self.slot_position < 16:
            raise ValueError("round participant slot must be between zero and fifteen")
        if self.team not in {0, 1, 2}:
            raise ValueError("round participant team is invalid")
        object.__setattr__(self, "mods", tuple(self.mods))


@dataclass(frozen=True, slots=True)
class DurableRoomSnapshot:
    """Carry the PostgreSQL facts needed to rebuild a lost room projection."""

    room: RoomRecord
    active_account_ids: tuple[int, ...]
    round_id: uuid.UUID | None = None
    round_participants: tuple[RoundParticipantSelection, ...] = ()

    def __post_init__(self) -> None:
        """Freeze participant collections and require a coherent active round."""
        accounts = tuple(self.active_account_ids)
        participants = tuple(self.round_participants)
        if len(accounts) != len(set(accounts)) or any(account_id < 1 for account_id in accounts):
            raise ValueError("active account IDs must be unique and positive")
        if self.round_id is None and participants:
            raise ValueError("round participants require an active round identity")
        if not {participant.account_id for participant in participants} <= set(accounts):
            raise ValueError("active round participants must have an active presence")
        object.__setattr__(self, "active_account_ids", accounts)
        object.__setattr__(self, "round_participants", participants)


@dataclass(frozen=True, slots=True)
class CreateRoom:
    """Create one persistent room and active hosting session."""

    meta: CommandMeta
    settings: RoomSettings
    password: str = field(default="", repr=False)
    capacity: int = 16

    def __post_init__(self) -> None:
        """Require an authenticated actor and bounded password/capacity."""
        if self.meta.actor is None:
            raise ValueError("create room requires an authenticated actor")
        if not 1 <= self.capacity <= MAX_ROOM_CAPACITY:
            raise ValueError("capacity is outside the supported range")
        if len(self.password) > 64:
            raise ValueError("room password must contain at most 64 characters")


@dataclass(frozen=True, slots=True)
class JoinRoom:
    """Join one active room using an optional password proof."""

    meta: CommandMeta
    public_id: int
    password: str = field(default="", repr=False)

    def __post_init__(self) -> None:
        """Require an authenticated actor and valid room identifier."""
        if self.meta.actor is None:
            raise ValueError("join room requires an authenticated actor")
        _positive("public_id", self.public_id)
        if len(self.password) > 512:
            raise ValueError("room admission credential must contain at most 512 characters")


@dataclass(frozen=True, slots=True)
class LeaveRoom:
    """Leave the actor's current active room."""

    meta: CommandMeta
    public_id: int


@dataclass(frozen=True, slots=True)
class KickParticipant:
    """Remove a non-host participant from a room."""

    meta: CommandMeta
    public_id: int
    expected_version: int
    target_account_id: int


@dataclass(frozen=True, slots=True)
class UpdateRoomSettings:
    """Replace mutable room settings under an expected aggregate version."""

    meta: CommandMeta
    public_id: int
    expected_version: int
    settings: RoomSettings


@dataclass(frozen=True, slots=True)
class ChangeHost:
    """Transfer room authority to an active participant."""

    meta: CommandMeta
    public_id: int
    expected_version: int
    target_account_id: int


@dataclass(frozen=True, slots=True)
class ChangeRoomPassword:
    """Replace or remove a room password under an expected aggregate version."""

    meta: CommandMeta
    public_id: int
    expected_version: int
    password: str = field(default="", repr=False)

    def __post_init__(self) -> None:
        """Require a bounded password value."""
        if len(self.password) > 64:
            raise ValueError("room password must contain at most 64 characters")


@dataclass(frozen=True, slots=True)
class StartRound:
    """Freeze the current participants and begin a synchronized round."""

    meta: CommandMeta
    public_id: int
    expected_version: int


@dataclass(frozen=True, slots=True)
class CompleteRound:
    """Complete the current synchronized round."""

    meta: CommandMeta
    public_id: int
    expected_version: int
    aborted: bool = False


@dataclass(frozen=True, slots=True)
class CleanupPresence:
    """Close a durable multiplayer presence owned by an expired realtime session."""

    meta: CommandMeta
    account_id: int
    connection_session_id: uuid.UUID
    reason: str = "session_expired"

    def __post_init__(self) -> None:
        """Require a bounded cleanup reason and positive account identity."""
        _positive("account_id", self.account_id)
        if not self.reason or len(self.reason) > 32:
            raise ValueError("cleanup reason must contain at most 32 characters")
