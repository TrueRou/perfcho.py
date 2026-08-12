"""Wire-level value objects used by the osu! Stable Bancho protocol."""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum, unique

MATCH_SLOT_COUNT = 16
OCCUPIED_SLOT_MASK = 0x7C


@unique
class ClientPacket(IntEnum):
    """Packet identifiers sent by the latest osu! Stable client."""

    CHANGE_ACTION = 0
    SEND_PUBLIC_MESSAGE = 1
    LOGOUT = 2
    REQUEST_STATUS_UPDATE = 3
    PING = 4  # Named Osu_Pong in the client; a keepalive, not a request.
    START_SPECTATING = 16
    STOP_SPECTATING = 17
    SPECTATE_FRAMES = 18
    ERROR_REPORT = 20
    CANT_SPECTATE = 21
    SEND_PRIVATE_MESSAGE = 25
    PART_LOBBY = 29
    JOIN_LOBBY = 30
    CREATE_MATCH = 31
    JOIN_MATCH = 32
    PART_MATCH = 33
    MATCH_CHANGE_SLOT = 38
    MATCH_READY = 39
    MATCH_LOCK = 40
    MATCH_CHANGE_SETTINGS = 41
    MATCH_START = 44
    MATCH_SCORE_UPDATE = 47
    MATCH_COMPLETE = 49
    MATCH_CHANGE_MODS = 51
    MATCH_LOAD_COMPLETE = 52
    MATCH_NO_BEATMAP = 54
    MATCH_NOT_READY = 55
    MATCH_FAILED = 56
    MATCH_HAS_BEATMAP = 59
    MATCH_SKIP_REQUEST = 60
    CHANNEL_JOIN = 63
    BEATMAP_INFO_REQUEST = 68
    MATCH_TRANSFER_HOST = 70
    FRIEND_ADD = 73
    FRIEND_REMOVE = 74
    MATCH_CHANGE_TEAM = 77
    CHANNEL_PART = 78
    RECEIVE_UPDATES = 79
    SET_AWAY_MESSAGE = 82
    IRC_ONLY = 84
    USER_STATS_REQUEST = 85
    MATCH_INVITE = 87
    MATCH_CHANGE_PASSWORD = 90
    TOURNAMENT_MATCH_INFO_REQUEST = 93
    USER_PRESENCE_REQUEST = 97
    USER_PRESENCE_REQUEST_ALL = 98
    TOGGLE_BLOCK_NON_FRIEND_DMS = 99
    TOURNAMENT_JOIN_MATCH_CHANNEL = 108
    TOURNAMENT_LEAVE_MATCH_CHANNEL = 109


@unique
class ServerPacket(IntEnum):
    """Packet identifiers sent to the latest osu! Stable client."""

    USER_ID = 5
    SEND_MESSAGE = 7
    PONG = 8  # Named Bancho_Ping in the client; requests an Osu_Pong.
    HANDLE_IRC_CHANGE_USERNAME = 9
    HANDLE_IRC_QUIT = 10
    USER_STATS = 11
    USER_LOGOUT = 12
    SPECTATOR_JOINED = 13
    SPECTATOR_LEFT = 14
    SPECTATE_FRAMES = 15
    VERSION_UPDATE = 19
    SPECTATOR_CANT_SPECTATE = 22
    GET_ATTENTION = 23
    NOTIFICATION = 24
    UPDATE_MATCH = 26
    NEW_MATCH = 27
    DISPOSE_MATCH = 28
    TOGGLE_BLOCK_NON_FRIEND_DMS = 34
    MATCH_JOIN_SUCCESS = 36
    MATCH_JOIN_FAIL = 37
    FELLOW_SPECTATOR_JOINED = 42
    FELLOW_SPECTATOR_LEFT = 43
    ALL_PLAYERS_LOADED = 45
    MATCH_START = 46
    MATCH_SCORE_UPDATE = 48
    MATCH_TRANSFER_HOST = 50
    MATCH_ALL_PLAYERS_LOADED = 53
    MATCH_PLAYER_FAILED = 57
    MATCH_COMPLETE = 58
    MATCH_SKIP = 61
    UNAUTHORIZED = 62
    CHANNEL_JOIN_SUCCESS = 64
    CHANNEL_INFO = 65
    CHANNEL_KICK = 66
    CHANNEL_AUTO_JOIN = 67
    BEATMAP_INFO_REPLY = 69
    PRIVILEGES = 71
    FRIENDS_LIST = 72
    PROTOCOL_VERSION = 75
    MAIN_MENU_ICON = 76
    MONITOR = 80
    MATCH_PLAYER_SKIPPED = 81
    USER_PRESENCE = 83
    RESTART = 86
    MATCH_INVITE = 88
    CHANNEL_INFO_END = 89
    MATCH_CHANGE_PASSWORD = 91
    SILENCE_END = 92
    USER_SILENCED = 94
    USER_PRESENCE_SINGLE = 95
    USER_PRESENCE_BUNDLE = 96
    USER_DM_BLOCKED = 100
    TARGET_IS_SILENCED = 101
    VERSION_UPDATE_FORCED = 102
    SWITCH_SERVER = 103
    ACCOUNT_RESTRICTED = 104
    RTX = 105
    MATCH_ABORT = 106
    SWITCH_TOURNAMENT_SERVER = 107


@unique
class LoginFailureReason(IntEnum):
    """Negative user identifiers understood as login failures by Stable."""

    AUTHENTICATION_FAILED = -1
    OLD_CLIENT = -2
    BANNED = -3
    ERROR = -5
    NEEDS_SUPPORTER = -6
    PASSWORD_RESET = -7
    REQUIRES_VERIFICATION = -8


@unique
class ReplayAction(IntEnum):
    """Actions carried by Stable spectator replay bundles."""

    STANDARD = 0
    NEW_SONG = 1
    SKIP = 2
    COMPLETION = 3
    FAIL = 4
    PAUSE = 5
    UNPAUSE = 6
    SONG_SELECT = 7
    WATCHING_OTHER = 8


@dataclass(frozen=True, slots=True)
class Message:
    """Stable chat message payload."""

    sender: str
    text: str
    recipient: str
    sender_id: int


@dataclass(frozen=True, slots=True)
class Channel:
    """Stable channel description payload."""

    name: str
    topic: str
    player_count: int


@dataclass(frozen=True, slots=True)
class ClientStatus:
    """Status fields sent in a client change-action packet."""

    action: int
    info_text: str
    beatmap_md5: str
    mods: int
    mode: int
    beatmap_id: int

    def __post_init__(self) -> None:
        """Reject values outside the Stable action and ruleset inventories."""
        if isinstance(self.action, bool) or not isinstance(self.action, int) or not 0 <= self.action <= 13:
            raise ValueError("Stable client action must be between 0 and 13")
        if isinstance(self.mode, bool) or not isinstance(self.mode, int) or not 0 <= self.mode <= 3:
            raise ValueError("Stable client mode must be between 0 and 3")


@dataclass(frozen=True, slots=True)
class UserPresence:
    """Stable user-presence payload with an unpacked mode and UTC offset."""

    user_id: int
    username: str
    utc_offset: int
    country_code: int
    privileges: int
    mode: int
    longitude: float
    latitude: float
    global_rank: int


@dataclass(frozen=True, slots=True)
class UserStats:
    """Stable user-statistics payload; accuracy is the wire ratio from zero to one."""

    user_id: int
    action: int
    info_text: str
    beatmap_md5: str
    mods: int
    mode: int
    beatmap_id: int
    ranked_score: int
    accuracy: float
    play_count: int
    total_score: int
    global_rank: int
    performance: int


EMPTY_SLOT_BYTES = (0,) * MATCH_SLOT_COUNT
EMPTY_SLOT_USERS = (None,) * MATCH_SLOT_COUNT


@dataclass(frozen=True, slots=True)
class MultiplayerMatch:
    """Canonical representation of the Stable multiplayer match wire structure."""

    match_id: int = 0
    in_progress: bool = False
    match_type: int = 0
    mods: int = 0
    name: str = ""
    password: str = ""
    beatmap_name: str = ""
    beatmap_id: int = 0
    beatmap_md5: str = ""
    slot_statuses: tuple[int, ...] = EMPTY_SLOT_BYTES
    slot_teams: tuple[int, ...] = EMPTY_SLOT_BYTES
    slot_user_ids: tuple[int | None, ...] = EMPTY_SLOT_USERS
    host_id: int = 0
    mode: int = 0
    win_condition: int = 0
    team_type: int = 0
    freemods: bool = False
    slot_mods: tuple[int, ...] = ()
    seed: int = 0


@dataclass(frozen=True, slots=True)
class ScoreFrame:
    """Score state embedded in multiplayer and spectator frame payloads."""

    time: int
    frame_id: int
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
    current_hp: int
    tag_byte: int
    score_v2: bool
    combo_portion: float | None = None
    bonus_portion: float | None = None


@dataclass(frozen=True, slots=True)
class ReplayFrame:
    """One input frame in a Stable spectator frame bundle."""

    button_state: int
    taiko_byte: int
    x: float
    y: float
    time: int


@dataclass(frozen=True, slots=True)
class ReplayFrameBundle:
    """Parsed Stable spectator frame bundle."""

    frames: tuple[ReplayFrame, ...]
    score_frame: ScoreFrame
    action: ReplayAction
    extra: int
    sequence: int

    def __post_init__(self) -> None:
        """Normalize and validate the bounded spectator action inventory."""
        if isinstance(self.action, bool):
            raise ValueError("replay action must be between 0 and 8")
        try:
            action = ReplayAction(self.action)
        except (TypeError, ValueError) as error:
            raise ValueError("replay action must be between 0 and 8") from error
        object.__setattr__(self, "action", action)
