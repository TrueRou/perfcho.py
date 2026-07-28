from enum import StrEnum

from sqlalchemy import Enum as SqlEnum


class AccountType(StrEnum):
    USER = "user"
    BOT = "bot"
    SERVICE = "service"


class AccountStatus(StrEnum):
    PENDING = "pending"
    ACTIVE = "active"
    LOCKED = "locked"
    DEACTIVATED = "deactivated"
    DELETED = "deleted"


class ClientFamily(StrEnum):
    STABLE = "stable"
    LAZER = "lazer"
    WEB = "web"
    API = "api"


class TokenKind(StrEnum):
    ACCESS = "access"
    REFRESH = "refresh"
    API_KEY = "api_key"
    STABLE_SESSION = "stable_session"


class ChallengeKind(StrEnum):
    EMAIL_VERIFICATION = "email_verification"
    PASSWORD_RESET = "password_reset"
    LOGIN_MFA = "login_mfa"
    OAUTH_CODE = "oauth_code"


class GrantEffect(StrEnum):
    ALLOW = "allow"
    DENY = "deny"


class SanctionKind(StrEnum):
    RESTRICTION = "restriction"
    SILENCE = "silence"
    CHANNEL_MUTE = "channel_mute"
    TOURNAMENT_BAN = "tournament_ban"
    LEADERBOARD_FREEZE = "leaderboard_freeze"


class Ruleset(StrEnum):
    OSU = "osu"
    TAIKO = "taiko"
    FRUITS = "fruits"
    MANIA = "mania"


class ScoreboardVariant(StrEnum):
    VANILLA = "vanilla"
    RELAX = "relax"
    AUTOPILOT = "autopilot"


class BeatmapStatus(StrEnum):
    GRAVEYARD = "graveyard"
    WIP = "wip"
    PENDING = "pending"
    RANKED = "ranked"
    APPROVED = "approved"
    QUALIFIED = "qualified"
    LOVED = "loved"


class ScoreOutcome(StrEnum):
    ABANDONED = "abandoned"
    FAILED = "failed"
    PASSED = "passed"


class ScoreGrade(StrEnum):
    N = "N"
    F = "F"
    D = "D"
    C = "C"
    B = "B"
    A = "A"
    S = "S"
    SH = "SH"
    X = "X"
    XH = "XH"


class ChannelKind(StrEnum):
    PUBLIC = "public"
    PRIVATE = "private"
    GROUP = "group"
    TEAM = "team"
    MULTIPLAYER = "multiplayer"
    SPECTATOR = "spectator"
    SYSTEM = "system"


class TeamRole(StrEnum):
    OWNER = "owner"
    OFFICER = "officer"
    MEMBER = "member"


class RoomStatus(StrEnum):
    PENDING = "pending"
    OPEN = "open"
    STARTED = "started"
    ENDED = "ended"
    CANCELLED = "cancelled"


class SessionStatus(StrEnum):
    PENDING = "pending"
    ACTIVE = "active"
    PAUSED = "paused"
    COMPLETED = "completed"
    ABORTED = "aborted"


class AttemptStatus(StrEnum):
    ISSUED = "issued"
    STARTED = "started"
    SUBMITTED = "submitted"
    VERIFIED = "verified"
    REJECTED = "rejected"
    EXPIRED = "expired"


def enum_type[T: StrEnum](enum: type[T], name: str, length: int = 32) -> SqlEnum[T]:
    return SqlEnum(
        enum,
        name=name,
        native_enum=False,
        create_constraint=True,
        validate_strings=True,
        values_callable=lambda members: [member.value for member in members],
        length=length,
    )
