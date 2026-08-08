"""Define stable string values shared across canonical domains."""

from enum import StrEnum

from sqlalchemy import Enum as SqlEnum


class AccountType(StrEnum):
    """Classify human, bot, and internal service accounts."""

    USER = "user"
    BOT = "bot"
    SERVICE = "service"


class AccountStatus(StrEnum):
    """Describe the authoritative account lifecycle state."""

    PENDING = "pending"
    ACTIVE = "active"
    LOCKED = "locked"
    DEACTIVATED = "deactivated"
    DELETED = "deleted"


class ClientFamily(StrEnum):
    """Identify the protocol family that originated an operation."""

    STABLE = "stable"
    LAZER = "lazer"
    WEB = "web"
    API = "api"


class TokenKind(StrEnum):
    """Distinguish authentication token lifecycle and usage rules."""

    ACCESS = "access"
    REFRESH = "refresh"
    API_KEY = "api_key"
    STABLE_SESSION = "stable_session"


class ChallengeKind(StrEnum):
    """Distinguish single-use authentication challenge workflows."""

    EMAIL_VERIFICATION = "email_verification"
    PASSWORD_RESET = "password_reset"
    LOGIN_MFA = "login_mfa"
    OAUTH_CODE = "oauth_code"


class GrantEffect(StrEnum):
    """Apply an explicit allow or deny authorization decision."""

    ALLOW = "allow"
    DENY = "deny"


class SanctionKind(StrEnum):
    """Describe enforceable moderation restrictions."""

    RESTRICTION = "restriction"
    SILENCE = "silence"
    CHANNEL_MUTE = "channel_mute"
    TOURNAMENT_BAN = "tournament_ban"
    LEADERBOARD_FREEZE = "leaderboard_freeze"


class Ruleset(StrEnum):
    """Identify a canonical osu! gameplay ruleset."""

    OSU = "osu"
    TAIKO = "taiko"
    FRUITS = "fruits"
    MANIA = "mania"


class ScoreboardVariant(StrEnum):
    """Identify vanilla and Stable modification-based scoreboards."""

    VANILLA = "vanilla"
    RELAX = "relax"
    AUTOPILOT = "autopilot"


class BeatmapStatus(StrEnum):
    """Represent canonical beatmap ranking states."""

    GRAVEYARD = "graveyard"
    WIP = "wip"
    PENDING = "pending"
    RANKED = "ranked"
    APPROVED = "approved"
    QUALIFIED = "qualified"
    LOVED = "loved"


class ScoreOutcome(StrEnum):
    """Record whether a play ended, failed, or passed."""

    ABANDONED = "abandoned"
    FAILED = "failed"
    PASSED = "passed"


class ScoreGrade(StrEnum):
    """Represent the displayed Stable score grade."""

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
    """Classify persistent community channel behavior."""

    PUBLIC = "public"
    PRIVATE = "private"
    GROUP = "group"
    TEAM = "team"
    MULTIPLAYER = "multiplayer"
    SPECTATOR = "spectator"
    SYSTEM = "system"


class TeamRole(StrEnum):
    """Describe a member's authority within a team."""

    OWNER = "owner"
    OFFICER = "officer"
    MEMBER = "member"


class RoomStatus(StrEnum):
    """Describe a persistent multiplayer room lifecycle."""

    PENDING = "pending"
    OPEN = "open"
    STARTED = "started"
    ENDED = "ended"
    CANCELLED = "cancelled"


class SessionStatus(StrEnum):
    """Describe one hosted multiplayer session lifecycle."""

    PENDING = "pending"
    ACTIVE = "active"
    PAUSED = "paused"
    COMPLETED = "completed"
    ABORTED = "aborted"


class AttemptStatus(StrEnum):
    """Describe score and multiplayer attempt processing states."""

    ISSUED = "issued"
    STARTED = "started"
    SUBMITTED = "submitted"
    VERIFIED = "verified"
    REJECTED = "rejected"
    EXPIRED = "expired"


class CalculationKind(StrEnum):
    """Distinguish reusable difficulty and performance formulas."""

    DIFFICULTY = "difficulty"
    PERFORMANCE = "performance"


class OutboxDeliveryStatus(StrEnum):
    """Describe one durable outbox delivery lifecycle."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    DEAD = "dead"


def enum_type[T: StrEnum](enum: type[T], name: str, length: int = 32) -> SqlEnum:
    """Store a string enum with database checks and value validation."""
    return SqlEnum(
        enum,
        name=name,
        native_enum=False,
        create_constraint=True,
        validate_strings=True,
        values_callable=lambda members: [member.value for member in members],
        length=length,
    )
