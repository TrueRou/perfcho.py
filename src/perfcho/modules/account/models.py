"""Define immutable account registration values."""

from dataclasses import dataclass
from datetime import datetime

from perfcho.modules.common.models import CommandMeta
from perfcho.modules.scoring.models import Ruleset

_REGISTRATION_STATUSES = frozenset({"active", "pending"})


@dataclass(frozen=True, slots=True)
class RegisterAccount:
    """Request creation of one canonical user account."""

    meta: CommandMeta
    display_name: str
    email: str
    password_preverification: str
    activate_immediately: bool = False


@dataclass(frozen=True, slots=True)
class RegistrationResult:
    """Return the stable, non-secret result of account registration."""

    account_id: int
    display_name: str
    email: str
    status: str

    def __post_init__(self) -> None:
        """Reject malformed persisted receipt snapshots."""
        if self.account_id < 1:
            raise ValueError("account_id must be positive")
        if self.status not in _REGISTRATION_STATUSES:
            raise ValueError("registration status must be active or pending")

    @property
    def active(self) -> bool:
        """Return whether registration activated the account immediately."""
        return self.status == "active"


@dataclass(frozen=True, slots=True)
class RegistrationRecord:
    """Carry validated account facts into a persistence adapter."""

    display_name: str
    name_key: str
    email: str
    email_key: str
    password_verifier: str
    pepper_version: int
    status: str
    registered_at: datetime

    def __post_init__(self) -> None:
        """Require canonical lifecycle and temporal values."""
        if self.status not in _REGISTRATION_STATUSES:
            raise ValueError("registration status must be active or pending")
        if self.registered_at.tzinfo is None or self.registered_at.utcoffset() is None:
            raise ValueError("registered_at must be timezone-aware")


@dataclass(frozen=True, slots=True)
class RegistrationClaim:
    """Describe a new receipt claim or its exact prior result."""

    prior_result: RegistrationResult | None = None

    @property
    def replayed(self) -> bool:
        """Return whether this claim represents a completed exact replay."""
        return self.prior_result is not None


@dataclass(frozen=True, slots=True)
class PublicAccountView:
    """Describe public account and profile facts independently of an API."""

    account_id: int
    current_name: str
    account_type: str
    country_code: str | None
    registered_at: datetime
    last_seen_at: datetime | None
    default_ruleset: Ruleset
    location: str | None = None
    occupation: str | None = None
    interests: str | None = None
    website: str | None = None
