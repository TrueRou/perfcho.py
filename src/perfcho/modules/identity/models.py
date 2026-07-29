"""Define immutable Stable identity commands and results."""

import ipaddress
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from perfcho.modules.common.models import CommandMeta


@dataclass(frozen=True, slots=True)
class StableLogin:
    """Request one normal Stable client authentication session."""

    meta: CommandMeta
    identifier: str
    password_token: str = field(repr=False)
    client_version: str
    client_variant: str | None
    ip_address: str
    user_agent: str | None
    device_components: tuple[tuple[str, str], ...] = field(repr=False)
    session_lifetime: timedelta

    def __post_init__(self) -> None:
        """Validate bounded client evidence while leaving credentials opaque."""
        if self.meta.client.family != "stable":
            raise ValueError("Stable login commands require a stable client context")
        if not self.identifier:
            raise ValueError("identifier must not be empty")
        if not self.client_version or len(self.client_version) > 64:
            raise ValueError("client_version must contain at most 64 characters")
        if self.client_variant is not None and len(self.client_variant) > 32:
            raise ValueError("client_variant must contain at most 32 characters")
        if self.user_agent is not None and len(self.user_agent) > 512:
            raise ValueError("user_agent must contain at most 512 characters")
        try:
            ipaddress.ip_address(self.ip_address)
        except ValueError as error:
            raise ValueError("ip_address must be a valid IPv4 or IPv6 address") from error
        if self.session_lifetime <= timedelta(0):
            raise ValueError("session_lifetime must be positive")

        normalized_components: list[tuple[str, str]] = []
        kinds: set[str] = set()
        for kind, value in self.device_components:
            normalized_kind = kind.strip().casefold()
            if not normalized_kind or len(normalized_kind) > 32:
                raise ValueError("device component kinds must contain at most 32 characters")
            if not value:
                raise ValueError("device component values must not be empty")
            if normalized_kind in kinds:
                raise ValueError("device component kinds must be unique")
            kinds.add(normalized_kind)
            normalized_components.append((normalized_kind, value))
        if not normalized_components:
            raise ValueError("at least one device component is required")
        object.__setattr__(self, "device_components", tuple(normalized_components))


@dataclass(frozen=True, slots=True)
class CredentialSnapshot:
    """Carry scalar account and password facts across Argon2 verification."""

    account_id: int
    current_name: str
    account_status: str
    auth_version: int
    password_verifier: str = field(repr=False)
    pepper_version: int
    password_changed_at: datetime
    must_change: bool

    def __post_init__(self) -> None:
        """Reject malformed persistence projections."""
        if self.account_id < 1:
            raise ValueError("account_id must be positive")
        if self.auth_version < 1:
            raise ValueError("auth_version must be positive")
        if self.password_changed_at.tzinfo is None or self.password_changed_at.utcoffset() is None:
            raise ValueError("password_changed_at must be timezone-aware")


@dataclass(frozen=True, slots=True)
class StableSessionResult:
    """Return a newly created Stable session and its one-time bearer value."""

    account_id: int
    current_name: str
    session_id: uuid.UUID
    device_id: uuid.UUID
    raw_token: str = field(repr=False)
    expires_at: datetime

    def __post_init__(self) -> None:
        """Require a usable creation result."""
        if self.account_id < 1 or not self.raw_token:
            raise ValueError("Stable session results require an account and raw token")
        if self.expires_at.tzinfo is None or self.expires_at.utcoffset() is None:
            raise ValueError("expires_at must be timezone-aware")


@dataclass(frozen=True, slots=True)
class ResolvedStableSession:
    """Describe an active Stable bearer session without returning its token."""

    account_id: int
    current_name: str
    auth_version: int
    session_id: uuid.UUID
    device_id: uuid.UUID | None
    client_version: str | None
    client_variant: str | None
    expires_at: datetime

    def __post_init__(self) -> None:
        """Require authoritative identifiers and a timezone-aware expiry."""
        if self.account_id < 1 or self.auth_version < 1:
            raise ValueError("resolved sessions require positive account and auth versions")
        if self.expires_at.tzinfo is None or self.expires_at.utcoffset() is None:
            raise ValueError("expires_at must be timezone-aware")


@dataclass(frozen=True, slots=True)
class OpenStableSession:
    """Identify the one unclosed normal Stable session for an account."""

    session_id: uuid.UUID
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class StableWebPrincipal:
    """Identify credentials proven for an already-online Stable account."""

    account_id: int
    current_name: str
    session_id: uuid.UUID
    expires_at: datetime

    def __post_init__(self) -> None:
        """Validate the online principal and expiry."""
        if self.account_id < 1:
            raise ValueError("account_id must be positive")
        if self.expires_at.tzinfo is None or self.expires_at.utcoffset() is None:
            raise ValueError("expires_at must be timezone-aware")
