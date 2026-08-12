"""Define immutable identity commands and results shared by protocol adapters."""

import ipaddress
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from perfcho.modules.common.models import CommandMeta


@dataclass(frozen=True, slots=True)
class AuthenticateClientSession:
    """Request one direct client authentication session."""

    meta: CommandMeta
    identifier: str
    password_preverification: str = field(repr=False)
    client_version: str
    client_variant: str | None
    ip_address: str
    user_agent: str | None
    device_components: tuple[tuple[str, str], ...] = field(repr=False)
    session_lifetime: timedelta

    def __post_init__(self) -> None:
        """Validate bounded client evidence while leaving credentials opaque."""
        if not self.meta.client.family:
            raise ValueError("client session commands require a client family")
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
class PasswordGrant:
    """Request a password-authenticated OAuth session."""

    identifier: str
    password_preverification: str = field(repr=False)
    client_key: str
    client_secret: str = field(repr=False)
    requested_scope: str
    client_family: str
    client_version: str | None
    ip_address: str
    user_agent: str | None
    session_lifetime: timedelta
    access_token_lifetime: timedelta
    refresh_token_lifetime: timedelta

    def __post_init__(self) -> None:
        """Validate bounded transport evidence while leaving proofs opaque."""
        if not self.identifier or not self.client_key or not self.client_secret or not self.client_family:
            raise ValueError("password grants require identifier and client credentials")
        if self.requested_scope != "*":
            raise ValueError("password grants currently require wildcard scope")
        if self.client_version is not None and len(self.client_version) > 64:
            raise ValueError("client_version must contain at most 64 characters")
        if self.user_agent is not None and len(self.user_agent) > 512:
            raise ValueError("user_agent must contain at most 512 characters")
        try:
            ipaddress.ip_address(self.ip_address)
        except ValueError as error:
            raise ValueError("ip_address must be a valid IPv4 or IPv6 address") from error
        if min(self.session_lifetime, self.access_token_lifetime, self.refresh_token_lifetime) <= timedelta(0):
            raise ValueError("password grant lifetimes must be positive")
        if self.access_token_lifetime > self.session_lifetime:
            raise ValueError("access token lifetime must not exceed session lifetime")
        if self.refresh_token_lifetime > self.session_lifetime:
            raise ValueError("refresh token lifetime must not exceed session lifetime")


@dataclass(frozen=True, slots=True)
class RefreshGrant:
    """Request rotation of an OAuth refresh token."""

    refresh_token: str = field(repr=False)
    client_key: str
    client_secret: str = field(repr=False)
    requested_scope: str
    access_token_lifetime: timedelta
    refresh_token_lifetime: timedelta

    def __post_init__(self) -> None:
        """Validate the refresh request without exposing bearer values."""
        if not self.refresh_token or not self.client_key or not self.client_secret:
            raise ValueError("refresh grants require token and client credentials")
        if self.requested_scope != "*":
            raise ValueError("refresh grants currently require wildcard scope")
        if min(self.access_token_lifetime, self.refresh_token_lifetime) <= timedelta(0):
            raise ValueError("refresh grant lifetimes must be positive")


@dataclass(frozen=True, slots=True)
class OAuthClientSnapshot:
    """Describe an active OAuth client and its effective scopes."""

    client_id: uuid.UUID
    client_key: str
    first_party: bool
    scope_ids: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class OAuthTokenResult:
    """Return a newly issued OAuth access/refresh token pair."""

    access_token: str = field(repr=False)
    refresh_token: str = field(repr=False)
    expires_in: int
    scope: str = "*"
    token_type: str = "Bearer"

    def __post_init__(self) -> None:
        """Require a complete client-facing token response."""
        if not self.access_token or not self.refresh_token or self.expires_in < 1:
            raise ValueError("OAuth token results require non-empty tokens and positive expiry")


@dataclass(frozen=True, slots=True)
class RefreshTokenSnapshot:
    """Carry locked refresh-token lineage needed for atomic rotation."""

    token_id: uuid.UUID
    family_id: uuid.UUID
    session_id: uuid.UUID
    account_id: int
    client_id: uuid.UUID
    rotation_number: int
    session_expires_at: datetime
    token_expires_at: datetime
    consumed_at: datetime | None
    scope_ids: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class AuthenticatedAccount:
    """Describe the account represented by an active OAuth access token."""

    account_id: int
    current_name: str
    account_type: str
    country_code: str | None
    registered_at: datetime
    last_seen_at: datetime | None
    session_id: uuid.UUID
    scope_codes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class CredentialSnapshot:
    """Carry scalar account and password facts across password verification."""

    account_id: int
    current_name: str
    account_status: str
    auth_version: int
    password_verifier: str = field(repr=False)
    algorithm: str
    pepper_version: int | None
    password_changed_at: datetime
    must_change: bool
    country_code: str | None = None

    def __post_init__(self) -> None:
        """Reject malformed persistence projections."""
        if self.account_id < 1:
            raise ValueError("account_id must be positive")
        if self.auth_version < 1:
            raise ValueError("auth_version must be positive")
        if self.algorithm == "argon2id":
            if self.pepper_version is None or self.pepper_version < 1:
                raise ValueError("Argon2id credentials require a positive pepper version")
        elif self.algorithm == "bcrypt_md5":
            if self.pepper_version is not None:
                raise ValueError("bcrypt_md5 credentials must not have a pepper version")
        else:
            raise ValueError("unsupported password credential algorithm")
        if self.password_changed_at.tzinfo is None or self.password_changed_at.utcoffset() is None:
            raise ValueError("password_changed_at must be timezone-aware")


@dataclass(frozen=True, slots=True)
class ClientSessionResult:
    """Return a newly created client session and its one-time bearer value."""

    account_id: int
    current_name: str
    session_id: uuid.UUID
    device_id: uuid.UUID
    raw_token: str = field(repr=False)
    expires_at: datetime
    country_code: str | None = None

    def __post_init__(self) -> None:
        """Require a usable creation result."""
        if self.account_id < 1 or not self.raw_token:
            raise ValueError("client session results require an account and raw token")
        if self.expires_at.tzinfo is None or self.expires_at.utcoffset() is None:
            raise ValueError("expires_at must be timezone-aware")


@dataclass(frozen=True, slots=True)
class ResolvedClientSession:
    """Describe an active client bearer session without returning its token."""

    account_id: int
    current_name: str
    auth_version: int
    session_id: uuid.UUID
    device_id: uuid.UUID | None
    client_version: str | None
    client_variant: str | None
    expires_at: datetime
    country_code: str | None = None
    opened_at: datetime | None = None
    last_activity_at: datetime | None = None

    def __post_init__(self) -> None:
        """Require authoritative identifiers and a timezone-aware expiry."""
        if self.account_id < 1 or self.auth_version < 1:
            raise ValueError("resolved sessions require positive account and auth versions")
        for field_name, value in (
            ("expires_at", self.expires_at),
            ("opened_at", self.opened_at),
            ("last_activity_at", self.last_activity_at),
        ):
            if value is not None and (value.tzinfo is None or value.utcoffset() is None):
                raise ValueError(f"{field_name} must be timezone-aware")
        if (
            self.opened_at is not None
            and self.last_activity_at is not None
            and not self.opened_at <= self.last_activity_at <= self.expires_at
        ):
            raise ValueError("resolved session activity must fall within its lifetime")


@dataclass(frozen=True, slots=True)
class OpenClientSession:
    """Identify an unclosed direct client session for an account."""

    session_id: uuid.UUID
    opened_at: datetime
    last_activity_at: datetime
    expires_at: datetime

    def __post_init__(self) -> None:
        """Require ordered timezone-aware session activity facts."""
        for field_name, value in (
            ("opened_at", self.opened_at),
            ("last_activity_at", self.last_activity_at),
            ("expires_at", self.expires_at),
        ):
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError(f"{field_name} must be timezone-aware")
        if not self.opened_at <= self.last_activity_at <= self.expires_at:
            raise ValueError("session activity must fall within its lifetime")


@dataclass(frozen=True, slots=True)
class OnlineCredentialPrincipal:
    """Identify credentials proven for an account with an online session."""

    account_id: int
    current_name: str
    session_id: uuid.UUID
    expires_at: datetime
    country_code: str | None = None

    def __post_init__(self) -> None:
        """Validate the online principal and expiry."""
        if self.account_id < 1:
            raise ValueError("account_id must be positive")
        if self.expires_at.tzinfo is None or self.expires_at.utcoffset() is None:
            raise ValueError("expires_at must be timezone-aware")
