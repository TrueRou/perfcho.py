"""Map credentials, sessions, tokens, devices, and login evidence."""

import uuid
from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    LargeBinary,
    SmallInteger,
    String,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import INET, JSONB
from sqlalchemy.orm import Mapped, mapped_column

from perfcho.infra.db.base import DbBase
from perfcho.infra.db.enums import ChallengeKind, ClientFamily, TokenKind, enum_type
from perfcho.infra.db.mixins import BigIntIdentityMixin, CreatedAtMixin, TimestampMixin, Uuid7PrimaryKeyMixin


class PasswordCredential(TimestampMixin, DbBase):
    """Stores the current password verifier for an account."""

    __tablename__ = "password_credential"
    __table_args__ = (
        CheckConstraint(
            "(algorithm = 'argon2id' AND pepper_version IS NOT NULL AND pepper_version > 0) OR "
            "(algorithm = 'bcrypt_md5' AND pepper_version IS NULL)",
            name="algorithm_pepper_consistency",
        ),
        {"schema": "iam"},
    )

    account_id: Mapped[int] = mapped_column(ForeignKey("core.account.id", ondelete="RESTRICT"), primary_key=True)
    verifier: Mapped[str] = mapped_column(String(512), nullable=False)
    algorithm: Mapped[str] = mapped_column(String(16), nullable=False, default="argon2id", server_default="argon2id")
    pepper_version: Mapped[int | None] = mapped_column(SmallInteger)
    password_changed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    must_change: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")


class OAuthClient(Uuid7PrimaryKeyMixin, TimestampMixin, DbBase):
    """Defines first-party and third-party OAuth clients."""

    __tablename__ = "oauth_client"
    __table_args__ = (
        UniqueConstraint("client_key"),
        Index("ix_oauth_client_owner", "owner_account_id"),
        {"schema": "iam"},
    )

    client_key: Mapped[str] = mapped_column(String(64), nullable=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    owner_account_id: Mapped[int | None] = mapped_column(ForeignKey("core.account.id", ondelete="SET NULL"))
    is_confidential: Mapped[bool] = mapped_column(Boolean, nullable=False)
    first_party: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="true")


class OAuthClientRedirectUri(DbBase):
    """Lists the exact redirect URIs allowed for an OAuth client."""

    __tablename__ = "oauth_client_redirect_uri"
    __table_args__ = ({"schema": "iam"},)

    client_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("iam.oauth_client.id", ondelete="CASCADE"), primary_key=True
    )
    redirect_uri: Mapped[str] = mapped_column(String(2048), primary_key=True)


class OAuthClientSecret(Uuid7PrimaryKeyMixin, CreatedAtMixin, DbBase):
    """Stores rotatable OAuth client secret digests and validity periods."""

    __tablename__ = "oauth_client_secret"
    __table_args__ = (
        CheckConstraint("expires_at IS NULL OR expires_at > created_at", name="valid_period"),
        Index("ix_oauth_client_secret_client", "client_id", "created_at"),
        {"schema": "iam"},
    )

    client_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("iam.oauth_client.id", ondelete="CASCADE"), nullable=False)
    secret_digest: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False, unique=True)
    secret_prefix: Mapped[str] = mapped_column(String(16), nullable=False)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class Scope(DbBase):
    """Defines access scopes available to OAuth and API tokens."""

    __tablename__ = "scope"
    __table_args__ = (UniqueConstraint("code"), {"schema": "iam"})

    id: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    code: Mapped[str] = mapped_column(String(64), nullable=False)
    description: Mapped[str] = mapped_column(String(255), nullable=False)


class OAuthClientScope(DbBase):
    """Lists the scopes an OAuth client is allowed to request."""

    __tablename__ = "oauth_client_scope"
    __table_args__ = ({"schema": "iam"},)

    client_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("iam.oauth_client.id", ondelete="CASCADE"), primary_key=True
    )
    scope_id: Mapped[int] = mapped_column(ForeignKey("iam.scope.id", ondelete="RESTRICT"), primary_key=True)


class Device(Uuid7PrimaryKeyMixin, TimestampMixin, DbBase):
    """Represents a stable device assembled from multiple security identifiers."""

    __tablename__ = "device"
    __table_args__ = (UniqueConstraint("fingerprint_hmac"), {"schema": "iam"})

    fingerprint_hmac: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    platform: Mapped[str | None] = mapped_column(String(64))
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    risk_level: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=0, server_default="0")


class DeviceIdentifier(BigIntIdentityMixin, CreatedAtMixin, DbBase):
    """Stores server-keyed HMAC values for individual device identifier components."""

    __tablename__ = "device_identifier"
    __table_args__ = (
        UniqueConstraint("device_id", "kind", "value_hmac"),
        Index("ix_device_identifier_reverse", "kind", "value_hmac"),
        {"schema": "iam"},
    )

    device_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("iam.device.id", ondelete="CASCADE"), nullable=False)
    kind: Mapped[str] = mapped_column(String(32), nullable=False)
    value_hmac: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    quality: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=0, server_default="0")


class AccountDevice(DbBase):
    """Tracks device usage, trust, and revocation relationships for accounts."""

    __tablename__ = "account_device"
    __table_args__ = (
        CheckConstraint("use_count > 0", name="positive_use_count"),
        Index("ix_account_device_device", "device_id", "last_used_at"),
        {"schema": "iam"},
    )

    account_id: Mapped[int] = mapped_column(ForeignKey("core.account.id", ondelete="RESTRICT"), primary_key=True)
    device_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("iam.device.id", ondelete="RESTRICT"), primary_key=True)
    first_used_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_used_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    use_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default="1")
    trusted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class AuthSession(Uuid7PrimaryKeyMixin, CreatedAtMixin, DbBase):
    """Stores the authoritative lifecycle of a Stable, Lazer, web, or API login."""

    __tablename__ = "auth_session"
    __table_args__ = (
        CheckConstraint("expires_at > created_at", name="valid_period"),
        CheckConstraint(
            "last_activity_at >= created_at AND last_activity_at <= expires_at",
            name="activity_period",
        ),
        CheckConstraint("session_class IN ('normal', 'tourney')", name="session_class_values"),
        CheckConstraint(
            "session_class <> 'tourney' OR client_family = 'stable'",
            name="tourney_session_stable_only",
        ),
        UniqueConstraint("id", "account_id", name="uq_auth_session_id_account"),
        Index("ix_auth_session_account_created", "account_id", "created_at"),
        Index(
            "ix_auth_session_active_account",
            "account_id",
            postgresql_where=text("revoked_at IS NULL AND closed_at IS NULL"),
        ),
        Index(
            "uq_auth_session_active_client_account",
            "account_id",
            unique=True,
            postgresql_where=text(
                "oauth_client_id IS NULL AND session_class = 'normal' AND revoked_at IS NULL AND closed_at IS NULL"
            ),
        ),
        {"schema": "iam"},
    )

    account_id: Mapped[int] = mapped_column(ForeignKey("core.account.id", ondelete="RESTRICT"), nullable=False)
    oauth_client_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("iam.oauth_client.id", ondelete="SET NULL"))
    device_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("iam.device.id", ondelete="SET NULL"))
    client_family: Mapped[ClientFamily] = mapped_column(enum_type(ClientFamily, "client_family", 16), nullable=False)
    client_variant: Mapped[str | None] = mapped_column(String(32))
    session_class: Mapped[str] = mapped_column(String(16), nullable=False, default="normal", server_default="normal")
    client_version: Mapped[str | None] = mapped_column(String(64))
    ip_address: Mapped[str] = mapped_column(INET, nullable=False)
    user_agent: Mapped[str | None] = mapped_column(String(512))
    mfa_verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_activity_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    close_reason: Mapped[str | None] = mapped_column(String(64))


class AuthTokenFamily(Uuid7PrimaryKeyMixin, CreatedAtMixin, DbBase):
    """Groups refresh-token rotations under one independently revocable session lineage."""

    __tablename__ = "auth_token_family"
    __table_args__ = (
        CheckConstraint(
            "compromised_at IS NULL OR revoked_at IS NOT NULL",
            name="compromise_revokes_family",
        ),
        ForeignKeyConstraint(
            ["session_id", "account_id"],
            ["iam.auth_session.id", "iam.auth_session.account_id"],
            name="fk_auth_token_family_session_account",
            ondelete="CASCADE",
        ),
        UniqueConstraint(
            "id",
            "session_id",
            "account_id",
            name="uq_auth_token_family_id_session_account",
        ),
        Index("ix_auth_token_family_session_created", "session_id", "created_at"),
        Index(
            "ix_auth_token_family_active_session",
            "session_id",
            postgresql_where=text("revoked_at IS NULL"),
        ),
        {"schema": "iam"},
    )

    session_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    account_id: Mapped[int] = mapped_column(ForeignKey("core.account.id", ondelete="RESTRICT"), nullable=False)
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    compromised_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    revoke_reason: Mapped[str | None] = mapped_column(String(64))


class AuthToken(Uuid7PrimaryKeyMixin, CreatedAtMixin, DbBase):
    """Stores digests for access, refresh, API, and Stable session tokens."""

    __tablename__ = "auth_token"
    __table_args__ = (
        CheckConstraint("expires_at > created_at", name="valid_period"),
        CheckConstraint(
            "(kind = 'refresh' AND family_id IS NOT NULL AND rotation_number IS NOT NULL AND "
            "((rotation_number = 0 AND parent_token_id IS NULL) OR "
            "(rotation_number > 0 AND parent_token_id IS NOT NULL))) OR "
            "(kind <> 'refresh' AND family_id IS NULL AND rotation_number IS NULL AND parent_token_id IS NULL)",
            name="refresh_family_required",
        ),
        CheckConstraint("rotation_number IS NULL OR rotation_number >= 0", name="nonnegative_rotation"),
        ForeignKeyConstraint(
            ["session_id", "account_id"],
            ["iam.auth_session.id", "iam.auth_session.account_id"],
            name="fk_auth_token_session_account",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["family_id", "session_id", "account_id"],
            [
                "iam.auth_token_family.id",
                "iam.auth_token_family.session_id",
                "iam.auth_token_family.account_id",
            ],
            name="fk_auth_token_family_session_account",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["parent_token_id", "family_id"],
            ["iam.auth_token.id", "iam.auth_token.family_id"],
            name="fk_auth_token_parent_family",
            ondelete="RESTRICT",
        ),
        UniqueConstraint("digest"),
        UniqueConstraint("jti"),
        UniqueConstraint("id", "family_id", name="uq_auth_token_id_family"),
        UniqueConstraint("family_id", "rotation_number", name="uq_auth_token_family_rotation"),
        Index("ix_auth_token_session", "session_id", "created_at"),
        Index("ix_auth_token_family", "family_id", "rotation_number"),
        Index("ix_auth_token_account_expiry", "account_id", "expires_at"),
        Index("ix_auth_token_expiry", "expires_at"),
        {"schema": "iam"},
    )

    session_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    account_id: Mapped[int] = mapped_column(ForeignKey("core.account.id", ondelete="RESTRICT"), nullable=False)
    family_id: Mapped[uuid.UUID | None] = mapped_column()
    parent_token_id: Mapped[uuid.UUID | None] = mapped_column()
    rotation_number: Mapped[int | None] = mapped_column(Integer)
    kind: Mapped[TokenKind] = mapped_column(enum_type(TokenKind, "token_kind", 24), nullable=False)
    digest: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    prefix: Mapped[str] = mapped_column(String(16), nullable=False)
    jti: Mapped[uuid.UUID] = mapped_column(nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    consumed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class AuthTokenScope(DbBase):
    """Associates issued authentication tokens with their effective OAuth scopes."""

    __tablename__ = "auth_token_scope"
    __table_args__ = ({"schema": "iam"},)

    token_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("iam.auth_token.id", ondelete="CASCADE"), primary_key=True)
    scope_id: Mapped[int] = mapped_column(ForeignKey("iam.scope.id", ondelete="RESTRICT"), primary_key=True)


class AuthChallenge(Uuid7PrimaryKeyMixin, CreatedAtMixin, DbBase):
    """Stores single-use email, password reset, MFA, and OAuth authorization challenges."""

    __tablename__ = "auth_challenge"
    __table_args__ = (
        CheckConstraint("attempt_count >= 0 AND attempt_count <= max_attempts", name="attempt_count_range"),
        CheckConstraint("max_attempts > 0", name="positive_max_attempts"),
        CheckConstraint("expires_at > created_at", name="valid_period"),
        CheckConstraint(
            "(kind = 'oauth_code' AND oauth_client_id IS NOT NULL AND redirect_uri IS NOT NULL) OR "
            "(kind <> 'oauth_code' AND redirect_uri IS NULL)",
            name="oauth_redirect_snapshot",
        ),
        UniqueConstraint("code_digest"),
        Index("ix_auth_challenge_account_kind", "account_id", "kind", "expires_at"),
        Index("ix_auth_challenge_expiry", "expires_at"),
        {"schema": "iam"},
    )

    account_id: Mapped[int | None] = mapped_column(ForeignKey("core.account.id", ondelete="RESTRICT"))
    session_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("iam.auth_session.id", ondelete="CASCADE"))
    oauth_client_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("iam.oauth_client.id", ondelete="CASCADE"))
    kind: Mapped[ChallengeKind] = mapped_column(enum_type(ChallengeKind, "challenge_kind", 32), nullable=False)
    target: Mapped[str | None] = mapped_column(String(254))
    code_digest: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    redirect_uri: Mapped[str | None] = mapped_column(String(2048))
    pkce_challenge: Mapped[str | None] = mapped_column(String(128))
    pkce_method: Mapped[str | None] = mapped_column(String(16))
    attempt_count: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=0, server_default="0")
    max_attempts: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=5, server_default="5")
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    consumed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    superseded_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class AuthChallengeScope(DbBase):
    """Snapshots the exact effective OAuth scopes bound to an authorization challenge."""

    __tablename__ = "auth_challenge_scope"
    __table_args__ = (Index("ix_auth_challenge_scope_scope", "scope_id", "challenge_id"), {"schema": "iam"})

    challenge_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("iam.auth_challenge.id", ondelete="CASCADE"), primary_key=True
    )
    scope_id: Mapped[int] = mapped_column(ForeignKey("iam.scope.id", ondelete="RESTRICT"), primary_key=True)


class TotpFactor(Uuid7PrimaryKeyMixin, CreatedAtMixin, DbBase):
    """Stores encrypted TOTP factor secrets and their activation state."""

    __tablename__ = "totp_factor"
    __table_args__ = (
        CheckConstraint("digits BETWEEN 6 AND 8", name="digits_range"),
        CheckConstraint("period_seconds BETWEEN 15 AND 120", name="period_range"),
        CheckConstraint("last_accepted_counter IS NULL OR last_accepted_counter >= 0", name="nonnegative_counter"),
        Index(
            "uq_totp_factor_active_account", "account_id", unique=True, postgresql_where=text("disabled_at IS NULL")
        ),
        {"schema": "iam"},
    )

    account_id: Mapped[int] = mapped_column(ForeignKey("core.account.id", ondelete="RESTRICT"), nullable=False)
    encrypted_secret: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    key_id: Mapped[str] = mapped_column(String(128), nullable=False)
    digits: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=6, server_default="6")
    period_seconds: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=30, server_default="30")
    last_accepted_counter: Mapped[int | None] = mapped_column(BigInteger)
    confirmed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    disabled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class RecoveryCode(DbBase):
    """Stores single-use recovery code digests for a TOTP factor."""

    __tablename__ = "recovery_code"
    __table_args__ = (UniqueConstraint("code_digest"), {"schema": "iam"})

    factor_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("iam.totp_factor.id", ondelete="CASCADE"), primary_key=True
    )
    ordinal: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    code_digest: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    consumed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class AuthAttempt(BigIntIdentityMixin, CreatedAtMixin, DbBase):
    """Records successful and failed authentication attempts with risk context."""

    __tablename__ = "auth_attempt"
    __table_args__ = (
        Index("ix_auth_attempt_account_created", "account_id", "created_at"),
        Index("ix_auth_attempt_ip_created", "ip_address", "created_at"),
        Index("ix_auth_attempt_identifier_created", "identifier_hmac", "created_at"),
        {"schema": "iam"},
    )

    account_id: Mapped[int | None] = mapped_column(ForeignKey("core.account.id", ondelete="SET NULL"))
    session_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("iam.auth_session.id", ondelete="SET NULL"))
    device_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("iam.device.id", ondelete="SET NULL"))
    identifier_hmac: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    ip_address: Mapped[str] = mapped_column(INET, nullable=False)
    client_family: Mapped[ClientFamily] = mapped_column(
        enum_type(ClientFamily, "auth_attempt_client_family", 16), nullable=False
    )
    client_version: Mapped[str | None] = mapped_column(String(64))
    result: Mapped[str] = mapped_column(String(32), nullable=False)
    failure_reason: Mapped[str | None] = mapped_column(String(64))
    country_code: Mapped[str | None] = mapped_column(String(2))
    asn: Mapped[int | None] = mapped_column(BigInteger)
    context: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False, default=dict, server_default="{}")
