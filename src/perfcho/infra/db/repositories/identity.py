"""Persist the Stable identity lifecycle in caller-owned transactions."""

import uuid
from datetime import datetime
from typing import Protocol, cast

from sqlalchemy import Select, func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from perfcho.infra.db.enums import AccountStatus, ClientFamily, TokenKind
from perfcho.infra.db.locks import acquire_transaction_lock
from perfcho.infra.db.models.core import Account, AccountEmail, AccountName
from perfcho.infra.db.models.iam import (
    AccountDevice,
    AuthAttempt,
    AuthSession,
    AuthToken,
    Device,
    DeviceIdentifier,
    PasswordCredential,
)
from perfcho.modules.identity.errors import StableSessionAlreadyActive
from perfcho.modules.identity.models import CredentialSnapshot, OpenStableSession, ResolvedStableSession

_ACTIVE_STABLE_SESSION_CONSTRAINT = "uq_auth_sessions_active_normal_stable_account"


class _ResolvedSessionRow(Protocol):
    account_id: int
    current_name: str
    auth_version: int
    country_code: str | None
    session_id: uuid.UUID
    device_id: uuid.UUID | None
    client_version: str | None
    client_variant: str | None
    opened_at: datetime
    last_activity_at: datetime
    expires_at: datetime


class SqlAlchemyIdentityRepository:
    """Query and mutate canonical identity facts through an AsyncSession."""

    def __init__(self, session: AsyncSession) -> None:
        """Bind all operations to the caller-owned session."""
        self._session = session

    async def find_credential(self, identifier_kind: str, identifier_key: str) -> CredentialSnapshot | None:
        """Look up credentials by numeric ID, current normalized name, or active normalized email."""
        current_name = aliased(AccountName)
        statement = (
            select(
                Account.id,
                current_name.display_name,
                Account.status,
                Account.auth_version,
                Account.country_code,
                PasswordCredential.verifier,
                PasswordCredential.algorithm,
                PasswordCredential.pepper_version,
                PasswordCredential.password_changed_at,
                PasswordCredential.must_change,
            )
            .join(current_name, current_name.account_id == Account.id)
            .join(PasswordCredential, PasswordCredential.account_id == Account.id)
            .where(current_name.ended_at.is_(None))
        )
        if identifier_kind == "id":
            statement = statement.where(Account.id == int(identifier_key))
        elif identifier_kind == "name":
            statement = statement.where(current_name.name_key == identifier_key)
        elif identifier_kind == "email":
            statement = statement.join(AccountEmail, AccountEmail.account_id == Account.id).where(
                AccountEmail.email_key == identifier_key,
                AccountEmail.retired_at.is_(None),
            )
        else:
            raise ValueError(f"unsupported identity identifier kind: {identifier_kind}")
        return _credential_snapshot((await self._session.execute(statement.limit(1))).one_or_none())

    async def get_current_credential(self, account_id: int) -> CredentialSnapshot | None:
        """Re-read current account, auth version, name, and password facts by account ID."""
        return await self.find_credential("id", str(account_id))

    async def upgrade_legacy_credential(
        self,
        *,
        account_id: int,
        expected_verifier: str,
        expected_password_changed_at: datetime,
        password_verifier: str,
        pepper_version: int,
        password_changed_at: datetime,
    ) -> bool:
        """Replace only the exact legacy row observed and verified by the caller."""
        upgraded_account_id = await self._session.scalar(
            update(PasswordCredential)
            .where(
                PasswordCredential.account_id == account_id,
                PasswordCredential.algorithm == "bcrypt_md5",
                PasswordCredential.pepper_version.is_(None),
                PasswordCredential.verifier == expected_verifier,
                PasswordCredential.password_changed_at == expected_password_changed_at,
            )
            .values(
                verifier=password_verifier,
                algorithm="argon2id",
                pepper_version=pepper_version,
                password_changed_at=password_changed_at,
                updated_at=password_changed_at,
            )
            .returning(PasswordCredential.account_id)
        )
        return upgraded_account_id is not None

    async def acquire_stable_session_lock(self, account_id: int) -> None:
        """Acquire the account's transaction-scoped Stable session lock."""
        await acquire_transaction_lock(self._session, "identity-stable-session", account_id)

    async def find_open_stable_session(self, account_id: int) -> OpenStableSession | None:
        """Return the one unclosed normal Stable session, even when it has expired."""
        row = (
            await self._session.execute(
                select(
                    AuthSession.id,
                    AuthSession.created_at,
                    AuthSession.last_activity_at,
                    AuthSession.expires_at,
                )
                .where(
                    AuthSession.account_id == account_id,
                    AuthSession.client_family == ClientFamily.STABLE,
                    AuthSession.session_class == "normal",
                    AuthSession.closed_at.is_(None),
                    AuthSession.revoked_at.is_(None),
                )
                .limit(1)
                .with_for_update()
            )
        ).one_or_none()
        return (
            OpenStableSession(row.id, row.created_at, row.last_activity_at, row.expires_at) if row is not None else None
        )

    async def find_stable_web_candidate(
        self,
        identifier_kind: str,
        identifier_key: str,
        *,
        at: datetime,
    ) -> tuple[CredentialSnapshot, OpenStableSession] | None:
        """Load credentials only for an active account with an open Stable session."""
        current_name = aliased(AccountName)
        statement = (
            select(
                Account.id,
                current_name.display_name,
                Account.status,
                Account.auth_version,
                Account.country_code,
                PasswordCredential.verifier,
                PasswordCredential.algorithm,
                PasswordCredential.pepper_version,
                PasswordCredential.password_changed_at,
                PasswordCredential.must_change,
                AuthSession.id.label("session_id"),
                AuthSession.created_at.label("opened_at"),
                AuthSession.last_activity_at,
                AuthSession.expires_at,
            )
            .join(current_name, current_name.account_id == Account.id)
            .join(PasswordCredential, PasswordCredential.account_id == Account.id)
            .join(AuthSession, AuthSession.account_id == Account.id)
            .where(
                Account.status == AccountStatus.ACTIVE,
                Account.deleted_at.is_(None),
                current_name.ended_at.is_(None),
                PasswordCredential.must_change.is_(False),
                AuthSession.client_family == ClientFamily.STABLE,
                AuthSession.session_class == "normal",
                AuthSession.closed_at.is_(None),
                AuthSession.revoked_at.is_(None),
                AuthSession.expires_at > at,
            )
        )
        if identifier_kind == "id":
            statement = statement.where(Account.id == int(identifier_key))
        elif identifier_kind == "name":
            statement = statement.where(current_name.name_key == identifier_key)
        elif identifier_kind == "email":
            statement = statement.join(AccountEmail, AccountEmail.account_id == Account.id).where(
                AccountEmail.email_key == identifier_key,
                AccountEmail.retired_at.is_(None),
            )
        else:
            raise ValueError(f"unsupported identity identifier kind: {identifier_kind}")

        row = (await self._session.execute(statement.limit(1))).one_or_none()
        if row is None:
            return None
        snapshot = _credential_snapshot(row[:10])
        if snapshot is None:
            raise RuntimeError("Stable web candidate did not contain credentials")
        return snapshot, OpenStableSession(row.session_id, row.opened_at, row.last_activity_at, row.expires_at)

    async def get_or_create_device(
        self,
        *,
        proposed_device_id: uuid.UUID,
        fingerprint_hmac: bytes,
        component_hmacs: tuple[tuple[str, bytes], ...],
        account_id: int,
        platform: str | None,
        now: datetime,
    ) -> uuid.UUID:
        """Atomically upsert a device, HMAC components, and its account relationship."""
        device_statement = (
            insert(Device)
            .values(
                id=proposed_device_id,
                fingerprint_hmac=fingerprint_hmac,
                platform=platform,
                first_seen_at=now,
                last_seen_at=now,
                risk_level=0,
            )
            .on_conflict_do_update(
                index_elements=(Device.fingerprint_hmac,),
                set_={
                    "last_seen_at": now,
                    "platform": func.coalesce(Device.platform, platform),
                },
            )
            .returning(Device.id)
        )
        device_id = await self._session.scalar(device_statement)
        if device_id is None:
            raise RuntimeError("database did not return a device identifier")

        if component_hmacs:
            await self._session.execute(
                insert(DeviceIdentifier)
                .values(
                    [
                        {"device_id": device_id, "kind": kind, "value_hmac": digest, "quality": 0}
                        for kind, digest in component_hmacs
                    ]
                )
                .on_conflict_do_nothing(
                    index_elements=(
                        DeviceIdentifier.device_id,
                        DeviceIdentifier.kind,
                        DeviceIdentifier.value_hmac,
                    )
                )
            )

        await self._session.execute(
            insert(AccountDevice)
            .values(
                account_id=account_id,
                device_id=device_id,
                first_used_at=now,
                last_used_at=now,
                use_count=1,
            )
            .on_conflict_do_update(
                index_elements=(AccountDevice.account_id, AccountDevice.device_id),
                set_={
                    "last_used_at": now,
                    "use_count": AccountDevice.use_count + 1,
                },
            )
        )
        return device_id

    async def create_stable_session(
        self,
        *,
        session_id: uuid.UUID,
        token_id: uuid.UUID,
        token_jti: uuid.UUID,
        account_id: int,
        device_id: uuid.UUID,
        client_version: str,
        client_variant: str | None,
        ip_address: str,
        user_agent: str | None,
        token_digest: bytes,
        token_prefix: str,
        now: datetime,
        expires_at: datetime,
    ) -> None:
        """Insert the normal Stable session and its HMAC-only token record."""
        await self._session.execute(
            update(Account)
            .where(Account.id == account_id)
            .values(
                first_stable_login_at=func.coalesce(Account.first_stable_login_at, now),
                last_seen_at=now,
            )
        )
        self._session.add_all(
            (
                AuthSession(
                    id=session_id,
                    account_id=account_id,
                    device_id=device_id,
                    client_family=ClientFamily.STABLE,
                    client_variant=client_variant,
                    session_class="normal",
                    client_version=client_version,
                    ip_address=ip_address,
                    user_agent=user_agent,
                    expires_at=expires_at,
                    last_activity_at=now,
                    created_at=now,
                ),
                AuthToken(
                    id=token_id,
                    session_id=session_id,
                    account_id=account_id,
                    kind=TokenKind.STABLE_SESSION,
                    digest=token_digest,
                    prefix=token_prefix,
                    jti=token_jti,
                    expires_at=expires_at,
                    created_at=now,
                ),
            )
        )
        try:
            await self._session.flush()
        except IntegrityError as error:
            if _integrity_constraint_name(error) == _ACTIVE_STABLE_SESSION_CONSTRAINT or (
                _ACTIVE_STABLE_SESSION_CONSTRAINT in str(error).lower()
            ):
                raise StableSessionAlreadyActive("a normal Stable session is already active") from error
            raise

    async def append_auth_attempt(
        self,
        *,
        account_id: int | None,
        session_id: uuid.UUID | None,
        device_id: uuid.UUID | None,
        identifier_hmac: bytes,
        ip_address: str,
        client_version: str | None,
        result: str,
        failure_reason: str | None,
        context: dict[str, object],
        now: datetime,
    ) -> None:
        """Append one successful or failed authentication attempt."""
        self._session.add(
            AuthAttempt(
                account_id=account_id,
                session_id=session_id,
                device_id=device_id,
                identifier_hmac=identifier_hmac,
                ip_address=ip_address,
                client_family=ClientFamily.STABLE,
                client_version=client_version,
                result=result,
                failure_reason=failure_reason,
                context=context,
                created_at=now,
            )
        )

    async def resolve_stable_session(self, token_digest: bytes, *, at: datetime) -> ResolvedStableSession | None:
        """Resolve only a fully active Stable token, session, account, and current name."""
        row = (await self._session.execute(_active_stable_session_statement(token_digest, at=at))).one_or_none()
        return _resolved_stable_session(row)

    async def touch_stable_session(self, token_digest: bytes, *, at: datetime) -> ResolvedStableSession | None:
        """Resolve and monotonically touch an active Stable session while locking its row."""
        statement = _active_stable_session_statement(token_digest, at=at).with_for_update(of=AuthSession)
        row = (await self._session.execute(statement)).one_or_none()
        if row is None:
            return None

        last_activity_at = max(row.last_activity_at, at)
        if last_activity_at != row.last_activity_at:
            await self._session.execute(
                update(AuthSession)
                .where(
                    AuthSession.id == row.session_id,
                    AuthSession.closed_at.is_(None),
                    AuthSession.revoked_at.is_(None),
                )
                .values(last_activity_at=last_activity_at)
            )
        return _resolved_stable_session(row, last_activity_at=last_activity_at)

    async def get_stable_session_account_id(self, session_id: uuid.UUID) -> int | None:
        """Return the owning account ID for a normal Stable session."""
        return await self._session.scalar(
            select(AuthSession.account_id).where(
                AuthSession.id == session_id,
                AuthSession.client_family == ClientFamily.STABLE,
                AuthSession.session_class == "normal",
            )
        )

    async def close_stable_session(
        self,
        session_id: uuid.UUID,
        *,
        now: datetime,
        reason: str,
        revoke: bool,
    ) -> int | None:
        """Close or revoke one open Stable session and invalidate all bearer tokens."""
        auth_session = await self._session.scalar(
            select(AuthSession)
            .where(
                AuthSession.id == session_id,
                AuthSession.client_family == ClientFamily.STABLE,
                AuthSession.session_class == "normal",
            )
            .with_for_update()
        )
        if auth_session is None or auth_session.closed_at is not None or auth_session.revoked_at is not None:
            return None

        if revoke:
            auth_session.revoked_at = now
        else:
            auth_session.closed_at = now
        auth_session.close_reason = reason
        await self._session.execute(
            update(AuthToken)
            .where(AuthToken.session_id == session_id, AuthToken.revoked_at.is_(None))
            .values(revoked_at=now)
        )
        if revoke:
            await self._session.execute(
                update(Account)
                .where(Account.id == auth_session.account_id)
                .values(auth_version=Account.auth_version + 1)
            )
        await self._session.flush()
        return auth_session.account_id


def _credential_snapshot(row: object | None) -> CredentialSnapshot | None:
    if row is None:
        return None
    (
        account_id,
        current_name,
        account_status,
        auth_version,
        country_code,
        password_verifier,
        algorithm,
        pepper_version,
        password_changed_at,
        must_change,
    ) = cast(tuple[int, str, AccountStatus, int, str | None, str, str, int | None, datetime, bool], row)
    return CredentialSnapshot(
        account_id=account_id,
        current_name=current_name,
        account_status=str(account_status),
        auth_version=auth_version,
        password_verifier=password_verifier,
        algorithm=algorithm,
        pepper_version=pepper_version,
        password_changed_at=password_changed_at,
        must_change=must_change,
        country_code=country_code,
    )


def _active_stable_session_statement(token_digest: bytes, *, at: datetime) -> Select:
    current_name = aliased(AccountName)
    return (
        select(
            Account.id.label("account_id"),
            current_name.display_name.label("current_name"),
            Account.auth_version,
            Account.country_code,
            AuthSession.id.label("session_id"),
            AuthSession.device_id,
            AuthSession.client_version,
            AuthSession.client_variant,
            AuthSession.created_at.label("opened_at"),
            AuthSession.last_activity_at,
            AuthSession.expires_at,
        )
        .select_from(AuthToken)
        .join(AuthSession, AuthSession.id == AuthToken.session_id)
        .join(Account, Account.id == AuthSession.account_id)
        .join(current_name, current_name.account_id == Account.id)
        .where(
            AuthToken.digest == token_digest,
            AuthToken.kind == TokenKind.STABLE_SESSION,
            AuthToken.consumed_at.is_(None),
            AuthToken.revoked_at.is_(None),
            AuthToken.expires_at > at,
            AuthSession.client_family == ClientFamily.STABLE,
            AuthSession.session_class == "normal",
            AuthSession.closed_at.is_(None),
            AuthSession.revoked_at.is_(None),
            AuthSession.expires_at > at,
            Account.status == AccountStatus.ACTIVE,
            Account.deleted_at.is_(None),
            current_name.ended_at.is_(None),
        )
        .limit(1)
    )


def _resolved_stable_session(
    row: object | None,
    *,
    last_activity_at: datetime | None = None,
) -> ResolvedStableSession | None:
    if row is None:
        return None
    resolved_row = cast(_ResolvedSessionRow, row)
    return ResolvedStableSession(
        account_id=resolved_row.account_id,
        current_name=resolved_row.current_name,
        auth_version=resolved_row.auth_version,
        session_id=resolved_row.session_id,
        device_id=resolved_row.device_id,
        client_version=resolved_row.client_version,
        client_variant=resolved_row.client_variant,
        expires_at=resolved_row.expires_at,
        country_code=resolved_row.country_code,
        opened_at=resolved_row.opened_at,
        last_activity_at=last_activity_at if last_activity_at is not None else resolved_row.last_activity_at,
    )


def _integrity_constraint_name(error: IntegrityError) -> str | None:
    candidates = (error.orig, getattr(error.orig, "__cause__", None))
    for candidate in candidates:
        if candidate is None:
            continue
        constraint_name = getattr(candidate, "constraint_name", None)
        if isinstance(constraint_name, str):
            return constraint_name
        diagnostic = getattr(candidate, "diag", None)
        constraint_name = getattr(diagnostic, "constraint_name", None)
        if isinstance(constraint_name, str):
            return constraint_name
    return None
