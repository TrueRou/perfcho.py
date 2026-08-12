"""Persist the identity lifecycle in caller-owned transactions."""

import uuid
from datetime import datetime, timedelta
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
    AuthTokenFamily,
    AuthTokenScope,
    Device,
    DeviceIdentifier,
    OAuthClient,
    OAuthClientScope,
    OAuthClientSecret,
    PasswordCredential,
    Scope,
)
from perfcho.modules.identity.errors import SessionAlreadyActive
from perfcho.modules.identity.models import (
    AuthenticatedAccount,
    CredentialSnapshot,
    OAuthClientSnapshot,
    OpenClientSession,
    RefreshTokenSnapshot,
    ResolvedClientSession,
)

_ACTIVE_CLIENT_SESSION_CONSTRAINT = "uq_auth_sessions_active_client_account"


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

    async def find_oauth_client(
        self,
        client_key: str,
        secret_digest: bytes,
        *,
        at: datetime,
    ) -> OAuthClientSnapshot | None:
        """Resolve one active OAuth client and its configured scopes."""
        row = (
            await self._session.execute(
                select(OAuthClient.id, OAuthClient.client_key, OAuthClient.first_party)
                .join(OAuthClientSecret, OAuthClientSecret.client_id == OAuthClient.id)
                .where(
                    OAuthClient.client_key == client_key,
                    OAuthClient.active.is_(True),
                    OAuthClientSecret.secret_digest == secret_digest,
                    OAuthClientSecret.revoked_at.is_(None),
                    (OAuthClientSecret.expires_at.is_(None) | (OAuthClientSecret.expires_at > at)),
                )
                .limit(1)
            )
        ).one_or_none()
        if row is None:
            return None
        scope_ids = tuple(
            await self._session.scalars(
                select(OAuthClientScope.scope_id)
                .where(OAuthClientScope.client_id == row.id)
                .order_by(OAuthClientScope.scope_id)
            )
        )
        return OAuthClientSnapshot(row.id, row.client_key, row.first_party, scope_ids)

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

    async def acquire_session_lock(self, account_id: int) -> None:
        """Acquire the account's transaction-scoped client session lock."""
        await acquire_transaction_lock(self._session, "identity-client-session", account_id)

    async def find_open_client_session(self, account_id: int) -> OpenClientSession | None:
        """Return the unclosed direct client session, even when it has expired."""
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
                    AuthSession.oauth_client_id.is_(None),
                    AuthSession.session_class == "normal",
                    AuthSession.closed_at.is_(None),
                    AuthSession.revoked_at.is_(None),
                )
                .limit(1)
                .with_for_update()
            )
        ).one_or_none()
        return (
            OpenClientSession(row.id, row.created_at, row.last_activity_at, row.expires_at) if row is not None else None
        )

    async def find_online_credential_candidate(
        self,
        identifier_kind: str,
        identifier_key: str,
        *,
        at: datetime,
    ) -> tuple[CredentialSnapshot, OpenClientSession] | None:
        """Load credentials only for an active account with an open client session."""
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
                AuthSession.oauth_client_id.is_(None),
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
            raise RuntimeError("online credential candidate did not contain credentials")
        return snapshot, OpenClientSession(row.session_id, row.opened_at, row.last_activity_at, row.expires_at)

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

    async def create_client_session(
        self,
        *,
        session_id: uuid.UUID,
        token_id: uuid.UUID,
        token_jti: uuid.UUID,
        account_id: int,
        client_family: str,
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
        """Insert a direct client session and its HMAC-only token record."""
        await self._session.execute(
            update(Account)
            .where(Account.id == account_id)
            .values(
                first_client_login_at=func.coalesce(Account.first_client_login_at, now),
                last_seen_at=now,
            )
        )
        self._session.add_all(
            (
                AuthSession(
                    id=session_id,
                    account_id=account_id,
                    device_id=device_id,
                    client_family=ClientFamily(client_family),
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
                    kind=TokenKind.CLIENT_SESSION,
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
            if _integrity_constraint_name(error) == _ACTIVE_CLIENT_SESSION_CONSTRAINT or (
                _ACTIVE_CLIENT_SESSION_CONSTRAINT in str(error).lower()
            ):
                raise SessionAlreadyActive("a client session is already active") from error
            raise

    async def create_oauth_session(
        self,
        *,
        session_id: uuid.UUID,
        family_id: uuid.UUID,
        access_token_id: uuid.UUID,
        access_token_jti: uuid.UUID,
        refresh_token_id: uuid.UUID,
        refresh_token_jti: uuid.UUID,
        account_id: int,
        client_id: uuid.UUID,
        client_family: str,
        client_version: str | None,
        ip_address: str,
        user_agent: str | None,
        access_digest: bytes,
        access_prefix: str,
        access_expires_at: datetime,
        refresh_digest: bytes,
        refresh_prefix: str,
        refresh_expires_at: datetime,
        scope_ids: tuple[int, ...],
        now: datetime,
        session_expires_at: datetime,
    ) -> None:
        """Insert one OAuth session, refresh family, and initial token pair."""
        await self._session.execute(update(Account).where(Account.id == account_id).values(last_seen_at=now))
        session = AuthSession(
            id=session_id,
            account_id=account_id,
            oauth_client_id=client_id,
            client_family=ClientFamily(client_family),
            session_class="normal",
            client_version=client_version,
            ip_address=ip_address,
            user_agent=user_agent,
            expires_at=session_expires_at,
            last_activity_at=now,
            created_at=now,
        )
        family = AuthTokenFamily(
            id=family_id,
            session_id=session_id,
            account_id=account_id,
            created_at=now,
        )
        access = AuthToken(
            id=access_token_id,
            session_id=session_id,
            account_id=account_id,
            kind=TokenKind.ACCESS,
            digest=access_digest,
            prefix=access_prefix,
            jti=access_token_jti,
            expires_at=access_expires_at,
            created_at=now,
        )
        refresh = AuthToken(
            id=refresh_token_id,
            session_id=session_id,
            account_id=account_id,
            family_id=family_id,
            rotation_number=0,
            kind=TokenKind.REFRESH,
            digest=refresh_digest,
            prefix=refresh_prefix,
            jti=refresh_token_jti,
            expires_at=refresh_expires_at,
            created_at=now,
        )
        self._session.add_all((session, family))
        await self._session.flush()
        self._session.add_all((access, refresh))
        await self._session.flush()
        if scope_ids:
            self._session.add_all(
                [
                    AuthTokenScope(token_id=token_id, scope_id=scope_id)
                    for token_id in (access_token_id, refresh_token_id)
                    for scope_id in scope_ids
                ]
            )
        await self._session.flush()

    async def resolve_refresh_token(
        self,
        token_digest: bytes,
        *,
        client_id: uuid.UUID,
        at: datetime,
    ) -> RefreshTokenSnapshot | None:
        """Lock one refresh token and return its active lineage."""
        row = (
            await self._session.execute(
                select(
                    AuthToken.id,
                    AuthToken.family_id,
                    AuthToken.session_id,
                    AuthToken.account_id,
                    AuthSession.oauth_client_id,
                    AuthToken.rotation_number,
                    AuthSession.expires_at.label("session_expires_at"),
                    AuthToken.expires_at.label("token_expires_at"),
                    AuthToken.consumed_at,
                )
                .join(AuthSession, AuthSession.id == AuthToken.session_id)
                .join(AuthTokenFamily, AuthTokenFamily.id == AuthToken.family_id)
                .join(Account, Account.id == AuthToken.account_id)
                .where(
                    AuthToken.digest == token_digest,
                    AuthToken.kind == TokenKind.REFRESH,
                    AuthToken.revoked_at.is_(None),
                    AuthToken.expires_at > at,
                    AuthSession.oauth_client_id == client_id,
                    AuthSession.client_family == ClientFamily.LAZER,
                    AuthSession.closed_at.is_(None),
                    AuthSession.revoked_at.is_(None),
                    AuthSession.expires_at > at,
                    AuthTokenFamily.revoked_at.is_(None),
                    Account.status == AccountStatus.ACTIVE,
                    Account.deleted_at.is_(None),
                )
                .with_for_update(of=AuthToken)
            )
        ).one_or_none()
        if row is None or row.family_id is None or row.rotation_number is None or row.oauth_client_id is None:
            return None
        scope_ids = tuple(
            await self._session.scalars(
                select(AuthTokenScope.scope_id)
                .where(AuthTokenScope.token_id == row.id)
                .order_by(AuthTokenScope.scope_id)
            )
        )
        return RefreshTokenSnapshot(
            token_id=row.id,
            family_id=row.family_id,
            session_id=row.session_id,
            account_id=row.account_id,
            client_id=row.oauth_client_id,
            rotation_number=row.rotation_number,
            session_expires_at=row.session_expires_at,
            token_expires_at=row.token_expires_at,
            consumed_at=row.consumed_at,
            scope_ids=scope_ids,
        )

    async def rotate_refresh_token(
        self,
        snapshot: RefreshTokenSnapshot,
        *,
        access_token_id: uuid.UUID,
        access_token_jti: uuid.UUID,
        refresh_token_id: uuid.UUID,
        refresh_token_jti: uuid.UUID,
        access_digest: bytes,
        access_prefix: str,
        access_expires_at: datetime,
        refresh_digest: bytes,
        refresh_prefix: str,
        refresh_expires_at: datetime,
        now: datetime,
    ) -> None:
        """Consume the current refresh token and append its successor pair."""
        consumed = await self._session.scalar(
            update(AuthToken)
            .where(AuthToken.id == snapshot.token_id, AuthToken.consumed_at.is_(None))
            .values(consumed_at=now)
            .returning(AuthToken.id)
        )
        if consumed is None:
            raise RuntimeError("refresh token changed after it was locked")
        self._session.add_all(
            (
                AuthToken(
                    id=access_token_id,
                    session_id=snapshot.session_id,
                    account_id=snapshot.account_id,
                    kind=TokenKind.ACCESS,
                    digest=access_digest,
                    prefix=access_prefix,
                    jti=access_token_jti,
                    expires_at=access_expires_at,
                    created_at=now,
                ),
                AuthToken(
                    id=refresh_token_id,
                    session_id=snapshot.session_id,
                    account_id=snapshot.account_id,
                    family_id=snapshot.family_id,
                    parent_token_id=snapshot.token_id,
                    rotation_number=snapshot.rotation_number + 1,
                    kind=TokenKind.REFRESH,
                    digest=refresh_digest,
                    prefix=refresh_prefix,
                    jti=refresh_token_jti,
                    expires_at=refresh_expires_at,
                    created_at=now,
                ),
            )
        )
        if snapshot.scope_ids:
            self._session.add_all(
                [
                    AuthTokenScope(token_id=token_id, scope_id=scope_id)
                    for token_id in (access_token_id, refresh_token_id)
                    for scope_id in snapshot.scope_ids
                ]
            )
        await self._session.execute(
            update(AuthSession)
            .where(AuthSession.id == snapshot.session_id)
            .values(last_activity_at=func.greatest(AuthSession.last_activity_at, now))
        )
        await self._session.flush()

    async def compromise_token_family(self, family_id: uuid.UUID, *, now: datetime, reason: str) -> None:
        """Revoke a refresh family, all session tokens, and the owning session."""
        family = await self._session.scalar(
            select(AuthTokenFamily).where(AuthTokenFamily.id == family_id).with_for_update()
        )
        if family is None:
            return
        family.compromised_at = now
        family.revoked_at = now
        family.revoke_reason = reason
        await self._session.execute(
            update(AuthToken)
            .where(AuthToken.session_id == family.session_id, AuthToken.revoked_at.is_(None))
            .values(revoked_at=now)
        )
        await self._session.execute(
            update(AuthSession)
            .where(AuthSession.id == family.session_id, AuthSession.revoked_at.is_(None))
            .values(revoked_at=now, close_reason=reason)
        )
        await self._session.flush()

    async def resolve_access_token(self, token_digest: bytes, *, at: datetime) -> AuthenticatedAccount | None:
        """Resolve one active OAuth access token and its effective scopes."""
        current_name = aliased(AccountName)
        row = (
            await self._session.execute(
                select(
                    Account.id,
                    current_name.display_name,
                    Account.type,
                    Account.country_code,
                    Account.registered_at,
                    Account.last_seen_at,
                    AuthSession.id.label("session_id"),
                )
                .select_from(AuthToken)
                .join(AuthSession, AuthSession.id == AuthToken.session_id)
                .join(Account, Account.id == AuthToken.account_id)
                .join(current_name, current_name.account_id == Account.id)
                .where(
                    AuthToken.digest == token_digest,
                    AuthToken.kind == TokenKind.ACCESS,
                    AuthToken.consumed_at.is_(None),
                    AuthToken.revoked_at.is_(None),
                    AuthToken.expires_at > at,
                    AuthSession.client_family == ClientFamily.LAZER,
                    AuthSession.closed_at.is_(None),
                    AuthSession.revoked_at.is_(None),
                    AuthSession.expires_at > at,
                    Account.status == AccountStatus.ACTIVE,
                    Account.deleted_at.is_(None),
                    current_name.ended_at.is_(None),
                )
            )
        ).one_or_none()
        if row is None:
            return None
        scope_codes = tuple(
            await self._session.scalars(
                select(Scope.code)
                .join(AuthTokenScope, AuthTokenScope.scope_id == Scope.id)
                .join(AuthToken, AuthToken.id == AuthTokenScope.token_id)
                .where(AuthToken.digest == token_digest)
                .order_by(Scope.id)
            )
        )
        return AuthenticatedAccount(
            account_id=row.id,
            current_name=row.display_name,
            account_type=str(row.type),
            country_code=row.country_code,
            registered_at=row.registered_at,
            last_seen_at=row.last_seen_at,
            session_id=row.session_id,
            scope_codes=scope_codes,
        )

    async def append_auth_attempt(
        self,
        *,
        account_id: int | None,
        session_id: uuid.UUID | None,
        device_id: uuid.UUID | None,
        identifier_hmac: bytes,
        ip_address: str,
        client_version: str | None,
        client_family: str = "stable",
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
                client_family=ClientFamily(client_family),
                client_version=client_version,
                result=result,
                failure_reason=failure_reason,
                context=context,
                created_at=now,
            )
        )

    async def resolve_client_session(self, token_digest: bytes, *, at: datetime) -> ResolvedClientSession | None:
        """Resolve a fully active client token, session, account, and current name."""
        row = (await self._session.execute(_active_client_session_statement(token_digest, at=at))).one_or_none()
        return _resolved_client_session(row)

    async def touch_client_session(
        self,
        token_digest: bytes,
        *,
        at: datetime,
        minimum_interval: timedelta,
    ) -> tuple[ResolvedClientSession, bool] | None:
        """Resolve an active session and persist its heartbeat only when the interval elapsed."""
        if minimum_interval <= timedelta(0):
            raise ValueError("minimum_interval must be positive")
        statement = _active_client_session_statement(token_digest, at=at)
        row = (await self._session.execute(statement)).one_or_none()
        if row is None:
            return None

        if at - row.last_activity_at < minimum_interval:
            resolved = _resolved_client_session(row)
            return (resolved, False) if resolved is not None else None
        await self._session.execute(
            update(AuthSession)
            .where(
                AuthSession.id == row.session_id,
                AuthSession.closed_at.is_(None),
                AuthSession.revoked_at.is_(None),
                AuthSession.last_activity_at < at,
            )
            .values(last_activity_at=func.greatest(AuthSession.last_activity_at, at))
        )
        resolved = _resolved_client_session(row, last_activity_at=at)
        return (resolved, True) if resolved is not None else None

    async def get_client_session_account_id(self, session_id: uuid.UUID) -> int | None:
        """Return the owning account ID for a direct client session."""
        return await self._session.scalar(
            select(AuthSession.account_id).where(
                AuthSession.id == session_id,
                AuthSession.oauth_client_id.is_(None),
                AuthSession.session_class == "normal",
            )
        )

    async def close_client_session(
        self,
        session_id: uuid.UUID,
        *,
        now: datetime,
        reason: str,
        revoke: bool,
    ) -> int | None:
        """Close or revoke one open client session and invalidate all bearer tokens."""
        auth_session = await self._session.scalar(
            select(AuthSession)
            .where(
                AuthSession.id == session_id,
                AuthSession.oauth_client_id.is_(None),
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


def _active_client_session_statement(token_digest: bytes, *, at: datetime) -> Select:
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
            AuthToken.kind == TokenKind.CLIENT_SESSION,
            AuthToken.consumed_at.is_(None),
            AuthToken.revoked_at.is_(None),
            AuthToken.expires_at > at,
            AuthSession.oauth_client_id.is_(None),
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


def _resolved_client_session(
    row: object | None,
    *,
    last_activity_at: datetime | None = None,
) -> ResolvedClientSession | None:
    if row is None:
        return None
    resolved_row = cast(_ResolvedSessionRow, row)
    return ResolvedClientSession(
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
