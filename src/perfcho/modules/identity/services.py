"""Provide the protocol-neutral identity lifecycle."""

import asyncio
import time
import unicodedata
import uuid
from collections.abc import Callable
from datetime import datetime, timedelta

from perfcho.infra.logging import duration_ms, log_event
from perfcho.infra.security.password import (
    Argon2Policy,
    PasswordHash,
    PasswordPepper,
    PasswordVerification,
    PasswordVerificationStatus,
    hash_password,
    validate_password_preverification,
    verify_dummy_password,
    verify_legacy_bcrypt_md5,
    verify_password,
)
from perfcho.infra.security.tokens import (
    digest_device_component,
    digest_opaque_token,
    generate_urlsafe_token,
    hmac_sha256_digest,
)
from perfcho.modules.common.models import PendingEvent
from perfcho.modules.common.normalization import normalize_email, normalize_name
from perfcho.modules.common.ports import Clock, IdGenerator, OutboxWriterFactory
from perfcho.modules.identity.errors import (
    InvalidAccessToken,
    InvalidCredentials,
    InvalidOAuthClient,
    InvalidOAuthGrant,
    InvalidSession,
    SessionAlreadyActive,
)
from perfcho.modules.identity.models import (
    AuthenticateClientSession,
    AuthenticatedAccount,
    ClientSessionResult,
    CredentialSnapshot,
    OAuthClientSnapshot,
    OAuthTokenResult,
    OnlineCredentialPrincipal,
    OpenClientSession,
    PasswordGrant,
    RefreshGrant,
    ResolvedClientSession,
)
from perfcho.modules.identity.ports import (
    IdentityRepository,
    IdentityRepositoryFactory,
    IdentityUnitOfWork,
    OnlineCredentialVerificationCache,
)

_IDENTITY_CONSUMERS = ("identity-projector.v1",)
_ACCOUNT_ID_MAX = 2_147_483_647
_TOKEN_PREFIX_LENGTH = 16


class IdentityService:
    """Authenticate, resolve, close, and revoke identity sessions."""

    def __init__(
        self,
        uow_factory: Callable[[], IdentityUnitOfWork],
        repository_factory: IdentityRepositoryFactory,
        outbox_writer_factory: OutboxWriterFactory,
        password_pepper: PasswordPepper,
        argon2_policy: Argon2Policy,
        token_hmac_key: bytes,
        device_hmac_key: bytes,
        clock: Clock,
        id_generator: IdGenerator,
        *,
        client_session_stale_grace: timedelta,
        client_session_touch_interval: timedelta = timedelta(seconds=30),
        online_credential_verification_cache: OnlineCredentialVerificationCache | None = None,
        token_factory: Callable[[], str] = generate_urlsafe_token,
    ) -> None:
        """Bind explicit transaction, security, event, time, and ID dependencies."""
        if not token_hmac_key or not device_hmac_key:
            raise ValueError("identity HMAC keys must not be empty")
        if client_session_stale_grace <= timedelta(0):
            raise ValueError("client session stale grace must be positive")
        if not timedelta(0) < client_session_touch_interval < client_session_stale_grace:
            raise ValueError("client session touch interval must be positive and shorter than stale grace")
        self._uow_factory = uow_factory
        self._repository_factory = repository_factory
        self._outbox_writer_factory = outbox_writer_factory
        self._password_pepper = password_pepper
        self._argon2_policy = argon2_policy
        self._token_hmac_key = token_hmac_key
        self._device_hmac_key = device_hmac_key
        self._clock = clock
        self._id_generator = id_generator
        self._client_session_stale_grace = client_session_stale_grace
        self._client_session_touch_interval = client_session_touch_interval
        self._online_credential_verification_cache = online_credential_verification_cache
        self._token_factory = token_factory

    async def authenticate_client_session(self, command: AuthenticateClientSession) -> ClientSessionResult:
        """Verify outside a write transaction, then atomically open a client session."""
        started_ns = time.monotonic_ns()
        normalized_identifier = _normalize_identifier(command.identifier)
        identifier_hmac = _identifier_hmac(normalized_identifier, command.identifier, key=self._device_hmac_key)
        snapshot: CredentialSnapshot | None = None

        if normalized_identifier is not None:
            async with self._uow_factory() as uow:
                repository = self._repository_factory(uow.session)
                snapshot = await repository.find_credential(*normalized_identifier)

        if snapshot is None or snapshot.account_status != "active" or snapshot.must_change:
            await self._record_failed_login(command, identifier_hmac, snapshot, "invalid_credentials")
            raise InvalidCredentials("invalid credentials")

        verification = await asyncio.to_thread(
            _verify_credential,
            command.password_preverification,
            snapshot,
            pepper=self._password_pepper,
            policy=self._argon2_policy,
        )
        if not verification.verified:
            await self._record_failed_login(command, identifier_hmac, snapshot, "invalid_credentials")
            raise InvalidCredentials("invalid credentials")

        replacement_hash: PasswordHash | None = None
        if snapshot.algorithm == "bcrypt_md5":
            replacement_hash = await asyncio.to_thread(
                hash_password,
                command.password_preverification,
                pepper=self._password_pepper,
                policy=self._argon2_policy,
            )

        component_hmacs, fingerprint_hmac = _digest_device_components(
            command.device_components,
            key=self._device_hmac_key,
        )
        now = self._clock.now()
        expires_at = now + command.session_lifetime
        raw_token = self._token_factory()
        if len(raw_token) <= _TOKEN_PREFIX_LENGTH:
            raise RuntimeError("token factory returned a token too short for a non-secret prefix")
        token_digest = digest_opaque_token(raw_token, key=self._token_hmac_key)
        session_id = self._id_generator.new()
        proposed_device_id = self._id_generator.new()
        token_id = self._id_generator.new()
        token_jti = self._id_generator.new()
        result: ClientSessionResult | None = None
        failure: InvalidCredentials | SessionAlreadyActive | None = None
        replaced_session: tuple[int, uuid.UUID] | None = None

        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            await repository.acquire_session_lock(snapshot.account_id)
            current = await repository.get_current_credential(snapshot.account_id)
            if not _credential_is_current(snapshot, current):
                await _append_attempt(
                    repository,
                    command,
                    identifier_hmac,
                    account_id=snapshot.account_id,
                    result="failure",
                    failure_reason="invalid_credentials",
                    now=now,
                )
                await uow.commit()
                failure = InvalidCredentials("invalid credentials")
            else:
                open_session = await repository.find_open_client_session(snapshot.account_id)
                if (
                    open_session is not None
                    and open_session.expires_at > now
                    and not _session_is_stale(
                        open_session.last_activity_at, at=now, grace=self._client_session_stale_grace
                    )
                ):
                    await _append_attempt(
                        repository,
                        command,
                        identifier_hmac,
                        account_id=snapshot.account_id,
                        result="failure",
                        failure_reason="active_session",
                        now=now,
                    )
                    await uow.commit()
                    failure = SessionAlreadyActive("a client session is already active")
                else:
                    upgraded = replacement_hash is None or await repository.upgrade_legacy_credential(
                        account_id=snapshot.account_id,
                        expected_verifier=snapshot.password_verifier,
                        expected_password_changed_at=snapshot.password_changed_at,
                        password_verifier=replacement_hash.verifier,
                        pepper_version=replacement_hash.pepper_version,
                        password_changed_at=now,
                    )
                    if not upgraded:
                        await _append_attempt(
                            repository,
                            command,
                            identifier_hmac,
                            account_id=snapshot.account_id,
                            result="failure",
                            failure_reason="invalid_credentials",
                            now=now,
                        )
                        await uow.commit()
                        failure = InvalidCredentials("invalid credentials")
                    else:
                        outbox = self._outbox_writer_factory(uow.session)
                        if open_session is not None:
                            close_reason = "expired" if open_session.expires_at <= now else "stale"
                            closed_account_id = await repository.close_client_session(
                                open_session.session_id,
                                now=now,
                                reason=close_reason,
                                revoke=False,
                            )
                            if closed_account_id is not None:
                                replaced_session = (closed_account_id, open_session.session_id)
                                await outbox.append(
                                    _session_closed_event(
                                        closed_account_id,
                                        open_session.session_id,
                                        now=now,
                                        reason=close_reason,
                                        revoked=False,
                                    )
                                )

                        assert current is not None
                        device_id = await repository.get_or_create_device(
                            proposed_device_id=proposed_device_id,
                            fingerprint_hmac=fingerprint_hmac,
                            component_hmacs=component_hmacs,
                            account_id=snapshot.account_id,
                            platform=None,
                            now=now,
                        )
                        await repository.create_client_session(
                            session_id=session_id,
                            token_id=token_id,
                            token_jti=token_jti,
                            account_id=snapshot.account_id,
                            client_family=command.meta.client.family,
                            device_id=device_id,
                            client_version=command.client_version,
                            client_variant=command.client_variant,
                            ip_address=command.ip_address,
                            user_agent=command.user_agent,
                            token_digest=token_digest,
                            token_prefix=raw_token[:_TOKEN_PREFIX_LENGTH],
                            now=now,
                            expires_at=expires_at,
                        )
                        await _append_attempt(
                            repository,
                            command,
                            identifier_hmac,
                            account_id=snapshot.account_id,
                            session_id=session_id,
                            device_id=device_id,
                            result="success",
                            failure_reason=None,
                            now=now,
                        )
                        await outbox.append(
                            _session_opened_event(
                                command,
                                account_id=snapshot.account_id,
                                session_id=session_id,
                                device_id=device_id,
                                opened_at=now,
                                expires_at=expires_at,
                            )
                        )
                        await uow.commit()
                        if replaced_session is not None:
                            log_event(
                                "INFO",
                                "identity.client_session.closed",
                                account_id=replaced_session[0],
                                session_id=str(replaced_session[1]),
                                duration_ms=duration_ms(started_ns),
                            )
                        log_event(
                            "INFO",
                            "identity.client_session.opened",
                            account_id=snapshot.account_id,
                            session_id=str(session_id),
                            device_id=str(device_id),
                            credential_upgraded=replacement_hash is not None,
                            duration_ms=duration_ms(started_ns),
                        )
                        result = ClientSessionResult(
                            account_id=snapshot.account_id,
                            current_name=current.current_name,
                            session_id=session_id,
                            device_id=device_id,
                            raw_token=raw_token,
                            expires_at=expires_at,
                            country_code=current.country_code,
                        )

        if failure is not None:
            raise failure
        if result is None:
            raise RuntimeError("client session authentication completed without a result")
        return result

    async def exchange_password(self, command: PasswordGrant) -> OAuthTokenResult:
        """Authenticate an account and atomically create an OAuth token family."""
        client = await self._authenticate_oauth_client(command.client_key, command.client_secret)
        normalized_identifier = _normalize_identifier(command.identifier)
        identifier_hmac = _identifier_hmac(normalized_identifier, command.identifier, key=self._device_hmac_key)
        snapshot: CredentialSnapshot | None = None
        if normalized_identifier is not None:
            async with self._uow_factory() as uow:
                snapshot = await self._repository_factory(uow.session).find_credential(*normalized_identifier)

        if snapshot is None or snapshot.account_status != "active" or snapshot.must_change:
            await asyncio.to_thread(
                verify_dummy_password,
                pepper=self._password_pepper,
                policy=self._argon2_policy,
            )
            await self._record_oauth_attempt(command, identifier_hmac, snapshot, "invalid_credentials")
            raise InvalidOAuthGrant("invalid credentials")

        verification = await asyncio.to_thread(
            _verify_credential,
            command.password_preverification,
            snapshot,
            pepper=self._password_pepper,
            policy=self._argon2_policy,
        )
        if not verification.verified:
            await self._record_oauth_attempt(command, identifier_hmac, snapshot, "invalid_credentials")
            raise InvalidOAuthGrant("invalid credentials")

        replacement_hash: PasswordHash | None = None
        if snapshot.algorithm == "bcrypt_md5" or verification.needs_rehash:
            replacement_hash = await asyncio.to_thread(
                hash_password,
                command.password_preverification,
                pepper=self._password_pepper,
                policy=self._argon2_policy,
            )

        now = self._clock.now()
        session_expires_at = now + command.session_lifetime
        access_expires_at = min(now + command.access_token_lifetime, session_expires_at)
        refresh_expires_at = min(now + command.refresh_token_lifetime, session_expires_at)
        access_token = self._token_factory()
        refresh_token = self._token_factory()
        _validate_generated_token(access_token)
        _validate_generated_token(refresh_token)
        session_id, family_id = self._id_generator.new(), self._id_generator.new()
        access_token_id, access_token_jti = self._id_generator.new(), self._id_generator.new()
        refresh_token_id, refresh_token_jti = self._id_generator.new(), self._id_generator.new()

        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            current = await repository.get_current_credential(snapshot.account_id)
            if not _credential_is_current(snapshot, current):
                await repository.append_auth_attempt(
                    account_id=snapshot.account_id,
                    session_id=None,
                    device_id=None,
                    identifier_hmac=identifier_hmac,
                    ip_address=command.ip_address,
                    client_version=command.client_version,
                    client_family=command.client_family,
                    result="failure",
                    failure_reason="invalid_credentials",
                    context={"user_agent": command.user_agent, "oauth_client": command.client_key},
                    now=now,
                )
                await uow.commit()
                raise InvalidOAuthGrant("invalid credentials")

            if replacement_hash is not None:
                upgraded = await repository.upgrade_legacy_credential(
                    account_id=snapshot.account_id,
                    expected_verifier=snapshot.password_verifier,
                    expected_password_changed_at=snapshot.password_changed_at,
                    password_verifier=replacement_hash.verifier,
                    pepper_version=replacement_hash.pepper_version,
                    password_changed_at=now,
                )
                if not upgraded:
                    raise InvalidOAuthGrant("invalid credentials")

            await repository.create_oauth_session(
                session_id=session_id,
                family_id=family_id,
                access_token_id=access_token_id,
                access_token_jti=access_token_jti,
                refresh_token_id=refresh_token_id,
                refresh_token_jti=refresh_token_jti,
                account_id=snapshot.account_id,
                client_id=client.client_id,
                client_family=command.client_family,
                client_version=command.client_version,
                ip_address=command.ip_address,
                user_agent=command.user_agent,
                access_digest=digest_opaque_token(access_token, key=self._token_hmac_key),
                access_prefix=access_token[:_TOKEN_PREFIX_LENGTH],
                access_expires_at=access_expires_at,
                refresh_digest=digest_opaque_token(refresh_token, key=self._token_hmac_key),
                refresh_prefix=refresh_token[:_TOKEN_PREFIX_LENGTH],
                refresh_expires_at=refresh_expires_at,
                scope_ids=client.scope_ids,
                now=now,
                session_expires_at=session_expires_at,
            )
            await repository.append_auth_attempt(
                account_id=snapshot.account_id,
                session_id=session_id,
                device_id=None,
                identifier_hmac=identifier_hmac,
                ip_address=command.ip_address,
                client_version=command.client_version,
                client_family=command.client_family,
                result="success",
                failure_reason=None,
                context={"user_agent": command.user_agent, "oauth_client": command.client_key},
                now=now,
            )
            await self._outbox_writer_factory(uow.session).append(
                _oauth_session_opened_event(
                    account_id=snapshot.account_id,
                    session_id=session_id,
                    client_family=command.client_family,
                    client_version=command.client_version,
                    opened_at=now,
                    expires_at=session_expires_at,
                )
            )
            await uow.commit()

        return OAuthTokenResult(
            access_token=access_token,
            refresh_token=refresh_token,
            expires_in=int((access_expires_at - now).total_seconds()),
        )

    async def exchange_refresh(self, command: RefreshGrant) -> OAuthTokenResult:
        """Rotate an OAuth refresh token and revoke its family on replay."""
        client = await self._authenticate_oauth_client(command.client_key, command.client_secret)
        now = self._clock.now()
        access_token, refresh_token = self._token_factory(), self._token_factory()
        _validate_generated_token(access_token)
        _validate_generated_token(refresh_token)
        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            snapshot = await repository.resolve_refresh_token(
                digest_opaque_token(command.refresh_token, key=self._token_hmac_key),
                client_id=client.client_id,
                at=now,
            )
            if snapshot is None:
                raise InvalidOAuthGrant("invalid refresh token")
            if snapshot.consumed_at is not None:
                await repository.compromise_token_family(snapshot.family_id, now=now, reason="refresh_reuse")
                await uow.commit()
                raise InvalidOAuthGrant("invalid refresh token")

            access_expires_at = min(now + command.access_token_lifetime, snapshot.session_expires_at)
            refresh_expires_at = min(now + command.refresh_token_lifetime, snapshot.session_expires_at)
            await repository.rotate_refresh_token(
                snapshot,
                access_token_id=self._id_generator.new(),
                access_token_jti=self._id_generator.new(),
                refresh_token_id=self._id_generator.new(),
                refresh_token_jti=self._id_generator.new(),
                access_digest=digest_opaque_token(access_token, key=self._token_hmac_key),
                access_prefix=access_token[:_TOKEN_PREFIX_LENGTH],
                access_expires_at=access_expires_at,
                refresh_digest=digest_opaque_token(refresh_token, key=self._token_hmac_key),
                refresh_prefix=refresh_token[:_TOKEN_PREFIX_LENGTH],
                refresh_expires_at=refresh_expires_at,
                now=now,
            )
            await uow.commit()
        return OAuthTokenResult(
            access_token=access_token,
            refresh_token=refresh_token,
            expires_in=int((access_expires_at - now).total_seconds()),
        )

    async def authenticate_access_token(self, raw_token: str) -> AuthenticatedAccount:
        """Resolve a Bearer access token to the current authenticated account."""
        async with self._uow_factory() as uow:
            account = await self._repository_factory(uow.session).resolve_access_token(
                digest_opaque_token(raw_token, key=self._token_hmac_key),
                at=self._clock.now(),
            )
        if account is None:
            raise InvalidAccessToken("invalid access token")
        return account

    async def _authenticate_oauth_client(self, client_key: str, client_secret: str) -> OAuthClientSnapshot:
        now = self._clock.now()
        async with self._uow_factory() as uow:
            client = await self._repository_factory(uow.session).find_oauth_client(
                client_key,
                digest_opaque_token(client_secret, key=self._token_hmac_key),
                at=now,
            )
        if client is None or not client.first_party:
            raise InvalidOAuthClient("invalid OAuth client")
        return client

    async def _record_oauth_attempt(
        self,
        command: PasswordGrant,
        identifier_hmac: bytes,
        snapshot: CredentialSnapshot | None,
        failure_reason: str,
    ) -> None:
        now = self._clock.now()
        async with self._uow_factory() as uow:
            await self._repository_factory(uow.session).append_auth_attempt(
                account_id=snapshot.account_id if snapshot is not None else None,
                session_id=None,
                device_id=None,
                identifier_hmac=identifier_hmac,
                ip_address=command.ip_address,
                client_version=command.client_version,
                client_family=command.client_family,
                result="failure",
                failure_reason=failure_reason,
                context={"user_agent": command.user_agent, "oauth_client": command.client_key},
                now=now,
            )
            await uow.commit()

    async def resolve_client_session(self, raw_token: str) -> ResolvedClientSession:
        """Resolve an opaque bearer token to current active account context."""
        token_digest = digest_opaque_token(raw_token, key=self._token_hmac_key)
        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            resolved = await repository.resolve_client_session(token_digest, at=self._clock.now())
        if resolved is None:
            raise InvalidSession("invalid client session")
        return resolved

    async def touch_client_session(self, raw_token: str) -> ResolvedClientSession:
        """Resolve a Poll bearer and persist a monotonic last-activity heartbeat."""
        token_digest = digest_opaque_token(raw_token, key=self._token_hmac_key)
        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            touch = await repository.touch_client_session(
                token_digest,
                at=self._clock.now(),
                minimum_interval=self._client_session_touch_interval,
            )
            if touch is None:
                raise InvalidSession("invalid client session")
            resolved, persisted = touch
            if persisted:
                await uow.commit()
        return resolved

    async def verify_online_credentials(
        self, identifier: str, password_preverification: str
    ) -> OnlineCredentialPrincipal:
        """Verify credentials and require an existing online client session."""
        normalized_identifier = _normalize_identifier(identifier)
        now = self._clock.now()
        candidate: tuple[CredentialSnapshot, OpenClientSession] | None = None
        if normalized_identifier is not None:
            async with self._uow_factory() as uow:
                candidate = await self._repository_factory(uow.session).find_online_credential_candidate(
                    *normalized_identifier,
                    at=now,
                )
        if candidate is None:
            await asyncio.to_thread(
                verify_dummy_password,
                pepper=self._password_pepper,
                policy=self._argon2_policy,
            )
            raise InvalidCredentials("invalid credentials")

        snapshot, observed_session = candidate
        if (
            snapshot.account_status != "active"
            or snapshot.must_change
            or _session_is_stale(
                observed_session.last_activity_at,
                at=now,
                grace=self._client_session_stale_grace,
            )
            or not _is_password_preverification(password_preverification)
        ):
            await asyncio.to_thread(
                verify_dummy_password,
                pepper=self._password_pepper,
                policy=self._argon2_policy,
            )
            raise InvalidCredentials("invalid credentials")

        password_proof = _online_password_proof(
            snapshot.account_id, password_preverification, key=self._device_hmac_key
        )
        credential_fingerprint = _credential_fingerprint(snapshot, key=self._device_hmac_key)
        cache = self._online_credential_verification_cache
        if cache is not None:
            try:
                if await cache.matches(
                    account_id=snapshot.account_id,
                    session_id=observed_session.session_id,
                    password_proof=password_proof,
                    credential_fingerprint=credential_fingerprint,
                ):
                    return OnlineCredentialPrincipal(
                        snapshot.account_id,
                        snapshot.current_name,
                        observed_session.session_id,
                        observed_session.expires_at,
                        snapshot.country_code,
                    )
            except Exception as error:
                log_event(
                    "WARNING",
                    "identity.online_credential_verification_cache.failed",
                    exception=error,
                    operation="read",
                    account_id=snapshot.account_id,
                    error_type=type(error).__name__,
                )

        verification = await asyncio.to_thread(
            _verify_credential,
            password_preverification,
            snapshot,
            pepper=self._password_pepper,
            policy=self._argon2_policy,
        )
        if not verification.verified:
            raise InvalidCredentials("invalid credentials")
        validated_at = self._clock.now()
        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            current = await repository.get_current_credential(snapshot.account_id)
            session = await repository.find_open_client_session(snapshot.account_id)
        if (
            not _credential_is_current(snapshot, current)
            or session is None
            or session.session_id != observed_session.session_id
            or session.expires_at <= validated_at
            or _session_is_stale(
                session.last_activity_at,
                at=validated_at,
                grace=self._client_session_stale_grace,
            )
        ):
            raise InvalidCredentials("invalid credentials")
        assert current is not None
        if cache is not None:
            try:
                await cache.store(
                    account_id=snapshot.account_id,
                    session_id=session.session_id,
                    password_proof=password_proof,
                    credential_fingerprint=_credential_fingerprint(current, key=self._device_hmac_key),
                )
            except Exception as error:
                log_event(
                    "WARNING",
                    "identity.online_credential_verification_cache.failed",
                    exception=error,
                    operation="write",
                    account_id=snapshot.account_id,
                    error_type=type(error).__name__,
                )
        return OnlineCredentialPrincipal(
            snapshot.account_id, current.current_name, session.session_id, session.expires_at, current.country_code
        )

    async def close_client_session(self, raw_token: str, *, reason: str = "client_closed") -> None:
        """Close the active session represented by a bearer token."""
        started_ns = time.monotonic_ns()
        _validate_close_reason(reason)
        now = self._clock.now()
        token_digest = digest_opaque_token(raw_token, key=self._token_hmac_key)
        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            resolved = await repository.resolve_client_session(token_digest, at=now)
            if resolved is None:
                raise InvalidSession("invalid client session")
            await repository.acquire_session_lock(resolved.account_id)
            account_id = await repository.close_client_session(
                resolved.session_id,
                now=now,
                reason=reason,
                revoke=False,
            )
            if account_id is None:
                raise InvalidSession("invalid client session")
            await self._outbox_writer_factory(uow.session).append(
                _session_closed_event(account_id, resolved.session_id, now=now, reason=reason, revoked=False)
            )
            await uow.commit()
            log_event(
                "INFO",
                "identity.client_session.closed",
                account_id=account_id,
                session_id=str(resolved.session_id),
                duration_ms=duration_ms(started_ns),
            )

    async def revoke_client_session(self, session_id: uuid.UUID, *, reason: str) -> None:
        """Administratively revoke one client session and advance account auth version."""
        started_ns = time.monotonic_ns()
        _validate_close_reason(reason)
        now = self._clock.now()
        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            account_id = await repository.get_client_session_account_id(session_id)
            if account_id is None:
                raise InvalidSession("invalid client session")
            await repository.acquire_session_lock(account_id)
            closed_account_id = await repository.close_client_session(
                session_id,
                now=now,
                reason=reason,
                revoke=True,
            )
            if closed_account_id is None:
                raise InvalidSession("invalid client session")
            await self._outbox_writer_factory(uow.session).append(
                _session_closed_event(closed_account_id, session_id, now=now, reason=reason, revoked=True)
            )
            await uow.commit()
            log_event(
                "INFO",
                "identity.client_session.revoked",
                account_id=closed_account_id,
                session_id=str(session_id),
                duration_ms=duration_ms(started_ns),
            )

    async def _record_failed_login(
        self,
        command: AuthenticateClientSession,
        identifier_hmac: bytes,
        snapshot: CredentialSnapshot | None,
        failure_reason: str,
    ) -> None:
        now = self._clock.now()
        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            await _append_attempt(
                repository,
                command,
                identifier_hmac,
                account_id=snapshot.account_id if snapshot is not None else None,
                result="failure",
                failure_reason=failure_reason,
                now=now,
            )
            await uow.commit()


def _normalize_identifier(identifier: str) -> tuple[str, str] | None:
    normalized = unicodedata.normalize("NFKC", identifier).strip()
    try:
        if normalized.isdecimal():
            account_id = int(normalized)
            if not 1 <= account_id <= _ACCOUNT_ID_MAX:
                return None
            return "id", str(account_id)
        if "@" in normalized:
            return "email", normalize_email(normalized)
        return "name", normalize_name(normalized)
    except ValueError:
        return None


def _identifier_hmac(normalized: tuple[str, str] | None, raw_identifier: str, *, key: bytes) -> bytes:
    if normalized is None:
        canonical = f"invalid:{unicodedata.normalize('NFKC', raw_identifier).strip().casefold()}"
    else:
        canonical = f"{normalized[0]}:{normalized[1]}"
    return hmac_sha256_digest(canonical, key=key)


def _online_password_proof(account_id: int, password_preverification: str, *, key: bytes) -> bytes:
    return hmac_sha256_digest(f"online:{account_id}:{password_preverification}", key=key)


def _credential_fingerprint(snapshot: CredentialSnapshot, *, key: bytes) -> bytes:
    material = "\0".join(
        (
            str(snapshot.account_id),
            str(snapshot.auth_version),
            snapshot.password_verifier,
            snapshot.algorithm,
            str(snapshot.pepper_version),
            snapshot.password_changed_at.isoformat(),
            str(int(snapshot.must_change)),
        )
    )
    return hmac_sha256_digest(material, key=key)


def _digest_device_components(
    components: tuple[tuple[str, str], ...],
    *,
    key: bytes,
) -> tuple[tuple[tuple[str, bytes], ...], bytes]:
    digests = tuple(sorted((kind, digest_device_component(value, key=key)) for kind, value in components))
    fingerprint_material = b"".join(
        len(kind.encode("utf-8")).to_bytes(2, "big") + kind.encode("utf-8") + digest for kind, digest in digests
    )
    return digests, digest_device_component(fingerprint_material, key=key)


def _credential_is_current(original: CredentialSnapshot, current: CredentialSnapshot | None) -> bool:
    return (
        current is not None
        and current.account_id == original.account_id
        and current.account_status == "active"
        and not current.must_change
        and current.auth_version == original.auth_version
        and current.password_verifier == original.password_verifier
        and current.algorithm == original.algorithm
        and current.pepper_version == original.pepper_version
        and current.password_changed_at == original.password_changed_at
    )


def _session_is_stale(last_activity_at: datetime, *, at: datetime, grace: timedelta) -> bool:
    return last_activity_at <= at - grace


def _is_password_preverification(value: str) -> bool:
    try:
        validate_password_preverification(value)
    except ValueError:
        return False
    return True


def _verify_credential(
    preverification: str,
    snapshot: CredentialSnapshot,
    *,
    pepper: PasswordPepper,
    policy: Argon2Policy,
) -> PasswordVerification:
    if snapshot.algorithm == "bcrypt_md5":
        return verify_legacy_bcrypt_md5(preverification, snapshot.password_verifier)
    if snapshot.algorithm == "argon2id" and snapshot.pepper_version is not None:
        return verify_password(
            preverification,
            PasswordHash(snapshot.password_verifier, snapshot.pepper_version),
            pepper=pepper,
            policy=policy,
        )
    return PasswordVerification(PasswordVerificationStatus.MISMATCH)


async def _append_attempt(
    repository: IdentityRepository,
    command: AuthenticateClientSession,
    identifier_hmac: bytes,
    *,
    account_id: int | None,
    result: str,
    failure_reason: str | None,
    now: datetime,
    session_id: uuid.UUID | None = None,
    device_id: uuid.UUID | None = None,
) -> None:
    await repository.append_auth_attempt(
        account_id=account_id,
        session_id=session_id,
        device_id=device_id,
        identifier_hmac=identifier_hmac,
        ip_address=command.ip_address,
        client_version=command.client_version,
        client_family=command.meta.client.family,
        result=result,
        failure_reason=failure_reason,
        context={"client_variant": command.client_variant, "user_agent": command.user_agent},
        now=now,
    )


def _session_opened_event(
    command: AuthenticateClientSession,
    *,
    account_id: int,
    session_id: uuid.UUID,
    device_id: uuid.UUID,
    opened_at: datetime,
    expires_at: datetime,
) -> PendingEvent:
    return PendingEvent(
        aggregate_type="identity_session",
        aggregate_id=str(session_id),
        event_type="identity.session-opened.v1",
        schema_version=1,
        payload={
            "account_id": account_id,
            "session_id": str(session_id),
            "device_id": str(device_id),
            "client_family": command.meta.client.family,
            "client_version": command.client_version,
            "client_variant": command.client_variant,
            "opened_at": opened_at.isoformat(),
            "expires_at": expires_at.isoformat(),
            "request_id": str(command.meta.request_id),
        },
        consumers=_IDENTITY_CONSUMERS,
        partition_key=f"account:{account_id}",
    )


def _session_closed_event(
    account_id: int,
    session_id: uuid.UUID,
    *,
    now: datetime,
    reason: str,
    revoked: bool,
) -> PendingEvent:
    return PendingEvent(
        aggregate_type="identity_session",
        aggregate_id=str(session_id),
        event_type="identity.session-closed.v1",
        schema_version=1,
        payload={
            "account_id": account_id,
            "session_id": str(session_id),
            "closed_at": now.isoformat(),
            "reason": reason,
            "revoked": revoked,
        },
        consumers=_IDENTITY_CONSUMERS,
        partition_key=f"account:{account_id}",
    )


def _validate_close_reason(reason: str) -> None:
    if not reason or len(reason) > 64:
        raise ValueError("session close reasons must contain at most 64 characters")


def _validate_generated_token(token: str) -> None:
    if len(token) <= _TOKEN_PREFIX_LENGTH:
        raise RuntimeError("token factory returned a token too short for a non-secret prefix")


def _oauth_session_opened_event(
    *,
    account_id: int,
    session_id: uuid.UUID,
    client_family: str,
    client_version: str | None,
    opened_at: datetime,
    expires_at: datetime,
) -> PendingEvent:
    return PendingEvent(
        aggregate_type="identity_session",
        aggregate_id=str(session_id),
        event_type="identity.session-opened.v1",
        schema_version=1,
        payload={
            "account_id": account_id,
            "session_id": str(session_id),
            "device_id": None,
            "client_family": client_family,
            "client_version": client_version,
            "client_variant": None,
            "opened_at": opened_at.isoformat(),
            "expires_at": expires_at.isoformat(),
        },
        consumers=_IDENTITY_CONSUMERS,
        partition_key=f"account:{account_id}",
    )
