"""Provide protocol-neutral account registration."""

import asyncio
import time
import unicodedata
from collections.abc import Callable
from datetime import timedelta

from perfcho.infra.logging import duration_ms, log_event
from perfcho.infra.security.password import (
    Argon2Policy,
    PasswordPepper,
    hash_password,
    validate_stable_password_token,
)
from perfcho.modules.account.errors import EmailUnavailable, NameUnavailable, RegistrationRejected
from perfcho.modules.account.models import RegisterAccount, RegistrationRecord, RegistrationResult
from perfcho.modules.account.ports import AccountRepositoryFactory, AccountUnitOfWork
from perfcho.modules.common.models import PendingEvent
from perfcho.modules.common.normalization import normalize_email, normalize_stable_name
from perfcho.modules.common.ports import Clock, OutboxWriterFactory

_RECEIPT_SCOPE = "account.register"
_DEFAULT_RECEIPT_TTL = timedelta(days=1)
_REGISTRATION_CONSUMERS = ("account-projector.v1",)


class AccountService:
    """Register canonical user accounts in one explicit transaction."""

    def __init__(
        self,
        uow_factory: Callable[[], AccountUnitOfWork],
        repository_factory: AccountRepositoryFactory,
        outbox_writer_factory: OutboxWriterFactory,
        password_pepper: PasswordPepper,
        argon2_policy: Argon2Policy,
        clock: Clock,
        *,
        receipt_ttl: timedelta = _DEFAULT_RECEIPT_TTL,
    ) -> None:
        """Bind transaction, persistence, password, event, and time dependencies."""
        if receipt_ttl <= timedelta(0):
            raise ValueError("receipt_ttl must be positive")
        self._uow_factory = uow_factory
        self._repository_factory = repository_factory
        self._outbox_writer_factory = outbox_writer_factory
        self._password_pepper = password_pepper
        self._argon2_policy = argon2_policy
        self._clock = clock
        self._receipt_ttl = receipt_ttl

    async def register(self, command: RegisterAccount) -> RegistrationResult:
        """Validate, hash, and atomically persist one account registration."""
        started_ns = time.monotonic_ns()
        try:
            display_name = unicodedata.normalize("NFKC", command.display_name)
            name_key = normalize_stable_name(display_name)
            email = normalize_email(command.email)
            password_preverification = validate_stable_password_token(command.password_preverification)
        except ValueError as error:
            raise RegistrationRejected(str(error)) from error

        password_hash = await asyncio.to_thread(
            hash_password,
            password_preverification,
            pepper=self._password_pepper,
            policy=self._argon2_policy,
        )
        now = self._clock.now()
        status = "active" if command.activate_immediately else "pending"
        record = RegistrationRecord(
            display_name=display_name,
            name_key=name_key,
            email=email,
            email_key=email,
            password_verifier=password_hash.verifier,
            pepper_version=password_hash.pepper_version,
            status=status,
            registered_at=now,
        )

        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            claim = await repository.claim_registration(
                idempotency_key=command.meta.idempotency_key,
                request_digest=command.meta.request_digest,
                now=now,
                expires_at=now + self._receipt_ttl,
            )
            if claim.prior_result is not None:
                await uow.commit()
                log_event(
                    "DEBUG",
                    "account.registration.replayed",
                    account_id=claim.prior_result.account_id,
                    request_id=str(command.meta.request_id),
                    duration_ms=duration_ms(started_ns),
                )
                return claim.prior_result

            await repository.acquire_identifier_locks(name_key, email)
            if await repository.name_exists(name_key):
                raise NameUnavailable("account name is already in use")
            if await repository.email_exists(email):
                raise EmailUnavailable("account email is already in use")

            result = await repository.create_account(record)
            outbox_writer = self._outbox_writer_factory(uow.session)
            await outbox_writer.append(
                PendingEvent(
                    aggregate_type="account",
                    aggregate_id=str(result.account_id),
                    event_type="account.registered.v1",
                    schema_version=1,
                    payload={
                        "account_id": result.account_id,
                        "display_name": result.display_name,
                        "status": result.status,
                        "registered_at": now.isoformat(),
                        "request_id": str(command.meta.request_id),
                    },
                    consumers=_REGISTRATION_CONSUMERS,
                    partition_key=f"account:{result.account_id}",
                )
            )
            await repository.complete_registration(command.meta.idempotency_key, result)
            await uow.commit()
            log_event(
                "INFO",
                "account.registration.committed",
                account_id=result.account_id,
                status=result.status,
                request_id=str(command.meta.request_id),
                duration_ms=duration_ms(started_ns),
            )
            return result
