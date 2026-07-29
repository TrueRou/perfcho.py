"""Persist complete account registrations in a caller-owned transaction."""

import uuid
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra.db.advisory_lock import acquire_transaction_locks, advisory_lock_key
from perfcho.infra.db.enums import AccountStatus, AccountType, Ruleset
from perfcho.infra.db.idempotency import CommandReceiptRepository, ReceiptClaim, ReceiptClaimState
from perfcho.infra.db.models.authz import AccountRoleGrant, Role
from perfcho.infra.db.models.core import Account, AccountEmail, AccountName, UserPreference, UserProfile
from perfcho.infra.db.models.iam import PasswordCredential
from perfcho.infra.outbox import write_outbox_event
from perfcho.modules.account.errors import EmailUnavailable, NameUnavailable
from perfcho.modules.account.models import RegistrationClaim, RegistrationRecord, RegistrationResult
from perfcho.modules.common.models import PendingEvent

_RECEIPT_SCOPE = "account.register"
_NAME_CONSTRAINTS = frozenset({"uq_account_names_current_key"})
_EMAIL_CONSTRAINTS = frozenset({"uq_account_emails_active_key"})


class SqlAlchemyAccountRepository:
    """Write account facts through an existing asynchronous session."""

    def __init__(self, session: AsyncSession) -> None:
        """Bind all operations to the caller-owned session."""
        self._session = session
        self._receipts = CommandReceiptRepository(session)

    async def claim_registration(
        self,
        *,
        idempotency_key: str,
        request_digest: bytes,
        now: datetime,
        expires_at: datetime,
    ) -> RegistrationClaim:
        """Claim registration idempotency and deserialize an exact replay."""
        claim = await self._receipts.claim(
            scope=_RECEIPT_SCOPE,
            idempotency_key=idempotency_key,
            request_digest=request_digest,
            now=now,
            expires_at=expires_at,
        )
        if claim.state is ReceiptClaimState.NEW:
            return RegistrationClaim()
        return RegistrationClaim(_registration_result_from_receipt(claim))

    async def acquire_identifier_locks(self, name_key: str, email_key: str) -> None:
        """Acquire name and email transaction locks in deterministic numeric order."""
        await acquire_transaction_locks(
            self._session,
            (
                advisory_lock_key("account-registration", "name", name_key),
                advisory_lock_key("account-registration", "email", email_key),
            ),
        )

    async def name_exists(self, name_key: str) -> bool:
        """Check current name ownership using a scalar projection."""
        identifier = await self._session.scalar(
            select(AccountName.id).where(AccountName.name_key == name_key, AccountName.ended_at.is_(None)).limit(1)
        )
        return identifier is not None

    async def email_exists(self, email_key: str) -> bool:
        """Check active email ownership using a scalar projection."""
        identifier = await self._session.scalar(
            select(AccountEmail.id)
            .where(AccountEmail.email_key == email_key, AccountEmail.retired_at.is_(None))
            .limit(1)
        )
        return identifier is not None

    async def create_account(self, record: RegistrationRecord) -> RegistrationResult:
        """Insert the canonical account graph and only the seeded user role."""
        role_id = await self._session.scalar(select(Role.id).where(Role.code == "user"))
        if role_id is None:
            raise RuntimeError("seeded user role is unavailable")

        status = AccountStatus(record.status)
        account = Account(
            type=AccountType.USER,
            status=status,
            registered_at=record.registered_at,
            activated_at=record.registered_at if status is AccountStatus.ACTIVE else None,
        )
        self._session.add(account)
        try:
            await self._session.flush()
            account_id = account.id
            if account_id is None:
                raise RuntimeError("database did not assign an account identifier")
            self._session.add_all(
                (
                    AccountName(
                        account_id=account_id,
                        display_name=record.display_name,
                        name_key=record.name_key,
                        started_at=record.registered_at,
                    ),
                    AccountEmail(
                        id=uuid.uuid7(),
                        account_id=account_id,
                        email=record.email,
                        email_key=record.email_key,
                        is_primary=True,
                        added_at=record.registered_at,
                        verified_at=record.registered_at if status is AccountStatus.ACTIVE else None,
                    ),
                    UserProfile(
                        account_id=account_id,
                        social_links={},
                        default_ruleset=Ruleset.OSU,
                        play_style=[],
                    ),
                    UserPreference(
                        account_id=account_id,
                        locale="en",
                        timezone="UTC",
                        theme="system",
                        master_volume=1.0,
                        music_volume=1.0,
                        effect_volume=1.0,
                        private_message_policy="friends",
                        invisible_online=False,
                        profile_section_order=[],
                        extra={},
                    ),
                    PasswordCredential(
                        account_id=account_id,
                        verifier=record.password_verifier,
                        algorithm="argon2id",
                        pepper_version=record.pepper_version,
                        password_changed_at=record.registered_at,
                        must_change=False,
                    ),
                    AccountRoleGrant(
                        id=uuid.uuid7(),
                        account_id=account_id,
                        role_id=role_id,
                        starts_at=record.registered_at,
                        reason="Default account registration role.",
                    ),
                )
            )
            await self._session.flush()
        except IntegrityError as error:
            mapped_error = _map_registration_integrity_error(error)
            if mapped_error is not None:
                raise mapped_error from error
            raise

        return RegistrationResult(
            account_id=account_id,
            display_name=record.display_name,
            email=record.email,
            status=record.status,
        )

    async def complete_registration(self, idempotency_key: str, result: RegistrationResult) -> None:
        """Persist the non-secret registration result on its receipt."""
        await self._receipts.complete(
            scope=_RECEIPT_SCOPE,
            idempotency_key=idempotency_key,
            resource_type="account",
            resource_id=str(result.account_id),
            result_snapshot={
                "account_id": result.account_id,
                "display_name": result.display_name,
                "email": result.email,
                "status": result.status,
            },
        )


class SqlAlchemyOutboxWriter:
    """Adapt the shared outbox function to a transaction-bound writer port."""

    def __init__(self, session: AsyncSession) -> None:
        """Bind event writes to the caller-owned session."""
        self._session = session

    async def append(self, event: PendingEvent) -> uuid.UUID:
        """Append one event and its deliveries without committing."""
        persisted = await write_outbox_event(
            self._session,
            aggregate_type=event.aggregate_type,
            aggregate_id=event.aggregate_id,
            event_type=event.event_type,
            schema_version=event.schema_version,
            payload=dict(event.payload),
            consumers=event.consumers,
            partition_key=event.partition_key,
        )
        return persisted.id


def _registration_result_from_receipt(claim: ReceiptClaim) -> RegistrationResult:
    snapshot = claim.result_snapshot
    account_id = snapshot.get("account_id")
    display_name = snapshot.get("display_name")
    email = snapshot.get("email")
    status = snapshot.get("status")
    if (
        not isinstance(account_id, int)
        or isinstance(account_id, bool)
        or not isinstance(display_name, str)
        or not isinstance(email, str)
        or not isinstance(status, str)
        or claim.resource_type != "account"
        or claim.resource_id != str(account_id)
    ):
        raise RuntimeError("completed registration receipt contains an invalid result")
    return RegistrationResult(account_id, display_name, email, status)


def _map_registration_integrity_error(error: IntegrityError) -> NameUnavailable | EmailUnavailable | None:
    constraint_name = _integrity_constraint_name(error)
    message = str(error).lower()
    if constraint_name in _NAME_CONSTRAINTS or "uq_account_names_current_key" in message:
        return NameUnavailable("account name is already in use")
    if constraint_name in _EMAIL_CONSTRAINTS or "uq_account_emails_active_key" in message:
        return EmailUnavailable("account email is already in use")
    return None


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
