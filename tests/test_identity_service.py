import hashlib
import uuid
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace, TracebackType
from typing import cast
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import func, select
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra.db import engine as infra_db
from perfcho.infra.db.enums import AccountStatus, ClientFamily, TokenKind
from perfcho.infra.db.models.core import Account
from perfcho.infra.db.models.events import OutboxEvent
from perfcho.infra.db.models.iam import (
    AccountDevice,
    AuthAttempt,
    AuthSession,
    AuthToken,
    Device,
    DeviceIdentifier,
)
from perfcho.infra.db.repositories.account import SqlAlchemyAccountRepository, SqlAlchemyOutboxWriter
from perfcho.infra.db.repositories.identity import SqlAlchemyIdentityRepository
from perfcho.infra.db.uow import SqlAlchemyUnitOfWorkFactory
from perfcho.infra.security.password import (
    Argon2Policy,
    PasswordHash,
    PasswordPepper,
    PasswordVerification,
    PasswordVerificationStatus,
)
from perfcho.infra.security.tokens import digest_device_component, digest_opaque_token
from perfcho.modules.account import AccountService, RegisterAccount
from perfcho.modules.common import ClientContext, CommandMeta
from perfcho.modules.common.models import PendingEvent
from perfcho.modules.identity import (
    CredentialSnapshot,
    IdentityService,
    InvalidCredentials,
    InvalidStableSession,
    ResolvedStableSession,
    StableLogin,
    StableSessionAlreadyActive,
    StableSessionResult,
)
from perfcho.modules.identity.models import OpenStableSession

INSTANT = datetime(2026, 7, 29, 12, 0, tzinfo=UTC)
POLICY = Argon2Policy(1, 8, 1)
PEPPER = PasswordPepper(1, b"identity-test-pepper")
TOKEN_KEY = b"identity-test-token-key"
DEVICE_KEY = b"identity-test-device-key"


class FixedClock:
    def __init__(self, instant: datetime = INSTANT) -> None:
        self.instant = instant

    def now(self) -> datetime:
        return self.instant


class SequenceIds:
    def __init__(self) -> None:
        self.values = iter(uuid.uuid7() for _ in range(32))

    def new(self) -> uuid.UUID:
        return next(self.values)


class FakeUnitOfWork:
    def __init__(self, calls: list[str], units: list[FakeUnitOfWork]) -> None:
        self.calls = calls
        self.session = object()
        self.active = False
        self.committed = False
        units.append(self)

    async def __aenter__(self) -> FakeUnitOfWork:
        self.active = True
        self.calls.append("enter")
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.active = False
        self.calls.append("exit")

    async def commit(self) -> None:
        self.committed = True
        self.calls.append("commit")


class FakeIdentityRepository:
    def __init__(self, calls: list[str], snapshot: CredentialSnapshot | None) -> None:
        self.calls = calls
        self.snapshot = snapshot
        self.current = snapshot
        self.open_session: OpenStableSession | None = None
        self.web_candidate: tuple[CredentialSnapshot, OpenStableSession] | None = None
        self.resolved: ResolvedStableSession | None = None
        self.session_account_id: int | None = snapshot.account_id if snapshot is not None else None
        self.attempts: list[dict[str, object]] = []
        self.device_write: dict[str, object] | None = None
        self.session_write: dict[str, object] | None = None
        self.credential_upgrade: dict[str, object] | None = None
        self.upgrade_succeeds = True
        self.closed: list[tuple[uuid.UUID, bool, str]] = []

    async def find_credential(self, identifier_kind: str, identifier_key: str) -> CredentialSnapshot | None:
        self.calls.append(f"lookup:{identifier_kind}:{identifier_key}")
        return self.snapshot

    async def get_current_credential(self, account_id: int) -> CredentialSnapshot | None:
        self.calls.append(f"recheck:{account_id}")
        return self.current

    async def upgrade_legacy_credential(self, **kwargs: object) -> bool:
        self.calls.append("upgrade")
        self.credential_upgrade = dict(kwargs)
        return self.upgrade_succeeds

    async def acquire_stable_session_lock(self, account_id: int) -> None:
        self.calls.append(f"lock:{account_id}")

    async def find_open_stable_session(self, account_id: int) -> OpenStableSession | None:
        self.calls.append(f"open:{account_id}")
        return self.open_session

    async def find_stable_web_candidate(
        self,
        identifier_kind: str,
        identifier_key: str,
        *,
        at: datetime,
    ) -> tuple[CredentialSnapshot, OpenStableSession] | None:
        self.calls.append(f"web-candidate:{identifier_kind}:{identifier_key}")
        return self.web_candidate

    async def get_or_create_device(self, **kwargs: object) -> uuid.UUID:
        self.calls.append("device")
        self.device_write = dict(kwargs)
        return cast(uuid.UUID, kwargs["proposed_device_id"])

    async def create_stable_session(self, **kwargs: object) -> None:
        self.calls.append("create")
        self.session_write = dict(kwargs)

    async def append_auth_attempt(self, **kwargs: object) -> None:
        self.calls.append(f"attempt:{kwargs['result']}")
        self.attempts.append(dict(kwargs))

    async def resolve_stable_session(self, token_digest: bytes, *, at: datetime) -> ResolvedStableSession | None:
        self.calls.append("resolve")
        return self.resolved

    async def touch_stable_session(self, token_digest: bytes, *, at: datetime) -> ResolvedStableSession | None:
        self.calls.append("touch")
        return replace(self.resolved, last_activity_at=at) if self.resolved is not None else None

    async def get_stable_session_account_id(self, session_id: uuid.UUID) -> int | None:
        self.calls.append("session-account")
        return self.session_account_id

    async def close_stable_session(
        self,
        session_id: uuid.UUID,
        *,
        now: datetime,
        reason: str,
        revoke: bool,
    ) -> int | None:
        self.calls.append(f"close:{revoke}")
        self.closed.append((session_id, revoke, reason))
        return self.session_account_id


class FakeOutboxWriter:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls
        self.events: list[PendingEvent] = []

    async def append(self, event: PendingEvent) -> uuid.UUID:
        self.calls.append(f"event:{event.event_type}")
        self.events.append(event)
        return uuid.uuid7()


def _snapshot(
    *,
    auth_version: int = 1,
    password_verifier: str = "$argon2id$test",
    algorithm: str = "argon2id",
    pepper_version: int | None = 1,
) -> CredentialSnapshot:
    return CredentialSnapshot(
        account_id=42,
        current_name="Alice",
        account_status="active",
        auth_version=auth_version,
        password_verifier=password_verifier,
        algorithm=algorithm,
        pepper_version=pepper_version,
        password_changed_at=INSTANT - timedelta(days=1),
        must_change=False,
    )


def _meta(value: bytes = b"identity-login") -> CommandMeta:
    return CommandMeta(
        request_id=uuid.uuid7(),
        idempotency_key="stable-login:test",
        request_digest=hashlib.sha256(value).digest(),
        actor=None,
        client=ClientContext("stable", "b20260729", "cuttingedge", "127.0.0.1", "osu!"),
        received_at=INSTANT,
    )


def _login(*, identifier: str = "Ａlice", password_token: str = "a" * 32) -> StableLogin:
    return StableLogin(
        meta=_meta(),
        identifier=identifier,
        password_token=password_token,
        client_version="b20260729",
        client_variant="cuttingedge",
        ip_address="127.0.0.1",
        user_agent="osu!",
        device_components=(("Adapters", "adapter-secret"), ("uninstall", "uninstall-secret")),
        session_lifetime=timedelta(hours=12),
    )


def _service(
    repository: FakeIdentityRepository,
    outbox: FakeOutboxWriter,
    calls: list[str],
) -> tuple[IdentityService, list[FakeUnitOfWork]]:
    units: list[FakeUnitOfWork] = []

    def create_uow() -> FakeUnitOfWork:
        calls.append("uow")
        return FakeUnitOfWork(calls, units)

    return (
        IdentityService(
            uow_factory=create_uow,
            repository_factory=lambda session: repository,
            outbox_writer_factory=lambda session: outbox,
            password_pepper=PEPPER,
            argon2_policy=POLICY,
            token_hmac_key=TOKEN_KEY,
            device_hmac_key=DEVICE_KEY,
            clock=FixedClock(),
            id_generator=SequenceIds(),
            stable_session_stale_grace=timedelta(minutes=2),
            token_factory=lambda: "raw-stable-token-secret",
        ),
        units,
    )


def test_identity_values_are_frozen_slotted_and_hide_secrets() -> None:
    command = _login()
    result = StableSessionResult(42, "Alice", uuid.uuid7(), uuid.uuid7(), "secret", INSTANT)

    assert command.__slots__
    assert result.__slots__
    assert "a" * 32 not in repr(command)
    assert "secret" not in repr(result)
    assert command.device_components[0][0] == "adapters"
    with pytest.raises(FrozenInstanceError):
        command.__setattr__("identifier", "other")


@pytest.mark.asyncio
async def test_login_verifies_outside_transactions_rechecks_and_persists_only_hmacs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    repository = FakeIdentityRepository(calls, _snapshot())
    outbox = FakeOutboxWriter(calls)
    service, units = _service(repository, outbox, calls)

    def verify_outside_transaction(*args: object, **kwargs: object) -> PasswordVerification:
        assert units and not any(unit.active for unit in units)
        calls.append("verify")
        return PasswordVerification(PasswordVerificationStatus.MATCH)

    monkeypatch.setattr("perfcho.modules.identity.services.verify_password", verify_outside_transaction)
    result = await service.login_stable(_login())

    assert result.raw_token == "raw-stable-token-secret"
    assert calls == [
        "uow",
        "enter",
        "lookup:name:alice",
        "exit",
        "verify",
        "uow",
        "enter",
        "lock:42",
        "recheck:42",
        "open:42",
        "device",
        "create",
        "attempt:success",
        "event:identity.session-opened.v1",
        "commit",
        "exit",
    ]
    assert not units[0].committed and units[1].committed
    assert repository.session_write is not None
    assert repository.session_write["token_digest"] == digest_opaque_token("raw-stable-token-secret", key=TOKEN_KEY)
    assert repository.session_write["token_prefix"] == "raw-stable-token"
    assert "raw-stable-token-secret" not in repr(repository.session_write)
    assert repository.device_write is not None
    component_hmacs = cast(tuple[tuple[str, bytes], ...], repository.device_write["component_hmacs"])
    assert component_hmacs == (
        ("adapters", digest_device_component("adapter-secret", key=DEVICE_KEY)),
        ("uninstall", digest_device_component("uninstall-secret", key=DEVICE_KEY)),
    )
    assert "adapter-secret" not in repr(repository.device_write)
    assert repository.attempts[0]["failure_reason"] is None
    assert outbox.events[0].payload["session_id"] == str(result.session_id)
    assert "raw-stable-token-secret" not in repr(outbox.events[0])


@pytest.mark.asyncio
async def test_successful_legacy_login_upgrades_after_recheck_in_the_session_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    legacy = _snapshot(password_verifier="$2b$legacy", algorithm="bcrypt_md5", pepper_version=None)
    repository = FakeIdentityRepository(calls, legacy)
    outbox = FakeOutboxWriter(calls)
    service, units = _service(repository, outbox, calls)

    def verify_legacy_outside_transaction(*args: object, **kwargs: object) -> PasswordVerification:
        assert units and not any(unit.active for unit in units)
        calls.append("verify-legacy")
        return PasswordVerification(PasswordVerificationStatus.MATCH)

    def hash_argon_outside_transaction(*args: object, **kwargs: object) -> PasswordHash:
        assert not any(unit.active for unit in units)
        calls.append("hash-argon2")
        return PasswordHash("$argon2id$replacement", PEPPER.version)

    monkeypatch.setattr(
        "perfcho.modules.identity.services.verify_legacy_bcrypt_md5",
        verify_legacy_outside_transaction,
    )
    monkeypatch.setattr("perfcho.modules.identity.services.hash_password", hash_argon_outside_transaction)

    await service.login_stable(_login())

    assert calls.index("verify-legacy") < calls.index("hash-argon2") < calls.index("lock:42")
    assert calls.index("recheck:42") < calls.index("upgrade") < calls.index("create") < calls.index("commit")
    assert repository.credential_upgrade == {
        "account_id": 42,
        "expected_verifier": "$2b$legacy",
        "expected_password_changed_at": INSTANT - timedelta(days=1),
        "password_verifier": "$argon2id$replacement",
        "pepper_version": PEPPER.version,
        "password_changed_at": INSTANT,
    }
    assert units[1].committed


@pytest.mark.asyncio
async def test_concurrent_argon_credential_wins_over_a_verified_legacy_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    legacy = _snapshot(password_verifier="$2b$legacy", algorithm="bcrypt_md5", pepper_version=None)
    repository = FakeIdentityRepository(calls, legacy)
    repository.current = _snapshot(password_verifier="$argon2id$current")
    outbox = FakeOutboxWriter(calls)
    service, units = _service(repository, outbox, calls)
    monkeypatch.setattr(
        "perfcho.modules.identity.services.verify_legacy_bcrypt_md5",
        lambda *args, **kwargs: PasswordVerification(PasswordVerificationStatus.MATCH),
    )
    monkeypatch.setattr(
        "perfcho.modules.identity.services.hash_password",
        lambda *args, **kwargs: PasswordHash("$argon2id$replacement", PEPPER.version),
    )

    with pytest.raises(InvalidCredentials):
        await service.login_stable(_login())

    assert repository.credential_upgrade is None
    assert repository.session_write is None
    assert repository.attempts[0]["failure_reason"] == "invalid_credentials"
    assert units[1].committed


@pytest.mark.asyncio
async def test_failed_legacy_compare_and_replace_does_not_create_a_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    legacy = _snapshot(password_verifier="$2b$legacy", algorithm="bcrypt_md5", pepper_version=None)
    repository = FakeIdentityRepository(calls, legacy)
    repository.upgrade_succeeds = False
    outbox = FakeOutboxWriter(calls)
    service, units = _service(repository, outbox, calls)
    monkeypatch.setattr(
        "perfcho.modules.identity.services.verify_legacy_bcrypt_md5",
        lambda *args, **kwargs: PasswordVerification(PasswordVerificationStatus.MATCH),
    )
    monkeypatch.setattr(
        "perfcho.modules.identity.services.hash_password",
        lambda *args, **kwargs: PasswordHash("$argon2id$replacement", PEPPER.version),
    )

    with pytest.raises(InvalidCredentials):
        await service.login_stable(_login())

    assert repository.credential_upgrade is not None
    assert repository.session_write is None
    assert repository.attempts[0]["failure_reason"] == "invalid_credentials"
    assert units[1].committed


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("snapshot", "password_matches"),
    ((None, True), (_snapshot(), False)),
)
async def test_missing_identifier_and_wrong_password_are_indistinguishable_and_audited(
    monkeypatch: pytest.MonkeyPatch,
    snapshot: CredentialSnapshot | None,
    password_matches: bool,
) -> None:
    calls: list[str] = []
    repository = FakeIdentityRepository(calls, snapshot)
    outbox = FakeOutboxWriter(calls)
    service, units = _service(repository, outbox, calls)

    monkeypatch.setattr(
        "perfcho.modules.identity.services.verify_password",
        lambda *args, **kwargs: PasswordVerification(
            PasswordVerificationStatus.MATCH if password_matches else PasswordVerificationStatus.MISMATCH
        ),
    )
    with pytest.raises(InvalidCredentials) as raised:
        await service.login_stable(_login(identifier="Unknown"))

    assert raised.value.code == "invalid_credentials"
    assert str(raised.value) == "invalid credentials"
    assert repository.attempts == [
        {
            "account_id": snapshot.account_id if snapshot is not None else None,
            "session_id": None,
            "device_id": None,
            "identifier_hmac": repository.attempts[0]["identifier_hmac"],
            "ip_address": "127.0.0.1",
            "client_version": "b20260729",
            "result": "failure",
            "failure_reason": "invalid_credentials",
            "context": {"client_variant": "cuttingedge", "user_agent": "osu!"},
            "now": INSTANT,
        }
    ]
    assert "Unknown" not in repr(repository.attempts[0])
    assert units[-1].committed
    assert outbox.events == []


@pytest.mark.asyncio
async def test_changed_credential_is_rejected_and_audited_in_second_uow(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    repository = FakeIdentityRepository(calls, _snapshot())
    repository.current = _snapshot(auth_version=2)
    outbox = FakeOutboxWriter(calls)
    service, units = _service(repository, outbox, calls)
    monkeypatch.setattr(
        "perfcho.modules.identity.services.verify_password",
        lambda *args, **kwargs: PasswordVerification(PasswordVerificationStatus.MATCH),
    )

    with pytest.raises(InvalidCredentials):
        await service.login_stable(_login())

    assert units[1].committed
    assert repository.attempts[0]["failure_reason"] == "invalid_credentials"
    assert repository.session_write is None
    assert outbox.events == []


@pytest.mark.asyncio
async def test_login_enforces_one_active_normal_stable_session(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    repository = FakeIdentityRepository(calls, _snapshot())
    repository.open_session = OpenStableSession(
        uuid.uuid7(),
        INSTANT - timedelta(hours=1),
        INSTANT - timedelta(seconds=1),
        INSTANT + timedelta(minutes=1),
    )
    outbox = FakeOutboxWriter(calls)
    service, units = _service(repository, outbox, calls)
    monkeypatch.setattr(
        "perfcho.modules.identity.services.verify_password",
        lambda *args, **kwargs: PasswordVerification(PasswordVerificationStatus.MATCH),
    )

    with pytest.raises(StableSessionAlreadyActive):
        await service.login_stable(_login())

    assert units[1].committed
    assert repository.attempts[0]["failure_reason"] == "active_session"
    assert repository.session_write is None


@pytest.mark.asyncio
async def test_expired_open_session_is_closed_before_replacement(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    repository = FakeIdentityRepository(calls, _snapshot())
    expired_id = uuid.uuid7()
    repository.open_session = OpenStableSession(
        expired_id,
        INSTANT - timedelta(hours=2),
        INSTANT - timedelta(hours=1),
        INSTANT - timedelta(seconds=1),
    )
    outbox = FakeOutboxWriter(calls)
    service, _ = _service(repository, outbox, calls)
    monkeypatch.setattr(
        "perfcho.modules.identity.services.verify_password",
        lambda *args, **kwargs: PasswordVerification(PasswordVerificationStatus.MATCH),
    )

    await service.login_stable(_login())

    assert repository.closed == [(expired_id, False, "expired")]
    assert [event.event_type for event in outbox.events] == [
        "identity.session-closed.v1",
        "identity.session-opened.v1",
    ]


@pytest.mark.asyncio
async def test_stale_open_session_is_atomically_closed_before_replacement(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    repository = FakeIdentityRepository(calls, _snapshot())
    stale_id = uuid.uuid7()
    repository.open_session = OpenStableSession(
        stale_id,
        INSTANT - timedelta(hours=1),
        INSTANT - timedelta(minutes=2),
        INSTANT + timedelta(hours=1),
    )
    outbox = FakeOutboxWriter(calls)
    service, units = _service(repository, outbox, calls)
    monkeypatch.setattr(
        "perfcho.modules.identity.services.verify_password",
        lambda *args, **kwargs: PasswordVerification(PasswordVerificationStatus.MATCH),
    )

    await service.login_stable(_login())

    assert repository.closed == [(stale_id, False, "stale")]
    assert calls.index("lock:42") < calls.index("open:42") < calls.index("close:False") < calls.index("create")
    assert units[-1].committed
    assert [event.payload.get("reason") for event in outbox.events] == ["stale", None]


@pytest.mark.asyncio
async def test_resolve_close_and_revoke_use_digest_and_emit_closed_events() -> None:
    calls: list[str] = []
    repository = FakeIdentityRepository(calls, _snapshot())
    session_id = uuid.uuid7()
    repository.resolved = ResolvedStableSession(
        account_id=42,
        current_name="Alice",
        auth_version=1,
        session_id=session_id,
        device_id=uuid.uuid7(),
        client_version="b20260729",
        client_variant="cuttingedge",
        expires_at=INSTANT + timedelta(hours=1),
        opened_at=INSTANT - timedelta(minutes=5),
        last_activity_at=INSTANT - timedelta(seconds=5),
    )
    outbox = FakeOutboxWriter(calls)
    service, units = _service(repository, outbox, calls)

    assert await service.resolve_stable_session("bearer") == repository.resolved
    touched = await service.touch_stable_session("bearer")
    await service.close_stable_session("bearer", reason="login_bootstrap_failed")
    await service.revoke_stable_session(session_id, reason="credential_reset")

    assert touched.opened_at == INSTANT - timedelta(minutes=5)
    assert touched.last_activity_at == INSTANT
    assert repository.closed == [
        (session_id, False, "login_bootstrap_failed"),
        (session_id, True, "credential_reset"),
    ]
    assert [event.event_type for event in outbox.events] == [
        "identity.session-closed.v1",
        "identity.session-closed.v1",
    ]
    assert outbox.events[1].payload["revoked"] is True
    assert sum(unit.committed for unit in units) == 3


@pytest.mark.asyncio
async def test_web_verification_checks_session_before_password_and_rechecks_afterward(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    snapshot = _snapshot()
    session = OpenStableSession(
        uuid.uuid7(),
        INSTANT - timedelta(minutes=5),
        INSTANT - timedelta(seconds=5),
        INSTANT + timedelta(hours=1),
    )
    repository = FakeIdentityRepository(calls, snapshot)
    repository.web_candidate = (snapshot, session)
    repository.open_session = session
    outbox = FakeOutboxWriter(calls)
    service, _ = _service(repository, outbox, calls)

    def verify(*args: object, **kwargs: object) -> PasswordVerification:
        calls.append("verify-web")
        return PasswordVerification(PasswordVerificationStatus.MATCH)

    monkeypatch.setattr("perfcho.modules.identity.services.verify_password", verify)

    principal = await service.verify_stable_web("Alice", "a" * 32)

    assert principal.session_id == session.session_id
    assert calls.index("web-candidate:name:alice") < calls.index("verify-web") < calls.index("recheck:42")


@pytest.mark.asyncio
async def test_web_verification_uses_dummy_kdf_without_an_online_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    repository = FakeIdentityRepository(calls, None)
    outbox = FakeOutboxWriter(calls)
    service, _ = _service(repository, outbox, calls)

    def dummy(*args: object, **kwargs: object) -> PasswordVerification:
        calls.append("dummy-kdf")
        return PasswordVerification(PasswordVerificationStatus.MISMATCH)

    monkeypatch.setattr("perfcho.modules.identity.services.verify_dummy_password", dummy)
    monkeypatch.setattr(
        "perfcho.modules.identity.services.verify_password",
        lambda *args, **kwargs: pytest.fail("account password KDF must not run without an online candidate"),
    )

    with pytest.raises(InvalidCredentials):
        await service.verify_stable_web("Unknown", "a" * 32)

    assert calls == ["uow", "enter", "web-candidate:name:unknown", "exit", "dummy-kdf"]


@pytest.mark.asyncio
async def test_web_verification_uses_dummy_kdf_for_malformed_password_shape(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    snapshot = _snapshot()
    session = OpenStableSession(
        uuid.uuid7(),
        INSTANT - timedelta(minutes=5),
        INSTANT - timedelta(seconds=5),
        INSTANT + timedelta(hours=1),
    )
    repository = FakeIdentityRepository(calls, snapshot)
    repository.web_candidate = (snapshot, session)
    outbox = FakeOutboxWriter(calls)
    service, _ = _service(repository, outbox, calls)
    monkeypatch.setattr(
        "perfcho.modules.identity.services.verify_dummy_password",
        lambda **kwargs: PasswordVerification(PasswordVerificationStatus.MISMATCH),
    )
    monkeypatch.setattr(
        "perfcho.modules.identity.services.verify_password",
        lambda *args, **kwargs: pytest.fail("malformed passwords must use only the dummy verifier"),
    )

    with pytest.raises(InvalidCredentials):
        await service.verify_stable_web("Alice", "z" * 32)


@pytest.mark.asyncio
async def test_unresolvable_token_raises_domain_error_without_writes() -> None:
    calls: list[str] = []
    repository = FakeIdentityRepository(calls, _snapshot())
    outbox = FakeOutboxWriter(calls)
    service, units = _service(repository, outbox, calls)

    with pytest.raises(InvalidStableSession):
        await service.resolve_stable_session("missing")

    assert not units[0].committed
    assert outbox.events == []


@pytest.mark.asyncio
async def test_sqlalchemy_repository_creates_digest_only_session_models() -> None:
    session = MagicMock(spec=AsyncSession)
    session.execute = AsyncMock()
    session.flush = AsyncMock()
    repository = SqlAlchemyIdentityRepository(session)
    session_id = uuid.uuid7()
    token_id = uuid.uuid7()
    token_jti = uuid.uuid7()
    token_digest = hashlib.sha256(b"persisted-digest").digest()

    await repository.create_stable_session(
        session_id=session_id,
        token_id=token_id,
        token_jti=token_jti,
        account_id=42,
        device_id=uuid.uuid7(),
        client_version="b20260729",
        client_variant="cuttingedge",
        ip_address="127.0.0.1",
        user_agent="osu!",
        token_digest=token_digest,
        token_prefix="safe-prefix",
        now=INSTANT,
        expires_at=INSTANT + timedelta(hours=1),
    )

    records = session.add_all.call_args.args[0]
    auth_session = next(record for record in records if isinstance(record, AuthSession))
    token = next(record for record in records if isinstance(record, AuthToken))
    assert auth_session.client_family is ClientFamily.STABLE
    assert auth_session.session_class == "normal"
    assert auth_session.last_activity_at == INSTANT
    assert token.kind is TokenKind.STABLE_SESSION
    assert token.digest == token_digest
    assert token.prefix == "safe-prefix"
    assert token.jti == token_jti
    assert not hasattr(token, "raw_token")
    session.commit.assert_not_called()


@pytest.mark.asyncio
async def test_sqlalchemy_repository_touches_session_monotonically_under_row_lock() -> None:
    row = SimpleNamespace(
        account_id=42,
        current_name="Alice",
        auth_version=1,
        country_code=None,
        session_id=uuid.uuid7(),
        device_id=uuid.uuid7(),
        client_version="b20260729",
        client_variant="cuttingedge",
        opened_at=INSTANT - timedelta(minutes=5),
        last_activity_at=INSTANT - timedelta(seconds=5),
        expires_at=INSTANT + timedelta(hours=1),
    )
    query_result = MagicMock()
    query_result.one_or_none.return_value = row
    session = MagicMock(spec=AsyncSession)
    session.execute = AsyncMock(side_effect=(query_result, MagicMock()))
    repository = SqlAlchemyIdentityRepository(session)

    resolved = await repository.touch_stable_session(hashlib.sha256(b"token").digest(), at=INSTANT)

    assert resolved is not None
    assert resolved.opened_at == row.opened_at
    assert resolved.last_activity_at == INSTANT
    lock_statement = session.execute.await_args_list[0].args[0]
    lock_sql = str(lock_statement.compile(dialect=postgresql.dialect()))
    assert "FOR UPDATE OF auth_sessions" in lock_sql
    update_statement = session.execute.await_args_list[1].args[0]
    assert INSTANT in update_statement.compile().params.values()
    session.commit.assert_not_called()


@pytest.mark.asyncio
async def test_sqlalchemy_repository_upgrades_only_the_observed_legacy_credential() -> None:
    session = MagicMock(spec=AsyncSession)
    session.scalar = AsyncMock(return_value=42)
    repository = SqlAlchemyIdentityRepository(session)

    upgraded = await repository.upgrade_legacy_credential(
        account_id=42,
        expected_verifier="$2b$legacy",
        expected_password_changed_at=INSTANT - timedelta(days=1),
        password_verifier="$argon2id$replacement",
        pepper_version=PEPPER.version,
        password_changed_at=INSTANT,
    )

    statement = session.scalar.await_args.args[0]
    parameters = statement.compile().params
    assert upgraded
    assert str(statement).startswith("UPDATE iam.password_credentials")
    assert {"bcrypt_md5", "$2b$legacy", "$argon2id$replacement", "argon2id"} <= set(parameters.values())
    assert INSTANT in parameters.values()
    session.commit.assert_not_called()


@pytest.mark.asyncio
async def test_sqlalchemy_repository_appends_attempt_and_revoke_advances_auth_version() -> None:
    session = MagicMock(spec=AsyncSession)
    session.execute = AsyncMock()
    session.flush = AsyncMock()
    auth_session = AuthSession(
        id=uuid.uuid7(),
        account_id=42,
        client_family=ClientFamily.STABLE,
        session_class="normal",
        ip_address="127.0.0.1",
        last_activity_at=INSTANT,
        expires_at=INSTANT + timedelta(hours=1),
        created_at=INSTANT,
    )
    session.scalar = AsyncMock(return_value=auth_session)
    repository = SqlAlchemyIdentityRepository(session)
    identifier_hmac = hashlib.sha256(b"identifier").digest()

    await repository.append_auth_attempt(
        account_id=42,
        session_id=None,
        device_id=None,
        identifier_hmac=identifier_hmac,
        ip_address="127.0.0.1",
        client_version="b20260729",
        result="failure",
        failure_reason="invalid_credentials",
        context={},
        now=INSTANT,
    )
    assert (
        await repository.close_stable_session(
            auth_session.id,
            now=INSTANT,
            reason="credential_reset",
            revoke=True,
        )
        == 42
    )

    attempt = session.add.call_args.args[0]
    assert isinstance(attempt, AuthAttempt)
    assert attempt.identifier_hmac == identifier_hmac
    assert attempt.result == "failure"
    assert auth_session.revoked_at == INSTANT
    assert session.execute.await_count == 2
    session.commit.assert_not_called()


class Uuid7Generator:
    def new(self) -> uuid.UUID:
        return uuid.uuid7()


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_postgres_stable_identity_lifecycle_is_digest_only_and_audited(postgres_database_url: str) -> None:
    db_engine = await infra_db.create_engine()
    session_factory = infra_db.create_session_factory(db_engine)
    try:
        account_service = AccountService(
            uow_factory=SqlAlchemyUnitOfWorkFactory(session_factory),
            repository_factory=lambda session: SqlAlchemyAccountRepository(cast(AsyncSession, session)),
            outbox_writer_factory=lambda session: SqlAlchemyOutboxWriter(cast(AsyncSession, session)),
            password_pepper=PEPPER,
            argon2_policy=POLICY,
            clock=FixedClock(),
        )
        account = await account_service.register(
            RegisterAccount(
                meta=_meta(b"identity-account"),
                display_name="Alice",
                email="alice@example.com",
                password_preverification="a" * 32,
                activate_immediately=True,
            )
        )
        stable_tokens = iter(f"postgres-stable-token-secret-{index}" for index in range(10))
        identity_clock = FixedClock()
        identity = IdentityService(
            uow_factory=SqlAlchemyUnitOfWorkFactory(session_factory),
            repository_factory=lambda session: SqlAlchemyIdentityRepository(cast(AsyncSession, session)),
            outbox_writer_factory=lambda session: SqlAlchemyOutboxWriter(cast(AsyncSession, session)),
            password_pepper=PEPPER,
            argon2_policy=POLICY,
            token_hmac_key=TOKEN_KEY,
            device_hmac_key=DEVICE_KEY,
            clock=identity_clock,
            id_generator=Uuid7Generator(),
            stable_session_stale_grace=timedelta(minutes=2),
            token_factory=lambda: next(stable_tokens),
        )

        created = await identity.login_stable(_login(identifier="alice@example.com"))
        resolved = await identity.resolve_stable_session(created.raw_token)
        assert resolved.account_id == account.account_id
        assert resolved.session_id == created.session_id
        assert resolved.opened_at == INSTANT
        assert resolved.last_activity_at == INSTANT
        identity_clock.instant = INSTANT + timedelta(seconds=30)
        touched = await identity.touch_stable_session(created.raw_token)
        assert touched.last_activity_at == identity_clock.instant
        with pytest.raises(StableSessionAlreadyActive):
            await identity.login_stable(_login(identifier=str(account.account_id)))

        async with session_factory() as session:
            token = await session.scalar(select(AuthToken).where(AuthToken.session_id == created.session_id))
            assert token is not None
            assert token.digest == digest_opaque_token(created.raw_token, key=TOKEN_KEY)
            assert token.kind is TokenKind.STABLE_SESSION
            assert await session.scalar(select(func.count()).select_from(Device)) == 1
            assert await session.scalar(select(func.count()).select_from(DeviceIdentifier)) == 2
            assert await session.scalar(select(func.count()).select_from(AccountDevice)) == 1
            assert await session.scalar(select(func.count()).select_from(AuthAttempt)) == 2
            event_types = set(await session.scalars(select(OutboxEvent.event_type)))
            assert "identity.session-opened.v1" in event_types

        await identity.close_stable_session(created.raw_token, reason="login_bootstrap_failed")
        with pytest.raises(InvalidStableSession):
            await identity.resolve_stable_session(created.raw_token)

        second = await identity.login_stable(_login(identifier="Alice"))
        identity_clock.instant += timedelta(seconds=121)
        replacement = await identity.login_stable(_login(identifier="Alice"))
        with pytest.raises(InvalidStableSession):
            await identity.resolve_stable_session(second.raw_token)
        await identity.revoke_stable_session(replacement.session_id, reason="credential_reset")
        async with session_factory() as session:
            account_row = await session.get(Account, account.account_id)
            assert account_row is not None
            assert account_row.status is AccountStatus.ACTIVE
            assert account_row.auth_version == 2
            assert await session.scalar(select(func.count()).select_from(AuthSession)) == 3
            assert await session.scalar(select(func.count()).select_from(AuthToken)) == 3
            assert await session.scalar(select(func.count()).select_from(AuthAttempt)) == 4
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(OutboxEvent)
                    .where(OutboxEvent.event_type == "identity.session-closed.v1")
                )
                == 3
            )
    finally:
        await db_engine.dispose()
