import hashlib
import uuid
from datetime import UTC, datetime
from typing import cast

import httpx
import pytest
from fastapi import FastAPI
from pydantic import SecretStr

from perfcho.api.stable import router
from perfcho.api.stable.dependencies import get_stable_services
from perfcho.infra.glue.stable import StableServices
from perfcho.infra.settings import Settings
from perfcho.modules.account import AccountService, EmailUnavailable, RegisterAccount, RegistrationResult
from perfcho.modules.authorization import AuthorizationQueryService
from perfcho.modules.common import Clock, IdGenerator
from perfcho.modules.identity import IdentityService
from perfcho.modules.realtime import RealtimeRepository

NOW = datetime(2026, 7, 31, 12, 30, tzinfo=UTC)
REQUEST_ID = uuid.UUID("019867b2-1d40-7000-8000-000000000001")


class FixedClock:
    def now(self) -> datetime:
        return NOW


class FixedIdGenerator:
    def new(self) -> uuid.UUID:
        return REQUEST_ID


class FakeAccount:
    def __init__(self) -> None:
        self.checks: list[tuple[str, str]] = []
        self.commands: list[RegisterAccount] = []
        self.email_taken = False

    async def check_availability(self, display_name: str, email_address: str) -> None:
        self.checks.append((display_name, email_address))
        if self.email_taken:
            raise EmailUnavailable("account email is already in use")

    async def register(self, command: RegisterAccount) -> RegistrationResult:
        self.commands.append(command)
        if self.email_taken:
            raise EmailUnavailable("account email is already in use")
        return RegistrationResult(42, command.display_name, command.email, "active")


def registration_app(account: FakeAccount) -> FastAPI:
    services = StableServices(
        identity=cast(IdentityService, object()),
        authorization=cast(AuthorizationQueryService, object()),
        realtime=cast(RealtimeRepository, object()),
        clock=cast(Clock, FixedClock()),
        id_generator=cast(IdGenerator, FixedIdGenerator()),
        settings=Settings(device_hmac_key=SecretStr("registration-test-hmac-key")),
        account=cast(AccountService, account),
    )
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_stable_services] = lambda: services
    return app


def registration_form(*, check: int = 0, password: str = "correct horse") -> dict[str, str]:
    return {
        "user[username]": " Alice ",
        "user[user_email]": " Alice@Example.COM ",
        "user[password]": password,
        "check": str(check),
    }


@pytest.mark.asyncio
async def test_stable_registration_check_validates_availability_without_creating_account() -> None:
    account = FakeAccount()
    app = registration_app(account)

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://c.test") as client:
        response = await client.post("/users", data=registration_form(check=2))

    assert response.status_code == 200
    assert response.content == b"ok"
    assert account.checks == [("Alice", "alice@example.com")]
    assert account.commands == []


@pytest.mark.asyncio
async def test_stable_registration_creates_active_canonical_account_with_retry_stable_receipt() -> None:
    account = FakeAccount()
    app = registration_app(account)

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://c.test") as client:
        first = await client.post("/users", data=registration_form())
        second = await client.post("/users", data=registration_form())

    assert first.content == second.content == b"ok"
    first_command, second_command = account.commands
    expected_token = hashlib.md5(b"correct horse", usedforsecurity=False).hexdigest()
    assert first_command.password_preverification == expected_token
    assert first_command.display_name == "Alice"
    assert first_command.email == "alice@example.com"
    assert first_command.activate_immediately
    assert first_command.meta.actor is None
    assert first_command.meta.client.family == "stable"
    assert first_command.meta.client.ip_address == "127.0.0.1"
    assert first_command.meta.idempotency_key == second_command.meta.idempotency_key
    assert first_command.meta.request_digest == second_command.meta.request_digest
    assert expected_token not in first_command.meta.idempotency_key


@pytest.mark.asyncio
async def test_stable_registration_returns_client_field_errors_without_calling_service() -> None:
    account = FakeAccount()
    app = registration_app(account)

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://c.test") as client:
        invalid_name = await client.post(
            "/users",
            data={**registration_form(), "user[username]": "_"},
        )
        weak_password = await client.post("/users", data=registration_form(password="aaaaaaaa"))

    assert invalid_name.status_code == 400
    assert set(invalid_name.json()["form_error"]["user"]) == {"username"}
    assert weak_password.status_code == 400
    assert weak_password.json() == {"form_error": {"user": {"password": ["Must have more than 3 unique characters."]}}}
    assert account.checks == []
    assert account.commands == []


@pytest.mark.asyncio
@pytest.mark.parametrize("check", [0, 1])
async def test_stable_registration_maps_email_conflicts_to_stable_form_contract(check: int) -> None:
    account = FakeAccount()
    account.email_taken = True
    app = registration_app(account)

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://c.test") as client:
        response = await client.post("/users", data=registration_form(check=check))

    assert response.status_code == 400
    assert response.json() == {"form_error": {"user": {"user_email": ["account email is already in use"]}}}
