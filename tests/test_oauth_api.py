from datetime import UTC, datetime
from typing import cast

import httpx
import pytest
from fastapi import FastAPI

from perfcho.api import router
from perfcho.api.canonical.dependencies import get_canonical_services
from perfcho.infra.compose import StableServices
from perfcho.infra.settings import Settings
from perfcho.modules.authorization import AuthorizationQueryService
from perfcho.modules.common import Clock, IdGenerator
from perfcho.modules.identity import (
    AuthenticatedAccount,
    IdentityService,
    InvalidOAuthClient,
    InvalidOAuthGrant,
    OAuthTokenResult,
    PasswordGrant,
    RefreshGrant,
)
from perfcho.modules.realtime import RealtimeStateRepository

NOW = datetime(2026, 8, 10, 12, tzinfo=UTC)


class FakeIdentity:
    def __init__(self) -> None:
        self.password_grants: list[PasswordGrant] = []
        self.refresh_grants: list[RefreshGrant] = []
        self.access_tokens: list[str] = []
        self.error: Exception | None = None

    async def exchange_password(self, command: PasswordGrant) -> OAuthTokenResult:
        self.password_grants.append(command)
        if self.error:
            raise self.error
        return OAuthTokenResult("access-value", "refresh-value", 3600)

    async def exchange_refresh(self, command: RefreshGrant) -> OAuthTokenResult:
        self.refresh_grants.append(command)
        if self.error:
            raise self.error
        return OAuthTokenResult("access-next", "refresh-next", 3600)

    async def authenticate_access_token(self, token: str) -> AuthenticatedAccount:
        self.access_tokens.append(token)
        return AuthenticatedAccount(
            account_id=42,
            current_name="Alice",
            account_type="user",
            country_code="cn",
            registered_at=NOW,
            last_seen_at=NOW,
            session_id=__import__("uuid").uuid7(),
            scope_codes=("public", "identify", "lazer"),
        )


def oauth_app(identity: FakeIdentity) -> FastAPI:
    services = StableServices(
        identity=cast(IdentityService, identity),
        authorization=cast(AuthorizationQueryService, object()),
        realtime=cast(RealtimeStateRepository, object()),
        clock=cast(Clock, object()),
        id_generator=cast(IdGenerator, object()),
        settings=Settings(
            argon2_time_cost=1,
            argon2_memory_cost_kib=8,
            argon2_parallelism=1,
        ),
    )
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_canonical_services] = lambda: services
    return app


@pytest.mark.asyncio
async def test_password_grant_matches_lazer_form_and_returns_token_contract() -> None:
    identity = FakeIdentity()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=oauth_app(identity)),
        base_url="http://api.test",
    ) as client:
        response = await client.post(
            "/oauth/token",
            headers={"x-api-version": "20260810", "user-agent": "osu!"},
            data={
                "grant_type": "password",
                "client_id": "5",
                "client_secret": "client-secret",
                "scope": "*",
                "username": "Alice",
                "password": "correct horse",
            },
        )

    assert response.status_code == 200
    assert response.json() == {
        "access_token": "access-value",
        "token_type": "Bearer",
        "expires_in": 3600,
        "refresh_token": "refresh-value",
        "scope": "*",
    }
    grant = identity.password_grants[0]
    assert grant.identifier == "Alice"
    assert grant.client_key == "5"
    assert grant.client_version == "20260810"
    assert grant.ip_address == "127.0.0.1"
    assert grant.password_preverification == "3cb4e732631f47e6eb961f34554b7cde"


@pytest.mark.asyncio
async def test_refresh_grant_rotates_using_same_client_credentials() -> None:
    identity = FakeIdentity()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=oauth_app(identity)),
        base_url="http://api.test",
    ) as client:
        response = await client.post(
            "/oauth/token",
            data={
                "grant_type": "refresh_token",
                "client_id": "5",
                "client_secret": "client-secret",
                "scope": "*",
                "refresh_token": "refresh-value",
            },
        )

    assert response.status_code == 200
    assert response.json()["refresh_token"] == "refresh-next"
    assert identity.refresh_grants[0].refresh_token == "refresh-value"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error", "status_code", "identifier"),
    (
        (InvalidOAuthClient(), 401, "invalid_client"),
        (InvalidOAuthGrant(), 400, "invalid_grant"),
    ),
)
async def test_token_errors_use_oauth_shape(error: Exception, status_code: int, identifier: str) -> None:
    identity = FakeIdentity()
    identity.error = error
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=oauth_app(identity)),
        base_url="http://api.test",
    ) as client:
        response = await client.post(
            "/oauth/token",
            data={
                "grant_type": "password",
                "client_id": "5",
                "client_secret": "wrong",
                "scope": "*",
                "username": "Alice",
                "password": "wrong",
            },
        )

    assert response.status_code == status_code
    assert response.json()["error"] == identifier


@pytest.mark.asyncio
async def test_me_uses_bearer_token_and_returns_login_bootstrap_identity() -> None:
    identity = FakeIdentity()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=oauth_app(identity)),
        base_url="http://api.test",
    ) as client:
        response = await client.get("/api/v2/me", headers={"Authorization": "Bearer access-value"})

    assert response.status_code == 200
    assert response.json()["id"] == 42
    assert response.json()["username"] == "Alice"
    assert response.json()["country_code"] == "CN"
    assert response.json()["session_verification_method"] is None
    assert identity.access_tokens == ["access-value"]
