"""Expose the OAuth token exchange endpoint expected by osu!lazer."""

import hashlib
from datetime import timedelta
from typing import Annotated, Literal

from fastapi import APIRouter, Form, Header, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from perfcho.api.canonical.dependencies import CanonicalServicesDependency
from perfcho.api.stable.canonize.ipaddr import resolve_client_ip
from perfcho.modules.identity import (
    InvalidOAuthClient,
    InvalidOAuthGrant,
    PasswordGrant,
    RefreshGrant,
)

router = APIRouter()


class TokenResponse(BaseModel):
    """Serialize the OAuth token contract expected by osu!lazer."""

    access_token: str
    token_type: str
    expires_in: int
    refresh_token: str
    scope: str


def _oauth_error(error: str, hint: str, *, status_code: int = 400) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={"error": error, "message": hint, "hint": hint},
        headers={"Cache-Control": "no-store", "Pragma": "no-cache"},
    )


@router.post("/oauth/token", response_model=TokenResponse)
async def token(
    request: Request,
    services: CanonicalServicesDependency,
    grant_type: Annotated[Literal["password", "refresh_token"], Form()],
    client_id: Annotated[str, Form()],
    client_secret: Annotated[str, Form()],
    scope: Annotated[str, Form()] = "*",
    username: Annotated[str | None, Form()] = None,
    password: Annotated[str | None, Form()] = None,
    refresh_token: Annotated[str | None, Form()] = None,
    x_api_version: Annotated[str | None, Header(alias="x-api-version")] = None,
) -> TokenResponse | JSONResponse:
    """Exchange password or refresh credentials for an opaque token pair."""
    settings = services.settings
    try:
        if grant_type == "password":
            if not username or not password:
                return _oauth_error("invalid_request", "Username and password are required.")
            result = await services.identity.exchange_password(
                PasswordGrant(
                    identifier=username,
                    password_preverification=hashlib.md5(password.encode(), usedforsecurity=False).hexdigest(),
                    client_key=client_id,
                    client_secret=client_secret,
                    requested_scope=scope,
                    client_family="lazer",
                    client_version=x_api_version,
                    ip_address=resolve_client_ip(request, settings.trusted_proxy_cidrs),
                    user_agent=request.headers.get("user-agent"),
                    session_lifetime=timedelta(seconds=settings.oauth_session_lifetime_seconds),
                    access_token_lifetime=timedelta(seconds=settings.oauth_access_token_lifetime_seconds),
                    refresh_token_lifetime=timedelta(seconds=settings.oauth_refresh_token_lifetime_seconds),
                )
            )
        else:
            if not refresh_token:
                return _oauth_error("invalid_request", "Refresh token is required.")
            result = await services.identity.exchange_refresh(
                RefreshGrant(
                    refresh_token=refresh_token,
                    client_key=client_id,
                    client_secret=client_secret,
                    requested_scope=scope,
                    access_token_lifetime=timedelta(seconds=settings.oauth_access_token_lifetime_seconds),
                    refresh_token_lifetime=timedelta(seconds=settings.oauth_refresh_token_lifetime_seconds),
                )
            )
    except InvalidOAuthClient:
        return _oauth_error("invalid_client", "Client authentication failed.", status_code=401)
    except InvalidOAuthGrant:
        return _oauth_error("invalid_grant", "The supplied authorization grant is invalid.")
    except ValueError:
        return _oauth_error("invalid_scope", "Only the wildcard scope is supported for osu!lazer login.")

    return TokenResponse(
        access_token=result.access_token,
        token_type=result.token_type,
        expires_in=result.expires_in,
        refresh_token=result.refresh_token,
        scope=result.scope,
    )
