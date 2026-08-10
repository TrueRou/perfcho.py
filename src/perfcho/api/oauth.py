"""Expose OAuth token exchange and the minimum authenticated API surface."""

import hashlib
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Annotated, Literal

from fastapi import APIRouter, Form, Header, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from perfcho.api.cho.canonize.ipaddr import resolve_client_ip
from perfcho.api.cho.dependencies import StableServicesDependency
from perfcho.modules.identity import (
    InvalidAccessToken,
    InvalidOAuthClient,
    InvalidOAuthGrant,
    PasswordGrant,
    RefreshGrant,
)
from perfcho.modules.scoring import AccountStatsView, Ruleset, ScoreboardVariant

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
    services: StableServicesDependency,
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
                    password_token=hashlib.md5(password.encode(), usedforsecurity=False).hexdigest(),
                    client_key=client_id,
                    client_secret=client_secret,
                    requested_scope=scope,
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


@router.get("/api/v2/me", response_model=None)
@router.get("/api/v2/me/{ruleset}", response_model=None)
async def me(
    services: StableServicesDependency,
    authorization: Annotated[str | None, Header()] = None,
    ruleset: str | None = None,
) -> dict[str, object] | JSONResponse:
    """Return the compact authenticated account shape needed to finish client login."""
    if authorization is None or not authorization.startswith("Bearer "):
        return _oauth_error("unauthenticated", "A Bearer access token is required.", status_code=401)
    try:
        account = await services.identity.authenticate_access_token(authorization.removeprefix("Bearer ").strip())
    except InvalidAccessToken:
        return _oauth_error("unauthenticated", "The access token is invalid or expired.", status_code=401)

    selected = _parse_ruleset(ruleset)
    if ruleset is not None and selected is None:
        return _oauth_error("invalid_request", "The requested ruleset is invalid.", status_code=422)
    statistics_rulesets = await _statistics_rulesets(services, account.account_id)
    response = _user_response(
        services,
        account.account_id,
        account.current_name,
        account.account_type,
        account.country_code,
        account.registered_at,
        account.last_seen_at,
        selected or Ruleset.OSU,
        statistics_rulesets,
    )
    if ruleset is not None:
        response["statistics"] = statistics_rulesets[selected.value] if selected is not None else None
    return response


@router.get("/api/v2/users/{lookup}/{ruleset}", response_model=None)
async def user(
    lookup: str,
    ruleset: str,
    services: StableServicesDependency,
    key: Literal["id", "username"] = "id",
) -> dict[str, object] | JSONResponse:
    """Return a public account and selected ruleset statistics."""
    selected = _parse_ruleset(ruleset)
    if selected is None:
        return _oauth_error("invalid_request", "The requested ruleset is invalid.", status_code=422)
    if services.account is None:
        return _oauth_error("service_unavailable", "Account queries are unavailable.", status_code=503)
    account = await services.account.get_public(lookup, key=key)
    if account is None:
        return _oauth_error("not_found", "The requested user does not exist.", status_code=404)
    statistics_rulesets = await _statistics_rulesets(services, account.account_id)
    response = _user_response(
        services,
        account.account_id,
        account.current_name,
        account.account_type,
        account.country_code,
        account.registered_at,
        account.last_seen_at,
        account.default_ruleset,
        statistics_rulesets,
    )
    response["statistics"] = statistics_rulesets[selected.value]
    return response


async def _statistics_rulesets(
    services: object,
    account_id: int,
) -> dict[str, dict[str, object]]:
    statistics = getattr(services, "account_statistics", None)
    result: dict[str, dict[str, object]] = {}
    for ruleset in Ruleset:
        view = (
            await statistics.get_for_display(account_id, ruleset, ScoreboardVariant.VANILLA)
            if statistics is not None
            else AccountStatsView(0, Decimal(0), 0, 0, None)
        )
        result[ruleset.value] = _statistics_response(view)
    return result


def _statistics_response(stats: AccountStatsView) -> dict[str, object]:
    return {
        "count_100": 0,
        "count_300": 0,
        "count_50": 0,
        "count_miss": 0,
        "level": {"current": 0, "progress": 0},
        "global_rank": stats.global_rank or None,
        "country_rank": stats.country_rank or None,
        "pp": stats.performance,
        "ranked_score": stats.ranked_score,
        "hit_accuracy": float(stats.accuracy * 100),
        "play_count": stats.play_count,
        "play_time": stats.play_time_ms // 1000,
        "total_score": stats.total_score,
        "total_hits": stats.total_hits,
        "maximum_combo": stats.maximum_combo,
        "replays_watched_by_others": stats.replay_views,
        "grade_counts": {
            "ssh": stats.grade_counts["XH"],
            "ss": stats.grade_counts["X"],
            "sh": stats.grade_counts["SH"],
            "s": stats.grade_counts["S"],
            "a": stats.grade_counts["A"],
        },
        "is_ranked": bool(stats.global_rank),
        "global_rank_percent": None,
        "variants": None,
    }


def _user_response(
    services: object,
    account_id: int,
    username: str,
    account_type: str,
    country_code: str | None,
    registered_at: datetime,
    last_seen_at: datetime | None,
    default_ruleset: Ruleset,
    statistics_rulesets: dict[str, dict[str, object]],
) -> dict[str, object]:
    avatar_base = services.settings.stable_avatar_base_url.rstrip("/")
    registered = registered_at.isoformat()
    last_seen = last_seen_at.isoformat() if last_seen_at else registered
    return {
        "id": account_id,
        "username": username,
        "join_date": registered,
        "country_code": (country_code or "XX").upper(),
        "avatar_url": f"{avatar_base}/{account_id}",
        "is_active": True,
        "is_bot": account_type == "bot",
        "is_online": True,
        "is_supporter": False,
        "support_level": 0,
        "last_visit": last_seen,
        "pm_friends_only": False,
        "playmode": default_ruleset.value,
        "previous_usernames": [],
        "profile_order": [],
        "statistics_rulesets": statistics_rulesets,
        "groups": [],
        "session_verification_method": None,
        "score_processing_notice_url": "",
    }


def _parse_ruleset(value: str | None) -> Ruleset | None:
    if value is None:
        return None
    try:
        return Ruleset(value)
    except ValueError:
        return None
