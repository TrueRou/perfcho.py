"""Adapt osu!lazer user profile endpoints onto canonical account services."""

from typing import Annotated

from fastapi import APIRouter, Header, Query
from fastapi.responses import JSONResponse

from perfcho.api.canonical.dependencies import CanonicalAccountDependency, CanonicalServicesDependency
from perfcho.api.canonical.router._shared import error, parse_ruleset, statistics_rulesets, user_response
from perfcho.modules.account import PublicAccountView
from perfcho.modules.identity import InvalidAccessToken
from perfcho.modules.scoring import Ruleset

router = APIRouter()


@router.get("/me", response_model=None, tags=["Users"])
@router.get("/me/{ruleset}", response_model=None, tags=["Users"])
async def me(
    services: CanonicalServicesDependency,
    authorization: Annotated[str | None, Header()] = None,
    ruleset: str | None = None,
) -> dict[str, object] | JSONResponse:
    """Return the authenticated account shape needed to finish client login."""
    if authorization is None or not authorization.startswith("Bearer "):
        return error(401, "unauthenticated", "A Bearer access token is required.")
    try:
        account = await services.identity.authenticate_access_token(authorization.removeprefix("Bearer ").strip())
    except InvalidAccessToken:
        return error(401, "unauthenticated", "The access token is invalid or expired.")
    selected = parse_ruleset(ruleset)
    if ruleset is not None and selected is None:
        return error(422, "invalid_request", "The requested ruleset is invalid.")
    statistics = await statistics_rulesets(services, account.account_id)
    response = user_response(
        services,
        account.account_id,
        account.current_name,
        account.account_type,
        account.country_code,
        account.registered_at,
        account.last_seen_at,
        selected or Ruleset.OSU,
        statistics,
    )
    if ruleset is not None:
        response["statistics"] = statistics[selected.value] if selected is not None else None
    return response


@router.get("/users/{lookup}", response_model=None, tags=["Users"])
@router.get("/users/{lookup}/{ruleset}", response_model=None, tags=["Users"])
async def get_user(
    lookup: str,
    services: CanonicalServicesDependency,
    ruleset: str | None = None,
    key: Annotated[str, Query(pattern="^(id|username)$")] = "id",
) -> dict[str, object] | JSONResponse:
    """Return a public account and optional per-ruleset statistics."""
    selected = parse_ruleset(ruleset)
    if ruleset is not None and selected is None:
        return error(422, "invalid_request", "The requested ruleset is invalid.")
    account = await _public_account(services, lookup, key)
    if account is None:
        return error(404, "not_found", "The requested user does not exist.")
    statistics = await statistics_rulesets(services, account.account_id)
    response = user_response(
        services,
        account.account_id,
        account.current_name,
        account.account_type,
        account.country_code,
        account.registered_at,
        account.last_seen_at,
        account.default_ruleset,
        statistics,
    )
    if selected is not None:
        response["statistics"] = statistics[selected.value]
    return response


@router.get("/users", response_model=None, tags=["Users"])
@router.get("/users/lookup", response_model=None, tags=["Users"])
async def get_users(
    services: CanonicalServicesDependency,
    ids: Annotated[list[int] | None, Query(alias="ids[]")] = None,
) -> dict[str, object]:
    """Return a batch of public accounts by ID."""
    if not ids:
        return {"users": []}
    users: list[dict[str, object]] = []
    for account_id in dict.fromkeys(ids):
        account = await _public_account(services, str(account_id), "id")
        if account is None:
            continue
        statistics = await statistics_rulesets(services, account.account_id)
        users.append(
            user_response(
                services,
                account.account_id,
                account.current_name,
                account.account_type,
                account.country_code,
                account.registered_at,
                account.last_seen_at,
                account.default_ruleset,
                statistics,
            )
        )
    return {"users": users}


@router.get("/users/{lookup}/scores/{score_type}", response_model=None, tags=["Users"])
async def get_user_scores(
    lookup: str,
    score_type: str,
    services: CanonicalServicesDependency,
    account: CanonicalAccountDependency,
) -> dict[str, object]:
    """Return a user's best, recent, or pinned scores.

    Delegated to the scoring milestone (M2); returns a valid empty page for now.
    """
    del lookup, services, account
    if score_type not in {"best", "recent", "pinned"}:
        return {"scores": []}
    return {"scores": []}


async def _public_account(
    services: CanonicalServicesDependency,
    lookup: str,
    key: str,
) -> PublicAccountView | None:
    if services.account is None:
        return None
    return await services.account.get_public(lookup, key=key)

