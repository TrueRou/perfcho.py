"""Resolve authenticated osu! API v2 request context."""

from typing import Annotated

from fastapi import Depends, Header, HTTPException, status

from perfcho.api.cho.dependencies import StableServicesDependency
from perfcho.modules.identity import AuthenticatedAccount, InvalidAccessToken


async def authenticate_v2_account(
    services: StableServicesDependency,
    authorization: Annotated[str | None, Header()] = None,
) -> AuthenticatedAccount:
    """Authenticate a Lazer access token with the required client scope."""
    if authorization is None or not authorization.startswith("Bearer "):
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            detail="A Bearer access token is required.",
        )
    try:
        account = await services.identity.authenticate_access_token(authorization.removeprefix("Bearer ").strip())
    except InvalidAccessToken as error:
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            detail="The access token is invalid or expired.",
        ) from error
    if "lazer" not in account.scope_codes:
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            detail="The access token does not permit Lazer API access.",
        )
    return account


V2AccountDependency = Annotated[AuthenticatedAccount, Depends(authenticate_v2_account)]
