"""Resolve Canonical API services and authenticated request context."""

from typing import Annotated

from fastapi import Depends, Header, HTTPException, Request, status

from perfcho.infra.compose import StableServices
from perfcho.modules.identity import AuthenticatedAccount, InvalidAccessToken


async def get_canonical_services(request: Request) -> StableServices:
    """Return the process-owned services used by the Canonical adapter."""
    return request.state.stable_services


CanonicalServicesDependency = Annotated[StableServices, Depends(get_canonical_services)]


async def authenticate_canonical_account(
    services: CanonicalServicesDependency,
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


CanonicalAccountDependency = Annotated[AuthenticatedAccount, Depends(authenticate_canonical_account)]
