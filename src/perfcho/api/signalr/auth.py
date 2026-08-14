"""Resolve the authenticated account for a lazer SignalR connection."""

from typing import TYPE_CHECKING

from perfcho.modules.identity import AuthenticatedAccount, InvalidAccessToken

if TYPE_CHECKING:
    from perfcho.infra.compose import StableServices


def bearer_token(headers: dict[str, str]) -> str | None:
    """Extract a Bearer token from SignalR connection headers, if present."""
    for name, value in headers.items():
        if name.lower() == "authorization":
            if value.startswith("Bearer "):
                return value.removeprefix("Bearer ").strip()
            return None
    return None


async def authenticate(services: StableServices, headers: dict[str, str]) -> AuthenticatedAccount:
    """Authenticate a connection, raising :class:`InvalidAccessToken` on failure."""
    token = bearer_token(headers)
    if token is None:
        raise InvalidAccessToken("a Bearer access token is required")
    return await services.identity.authenticate_access_token(token)
