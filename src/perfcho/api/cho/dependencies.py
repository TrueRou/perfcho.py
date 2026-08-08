"""Resolve process-owned Stable application services."""

from typing import Annotated

from fastapi import Depends, Request

from perfcho.infra.compose import StableServices


async def get_stable_services(request: Request) -> StableServices:
    """Return the process-owned Stable composition from lifespan state."""
    return request.state.stable_services


StableServicesDependency = Annotated[StableServices, Depends(get_stable_services)]
