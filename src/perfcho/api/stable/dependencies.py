"""Resolve process-owned resources into Stable application services."""

from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import Depends, Request

from perfcho.composition import StableServices, compose_stable_services


async def get_stable_services(request: Request) -> AsyncIterator[StableServices]:
    """Compose request-scoped Stable services from lifespan state."""
    async with compose_stable_services(
        request.state.db_session_factory,
        request.state.redis_engine,
    ) as services:
        yield services


StableServicesDependency = Annotated[StableServices, Depends(get_stable_services)]
