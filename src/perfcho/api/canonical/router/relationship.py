"""Adapt osu!lazer friends/blocks endpoints onto the social service."""

from typing import Annotated

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from perfcho.api.canonical.dependencies import CanonicalAccountDependency, CanonicalServicesDependency
from perfcho.api.canonical.router._shared import error
from perfcho.modules.social import SocialInteractionBlocked

router = APIRouter()


@router.get("/friends", response_model=None, tags=["Friends"])
async def list_friends(
    services: CanonicalServicesDependency,
    account: CanonicalAccountDependency,
) -> list[dict[str, object]] | JSONResponse:
    """Return the authenticated account's friend relationships."""
    if services.social is None:
        return error(503, "service_unavailable", "Social service is unavailable.")
    friends = await services.social.list_friends(account.account_id)
    return [_relation(friend.account_id, friend.display_name, friend.mutual) for friend in friends]


@router.get("/blocks", response_model=None, tags=["Blocks"])
async def list_blocks(
    services: CanonicalServicesDependency,
    account: CanonicalAccountDependency,
) -> list[dict[str, object]] | JSONResponse:
    """Return the authenticated account's block relationships."""
    if services.social is None:
        return error(503, "service_unavailable", "Social service is unavailable.")
    blocks = await services.social.list_blocks(account.account_id)
    return [_relation(block.account_id, block.display_name, False) for block in blocks]


@router.post("/friends", response_model=None, tags=["Friends"])
async def add_friend(
    services: CanonicalServicesDependency,
    account: CanonicalAccountDependency,
    target: Annotated[int, Query(gt=0)],
) -> JSONResponse:
    """Follow the target account."""
    if services.social is None:
        return error(503, "service_unavailable", "Social service is unavailable.")
    try:
        await services.social.follow(account.account_id, target)
    except SocialInteractionBlocked as exc:
        return error(403, "forbidden", str(exc))
    return _no_content()


@router.delete("/friends/{target}", response_model=None, tags=["Friends"])
async def remove_friend(
    target: int,
    services: CanonicalServicesDependency,
    account: CanonicalAccountDependency,
) -> JSONResponse:
    """Unfollow the target account."""
    if services.social is None:
        return error(503, "service_unavailable", "Social service is unavailable.")
    await services.social.unfollow(account.account_id, target)
    return _no_content()


@router.post("/blocks", response_model=None, tags=["Blocks"])
async def block_user(
    services: CanonicalServicesDependency,
    account: CanonicalAccountDependency,
    target: Annotated[int, Query(gt=0)],
) -> JSONResponse:
    """Block the target account."""
    if services.social is None:
        return error(503, "service_unavailable", "Social service is unavailable.")
    await services.social.block(account.account_id, target)
    return _no_content()


@router.delete("/blocks/{target}", response_model=None, tags=["Blocks"])
async def unblock_user(
    target: int,
    services: CanonicalServicesDependency,
    account: CanonicalAccountDependency,
) -> JSONResponse:
    """Unblock the target account."""
    if services.social is None:
        return error(503, "service_unavailable", "Social service is unavailable.")
    await services.social.unblock(account.account_id, target)
    return _no_content()


def _relation(target_id: int, username: str, mutual: bool) -> dict[str, object]:
    return {
        "target_id": target_id,
        "relation_type": "friend",
        "mutual": mutual,
        "target": {"id": target_id, "username": username},
    }


def _no_content() -> JSONResponse:
    return JSONResponse(status_code=204, content=None)
