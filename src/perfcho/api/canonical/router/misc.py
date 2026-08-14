"""Adapt osu!lazer misc content endpoints (tags, seasonal, news, wiki)."""

from typing import Annotated

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from perfcho.api.canonical.dependencies import CanonicalAccountDependency, CanonicalServicesDependency
from perfcho.api.canonical.router._shared import error

router = APIRouter()


@router.get("/seasonal-backgrounds", response_model=None, tags=["Misc"])
async def seasonal_backgrounds(services: CanonicalServicesDependency) -> dict[str, object]:
    """Return seasonal backgrounds (no configured source; empty list)."""
    del services
    return {"backgrounds": []}


@router.get("/tags", response_model=None, tags=["Tags"])
async def list_tags(services: CanonicalServicesDependency) -> dict[str, object]:
    """Return available beatmap tags (durable tag projection lands later)."""
    del services
    return {"tags": []}


@router.put("/beatmaps/{beatmap_id}/tags/{tag_id}", response_model=None, tags=["Tags"])
async def add_tag(
    beatmap_id: int,
    tag_id: int,
    services: CanonicalServicesDependency,
    account: CanonicalAccountDependency,
) -> JSONResponse:
    """Vote for a beatmap tag (durable projection lands later)."""
    del beatmap_id, tag_id, services, account
    return JSONResponse(status_code=204, content=None)


@router.delete("/beatmaps/{beatmap_id}/tags/{tag_id}", response_model=None, tags=["Tags"])
async def remove_tag(
    beatmap_id: int,
    tag_id: int,
    services: CanonicalServicesDependency,
    account: CanonicalAccountDependency,
) -> JSONResponse:
    """Remove a beatmap tag vote (durable projection lands later)."""
    del beatmap_id, tag_id, services, account
    return JSONResponse(status_code=204, content=None)


@router.get("/news", response_model=None, tags=["Misc"])
async def news(
    services: CanonicalServicesDependency,
    limit: Annotated[int, Query(ge=1, le=21)] = 12,
) -> dict[str, object]:
    """Return news (no configured source; empty feed)."""
    del services, limit
    return {"news_posts": [], "news_sidebar": {"current_year": 0, "years": [], "posts": []}}


@router.get("/changelog", response_model=None, tags=["Misc"])
async def changelog(services: CanonicalServicesDependency) -> dict[str, object]:
    """Return the changelog (no configured source; empty index)."""
    del services
    return {"builds": [], "streams": []}


@router.get("/wiki/{lang}/{page}", response_model=None, tags=["Misc"])
async def wiki(lang: str, page: str, services: CanonicalServicesDependency) -> JSONResponse:
    """Return a wiki page (no configured source)."""
    del lang, page, services
    return error(404, "not_found", "Wiki page was not found.")


@router.get("/spotlights", response_model=None, tags=["Misc"])
async def spotlights(services: CanonicalServicesDependency) -> dict[str, object]:
    """Return spotlights (no configured source; empty list)."""
    del services
    return {"spotlights": []}


@router.get("/search", response_model=None, tags=["Misc"])
async def search(
    services: CanonicalServicesDependency,
    mode: Annotated[str, Query()] = "all",
    query: Annotated[str, Query()] = "",
) -> dict[str, object]:
    """Search users and beatmaps (delegated to content/social)."""
    del services, mode, query
    return {"user": {"data": [], "total": 0}, "beatmapset": {"data": [], "total": 0}}
