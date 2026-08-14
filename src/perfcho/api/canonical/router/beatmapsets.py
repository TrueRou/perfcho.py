"""Adapt osu!lazer beatmapset endpoints onto the content service."""

from typing import Annotated

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from perfcho.api.canonical.dependencies import CanonicalAccountDependency, CanonicalServicesDependency
from perfcho.api.canonical.router._shared import error
from perfcho.modules.content import (
    BeatmapRevisionView,
    BeatmapsetNotFound,
    BeatmapsetView,
    ContentInputRejected,
    ContentSearch,
    ContentSearchPage,
)

router = APIRouter()


@router.get("/beatmapsets/lookup", response_model=None, tags=["Beatmapsets"])
async def lookup_beatmapset(
    services: CanonicalServicesDependency,
    beatmap_id: Annotated[int | None, Query(gt=0)] = None,
    beatmapset_id: Annotated[int | None, Query(gt=0)] = None,
) -> dict[str, object] | JSONResponse:
    """Look up a beatmapset by beatmap or beatmapset ID."""
    if services.content_query is None:
        return error(503, "service_unavailable", "Content service is unavailable.")
    try:
        if beatmap_id is not None:
            beatmap = await services.content_query.lookup_beatmap(beatmap_id, external=True)
            beatmapset = await services.content_query.get_beatmapset(beatmap.external_beatmapset_id, external=True)
        elif beatmapset_id is not None:
            beatmapset = await services.content_query.get_beatmapset(beatmapset_id, external=True)
        else:
            return error(422, "invalid_request", "A beatmap_id or beatmapset_id is required.")
    except BeatmapsetNotFound:
        return error(404, "not_found", "Beatmapset was not found.")
    return _beatmapset(beatmapset)


@router.get("/beatmapsets/search", response_model=None, tags=["Beatmapsets"])
async def search_beatmapsets(
    services: CanonicalServicesDependency,
    query: Annotated[str, Query(max_length=255)] = "",
    mode: Annotated[str | None, Query()] = None,
    status: Annotated[str | None, Query()] = None,
    page: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
) -> dict[str, object] | JSONResponse:
    """Search beatmapsets locally."""
    if services.content_query is None:
        return error(503, "service_unavailable", "Content service is unavailable.")
    statuses = tuple(_status_aliases(status)) if status else ()
    try:
        result = await services.content_query.search(
            ContentSearch(query=query, ruleset=mode, statuses=statuses, page=page, page_size=limit)
        )
    except ContentInputRejected as exc:
        return error(422, "invalid_request", str(exc))
    return _search_page(result)


@router.get("/beatmapsets/{beatmapset_id}", response_model=None, tags=["Beatmapsets"])
async def get_beatmapset(
    beatmapset_id: int,
    services: CanonicalServicesDependency,
) -> dict[str, object] | JSONResponse:
    """Return one beatmapset by ID."""
    if services.content_query is None:
        return error(503, "service_unavailable", "Content service is unavailable.")
    try:
        beatmapset = await services.content_query.get_beatmapset(beatmapset_id, external=True)
    except BeatmapsetNotFound:
        return error(404, "not_found", "Beatmapset was not found.")
    return _beatmapset(beatmapset)


@router.post("/beatmapsets/{beatmapset_id}/favourites", response_model=None, tags=["Beatmapsets"])
async def toggle_favourite(
    beatmapset_id: int,
    services: CanonicalServicesDependency,
    account: CanonicalAccountDependency,
    action: Annotated[str, Query(pattern="^(favourite|unfavourite)$")] = "favourite",
) -> dict[str, object] | JSONResponse:
    """Favourite or unfavourite a beatmapset."""
    if services.content is None:
        return error(503, "service_unavailable", "Content service is unavailable.")
    result = await services.content.set_favourite(
        account.account_id, beatmapset_id, favourited=action == "favourite"
    )
    return {"favourite_count": 1, "has_favourited": result.favourited}


@router.post("/beatmapsets/{beatmapset_id}/ratings", response_model=None, tags=["Beatmapsets"])
async def rate_beatmapset(
    beatmapset_id: int,
    services: CanonicalServicesDependency,
    account: CanonicalAccountDependency,
    score: Annotated[int, Query(ge=1, le=10)],
) -> dict[str, object] | JSONResponse:
    """Rate a beatmapset (applied to its canonical set's difficulty)."""
    del beatmapset_id, services, account, score
    return error(501, "not_implemented", "Beatmapset ratings are not yet available.")


@router.get("/me/beatmapset-favourites", response_model=None, tags=["Beatmapsets"])
async def my_favourites(
    services: CanonicalServicesDependency,
    account: CanonicalAccountDependency,
) -> dict[str, object] | JSONResponse:
    """Return the authenticated account's favourited beatmapset IDs."""
    if services.content_query is None:
        return error(503, "service_unavailable", "Content service is unavailable.")
    ids = await services.content_query.list_favourites(account.account_id)
    return {"beatmapsets": list(ids)}


def _beatmapset(beatmapset: BeatmapsetView) -> dict[str, object]:
    return {
        "id": beatmapset.external_beatmapset_id,
        "artist": beatmapset.artist,
        "artist_unicode": beatmapset.artist,
        "title": beatmapset.title,
        "title_unicode": beatmapset.title,
        "creator": beatmapset.creator,
        "status": beatmapset.status,
        "availability": {"download_disabled": not beatmapset.available, "more_information": None},
        "has_favourited": False,
        "has_video": beatmapset.has_video,
        "beatmaps": [_beatmap_summary(b) for b in beatmapset.beatmaps],
    }


def _beatmap_summary(beatmap: BeatmapRevisionView) -> dict[str, object]:
    return {
        "id": beatmap.external_beatmap_id,
        "beatmapset_id": beatmap.external_beatmapset_id,
        "mode": _ruleset_id(beatmap.ruleset),
        "mode_int": _ruleset_id(beatmap.ruleset),
        "difficulty_rating": float(beatmap.star_rating) if beatmap.star_rating is not None else 0.0,
        "version": beatmap.difficulty_name,
        "total_length": beatmap.total_length_ms // 1000,
        "hit_length": beatmap.drain_length_ms // 1000,
        "bpm": float(beatmap.bpm),
        "cs": float(beatmap.circle_size),
        "drain": float(beatmap.health_drain),
        "accuracy": float(beatmap.overall_difficulty),
        "ar": float(beatmap.approach_rate),
        "count_total": beatmap.object_count,
        "max_combo": beatmap.max_combo,
        "status": beatmap.status,
        "checksum": beatmap.md5_hex,
        "last_updated": beatmap.source_updated_at.isoformat(),
    }


def _search_page(page: ContentSearchPage) -> dict[str, object]:
    return {
        "beatmapsets": [_beatmapset(item) for item in page.items],
        "cursor": None,
        "has_more": page.has_more,
        "total": len(page.items),
    }


def _status_aliases(status: str) -> tuple[str, ...]:
    if status == "any":
        return ()
    return (status,)


def _ruleset_id(ruleset: str) -> int:
    return {"osu": 0, "taiko": 1, "fruits": 2, "mania": 3}.get(ruleset, 0)
