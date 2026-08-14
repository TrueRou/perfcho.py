"""Adapt osu!lazer beatmap endpoints onto the content query service."""

from typing import Annotated

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from perfcho.api.canonical.dependencies import CanonicalServicesDependency
from perfcho.api.canonical.router._shared import error
from perfcho.modules.content import BeatmapNotFound, BeatmapRevisionView, BeatmapsetNotFound

router = APIRouter()

_STATUS_IDS = {
    "graveyard": -2,
    "wip": -1,
    "pending": 0,
    "ranked": 1,
    "approved": 2,
    "qualified": 3,
    "loved": 4,
}


@router.get("/beatmaps/lookup", response_model=None, tags=["Beatmaps"])
async def lookup_beatmap(
    services: CanonicalServicesDependency,
    id: Annotated[int | None, Query(gt=0)] = None,
    checksum: Annotated[str | None, Query(min_length=32, max_length=32)] = None,
    filename: Annotated[str | None, Query()] = None,
) -> dict[str, object] | JSONResponse:
    """Look up one beatmap by ID, MD5, or filename."""
    if services.content_query is None:
        return error(503, "service_unavailable", "Content service is unavailable.")
    try:
        if id is not None:
            beatmap = await services.content_query.lookup_beatmap(id, external=True)
        elif checksum is not None:
            beatmap = await services.content_query.lookup_md5(checksum)
        elif filename is not None:
            beatmap = await services.content_query.lookup_filename(filename)
        else:
            return error(422, "invalid_request", "One of id, checksum, or filename is required.")
    except BeatmapNotFound:
        return error(404, "not_found", "Beatmap was not found.")
    return _beatmap(beatmap)


@router.get("/beatmaps/{beatmap_id}", response_model=None, tags=["Beatmaps"])
async def get_beatmap(
    beatmap_id: int,
    services: CanonicalServicesDependency,
) -> dict[str, object] | JSONResponse:
    """Return one beatmap by ID."""
    if services.content_query is None:
        return error(503, "service_unavailable", "Content service is unavailable.")
    try:
        beatmap = await services.content_query.lookup_beatmap(beatmap_id, external=True)
    except BeatmapNotFound:
        return error(404, "not_found", "Beatmap was not found.")
    return _beatmap(beatmap)


@router.get("/beatmaps", response_model=None, tags=["Beatmaps"])
async def get_beatmaps(
    services: CanonicalServicesDependency,
    ids: Annotated[list[int] | None, Query(alias="ids[]")] = None,
) -> dict[str, object] | JSONResponse:
    """Return a batch of beatmaps by ID."""
    if services.content_query is None:
        return error(503, "service_unavailable", "Content service is unavailable.")
    if not ids:
        return {"beatmaps": []}
    try:
        beatmaps = await services.content_query.batch_lookup((), tuple(ids))
    except BeatmapsetNotFound:
        beatmaps = ()
    return {"beatmaps": [_beatmap(beatmap) for beatmap in beatmaps]}


def _beatmap(beatmap: BeatmapRevisionView) -> dict[str, object]:
    return {
        "id": beatmap.external_beatmap_id,
        "beatmapset_id": beatmap.external_beatmapset_id,
        "mode": _ruleset_id(beatmap.ruleset),
        "mode_int": _ruleset_id(beatmap.ruleset),
        "convert": False,
        "difficulty_rating": float(beatmap.star_rating) if beatmap.star_rating is not None else 0.0,
        "version": beatmap.difficulty_name,
        "total_length": beatmap.total_length_ms // 1000,
        "hit_length": beatmap.drain_length_ms // 1000,
        "bpm": float(beatmap.bpm),
        "cs": float(beatmap.circle_size),
        "drain": float(beatmap.health_drain),
        "accuracy": float(beatmap.overall_difficulty),
        "ar": float(beatmap.approach_rate),
        "playcount": 0,
        "passcount": 0,
        "count_circles": 0,
        "count_sliders": 0,
        "count_spinners": 0,
        "count_total": beatmap.object_count,
        "max_combo": beatmap.max_combo,
        "status": _STATUS_IDS.get(beatmap.status, 0),
        "checksum": beatmap.md5_hex,
        "last_updated": beatmap.source_updated_at.isoformat(),
        "beatmapset": {
            "id": beatmap.external_beatmapset_id,
            "artist": beatmap.artist,
            "artist_unicode": beatmap.artist,
            "title": beatmap.title,
            "title_unicode": beatmap.title,
            "creator": beatmap.creator,
        },
    }


def _ruleset_id(ruleset: str) -> int:
    return {"osu": 0, "taiko": 1, "fruits": 2, "mania": 3}.get(ruleset, 0)
