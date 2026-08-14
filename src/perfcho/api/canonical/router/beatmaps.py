"""Adapt osu!lazer beatmap endpoints onto the content query service."""

from typing import Annotated

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from perfcho.api.canonical.dependencies import CanonicalServicesDependency
from perfcho.api.canonical.router._shared import error
from perfcho.modules.content import BeatmapNotFound, BeatmapRevisionView, BeatmapsetNotFound
from perfcho.modules.scoring import CanonicalMod, Ruleset
from perfcho.modules.scoring.mods import canonical_json_digest

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


@router.post("/beatmaps/{beatmap_id}/attributes", response_model=None, tags=["Beatmaps"])
async def get_beatmap_attributes(
    beatmap_id: int,
    services: CanonicalServicesDependency,
    mods: Annotated[list[str] | None, Query()] = None,
    ruleset: Annotated[str | None, Query()] = None,
    ruleset_id: Annotated[int | None, Query(ge=0, le=3)] = None,
) -> dict[str, object] | JSONResponse:
    """Return calculated difficulty attributes for a beatmap and mods."""
    if services.content_query is None or services.difficulty_query is None:
        return error(503, "service_unavailable", "Difficulty calculation is unavailable.")
    try:
        beatmap = await services.content_query.lookup_beatmap(beatmap_id, external=True)
    except BeatmapNotFound:
        return error(404, "not_found", "Beatmap was not found.")
    if beatmap.file_storage_key is None:
        return error(404, "not_found", "Beatmap object is unavailable.")

    selected = _ruleset_from(ruleset, ruleset_id) or _ruleset_from_name(beatmap.ruleset)
    if selected is None:
        return error(422, "invalid_request", "The requested ruleset is invalid.")

    canonical_mods = tuple(_parse_mod(value) for value in (mods or ()))
    mods_digest = _mods_digest(canonical_mods)
    try:
        result = await services.difficulty_query.resolve(
            beatmap_revision_id=beatmap.revision_id,
            beatmap_sha256=beatmap.sha256,
            beatmap_storage_key=beatmap.file_storage_key,
            ruleset=selected,
            mods_digest=mods_digest,
            mods=canonical_mods,
        )
    except Exception as exc:
        return error(502, "calculation_failed", str(exc))

    return {
        "star_rating": float(result.star_rating),
        "max_combo": result.max_combo,
        "attributes": dict(result.attributes),
    }


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


_RULESET_BY_ID = {0: Ruleset.OSU, 1: Ruleset.TAIKO, 2: Ruleset.FRUITS, 3: Ruleset.MANIA}


def _ruleset_from(name: str | None, identifier: int | None) -> Ruleset | None:
    if identifier is not None:
        return _RULESET_BY_ID.get(identifier)
    return _ruleset_from_name(name) if name else None


def _ruleset_from_name(name: str | None) -> Ruleset | None:
    if name is None:
        return None
    try:
        return Ruleset(name)
    except ValueError:
        return None


def _parse_mod(value: str) -> CanonicalMod:
    return CanonicalMod(value.strip().upper())


def _mods_digest(mods: tuple[CanonicalMod, ...]) -> bytes:
    return canonical_json_digest([mod.as_json() for mod in sorted(mods, key=lambda m: m.acronym)])
