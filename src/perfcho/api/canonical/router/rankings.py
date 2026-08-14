"""Adapt osu!lazer ranking endpoints onto the ranking query service."""

from typing import Annotated

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from perfcho.api.canonical.dependencies import CanonicalServicesDependency
from perfcho.api.canonical.router._shared import error, parse_ruleset
from perfcho.modules.scoring import Ruleset, UserRankingView

router = APIRouter()


@router.get("/rankings/{ruleset}/{sort}", response_model=None, tags=["Rankings"])
async def get_rankings(
    ruleset: str,
    sort: str,
    services: CanonicalServicesDependency,
    country: Annotated[str | None, Query(min_length=2, max_length=2)] = None,
    page: Annotated[int, Query(ge=1)] = 1,
) -> dict[str, object] | JSONResponse:
    """Return a global or country ranking list."""
    selected = parse_ruleset(ruleset)
    if selected is None:
        return error(422, "invalid_request", "The requested ruleset is invalid.")
    if sort not in {"performance", "score"}:
        return error(422, "invalid_request", "The requested sort is invalid.")
    if services.ranking_query is None:
        return error(503, "service_unavailable", "Ranking is unavailable.")
    result = await services.ranking_query.list_rankings(
        ruleset=selected,
        sort=sort,
        country_code=country.upper() if country else None,
        page=page - 1,
        page_size=50,
    )
    return {"ranking": [_ranking(selected, row) for row in result.rows], "total": result.total_count}


@router.get("/rankings/{ruleset}/country", response_model=None, tags=["Rankings"])
@router.get("/rankings/{ruleset}/country/{sort}", response_model=None, tags=["Rankings"])
async def get_country_rankings(
    ruleset: str,
    services: CanonicalServicesDependency,
    sort: str = "performance",
    country: Annotated[str | None, Query(min_length=2, max_length=2)] = None,
    page: Annotated[int, Query(ge=1)] = 1,
) -> dict[str, object] | JSONResponse:
    """Return a country-scoped ranking list."""
    selected = parse_ruleset(ruleset)
    if selected is None:
        return error(422, "invalid_request", "The requested ruleset is invalid.")
    if sort not in {"performance", "score"}:
        return error(422, "invalid_request", "The requested sort is invalid.")
    if services.ranking_query is None:
        return error(503, "service_unavailable", "Ranking is unavailable.")
    result = await services.ranking_query.list_rankings(
        ruleset=selected,
        sort=sort,
        country_code=country.upper() if country else None,
        page=page - 1,
        page_size=50,
    )
    return {"ranking": [_ranking(selected, row) for row in result.rows], "total": result.total_count}


def _ranking(ruleset: Ruleset, row: UserRankingView) -> dict[str, object]:
    return {
        "user": {
            "id": row.account_id,
            "username": row.display_name,
            "country_code": (row.country_code or "XX").upper(),
            "avatar_url": None,
        },
        "level": {"current": 0, "progress": 0},
        "is_ranked": True,
        "global_rank": row.rank,
        "global_rank_percent": None,
        "country_rank": row.rank,
        "pp": row.performance,
        "ranked_score": row.ranked_score,
        "hit_accuracy": float(row.accuracy * 100),
        "play_count": 0,
        "play_time": None,
        "total_score": 0,
        "total_hits": 0,
        "maximum_combo": 0,
        "replays_watched_by_others": 0,
        "grade_counts": {"ssh": 0, "ss": 0, "sh": 0, "s": 0, "a": 0},
        "variants": None,
        "playmode": ruleset.value,
    }
