"""Shared lazer wire serializers for canonical osu! API v2 adapters."""

from datetime import datetime
from decimal import Decimal

from fastapi.responses import JSONResponse

from perfcho.infra.compose import StableServices
from perfcho.modules.scoring import AccountStatsView, Ruleset


def error(status_code: int, code: str, message: str) -> JSONResponse:
    """Return a lazer-shaped error response."""
    return JSONResponse(
        status_code=status_code,
        content={"error": code, "message": message, "hint": message},
    )


def parse_ruleset(value: str | None) -> Ruleset | None:
    """Parse a lazer ruleset path segment, returning None when invalid."""
    if value is None:
        return None
    try:
        return Ruleset(value)
    except ValueError:
        return None


async def statistics_rulesets(
    services: StableServices,
    account_id: int,
) -> dict[str, dict[str, object]]:
    """Load per-ruleset display statistics for one account."""
    statistics = getattr(services, "account_statistics", None)
    result: dict[str, dict[str, object]] = {}
    for ruleset in Ruleset:
        view = (
            await statistics.get_for_display(account_id, ruleset)
            if statistics is not None
            else AccountStatsView(0, Decimal(0), 0, 0, None)
        )
        result[ruleset.value] = statistics_response(view)
    return result


def statistics_response(stats: AccountStatsView) -> dict[str, object]:
    """Serialize one canonical statistics projection into the lazer shape."""
    return {
        "count_100": 0,
        "count_300": 0,
        "count_50": 0,
        "count_miss": 0,
        "level": {"current": 0, "progress": 0},
        "global_rank": stats.global_rank or None,
        "country_rank": stats.country_rank or None,
        "pp": stats.performance,
        "ranked_score": stats.ranked_score,
        "hit_accuracy": float(stats.accuracy * 100),
        "play_count": stats.play_count,
        "play_time": stats.play_time_ms // 1000,
        "total_score": stats.total_score,
        "total_hits": stats.total_hits,
        "maximum_combo": stats.maximum_combo,
        "replays_watched_by_others": stats.replay_views,
        "grade_counts": {
            "ssh": stats.grade_counts["XH"],
            "ss": stats.grade_counts["X"],
            "sh": stats.grade_counts["SH"],
            "s": stats.grade_counts["S"],
            "a": stats.grade_counts["A"],
        },
        "is_ranked": bool(stats.global_rank),
        "global_rank_percent": None,
        "variants": None,
    }


def user_response(
    services: StableServices,
    account_id: int,
    username: str,
    account_type: str,
    country_code: str | None,
    registered_at: datetime,
    last_seen_at: datetime | None,
    default_ruleset: Ruleset,
    statistics_rulesets: dict[str, dict[str, object]],
) -> dict[str, object]:
    """Serialize one public account into the lazer APIUser shape."""
    avatar_base = services.settings.stable_avatar_base_url.rstrip("/")
    registered = registered_at.isoformat()
    last_seen = last_seen_at.isoformat() if last_seen_at else registered
    return {
        "id": account_id,
        "username": username,
        "join_date": registered,
        "country_code": (country_code or "XX").upper(),
        "avatar_url": f"{avatar_base}/{account_id}",
        "is_active": True,
        "is_bot": account_type == "bot",
        "is_online": True,
        "is_supporter": False,
        "support_level": 0,
        "last_visit": last_seen,
        "pm_friends_only": False,
        "playmode": default_ruleset.value,
        "previous_usernames": [],
        "profile_order": [],
        "statistics_rulesets": statistics_rulesets,
        "groups": [],
        "session_verification_method": None,
        "score_processing_notice_url": "",
    }
