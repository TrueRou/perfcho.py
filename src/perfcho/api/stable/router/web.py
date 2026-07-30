"""Adapt Stable web requests to shared content and social application services."""

from __future__ import annotations

import hashlib
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal
from time import monotonic_ns
from typing import Annotated, Literal
from urllib.parse import parse_qs, quote

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status
from fastapi.responses import RedirectResponse, StreamingResponse
from starlette.formparsers import MultiPartException

from perfcho.api.stable.canonize.ipaddr import resolve_client_ip
from perfcho.api.stable.canonize.scoring import (
    ParsedStableScore,
    decrypt_stable_score,
    normalize_stable_attestation,
    parse_stable_submission_form,
    read_stable_replay,
    stable_submission_digest,
    validate_stable_submission_evidence,
    validate_stable_submission_time,
    verify_stable_online_checksum,
)
from perfcho.api.stable.dependencies import StableServicesDependency
from perfcho.infra.logging import duration_ms, log_event, rate_limit
from perfcho.modules.common import (
    Actor,
    ApplicationError,
    ClientContext,
    CommandMeta,
    ObjectStorage,
    ObjectUnavailable,
)
from perfcho.modules.community import CommunityService
from perfcho.modules.content import (
    BeatmapNotFound,
    BeatmapRevisionView,
    BeatmapsetNotFound,
    BeatmapsetView,
    ContentQueryService,
    ContentSearch,
    ContentService,
    RatingSummary,
)
from perfcho.modules.identity import InvalidCredentials, StableWebPrincipal
from perfcho.modules.realtime import RealtimeSessionFenced, RealtimeSessionNotFound
from perfcho.modules.scoring import (
    AcceptScore,
    BeatmapReference,
    BeatmapRevisionNotFound,
    LeaderboardPage,
    LeaderboardScoreView,
    RankingQueryService,
    ReplayNotFound,
    ReplayQueryService,
    ReplayService,
    Ruleset,
    ScoreGrade,
    ScoringService,
    StagedReplayManifest,
)
from perfcho.modules.scoring.mods import parse_legacy_mods
from perfcho.modules.social import SocialService

router = APIRouter(default_response_class=Response)

_BEATMAP_LOOKUP_LIMIT = 512
_EMPTY_DIRECT_RESULT = b"0"
_RULESETS = {0: "osu", 1: "taiko", 2: "fruits", 3: "mania"}
_DIRECT_STATUS_FILTERS = {
    0: ("ranked", "approved"),
    2: ("pending", "wip"),
    3: ("qualified",),
    4: (),
    5: ("graveyard",),
    7: ("ranked", "approved"),
    8: ("loved",),
}
_EMPTY_DATETIME = datetime(1970, 1, 1, tzinfo=UTC)
_GRADE_RULESETS = (Ruleset.OSU, Ruleset.TAIKO, Ruleset.FRUITS, Ruleset.MANIA)
_RULESET_IDS = {ruleset.value: index for index, ruleset in enumerate(_GRADE_RULESETS)}
_OFFICIAL_DOWNLOAD_BASE_URL = "https://osu.ppy.sh/beatmapsets"
_PUBLIC_DOWNLOAD_BASE_URL = "https://api.nerinyan.moe/d"


@dataclass(frozen=True, slots=True)
class _WebAuthentication:
    principal: StableWebPrincipal | None


def _content_query(services: StableServicesDependency) -> ContentQueryService:
    if services.content_query is None:
        raise RuntimeError("Stable content queries are not configured")
    return services.content_query


def _content(services: StableServicesDependency) -> ContentService:
    if services.content is None:
        raise RuntimeError("Stable content commands are not configured")
    return services.content


def _social(services: StableServicesDependency) -> SocialService:
    if services.social is None:
        raise RuntimeError("Stable social services are not configured")
    return services.social


def _community(services: StableServicesDependency) -> CommunityService:
    if services.community is None:
        raise RuntimeError("Stable community services are not configured")
    return services.community


def _object_storage(services: StableServicesDependency) -> ObjectStorage:
    if services.object_storage is None:
        raise RuntimeError("Stable object storage is not configured")
    return services.object_storage


def _scoring(services: StableServicesDependency) -> ScoringService:
    if services.scoring is None:
        raise RuntimeError("Stable scoring is not configured")
    return services.scoring


def _replay_query(services: StableServicesDependency) -> ReplayQueryService:
    if services.replay_query is None:
        raise RuntimeError("Stable replay queries are not configured")
    return services.replay_query


def _replay(services: StableServicesDependency) -> ReplayService:
    if services.replay is None:
        raise RuntimeError("Stable replay commands are not configured")
    return services.replay


def _ranking_query(services: StableServicesDependency) -> RankingQueryService:
    if services.ranking_query is None:
        raise RuntimeError("Stable ranking queries are not configured")
    return services.ranking_query


async def _authenticate(
    services: StableServicesDependency,
    username: str,
    password_token: str,
) -> _WebAuthentication:
    try:
        principal = await services.identity.verify_stable_web(username, password_token)
        realtime = await services.realtime.resolve_session(principal.session_id, at=services.clock.now())
        if realtime.account_id != principal.account_id:
            raise RealtimeSessionFenced("Stable Web principal does not own the realtime session")
    except (InvalidCredentials, RealtimeSessionNotFound, RealtimeSessionFenced) as error:
        if rate_limit(f"stable-web-auth:{error.code}", interval_seconds=5):
            log_event(
                "INFO",
                "stable.web.auth_rejected",
                outcome="rejected",
                error_code=error.code,
                error_type=type(error).__name__,
            )
        principal = None
    return _WebAuthentication(principal)


async def _web_authentication(
    services: StableServicesDependency,
    username: Annotated[str, Query(alias="u", min_length=1, max_length=64)],
    password_token: Annotated[str, Query(alias="h", min_length=32, max_length=32)],
) -> _WebAuthentication:
    return await _authenticate(services, username, password_token)


async def _rating_authentication(
    services: StableServicesDependency,
    username: Annotated[str, Query(alias="u", min_length=1, max_length=64)],
    password_token: Annotated[str, Query(alias="p", min_length=32, max_length=32)],
) -> _WebAuthentication:
    return await _authenticate(services, username, password_token)


async def _leaderboard_authentication(
    services: StableServicesDependency,
    username: Annotated[str, Query(alias="us", min_length=1, max_length=64)],
    password_token: Annotated[str, Query(alias="ha", min_length=32, max_length=32)],
) -> _WebAuthentication:
    return await _authenticate(services, username, password_token)


def _authentication_failed(*, rating: bool = False) -> Response:
    content = b"auth fail" if rating else b""
    return Response(content, status_code=status.HTTP_401_UNAUTHORIZED)


def _log_score_stage(started_ns: int, stage: str, *, account_id: int | None = None) -> None:
    log_event(
        "DEBUG",
        "stable.score_submission.stage",
        stage=stage,
        outcome="completed",
        account_id=account_id,
        duration_ms=duration_ms(started_ns),
    )


def _log_score_rejection(
    started_ns: int,
    stage: str,
    *,
    account_id: int | None = None,
    error: BaseException | None = None,
    error_code: str | None = None,
    error_type: str | None = None,
) -> None:
    log_event(
        "INFO",
        "stable.score_submission.rejected",
        stage=stage,
        outcome="rejected",
        account_id=account_id,
        error_code=error_code or getattr(error, "code", "stable_input_rejected"),
        error_type=error_type or (type(error).__name__ if error is not None else "SubmissionRejected"),
        duration_ms=duration_ms(started_ns),
    )


async def _read_limited_body(request: Request, maximum: int) -> bytes:
    content_length = request.headers.get("content-length")
    if content_length is not None:
        try:
            if int(content_length) > maximum:
                raise ValueError("request body is too large")
        except ValueError as error:
            raise ValueError("invalid request content length") from error
    body = bytearray()
    async for chunk in request.stream():
        if len(body) + len(chunk) > maximum:
            raise ValueError("request body is too large")
        body.extend(chunk)
    return bytes(body)


def _parse_beatmap_info_body(body: bytes, content_type: str) -> tuple[tuple[str, ...], tuple[int, ...]]:
    try:
        if content_type.partition(";")[0].strip().casefold() == "application/json":
            payload = json.loads(body)
            if not isinstance(payload, dict):
                raise ValueError("beatmap info body must be an object")
            filenames_value = payload.get("Filenames", [])
            ids_value = payload.get("Ids", [])
        else:
            payload = parse_qs(body.decode("utf-8"), keep_blank_values=True, max_num_fields=1024)
            filenames_value = payload.get("Filenames", payload.get("Filenames[]", []))
            ids_value = payload.get("Ids", payload.get("Ids[]", []))
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as error:
        raise ValueError("invalid beatmap info body") from error
    if not isinstance(filenames_value, list) or not isinstance(ids_value, list):
        raise ValueError("beatmap info selectors must be arrays")
    if len(filenames_value) + len(ids_value) > _BEATMAP_LOOKUP_LIMIT:
        raise ValueError("too many beatmap info selectors")
    if any(not isinstance(item, str) or not item for item in filenames_value):
        raise ValueError("invalid beatmap filename")
    try:
        beatmap_ids = tuple(int(item) for item in ids_value)
    except (TypeError, ValueError) as error:
        raise ValueError("invalid beatmap ID") from error
    if any(identifier < 1 for identifier in beatmap_ids):
        raise ValueError("invalid beatmap ID")
    return tuple(filenames_value), beatmap_ids


def _beatmap_info_status(value: str) -> int:
    return {
        "graveyard": -2,
        "wip": -1,
        "pending": 0,
        "ranked": 1,
        "approved": 2,
        "qualified": 3,
        "loved": 4,
    }.get(value, 0)


def _direct_status(value: str) -> int:
    return {
        "graveyard": 0,
        "wip": 0,
        "pending": 0,
        "ranked": 2,
        "approved": 3,
        "qualified": 4,
        "loved": 5,
    }.get(value, 0)


def _ruleset_id(value: str) -> int:
    return {ruleset: identifier for identifier, ruleset in _RULESETS.items()}[value]


def _clean_direct_text(value: str) -> str:
    return value.replace("|", "I").replace("\r", " ").replace("\n", " ")


def _format_beatmap_info(
    request_index: int,
    beatmap: BeatmapRevisionView,
    grades: dict[Ruleset, ScoreGrade],
) -> str:
    grade_fields = "|".join(grades.get(ruleset, ScoreGrade.N).value for ruleset in _GRADE_RULESETS)
    return (
        f"{request_index}|{beatmap.external_beatmap_id}|{beatmap.external_beatmapset_id}|"
        f"{beatmap.md5_hex}|{_beatmap_info_status(beatmap.status)}|{grade_fields}"
    )


def _format_direct_difficulty(beatmap: BeatmapRevisionView) -> str:
    stars = beatmap.star_rating if beatmap.star_rating is not None else Decimal(0)
    return (
        f"[{stars:.2f}\u2b50] {_clean_direct_text(beatmap.difficulty_name)} "
        f"{{cs: {beatmap.circle_size} / od: {beatmap.overall_difficulty} / "
        f"ar: {beatmap.approach_rate} / hp: {beatmap.health_drain}}}@{_ruleset_id(beatmap.ruleset)}"
    )


def _format_direct_set(beatmapset: BeatmapsetView) -> str:
    beatmaps = ",".join(
        _format_direct_difficulty(beatmap)
        for beatmap in sorted(
            beatmapset.beatmaps,
            key=lambda item: item.star_rating if item.star_rating is not None else Decimal(0),
        )
    )
    last_updated_at = beatmapset.last_updated_at or max(
        (beatmap.source_updated_at for beatmap in beatmapset.beatmaps),
        default=_EMPTY_DATETIME,
    )
    return (
        f"{beatmapset.external_beatmapset_id}.osz|{_clean_direct_text(beatmapset.artist)}|"
        f"{_clean_direct_text(beatmapset.title)}|{_clean_direct_text(beatmapset.creator)}|"
        f"{_direct_status(beatmapset.status)}|10.0|{last_updated_at:%Y-%m-%d %H:%M:%S}|"
        f"{beatmapset.external_beatmapset_id}|0|{int(beatmapset.has_video)}|0|0|0|{beatmaps}"
    )


def _format_rating(summary: RatingSummary) -> str:
    average = summary.average if summary.average is not None else Decimal(0)
    return format(average, ".2f")


@router.get("/web/osu-getfriends.php")
async def get_friends(
    authentication: Annotated[_WebAuthentication, Depends(_web_authentication)],
    social: Annotated[SocialService, Depends(_social)],
) -> Response:
    """Return outgoing Stable friend account IDs."""
    if authentication.principal is None:
        return _authentication_failed()
    friends = await social.list_friends(authentication.principal.account_id)
    friend_ids = tuple(dict.fromkeys((1, *(friend.account_id for friend in friends))))
    return Response("\n".join(str(account_id) for account_id in friend_ids))


@router.get("/web/osu-markasread.php")
async def mark_direct_conversation_read(
    authentication: Annotated[_WebAuthentication, Depends(_web_authentication)],
    social: Annotated[SocialService, Depends(_social)],
    community: Annotated[CommunityService, Depends(_community)],
    channel: Annotated[str, Query(max_length=64)],
) -> Response:
    """Advance the authenticated account's read cursor for one direct-message peer."""
    principal = authentication.principal
    if principal is None:
        return _authentication_failed()
    if not channel:
        return Response(b"")
    try:
        target = await social.resolve_account_by_name(channel)
        await community.mark_direct_conversation_read(principal.account_id, target.account_id)
    except ApplicationError:
        pass
    return Response(b"")


@router.post("/web/osu-getbeatmapinfo.php")
async def get_beatmap_info(
    request: Request,
    services: StableServicesDependency,
    authentication: Annotated[_WebAuthentication, Depends(_web_authentication)],
    content_query: Annotated[ContentQueryService, Depends(_content_query)],
) -> Response:
    """Resolve a bounded song-select beatmap batch in request order."""
    if authentication.principal is None:
        return _authentication_failed()
    try:
        body = await _read_limited_body(request, services.settings.stable_max_body_bytes)
        filenames, beatmap_ids = _parse_beatmap_info_body(body, request.headers.get("content-type", ""))
    except ValueError:
        return Response(b"", status_code=status.HTTP_400_BAD_REQUEST)
    beatmaps = await content_query.batch_lookup(filenames, beatmap_ids)
    grades: dict[int, dict[Ruleset, ScoreGrade]] = {}
    ranking_query: RankingQueryService | None = services.ranking_query
    if ranking_query is not None:
        projected_grades = await ranking_query.get_beatmap_grades(
            authentication.principal.account_id,
            tuple(dict.fromkeys(beatmap.beatmap_id for beatmap in beatmaps)),
        )
        for projected in projected_grades:
            grades.setdefault(projected.beatmap_id, {})[projected.ruleset] = projected.grade
    by_filename = {beatmap.file_name.strip().casefold(): beatmap for beatmap in beatmaps}
    by_id = {beatmap.external_beatmap_id: beatmap for beatmap in beatmaps}
    lines = [
        _format_beatmap_info(index, beatmap, grades.get(beatmap.beatmap_id, {}))
        for index, filename in enumerate(filenames)
        if (beatmap := by_filename.get(filename.strip().casefold())) is not None
    ]
    lines.extend(
        _format_beatmap_info(len(filenames) + index, beatmap, grades.get(beatmap.beatmap_id, {}))
        for index, identifier in enumerate(beatmap_ids)
        if (beatmap := by_id.get(identifier)) is not None
    )
    return Response("\n".join(lines))


@router.get("/web/osu-search.php")
async def direct_search(
    authentication: Annotated[_WebAuthentication, Depends(_web_authentication)],
    content_query: Annotated[ContentQueryService, Depends(_content_query)],
    ranked_status: Annotated[int, Query(alias="r", ge=0, le=8)],
    query: Annotated[str, Query(alias="q", max_length=255)],
    mode: Annotated[int, Query(alias="m", ge=-1, le=3)],
    page: Annotated[int, Query(alias="p", ge=0)],
) -> Response:
    """Return one Stable Direct search page from locally indexed content."""
    if authentication.principal is None:
        return _authentication_failed()
    result = await content_query.search(
        ContentSearch(
            query="" if query in {"Newest", "Top+Rated", "Most+Played"} else query,
            ruleset=None if mode == -1 else _RULESETS[mode],
            statuses=_DIRECT_STATUS_FILTERS.get(ranked_status, ()),
            page=page,
        )
    )
    if not result.items:
        return Response(_EMPTY_DIRECT_RESULT)
    count = 101 if result.has_more else len(result.items)
    return Response("\n".join((str(count), *(_format_direct_set(item) for item in result.items))))


@router.get("/web/osu-search-set.php")
async def direct_search_set(
    authentication: Annotated[_WebAuthentication, Depends(_web_authentication)],
    content_query: Annotated[ContentQueryService, Depends(_content_query)],
    beatmapset_id: Annotated[int | None, Query(alias="s", gt=0)] = None,
    beatmap_id: Annotated[int | None, Query(alias="b", gt=0)] = None,
    checksum: Annotated[str | None, Query(alias="c", min_length=32, max_length=32)] = None,
) -> Response:
    """Resolve one Stable Direct beatmapset by set, map, or checksum."""
    if authentication.principal is None:
        return _authentication_failed()
    try:
        if beatmapset_id is not None:
            result = await content_query.get_beatmapset(beatmapset_id)
        elif beatmap_id is not None:
            beatmap = await content_query.lookup_beatmap(beatmap_id)
            result = await content_query.get_beatmapset(beatmap.external_beatmapset_id)
        elif checksum is not None:
            beatmap = await content_query.lookup_md5(checksum)
            result = await content_query.get_beatmapset(beatmap.external_beatmapset_id)
        else:
            return Response(b"")
    except BeatmapNotFound, BeatmapsetNotFound:
        return Response(b"")
    return Response(_format_direct_set(result))


@router.get("/web/osu-getfavourites.php")
async def get_favourites(
    authentication: Annotated[_WebAuthentication, Depends(_web_authentication)],
    content_query: Annotated[ContentQueryService, Depends(_content_query)],
) -> Response:
    """Return an account's favourited public beatmapset IDs."""
    if authentication.principal is None:
        return _authentication_failed()
    favourites = await content_query.list_favourites(authentication.principal.account_id)
    return Response("\n".join(str(identifier) for identifier in favourites))


@router.get("/web/osu-addfavourite.php")
async def add_favourite(
    authentication: Annotated[_WebAuthentication, Depends(_web_authentication)],
    content: Annotated[ContentService, Depends(_content)],
    beatmapset_id: Annotated[int, Query(alias="a", gt=0)],
) -> Response:
    """Idempotently favourite one public beatmapset."""
    if authentication.principal is None:
        return _authentication_failed()
    result = await content.set_favourite(authentication.principal.account_id, beatmapset_id)
    if not result.favourited:
        return Response(b"Beatmap not found.")
    message = b"Added favourite!" if result.changed else b"You've already favourited this beatmap!"
    return Response(message)


@router.get("/web/osu-rate.php")
async def rate_beatmap(
    authentication: Annotated[_WebAuthentication, Depends(_rating_authentication)],
    content_query: Annotated[ContentQueryService, Depends(_content_query)],
    content: Annotated[ContentService, Depends(_content)],
    checksum: Annotated[str, Query(alias="c", min_length=32, max_length=32)],
    rating: Annotated[int | None, Query(alias="v", ge=1, le=10)] = None,
) -> Response:
    """Execute the Stable rating eligibility and submission handshake."""
    if authentication.principal is None:
        return _authentication_failed(rating=True)
    try:
        beatmap = await content_query.lookup_md5(checksum)
    except BeatmapNotFound:
        return Response(b"no exist")
    account_id = authentication.principal.account_id
    if rating is None:
        summary = await content_query.get_rating(beatmap.beatmap_id, account_id)
        if summary.account_rating is None:
            return Response(b"ok")
    else:
        summary = await content.rate(account_id, beatmap.beatmap_id, rating)
    return Response(f"alreadyvoted\n{_format_rating(summary)}")


@router.get("/web/bancho_connect.php")
async def bancho_connect(
    version: Annotated[str, Query(alias="v", min_length=1, max_length=64)],
    failed_endpoint: Annotated[str | None, Query(alias="fail", max_length=512)] = None,
    framework_versions: Annotated[str | None, Query(alias="fx", max_length=512)] = None,
    client_hash: Annotated[str | None, Query(alias="ch", max_length=512)] = None,
    retrying: Annotated[bool | None, Query(alias="retry")] = None,
) -> Response:
    """Accept the Stable connectivity probe without creating a session."""
    del version, failed_endpoint, framework_versions, client_hash, retrying
    return Response(b"")


@router.get("/web/check-updates.php")
async def check_updates(
    action: Annotated[Literal["check", "path", "error"], Query()],
    stream: Annotated[Literal["cuttingedge", "stable40", "beta40", "stable"], Query()],
) -> Response:
    """Accept the Stable updater probe for supported release streams."""
    del action, stream
    return Response(b"")


@router.get("/web/maps/{map_filename}")
async def get_updated_beatmap(
    services: StableServicesDependency,
    content_query: Annotated[ContentQueryService, Depends(_content_query)],
    object_storage: Annotated[ObjectStorage, Depends(_object_storage)],
    map_filename: str,
) -> Response:
    """Stream the current immutable beatmap file or fall back to the upstream source."""
    fallback = f"{services.settings.stable_beatmap_file_base_url.rstrip('/')}/{quote(map_filename, safe='')}"
    try:
        beatmap = await content_query.lookup_filename(map_filename)
    except BeatmapNotFound:
        return RedirectResponse(fallback, status_code=status.HTTP_302_FOUND)
    if beatmap.file_storage_key is None:
        return RedirectResponse(fallback, status_code=status.HTTP_302_FOUND)
    stream_context = object_storage.open(beatmap.file_storage_key)
    try:
        object_stream = await stream_context.__aenter__()
    except ObjectUnavailable:
        return RedirectResponse(fallback, status_code=status.HTTP_302_FOUND)

    async def body() -> AsyncIterator[bytes]:
        try:
            async for chunk in object_stream.iter_chunks():
                yield chunk
        finally:
            await stream_context.__aexit__(None, None, None)

    size_bytes = beatmap.file_size_bytes or object_stream.metadata.size_bytes
    media_type = beatmap.file_media_type or object_stream.metadata.media_type
    return StreamingResponse(
        body(),
        media_type=media_type,
        headers={"Content-Length": str(size_bytes)},
    )


@router.get("/d/{beatmapset_selector}")
async def download_beatmapset(
    services: StableServicesDependency,
    beatmapset_selector: str,
) -> Response:
    """Redirect a Stable Direct download to the configured upstream source."""
    no_video = beatmapset_selector.endswith("n")
    identifier = beatmapset_selector[:-1] if no_video else beatmapset_selector
    if not identifier.isascii() or not identifier.isdigit() or int(identifier) < 1:
        return Response(b"", status_code=status.HTTP_404_NOT_FOUND)
    base_url = services.settings.stable_beatmap_download_base_url.rstrip("/")
    if base_url == _OFFICIAL_DOWNLOAD_BASE_URL:
        base_url = _PUBLIC_DOWNLOAD_BASE_URL
    if base_url.endswith("/d"):
        target = f"{base_url}/{identifier}"
        if no_video:
            target += "?nv=1"
    else:
        target = f"{base_url}/{identifier}/download"
        if no_video:
            target += "?noVideo=1"
    return RedirectResponse(target, status_code=status.HTTP_302_FOUND)


@router.post("/web/osu-submit-modular-selector.php")
async def submit_score(
    request: Request,
    services: StableServicesDependency,
    scoring: Annotated[ScoringService, Depends(_scoring)],
    content_query: Annotated[ContentQueryService, Depends(_content_query)],
    object_storage: Annotated[ObjectStorage, Depends(_object_storage)],
) -> Response:
    """Decrypt, authenticate, stage, and atomically accept one Stable score."""
    started_ns = monotonic_ns()
    try:
        form = await parse_stable_submission_form(request, services.settings.stable_score_submission_max_bytes)
        parsed = decrypt_stable_score(
            score_data_b64=form.encrypted_score,
            client_hash_b64=form.encrypted_client_hash,
            iv_b64=form.iv,
            osu_version=form.osu_version,
            exited=form.exited,
            fail_time_ms=form.fail_time_ms,
            score_time_ms=form.score_time_ms,
            supported_build=services.settings.stable_build,
        )
        validate_stable_submission_evidence(form, parsed)
    except (HTTPException, MultiPartException, ValueError) as error:
        _log_score_rejection(started_ns, "parse", error=error)
        return Response(b"error: no")
    _log_score_stage(started_ns, "parsed")

    authentication = await _authenticate(services, parsed.username, form.password_token)
    principal = authentication.principal
    if principal is None:
        _log_score_rejection(started_ns, "authentication")
        return Response(b"error: pass")
    _log_score_stage(started_ns, "authenticated", account_id=principal.account_id)
    try:
        verify_stable_online_checksum(
            parsed,
            osu_version=form.osu_version,
            storyboard_hash=form.storyboard_hash,
            username=principal.current_name,
        )
    except ValueError as error:
        _log_score_rejection(started_ns, "integrity_validation", account_id=principal.account_id, error=error)
        return Response(b"error: no")
    try:
        beatmap = await content_query.lookup_md5(parsed.beatmap_md5)
    except BeatmapNotFound as error:
        _log_score_rejection(started_ns, "beatmap_lookup", account_id=principal.account_id, error=error)
        return Response(b"error: beatmap")
    received_at = services.clock.now()
    try:
        validate_stable_submission_time(parsed, received_at)
    except ValueError as error:
        _log_score_rejection(started_ns, "attempt_time", account_id=principal.account_id, error=error)
        return Response(b"error: no")

    try:
        replay_content = await read_stable_replay(form.replay, services.settings.stable_replay_max_bytes)
    except ValueError as error:
        _log_score_rejection(started_ns, "artifact_validation", account_id=principal.account_id, error=error)
        return Response(b"error: no")
    finally:
        await form.replay.close()
    replay_digest = hashlib.sha256(replay_content).digest()
    storage_key = f"replays/stable/{principal.account_id}/{replay_digest.hex()}.osr"
    try:
        # Content-addressed upload precedes the database transaction; reconciliation removes orphaned objects.
        stored = await object_storage.put(
            storage_key,
            replay_content,
            media_type="application/octet-stream",
            expected_sha256=replay_digest,
        )
    except ObjectUnavailable as error:
        _log_score_rejection(started_ns, "object_staging", account_id=principal.account_id, error=error)
        return Response(b"")
    _log_score_stage(started_ns, "object_staged", account_id=principal.account_id)

    request_id = services.id_generator.new()
    online_checksum = parsed.score.online_checksum
    if online_checksum is None:
        _log_score_rejection(started_ns, "integrity_validation", account_id=principal.account_id)
        return Response(b"error: no")
    command = AcceptScore(
        meta=CommandMeta(
            request_id=request_id,
            idempotency_key=f"stable-score:{principal.account_id}:{online_checksum.hex()}",
            request_digest=stable_submission_digest(form, replay_digest),
            actor=Actor(principal.account_id, principal.session_id),
            client=ClientContext(
                family="stable",
                version=services.settings.stable_build,
                variant=None,
                ip_address=resolve_client_ip(request, services.settings.trusted_proxy_cidrs),
                user_agent=request.headers.get("user-agent"),
            ),
            received_at=received_at,
        ),
        beatmap=BeatmapReference(md5=parsed.beatmap_md5),
        ruleset=parsed.ruleset,
        variant=parsed.variant,
        mods=parsed.mods,
        attempt=parsed.attempt,
        score=parsed.score,
        replay=StagedReplayManifest(
            format="stable",
            sha256=replay_digest,
            size_bytes=stored.size_bytes,
            storage_key=stored.storage_key,
            client_version=services.settings.stable_build,
        ),
        attestation=normalize_stable_attestation(form, parsed),
        multiplayer=None,
    )
    try:
        if services.multiplayer is not None:
            command = replace(
                command,
                multiplayer=await services.multiplayer.resolve_submission_context(
                    principal.account_id,
                    beatmap.revision_id,
                    started_at=parsed.attempt.started_at,
                    ended_at=parsed.attempt.ended_at,
                ),
            )
        result = await scoring.accept(command)
    except BeatmapRevisionNotFound as error:
        _log_score_rejection(started_ns, "canonical_accept", account_id=principal.account_id, error=error)
        return Response(b"error: beatmap")
    except (ApplicationError, ValueError) as error:
        _log_score_rejection(started_ns, "canonical_accept", account_id=principal.account_id, error=error)
        return Response(b"error: no")
    if result.outcome.value != "passed":
        _log_score_rejection(
            started_ns,
            "canonical_outcome",
            account_id=principal.account_id,
            error_code="score_outcome_rejected",
            error_type=type(result.outcome).__name__,
        )
        return Response(b"error: no")
    log_event(
        "INFO",
        "stable.score_submission.completed",
        outcome="accepted",
        account_id=principal.account_id,
        score_id=result.score_id,
        beatmap_id=result.beatmap_id,
        beatmap_revision_id=result.beatmap_revision_id,
        ruleset=parsed.ruleset.value,
        duration_ms=duration_ms(started_ns),
    )
    return Response(_submission_chart(result.score_id, beatmap, parsed))


@router.get("/web/osu-getreplay.php")
async def get_replay(
    authentication: Annotated[_WebAuthentication, Depends(_web_authentication)],
    replay_query: Annotated[ReplayQueryService, Depends(_replay_query)],
    replay_service: Annotated[ReplayService, Depends(_replay)],
    object_storage: Annotated[ObjectStorage, Depends(_object_storage)],
    services: StableServicesDependency,
    mode: Annotated[int, Query(alias="m", ge=0, le=3)],
    score_id: Annotated[int, Query(alias="c", gt=0)],
) -> Response:
    """Stream one ready replay and append an idempotent non-owner view fact."""
    principal = authentication.principal
    if principal is None:
        return Response(b"", status_code=status.HTTP_401_UNAUTHORIZED)
    try:
        replay = await replay_query.get(score_id)
    except ReplayNotFound:
        return Response(b"", status_code=status.HTTP_404_NOT_FOUND)
    if _RULESET_IDS[replay.ruleset.value] != mode:
        return Response(b"", status_code=status.HTTP_404_NOT_FOUND)
    stream_context = object_storage.open(replay.storage_key)
    try:
        object_stream = await stream_context.__aenter__()
    except ObjectUnavailable:
        return Response(b"", status_code=status.HTTP_404_NOT_FOUND)
    await replay_service.record_view(
        request_id=services.id_generator.new(),
        replay=replay,
        viewer_account_id=principal.account_id,
    )

    async def body() -> AsyncIterator[bytes]:
        try:
            async for chunk in object_stream.iter_chunks():
                yield chunk
        finally:
            await stream_context.__aexit__(None, None, None)

    return StreamingResponse(
        body(),
        media_type="application/octet-stream",
        headers={"Content-Length": str(replay.size_bytes)},
    )


@router.get("/web/osu-osz2-getscores.php")
async def get_scores(
    authentication: Annotated[_WebAuthentication, Depends(_leaderboard_authentication)],
    content_query: Annotated[ContentQueryService, Depends(_content_query)],
    ranking_query: Annotated[RankingQueryService, Depends(_ranking_query)],
    social: Annotated[SocialService, Depends(_social)],
    requesting_from_editor: Annotated[bool, Query(alias="s")],
    leaderboard_version: Annotated[int, Query(alias="vv", ge=0)],
    leaderboard_type: Annotated[int, Query(alias="v", ge=0, le=4)],
    map_md5: Annotated[str, Query(alias="c", min_length=32, max_length=32)],
    map_filename: Annotated[str, Query(alias="f", min_length=1, max_length=255)],
    mode: Annotated[int, Query(alias="m", ge=0, le=3)],
    map_set_id: Annotated[int, Query(alias="i", ge=-1, le=2_147_483_647)],
    legacy_mod_bits: Annotated[int, Query(alias="mods", ge=0, le=2_147_483_647)],
    map_package_hash: Annotated[str, Query(alias="h", max_length=512)],
    legacy_client_flag: Annotated[bool, Query(alias="a")],
) -> Response:
    """Return one Stable leaderboard page from ranking projections."""
    del leaderboard_version, map_package_hash, legacy_client_flag
    principal = authentication.principal
    if principal is None:
        return Response(b"", status_code=status.HTTP_401_UNAUTHORIZED)
    try:
        beatmap = await content_query.lookup_md5(map_md5)
    except BeatmapNotFound:
        try:
            await content_query.lookup_filename(map_filename)
        except BeatmapNotFound:
            return Response(b"-1|false")
        return Response(b"1|false")
    if not beatmap.is_current:
        return Response(b"1|false")
    if map_set_id > 0 and map_set_id != beatmap.external_beatmapset_id:
        return Response(b"-1|false")
    try:
        _, variant = parse_legacy_mods(legacy_mod_bits)
    except ValueError:
        return Response(b"-1|false")
    ruleset = _GRADE_RULESETS[mode]
    if leaderboard_type == 3:
        friends = await social.list_friends(principal.account_id)
        friend_ids = tuple(friend.account_id for friend in friends)
    else:
        friend_ids = ()
    page = (
        LeaderboardPage((), None)
        if requesting_from_editor
        else await ranking_query.get_stable_leaderboard(
            beatmap_id=beatmap.beatmap_id,
            ruleset=ruleset,
            variant=variant,
            leaderboard_type=leaderboard_type,
            legacy_mod_bits=legacy_mod_bits,
            requester_account_id=principal.account_id,
            friend_account_ids=friend_ids,
        )
    )
    rating = await content_query.get_rating(beatmap.beatmap_id, principal.account_id)
    average_rating = rating.average if rating.average is not None else 0
    lines = [
        (
            f"{_direct_status(beatmap.status)}|false|{beatmap.external_beatmap_id}|"
            f"{beatmap.external_beatmapset_id}|{len(page.scores)}|0|"
        ),
        f"0\n{beatmap.artist} - {beatmap.title} [{beatmap.difficulty_name}]\n{average_rating}",
        _format_leaderboard_score(page.personal_best) if page.personal_best is not None else "",
    ]
    lines.extend(_format_leaderboard_score(score) for score in page.scores)
    if not page.scores:
        lines.append("")
    return Response("\n".join(lines))


def _submission_chart(score_id: int, beatmap: BeatmapRevisionView, parsed: ParsedStableScore) -> str:
    accuracy = format(parsed.score.accuracy * 100, ".2f")
    return "|".join(
        (
            f"beatmapId:{beatmap.external_beatmap_id}",
            f"beatmapSetId:{beatmap.external_beatmapset_id}",
            "beatmapPlaycount:0",
            "beatmapPasscount:0",
            f"approvedDate:{beatmap.source_updated_at:%Y-%m-%d %H:%M:%S}",
            "\n",
            "chartId:beatmap",
            f"chartUrl:https://osu.ppy.sh/b/{beatmap.external_beatmap_id}",
            "chartName:Beatmap Ranking",
            "rankBefore:",
            "rankAfter:",
            "rankedScoreBefore:",
            f"rankedScoreAfter:{parsed.score.total_score}",
            "totalScoreBefore:",
            f"totalScoreAfter:{parsed.score.total_score}",
            "maxComboBefore:",
            f"maxComboAfter:{parsed.score.max_combo}",
            "accuracyBefore:",
            f"accuracyAfter:{accuracy}",
            "ppBefore:",
            "ppAfter:",
            f"onlineScoreId:{score_id}",
            "\n",
            "chartId:overall",
            "chartName:Overall Ranking",
            "achievements-new:",
        )
    )


def _format_leaderboard_score(score: LeaderboardScoreView) -> str:
    return (
        f"{score.score_id}|{score.display_name}|{round(score.metric_value)}|{score.max_combo}|"
        f"{score.n50}|{score.n100}|{score.n300}|{score.nmiss}|{score.nkatu}|{score.ngeki}|"
        f"{int(score.perfect)}|{score.legacy_mod_bits}|{score.account_id}|{score.rank}|"
        f"{int(score.ended_at.timestamp())}|{int(score.has_replay)}"
    )
