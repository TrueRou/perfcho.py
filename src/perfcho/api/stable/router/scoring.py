"""Adapt Stable score submission and replay transfer to canonical scoring services."""

from __future__ import annotations

import hashlib
import hmac
import struct
from collections.abc import AsyncGenerator, AsyncIterator
from dataclasses import dataclass, replace
from datetime import timedelta
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status
from fastapi.responses import StreamingResponse
from starlette.datastructures import FormData, UploadFile
from starlette.formparsers import MultiPartException, MultiPartParser

from perfcho.api.stable.client_ip import resolve_client_ip
from perfcho.api.stable.dependencies import StableServicesDependency
from perfcho.api.stable.score_submission import (
    ParsedStableScore,
    decrypt_stable_score,
    verify_stable_online_checksum,
)
from perfcho.modules.common import (
    Actor,
    ApplicationError,
    ClientContext,
    CommandMeta,
    ObjectStorage,
    ObjectUnavailable,
)
from perfcho.modules.content import BeatmapNotFound, BeatmapRevisionView, ContentQueryService
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
    ScoringService,
    StagedReplayManifest,
)
from perfcho.modules.scoring.mods import parse_legacy_mods
from perfcho.modules.social import SocialService

router = APIRouter(include_in_schema=False, default_response_class=Response)

_RULESETS = (Ruleset.OSU, Ruleset.TAIKO, Ruleset.FRUITS, Ruleset.MANIA)
_RULESET_IDS = {ruleset.value: index for index, ruleset in enumerate(_RULESETS)}
_MIN_STABLE_REPLAY_BYTES = 24
_MAX_STABLE_ELAPSED_MS = 7 * 24 * 60 * 60 * 1000
_MAX_STABLE_SUBMISSION_AGE = timedelta(days=30)
_MAX_STABLE_CLOCK_SKEW = timedelta(minutes=5)


@dataclass(frozen=True, slots=True)
class _SubmissionForm:
    encrypted_score: str
    replay: UploadFile
    exited: bool
    fail_time_ms: int
    score_time_ms: int
    password_token: str
    osu_version: str
    encrypted_client_hash: str
    iv: str
    updated_beatmap_hash: str
    storyboard_hash: str | None
    unique_ids: str


@dataclass(frozen=True, slots=True)
class _ReplayAuthentication:
    principal: StableWebPrincipal | None


def _scoring_service(services: StableServicesDependency) -> ScoringService:
    if services.scoring is None:
        raise RuntimeError("Stable scoring is not configured")
    return services.scoring


def _content_query(services: StableServicesDependency) -> ContentQueryService:
    if services.content_query is None:
        raise RuntimeError("Stable content queries are not configured")
    return services.content_query


def _object_storage(services: StableServicesDependency) -> ObjectStorage:
    if services.object_storage is None:
        raise RuntimeError("Stable object storage is not configured")
    return services.object_storage


def _replay_query(services: StableServicesDependency) -> ReplayQueryService:
    if services.replay_query is None:
        raise RuntimeError("Stable replay queries are not configured")
    return services.replay_query


def _replay_service(services: StableServicesDependency) -> ReplayService:
    if services.replay is None:
        raise RuntimeError("Stable replay commands are not configured")
    return services.replay


def _ranking_query(services: StableServicesDependency) -> RankingQueryService:
    if services.ranking_query is None:
        raise RuntimeError("Stable ranking queries are not configured")
    return services.ranking_query


def _social_service(services: StableServicesDependency) -> SocialService:
    if services.social is None:
        raise RuntimeError("Stable social services are not configured")
    return services.social


async def _replay_authentication(
    services: StableServicesDependency,
    username: Annotated[str, Query(alias="u", min_length=1, max_length=64)],
    password_token: Annotated[str, Query(alias="h", min_length=32, max_length=32)],
) -> _ReplayAuthentication:
    return _ReplayAuthentication(await _verify_online_web_principal(services, username, password_token))


async def _leaderboard_authentication(
    services: StableServicesDependency,
    username: Annotated[str, Query(alias="us", min_length=1, max_length=64)],
    password_token: Annotated[str, Query(alias="ha", min_length=32, max_length=32)],
) -> _ReplayAuthentication:
    return _ReplayAuthentication(await _verify_online_web_principal(services, username, password_token))


@router.post("/web/osu-submit-modular-selector.php")
async def submit_score(
    request: Request,
    services: StableServicesDependency,
    scoring: Annotated[ScoringService, Depends(_scoring_service)],
    content_query: Annotated[ContentQueryService, Depends(_content_query)],
    object_storage: Annotated[ObjectStorage, Depends(_object_storage)],
) -> Response:
    """Decrypt, authenticate, stage, and atomically accept one Stable score."""
    try:
        form = await _submission_form(request, services.settings.stable_score_submission_max_bytes)
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
        _validate_submission_evidence(form, parsed)
    except HTTPException, MultiPartException, ValueError:
        return Response(b"error: no")

    principal = await _authenticate_submission(services, parsed, form.password_token)
    if principal is None:
        return Response(b"error: pass")
    try:
        verify_stable_online_checksum(
            parsed,
            osu_version=form.osu_version,
            storyboard_hash=form.storyboard_hash,
            username=principal.current_name,
        )
    except ValueError:
        return Response(b"error: no")
    try:
        beatmap = await content_query.lookup_md5(parsed.beatmap_md5)
    except BeatmapNotFound:
        return Response(b"error: beatmap")
    received_at = services.clock.now()
    if not received_at - _MAX_STABLE_SUBMISSION_AGE <= parsed.attempt.ended_at <= received_at + _MAX_STABLE_CLOCK_SKEW:
        return Response(b"error: no")

    try:
        replay_content = await _read_replay(form.replay, services.settings.stable_replay_max_bytes)
    except ValueError:
        return Response(b"error: no")
    finally:
        await form.replay.close()
    replay_digest = hashlib.sha256(replay_content).digest()
    storage_key = f"replays/stable/{principal.account_id}/{replay_digest.hex()}.osr"
    try:
        stored = await object_storage.put(
            storage_key,
            replay_content,
            media_type="application/octet-stream",
            expected_sha256=replay_digest,
        )
    except ObjectUnavailable:
        return Response(b"")

    request_digest = _submission_digest(form, replay_digest)
    request_id = services.id_generator.new()
    online_checksum = parsed.score.online_checksum
    if online_checksum is None:
        return Response(b"error: no")
    attestation = replace(
        parsed.attestation,
        checksum=online_checksum,
        client_integrity_digest=hashlib.sha256(parsed.client_hash.encode()).digest(),
        evidence={
            **dict(parsed.attestation.evidence),
            "online_checksum": "verified",
            "updated_beatmap_hash": "verified",
            "storyboard_hash": "format_valid_authoritative_match_pending"
            if form.storyboard_hash is not None
            else "not_supplied",
            "client_hash": "format_valid_authoritative_session_match_pending",
            "unique_ids": "format_valid_authoritative_session_match_pending",
            "unique_ids_digest": hashlib.sha256(form.unique_ids.encode()).hexdigest(),
        },
    )
    command = AcceptScore(
        meta=CommandMeta(
            request_id=request_id,
            idempotency_key=f"stable-score:{principal.account_id}:{online_checksum.hex()}",
            request_digest=request_digest,
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
        attestation=attestation,
        multiplayer=None,
    )
    try:
        if services.multiplayer is not None:
            command = replace(
                command,
                multiplayer=await services.multiplayer.resolve_submission_context(
                    principal.account_id,
                    beatmap.revision_id,
                ),
            )
        result = await scoring.accept(command)
    except BeatmapRevisionNotFound:
        return Response(b"error: beatmap")
    except ApplicationError, ValueError:
        return Response(b"error: no")
    if result.outcome.value != "passed":
        return Response(b"error: no")
    return Response(_submission_chart(result.score_id, beatmap, parsed))


@router.get("/web/osu-getreplay.php")
async def get_replay(
    services: StableServicesDependency,
    authentication: Annotated[_ReplayAuthentication, Depends(_replay_authentication)],
    replay_query: Annotated[ReplayQueryService, Depends(_replay_query)],
    replay_service: Annotated[ReplayService, Depends(_replay_service)],
    object_storage: Annotated[ObjectStorage, Depends(_object_storage)],
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
    authentication: Annotated[_ReplayAuthentication, Depends(_leaderboard_authentication)],
    content_query: Annotated[ContentQueryService, Depends(_content_query)],
    ranking_query: Annotated[RankingQueryService, Depends(_ranking_query)],
    social: Annotated[SocialService, Depends(_social_service)],
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
    ruleset = _RULESETS[mode]
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
            f"{_leaderboard_status(beatmap.status)}|false|{beatmap.external_beatmap_id}|"
            f"{beatmap.external_beatmapset_id}|{len(page.scores)}|0|"
        ),
        f"0\n{beatmap.artist} - {beatmap.title} [{beatmap.difficulty_name}]\n{average_rating}",
        _format_leaderboard_score(page.personal_best) if page.personal_best is not None else "",
    ]
    lines.extend(_format_leaderboard_score(score) for score in page.scores)
    if not page.scores:
        lines.append("")
    return Response("\n".join(lines))


async def _submission_form(request: Request, maximum: int) -> _SubmissionForm:
    content_length = request.headers.get("content-length")
    if content_length is not None and (
        not content_length.isascii() or not content_length.isdigit() or int(content_length) > maximum
    ):
        raise MultiPartException("Stable score submission is too large")
    if request.headers.get("content-type", "").partition(";")[0].strip().casefold() != "multipart/form-data":
        raise MultiPartException("Stable score submission must be multipart")
    parser = MultiPartParser(
        request.headers,
        _limited_multipart_stream(request, maximum),
        max_files=2,
        max_fields=32,
        max_part_size=maximum,
    )
    form = await parser.parse()
    return _parse_submission_form(form)


async def _limited_multipart_stream(request: Request, maximum: int) -> AsyncGenerator[bytes]:
    total = 0
    async for chunk in request.stream():
        total += len(chunk)
        if total > maximum:
            raise MultiPartException("Stable score submission is too large")
        yield chunk


def _parse_submission_form(form: FormData) -> _SubmissionForm:
    score_parts = form.getlist("score")
    encrypted_score = next((part for part in score_parts if isinstance(part, str)), None)
    replay = next((part for part in score_parts if isinstance(part, UploadFile)), None)
    if encrypted_score is None or replay is None or len(score_parts) != 2:
        raise ValueError("Stable score multipart fields are invalid")
    return _SubmissionForm(
        encrypted_score=encrypted_score,
        replay=replay,
        exited=_form_boolean(form, "x"),
        fail_time_ms=_form_integer(form, "ft", maximum=_MAX_STABLE_ELAPSED_MS),
        score_time_ms=_form_integer(form, "st", maximum=_MAX_STABLE_ELAPSED_MS),
        password_token=_form_text(form, "pass", maximum=32),
        osu_version=_form_text(form, "osuver", maximum=16),
        encrypted_client_hash=_form_text(form, "s", maximum=16_384),
        iv=_form_text(form, "iv", maximum=1024),
        updated_beatmap_hash=_form_text(form, "bmk", maximum=128),
        storyboard_hash=_optional_form_text(form, "sbk", maximum=128),
        unique_ids=_form_text(form, "c1", maximum=2048),
    )


def _form_text(form: FormData, key: str, *, maximum: int) -> str:
    value = form.get(key)
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise ValueError(f"Stable score field {key} is invalid")
    return value


def _optional_form_text(form: FormData, key: str, *, maximum: int) -> str | None:
    value = form.get(key)
    if value is None or value == "":
        return None
    if not isinstance(value, str) or len(value) > maximum:
        raise ValueError(f"Stable score field {key} is invalid")
    return value


def _form_integer(form: FormData, key: str, *, maximum: int) -> int:
    value = _form_text(form, key, maximum=20)
    if not value.isascii() or not value.isdigit():
        raise ValueError(f"Stable score field {key} is invalid")
    result = int(value)
    if result > maximum:
        raise ValueError(f"Stable score field {key} is outside its supported range")
    return result


def _form_boolean(form: FormData, key: str) -> bool:
    value = _form_text(form, key, maximum=5)
    if value in {"1", "True"}:
        return True
    if value in {"0", "False"}:
        return False
    raise ValueError(f"Stable score field {key} is invalid")


async def _authenticate_submission(
    services: StableServicesDependency,
    parsed: ParsedStableScore,
    password_token: str,
) -> StableWebPrincipal | None:
    return await _verify_online_web_principal(services, parsed.username, password_token)


async def _verify_online_web_principal(
    services: StableServicesDependency,
    username: str,
    password_token: str,
) -> StableWebPrincipal | None:
    try:
        principal = await services.identity.verify_stable_web(username, password_token)
        realtime = await services.realtime.resolve_session(principal.session_id, at=services.clock.now())
        if realtime.account_id != principal.account_id:
            raise RealtimeSessionFenced("Stable Web principal does not own the realtime session")
        return principal
    except InvalidCredentials, RealtimeSessionNotFound, RealtimeSessionFenced:
        return None


async def _read_replay(upload: UploadFile, maximum: int) -> bytes:
    content = await upload.read(maximum + 1)
    if len(content) > maximum:
        raise ValueError("Stable replay exceeds the configured limit")
    if len(content) < _MIN_STABLE_REPLAY_BYTES:
        raise ValueError("Stable replay does not contain its minimum structure")
    return content


def _validate_submission_evidence(form: _SubmissionForm, parsed: ParsedStableScore) -> None:
    updated_beatmap_hash = _md5_bytes(form.updated_beatmap_hash, "bmk")
    if not hmac.compare_digest(updated_beatmap_hash, parsed.beatmap_md5):
        raise ValueError("Stable updated beatmap hash does not match the score")
    if form.storyboard_hash is not None:
        _md5_bytes(form.storyboard_hash, "sbk")
    identifiers = form.unique_ids.split("|")
    if len(identifiers) != 2 or any(
        not value or len(value) > 1024 or not all(character.isprintable() for character in value)
        for value in identifiers
    ):
        raise ValueError("Stable unique client identifiers are invalid")


def _md5_bytes(value: str, field_name: str) -> bytes:
    if len(value) != 32:
        raise ValueError(f"Stable score field {field_name} is not an MD5")
    try:
        digest = bytes.fromhex(value)
    except ValueError as error:
        raise ValueError(f"Stable score field {field_name} is not an MD5") from error
    if len(digest) != 16:
        raise ValueError(f"Stable score field {field_name} is not an MD5")
    return digest


def _submission_digest(form: _SubmissionForm, replay_digest: bytes) -> bytes:
    fields = (
        ("score", form.encrypted_score.encode()),
        ("replay_sha256", replay_digest),
        ("x", str(int(form.exited)).encode()),
        ("ft", str(form.fail_time_ms).encode()),
        ("st", str(form.score_time_ms).encode()),
        ("pass", form.password_token.encode()),
        ("osuver", form.osu_version.encode()),
        ("s", form.encrypted_client_hash.encode()),
        ("iv", form.iv.encode()),
        ("bmk", form.updated_beatmap_hash.encode()),
        ("sbk", (form.storyboard_hash or "").encode()),
        ("c1", form.unique_ids.encode()),
    )
    digest = hashlib.sha256()
    for name, value in fields:
        encoded_name = name.encode()
        digest.update(struct.pack(">H", len(encoded_name)))
        digest.update(encoded_name)
        digest.update(struct.pack(">Q", len(value)))
        digest.update(value)
    return digest.digest()


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


def _leaderboard_status(value: str) -> int:
    return {
        "graveyard": 0,
        "wip": 0,
        "pending": 0,
        "ranked": 2,
        "approved": 3,
        "qualified": 4,
        "loved": 5,
    }.get(value, 0)


def _format_leaderboard_score(score: LeaderboardScoreView) -> str:
    return (
        f"{score.score_id}|{score.display_name}|{round(score.metric_value)}|{score.max_combo}|"
        f"{score.n50}|{score.n100}|{score.n300}|{score.nmiss}|{score.nkatu}|{score.ngeki}|"
        f"{int(score.perfect)}|{score.legacy_mod_bits}|{score.account_id}|{score.rank}|"
        f"{int(score.ended_at.timestamp())}|{int(score.has_replay)}"
    )
