"""Adapt osu!lazer solo score upload onto canonical scoring commands."""

import hashlib
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Literal

import orjson
from fastapi import APIRouter, Body, Form, Header, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from perfcho.api.cho.canonize.ipaddr import resolve_client_ip
from perfcho.api.cho.dependencies import StableServicesDependency
from perfcho.api.v2.dependencies import V2AccountDependency
from perfcho.infra.compose import StableServices
from perfcho.modules.common import Actor, ClientContext, CommandMeta
from perfcho.modules.common.errors import ApplicationError
from perfcho.modules.scoring import (
    AcceptScore,
    BeatmapReference,
    CanonicalMod,
    ClientFamily,
    HitStatistic,
    IssueSoloScoreToken,
    LeaderboardScope,
    PlayAttemptSubmission,
    Ruleset,
    ScoreAttestation,
    ScoreboardVariant,
    ScoreDetailView,
    ScoreGrade,
    ScoreOutcome,
    ScoreSubmission,
)

router = APIRouter()

_RULESETS = {
    0: Ruleset.OSU,
    1: Ruleset.TAIKO,
    2: Ruleset.FRUITS,
    3: Ruleset.MANIA,
}
_REQUIRED_HITS = {
    Ruleset.OSU: ("great", "ok", "meh", "miss"),
    Ruleset.TAIKO: ("great", "ok", "miss"),
    Ruleset.FRUITS: ("great", "large_tick_hit", "small_tick_hit", "small_tick_miss", "miss"),
    Ruleset.MANIA: ("perfect", "great", "good", "ok", "meh", "miss"),
}


class APIMod(BaseModel):
    """Describe one structured Lazer mod."""

    model_config = ConfigDict(extra="forbid")

    acronym: str = Field(min_length=1, max_length=8)
    settings: dict[str, object] = Field(default_factory=dict)


class SoloScoreSubmissionRequest(BaseModel):
    """Deserialize the score shape emitted by SoloScoreInfo.ForSubmission."""

    model_config = ConfigDict(extra="ignore")

    rank: Literal["X", "XH", "S", "SH", "A", "B", "C", "D", "F"]
    total_score: int = Field(ge=0)
    total_score_without_mods: int = Field(ge=0)
    accuracy: Decimal = Field(ge=0, le=1)
    max_combo: int = Field(ge=0)
    ruleset_id: int = Field(ge=0, le=3)
    passed: bool = False
    mods: list[APIMod] = Field(default_factory=list)
    statistics: dict[str, int] = Field(default_factory=dict)
    maximum_statistics: dict[str, int] = Field(default_factory=dict)
    pauses: list[int] = Field(default_factory=list)


class SoloScoreTokenResponse(BaseModel):
    """Return the numeric score token consumed by osu!lazer."""

    id: int


class SoloScoreResponse(BaseModel):
    """Return the minimum complete score shape consumed after submission."""

    id: int
    user_id: int
    user: dict[str, object]
    beatmap_id: int
    ruleset_id: int
    rank: str
    total_score: int
    total_score_without_mods: int
    accuracy: Decimal
    max_combo: int
    mods: list[APIMod]
    statistics: dict[str, int]
    maximum_statistics: dict[str, int]
    passed: bool
    ended_at: datetime
    position: int | None = None
    pp: Decimal | None = None
    has_replay: bool = False
    ranked: bool


@router.post(
    "/beatmaps/{beatmap_id}/solo/scores",
    response_model=SoloScoreTokenResponse,
    tags=["Gameplay"],
)
async def create_solo_score_token(
    request: Request,
    beatmap_id: int,
    services: StableServicesDependency,
    account: V2AccountDependency,
    beatmap_hash: Annotated[str, Form(min_length=32, max_length=32)],
    ruleset_id: Annotated[int, Form(ge=0, le=3)],
    version_hash: Annotated[str, Form()] = "",
    ruleset_hash: Annotated[str, Form()] = "",
    x_api_version: Annotated[str | None, Header(alias="x-api-version")] = None,
) -> SoloScoreTokenResponse | JSONResponse:
    """Issue the short-lived token required before a Lazer solo play."""
    del version_hash, ruleset_hash
    scoring = services.scoring
    if scoring is None:
        return _error(503, "service_unavailable", "Scoring is unavailable.")
    try:
        ruleset = _RULESETS[ruleset_id]
        md5 = bytes.fromhex(beatmap_hash)
        if len(md5) != 16:
            raise ValueError
        received_at = services.clock.now()
        digest = hashlib.sha256(
            orjson.dumps(
                {"beatmap_id": beatmap_id, "beatmap_hash": beatmap_hash.lower(), "ruleset_id": ruleset_id},
                option=orjson.OPT_SORT_KEYS,
            )
        ).digest()
        token = await scoring.issue_solo_token(
            IssueSoloScoreToken(
                meta=_command_meta(
                    request,
                    services,
                    account.account_id,
                    account.session_id,
                    idempotency_key=f"lazer-solo-token:{account.account_id}:{uuid.uuid7()}",
                    request_digest=digest,
                    received_at=received_at,
                    client_version=x_api_version,
                ),
                beatmap=BeatmapReference(beatmap_id=beatmap_id, md5=md5),
                ruleset=ruleset,
            )
        )
    except (ApplicationError, ValueError) as error:
        return _error(422, getattr(error, "code", "invalid_request"), str(error))
    return SoloScoreTokenResponse(id=token.token_id)


@router.put(
    "/beatmaps/{beatmap_id}/solo/scores/{token}",
    response_model=SoloScoreResponse,
    tags=["Gameplay"],
)
async def submit_solo_score(
    request: Request,
    beatmap_id: int,
    token: int,
    services: StableServicesDependency,
    account: V2AccountDependency,
    body: Annotated[SoloScoreSubmissionRequest, Body()],
    x_api_version: Annotated[str | None, Header(alias="x-api-version")] = None,
) -> SoloScoreResponse | JSONResponse:
    """Validate and atomically accept one authorized Lazer solo score."""
    scoring = services.scoring
    if scoring is None:
        return _error(503, "service_unavailable", "Scoring is unavailable.")
    try:
        ruleset = _RULESETS[body.ruleset_id]
        outcome = ScoreOutcome.PASSED if body.passed else ScoreOutcome.FAILED
        ended_at = services.clock.now()
        client_version = x_api_version or "unknown"
        hits = _hit_statistics(ruleset, body.statistics, body.maximum_statistics)
        digest_payload = body.model_dump(mode="json")
        request_digest = hashlib.sha256(orjson.dumps(digest_payload, option=orjson.OPT_SORT_KEYS)).digest()
        mods = tuple(CanonicalMod(mod.acronym, mod.settings) for mod in body.mods)
        command = AcceptScore(
            meta=_command_meta(
                request,
                services,
                account.account_id,
                account.session_id,
                idempotency_key=f"lazer-solo-score:{token}",
                request_digest=request_digest,
                received_at=ended_at,
                client_version=client_version,
            ),
            beatmap=BeatmapReference(beatmap_id=beatmap_id),
            ruleset=ruleset,
            variant=_variant(mods),
            mods=mods,
            attempt=PlayAttemptSubmission(
                idempotency_key=f"lazer-solo-attempt:{token}",
                started_at=ended_at,
                ended_at=ended_at,
                progress=Decimal(1) if body.passed else Decimal(0),
                client_metadata={"pauses": body.pauses},
            ),
            score=ScoreSubmission(
                total_score=body.total_score,
                classic_score=body.total_score_without_mods,
                accuracy=body.accuracy,
                max_combo=body.max_combo,
                grade=ScoreGrade(body.rank),
                outcome=outcome,
                perfect=False,
                hits=hits,
            ),
            replay=None,
            attestation=ScoreAttestation(
                client_family=ClientFamily.LAZER,
                client_version=client_version,
                verification_state="pending",
                evidence={},
            ),
            solo_token_id=token,
        )
        result = await scoring.accept(command)
        detail = await services.score_query.get(result.score_id) if services.score_query is not None else None
    except (ApplicationError, ValueError) as error:
        return _error(422, getattr(error, "code", "invalid_request"), str(error))

    if detail is None:
        return _error(503, "projection_unavailable", "Accepted score could not be read.")
    return _score_response(detail)


@router.get("/scores/{score_id}", response_model=SoloScoreResponse, tags=["Scores"])
async def get_score(
    score_id: int,
    services: StableServicesDependency,
) -> SoloScoreResponse | JSONResponse:
    """Return one canonical score detail."""
    return await _get_score_response(services, score_id)


@router.get("/scores/{ruleset}/{score_id}", response_model=SoloScoreResponse, tags=["Scores"])
async def get_score_for_ruleset(
    ruleset: Ruleset,
    score_id: int,
    services: StableServicesDependency,
) -> SoloScoreResponse | JSONResponse:
    """Return one score only when the URL ruleset matches."""
    return await _get_score_response(services, score_id, ruleset)


@router.get("/beatmaps/{beatmap_id}/scores", response_model=None, tags=["Scores"])
async def get_beatmap_scores(
    beatmap_id: int,
    services: StableServicesDependency,
    account: V2AccountDependency,
    mode: Annotated[Ruleset, Query()],
    type: Annotated[Literal["global", "friend", "country"], Query()] = "global",
    mods: Annotated[list[str] | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
) -> dict[str, object] | JSONResponse:
    """Return a Lazer score collection with personal score outside the top limit."""
    if services.ranking_query is None or services.score_query is None:
        return _error(503, "service_unavailable", "Ranking is unavailable.")
    variant = ScoreboardVariant.VANILLA
    acronyms = frozenset(value.strip().upper() for value in mods or ())
    assistance = acronyms & {"RX", "AP"}
    if len(assistance) > 1:
        return _error(422, "invalid_request", "RX and AP cannot be combined.")
    if "RX" in assistance:
        variant = ScoreboardVariant.RELAX
    elif "AP" in assistance:
        variant = ScoreboardVariant.AUTOPILOT
    acronyms -= assistance
    if mods is not None:
        scope = LeaderboardScope.exact_mods(acronyms)
    elif type == "friend":
        if services.social is None:
            return _error(503, "service_unavailable", "Social service is unavailable.")
        friends = await services.social.list_friends(account.account_id)
        scope = LeaderboardScope.friends(frozenset({account.account_id, *(friend.account_id for friend in friends)}))
    elif type == "country":
        if not account.country_code:
            return {"scores": [], "user_score": None, "score_count": 0}
        scope = LeaderboardScope.country(account.country_code)
    else:
        scope = LeaderboardScope.overall()
    page = await services.ranking_query.get_combined_leaderboard(
        beatmap_id=beatmap_id,
        ruleset=mode,
        variant=variant,
        scope=scope,
        requester_account_id=account.account_id,
        limit=limit,
    )
    details = {
        score.score_id: await services.score_query.get(score.score_id)
        for score in (*page.scores, *((page.personal_best,) if page.personal_best else ()))
    }
    scores = [
        _score_response(details[row.score_id]).model_dump(mode="json") for row in page.scores if details[row.score_id]
    ]
    user_score = None
    if page.personal_best is not None and details[page.personal_best.score_id] is not None:
        user_score = {
            "position": page.personal_best.rank,
            "score": _score_response(details[page.personal_best.score_id]).model_dump(mode="json"),
        }
    return {"scores": scores, "user_score": user_score, "score_count": page.total_count}


async def _get_score_response(
    services: StableServices, score_id: int, ruleset: Ruleset | None = None
) -> SoloScoreResponse | JSONResponse:
    if services.score_query is None:
        return _error(503, "service_unavailable", "Score queries are unavailable.")
    detail = await services.score_query.get(score_id, ruleset)
    if detail is None:
        return _error(404, "not_found", "Score was not found.")
    return _score_response(detail)


def _score_response(detail: ScoreDetailView) -> SoloScoreResponse:
    return SoloScoreResponse(
        id=detail.score_id,
        user_id=detail.account_id,
        user={
            "id": detail.account_id,
            "username": detail.display_name,
            "country_code": (detail.country_code or "XX").upper(),
        },
        beatmap_id=detail.beatmap_id,
        ruleset_id=list(Ruleset).index(detail.ruleset),
        rank=detail.grade.value,
        total_score=detail.total_score,
        total_score_without_mods=detail.classic_score,
        accuracy=detail.accuracy,
        max_combo=detail.max_combo,
        mods=[APIMod(acronym=mod.acronym, settings=dict(mod.settings)) for mod in detail.mods],
        statistics=dict(detail.statistics),
        maximum_statistics=dict(detail.maximum_statistics),
        passed=detail.outcome is ScoreOutcome.PASSED,
        ended_at=detail.ended_at,
        position=detail.position,
        pp=detail.pp,
        has_replay=detail.has_replay,
        ranked=detail.ranked,
    )


def _command_meta(
    request: Request,
    services: StableServices,
    account_id: int,
    session_id: uuid.UUID,
    *,
    idempotency_key: str,
    request_digest: bytes,
    received_at: datetime,
    client_version: str | None,
) -> CommandMeta:
    settings = services.settings
    return CommandMeta(
        request_id=services.id_generator.new(),
        idempotency_key=idempotency_key,
        request_digest=request_digest,
        actor=Actor(account_id, session_id),
        client=ClientContext(
            family=ClientFamily.LAZER.value,
            version=client_version or "unknown",
            variant=None,
            ip_address=resolve_client_ip(request, settings.trusted_proxy_cidrs),
            user_agent=request.headers.get("user-agent"),
        ),
        received_at=received_at,
    )


def _hit_statistics(
    ruleset: Ruleset,
    actual: dict[str, int],
    maximum: dict[str, int],
) -> tuple[HitStatistic, ...]:
    names = set(actual) | set(maximum) | set(_REQUIRED_HITS[ruleset])
    return tuple(HitStatistic(name, actual.get(name, 0), maximum.get(name)) for name in sorted(names))


def _variant(mods: tuple[CanonicalMod, ...]) -> ScoreboardVariant:
    acronyms = {mod.acronym for mod in mods}
    if "RX" in acronyms:
        return ScoreboardVariant.RELAX
    if "AP" in acronyms:
        return ScoreboardVariant.AUTOPILOT
    return ScoreboardVariant.VANILLA


def _error(status_code: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={"error": code, "message": message, "hint": message},
    )
