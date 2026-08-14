"""Adapt osu!lazer multiplayer room endpoints onto the multiplayer service."""

import hashlib
import uuid
from typing import Annotated

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from perfcho.api.canonical.dependencies import CanonicalAccountDependency, CanonicalServicesDependency
from perfcho.api.canonical.router._multiplayer import multiplayer_room
from perfcho.api.canonical.router._shared import error
from perfcho.modules.common import Actor, ClientContext, CommandMeta
from perfcho.modules.multiplayer import (
    CreateRoom,
    JoinRoom,
    LeaveRoom,
    MatchNotFound,
    MatchPasswordRejected,
    RoomSettings,
    TeamMode,
    WinCondition,
)
from perfcho.modules.scoring import CanonicalMod, Ruleset, ScoreboardVariant

router = APIRouter()

_MATCH_TYPES = {
    "head_to_head": TeamMode.HEAD_TO_HEAD,
    "team_vs": TeamMode.TEAM_VS,
    "tag_coop": TeamMode.TAG_COOP,
    "tag_team_vs": TeamMode.TAG_TEAM_VS,
}
_WIN_CONDITIONS = {
    "score": WinCondition.SCORE,
    "accuracy": WinCondition.ACCURACY,
    "combo": WinCondition.COMBO,
    "score_v2": WinCondition.SCORE_V2,
}
_RULESET_IDS = {0: Ruleset.OSU, 1: Ruleset.TAIKO, 2: Ruleset.FRUITS, 3: Ruleset.MANIA}


class LazerRoomSettings(BaseModel):
    """Deserialize the lazer MultiplayerRoomSettings shape."""

    model_config = ConfigDict(extra="ignore")

    name: str = Field(default="Unnamed room", max_length=255)
    playlistItemId: int = 0
    password: str = Field(default="", max_length=64)
    matchType: str = Field(default="head_to_head")
    queueMode: str = Field(default="host_only")
    maxParticipants: int | None = None


class LazerRoom(BaseModel):
    """Deserialize the lazer MultiplayerRoom creation shape."""

    model_config = ConfigDict(extra="ignore")

    roomID: int = 0
    settings: LazerRoomSettings = Field(default_factory=LazerRoomSettings)
    playlist: list[dict[str, object]] = Field(default_factory=list)


@router.get("/rooms", response_model=None, tags=["Multiplayer"])
async def list_rooms(
    services: CanonicalServicesDependency,
    account: CanonicalAccountDependency,
    mode: Annotated[str, Query()] = "all",
) -> dict[str, object] | JSONResponse:
    """Return the public room list."""
    del account, mode
    if services.multiplayer is None:
        return error(503, "service_unavailable", "Multiplayer is unavailable.")
    rooms = await services.multiplayer.list_public_rooms(limit=100)
    return {"rooms": [multiplayer_room(state) for state in rooms]}


@router.post("/rooms", response_model=None, tags=["Multiplayer"])
async def create_room(
    body: LazerRoom,
    services: CanonicalServicesDependency,
    account: CanonicalAccountDependency,
) -> dict[str, object] | JSONResponse:
    """Create a room and return its projection."""
    if services.multiplayer is None:
        return error(503, "service_unavailable", "Multiplayer is unavailable.")
    settings = _settings_from_lazer(body, account.account_id)
    state = await services.multiplayer.create_room(
        CreateRoom(
            meta=_meta(services, account, "create"),
            settings=settings,
            capacity=body.settings.maxParticipants or 16,
            password=body.settings.password,
        )
    )
    return multiplayer_room(state)


@router.get("/rooms/{room_id}", response_model=None, tags=["Multiplayer"])
async def get_room(
    room_id: int,
    services: CanonicalServicesDependency,
    account: CanonicalAccountDependency,
) -> dict[str, object] | JSONResponse:
    """Return one room projection."""
    del account
    if services.multiplayer is None:
        return error(503, "service_unavailable", "Multiplayer is unavailable.")
    try:
        state = await services.multiplayer.get_room(room_id)
    except MatchNotFound:
        return error(404, "not_found", "Room was not found.")
    return multiplayer_room(state)


@router.put("/rooms/{room_id}/users/{user_id}", response_model=None, tags=["Multiplayer"])
async def join_room(
    room_id: int,
    user_id: int,
    services: CanonicalServicesDependency,
    account: CanonicalAccountDependency,
    password: Annotated[str | None, Query(max_length=64)] = None,
) -> dict[str, object] | JSONResponse:
    """Join a room."""
    if services.multiplayer is None:
        return error(503, "service_unavailable", "Multiplayer is unavailable.")
    if account.account_id != user_id:
        return error(403, "forbidden", "Cannot join a room on behalf of another user.")
    try:
        state = await services.multiplayer.join_room(
            JoinRoom(_meta(services, account, "join"), room_id, password or "")
        )
    except MatchNotFound:
        return error(404, "not_found", "Room was not found.")
    except MatchPasswordRejected:
        return error(403, "forbidden", "Room password is incorrect.")
    return multiplayer_room(state)


@router.delete("/rooms/{room_id}/users/{user_id}", response_model=None, tags=["Multiplayer"])
async def leave_room(
    room_id: int,
    user_id: int,
    services: CanonicalServicesDependency,
    account: CanonicalAccountDependency,
) -> JSONResponse:
    """Leave a room."""
    if services.multiplayer is None:
        return error(503, "service_unavailable", "Multiplayer is unavailable.")
    if account.account_id != user_id:
        return error(403, "forbidden", "Cannot leave a room on behalf of another user.")
    await services.multiplayer.leave_room(LeaveRoom(_meta(services, account, "leave"), room_id))
    return JSONResponse(status_code=204, content=None)


@router.delete("/rooms/{room_id}", response_model=None, tags=["Multiplayer"])
async def close_room(
    room_id: int,
    services: CanonicalServicesDependency,
    account: CanonicalAccountDependency,
) -> JSONResponse:
    """Close a room (host leaves, ending the room)."""
    if services.multiplayer is None:
        return error(503, "service_unavailable", "Multiplayer is unavailable.")
    state = await services.multiplayer.get_room(room_id)
    if state.room.host_account_id != account.account_id:
        return error(403, "forbidden", "Only the host can close the room.")
    await services.multiplayer.leave_room(LeaveRoom(_meta(services, account, "close"), room_id))
    return JSONResponse(status_code=204, content=None)


def _settings_from_lazer(body: LazerRoom, host_account_id: int) -> RoomSettings:
    del host_account_id
    first = body.playlist[0] if body.playlist else {}
    beatmap_id = _as_int(first.get("beatmapID"), default=-1)
    checksum = str(first.get("beatmapChecksum") or "")
    ruleset_id = _as_int(first.get("rulesetID"), default=0)
    ruleset = _RULESET_IDS.get(ruleset_id, Ruleset.OSU)
    required_mods = first.get("requiredMods") or []
    if not isinstance(required_mods, list):
        required_mods = []
    required = [
        CanonicalMod(str(mod.get("acronym", "")), dict(mod.get("settings", {}) or {}))
        for mod in required_mods
        if isinstance(mod, dict)
    ]
    return RoomSettings(
        name=body.settings.name,
        beatmap_name="",
        external_beatmap_id=beatmap_id,
        beatmap_md5=bytes.fromhex(checksum) if checksum else None,
        ruleset=ruleset,
        variant=ScoreboardVariant.VANILLA,
        team_mode=_MATCH_TYPES.get(body.settings.matchType, TeamMode.HEAD_TO_HEAD),
        win_condition=WinCondition.SCORE,
        mods=tuple(required),
        free_mods=True,
    )


def _as_int(value: object, *, default: int) -> int:
    """Coerce a nullable wire value to an int, falling back to ``default``."""
    if isinstance(value, bool) or value is None:
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return default
    return default


def _meta(services: CanonicalServicesDependency, account: CanonicalAccountDependency, operation: str) -> CommandMeta:
    digest = hashlib.sha256(f"lazer-multiplayer:{operation}:{account.account_id}".encode()).digest()
    return CommandMeta(
        request_id=services.id_generator.new(),
        idempotency_key=f"lazer-multiplayer:{operation}:{account.session_id}:{uuid.uuid7()}",
        request_digest=digest,
        actor=Actor(account.account_id, account.session_id),
        client=ClientContext(family="lazer", version=None, variant=None, ip_address="127.0.0.1"),
        received_at=services.clock.now(),
    )
