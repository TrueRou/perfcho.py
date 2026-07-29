import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest
from redis.asyncio import Redis

from perfcho.infra.redis.multiplayer import RedisMultiplayerStateRepository
from perfcho.modules.multiplayer import (
    MatchAlreadyJoined,
    RoomRecord,
    RoomSettings,
    RoomSlot,
    RoomState,
    SlotStatus,
    TeamMode,
    WinCondition,
)
from perfcho.modules.scoring import Ruleset, ScoreboardVariant

NOW = datetime(2026, 7, 29, 12, tzinfo=UTC)


def state() -> RoomState:
    settings = RoomSettings(
        "Room",
        "Map",
        1,
        b"m" * 16,
        Ruleset.OSU,
        ScoreboardVariant.VANILLA,
        TeamMode.HEAD_TO_HEAD,
        WinCondition.SCORE,
    )
    room = RoomRecord(
        uuid.uuid7(),
        9,
        uuid.uuid7(),
        1,
        10,
        10,
        2,
        settings,
        True,
        "salt",
        "verifier",
    )
    return RoomState(
        room,
        1,
        (RoomSlot(0, SlotStatus.NOT_READY, 10), RoomSlot(1, SlotStatus.OPEN)),
        False,
        NOW + timedelta(minutes=10),
    )


@pytest.mark.asyncio
async def test_create_uses_atomic_cas_and_strips_password_proof() -> None:
    redis = AsyncMock(spec=Redis)
    scripts = [AsyncMock(return_value=[b"OK"]), AsyncMock(return_value=[b"OK"])]
    redis.register_script.side_effect = scripts
    pipeline = MagicMock()
    pipeline.__aenter__ = AsyncMock(return_value=pipeline)
    pipeline.__aexit__ = AsyncMock(return_value=None)
    pipeline.execute = AsyncMock(return_value=[0, []])
    redis.pipeline.return_value = pipeline
    repository = RedisMultiplayerStateRepository(redis, prefix="tests", state_ttl=timedelta(hours=1), max_rooms=4)

    created = await repository.create(state())

    assert created.room.password_protected
    assert created.room.password_salt is None
    assert created.room.password_verifier is None
    call = scripts[0].await_args
    assert call is not None
    payload = call.kwargs["args"][1]
    assert b"verifier" not in payload and b"salt" not in payload
    assert call.kwargs["keys"][-1] == "tests:v1:multiplayer:account:10"


@pytest.mark.asyncio
async def test_create_maps_account_conflict_to_domain_error() -> None:
    redis = AsyncMock(spec=Redis)
    scripts = [AsyncMock(return_value=[b"ACCOUNT_CONFLICT"]), AsyncMock(return_value=[b"OK"])]
    redis.register_script.side_effect = scripts
    pipeline = MagicMock()
    pipeline.__aenter__ = AsyncMock(return_value=pipeline)
    pipeline.__aexit__ = AsyncMock(return_value=None)
    pipeline.execute = AsyncMock(return_value=[0, []])
    redis.pipeline.return_value = pipeline
    repository = RedisMultiplayerStateRepository(redis, prefix="tests", state_ttl=3600, max_rooms=4)

    with pytest.raises(MatchAlreadyJoined):
        await repository.create(state())
