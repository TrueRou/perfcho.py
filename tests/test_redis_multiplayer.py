import asyncio
import os
import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest
from redis.asyncio import Redis

from perfcho.infra.redis.multiplayer import RedisMultiplayerStateRepository
from perfcho.modules.multiplayer import (
    MatchAlreadyJoined,
    MatchConcurrencyConflict,
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


def state(
    *,
    public_id: int = 9,
    host_account_id: int = 10,
    session_id: uuid.UUID | None = None,
    at: datetime = NOW,
) -> RoomState:
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
        public_id,
        session_id or uuid.uuid7(),
        1,
        host_account_id,
        host_account_id,
        2,
        settings,
        True,
        "salt",
        "verifier",
    )
    return RoomState(
        room,
        1,
        (RoomSlot(0, SlotStatus.NOT_READY, host_account_id), RoomSlot(1, SlotStatus.OPEN)),
        False,
        at + timedelta(minutes=10),
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
    payload = call.kwargs["args"][3]
    assert b"verifier" not in payload and b"salt" not in payload
    assert call.kwargs["keys"][-1] == "tests:v2:multiplayer:account:10"


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


@pytest.mark.skipif(not os.getenv("TEST_REDIS_URL"), reason="TEST_REDIS_URL is not configured")
@pytest.mark.asyncio
async def test_real_redis_fences_reused_public_id_and_expires_lobby_index() -> None:
    redis = Redis.from_url(os.environ["TEST_REDIS_URL"], decode_responses=False)
    prefix = f"tests:multiplayer:{uuid.uuid4()}"
    repository = RedisMultiplayerStateRepository(redis, prefix=prefix, state_ttl=30, max_rooms=4)
    try:
        now = datetime.now(UTC)
        old = await repository.create(state(session_id=uuid.uuid7(), at=now))
        replacement = state(session_id=uuid.uuid7(), at=now)
        current = await repository.create(replacement)

        assert current.room.session_id == replacement.room.session_id
        assert await redis.pttl(f"{prefix}:v2:multiplayer:rooms") > 0
        with pytest.raises(MatchConcurrencyConflict):
            await repository.replace(
                old,
                expected_state_revision=old.state_revision,
                expected_session_id=old.room.session_id,
            )
    finally:
        await redis.aclose()


@pytest.mark.skipif(not os.getenv("TEST_REDIS_URL"), reason="TEST_REDIS_URL is not configured")
@pytest.mark.asyncio
async def test_real_redis_concurrent_rooms_cannot_claim_one_account() -> None:
    redis = Redis.from_url(os.environ["TEST_REDIS_URL"], decode_responses=False)
    prefix = f"tests:multiplayer:{uuid.uuid4()}"
    repository = RedisMultiplayerStateRepository(redis, prefix=prefix, state_ttl=30, max_rooms=4)
    try:
        now = datetime.now(UTC)
        first = await repository.create(state(public_id=9, host_account_id=10, at=now))
        second = await repository.create(state(public_id=10, host_account_id=20, at=now))
        results = await asyncio.gather(
            repository.join(first.room, account_id=30, expires_at=now + timedelta(seconds=20)),
            repository.join(second.room, account_id=30, expires_at=now + timedelta(seconds=20)),
            return_exceptions=True,
        )

        assert sum(isinstance(result, RoomState) for result in results) == 1
        assert sum(isinstance(result, MatchAlreadyJoined) for result in results) == 1
    finally:
        await redis.aclose()
