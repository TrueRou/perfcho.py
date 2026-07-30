from datetime import timedelta

import pytest
from pydantic import SecretStr
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import async_sessionmaker

from perfcho.infra.redis.realtime import RedisRealtimeRepository
from perfcho.infra.settings import Settings


@pytest.mark.asyncio
async def test_stable_composition_wires_independent_security_keys() -> None:
    pytest.importorskip("perfcho.infra.redis.realtime", reason="realtime adapter is being changed in parallel")
    from perfcho.infra import composition

    token_key = "composition-token-hmac-key"
    match_password_key = "composition-match-password-hmac-key"
    admission_key = "composition-admission-hmac-key"
    config = Settings(
        token_hmac_key=SecretStr(token_key),
        match_password_hmac_key=SecretStr(match_password_key),
        admission_hmac_key=SecretStr(admission_key),
        stable_session_stale_grace_seconds=180,
        redis_realtime_max_channels_per_session=17,
        redis_spectator_max_viewers=23,
    )
    session_factory = async_sessionmaker(expire_on_commit=False)
    redis = Redis()
    try:
        async with composition.compose_stable_services(session_factory, redis, config=config) as services:
            assert services.identity._token_hmac_key == token_key.encode()
            assert services.identity._stable_session_stale_grace == timedelta(seconds=180)
            assert services.multiplayer is not None
            assert services.multiplayer._password_key == match_password_key.encode()
            assert services.multiplayer._admission_key == admission_key.encode()
            assert services.identity._token_hmac_key != services.multiplayer._password_key
            assert services.identity._token_hmac_key != services.multiplayer._admission_key
            assert services.multiplayer._password_key != services.multiplayer._admission_key
            assert services.community is not None
            assert services.community._active_memberships is services.realtime
            assert isinstance(services.realtime, RedisRealtimeRepository)
            assert services.realtime._max_channels_per_session == 17
            assert services.realtime._max_spectators_per_host == 23
    finally:
        await redis.aclose()


def test_stable_session_stale_grace_has_a_bounded_default() -> None:
    config = Settings()

    assert config.stable_session_stale_grace_seconds == 120
