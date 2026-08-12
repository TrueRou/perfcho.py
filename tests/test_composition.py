from datetime import timedelta
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from apscheduler import AsyncScheduler
from pydantic import SecretStr
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from perfcho.infra.cache import RedisCache
from perfcho.infra.compose import CoreServices, compose_core_services, compose_stable_services
from perfcho.infra.redis.realtime import RedisRealtimeStateRepository
from perfcho.infra.settings import Settings
from perfcho.modules.common.ports import Clock, IdGenerator
from perfcho.modules.realtime import RealtimeBubbleBus, RealtimePollGate


@pytest.mark.asyncio
async def test_stable_composition_wires_independent_security_keys() -> None:
    pytest.importorskip("perfcho.infra.redis.realtime", reason="realtime adapter is being changed in parallel")
    token_key = "composition-token-hmac-key"
    match_password_key = "composition-match-password-hmac-key"
    admission_key = "composition-admission-hmac-key"
    config = Settings(
        token_hmac_key=SecretStr(token_key),
        match_password_hmac_key=SecretStr(match_password_key),
        admission_hmac_key=SecretStr(admission_key),
        client_session_stale_grace_seconds=180,
        redis_realtime_max_channels_per_session=17,
        redis_spectator_max_viewers=23,
    )
    session_factory = async_sessionmaker(expire_on_commit=False)
    redis = Redis()
    try:
        core = CoreServices(
            config=config,
            clock=cast(Clock, MagicMock()),
            id_generator=cast(IdGenerator, MagicMock()),
            state_redis=redis,
            cache_redis=redis,
            cache=RedisCache(redis, prefix=config.redis_cache_prefix),
            postgres=cast(AsyncEngine, MagicMock()),
            session_factory=session_factory,
            scheduler=cast(AsyncScheduler, MagicMock()),
        )
        services = await compose_stable_services(core)
        assert services.account is not None
        assert services.bot is not None
        assert services.bot.bot_name == "BanchoBot"
        assert {command.name for command in services.bot.registry.get_commands()} == {
            "roll",
            "server",
            "reconnect",
            "quit",
            "help",
        }
        assert {group.name for group in services.bot.registry.get_groups()} == {"mp", "pool", "clan"}
        assert services.identity._token_hmac_key == token_key.encode()
        assert services.identity._client_session_stale_grace == timedelta(seconds=180)
        assert services.multiplayer is not None
        assert services.multiplayer._password_key == match_password_key.encode()
        assert services.multiplayer._admission_key == admission_key.encode()
        assert services.identity._token_hmac_key != services.multiplayer._password_key
        assert services.identity._token_hmac_key != services.multiplayer._admission_key
        assert services.multiplayer._password_key != services.multiplayer._admission_key
        assert services.community is not None
        assert services.community._active_memberships is services.realtime
        assert isinstance(services.realtime, RedisRealtimeStateRepository)
        assert services.realtime._max_channels_per_session == 17
        assert services.realtime._max_spectators_per_host == 23
        assert isinstance(services.bubbles, RealtimeBubbleBus)
        assert isinstance(services.poll_gate, RealtimePollGate)
    finally:
        await redis.aclose()


def test_client_session_stale_grace_has_a_bounded_default() -> None:
    config = Settings()

    assert config.client_session_stale_grace_seconds == 120


async def test_core_shutdown_attempts_every_cleanup_after_failures() -> None:
    scheduler = MagicMock()
    state_redis = MagicMock(aclose=AsyncMock(side_effect=RuntimeError("state close failed")))
    cache_redis = MagicMock(aclose=AsyncMock())
    bubble_redis = MagicMock(aclose=AsyncMock())
    postgres = MagicMock(dispose=AsyncMock(side_effect=RuntimeError("postgres close failed")))
    core = CoreServices(
        config=Settings(),
        clock=cast(Clock, MagicMock()),
        id_generator=cast(IdGenerator, MagicMock()),
        state_redis=state_redis,
        cache_redis=cache_redis,
        cache=cast(RedisCache, MagicMock()),
        postgres=postgres,
        session_factory=async_sessionmaker(expire_on_commit=False),
        scheduler=scheduler,
        bubble_redis=bubble_redis,
    )

    with (
        patch("perfcho.infra.compose.stop_scheduler", AsyncMock(side_effect=RuntimeError("scheduler failed"))) as stop,
        pytest.raises(BaseExceptionGroup) as raised,
    ):
        await core.aclose()

    assert len(raised.value.exceptions) == 3
    stop.assert_awaited_once_with(scheduler)
    cache_redis.aclose.assert_awaited_once()
    state_redis.aclose.assert_awaited_once()
    bubble_redis.aclose.assert_awaited_once()
    postgres.dispose.assert_awaited_once()


async def test_core_startup_failure_closes_resources_already_created() -> None:
    postgres = MagicMock(dispose=AsyncMock())
    state_redis = MagicMock(aclose=AsyncMock())
    bubble_redis = MagicMock(aclose=AsyncMock())

    with (
        patch("perfcho.infra.compose.infra_db.create_engine", AsyncMock(return_value=postgres)),
        patch("perfcho.infra.compose.infra_redis.create_state_redis", AsyncMock(return_value=state_redis)),
        patch("perfcho.infra.compose.infra_redis.create_bubble_redis", AsyncMock(return_value=bubble_redis)),
        patch(
            "perfcho.infra.compose.infra_redis.create_cache_redis",
            AsyncMock(side_effect=RuntimeError("startup failed")),
        ),
        pytest.raises(RuntimeError, match="startup failed"),
    ):
        await compose_core_services()

    bubble_redis.aclose.assert_awaited_once()
    state_redis.aclose.assert_awaited_once()
    postgres.dispose.assert_awaited_once()
