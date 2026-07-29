"""Compose application services from process-owned infrastructure."""

import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from redis.asyncio import Redis

from perfcho.infra.db.base import DbSessionFactory
from perfcho.infra.db.repositories.account import SqlAlchemyOutboxWriter
from perfcho.infra.db.repositories.authorization import SqlAlchemyAuthorizationRepository
from perfcho.infra.db.repositories.identity import SqlAlchemyIdentityRepository
from perfcho.infra.db.uow import SqlAlchemyUnitOfWorkFactory
from perfcho.infra.redis.realtime import RedisRealtimeRepository
from perfcho.infra.security.password import Argon2Policy, PasswordPepper
from perfcho.infra.settings import Settings, settings
from perfcho.modules.authorization import AuthorizationQueryService
from perfcho.modules.common import Clock, IdGenerator
from perfcho.modules.identity import IdentityService
from perfcho.modules.realtime import RealtimeRepository


class SystemClock:
    """Return the current UTC instant."""

    def now(self) -> datetime:
        """Return one timezone-aware wall-clock instant."""
        return datetime.now(UTC)


class Uuid7Generator:
    """Generate time-ordered application UUIDs."""

    def new(self) -> uuid.UUID:
        """Return a new UUIDv7 value."""
        return uuid.uuid7()


@dataclass(frozen=True, slots=True)
class StableServices:
    """Collect request-scoped services used by the Stable adapter."""

    identity: IdentityService
    authorization: AuthorizationQueryService
    realtime: RealtimeRepository
    clock: Clock
    id_generator: IdGenerator
    settings: Settings


@asynccontextmanager
async def compose_stable_services(
    session_factory: DbSessionFactory,
    redis: Redis,
    *,
    config: Settings = settings,
    clock: Clock | None = None,
    id_generator: IdGenerator | None = None,
) -> AsyncIterator[StableServices]:
    """Build Stable-facing services while owning one authorization read session."""
    application_clock = clock or SystemClock()
    application_ids = id_generator or Uuid7Generator()
    identity = IdentityService(
        SqlAlchemyUnitOfWorkFactory(session_factory),
        SqlAlchemyIdentityRepository,
        SqlAlchemyOutboxWriter,
        PasswordPepper(
            version=config.password_pepper_version,
            secret=config.password_pepper.get_secret_value().encode(),
        ),
        Argon2Policy(
            time_cost=config.argon2_time_cost,
            memory_cost_kib=config.argon2_memory_cost_kib,
            parallelism=config.argon2_parallelism,
        ),
        config.token_hmac_key.get_secret_value().encode(),
        config.device_hmac_key.get_secret_value().encode(),
        application_clock,
        application_ids,
    )
    realtime = RedisRealtimeRepository(
        redis,
        prefix=config.redis_state_prefix,
        session_ttl=timedelta(seconds=config.redis_session_ttl_seconds),
        presence_ttl=timedelta(seconds=config.redis_presence_ttl_seconds),
        mailbox_ttl=timedelta(seconds=config.redis_mailbox_ttl_seconds),
        max_packet_count=config.redis_mailbox_max_packets,
        max_packet_bytes=config.redis_mailbox_max_bytes,
        max_frame_count=config.redis_spectator_max_frames,
        max_frame_bytes=config.redis_spectator_max_bytes,
    )

    async with session_factory() as session:
        yield StableServices(
            identity=identity,
            authorization=AuthorizationQueryService(
                SqlAlchemyAuthorizationRepository(session),
                application_clock,
            ),
            realtime=realtime,
            clock=application_clock,
            id_generator=application_ids,
            settings=config,
        )
