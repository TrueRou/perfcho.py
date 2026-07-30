"""Compose application services from process-owned infrastructure."""

import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import cast

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra.db.base import DbSessionFactory
from perfcho.infra.db.repositories.authorization import SqlAlchemyAuthorizationRepository
from perfcho.infra.db.repositories.community import (
    SqlAlchemyActiveSilencePolicy,
    SqlAlchemyCommunityRepository,
)
from perfcho.infra.db.repositories.content import SqlAlchemyContentRepository
from perfcho.infra.db.repositories.identity import SqlAlchemyIdentityRepository
from perfcho.infra.db.repositories.multiplayer import (
    SqlAlchemyMultiplayerAccessPolicy,
    SqlAlchemyMultiplayerRepository,
)
from perfcho.infra.db.repositories.outbox import SqlAlchemyOutboxWriter
from perfcho.infra.db.repositories.performance.query import SqlAlchemyPerformanceQueryRepository
from perfcho.infra.db.repositories.performance.scheduling import SqlAlchemyPerformanceJobScheduler
from perfcho.infra.db.repositories.scoring import (
    SqlAlchemyAccountSubmissionValidator,
    SqlAlchemyMultiplayerSubmissionValidator,
    SqlAlchemyScoringRepository,
)
from perfcho.infra.db.repositories.social import SqlAlchemySocialRepository
from perfcho.infra.db.uow import SqlAlchemyUnitOfWorkFactory
from perfcho.infra.redis.multiplayer import RedisMultiplayerStateRepository
from perfcho.infra.redis.realtime import RedisRealtimeRepository
from perfcho.infra.security.password import Argon2Policy, PasswordPepper
from perfcho.infra.settings import Settings, settings
from perfcho.infra.storage import S3ObjectStorage
from perfcho.modules.authorization import AuthorizationQueryService
from perfcho.modules.common import Clock, IdGenerator, ObjectStorage
from perfcho.modules.community import CommunityService
from perfcho.modules.content import ContentQueryService, ContentService
from perfcho.modules.identity import IdentityService
from perfcho.modules.multiplayer import MultiplayerService
from perfcho.modules.performance.services import PerformanceQueryService
from perfcho.modules.realtime import RealtimeRepository
from perfcho.modules.scoring import (
    RankingQueryService,
    ReplayQueryService,
    ReplayService,
    ScoringService,
)
from perfcho.modules.social import SocialService


def _identity_repository(session: object) -> SqlAlchemyIdentityRepository:
    return SqlAlchemyIdentityRepository(cast(AsyncSession, session))


def _authorization_repository(session: object) -> SqlAlchemyAuthorizationRepository:
    return SqlAlchemyAuthorizationRepository(cast(AsyncSession, session))


def _content_repository(session: object) -> SqlAlchemyContentRepository:
    return SqlAlchemyContentRepository(cast(AsyncSession, session))


def _social_repository(session: object) -> SqlAlchemySocialRepository:
    return SqlAlchemySocialRepository(cast(AsyncSession, session))


def _community_repository(session: object) -> SqlAlchemyCommunityRepository:
    return SqlAlchemyCommunityRepository(cast(AsyncSession, session))


def _silence_policy(session: object) -> SqlAlchemyActiveSilencePolicy:
    return SqlAlchemyActiveSilencePolicy(cast(AsyncSession, session))


def _outbox_writer(session: object) -> SqlAlchemyOutboxWriter:
    return SqlAlchemyOutboxWriter(cast(AsyncSession, session))


def _scoring_repository(session: object) -> SqlAlchemyScoringRepository:
    return SqlAlchemyScoringRepository(cast(AsyncSession, session))


def _performance_job_scheduler(session: object) -> SqlAlchemyPerformanceJobScheduler:
    return SqlAlchemyPerformanceJobScheduler(cast(AsyncSession, session))


def _performance_query_repository(session: object) -> SqlAlchemyPerformanceQueryRepository:
    return SqlAlchemyPerformanceQueryRepository(cast(AsyncSession, session))


def _account_submission_validator(session: object) -> SqlAlchemyAccountSubmissionValidator:
    return SqlAlchemyAccountSubmissionValidator(cast(AsyncSession, session))


def _multiplayer_submission_validator(session: object) -> SqlAlchemyMultiplayerSubmissionValidator:
    return SqlAlchemyMultiplayerSubmissionValidator(cast(AsyncSession, session))


def _multiplayer_repository(session: object) -> SqlAlchemyMultiplayerRepository:
    return SqlAlchemyMultiplayerRepository(cast(AsyncSession, session))


def _multiplayer_access_policy(session: object) -> SqlAlchemyMultiplayerAccessPolicy:
    return SqlAlchemyMultiplayerAccessPolicy(cast(AsyncSession, session))


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
    content_query: ContentQueryService | None = None
    content: ContentService | None = None
    social: SocialService | None = None
    community: CommunityService | None = None
    object_storage: ObjectStorage | None = None
    scoring: ScoringService | None = None
    performance_query: PerformanceQueryService | None = None
    replay_query: ReplayQueryService | None = None
    replay: ReplayService | None = None
    ranking_query: RankingQueryService | None = None
    multiplayer: MultiplayerService | None = None


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
        _identity_repository,
        _outbox_writer,
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
        stable_session_stale_grace=timedelta(seconds=config.stable_session_stale_grace_seconds),
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
        max_channels_per_session=config.redis_realtime_max_channels_per_session,
        max_spectators_per_host=config.redis_spectator_max_viewers,
    )
    uow_factory = SqlAlchemyUnitOfWorkFactory(session_factory)
    content_query = ContentQueryService(uow_factory, _content_repository)
    content = ContentService(uow_factory, _content_repository)
    social = SocialService(uow_factory, _social_repository, _outbox_writer, application_clock)
    community = CommunityService(
        uow_factory,
        _community_repository,
        _authorization_repository,
        _silence_policy,
        _outbox_writer,
        application_clock,
        realtime,
    )
    scoring = ScoringService(
        uow_factory,
        _scoring_repository,
        _outbox_writer,
        _account_submission_validator,
        _multiplayer_submission_validator,
        _performance_job_scheduler,
        application_clock,
        application_ids,
    )
    multiplayer = MultiplayerService(
        uow_factory,
        _multiplayer_repository,
        RedisMultiplayerStateRepository(
            redis,
            prefix=config.redis_state_prefix,
            state_ttl=timedelta(seconds=config.redis_multiplayer_ttl_seconds),
            max_rooms=config.redis_multiplayer_max_rooms,
        ),
        application_clock,
        config.match_password_hmac_key.get_secret_value().encode(),
        access_policy_factory=_multiplayer_access_policy,
        admission_key=config.token_hmac_key.get_secret_value().encode(),
        state_lifetime=timedelta(seconds=config.redis_multiplayer_ttl_seconds),
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
            content_query=content_query,
            content=content,
            social=social,
            community=community,
            object_storage=S3ObjectStorage.from_settings(config),
            scoring=scoring,
            performance_query=PerformanceQueryService(uow_factory, _performance_query_repository),
            replay_query=ReplayQueryService(uow_factory, _scoring_repository),
            replay=ReplayService(uow_factory, _scoring_repository),
            ranking_query=RankingQueryService(uow_factory, _scoring_repository),
            multiplayer=multiplayer,
        )
