"""Compose application services from process-owned infrastructure."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta
from typing import cast

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra.db.base import DbSessionFactory
from perfcho.infra.db.repositories.account import SqlAlchemyAccountRepository
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
from perfcho.infra.db.repositories.performance.query import SqlAlchemyPerformanceQueryRepository
from perfcho.infra.db.repositories.performance.scheduling import SqlAlchemyPerformanceJobScheduler
from perfcho.infra.db.repositories.scoring import (
    SqlAlchemyAccountSubmissionValidator,
    SqlAlchemyMultiplayerSubmissionValidator,
    SqlAlchemyScoringRepository,
)
from perfcho.infra.db.repositories.social import SqlAlchemySocialRepository
from perfcho.infra.db.uow import SqlAlchemyUnitOfWorkFactory
from perfcho.infra.glue.common import (
    SystemClock,
    Uuid7Generator,
    authorization_repository,
    outbox_writer,
)
from perfcho.infra.glue.content import ContentRuntime
from perfcho.infra.redis.multiplayer import RedisMultiplayerStateRepository
from perfcho.infra.redis.realtime import RedisRealtimeRepository
from perfcho.infra.security.password import Argon2Policy, PasswordPepper
from perfcho.infra.settings import Settings, settings
from perfcho.infra.storage import S3ObjectStorage
from perfcho.modules.account import AccountService
from perfcho.modules.authorization import AuthorizationQueryService
from perfcho.modules.bot import BotCommandService, register_core_commands
from perfcho.modules.common import Clock, IdGenerator, ObjectStorage
from perfcho.modules.community import CommunityService
from perfcho.modules.content import BeatmapRevisionView, ContentQueryService, ContentService, ContentSyncService
from perfcho.modules.identity import IdentityService
from perfcho.modules.multiplayer import (
    MultiplayerCommandDependencies,
    MultiplayerService,
    build_multiplayer_commands,
    build_pool_commands,
)
from perfcho.modules.performance.services import PerformanceQueryService
from perfcho.modules.realtime import RealtimeRepository
from perfcho.modules.scoring import (
    RankingQueryService,
    ReplayQueryService,
    ReplayService,
    ScoringService,
)
from perfcho.modules.social import SocialService, TransactionAchievementAwarder, build_clan_commands
from perfcho.modules.social.achievements import default_achievement_evaluator_registry

_ACHIEVEMENT_EVALUATORS = default_achievement_evaluator_registry()


def _identity_repository(session: object) -> SqlAlchemyIdentityRepository:
    return SqlAlchemyIdentityRepository(cast(AsyncSession, session))


def _account_repository(session: object) -> SqlAlchemyAccountRepository:
    return SqlAlchemyAccountRepository(cast(AsyncSession, session))


def _content_repository(session: object) -> SqlAlchemyContentRepository:
    return SqlAlchemyContentRepository(cast(AsyncSession, session))


def _social_repository(session: object) -> SqlAlchemySocialRepository:
    return SqlAlchemySocialRepository(cast(AsyncSession, session))


def _achievement_awarder(session: object) -> TransactionAchievementAwarder:
    return TransactionAchievementAwarder(
        _social_repository(session),
        outbox_writer(session),
        _ACHIEVEMENT_EVALUATORS,
    )


def _community_repository(session: object) -> SqlAlchemyCommunityRepository:
    return SqlAlchemyCommunityRepository(cast(AsyncSession, session))


def _silence_policy(session: object) -> SqlAlchemyActiveSilencePolicy:
    return SqlAlchemyActiveSilencePolicy(cast(AsyncSession, session))


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


def _bot_service(
    config: Settings,
    multiplayer: MultiplayerService,
    social: SocialService,
    content_query: ContentQueryService,
) -> BotCommandService:
    async def resolve_beatmap(selector: str) -> BeatmapRevisionView:
        if selector.isdigit():
            return await content_query.lookup_beatmap(int(selector), external=True)
        return await content_query.lookup_md5(selector)

    bot = BotCommandService(
        prefix=config.bot_command_prefix,
        bot_account_id=config.bot_account_id,
        bot_name=config.bot_name,
    )

    register_core_commands(bot)
    bot.register_group(
        build_multiplayer_commands(
            MultiplayerCommandDependencies(
                service=multiplayer,
                resolve_account=social.resolve_account_by_name,
                resolve_beatmap=resolve_beatmap,
            )
        )
    )
    bot.register_group(build_pool_commands())
    bot.register_group(build_clan_commands())

    return bot


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
    content_sync: ContentSyncService | None = None
    social: SocialService | None = None
    community: CommunityService | None = None
    object_storage: ObjectStorage | None = None
    scoring: ScoringService | None = None
    performance_query: PerformanceQueryService | None = None
    replay_query: ReplayQueryService | None = None
    replay: ReplayService | None = None
    ranking_query: RankingQueryService | None = None
    multiplayer: MultiplayerService | None = None
    account: AccountService | None = None
    bot: BotCommandService | None = None


@asynccontextmanager
async def compose_stable_services(
    session_factory: DbSessionFactory,
    redis: Redis,
    *,
    config: Settings = settings,
    clock: Clock | None = None,
    id_generator: IdGenerator | None = None,
    content_runtime: ContentRuntime | None = None,
) -> AsyncIterator[StableServices]:
    """Build Stable-facing services while owning one authorization read session."""
    application_clock = clock or SystemClock()
    application_ids = id_generator or Uuid7Generator()
    identity = IdentityService(
        SqlAlchemyUnitOfWorkFactory(session_factory),
        _identity_repository,
        outbox_writer,
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
    account = AccountService(
        uow_factory,
        _account_repository,
        outbox_writer,
        PasswordPepper(
            version=config.password_pepper_version,
            secret=config.password_pepper.get_secret_value().encode(),
        ),
        Argon2Policy(
            time_cost=config.argon2_time_cost,
            memory_cost_kib=config.argon2_memory_cost_kib,
            parallelism=config.argon2_parallelism,
        ),
        application_clock,
    )
    content_query = ContentQueryService(uow_factory, _content_repository)
    content = ContentService(uow_factory, _content_repository)
    social = SocialService(uow_factory, _social_repository, outbox_writer, application_clock)
    community = CommunityService(
        uow_factory,
        _community_repository,
        authorization_repository,
        _silence_policy,
        outbox_writer,
        application_clock,
        realtime,
    )
    scoring = ScoringService(
        uow_factory,
        _scoring_repository,
        outbox_writer,
        _account_submission_validator,
        _multiplayer_submission_validator,
        _performance_job_scheduler,
        _achievement_awarder,
        application_clock,
        application_ids,
    )
    multiplayer = MultiplayerService(
        uow_factory,
        _multiplayer_repository,
        outbox_writer,
        RedisMultiplayerStateRepository(
            redis,
            prefix=config.redis_state_prefix,
            state_ttl=timedelta(seconds=config.redis_multiplayer_ttl_seconds),
            max_rooms=config.redis_multiplayer_max_rooms,
        ),
        application_clock,
        config.match_password_hmac_key.get_secret_value().encode(),
        access_policy_factory=_multiplayer_access_policy,
        admission_key=config.admission_hmac_key.get_secret_value().encode(),
        state_lifetime=timedelta(seconds=config.redis_multiplayer_ttl_seconds),
    )

    bot = _bot_service(config, multiplayer=multiplayer, social=social, content_query=content_query)

    async with session_factory() as session:
        authorization = AuthorizationQueryService(
            authorization_repository(session),
            application_clock,
        )
        yield StableServices(
            identity=identity,
            authorization=authorization,
            realtime=realtime,
            clock=application_clock,
            id_generator=application_ids,
            settings=config,
            content_query=content_query,
            content=content,
            content_sync=content_runtime.sync if content_runtime is not None else None,
            social=social,
            community=community,
            object_storage=(
                content_runtime.object_storage if content_runtime is not None else S3ObjectStorage.from_settings(config)
            ),
            scoring=scoring,
            performance_query=PerformanceQueryService(uow_factory, _performance_query_repository),
            replay_query=ReplayQueryService(uow_factory, _scoring_repository),
            replay=ReplayService(uow_factory, _scoring_repository, outbox_writer),
            ranking_query=RankingQueryService(uow_factory, _scoring_repository),
            multiplayer=multiplayer,
            account=account,
            bot=bot,
        )
