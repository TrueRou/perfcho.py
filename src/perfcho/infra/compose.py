"""Process-role composition roots for the perfcho application."""

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import cast

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from perfcho.infra.db import engine as infra_db
from perfcho.infra.db.base import DbSessionFactory
from perfcho.infra.db.repositories.account import SqlAlchemyAccountRepository
from perfcho.infra.db.repositories.audit import SqlAlchemyAuditWriter
from perfcho.infra.db.repositories.authorization import (
    SqlAlchemyAuthorizationQueryRepository,
    SqlAlchemyAuthorizationRepository,
)
from perfcho.infra.db.repositories.command_receipt import SqlAlchemyCommandReceiptStore
from perfcho.infra.db.repositories.community import (
    SqlAlchemyActiveSilencePolicy,
    SqlAlchemyCommunityRepository,
)
from perfcho.infra.db.repositories.content import SqlAlchemyContentRepository
from perfcho.infra.db.repositories.identity import SqlAlchemyIdentityRepository
from perfcho.infra.db.repositories.moderation import SqlAlchemyModerationRepository
from perfcho.infra.db.repositories.multiplayer import (
    SqlAlchemyMultiplayerAccessPolicy,
    SqlAlchemyMultiplayerRepository,
)
from perfcho.infra.db.repositories.outbox import SqlAlchemyOutboxWriter
from perfcho.infra.db.repositories.performance.query import SqlAlchemyPerformanceQueryRepository
from perfcho.infra.db.repositories.scoring import (
    SqlAlchemyAccountSubmissionValidator,
    SqlAlchemyMultiplayerSubmissionValidator,
    SqlAlchemyScoringRepository,
)
from perfcho.infra.db.repositories.social import SqlAlchemySocialRepository
from perfcho.infra.db.uow import SqlAlchemyUnitOfWorkFactory
from perfcho.infra.redis import engine as infra_redis
from perfcho.infra.redis.identity import RedisStableWebVerificationCache
from perfcho.infra.redis.multiplayer import RedisMultiplayerStateRepository
from perfcho.infra.redis.realtime import RedisRealtimeRepository
from perfcho.infra.security.password import Argon2Policy, PasswordPepper
from perfcho.infra.settings import Settings
from perfcho.infra.storage import S3ObjectStorage
from perfcho.infra.upstream.bancho import BanchoUpstreamContentSource
from perfcho.modules.account import AccountService
from perfcho.modules.authorization import AuthorizationQueryService
from perfcho.modules.authorization.management import AuthorizationManagementService
from perfcho.modules.bot import BotCommandService, register_core_commands
from perfcho.modules.common import Clock, IdGenerator, ObjectStorage
from perfcho.modules.community import CommunityService
from perfcho.modules.content import BeatmapRevisionView, ContentQueryService, ContentService, ContentSyncService
from perfcho.modules.identity import IdentityService
from perfcho.modules.moderation.services import ModerationService
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

core_services: CoreServices | None = None
stable_services: StableServices | None = None

_ACHIEVEMENT_EVALUATORS = default_achievement_evaluator_registry()


def _authorization_repository(session: object) -> SqlAlchemyAuthorizationRepository:
    return SqlAlchemyAuthorizationRepository(cast(AsyncSession, session))


def _outbox_writer(session: object) -> SqlAlchemyOutboxWriter:
    return SqlAlchemyOutboxWriter(cast(AsyncSession, session))


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
        _outbox_writer(session),
        _ACHIEVEMENT_EVALUATORS,
    )


def _community_repository(session: object) -> SqlAlchemyCommunityRepository:
    return SqlAlchemyCommunityRepository(cast(AsyncSession, session))


def _silence_policy(session: object) -> SqlAlchemyActiveSilencePolicy:
    return SqlAlchemyActiveSilencePolicy(cast(AsyncSession, session))


def _scoring_repository(session: object) -> SqlAlchemyScoringRepository:
    return SqlAlchemyScoringRepository(cast(AsyncSession, session))


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


def _moderation_repository(session: object) -> SqlAlchemyModerationRepository:
    return SqlAlchemyModerationRepository(cast(AsyncSession, session))


def _audit_writer(session: object) -> SqlAlchemyAuditWriter:
    return SqlAlchemyAuditWriter(cast(AsyncSession, session))


def _receipt_store(session: object) -> SqlAlchemyCommandReceiptStore:
    return SqlAlchemyCommandReceiptStore(cast(AsyncSession, session))


class _SystemClock:
    def now(self) -> datetime:
        return datetime.now(UTC)


class _Uuid7Generator:
    def new(self) -> uuid.UUID:
        return uuid.uuid7()


@dataclass(frozen=True, slots=True)
class CoreServices:
    """Collect process-owned services used by all request scopes."""

    config: Settings
    clock: Clock
    id_generator: IdGenerator
    redis: Redis
    postgres: AsyncEngine
    session_factory: DbSessionFactory


async def compose_core_services() -> CoreServices:
    """Build process-owned services used by all request scopes."""
    postgres = await infra_db.create_engine()

    return CoreServices(
        config=Settings(),
        clock=_SystemClock(),
        id_generator=_Uuid7Generator(),
        redis=await infra_redis.create_redis(),
        postgres=await infra_db.create_engine(),
        session_factory=infra_db.create_session_factory(postgres),
    )


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


async def compose_stable_services(
    core: CoreServices,
) -> StableServices:
    """Build process-owned Stable-facing services without sharing transaction resources."""
    identity = IdentityService(
        SqlAlchemyUnitOfWorkFactory(core.session_factory),
        _identity_repository,
        _outbox_writer,
        PasswordPepper(
            version=core.config.password_pepper_version,
            secret=core.config.password_pepper.get_secret_value().encode(),
        ),
        Argon2Policy(
            time_cost=core.config.argon2_time_cost,
            memory_cost_kib=core.config.argon2_memory_cost_kib,
            parallelism=core.config.argon2_parallelism,
        ),
        core.config.token_hmac_key.get_secret_value().encode(),
        core.config.device_hmac_key.get_secret_value().encode(),
        core.clock,
        core.id_generator,
        stable_session_stale_grace=timedelta(seconds=core.config.stable_session_stale_grace_seconds),
        stable_session_touch_interval=timedelta(seconds=core.config.stable_session_touch_interval_seconds),
        stable_web_verification_cache=RedisStableWebVerificationCache(
            core.redis,
            prefix=core.config.redis_state_prefix,
            ttl_seconds=core.config.stable_web_auth_cache_ttl_seconds,
        ),
    )
    realtime = RedisRealtimeRepository(
        core.redis,
        prefix=core.config.redis_state_prefix,
        session_ttl=timedelta(seconds=core.config.redis_session_ttl_seconds),
        presence_ttl=timedelta(seconds=core.config.redis_presence_ttl_seconds),
        mailbox_ttl=timedelta(seconds=core.config.redis_mailbox_ttl_seconds),
        max_packet_count=core.config.redis_mailbox_max_packets,
        max_packet_bytes=core.config.redis_mailbox_max_bytes,
        max_frame_count=core.config.redis_spectator_max_frames,
        max_frame_bytes=core.config.redis_spectator_max_bytes,
        max_channels_per_session=core.config.redis_realtime_max_channels_per_session,
        max_spectators_per_host=core.config.redis_spectator_max_viewers,
    )
    uow_factory = SqlAlchemyUnitOfWorkFactory(core.session_factory)
    account = AccountService(
        uow_factory,
        _account_repository,
        _outbox_writer,
        PasswordPepper(
            version=core.config.password_pepper_version,
            secret=core.config.password_pepper.get_secret_value().encode(),
        ),
        Argon2Policy(
            time_cost=core.config.argon2_time_cost,
            memory_cost_kib=core.config.argon2_memory_cost_kib,
            parallelism=core.config.argon2_parallelism,
        ),
        core.clock,
    )
    content_query = ContentQueryService(uow_factory, _content_repository)
    content = ContentService(uow_factory, _content_repository)
    social = SocialService(uow_factory, _social_repository, _outbox_writer, core.clock)
    community = CommunityService(
        uow_factory,
        _community_repository,
        _authorization_repository,
        _silence_policy,
        _outbox_writer,
        core.clock,
        realtime,
    )
    scoring = ScoringService(
        uow_factory,
        _scoring_repository,
        _outbox_writer,
        _account_submission_validator,
        _multiplayer_submission_validator,
        _achievement_awarder,
        core.clock,
        core.id_generator,
    )
    multiplayer = MultiplayerService(
        uow_factory,
        _multiplayer_repository,
        _outbox_writer,
        RedisMultiplayerStateRepository(
            core.redis,
            prefix=core.config.redis_state_prefix,
            state_ttl=timedelta(seconds=core.config.redis_multiplayer_ttl_seconds),
            max_rooms=core.config.redis_multiplayer_max_rooms,
        ),
        core.clock,
        core.config.match_password_hmac_key.get_secret_value().encode(),
        access_policy_factory=_multiplayer_access_policy,
        admission_key=core.config.admission_hmac_key.get_secret_value().encode(),
        state_lifetime=timedelta(seconds=core.config.redis_multiplayer_ttl_seconds),
    )

    async def resolve_beatmap(selector: str) -> BeatmapRevisionView:
        if selector.isdigit():
            return await content_query.lookup_beatmap(int(selector), external=True)
        return await content_query.lookup_md5(selector)

    bot = BotCommandService(
        prefix=core.config.bot_command_prefix,
        bot_account_id=core.config.bot_account_id,
        bot_name=core.config.bot_name,
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

    authorization = AuthorizationQueryService(
        SqlAlchemyAuthorizationQueryRepository(core.session_factory),
        core.clock,
    )

    object_storage = S3ObjectStorage.from_settings(core.config)

    content_sync = ContentSyncService(
        uow_factory,
        _content_repository,
        _outbox_writer,
        BanchoUpstreamContentSource.from_settings(core.config),
        object_storage,
        core.clock,
        core.id_generator,
        max_concurrency=core.config.content_sync_max_concurrency,
    )

    return StableServices(
        identity=identity,
        authorization=authorization,
        realtime=realtime,
        clock=core.clock,
        id_generator=core.id_generator,
        settings=core.config,
        content_query=content_query,
        content=content,
        content_sync=content_sync,
        social=social,
        community=community,
        object_storage=object_storage,
        scoring=scoring,
        performance_query=PerformanceQueryService(uow_factory, _performance_query_repository),
        replay_query=ReplayQueryService(uow_factory, _scoring_repository),
        replay=ReplayService(uow_factory, _scoring_repository, _outbox_writer),
        ranking_query=RankingQueryService(uow_factory, _scoring_repository),
        multiplayer=multiplayer,
        account=account,
        bot=bot,
    )


@dataclass(frozen=True, slots=True)
class AdminServices:
    """Collect production-ready management application services."""

    authorization: AuthorizationManagementService
    moderation: ModerationService


def compose_admin_services(
    session_factory: DbSessionFactory,
    *,
    clock: Clock | None = None,
) -> AdminServices:
    """Compose management services without creating a protocol endpoint."""
    application_clock = clock or _SystemClock()
    uow_factory = SqlAlchemyUnitOfWorkFactory(session_factory)
    return AdminServices(
        authorization=AuthorizationManagementService(
            uow_factory,
            _authorization_repository,
            _audit_writer,
            _outbox_writer,
            application_clock,
            _receipt_store,
        ),
        moderation=ModerationService(
            uow_factory,
            _moderation_repository,
            _authorization_repository,
            _audit_writer,
            _outbox_writer,
            application_clock,
            _receipt_store,
        ),
    )
