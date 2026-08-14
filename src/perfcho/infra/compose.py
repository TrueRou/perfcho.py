"""Process-role composition roots for the perfcho application."""

import uuid
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import cast

from apscheduler import AsyncScheduler
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from perfcho.infra.cache import RedisCache
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
    SqlAlchemyAccountStatisticsRepository,
    SqlAlchemyAccountSubmissionValidator,
    SqlAlchemyBeatmapScoresRepository,
    SqlAlchemyMultiplayerSubmissionValidator,
    SqlAlchemyRankingRepository,
    SqlAlchemyReplayRepository,
    SqlAlchemyScoreQueryRepository,
    SqlAlchemyScoringAcceptanceRepository,
    SqlAlchemyScoringRepository,
)
from perfcho.infra.db.repositories.social import SqlAlchemySocialRepository
from perfcho.infra.db.uow import SqlAlchemyUnitOfWorkFactory
from perfcho.infra.redis import engine as infra_redis
from perfcho.infra.redis.bubbles import RedisRealtimeBubbleBus, RedisRealtimePollGate, RedisUserEventBus
from perfcho.infra.redis.identity import RedisOnlineCredentialVerificationCache
from perfcho.infra.redis.multiplayer import RedisMultiplayerStateRepository
from perfcho.infra.redis.realtime import RedisRealtimeStateRepository
from perfcho.infra.scheduler import start_scheduler, stop_scheduler
from perfcho.infra.security.password import Argon2Policy, PasswordPepper
from perfcho.infra.settings import Settings
from perfcho.infra.storage import S3ObjectStorage
from perfcho.infra.upstream.bancho import BanchoUpstreamContentSource
from perfcho.modules.account import AccountService
from perfcho.modules.authorization import AuthorizationQueryService
from perfcho.modules.authorization.management import AuthorizationManagementService
from perfcho.modules.bot import BotCommandService, register_core_commands
from perfcho.modules.common import Clock, IdGenerator, ObjectStorage
from perfcho.modules.community import CommunityQueryService, CommunityService
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
from perfcho.modules.realtime import RealtimeBubbleBus, RealtimePollGate, RealtimeStateRepository, UserEventBus
from perfcho.modules.scoring import (
    AccountStatisticsQueryService,
    BeatmapScoresQueryService,
    RankingQueryService,
    ReplayQueryService,
    ReplayService,
    ScoreQueryService,
    ScoringService,
)
from perfcho.modules.social import SocialQueryService, SocialService, TransactionAchievementAwarder, build_clan_commands
from perfcho.modules.social.achievements import default_achievement_evaluator_registry

core_services: CoreServices | None = None
stable_services: StableServices | None = None

_ACHIEVEMENT_EVALUATORS = default_achievement_evaluator_registry()

type AsyncCleanup = tuple[str, Callable[[], Awaitable[None]]]


async def _run_cleanups(cleanups: Sequence[AsyncCleanup], *, message: str) -> None:
    errors: list[BaseException] = []
    for name, cleanup in cleanups:
        try:
            await cleanup()
        except BaseException as error:
            error.add_note(f"while closing {name}")
            errors.append(error)
    if errors:
        raise BaseExceptionGroup(message, errors)


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


def _scoring_acceptance_repository(session: object) -> SqlAlchemyScoringAcceptanceRepository:
    return SqlAlchemyScoringAcceptanceRepository(cast(AsyncSession, session))


def _score_query_repository(session: object) -> SqlAlchemyScoreQueryRepository:
    return SqlAlchemyScoreQueryRepository(cast(AsyncSession, session))


def _replay_repository(session: object) -> SqlAlchemyReplayRepository:
    return SqlAlchemyReplayRepository(cast(AsyncSession, session))


def _ranking_repository(session: object) -> SqlAlchemyRankingRepository:
    return SqlAlchemyRankingRepository(cast(AsyncSession, session))


def _account_statistics_repository(session: object) -> SqlAlchemyAccountStatisticsRepository:
    return SqlAlchemyAccountStatisticsRepository(cast(AsyncSession, session))


def _beatmap_scores_repository(session: object) -> SqlAlchemyBeatmapScoresRepository:
    return SqlAlchemyBeatmapScoresRepository(cast(AsyncSession, session))


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
    """Collect process-owned services used by all request scopes.

    ``state_redis`` is the raw DB0 client for online state adapters.
    ``bubble_redis`` and ``cache_redis`` are separate DB0 clients owned by the process lifecycle.
    ``cache`` is the only cache API exposed to application services.
    ``scheduler`` owns APScheduler jobs for the API process lifecycle.
    """

    config: Settings
    clock: Clock
    id_generator: IdGenerator
    state_redis: Redis
    cache_redis: Redis
    cache: RedisCache
    postgres: AsyncEngine
    session_factory: DbSessionFactory
    scheduler: AsyncScheduler
    bubble_redis: Redis | None = None

    async def aclose(self) -> None:
        """Stop process-owned scheduling and close shared infrastructure resources."""
        cleanups: list[AsyncCleanup] = [
            ("scheduler", lambda: stop_scheduler(self.scheduler)),
            ("cache Redis", self.cache_redis.aclose),
            ("state Redis", self.state_redis.aclose),
        ]
        if self.bubble_redis is not None:
            cleanups.append(("bubble Redis", self.bubble_redis.aclose))
        cleanups.append(("PostgreSQL", self.postgres.dispose))
        await _run_cleanups(cleanups, message="core service shutdown failed")


async def compose_core_services() -> CoreServices:
    """Build process-owned services used by all request scopes."""
    config = Settings()
    cleanups: list[AsyncCleanup] = []
    try:
        postgres = await infra_db.create_engine()
        cleanups.append(("PostgreSQL", postgres.dispose))
        state_redis = await infra_redis.create_state_redis()
        cleanups.append(("state Redis", state_redis.aclose))
        bubble_redis = await infra_redis.create_bubble_redis()
        cleanups.append(("bubble Redis", bubble_redis.aclose))
        cache_redis = await infra_redis.create_cache_redis()
        cleanups.append(("cache Redis", cache_redis.aclose))
        cache = RedisCache(cache_redis, prefix=config.redis_cache_prefix)
        session_factory = infra_db.create_session_factory(postgres)
        scheduler = await start_scheduler(
            session_factory,
            user_ranking_snapshot_cron=config.user_ranking_snapshot_cron,
        )
    except BaseException as startup_error:
        try:
            await _run_cleanups(tuple(reversed(cleanups)), message="core service startup cleanup failed")
        except BaseException as cleanup_error:
            raise BaseExceptionGroup("core service startup failed", [startup_error, cleanup_error]) from None
        raise
    return CoreServices(
        config=config,
        clock=_SystemClock(),
        id_generator=_Uuid7Generator(),
        state_redis=state_redis,
        cache_redis=cache_redis,
        cache=cache,
        postgres=postgres,
        session_factory=session_factory,
        scheduler=scheduler,
        bubble_redis=bubble_redis,
    )


@dataclass(frozen=True, slots=True)
class StableServices:
    """Collect request-scoped services used by the Stable adapter."""

    identity: IdentityService
    authorization: AuthorizationQueryService
    realtime: RealtimeStateRepository
    clock: Clock
    id_generator: IdGenerator
    settings: Settings
    bubbles: RealtimeBubbleBus | None = None
    user_events: UserEventBus | None = None
    poll_gate: RealtimePollGate | None = None
    content_query: ContentQueryService | None = None
    content: ContentService | None = None
    content_sync: ContentSyncService | None = None
    social: SocialService | None = None
    social_query: SocialQueryService | None = None
    community: CommunityService | None = None
    community_query: CommunityQueryService | None = None
    object_storage: ObjectStorage | None = None
    scoring: ScoringService | None = None
    score_query: ScoreQueryService | None = None
    performance_query: PerformanceQueryService | None = None
    replay_query: ReplayQueryService | None = None
    replay: ReplayService | None = None
    ranking_query: RankingQueryService | None = None
    account_statistics: AccountStatisticsQueryService | None = None
    beatmap_scores: BeatmapScoresQueryService | None = None
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
        client_session_stale_grace=timedelta(seconds=core.config.client_session_stale_grace_seconds),
        client_session_touch_interval=timedelta(seconds=core.config.client_session_touch_interval_seconds),
        online_credential_verification_cache=RedisOnlineCredentialVerificationCache(
            core.state_redis,
            prefix=core.config.redis_state_prefix,
            ttl_seconds=core.config.online_credential_cache_ttl_seconds,
        ),
    )
    realtime = RedisRealtimeStateRepository(
        core.state_redis,
        prefix=core.config.redis_state_prefix,
        session_ttl=timedelta(seconds=core.config.redis_session_ttl_seconds),
        presence_ttl=timedelta(seconds=core.config.redis_presence_ttl_seconds),
        max_frame_count=core.config.redis_spectator_max_frames,
        max_frame_bytes=core.config.redis_spectator_max_bytes,
        max_channels_per_session=core.config.redis_realtime_max_channels_per_session,
        max_spectators_per_host=core.config.redis_spectator_max_viewers,
    )
    bubble_redis = core.bubble_redis or core.state_redis
    bubbles = RedisRealtimeBubbleBus(
        bubble_redis,
        prefix=core.config.redis_state_prefix,
        max_entries=core.config.redis_bubble_max_entries,
        ttl_seconds=core.config.redis_bubble_ttl_seconds,
    )
    user_events = RedisUserEventBus(
        bubble_redis,
        prefix=core.config.redis_state_prefix,
        max_entries=core.config.redis_bubble_max_entries,
        ttl_seconds=core.config.redis_bubble_ttl_seconds,
    )
    poll_gate = RedisRealtimePollGate(
        bubble_redis,
        prefix=core.config.redis_state_prefix,
        max_ttl_seconds=core.config.stable_poll_gate_seconds,
    )
    uow_factory = SqlAlchemyUnitOfWorkFactory(core.session_factory)
    authorization = AuthorizationQueryService(
        SqlAlchemyAuthorizationQueryRepository(core.session_factory),
        core.clock,
        core.cache,
    )
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
    content_query = ContentQueryService(uow_factory, _content_repository, core.cache)
    content = ContentService(uow_factory, _content_repository)
    social_query = SocialQueryService(uow_factory, _social_repository, core.cache)
    social = SocialService(uow_factory, _social_repository, _outbox_writer, core.clock, social_query)
    community = CommunityService(
        uow_factory,
        _community_repository,
        authorization,
        _silence_policy,
        _outbox_writer,
        core.clock,
        realtime,
    )
    scoring = ScoringService(
        uow_factory,
        _scoring_acceptance_repository,
        _outbox_writer,
        _account_submission_validator,
        _multiplayer_submission_validator,
        _achievement_awarder,
        core.clock,
        core.id_generator,
        solo_token_lifetime=timedelta(seconds=core.config.lazer_solo_score_token_lifetime_seconds),
    )
    multiplayer = MultiplayerService(
        uow_factory,
        _multiplayer_repository,
        _outbox_writer,
        RedisMultiplayerStateRepository(
            core.state_redis,
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
                resolve_account=social_query.resolve_account_by_name,
                resolve_beatmap=resolve_beatmap,
            )
        )
    )
    bot.register_group(build_pool_commands())
    bot.register_group(build_clan_commands())

    object_storage = S3ObjectStorage.from_settings(core.config)

    content_sync = ContentSyncService(
        uow_factory,
        _content_repository,
        _outbox_writer,
        BanchoUpstreamContentSource.from_settings(core.config),
        object_storage,
        core.clock,
        core.id_generator,
        core.cache,
        max_concurrency=core.config.content_sync_max_concurrency,
    )

    return StableServices(
        identity=identity,
        authorization=authorization,
        realtime=realtime,
        clock=core.clock,
        id_generator=core.id_generator,
        settings=core.config,
        bubbles=bubbles,
        user_events=user_events,
        poll_gate=poll_gate,
        content_query=content_query,
        content=content,
        content_sync=content_sync,
        social=social,
        social_query=social_query,
        community=community,
        object_storage=object_storage,
        scoring=scoring,
        score_query=ScoreQueryService(uow_factory, _score_query_repository),
        performance_query=PerformanceQueryService(uow_factory, _performance_query_repository),
        replay_query=ReplayQueryService(uow_factory, _replay_repository),
        replay=ReplayService(uow_factory, _replay_repository, _outbox_writer),
        ranking_query=RankingQueryService(uow_factory, _ranking_repository, core.cache),
        account_statistics=AccountStatisticsQueryService(uow_factory, _account_statistics_repository, core.cache),
        beatmap_scores=BeatmapScoresQueryService(uow_factory, _beatmap_scores_repository),
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
    cache: RedisCache,
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
            cache=cache,
            receipt_store_factory=_receipt_store,
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
