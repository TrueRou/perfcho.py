"""Compose process-scoped content synchronization resources."""

from dataclasses import dataclass
from typing import cast

from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra.db.base import DbSessionFactory
from perfcho.infra.db.repositories.content import SqlAlchemyContentRepository
from perfcho.infra.db.uow import SqlAlchemyUnitOfWorkFactory
from perfcho.infra.glue.common import SystemClock, Uuid7Generator, outbox_writer
from perfcho.infra.settings import Settings, settings
from perfcho.infra.storage import S3ObjectStorage
from perfcho.infra.upstream.osu import OsuUpstreamContentSource
from perfcho.modules.common import Clock, IdGenerator
from perfcho.modules.content import ContentSyncService


def _content_repository(session: object) -> SqlAlchemyContentRepository:
    """Bind the content repository to a caller-owned session."""
    return SqlAlchemyContentRepository(cast(AsyncSession, session))


@dataclass(frozen=True, slots=True)
class ContentRuntime:
    """Own process-scoped upstream content resources used beyond one request dependency."""

    object_storage: S3ObjectStorage
    upstream: OsuUpstreamContentSource
    sync: ContentSyncService

    async def aclose(self) -> None:
        """Drain synchronization work before closing the upstream HTTP transport."""
        await self.sync.aclose()
        await self.upstream.aclose()


def create_content_runtime(
    session_factory: DbSessionFactory,
    *,
    config: Settings = settings,
    clock: Clock | None = None,
    id_generator: IdGenerator | None = None,
) -> ContentRuntime:
    """Create process-scoped content synchronization resources."""
    application_clock = clock or SystemClock()
    application_ids = id_generator or Uuid7Generator()
    uow_factory = SqlAlchemyUnitOfWorkFactory(session_factory)
    object_storage = S3ObjectStorage.from_settings(config)
    upstream = OsuUpstreamContentSource.from_settings(config)
    return ContentRuntime(
        object_storage=object_storage,
        upstream=upstream,
        sync=ContentSyncService(
            uow_factory,
            _content_repository,
            outbox_writer,
            upstream,
            object_storage,
            application_clock,
            application_ids,
            max_concurrency=config.content_sync_max_concurrency,
        ),
    )
