"""Compose protocol-independent management application services."""

from dataclasses import dataclass
from typing import cast

from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra.db.base import DbSessionFactory
from perfcho.infra.db.repositories.audit import SqlAlchemyAuditWriter
from perfcho.infra.db.repositories.command_receipt import SqlAlchemyCommandReceiptStore
from perfcho.infra.db.repositories.moderation import SqlAlchemyModerationRepository
from perfcho.infra.db.uow import SqlAlchemyUnitOfWorkFactory
from perfcho.infra.glue.common import SystemClock, authorization_repository, outbox_writer
from perfcho.modules.authorization import AuthorizationManagementService
from perfcho.modules.common import Clock
from perfcho.modules.moderation import ModerationService


def _moderation_repository(session: object) -> SqlAlchemyModerationRepository:
    return SqlAlchemyModerationRepository(cast(AsyncSession, session))


def _audit_writer(session: object) -> SqlAlchemyAuditWriter:
    return SqlAlchemyAuditWriter(cast(AsyncSession, session))


def _receipt_store(session: object) -> SqlAlchemyCommandReceiptStore:
    return SqlAlchemyCommandReceiptStore(cast(AsyncSession, session))


@dataclass(frozen=True, slots=True)
class ManagementServices:
    """Collect production-ready management application services."""

    authorization: AuthorizationManagementService
    moderation: ModerationService


def compose_management_services(
    session_factory: DbSessionFactory,
    *,
    clock: Clock | None = None,
) -> ManagementServices:
    """Compose management services without creating a protocol endpoint."""
    application_clock = clock or SystemClock()
    uow_factory = SqlAlchemyUnitOfWorkFactory(session_factory)
    return ManagementServices(
        authorization=AuthorizationManagementService(
            uow_factory,
            authorization_repository,
            _audit_writer,
            outbox_writer,
            application_clock,
            _receipt_store,
        ),
        moderation=ModerationService(
            uow_factory,
            _moderation_repository,
            authorization_repository,
            _audit_writer,
            outbox_writer,
            application_clock,
            _receipt_store,
        ),
    )
