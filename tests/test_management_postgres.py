"""Verify management write atomicity against PostgreSQL."""

import uuid
from datetime import UTC, datetime, timedelta
from typing import cast

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.schema import CreateSchema

import perfcho.infra.db.models  # noqa: F401
from perfcho.infra.compose import compose_admin_services as compose_management_services
from perfcho.infra.db.base import MODEL_SCHEMAS, DbBase
from perfcho.infra.db.bootstrap import bootstrap_database
from perfcho.infra.db.engine import create_session_factory
from perfcho.infra.db.enums import AccountStatus, AccountType
from perfcho.infra.db.models.audit import AuditEvent
from perfcho.infra.db.models.authz import AccountRoleGrant
from perfcho.infra.db.models.core import Account
from perfcho.infra.db.models.events import ActivityEvent, OutboxDelivery, OutboxEvent, ProjectionCheckpoint
from perfcho.infra.db.models.moderation import CaseEntry, ModerationCase, Sanction, SanctionEvent
from perfcho.infra.db.projectors.management import project_authorization_event, project_moderation_event
from perfcho.infra.db.repositories.audit import SqlAlchemyAuditWriter
from perfcho.infra.db.repositories.authorization import SqlAlchemyAuthorizationRepository
from perfcho.infra.db.repositories.moderation import SqlAlchemyModerationRepository
from perfcho.infra.db.uow import SqlAlchemyUnitOfWorkFactory
from perfcho.modules.authorization import GrantRole
from perfcho.modules.authorization.ports import AuthorizationRepository
from perfcho.modules.common import Actor, ClientContext, CommandMeta
from perfcho.modules.common.errors import ResourceConflict
from perfcho.modules.moderation import AddCaseEntry, ImposeSanction, ModerationService, OpenCase, RevokeSanction
from perfcho.modules.moderation.ports import ModerationRepository
from tests.cache_support import RedisCacheFake

NOW = datetime(2026, 8, 3, 12, tzinfo=UTC)


class FixedClock:
    def now(self) -> datetime:
        return NOW


class FailingOutbox:
    async def append(self, event: object) -> uuid.UUID:
        del event
        raise RuntimeError("outbox unavailable")


def meta(label: str) -> CommandMeta:
    return CommandMeta(
        uuid.uuid7(),
        f"management:{label}",
        label.encode().ljust(32, b"0")[:32],
        Actor(1, uuid.uuid7()),
        ClientContext("api", "test", None, "127.0.0.1"),
        NOW,
    )


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_management_writes_are_atomic_audited_and_revoke_once(postgres_database_url: str) -> None:
    engine = create_async_engine(postgres_database_url)
    try:
        async with engine.begin() as connection:
            for schema in MODEL_SCHEMAS:
                await connection.execute(CreateSchema(schema, if_not_exists=True))
            await connection.run_sync(DbBase.metadata.create_all)
        session_factory = create_session_factory(engine)
        await bootstrap_database(session_factory)
        async with session_factory.begin() as session:
            session.add(
                Account(
                    id=2,
                    type=AccountType.USER,
                    status=AccountStatus.ACTIVE,
                    country_code="US",
                    registered_at=NOW,
                    activated_at=NOW,
                )
            )
            session.add(
                AccountRoleGrant(
                    account_id=1,
                    role_id=4,
                    starts_at=NOW - timedelta(days=1),
                    reason="integration-test administrator",
                )
            )

        services = compose_management_services(session_factory, RedisCacheFake(), clock=FixedClock())
        grant_command = GrantRole(meta("grant"), 2, "moderator", NOW, NOW + timedelta(days=30), "staff assignment")
        grant = await services.authorization.grant_role(grant_command)
        assert await services.authorization.grant_role(grant_command) == grant
        case_command = OpenCase(meta("case"), 2, "investigation", 25)
        case = await services.moderation.open_case(case_command)
        assert await services.moderation.open_case(case_command) == case
        await services.moderation.add_case_entry(AddCaseEntry(meta("entry"), case.case_id, "note", "reviewed evidence"))
        sanction = await services.moderation.impose_sanction(
            ImposeSanction(
                meta("sanction"),
                case.case_id,
                2,
                "silence",
                NOW,
                NOW + timedelta(hours=1),
                "abusive messages",
            )
        )
        revoked = await services.moderation.revoke_sanction(
            RevokeSanction(meta("revoke"), sanction.sanction_id, "appeal accepted")
        )

        assert revoked.revoked_at == NOW
        with pytest.raises(ResourceConflict):
            await services.authorization.grant_role(
                GrantRole(meta("duplicate"), 2, "moderator", NOW, NOW + timedelta(days=1), "duplicate")
            )
        with pytest.raises(ResourceConflict):
            await services.moderation.revoke_sanction(
                RevokeSanction(meta("duplicate-revoke"), sanction.sanction_id, "again")
            )

        async with session_factory() as session:
            assert (
                await session.scalar(
                    select(func.count()).select_from(AccountRoleGrant).where(AccountRoleGrant.id == grant.grant_id)
                )
                == 1
            )
            assert await session.scalar(select(func.count()).select_from(ModerationCase)) == 1
            assert await session.scalar(select(func.count()).select_from(CaseEntry)) == 1
            assert await session.scalar(select(func.count()).select_from(Sanction)) == 1
            assert tuple(await session.scalars(select(SanctionEvent.action).order_by(SanctionEvent.id))) == (
                "imposed",
                "revoked",
            )
            assert await session.scalar(select(func.count()).select_from(AuditEvent)) == 5
            assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 5
            assert await session.scalar(select(func.count()).select_from(OutboxDelivery)) == 5

        async with session_factory.begin() as session:
            events = tuple(await session.scalars(select(OutboxEvent).order_by(OutboxEvent.position)))
            for event in events:
                if event.event_type.startswith("authorization."):
                    await project_authorization_event(session, event, "account:2")
                else:
                    await project_moderation_event(
                        session,
                        event,
                        f"{event.aggregate_type}:{event.aggregate_id}",
                    )
        async with session_factory() as session:
            assert await session.scalar(select(func.count()).select_from(ActivityEvent)) == 5
            assert await session.scalar(select(func.count()).select_from(ProjectionCheckpoint)) == 4

        failing_service = ModerationService(
            SqlAlchemyUnitOfWorkFactory(session_factory),
            lambda session: cast(ModerationRepository, SqlAlchemyModerationRepository(cast(AsyncSession, session))),
            lambda session: cast(
                AuthorizationRepository,
                SqlAlchemyAuthorizationRepository(cast(AsyncSession, session)),
            ),
            lambda session: SqlAlchemyAuditWriter(cast(AsyncSession, session)),
            lambda session: FailingOutbox(),
            FixedClock(),
        )
        with pytest.raises(RuntimeError, match="outbox unavailable"):
            await failing_service.open_case(OpenCase(meta("atomic-failure"), 2, "must roll back", 1))

        async with session_factory() as session:
            assert await session.scalar(select(func.count()).select_from(ModerationCase)) == 1
            assert await session.scalar(select(func.count()).select_from(AuditEvent)) == 5
    finally:
        await engine.dispose()
