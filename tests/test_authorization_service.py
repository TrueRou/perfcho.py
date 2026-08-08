from dataclasses import FrozenInstanceError
from datetime import UTC, datetime
from typing import cast
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra.db.enums import GrantEffect
from perfcho.infra.db.repositories.authorization import SqlAlchemyAuthorizationRepository
from perfcho.modules.authorization import (
    AuthorizationQueryService,
    EffectiveAuthorization,
    StablePrivilege,
    project_stable_privileges,
)
from perfcho.modules.authorization.ports import AuthorizationRepository
from tests.cache_support import MemoryCache


class FixedClock:
    def __init__(self, instant: datetime) -> None:
        self.instant = instant

    def now(self) -> datetime:
        return self.instant


class StubAuthorizationRepository:
    def __init__(self, authorization: EffectiveAuthorization) -> None:
        self.authorization = authorization
        self.requested: tuple[int, datetime] | None = None

    async def get_effective(self, account_id: int, *, at: datetime) -> EffectiveAuthorization:
        self.requested = (account_id, at)
        return self.authorization


class RowResult:
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self.rows = rows

    def all(self) -> list[tuple[object, ...]]:
        return self.rows


class ScalarResult:
    def __init__(self, values: list[str]) -> None:
        self.values = values

    def all(self) -> list[str]:
        return self.values


def _authorization(
    instant: datetime,
    *,
    permissions: frozenset[str] = frozenset(),
    roles: frozenset[str] = frozenset(),
    entitlements: frozenset[str] = frozenset(),
) -> EffectiveAuthorization:
    return EffectiveAuthorization(
        account_id=42,
        evaluated_at=instant,
        permission_codes=permissions,
        role_codes=roles,
        entitlement_codes=entitlements,
    )


def test_effective_authorization_is_immutable_and_defensively_freezes_codes() -> None:
    instant = datetime(2026, 7, 28, tzinfo=UTC)
    permissions = {"account.login"}
    authorization = EffectiveAuthorization(
        account_id=42,
        evaluated_at=instant,
        permission_codes=cast(frozenset[str], permissions),
        role_codes=frozenset({"user"}),
        entitlement_codes=frozenset(),
    )
    permissions.add("admin.access")

    assert authorization.permission_codes == frozenset({"account.login"})
    assert authorization.allows("account.login")
    with pytest.raises(FrozenInstanceError):
        authorization.__setattr__("account_id", 7)
    with pytest.raises(ValueError, match="timezone-aware"):
        _authorization(datetime(2026, 7, 28))


@pytest.mark.asyncio
async def test_query_service_uses_one_clock_instant_for_the_repository() -> None:
    instant = datetime(2026, 7, 28, 12, 30, tzinfo=UTC)
    authorization = _authorization(instant, permissions=frozenset({"account.login"}))
    repository = StubAuthorizationRepository(authorization)
    service = AuthorizationQueryService(repository, FixedClock(instant), MemoryCache())

    assert await service.get_effective(42) is authorization
    assert repository.requested == (42, instant)
    assert await service.get_stable_privileges(42) == StablePrivilege.PLAYER


def test_stable_projection_uses_canonical_codes() -> None:
    instant = datetime(2026, 7, 28, tzinfo=UTC)
    authorization = _authorization(
        instant,
        permissions=frozenset({"account.login", "moderation.enforce", "admin.access"}),
        roles=frozenset({"administrator"}),
        entitlements=frozenset({"supporter"}),
    )

    assert project_stable_privileges(authorization) == (
        StablePrivilege.PLAYER
        | StablePrivilege.SUPPORTER
        | StablePrivilege.MODERATOR
        | StablePrivilege.DEVELOPER
        | StablePrivilege.OWNER
    )
    assert int(StablePrivilege.PLAYER) == 1
    assert int(StablePrivilege.MODERATOR) == 2
    assert int(StablePrivilege.SUPPORTER) == 4
    assert int(StablePrivilege.OWNER) == 8
    assert int(StablePrivilege.DEVELOPER) == 16
    assert int(project_stable_privileges(authorization)) == 31
    assert (
        project_stable_privileges(
            _authorization(
                instant,
                permissions=frozenset({"account.read", "moderation.read"}),
                roles=frozenset({"user", "moderator"}),
                entitlements=frozenset({"premium"}),
            )
        )
        == StablePrivilege.NONE
    )


@pytest.mark.asyncio
async def test_sqlalchemy_repository_filters_active_grants_and_denies_override_allows() -> None:
    instant = datetime(2026, 7, 28, 12, 30, tzinfo=UTC)
    session = MagicMock(spec=AsyncSession)
    session.execute = AsyncMock(
        side_effect=(
            RowResult(
                [
                    ("user", "account.login"),
                    ("user", "chat.write"),
                    ("moderator", "moderation.enforce"),
                ]
            ),
            RowResult(
                [
                    ("account.login", GrantEffect.DENY),
                    ("admin.access", GrantEffect.ALLOW),
                    ("chat.write", GrantEffect.DENY),
                ]
            ),
        )
    )
    session.scalars = AsyncMock(return_value=ScalarResult(["supporter"]))
    repository: AuthorizationRepository = SqlAlchemyAuthorizationRepository(session)

    authorization = await repository.get_effective(42, at=instant)

    assert authorization == EffectiveAuthorization(
        account_id=42,
        evaluated_at=instant,
        permission_codes=frozenset({"moderation.enforce", "admin.access"}),
        role_codes=frozenset({"user", "moderator"}),
        entitlement_codes=frozenset({"supporter"}),
    )
    statements = [call.args[0] for call in session.execute.await_args_list]
    statements.append(session.scalars.await_args.args[0])
    assert len(statements) == 3
    for statement in statements:
        sql = str(statement)
        assert "revoked_at IS NULL" in sql
        assert "starts_at <=" in sql
        assert "ends_at IS NULL OR" in sql
        assert "ends_at >" in sql
