"""Provide protocol-neutral authorization queries."""

from perfcho.modules.authorization.models import EffectiveAuthorization
from perfcho.modules.authorization.ports import AuthorizationRepository
from perfcho.modules.authorization.stable import StablePrivilege, project_stable_privileges
from perfcho.modules.common.ports import Clock


class AuthorizationQueryService:
    """Evaluate current authorization and protocol projections for an account."""

    def __init__(self, repository: AuthorizationRepository, clock: Clock) -> None:
        """Bind authoritative grant storage and the application clock."""
        self._repository = repository
        self._clock = clock

    async def get_effective(self, account_id: int) -> EffectiveAuthorization:
        """Return the account's authorization at one consistent current instant."""
        return await self._repository.get_effective(account_id, at=self._clock.now())

    async def get_stable_privileges(self, account_id: int) -> StablePrivilege:
        """Return Stable client bits projected from current canonical authorization."""
        return project_stable_privileges(await self.get_effective(account_id))
