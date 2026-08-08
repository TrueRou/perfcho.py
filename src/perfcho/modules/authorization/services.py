"""Provide protocol-neutral authorization queries."""

from perfcho.infra.cache.backend import CacheBackend
from perfcho.infra.cache.values import decode_json, encode_json
from perfcho.modules.authorization.models import EffectiveAuthorization
from perfcho.modules.authorization.ports import AuthorizationRepository
from perfcho.modules.authorization.stable import StablePrivilege, project_stable_privileges
from perfcho.modules.common.ports import Clock


class AuthorizationQueryService:
    """Evaluate current authorization and protocol projections for an account."""

    def __init__(self, repository: AuthorizationRepository, clock: Clock, cache: CacheBackend) -> None:
        """Bind authoritative grant storage and the application clock."""
        self._repository = repository
        self._clock = clock
        self._cache = cache

    async def get_effective(self, account_id: int) -> EffectiveAuthorization:
        """Return the account's authorization at one consistent current instant."""
        key = self._cache.key("authorization", "effective", str(account_id))
        raw = await self._cache.get(key)
        if raw is not None:
            try:
                value = decode_json(raw)
                return EffectiveAuthorization(
                    account_id=int(value["account_id"]),
                    evaluated_at=self._clock.now(),
                    permission_codes=frozenset(value["permission_codes"]),
                    role_codes=frozenset(value["role_codes"]),
                    entitlement_codes=frozenset(value["entitlement_codes"]),
                )
            except KeyError, TypeError, ValueError:
                await self._cache.delete(key)
        result = await self._repository.get_effective(account_id, at=self._clock.now())
        await self._cache.set(
            key,
            encode_json(
                {
                    "account_id": result.account_id,
                    "permission_codes": tuple(result.permission_codes),
                    "role_codes": tuple(result.role_codes),
                    "entitlement_codes": tuple(result.entitlement_codes),
                }
            ),
            ttl_seconds=10,
        )
        return result

    async def get_stable_privileges(self, account_id: int) -> StablePrivilege:
        """Return Stable client bits projected from current canonical authorization."""
        return project_stable_privileges(await self.get_effective(account_id))
