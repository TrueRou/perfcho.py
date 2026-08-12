"""Provide protocol-neutral authorization queries."""

from perfcho.infra.cache import cached
from perfcho.infra.cache.backend import CacheBackend
from perfcho.infra.cache.values import decode_json, encode_json
from perfcho.modules.authorization.models import EffectiveAuthorization
from perfcho.modules.authorization.ports import AuthorizationRepository
from perfcho.modules.common.ports import Clock


class AuthorizationQueryService:
    """Evaluate current authorization for an account."""

    def __init__(self, repository: AuthorizationRepository, clock: Clock, cache: CacheBackend) -> None:
        """Bind authoritative grant storage and the application clock."""
        self._repository = repository
        self._clock = clock
        self._cache = cache

    @cached(
        key_builder=lambda self, account_id: self._cache.key("authorization", "effective", str(account_id)),
        encode=lambda value: encode_json(value),
        decode=lambda raw: _authorization_from_cache(raw),
        ttl_seconds=10,
        return_loaded=True,
    )
    async def get_effective(self, account_id: int) -> EffectiveAuthorization:
        """Return the account's authorization at one consistent current instant."""
        return await self._repository.get_effective(account_id, at=self._clock.now())


def _authorization_from_cache(raw: bytes) -> EffectiveAuthorization:
    value = decode_json(raw)
    return EffectiveAuthorization(
        account_id=int(value["account_id"]),
        evaluated_at=value["evaluated_at"],
        permission_codes=frozenset(value["permission_codes"]),
        role_codes=frozenset(value["role_codes"]),
        entitlement_codes=frozenset(value["entitlement_codes"]),
    )
