"""Read social projections through mandatory query caching."""

from collections.abc import Callable
from datetime import datetime

from perfcho.infra.cache.backend import CacheBackend
from perfcho.infra.cache.values import decode_json, encode_json
from perfcho.modules.common.normalization import normalize_name
from perfcho.modules.social.errors import SocialAccountNotFound, SocialRelationRejected
from perfcho.modules.social.models import AccountIdentityView, BlockView, FollowView, PairRelationship
from perfcho.modules.social.ports import SocialRepositoryFactory, SocialUnitOfWork


class SocialQueryService:
    """Resolve accounts and social relationships with bounded Redis caching."""

    def __init__(
        self,
        uow_factory: Callable[[], SocialUnitOfWork],
        repository_factory: SocialRepositoryFactory,
        cache: CacheBackend,
    ) -> None:
        self._uow_factory = uow_factory
        self._repository_factory = repository_factory
        self._cache = cache

    async def get_pair_relationship(self, first_account_id: int, second_account_id: int) -> PairRelationship:
        _validate_pair(first_account_id, second_account_id)
        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            existing = await repository.existing_account_ids((first_account_id, second_account_id))
            if existing != frozenset((first_account_id, second_account_id)):
                raise SocialAccountNotFound("one or more accounts do not exist")
            return await repository.get_pair_relationship(first_account_id, second_account_id)

    async def resolve_account_by_name(self, display_name: str) -> AccountIdentityView:
        try:
            name_key = normalize_name(display_name)
        except ValueError as error:
            raise SocialAccountNotFound("account does not exist") from error
        key = self._cache.key("social", "account-name", name_key)
        raw = await self._cache.get(key)
        if raw is not None:
            value = decode_json(raw)
            if value is None:
                raise SocialAccountNotFound("account does not exist")
            return AccountIdentityView(int(value["account_id"]), str(value["display_name"]))
        async with self._uow_factory() as uow:
            account = await self._repository_factory(uow.session).resolve_account_by_name(name_key)
        if account is None:
            await self._cache.set(key, encode_json(None), ttl_seconds=5)
            raise SocialAccountNotFound("account does not exist")
        await self._cache.set(key, encode_json(account), ttl_seconds=120)
        return account

    async def are_mutual_friends(self, first_account_id: int, second_account_id: int) -> bool:
        relationship = await self.get_pair_relationship(first_account_id, second_account_id)
        return relationship.mutual_friends and not relationship.blocked

    async def list_friends(self, account_id: int) -> tuple[FollowView, ...]:
        _validate_account_id(account_id)
        key = self.friends_key(account_id)
        raw = await self._cache.get(key)
        if raw is not None:
            return tuple(_follow_view(value) for value in decode_json(raw))
        async with self._uow_factory() as uow:
            friends = await self._repository_factory(uow.session).list_friends(account_id)
        await self._cache.set(key, encode_json(friends), ttl_seconds=30)
        return friends

    async def list_incoming_follower_account_ids(
        self, target_account_id: int, candidate_actor_account_ids: tuple[int, ...]
    ) -> frozenset[int]:
        _validate_account_id(target_account_id)
        candidates = tuple(dict.fromkeys(candidate_actor_account_ids))
        if not candidates:
            return frozenset()
        async with self._uow_factory() as uow:
            return await self._repository_factory(uow.session).list_incoming_follower_account_ids(
                target_account_id, candidates
            )

    async def list_blocks(self, account_id: int) -> tuple[BlockView, ...]:
        _validate_account_id(account_id)
        async with self._uow_factory() as uow:
            return await self._repository_factory(uow.session).list_blocks(account_id)

    async def filter_message_recipients(
        self, sender_account_id: int, recipient_account_ids: tuple[int, ...]
    ) -> tuple[int, ...]:
        recipients = tuple(dict.fromkeys(recipient_account_ids))
        if not recipients:
            return ()
        async with self._uow_factory() as uow:
            blocked = await self._repository_factory(uow.session).list_blocking_account_ids(
                sender_account_id, recipients
            )
        return tuple(account_id for account_id in recipients if account_id not in blocked)

    def friends_key(self, account_id: int) -> str:
        return self._cache.key("social", "friends", str(account_id))

    async def invalidate_friends(self, *account_ids: int) -> None:
        for account_id in set(account_ids):
            await self._cache.delete(self.friends_key(account_id))


def _follow_view(value: object) -> FollowView:
    if not isinstance(value, dict):
        raise ValueError("invalid cached friend")
    followed_at = value["followed_at"]
    if isinstance(followed_at, dict):
        followed_at = followed_at["value"]
    return FollowView(
        int(value["account_id"]),
        str(value["display_name"]),
        value.get("remark"),
        datetime.fromisoformat(str(followed_at)),
        bool(value["mutual"]),
    )


def _validate_account_id(account_id: int) -> None:
    if isinstance(account_id, bool) or not isinstance(account_id, int) or account_id < 1:
        raise SocialRelationRejected("account id must be positive")


def _validate_pair(first: int, second: int) -> None:
    _validate_account_id(first)
    _validate_account_id(second)
    if first == second:
        raise SocialRelationRejected("social relations cannot target the same account")
