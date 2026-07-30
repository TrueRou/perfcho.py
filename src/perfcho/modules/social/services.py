"""Provide protocol-neutral social relationship and achievement services."""

import uuid
from collections.abc import Callable, Mapping

from perfcho.modules.common.models import PendingEvent
from perfcho.modules.common.normalization import normalize_stable_name
from perfcho.modules.common.ports import Clock
from perfcho.modules.social.errors import (
    AchievementNotFound,
    SocialAccountNotFound,
    SocialInteractionBlocked,
    SocialRelationRejected,
)
from perfcho.modules.social.models import (
    AccountIdentityView,
    Achievement,
    AchievementUnlockResult,
    BlockResult,
    BlockView,
    FollowRecord,
    FollowView,
    PairRelationship,
)
from perfcho.modules.social.ports import (
    SocialOutboxWriterFactory,
    SocialRepository,
    SocialRepositoryFactory,
    SocialUnitOfWork,
)

_SOCIAL_CONSUMERS = ("social-projection.v1",)
_ACHIEVEMENT_CONSUMERS = ("achievement-projection.v1",)


class SocialService:
    """Coordinate follows, blocks, friendship, and achievement facts."""

    def __init__(
        self,
        uow_factory: Callable[[], SocialUnitOfWork],
        repository_factory: SocialRepositoryFactory,
        outbox_writer_factory: SocialOutboxWriterFactory,
        clock: Clock,
    ) -> None:
        """Bind transaction, persistence, event, and time dependencies."""
        self._uow_factory = uow_factory
        self._repository_factory = repository_factory
        self._outbox_writer_factory = outbox_writer_factory
        self._clock = clock

    async def follow(
        self,
        actor_account_id: int,
        target_account_id: int,
        *,
        remark: str | None = None,
    ) -> FollowRecord:
        """Create a follow after clearing only the actor's block; preserve target blocks."""
        _validate_pair(actor_account_id, target_account_id)
        if remark is not None and len(remark) > 64:
            raise SocialRelationRejected("friend remark exceeds 64 characters")
        now = self._clock.now()
        low_account_id, high_account_id = sorted((actor_account_id, target_account_id))
        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            await repository.acquire_pair_lock(actor_account_id, target_account_id)
            await _require_accounts(repository, actor_account_id, target_account_id)
            pair = await repository.get_pair_relationship(actor_account_id, target_account_id)
            if pair.blocks(target_account_id, actor_account_id):
                raise SocialInteractionBlocked("the target account blocks this social interaction")
            if pair.blocks(actor_account_id, target_account_id):
                removed = await repository.delete_block(actor_account_id, target_account_id)
                if not removed:
                    raise RuntimeError("the actor's block disappeared under the account-pair lock")
                await self._outbox_writer_factory(uow.session).append(
                    PendingEvent(
                        aggregate_type="social_pair",
                        aggregate_id=f"{low_account_id}:{high_account_id}",
                        event_type="social.account-unblocked.v1",
                        schema_version=1,
                        payload={"actor_account_id": actor_account_id, "target_account_id": target_account_id},
                        consumers=_SOCIAL_CONSUMERS,
                        partition_key=f"social-pair:{low_account_id}:{high_account_id}",
                    )
                )
            previous = await repository.get_follow(actor_account_id, target_account_id)
            if previous is not None and previous.remark == remark:
                await uow.commit()
                return previous
            follow = await repository.upsert_follow(
                actor_account_id,
                target_account_id,
                remark=remark,
                now=now,
            )
            await self._outbox_writer_factory(uow.session).append(
                PendingEvent(
                    aggregate_type="social_pair",
                    aggregate_id=f"{low_account_id}:{high_account_id}",
                    event_type="social.account-followed.v1",
                    schema_version=1,
                    payload={
                        "actor_account_id": actor_account_id,
                        "target_account_id": target_account_id,
                        "mutual": _would_be_mutual(pair, actor_account_id),
                        "followed_at": follow.created_at.isoformat(),
                    },
                    consumers=_SOCIAL_CONSUMERS,
                    partition_key=f"social-pair:{low_account_id}:{high_account_id}",
                )
            )
            await uow.commit()
            return follow

    async def unfollow(self, actor_account_id: int, target_account_id: int) -> bool:
        """Remove an outgoing follow idempotently under the pair lock."""
        _validate_pair(actor_account_id, target_account_id)
        low_account_id, high_account_id = sorted((actor_account_id, target_account_id))
        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            await repository.acquire_pair_lock(actor_account_id, target_account_id)
            removed = await repository.delete_follow(actor_account_id, target_account_id)
            if removed:
                await self._outbox_writer_factory(uow.session).append(
                    PendingEvent(
                        aggregate_type="social_pair",
                        aggregate_id=f"{low_account_id}:{high_account_id}",
                        event_type="social.account-unfollowed.v1",
                        schema_version=1,
                        payload={"actor_account_id": actor_account_id, "target_account_id": target_account_id},
                        consumers=_SOCIAL_CONSUMERS,
                        partition_key=f"social-pair:{low_account_id}:{high_account_id}",
                    )
                )
            await uow.commit()
            return removed

    async def block(
        self,
        actor_account_id: int,
        target_account_id: int,
        *,
        reason: str | None = None,
    ) -> BlockResult:
        """Create a block and atomically remove both conflicting follow directions."""
        _validate_pair(actor_account_id, target_account_id)
        if reason is not None and len(reason) > 255:
            raise SocialRelationRejected("block reason exceeds 255 characters")
        now = self._clock.now()
        low_account_id, high_account_id = sorted((actor_account_id, target_account_id))
        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            await repository.acquire_pair_lock(actor_account_id, target_account_id)
            await _require_accounts(repository, actor_account_id, target_account_id)
            previous = await repository.get_block(actor_account_id, target_account_id)
            removed_follow_count = await repository.delete_pair_follows(actor_account_id, target_account_id)
            changed = previous is None or previous.reason != reason or removed_follow_count > 0
            block = (
                previous
                if previous is not None and previous.reason == reason
                else await repository.upsert_block(
                    actor_account_id,
                    target_account_id,
                    reason=reason,
                    now=now,
                )
            )
            if changed:
                await self._outbox_writer_factory(uow.session).append(
                    PendingEvent(
                        aggregate_type="social_pair",
                        aggregate_id=f"{low_account_id}:{high_account_id}",
                        event_type="social.account-blocked.v1",
                        schema_version=1,
                        payload={
                            "actor_account_id": actor_account_id,
                            "target_account_id": target_account_id,
                            "blocked_at": block.created_at.isoformat(),
                            "removed_follow_count": removed_follow_count,
                        },
                        consumers=_SOCIAL_CONSUMERS,
                        partition_key=f"social-pair:{low_account_id}:{high_account_id}",
                    )
                )
            await uow.commit()
            return BlockResult(block, removed_follow_count, changed)

    async def unblock(self, actor_account_id: int, target_account_id: int) -> bool:
        """Remove an outgoing block idempotently under the pair lock."""
        _validate_pair(actor_account_id, target_account_id)
        low_account_id, high_account_id = sorted((actor_account_id, target_account_id))
        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            await repository.acquire_pair_lock(actor_account_id, target_account_id)
            removed = await repository.delete_block(actor_account_id, target_account_id)
            if removed:
                await self._outbox_writer_factory(uow.session).append(
                    PendingEvent(
                        aggregate_type="social_pair",
                        aggregate_id=f"{low_account_id}:{high_account_id}",
                        event_type="social.account-unblocked.v1",
                        schema_version=1,
                        payload={"actor_account_id": actor_account_id, "target_account_id": target_account_id},
                        consumers=_SOCIAL_CONSUMERS,
                        partition_key=f"social-pair:{low_account_id}:{high_account_id}",
                    )
                )
            await uow.commit()
            return removed

    async def get_pair_relationship(self, first_account_id: int, second_account_id: int) -> PairRelationship:
        """Return follows, mutual friendship, and blocks for one account pair."""
        _validate_pair(first_account_id, second_account_id)
        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            await _require_accounts(repository, first_account_id, second_account_id)
            return await repository.get_pair_relationship(first_account_id, second_account_id)

    async def resolve_account_by_name(self, display_name: str) -> AccountIdentityView:
        """Resolve one current active account through the Stable name normalization rule."""
        try:
            name_key = normalize_stable_name(display_name)
        except ValueError as error:
            raise SocialAccountNotFound("account does not exist") from error
        async with self._uow_factory() as uow:
            account = await self._repository_factory(uow.session).resolve_account_by_name(name_key)
        if account is None:
            raise SocialAccountNotFound("account does not exist")
        return account

    async def are_mutual_friends(self, first_account_id: int, second_account_id: int) -> bool:
        """Return whether two accounts follow each other and neither blocks the other."""
        relationship = await self.get_pair_relationship(first_account_id, second_account_id)
        return relationship.mutual_friends and not relationship.blocked

    async def list_friends(self, account_id: int) -> tuple[FollowView, ...]:
        """Return outgoing Stable friend entries with a derived mutual flag."""
        _validate_account_id(account_id)
        async with self._uow_factory() as uow:
            return await self._repository_factory(uow.session).list_friends(account_id)

    async def list_follower_account_ids(self, account_id: int) -> frozenset[int]:
        """Return accounts currently following one account."""
        _validate_account_id(account_id)
        async with self._uow_factory() as uow:
            return await self._repository_factory(uow.session).list_follower_account_ids(account_id)

    async def list_blocks(self, account_id: int) -> tuple[BlockView, ...]:
        """Return outgoing block entries with current Stable names."""
        _validate_account_id(account_id)
        async with self._uow_factory() as uow:
            return await self._repository_factory(uow.session).list_blocks(account_id)

    async def filter_message_recipients(
        self,
        sender_account_id: int,
        recipient_account_ids: tuple[int, ...],
    ) -> tuple[int, ...]:
        """Remove recipients who currently block the sender using one batch query."""
        _validate_account_id(sender_account_id)
        if not isinstance(recipient_account_ids, tuple):
            raise SocialRelationRejected("recipient account IDs must be a tuple")
        recipients = tuple(dict.fromkeys(recipient_account_ids))
        for recipient_account_id in recipients:
            _validate_account_id(recipient_account_id)
        if not recipients:
            return ()
        async with self._uow_factory() as uow:
            blocked_recipient_ids = await self._repository_factory(uow.session).list_blocking_account_ids(
                sender_account_id,
                recipients,
            )
        return tuple(account_id for account_id in recipients if account_id not in blocked_recipient_ids)

    async def list_achievements(
        self,
        *,
        account_id: int | None = None,
        locale: str = "en",
        active_only: bool = True,
    ) -> tuple[Achievement, ...]:
        """Return localized achievement definitions and optional account unlocks."""
        if account_id is not None:
            _validate_account_id(account_id)
        normalized_locale = locale.strip().lower()
        if not normalized_locale or len(normalized_locale) > 16:
            raise ValueError("locale must contain between 1 and 16 characters")
        async with self._uow_factory() as uow:
            return await self._repository_factory(uow.session).list_achievements(
                account_id=account_id,
                locale=normalized_locale,
                active_only=active_only,
            )

    async def unlock_achievement(
        self,
        account_id: int,
        achievement_id: int,
        *,
        score_id: int | None = None,
        source_event_id: uuid.UUID | None = None,
        snapshot: Mapping[str, object] | None = None,
    ) -> AchievementUnlockResult:
        """Record an achievement's first unlock without implementing detection."""
        _validate_account_id(account_id)
        _validate_account_id(achievement_id)
        if score_id is not None:
            _validate_account_id(score_id)
        now = self._clock.now()
        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            if account_id not in await repository.existing_account_ids((account_id,)):
                raise SocialAccountNotFound("account does not exist")
            definition = await repository.get_achievement_definition(achievement_id)
            if definition is None or not definition.active:
                raise AchievementNotFound("achievement is unavailable")
            result = await repository.unlock_achievement(
                account_id=account_id,
                definition=definition,
                score_id=score_id,
                source_event_id=source_event_id,
                snapshot=dict(snapshot or {}),
                now=now,
            )
            if result.created:
                await self._outbox_writer_factory(uow.session).append(
                    PendingEvent(
                        aggregate_type="account",
                        aggregate_id=str(account_id),
                        event_type="social.achievement-unlocked.v1",
                        schema_version=1,
                        payload={
                            "account_id": account_id,
                            "achievement_id": achievement_id,
                            "definition_version": result.unlock.definition_version,
                            "score_id": score_id,
                            "unlocked_at": result.unlock.created_at.isoformat(),
                        },
                        consumers=_ACHIEVEMENT_CONSUMERS,
                        partition_key=f"account:{account_id}",
                    )
                )
            await uow.commit()
            return result


async def _require_accounts(repository: SocialRepository, *account_ids: int) -> None:
    existing_account_ids = await repository.existing_account_ids(tuple(account_ids))
    if existing_account_ids != frozenset(account_ids):
        raise SocialAccountNotFound("one or more accounts do not exist")


def _validate_account_id(account_id: int) -> None:
    if isinstance(account_id, bool) or not isinstance(account_id, int) or account_id < 1:
        raise SocialRelationRejected("account IDs must be positive integers")


def _validate_pair(first_account_id: int, second_account_id: int) -> None:
    _validate_account_id(first_account_id)
    _validate_account_id(second_account_id)
    if first_account_id == second_account_id:
        raise SocialRelationRejected("an account cannot relate to itself")


def _would_be_mutual(pair: PairRelationship, actor_account_id: int) -> bool:
    if actor_account_id == pair.low_account_id:
        return pair.high_follows_low
    return pair.low_follows_high
