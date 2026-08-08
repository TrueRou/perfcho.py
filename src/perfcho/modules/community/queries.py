"""Read community state outside command transactions."""

from collections.abc import Callable

from perfcho.modules.authorization.models import EffectiveAuthorization
from perfcho.modules.authorization.services import AuthorizationQueryService
from perfcho.modules.common.ports import Clock
from perfcho.modules.community.errors import ChannelAccessDenied, ChannelNotFound, CommunityInputRejected
from perfcho.modules.community.models import OfflineDirectMessage, OfflineDirectMessagePage, StableChannel
from perfcho.modules.community.ports import (
    ActiveChannelMembershipQuery,
    ActiveSilencePolicyFactory,
    CommunityRepositoryFactory,
    CommunityUnitOfWork,
)
from perfcho.modules.community.services import (
    _evaluate_permissions,
    _normalize_stable_channel_name,
    _remaining_seconds,
    _stable_channel,
    _validate_account_id,
)


class CommunityQueryService:
    """Serve channel and offline-message reads through canonical policies."""

    def __init__(
        self,
        uow_factory: Callable[[], CommunityUnitOfWork],
        repository_factory: CommunityRepositoryFactory,
        silence_policy_factory: ActiveSilencePolicyFactory,
        authorization: AuthorizationQueryService,
        active_memberships: ActiveChannelMembershipQuery,
        clock: Clock,
    ) -> None:
        self._uow_factory = uow_factory
        self._repository_factory = repository_factory
        self._silence_policy_factory = silence_policy_factory
        self._authorization = authorization
        self._active_memberships = active_memberships
        self._clock = clock

    async def list_public_channels(
        self, account_id: int, *, authorization: EffectiveAuthorization | None = None
    ) -> tuple[StableChannel, ...]:
        _validate_account_id(account_id)
        effective = authorization or await self._authorization.get_effective(account_id)
        async with self._uow_factory() as uow:
            channels = await self._repository_factory(uow.session).list_public_channels(account_id)
        return tuple(
            _stable_channel(channel, permissions)
            for channel in channels
            if (permissions := _evaluate_permissions(channel, account_id, effective)).can_read
        )

    async def get_public_channel_by_stable_name(self, account_id: int, stable_name: str) -> StableChannel:
        _validate_account_id(account_id)
        normalized = _normalize_stable_channel_name(stable_name)
        async with self._uow_factory() as uow:
            channel = await self._repository_factory(uow.session).get_public_channel_by_stable_name(
                normalized, account_id
            )
        if channel is None:
            raise ChannelNotFound("public channel does not exist")
        permissions = _evaluate_permissions(channel, account_id, await self._authorization.get_effective(account_id))
        if not permissions.can_read:
            raise ChannelNotFound("public channel does not exist")
        return _stable_channel(channel, permissions)

    async def list_unread_offline_direct_messages(
        self, account_id: int, *, limit: int = 100
    ) -> tuple[OfflineDirectMessage, ...]:
        return (await self.list_unread_offline_direct_message_page(account_id, limit=limit)).messages

    async def list_unread_offline_direct_message_page(
        self, account_id: int, *, after_message_id: int | None = None, limit: int = 100
    ) -> OfflineDirectMessagePage:
        _validate_account_id(account_id)
        if not 1 <= limit <= 500:
            raise CommunityInputRejected("offline direct-message limit must be between 1 and 500")
        async with self._uow_factory() as uow:
            messages = await self._repository_factory(uow.session).list_unread_direct_messages(
                account_id, after_message_id=after_message_id, limit=limit + 1
            )
        page = messages[:limit]
        return OfflineDirectMessagePage(page, page[-1].message_id if len(messages) > limit else None)

    async def get_global_silence_remaining_seconds(self, account_id: int) -> int:
        _validate_account_id(account_id)
        now = self._clock.now()
        async with self._uow_factory() as uow:
            silence = await self._silence_policy_factory(uow.session).get_active_silence(
                account_id, channel_id=None, at=now
            )
        if silence is None:
            return 0
        remaining = _remaining_seconds(silence.ends_at, now)
        return 2**31 - 1 if remaining is None else min(remaining, 2**31 - 1)

    async def get_channel_member_count(
        self, account_id: int, channel_id: int, *, already_authorized: bool = False
    ) -> int:
        _validate_account_id(account_id)
        _validate_account_id(channel_id)
        if not already_authorized:
            async with self._uow_factory() as uow:
                channel = await self._repository_factory(uow.session).get_channel(channel_id, account_id)
            if channel is None or channel.archived:
                raise ChannelNotFound("channel does not exist")
            authorization = await self._authorization.get_effective(account_id)
            if not _evaluate_permissions(channel, account_id, authorization).can_read:
                raise ChannelAccessDenied("channel is not readable")
        count = await self._active_memberships.count_active_members(channel_id, at=self._clock.now())
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise RuntimeError("active channel membership query returned an invalid count")
        return count
