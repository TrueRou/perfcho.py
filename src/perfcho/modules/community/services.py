"""Provide protocol-neutral durable channel and messaging services."""

import uuid
from collections.abc import Callable
from datetime import datetime

from perfcho.modules.authorization.models import EffectiveAuthorization
from perfcho.modules.common.models import PendingEvent
from perfcho.modules.common.ports import Clock
from perfcho.modules.community.errors import (
    AccountSilenced,
    ChannelAccessDenied,
    ChannelNotFound,
    CommunityInputRejected,
    DirectMessageBlocked,
    MembershipRejected,
    MessageIdempotencyConflict,
    MessageNotFound,
    PrivateMessageRejected,
)
from perfcho.modules.community.models import (
    ChannelMembershipResult,
    ChannelPermissions,
    ChannelRecord,
    DirectConversationResult,
    DirectMessageContext,
    MessageResult,
    OfflineDirectMessage,
    ReadCursorResult,
    StableChannel,
)
from perfcho.modules.community.ports import (
    ActiveSilencePolicyFactory,
    AuthorizationRepositoryFactory,
    CommunityOutboxWriterFactory,
    CommunityRepository,
    CommunityRepositoryFactory,
    CommunityUnitOfWork,
)

_MESSAGE_CONSUMERS = ("community-message.v1",)
_COMMUNITY_CONSUMERS = ("community-projection.v1",)
_DURABLE_MEMBERSHIP_KINDS = frozenset({"private", "group", "team"})


class CommunityService:
    """Coordinate channel authorization, direct messages, cursors, and memberships."""

    def __init__(
        self,
        uow_factory: Callable[[], CommunityUnitOfWork],
        repository_factory: CommunityRepositoryFactory,
        authorization_repository_factory: AuthorizationRepositoryFactory,
        silence_policy_factory: ActiveSilencePolicyFactory,
        outbox_writer_factory: CommunityOutboxWriterFactory,
        clock: Clock,
    ) -> None:
        """Bind transaction, persistence, policy, event, and time dependencies."""
        self._uow_factory = uow_factory
        self._repository_factory = repository_factory
        self._authorization_repository_factory = authorization_repository_factory
        self._silence_policy_factory = silence_policy_factory
        self._outbox_writer_factory = outbox_writer_factory
        self._clock = clock

    async def list_public_channels(self, account_id: int) -> tuple[StableChannel, ...]:
        """Return active public channels readable by current canonical authorization."""
        _validate_account_id(account_id)
        now = self._clock.now()
        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            authorization = await self._authorization_repository_factory(uow.session).get_effective(
                account_id,
                at=now,
            )
            channels = await repository.list_public_channels(account_id)
            visible: list[StableChannel] = []
            for channel in channels:
                permissions = _evaluate_permissions(channel, account_id, authorization)
                if permissions.can_read:
                    visible.append(_stable_channel(channel, permissions))
            return tuple(visible)

    async def get_public_channel_by_stable_name(self, account_id: int, stable_name: str) -> StableChannel:
        """Resolve one readable public channel by a normalized Stable name."""
        _validate_account_id(account_id)
        normalized_name = _normalize_stable_channel_name(stable_name)
        now = self._clock.now()
        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            channel = await repository.get_public_channel_by_stable_name(normalized_name, account_id)
            if channel is None:
                raise ChannelNotFound("public channel does not exist")
            authorization = await self._authorization_repository_factory(uow.session).get_effective(
                account_id,
                at=now,
            )
            permissions = _evaluate_permissions(channel, account_id, authorization)
            if not permissions.can_read:
                raise ChannelNotFound("public channel does not exist")
            return _stable_channel(channel, permissions)

    async def get_channel_permissions(self, account_id: int, channel_id: int) -> ChannelPermissions:
        """Evaluate account-specific channel permissions through canonical authorization."""
        _validate_account_id(account_id)
        _validate_account_id(channel_id)
        now = self._clock.now()
        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            channel = await repository.get_channel(channel_id, account_id)
            if channel is None or channel.archived:
                raise ChannelNotFound("channel does not exist")
            authorization = await self._authorization_repository_factory(uow.session).get_effective(
                account_id,
                at=now,
            )
            return _evaluate_permissions(channel, account_id, authorization)

    async def create_direct_conversation(
        self,
        first_account_id: int,
        second_account_id: int,
    ) -> DirectConversationResult:
        """Create or return the unique durable channel for an ordered account pair."""
        _validate_pair(first_account_id, second_account_id)
        now = self._clock.now()
        low_account_id, high_account_id = sorted((first_account_id, second_account_id))
        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            await repository.acquire_pair_lock(low_account_id, high_account_id)
            context = await repository.get_direct_message_context(first_account_id, second_account_id)
            _require_existing_pair(context, first_account_id, second_account_id)
            conversation = await repository.get_or_create_direct_conversation(
                low_account_id,
                high_account_id,
                now=now,
            )
            if conversation.created:
                await self._append_conversation_event(uow.session, conversation)
            await uow.commit()
            return conversation

    async def send_public_message(
        self,
        sender_account_id: int,
        stable_channel_name: str,
        client_message_id: uuid.UUID,
        content: str,
        *,
        is_action: bool = False,
        reply_to_id: int | None = None,
    ) -> MessageResult:
        """Authorize and durably send one idempotent public-channel message."""
        _validate_account_id(sender_account_id)
        _validate_client_message_id(client_message_id)
        if reply_to_id is not None:
            _validate_account_id(reply_to_id)
        normalized_name = _normalize_stable_channel_name(stable_channel_name)
        now = self._clock.now()
        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            channel = await repository.get_public_channel_by_stable_name(normalized_name, sender_account_id)
            if channel is None:
                raise ChannelNotFound("public channel does not exist")
            previous = await repository.get_message_by_client_id(sender_account_id, client_message_id)
            if previous is not None:
                _require_exact_message(previous, channel.channel_id, content, is_action, reply_to_id, None)
                await uow.commit()
                return _as_replay(previous)
            _validate_message_content(content, channel.message_length_limit)
            authorization = await self._authorization_repository_factory(uow.session).get_effective(
                sender_account_id,
                at=now,
            )
            permissions = _evaluate_permissions(channel, sender_account_id, authorization)
            if not permissions.can_write:
                raise ChannelAccessDenied("channel is not writable")
            await self._require_not_silenced(uow.session, sender_account_id, channel.channel_id, now)
            await _require_reply_target(repository, channel.channel_id, reply_to_id)
            message = await repository.insert_message(
                channel_id=channel.channel_id,
                sender_account_id=sender_account_id,
                client_message_id=client_message_id,
                content=content,
                is_action=is_action,
                reply_to_id=reply_to_id,
                now=now,
            )
            _require_exact_message(message, channel.channel_id, content, is_action, reply_to_id, None)
            if message.created:
                await self._append_message_event(uow.session, message)
            await uow.commit()
            return message

    async def send_direct_message(
        self,
        sender_account_id: int,
        recipient_account_id: int,
        client_message_id: uuid.UUID,
        content: str,
        *,
        is_action: bool = False,
        reply_to_id: int | None = None,
    ) -> MessageResult:
        """Apply blocks, PM policy, silence, and idempotency before a durable DM."""
        _validate_pair(sender_account_id, recipient_account_id)
        _validate_client_message_id(client_message_id)
        if reply_to_id is not None:
            _validate_account_id(reply_to_id)
        now = self._clock.now()
        low_account_id, high_account_id = sorted((sender_account_id, recipient_account_id))
        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            await repository.acquire_pair_lock(low_account_id, high_account_id)
            context = await repository.get_direct_message_context(sender_account_id, recipient_account_id)
            _require_existing_pair(context, sender_account_id, recipient_account_id)
            previous = await repository.get_message_by_client_id(sender_account_id, client_message_id)
            if previous is not None:
                _require_exact_direct_message(
                    previous,
                    recipient_account_id,
                    content,
                    is_action,
                    reply_to_id,
                )
                await uow.commit()
                return _as_replay(previous)
            _enforce_direct_message_context(context)
            authorization = await self._authorization_repository_factory(uow.session).get_effective(
                sender_account_id,
                at=now,
            )
            if not authorization.allows("chat.write"):
                raise ChannelAccessDenied("direct messages are not writable")
            conversation = await repository.get_or_create_direct_conversation(
                low_account_id,
                high_account_id,
                now=now,
            )
            _validate_message_content(content, conversation.message_length_limit)
            await self._require_not_silenced(uow.session, sender_account_id, conversation.channel_id, now)
            await _require_reply_target(repository, conversation.channel_id, reply_to_id)
            message = await repository.insert_message(
                channel_id=conversation.channel_id,
                sender_account_id=sender_account_id,
                client_message_id=client_message_id,
                content=content,
                is_action=is_action,
                reply_to_id=reply_to_id,
                now=now,
            )
            message = MessageResult(
                message_id=message.message_id,
                channel_id=message.channel_id,
                sender_account_id=message.sender_account_id,
                client_message_id=message.client_message_id,
                content=message.content,
                is_action=message.is_action,
                reply_to_id=message.reply_to_id,
                created_at=message.created_at,
                direct_recipient_account_id=recipient_account_id,
                created=message.created,
            )
            _require_exact_direct_message(message, recipient_account_id, content, is_action, reply_to_id)
            if conversation.created:
                await self._append_conversation_event(uow.session, conversation)
            if message.created:
                await self._append_message_event(uow.session, message)
            await uow.commit()
            return message

    async def mark_read(self, account_id: int, channel_id: int, message_id: int) -> ReadCursorResult:
        """Advance a readable channel cursor monotonically to a channel message."""
        _validate_account_id(account_id)
        _validate_account_id(channel_id)
        _validate_account_id(message_id)
        now = self._clock.now()
        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            channel = await repository.get_channel(channel_id, account_id)
            if channel is None or channel.archived:
                raise ChannelNotFound("channel does not exist")
            authorization = await self._authorization_repository_factory(uow.session).get_effective(
                account_id,
                at=now,
            )
            if not _evaluate_permissions(channel, account_id, authorization).can_read:
                raise ChannelAccessDenied("channel is not readable")
            if not await repository.message_belongs_to_channel(channel_id, message_id):
                raise MessageNotFound("message does not belong to channel")
            result = await repository.advance_read_cursor(
                channel_id,
                account_id,
                message_id,
                now=now,
            )
            await uow.commit()
            return result

    async def list_unread_offline_direct_messages(
        self,
        account_id: int,
        *,
        limit: int = 100,
    ) -> tuple[OfflineDirectMessage, ...]:
        """Return unread incoming direct messages for ordered Stable delivery."""
        _validate_account_id(account_id)
        if isinstance(limit, bool) or not 1 <= limit <= 500:
            raise CommunityInputRejected("offline direct-message limit must be between 1 and 500")
        async with self._uow_factory() as uow:
            return await self._repository_factory(uow.session).list_unread_direct_messages(account_id, limit=limit)

    async def join_channel(self, account_id: int, channel_id: int) -> ChannelMembershipResult:
        """Join a channel durably only when its kind has persistent membership."""
        return await self._change_membership(account_id, channel_id, joining=True)

    async def leave_channel(self, account_id: int, channel_id: int) -> ChannelMembershipResult:
        """Leave a channel durably only when its kind has persistent membership."""
        return await self._change_membership(account_id, channel_id, joining=False)

    async def _change_membership(
        self,
        account_id: int,
        channel_id: int,
        *,
        joining: bool,
    ) -> ChannelMembershipResult:
        _validate_account_id(account_id)
        _validate_account_id(channel_id)
        now = self._clock.now()
        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            channel = await repository.get_channel(channel_id, account_id)
            if channel is None or channel.archived:
                raise ChannelNotFound("channel does not exist")
            if channel.direct:
                raise MembershipRejected("direct-conversation membership is fixed by its account pair")
            authorization = await self._authorization_repository_factory(uow.session).get_effective(
                account_id,
                at=now,
            )
            permissions = _evaluate_permissions(channel, account_id, authorization)
            if joining and not permissions.can_read:
                raise ChannelAccessDenied("channel is not readable")
            durable = channel.kind in _DURABLE_MEMBERSHIP_KINDS
            changed = False
            if durable:
                if channel.kind == "team" and not channel.active_team_member and not permissions.can_manage:
                    raise ChannelAccessDenied("account is not an active team member")
                changed = (
                    await repository.join_membership(channel_id, account_id, now=now)
                    if joining
                    else await repository.leave_membership(channel_id, account_id, now=now)
                )
                if changed:
                    await self._outbox_writer_factory(uow.session).append(
                        PendingEvent(
                            aggregate_type="channel",
                            aggregate_id=str(channel_id),
                            event_type=(
                                "community.channel-member-joined.v1" if joining else "community.channel-member-left.v1"
                            ),
                            schema_version=1,
                            payload={"channel_id": channel_id, "account_id": account_id},
                            consumers=_COMMUNITY_CONSUMERS,
                            partition_key=f"channel:{channel_id}",
                        )
                    )
            await uow.commit()
            return ChannelMembershipResult(channel_id, account_id, joining, durable, changed)

    async def _require_not_silenced(
        self,
        session: object,
        account_id: int,
        channel_id: int,
        now: datetime,
    ) -> None:
        silence = await self._silence_policy_factory(session).get_active_silence(
            account_id,
            channel_id=channel_id,
            at=now,
        )
        if silence is not None:
            raise AccountSilenced(silence.reason)

    async def _append_conversation_event(self, session: object, result: DirectConversationResult) -> None:
        await self._outbox_writer_factory(session).append(
            PendingEvent(
                aggregate_type="channel",
                aggregate_id=str(result.channel_id),
                event_type="community.direct-conversation-created.v1",
                schema_version=1,
                payload={
                    "channel_id": result.channel_id,
                    "low_account_id": result.low_account_id,
                    "high_account_id": result.high_account_id,
                },
                consumers=_COMMUNITY_CONSUMERS,
                partition_key=f"channel:{result.channel_id}",
            )
        )

    async def _append_message_event(self, session: object, message: MessageResult) -> None:
        await self._outbox_writer_factory(session).append(
            PendingEvent(
                aggregate_type="channel",
                aggregate_id=str(message.channel_id),
                event_type="community.message-sent.v1",
                schema_version=1,
                payload={
                    "message_id": message.message_id,
                    "channel_id": message.channel_id,
                    "sender_account_id": message.sender_account_id,
                    "direct_recipient_account_id": message.direct_recipient_account_id,
                    "client_message_id": str(message.client_message_id),
                    "content": message.content,
                    "is_action": message.is_action,
                    "reply_to_id": message.reply_to_id,
                    "created_at": message.created_at.isoformat(),
                },
                consumers=_MESSAGE_CONSUMERS,
                partition_key=f"channel:{message.channel_id}",
            )
        )


def _evaluate_permissions(
    channel: ChannelRecord,
    account_id: int,
    authorization: EffectiveAuthorization,
) -> ChannelPermissions:
    manage = (
        authorization.allows("chat.manage")
        or channel.owner_account_id == account_id
        or channel.manage_permission_code is not None
        and authorization.allows(channel.manage_permission_code)
    )
    if channel.direct:
        in_scope = channel.includes_direct_account(account_id)
        can_read = in_scope and (authorization.allows("chat.read") or manage)
        can_write = in_scope and (authorization.allows("chat.write") or manage)
        return ChannelPermissions(channel.channel_id, account_id, can_read, can_write, manage and in_scope)

    public_scope = channel.kind in {"public", "system"}
    member_scope = channel.active_member or channel.active_team_member or channel.owner_account_id == account_id
    in_scope = public_scope or member_scope or manage
    can_read = in_scope and (
        channel.read_permission_code is None or authorization.allows(channel.read_permission_code) or manage
    )
    can_write = can_read and (
        channel.write_permission_code is None or authorization.allows(channel.write_permission_code) or manage
    )
    can_manage = in_scope and manage
    return ChannelPermissions(channel.channel_id, account_id, can_read, can_write, can_manage)


def _stable_channel(channel: ChannelRecord, permissions: ChannelPermissions) -> StableChannel:
    if channel.stable_name is None:
        raise RuntimeError("public channel has no Stable-facing name")
    return StableChannel(
        channel_id=channel.channel_id,
        name=channel.stable_name,
        topic=channel.description or "",
        auto_join=channel.auto_join,
        message_length_limit=channel.message_length_limit,
        can_write=permissions.can_write,
        can_manage=permissions.can_manage,
    )


def _enforce_direct_message_context(context: DirectMessageContext) -> None:
    if context.blocked:
        raise DirectMessageBlocked("direct messages are blocked between these accounts")
    if context.recipient_policy == "all":
        return
    if context.recipient_policy == "friends" and context.mutual_friends:
        return
    raise PrivateMessageRejected("recipient does not accept this private message")


def _require_existing_pair(context: DirectMessageContext, first_account_id: int, second_account_id: int) -> None:
    if context.existing_account_ids != frozenset((first_account_id, second_account_id)):
        raise ChannelNotFound("one or more direct-message accounts do not exist")


async def _require_reply_target(
    repository: CommunityRepository,
    channel_id: int,
    reply_to_id: int | None,
) -> None:
    if reply_to_id is not None and not await repository.message_belongs_to_channel(
        channel_id,
        reply_to_id,
    ):
        raise MessageNotFound("reply target does not belong to channel")


def _require_exact_message(
    message: MessageResult,
    channel_id: int,
    content: str,
    is_action: bool,
    reply_to_id: int | None,
    recipient_account_id: int | None,
) -> None:
    if (
        message.channel_id != channel_id
        or message.content != content
        or message.is_action != is_action
        or message.reply_to_id != reply_to_id
        or message.direct_recipient_account_id != recipient_account_id
    ):
        raise MessageIdempotencyConflict("client message UUID was already used for another message")


def _require_exact_direct_message(
    message: MessageResult,
    recipient_account_id: int,
    content: str,
    is_action: bool,
    reply_to_id: int | None,
) -> None:
    if (
        message.direct_recipient_account_id != recipient_account_id
        or message.content != content
        or message.is_action != is_action
        or message.reply_to_id != reply_to_id
    ):
        raise MessageIdempotencyConflict("client message UUID was already used for another message")


def _as_replay(message: MessageResult) -> MessageResult:
    return MessageResult(
        message_id=message.message_id,
        channel_id=message.channel_id,
        sender_account_id=message.sender_account_id,
        client_message_id=message.client_message_id,
        content=message.content,
        is_action=message.is_action,
        reply_to_id=message.reply_to_id,
        created_at=message.created_at,
        direct_recipient_account_id=message.direct_recipient_account_id,
        created=False,
    )


def _normalize_stable_channel_name(stable_name: str) -> str:
    normalized = stable_name.strip().casefold()
    if normalized and not normalized.startswith("#"):
        normalized = f"#{normalized}"
    if len(normalized) < 2 or len(normalized) > 100:
        raise CommunityInputRejected("invalid Stable channel name")
    return normalized


def _validate_message_content(content: str, limit: int) -> None:
    if not isinstance(content, str) or not 1 <= len(content) <= limit:
        raise CommunityInputRejected(f"message content must contain between 1 and {limit} characters")


def _validate_client_message_id(client_message_id: uuid.UUID) -> None:
    if not isinstance(client_message_id, uuid.UUID):
        raise CommunityInputRejected("client_message_id must be a UUID")


def _validate_account_id(account_id: int) -> None:
    if isinstance(account_id, bool) or not isinstance(account_id, int) or account_id < 1:
        raise CommunityInputRejected("identifiers must be positive integers")


def _validate_pair(first_account_id: int, second_account_id: int) -> None:
    _validate_account_id(first_account_id)
    _validate_account_id(second_account_id)
    if first_account_id == second_account_id:
        raise CommunityInputRejected("direct-conversation accounts must be distinct")
