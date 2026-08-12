"""Provide protocol-neutral durable channel and messaging services."""

import time
import uuid
from collections.abc import Callable
from dataclasses import replace
from datetime import datetime
from math import ceil

from perfcho.infra.logging import duration_ms, log_event
from perfcho.modules.authorization.models import EffectiveAuthorization
from perfcho.modules.authorization.services import AuthorizationQueryService
from perfcho.modules.common.models import PendingEvent
from perfcho.modules.common.ports import Clock, OutboxWriterFactory
from perfcho.modules.community.errors import (
    AccountSilenced,
    ChannelAccessDenied,
    ChannelMembershipRequired,
    ChannelMembershipUnavailable,
    ChannelNotFound,
    CommunityInputRejected,
    DirectMessageBlocked,
    MembershipRejected,
    MessageIdempotencyConflict,
    MessageNotFound,
    PrivateMessageRejected,
    TargetAccountSilenced,
)
from perfcho.modules.community.models import (
    ChannelMembershipResult,
    ChannelPermissions,
    ChannelRecord,
    ChannelSelector,
    ChannelView,
    ConversationReadCursor,
    DirectConversationResult,
    DirectMessageContext,
    MessageResult,
    OfflineDirectMessage,
    OfflineDirectMessagePage,
    ReadCursorResult,
)
from perfcho.modules.community.ports import (
    ActiveChannelMembershipQuery,
    ActiveSilencePolicyFactory,
    CommunityRepository,
    CommunityRepositoryFactory,
    CommunityUnitOfWork,
)

_MESSAGE_CONSUMERS = ("community-message-projector.v1",)
_COMMUNITY_CONSUMERS = ("community-projector.v1",)
_DURABLE_MEMBERSHIP_KINDS = frozenset({"private", "group", "team"})


class CommunityService:
    """Coordinate channel authorization, direct messages, cursors, and memberships."""

    def __init__(
        self,
        uow_factory: Callable[[], CommunityUnitOfWork],
        repository_factory: CommunityRepositoryFactory,
        authorization: AuthorizationQueryService,
        silence_policy_factory: ActiveSilencePolicyFactory,
        outbox_writer_factory: OutboxWriterFactory,
        clock: Clock,
        active_memberships: ActiveChannelMembershipQuery | None = None,
    ) -> None:
        """Bind transaction, persistence, policy, event, and time dependencies."""
        self._uow_factory = uow_factory
        self._repository_factory = repository_factory
        self._authorization = authorization
        self._silence_policy_factory = silence_policy_factory
        self._outbox_writer_factory = outbox_writer_factory
        self._clock = clock
        self._active_memberships = active_memberships

    async def list_public_channels(self, account_id: int) -> tuple[ChannelView, ...]:
        """Return active public channels readable by current canonical authorization."""
        _validate_account_id(account_id)
        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            authorization = await self._authorization.get_effective(account_id)
            channels = await repository.list_public_channels(account_id)
            visible: list[ChannelView] = []
            for channel in channels:
                permissions = _evaluate_permissions(channel, account_id, authorization)
                if permissions.can_read:
                    visible.append(_channel_view(channel, permissions))
            return tuple(visible)

    async def get_public_channel(self, account_id: int, selector: ChannelSelector) -> ChannelView:
        """Resolve one readable public channel by canonical selector."""
        _validate_account_id(account_id)
        selector = _normalize_channel_selector(selector)
        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            channel = await _load_public_channel(repository, account_id, selector)
            if channel is None:
                raise ChannelNotFound("public channel does not exist")
            authorization = await self._authorization.get_effective(account_id)
            permissions = _evaluate_permissions(channel, account_id, authorization)
            if not permissions.can_read:
                raise ChannelNotFound("public channel does not exist")
            return _channel_view(channel, permissions)

    async def get_channel_permissions(self, account_id: int, channel_id: int) -> ChannelPermissions:
        """Evaluate account-specific channel permissions through canonical authorization."""
        _validate_account_id(account_id)
        _validate_account_id(channel_id)
        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            channel = await repository.get_channel(channel_id, account_id)
            if channel is None or channel.archived:
                raise ChannelNotFound("channel does not exist")
            authorization = await self._authorization.get_effective(account_id)
            return _evaluate_permissions(channel, account_id, authorization)

    async def create_direct_conversation(
        self,
        first_account_id: int,
        second_account_id: int,
    ) -> DirectConversationResult:
        """Create or return the unique durable channel for an ordered account pair."""
        started_ns = time.monotonic_ns()
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
            if conversation.created:
                _log_conversation_change(conversation, started_ns=started_ns)
            return conversation

    async def send_public_message(
        self,
        sender_account_id: int,
        channel_selector: ChannelSelector,
        client_message_id: uuid.UUID,
        content: str,
        *,
        is_action: bool = False,
        reply_to_id: int | None = None,
    ) -> MessageResult:
        """Authorize and durably send one idempotent public-channel message."""
        started_ns = time.monotonic_ns()
        _validate_account_id(sender_account_id)
        _validate_client_message_id(client_message_id)
        if reply_to_id is not None:
            _validate_account_id(reply_to_id)
        selector = _normalize_channel_selector(channel_selector)
        now = self._clock.now()
        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            channel = await _load_public_channel(repository, sender_account_id, selector)
            if channel is None:
                raise ChannelNotFound("public channel does not exist")
            authorization = await self._authorization.get_effective(sender_account_id)
            permissions = _evaluate_permissions(channel, sender_account_id, authorization)
            previous = await repository.get_message_by_client_id(sender_account_id, client_message_id)
            if previous is not None:
                _require_exact_message(previous, channel.channel_id, content, is_action, reply_to_id, None)
                authorization = await self._authorization.get_effective(sender_account_id)
                await uow.commit()
                replay = _as_replay(previous)
                _log_message(replay, started_ns=started_ns)
                return replace(replay, resolved_channel=_channel_view(channel, permissions))
            _validate_message_content(content, channel.message_length_limit)
            authorization = await self._authorization.get_effective(sender_account_id)
            permissions = _evaluate_permissions(channel, sender_account_id, authorization)
            if not permissions.can_write:
                raise ChannelAccessDenied("channel is not writable")
            active_memberships = self._require_active_memberships()
            if not await active_memberships.is_active_member(
                channel.channel_id,
                sender_account_id,
                at=now,
            ):
                raise ChannelMembershipRequired("sender is not an active channel member")
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
            _log_message(message, started_ns=started_ns)
            return replace(message, resolved_channel=_channel_view(channel, permissions))

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
        started_ns = time.monotonic_ns()
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
                replay = _as_replay(previous)
                _log_message(replay, started_ns=started_ns)
                return replay
            _enforce_direct_message_context(context, sender_account_id, recipient_account_id)
            await self._require_target_not_silenced(uow.session, recipient_account_id, now)
            authorization = await self._authorization.get_effective(sender_account_id)
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
            if conversation.created:
                _log_conversation_change(conversation, started_ns=started_ns)
            _log_message(message, started_ns=started_ns)
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
            authorization = await self._authorization.get_effective(account_id)
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
        """Return the first unread incoming direct-message page."""
        page = await self.list_unread_offline_direct_message_page(account_id, limit=limit)
        return page.messages

    async def list_unread_offline_direct_message_page(
        self,
        account_id: int,
        *,
        after_message_id: int | None = None,
        limit: int = 100,
    ) -> OfflineDirectMessagePage:
        """Return an ascending keyset page with an optional continuation cursor."""
        _validate_account_id(account_id)
        if after_message_id is not None:
            _validate_account_id(after_message_id)
        if isinstance(limit, bool) or not 1 <= limit <= 500:
            raise CommunityInputRejected("offline direct-message limit must be between 1 and 500")
        async with self._uow_factory() as uow:
            messages = await self._repository_factory(uow.session).list_unread_direct_messages(
                account_id,
                after_message_id=after_message_id,
                limit=limit + 1,
            )
        page_messages = messages[:limit]
        next_after_message_id = page_messages[-1].message_id if len(messages) > limit else None
        return OfflineDirectMessagePage(page_messages, next_after_message_id)

    async def mark_direct_messages_read(
        self,
        account_id: int,
        cursors: tuple[ConversationReadCursor, ...],
    ) -> tuple[ReadCursorResult, ...]:
        """Atomically advance delivered mail to one cursor per direct conversation."""
        _validate_account_id(account_id)
        if not isinstance(cursors, tuple) or any(not isinstance(cursor, ConversationReadCursor) for cursor in cursors):
            raise CommunityInputRejected("direct-message cursors must be a tuple of conversation cursors")
        if len(cursors) > 500:
            raise CommunityInputRejected("at most 500 direct-message cursors may be marked at once")
        if not cursors:
            return ()
        latest_by_channel: dict[int, int] = {}
        for cursor in cursors:
            latest_by_channel[cursor.channel_id] = max(
                cursor.message_id,
                latest_by_channel.get(cursor.channel_id, 0),
            )
        normalized = tuple(
            ConversationReadCursor(channel_id, message_id)
            for channel_id, message_id in sorted(latest_by_channel.items())
        )
        now = self._clock.now()
        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            authorization = await self._authorization.get_effective(account_id)
            if not (authorization.allows("chat.read") or authorization.allows("chat.manage")):
                raise ChannelAccessDenied("direct messages are not readable")
            valid_cursors = await repository.list_valid_direct_read_cursors(account_id, normalized)
            if valid_cursors != frozenset(normalized):
                raise MessageNotFound("one or more direct-message cursors are invalid")
            results = await repository.advance_read_cursors(account_id, normalized, now=now)
            await uow.commit()
            return results

    async def mark_direct_conversation_read(
        self,
        account_id: int,
        other_account_id: int,
    ) -> ReadCursorResult | None:
        """Advance a direct-conversation cursor to the peer's latest durable message."""
        _validate_pair(account_id, other_account_id)
        now = self._clock.now()
        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            authorization = await self._authorization.get_effective(account_id)
            if not (authorization.allows("chat.read") or authorization.allows("chat.manage")):
                raise ChannelAccessDenied("direct messages are not readable")
            cursor = await repository.get_direct_conversation_read_cursor(account_id, other_account_id)
            if cursor is None:
                return None
            result = await repository.advance_read_cursor(
                cursor.channel_id,
                account_id,
                cursor.message_id,
                now=now,
            )
            await uow.commit()
            return result

    async def get_global_silence_remaining_seconds(self, account_id: int) -> int:
        """Return the bounded remaining duration of an active global silence."""
        _validate_account_id(account_id)
        now = self._clock.now()
        async with self._uow_factory() as uow:
            silence = await self._silence_policy_factory(uow.session).get_active_silence(
                account_id,
                channel_id=None,
                at=now,
            )
        if silence is None:
            return 0
        remaining = _remaining_seconds(silence.ends_at, now)
        return 2**31 - 1 if remaining is None else min(remaining, 2**31 - 1)

    async def get_channel_member_count(
        self,
        account_id: int,
        channel_id: int,
        *,
        already_authorized: bool = False,
    ) -> int:
        """Return authoritative active membership size for channel join/part updates."""
        _validate_account_id(account_id)
        _validate_account_id(channel_id)
        active_memberships = self._require_active_memberships()
        now = self._clock.now()
        if not already_authorized:
            async with self._uow_factory() as uow:
                repository = self._repository_factory(uow.session)
                channel = await repository.get_channel(channel_id, account_id)
                if channel is None or channel.archived:
                    raise ChannelNotFound("channel does not exist")
                authorization = await self._authorization.get_effective(account_id)
                if not _evaluate_permissions(channel, account_id, authorization).can_read:
                    raise ChannelAccessDenied("channel is not readable")
        count = await active_memberships.count_active_members(channel_id, at=now)
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise RuntimeError("active channel membership query returned an invalid count")
        return count

    async def set_private_message_policy(self, account_id: int, policy: str) -> str:
        """Set whether direct messages are accepted from all users or outgoing follows."""
        started_ns = time.monotonic_ns()
        _validate_account_id(account_id)
        if policy not in {"all", "friends"}:
            raise CommunityInputRejected("private message policy must be all or friends")
        async with self._uow_factory() as uow:
            result = await self._repository_factory(uow.session).set_private_message_policy(
                account_id,
                policy,
                now=self._clock.now(),
            )
            await uow.commit()
            log_event(
                "DEBUG",
                "community.message_policy.changed",
                account_id=account_id,
                policy=result,
                duration_ms=duration_ms(started_ns),
            )
            return result

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
        started_ns = time.monotonic_ns()
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
            authorization = await self._authorization.get_effective(account_id)
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
            result = ChannelMembershipResult(channel_id, account_id, joining, durable, changed)
            if durable and changed:
                log_event(
                    "DEBUG",
                    "community.membership.changed",
                    channel_id=channel_id,
                    account_id=account_id,
                    joined=joining,
                    duration_ms=duration_ms(started_ns),
                )
            return result

    async def _require_not_silenced(
        self,
        session: object,
        account_id: int,
        channel_id: int | None,
        now: datetime,
    ) -> None:
        silence = await self._silence_policy_factory(session).get_active_silence(
            account_id,
            channel_id=channel_id,
            at=now,
        )
        if silence is not None:
            raise AccountSilenced(
                silence.reason,
                account_id=account_id,
                ends_at=silence.ends_at,
                remaining_seconds=_remaining_seconds(silence.ends_at, now),
                channel_id=silence.channel_id,
            )

    async def _require_target_not_silenced(
        self,
        session: object,
        account_id: int,
        now: datetime,
    ) -> None:
        silence = await self._silence_policy_factory(session).get_active_silence(
            account_id,
            channel_id=None,
            at=now,
        )
        if silence is not None:
            raise TargetAccountSilenced(
                silence.reason,
                account_id=account_id,
                ends_at=silence.ends_at,
                remaining_seconds=_remaining_seconds(silence.ends_at, now),
                channel_id=silence.channel_id,
            )

    def _require_active_memberships(self) -> ActiveChannelMembershipQuery:
        if self._active_memberships is None:
            raise ChannelMembershipUnavailable("active channel membership is unavailable")
        return self._active_memberships

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


def _channel_view(channel: ChannelRecord, permissions: ChannelPermissions) -> ChannelView:
    if channel.name is None:
        raise RuntimeError("public channel has no name")
    return ChannelView(
        channel_id=channel.channel_id,
        name=channel.name,
        topic=channel.description or "",
        auto_join=channel.auto_join,
        message_length_limit=channel.message_length_limit,
        can_write=permissions.can_write,
        can_manage=permissions.can_manage,
    )


def _enforce_direct_message_context(
    context: DirectMessageContext,
    sender_account_id: int,
    recipient_account_id: int,
) -> None:
    if context.blocked:
        raise DirectMessageBlocked("direct messages are blocked between these accounts")
    if context.recipient_policy == "all":
        return
    if context.recipient_policy == "friends" and context.follows(recipient_account_id, sender_account_id):
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


def _remaining_seconds(ends_at: datetime | None, now: datetime) -> int | None:
    if ends_at is None:
        return None
    return max(0, ceil((ends_at - now).total_seconds()))


async def _load_public_channel(
    repository: CommunityRepository,
    account_id: int,
    selector: ChannelSelector,
) -> ChannelRecord | None:
    if selector.channel_id is not None:
        channel = await repository.get_channel(selector.channel_id, account_id)
        if channel is None or channel.kind != "public" or channel.archived:
            return None
        return channel
    if selector.name is None:
        raise RuntimeError("normalized channel selector is empty")
    return await repository.get_public_channel_by_name(selector.name, account_id)


def _normalize_channel_selector(selector: ChannelSelector) -> ChannelSelector:
    if not isinstance(selector, ChannelSelector):
        raise CommunityInputRejected("channel_selector must be a ChannelSelector")
    if selector.name is None:
        return selector
    normalized = selector.name.strip().casefold()
    if not 1 <= len(normalized) <= 100:
        raise CommunityInputRejected("invalid channel name")
    return ChannelSelector(name=normalized)


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


def _log_conversation_change(result: DirectConversationResult, *, started_ns: int) -> None:
    log_event(
        "DEBUG",
        "community.conversation.created",
        channel_id=result.channel_id,
        low_account_id=result.low_account_id,
        high_account_id=result.high_account_id,
        duration_ms=duration_ms(started_ns),
    )


def _log_message(message: MessageResult, *, started_ns: int) -> None:
    log_event(
        "DEBUG",
        "community.message.committed",
        message_id=message.message_id,
        channel_id=message.channel_id,
        sender_account_id=message.sender_account_id,
        direct_recipient_account_id=message.direct_recipient_account_id,
        client_message_id=str(message.client_message_id),
        reply_to_id=message.reply_to_id,
        content_length=len(message.content),
        replayed=not message.created,
        duration_ms=duration_ms(started_ns),
    )
