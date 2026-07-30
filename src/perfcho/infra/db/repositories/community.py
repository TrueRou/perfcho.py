"""Persist durable channels and messages in caller-owned transactions."""

import uuid
from datetime import datetime

from sqlalchemy import and_, case, func, literal, or_, select, tuple_, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased
from sqlalchemy.sql.selectable import Exists, Select

from perfcho.infra.db.advisory_lock import acquire_transaction_lock
from perfcho.infra.db.enums import ChannelKind, SanctionKind
from perfcho.infra.db.models.authz import Permission
from perfcho.infra.db.models.community import (
    Channel,
    ChannelMembership,
    ChannelUserState,
    DirectConversation,
    Message,
)
from perfcho.infra.db.models.core import Account, AccountName, UserPreference
from perfcho.infra.db.models.moderation import Sanction
from perfcho.infra.db.models.social import Block, Follow, TeamMembership
from perfcho.modules.community.models import (
    ActiveSilence,
    ChannelRecord,
    ConversationReadCursor,
    DirectConversationResult,
    DirectMessageContext,
    MessageResult,
    OfflineDirectMessage,
    ReadCursorResult,
)


class SqlAlchemyCommunityRepository:
    """Query and mutate canonical community facts through an AsyncSession."""

    def __init__(self, session: AsyncSession) -> None:
        """Bind all operations to the caller-owned session."""
        self._session = session

    async def acquire_pair_lock(self, first_account_id: int, second_account_id: int) -> None:
        """Acquire the account-pair lock shared with social relation changes."""
        low_account_id, high_account_id = sorted((first_account_id, second_account_id))
        await acquire_transaction_lock(
            self._session,
            "social-community-account-pair",
            low_account_id,
            high_account_id,
        )

    async def list_public_channels(self, account_id: int) -> tuple[ChannelRecord, ...]:
        """List active public channels and permission codes in one query."""
        rows = (
            await self._session.execute(
                _channel_statement(account_id)
                .where(Channel.kind == ChannelKind.PUBLIC, Channel.archived_at.is_(None))
                .order_by(Channel.id)
            )
        ).all()
        return tuple(_channel_record(row) for row in rows)

    async def get_public_channel_by_stable_name(self, stable_name: str, account_id: int) -> ChannelRecord | None:
        """Resolve one active public channel case-insensitively by Stable name."""
        row = (
            await self._session.execute(
                _channel_statement(account_id).where(
                    Channel.kind == ChannelKind.PUBLIC,
                    Channel.archived_at.is_(None),
                    func.lower(Channel.name) == stable_name,
                )
            )
        ).one_or_none()
        return _channel_record(row) if row is not None else None

    async def get_channel(self, channel_id: int, account_id: int) -> ChannelRecord | None:
        """Load one channel and all account-specific scope relationships in one query."""
        row = (
            await self._session.execute(_channel_statement(account_id).where(Channel.id == channel_id))
        ).one_or_none()
        return _channel_record(row) if row is not None else None

    async def get_direct_message_context(
        self,
        sender_account_id: int,
        recipient_account_id: int,
    ) -> DirectMessageContext:
        """Batch-load pair account, PM policy, follow, and block facts in one query."""
        low_account_id, high_account_id = sorted((sender_account_id, recipient_account_id))
        account_count = (
            select(func.count(Account.id)).where(Account.id.in_((low_account_id, high_account_id))).scalar_subquery()
        )
        recipient_policy = (
            select(UserPreference.private_message_policy)
            .where(UserPreference.account_id == recipient_account_id)
            .scalar_subquery()
        )

        def follows(actor_account_id: int, target_account_id: int) -> Exists:
            return (
                select(literal(True))
                .where(
                    Follow.actor_account_id == actor_account_id,
                    Follow.target_account_id == target_account_id,
                )
                .exists()
            )

        def blocks(actor_account_id: int, target_account_id: int) -> Exists:
            return (
                select(literal(True))
                .where(
                    Block.actor_account_id == actor_account_id,
                    Block.target_account_id == target_account_id,
                )
                .exists()
            )

        row = (
            await self._session.execute(
                select(
                    account_count.label("account_count"),
                    recipient_policy.label("recipient_policy"),
                    follows(low_account_id, high_account_id).label("low_follows_high"),
                    follows(high_account_id, low_account_id).label("high_follows_low"),
                    blocks(low_account_id, high_account_id).label("low_blocks_high"),
                    blocks(high_account_id, low_account_id).label("high_blocks_low"),
                )
            )
        ).one()
        existing_account_ids = frozenset((low_account_id, high_account_id)) if row.account_count == 2 else frozenset()
        return DirectMessageContext(
            existing_account_ids=existing_account_ids,
            recipient_policy=row.recipient_policy,
            low_account_id=low_account_id,
            high_account_id=high_account_id,
            low_follows_high=row.low_follows_high,
            high_follows_low=row.high_follows_low,
            low_blocks_high=row.low_blocks_high,
            high_blocks_low=row.high_blocks_low,
        )

    async def get_or_create_direct_conversation(
        self,
        low_account_id: int,
        high_account_id: int,
        *,
        now: datetime,
    ) -> DirectConversationResult:
        """Return or create the unique direct-conversation channel under its pair lock."""
        row = (
            await self._session.execute(
                select(
                    DirectConversation.channel_id,
                    DirectConversation.low_account_id,
                    DirectConversation.high_account_id,
                    Channel.message_length_limit,
                )
                .join(Channel, Channel.id == DirectConversation.channel_id)
                .where(
                    DirectConversation.low_account_id == low_account_id,
                    DirectConversation.high_account_id == high_account_id,
                )
            )
        ).one_or_none()
        if row is not None:
            return DirectConversationResult(
                row.channel_id,
                row.low_account_id,
                row.high_account_id,
                row.message_length_limit,
                created=False,
            )

        channel = Channel(
            kind=ChannelKind.PRIVATE,
            auto_join=False,
            message_length_limit=2000,
            created_at=now,
            updated_at=now,
        )
        self._session.add(channel)
        await self._session.flush()
        if channel.id is None:
            raise RuntimeError("database did not assign a channel identifier")
        self._session.add(
            DirectConversation(
                channel_id=channel.id,
                low_account_id=low_account_id,
                high_account_id=high_account_id,
            )
        )
        await self._session.flush()
        return DirectConversationResult(
            channel.id,
            low_account_id,
            high_account_id,
            channel.message_length_limit,
            created=True,
        )

    async def set_private_message_policy(self, account_id: int, policy: str, *, now: datetime) -> str:
        """Update one existing preference row without creating protocol defaults."""
        result = await self._session.scalar(
            update(UserPreference)
            .where(UserPreference.account_id == account_id)
            .values(private_message_policy=policy, updated_at=now)
            .returning(UserPreference.private_message_policy)
        )
        if result is None:
            raise RuntimeError("account user preference is unavailable")
        return result

    async def get_message_by_client_id(
        self,
        sender_account_id: int,
        client_message_id: uuid.UUID,
    ) -> MessageResult | None:
        """Resolve a sender's globally idempotent client UUID with any DM recipient."""
        row = (
            await self._session.execute(
                _message_statement().where(
                    Message.sender_account_id == sender_account_id,
                    Message.client_message_id == client_message_id,
                )
            )
        ).one_or_none()
        return _message_result(row, created=False) if row is not None else None

    async def insert_message(
        self,
        *,
        channel_id: int,
        sender_account_id: int,
        client_message_id: uuid.UUID,
        content: str,
        is_action: bool,
        reply_to_id: int | None,
        now: datetime,
    ) -> MessageResult:
        """Insert a message or return the sender-UUID row won by a concurrent request."""
        row = (
            await self._session.execute(
                insert(Message)
                .values(
                    channel_id=channel_id,
                    sender_account_id=sender_account_id,
                    client_message_id=client_message_id,
                    content=content,
                    is_action=is_action,
                    reply_to_id=reply_to_id,
                    created_at=now,
                )
                .on_conflict_do_nothing(
                    index_elements=(Message.sender_account_id, Message.client_message_id),
                )
                .returning(
                    Message.id,
                    Message.channel_id,
                    Message.sender_account_id,
                    Message.client_message_id,
                    Message.content,
                    Message.is_action,
                    Message.reply_to_id,
                    Message.created_at,
                )
            )
        ).one_or_none()
        if row is not None:
            return MessageResult(
                message_id=row.id,
                channel_id=row.channel_id,
                sender_account_id=row.sender_account_id,
                client_message_id=row.client_message_id,
                content=row.content,
                is_action=row.is_action,
                reply_to_id=row.reply_to_id,
                created_at=row.created_at,
                created=True,
            )
        existing = await self.get_message_by_client_id(sender_account_id, client_message_id)
        if existing is None:
            raise RuntimeError("message idempotency conflict did not expose the existing message")
        return existing

    async def message_belongs_to_channel(self, channel_id: int, message_id: int) -> bool:
        """Check channel-message membership using a scalar projection."""
        identifier = await self._session.scalar(
            select(Message.id).where(Message.id == message_id, Message.channel_id == channel_id).limit(1)
        )
        return identifier is not None

    async def advance_read_cursor(
        self,
        channel_id: int,
        account_id: int,
        message_id: int,
        *,
        now: datetime,
    ) -> ReadCursorResult:
        """Insert or monotonically advance a per-user channel read cursor."""
        statement = (
            insert(ChannelUserState)
            .values(
                channel_id=channel_id,
                account_id=account_id,
                last_read_message_id=message_id,
                created_at=now,
                updated_at=now,
            )
            .on_conflict_do_update(
                index_elements=(ChannelUserState.channel_id, ChannelUserState.account_id),
                set_={"last_read_message_id": message_id, "updated_at": now},
                where=or_(
                    ChannelUserState.last_read_message_id.is_(None),
                    ChannelUserState.last_read_message_id < message_id,
                ),
            )
            .returning(ChannelUserState.last_read_message_id)
        )
        advanced_message_id = await self._session.scalar(statement)
        if advanced_message_id is not None:
            return ReadCursorResult(channel_id, account_id, advanced_message_id, advanced=True)
        current_message_id = await self._session.scalar(
            select(ChannelUserState.last_read_message_id).where(
                ChannelUserState.channel_id == channel_id,
                ChannelUserState.account_id == account_id,
            )
        )
        if current_message_id is None:
            raise RuntimeError("read cursor disappeared during monotonic update")
        return ReadCursorResult(channel_id, account_id, current_message_id, advanced=False)

    async def list_unread_direct_messages(
        self,
        account_id: int,
        *,
        after_message_id: int | None,
        limit: int,
    ) -> tuple[OfflineDirectMessage, ...]:
        """List unread incoming direct messages in ascending order with sender names."""
        statement = (
            select(
                Message.id,
                Message.channel_id,
                Message.sender_account_id,
                AccountName.display_name,
                Message.client_message_id,
                Message.content,
                Message.is_action,
                Message.created_at,
            )
            .join(DirectConversation, DirectConversation.channel_id == Message.channel_id)
            .join(
                AccountName,
                (AccountName.account_id == Message.sender_account_id) & AccountName.ended_at.is_(None),
            )
            .outerjoin(
                ChannelUserState,
                (ChannelUserState.channel_id == Message.channel_id) & (ChannelUserState.account_id == account_id),
            )
            .where(
                or_(
                    DirectConversation.low_account_id == account_id,
                    DirectConversation.high_account_id == account_id,
                ),
                Message.sender_account_id != account_id,
                Message.deleted_at.is_(None),
                Message.id > func.coalesce(ChannelUserState.last_read_message_id, 0),
            )
            .order_by(Message.id)
            .limit(limit)
        )
        if after_message_id is not None:
            statement = statement.where(Message.id > after_message_id)
        rows = (await self._session.execute(statement)).all()
        return tuple(
            OfflineDirectMessage(
                message_id=row.id,
                channel_id=row.channel_id,
                sender_account_id=row.sender_account_id,
                sender_name=row.display_name,
                client_message_id=row.client_message_id,
                content=row.content,
                is_action=row.is_action,
                created_at=row.created_at,
            )
            for row in rows
        )

    async def get_direct_conversation_read_cursor(
        self,
        account_id: int,
        other_account_id: int,
    ) -> ConversationReadCursor | None:
        """Return the latest undeleted message sent by one direct-conversation peer."""
        low_account_id, high_account_id = sorted((account_id, other_account_id))
        row = (
            await self._session.execute(
                select(DirectConversation.channel_id, func.max(Message.id).label("message_id"))
                .join(Message, Message.channel_id == DirectConversation.channel_id)
                .where(
                    DirectConversation.low_account_id == low_account_id,
                    DirectConversation.high_account_id == high_account_id,
                    Message.sender_account_id == other_account_id,
                    Message.deleted_at.is_(None),
                )
                .group_by(DirectConversation.channel_id)
            )
        ).one_or_none()
        if row is None:
            return None
        return ConversationReadCursor(row.channel_id, row.message_id)

    async def list_valid_direct_read_cursors(
        self,
        account_id: int,
        cursors: tuple[ConversationReadCursor, ...],
    ) -> frozenset[ConversationReadCursor]:
        """Validate direct-conversation ownership and message positions in one query."""
        if not cursors:
            return frozenset()
        positions = tuple((cursor.channel_id, cursor.message_id) for cursor in cursors)
        rows = (
            await self._session.execute(
                select(Message.channel_id, Message.id)
                .join(DirectConversation, DirectConversation.channel_id == Message.channel_id)
                .where(
                    tuple_(Message.channel_id, Message.id).in_(positions),
                    or_(
                        DirectConversation.low_account_id == account_id,
                        DirectConversation.high_account_id == account_id,
                    ),
                    Message.deleted_at.is_(None),
                )
            )
        ).all()
        return frozenset(ConversationReadCursor(row.channel_id, row.id) for row in rows)

    async def advance_read_cursors(
        self,
        account_id: int,
        cursors: tuple[ConversationReadCursor, ...],
        *,
        now: datetime,
    ) -> tuple[ReadCursorResult, ...]:
        """Advance one cursor per direct conversation with a batch upsert."""
        if not cursors:
            return ()
        insert_statement = insert(ChannelUserState).values(
            [
                {
                    "channel_id": cursor.channel_id,
                    "account_id": account_id,
                    "last_read_message_id": cursor.message_id,
                    "created_at": now,
                    "updated_at": now,
                }
                for cursor in cursors
            ]
        )
        advanced_rows = (
            await self._session.execute(
                insert_statement.on_conflict_do_update(
                    index_elements=(ChannelUserState.channel_id, ChannelUserState.account_id),
                    set_={
                        "last_read_message_id": insert_statement.excluded.last_read_message_id,
                        "updated_at": now,
                    },
                    where=or_(
                        ChannelUserState.last_read_message_id.is_(None),
                        ChannelUserState.last_read_message_id < insert_statement.excluded.last_read_message_id,
                    ),
                ).returning(ChannelUserState.channel_id, ChannelUserState.last_read_message_id)
            )
        ).all()
        advanced_channel_ids = frozenset(row.channel_id for row in advanced_rows)
        current_rows = (
            await self._session.execute(
                select(ChannelUserState.channel_id, ChannelUserState.last_read_message_id).where(
                    ChannelUserState.account_id == account_id,
                    ChannelUserState.channel_id.in_(tuple(cursor.channel_id for cursor in cursors)),
                )
            )
        ).all()
        current_by_channel = {row.channel_id: row.last_read_message_id for row in current_rows}
        if len(current_by_channel) != len(cursors) or any(value is None for value in current_by_channel.values()):
            raise RuntimeError("one or more direct-message read cursors disappeared during batch update")
        return tuple(
            ReadCursorResult(
                cursor.channel_id,
                account_id,
                current_by_channel[cursor.channel_id],
                cursor.channel_id in advanced_channel_ids,
            )
            for cursor in cursors
        )

    async def join_membership(self, channel_id: int, account_id: int, *, now: datetime) -> bool:
        """Create a new active membership history row unless one is already current."""
        identifier = await self._session.scalar(
            insert(ChannelMembership)
            .values(channel_id=channel_id, account_id=account_id, role="member", created_at=now)
            .on_conflict_do_nothing(
                index_elements=(ChannelMembership.channel_id, ChannelMembership.account_id),
                index_where=ChannelMembership.left_at.is_(None),
            )
            .returning(ChannelMembership.id)
        )
        return identifier is not None

    async def leave_membership(self, channel_id: int, account_id: int, *, now: datetime) -> bool:
        """Close the current membership history row when one exists."""
        identifier = await self._session.scalar(
            update(ChannelMembership)
            .where(
                ChannelMembership.channel_id == channel_id,
                ChannelMembership.account_id == account_id,
                ChannelMembership.left_at.is_(None),
            )
            .values(left_at=now)
            .returning(ChannelMembership.id)
        )
        return identifier is not None


class SqlAlchemyActiveSilencePolicy:
    """Evaluate active global and channel message sanctions from authoritative facts."""

    def __init__(self, session: AsyncSession) -> None:
        """Bind policy queries to the caller-owned transaction."""
        self._session = session

    async def get_active_silence(
        self,
        account_id: int,
        *,
        channel_id: int | None,
        at: datetime,
    ) -> ActiveSilence | None:
        """Return the strongest active global or matching channel silence."""
        scope = (
            and_(Sanction.kind == SanctionKind.SILENCE, Sanction.channel_id.is_(None))
            if channel_id is None
            else or_(
                and_(Sanction.kind == SanctionKind.SILENCE, Sanction.channel_id.is_(None)),
                and_(Sanction.kind == SanctionKind.CHANNEL_MUTE, Sanction.channel_id == channel_id),
            )
        )
        row = (
            await self._session.execute(
                select(Sanction.subject_account_id, Sanction.reason, Sanction.ends_at, Sanction.channel_id)
                .where(
                    Sanction.subject_account_id == account_id,
                    scope,
                    Sanction.revoked_at.is_(None),
                    Sanction.starts_at <= at,
                    or_(Sanction.ends_at.is_(None), Sanction.ends_at > at),
                )
                .order_by(Sanction.channel_id.asc().nulls_first(), Sanction.ends_at.desc().nulls_first())
                .limit(1)
            )
        ).one_or_none()
        if row is None:
            return None
        return ActiveSilence(row.subject_account_id, row.reason, row.ends_at, row.channel_id)


def _channel_statement(account_id: int) -> Select:
    read_permission = aliased(Permission)
    write_permission = aliased(Permission)
    manage_permission = aliased(Permission)
    active_membership = (
        select(literal(True))
        .where(
            ChannelMembership.channel_id == Channel.id,
            ChannelMembership.account_id == account_id,
            ChannelMembership.left_at.is_(None),
        )
        .exists()
    )
    active_team_membership = (
        select(literal(True))
        .where(
            TeamMembership.team_id == Channel.team_id,
            TeamMembership.account_id == account_id,
            TeamMembership.left_at.is_(None),
        )
        .exists()
    )
    return (
        select(
            Channel.id,
            Channel.kind,
            Channel.name,
            Channel.description,
            Channel.owner_account_id,
            Channel.team_id,
            read_permission.code.label("read_permission_code"),
            write_permission.code.label("write_permission_code"),
            manage_permission.code.label("manage_permission_code"),
            Channel.auto_join,
            Channel.message_length_limit,
            Channel.archived_at,
            DirectConversation.low_account_id,
            DirectConversation.high_account_id,
            active_membership.label("active_member"),
            active_team_membership.label("active_team_member"),
        )
        .outerjoin(read_permission, read_permission.id == Channel.read_permission_id)
        .outerjoin(write_permission, write_permission.id == Channel.write_permission_id)
        .outerjoin(manage_permission, manage_permission.id == Channel.manage_permission_id)
        .outerjoin(DirectConversation, DirectConversation.channel_id == Channel.id)
    )


def _channel_record(row: object) -> ChannelRecord:
    return ChannelRecord(
        channel_id=row.id,  # type: ignore[attr-defined]
        kind=row.kind.value,  # type: ignore[attr-defined]
        stable_name=row.name,  # type: ignore[attr-defined]
        description=row.description,  # type: ignore[attr-defined]
        owner_account_id=row.owner_account_id,  # type: ignore[attr-defined]
        team_id=row.team_id,  # type: ignore[attr-defined]
        read_permission_code=row.read_permission_code,  # type: ignore[attr-defined]
        write_permission_code=row.write_permission_code,  # type: ignore[attr-defined]
        manage_permission_code=row.manage_permission_code,  # type: ignore[attr-defined]
        auto_join=row.auto_join,  # type: ignore[attr-defined]
        message_length_limit=row.message_length_limit,  # type: ignore[attr-defined]
        archived=row.archived_at is not None,  # type: ignore[attr-defined]
        direct_low_account_id=row.low_account_id,  # type: ignore[attr-defined]
        direct_high_account_id=row.high_account_id,  # type: ignore[attr-defined]
        active_member=row.active_member,  # type: ignore[attr-defined]
        active_team_member=row.active_team_member,  # type: ignore[attr-defined]
    )


def _message_statement() -> Select:
    direct_recipient = case(
        (DirectConversation.low_account_id == Message.sender_account_id, DirectConversation.high_account_id),
        (DirectConversation.high_account_id == Message.sender_account_id, DirectConversation.low_account_id),
        else_=None,
    ).label("direct_recipient_account_id")
    return select(
        Message.id,
        Message.channel_id,
        Message.sender_account_id,
        Message.client_message_id,
        Message.content,
        Message.is_action,
        Message.reply_to_id,
        Message.created_at,
        direct_recipient,
    ).outerjoin(DirectConversation, DirectConversation.channel_id == Message.channel_id)


def _message_result(row: object, *, created: bool) -> MessageResult:
    return MessageResult(
        message_id=row.id,  # type: ignore[attr-defined]
        channel_id=row.channel_id,  # type: ignore[attr-defined]
        sender_account_id=row.sender_account_id,  # type: ignore[attr-defined]
        client_message_id=row.client_message_id,  # type: ignore[attr-defined]
        content=row.content,  # type: ignore[attr-defined]
        is_action=row.is_action,  # type: ignore[attr-defined]
        reply_to_id=row.reply_to_id,  # type: ignore[attr-defined]
        created_at=row.created_at,  # type: ignore[attr-defined]
        direct_recipient_account_id=row.direct_recipient_account_id,  # type: ignore[attr-defined]
        created=created,
    )
