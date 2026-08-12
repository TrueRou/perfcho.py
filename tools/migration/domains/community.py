"""Migrate bancho.py public channels and private mail conversations."""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import case, func, select, text, tuple_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra.db.enums import ChannelKind
from perfcho.infra.db.models.authz import Permission
from perfcho.infra.db.models.community import Channel, ChannelUserState, DirectConversation, Message
from tools.migration.domains.common import run_batched_phase, run_single_phase
from tools.migration.models import DiagnosticSeverity, MigrationRuntime, SourceRow
from tools.migration.transforms import aware_datetime, unix_datetime

_NORMAL_READ_BITS = (1 << 0) | (1 << 1)
_CANONICAL_CHANNELS = {
    "osu": "osu",
    "announce": "announce",
    "help": "help",
    "lobby": "lobby",
}
_SLUG_SEPARATOR = re.compile(r"[\s_]+")
_SLUG_UNSAFE = re.compile(r"[^\w-]", re.UNICODE)


@dataclass(frozen=True, slots=True)
class _PreparedChannel:
    source_id: int
    slug: str
    name: str
    description: str | None
    read_mask: int
    write_mask: int
    auto_join: bool


@dataclass(frozen=True, slots=True)
class _PreparedMail:
    source_id: int
    sender_id: int
    recipient_id: int
    low_account_id: int
    high_account_id: int
    content: str
    sent_at: datetime
    read: bool


async def migrate_community(runtime: MigrationRuntime) -> None:
    """Migrate canonical public channels and deterministic direct-message history."""
    await _reconstruct_channel_mappings(runtime)

    async def sequence_handler(session: AsyncSession) -> None:
        await session.execute(
            text(
                "SELECT setval(pg_get_serial_sequence('community.channel', 'id'), "
                "GREATEST(1, (SELECT COALESCE(MAX(id), 0) FROM community.channel)), true)"
            )
        )

    await run_single_phase(runtime, phase="community.channel_sequence", handler=sequence_handler)

    async def channels_handler(session: AsyncSession, rows: list[SourceRow]) -> None:
        await _migrate_channel_batch(session, runtime, rows)

    await run_batched_phase(
        runtime,
        phase="community.channels",
        table="channels",
        key="id",
        handler=channels_handler,
    )

    async def mail_handler(session: AsyncSession, rows: list[SourceRow]) -> None:
        await _migrate_mail_batch(session, runtime, rows)

    await run_batched_phase(
        runtime,
        phase="community.mail",
        table="mail",
        key="id",
        handler=mail_handler,
    )


async def _reconstruct_channel_mappings(runtime: MigrationRuntime) -> None:
    for rows in runtime.source.iter_batches(
        "channels",
        key="id",
        batch_size=runtime.config.batch_size,
        columns=("id", "name", "topic", "read_priv", "write_priv", "auto_join"),
    ):
        prepared = [_prepare_channel(runtime, row, diagnose=False) for row in rows]
        valid = [channel for channel in prepared if channel is not None]
        if not valid:
            continue
        async with runtime.session_factory() as session:
            existing: dict[str | None, int] = dict(
                (
                    await session.execute(
                        select(Channel.slug, Channel.id).where(Channel.slug.in_({channel.slug for channel in valid}))
                    )
                ).all()
            )
        for channel in valid:
            target_id = existing.get(channel.slug)
            if target_id is not None:
                runtime.mappings.channels[channel.source_id] = target_id


async def _migrate_channel_batch(
    session: AsyncSession,
    runtime: MigrationRuntime,
    rows: list[SourceRow],
) -> None:
    permission_ids: dict[str, int] = dict(
        (
            await session.execute(
                select(Permission.code, Permission.id).where(
                    Permission.code.in_(("chat.read", "chat.write", "chat.announce", "chat.manage", "admin.access"))
                )
            )
        ).all()
    )
    if set(permission_ids) != {"chat.read", "chat.write", "chat.announce", "chat.manage", "admin.access"}:
        raise RuntimeError("bootstrap channel permission catalog is incomplete")

    for row in rows:
        channel = _prepare_channel(runtime, row, diagnose=True)
        if channel is None:
            continue
        read_code = _permission_code(runtime, channel, channel.read_mask, operation="read")
        write_code = _permission_code(runtime, channel, channel.write_mask, operation="write")
        statement = (
            insert(Channel)
            .values(
                kind=ChannelKind.PUBLIC,
                slug=channel.slug,
                name=channel.name,
                description=channel.description,
                read_permission_id=permission_ids[read_code] if read_code is not None else None,
                write_permission_id=permission_ids[write_code] if write_code is not None else None,
                manage_permission_id=permission_ids["chat.manage"],
                auto_join=channel.auto_join,
                message_length_limit=2000,
                created_at=runtime.started_at,
                updated_at=runtime.started_at,
            )
            .on_conflict_do_update(
                index_elements=(Channel.slug,),
                set_={"slug": Channel.slug},
            )
            .returning(Channel.id)
        )
        target_id = await session.scalar(statement)
        if target_id is None:
            runtime.report.increment("community.channels", "skipped")
            runtime.report.add(
                DiagnosticSeverity.ERROR,
                "channel_upsert_failed",
                "PostgreSQL did not return the resolved channel ID",
                entity="channels",
                source_id=channel.source_id,
            )
            continue
        previous = runtime.mappings.channels.get(channel.source_id)
        runtime.mappings.channels[channel.source_id] = target_id
        runtime.report.increment("community.channels", "resolved" if previous is not None else "imported")


def _prepare_channel(
    runtime: MigrationRuntime,
    row: SourceRow,
    *,
    diagnose: bool,
) -> _PreparedChannel | None:
    try:
        source_id = _positive_int(row.get("id"), "channel id")
        raw_name = row.get("name")
        if not isinstance(raw_name, str):
            raise ValueError("channel name must be text")
        name = unicodedata.normalize("NFKC", raw_name).strip()
        if not 1 <= len(name) <= 100:
            raise ValueError("channel name must contain between 1 and 100 characters")
        slug = _channel_slug(name)
        topic = row.get("topic")
        if topic is not None and not isinstance(topic, str):
            raise ValueError("channel topic must be text or null")
        if isinstance(topic, str) and len(topic) > 255:
            raise ValueError("channel topic exceeds 255 characters")
        read_mask = _privilege_mask(row.get("read_priv"), "read privilege")
        write_mask = _privilege_mask(row.get("write_priv"), "write privilege")
        auto_join = _legacy_bool(row.get("auto_join"), "auto_join")
        return _PreparedChannel(source_id, slug, name, topic or None, read_mask, write_mask, auto_join)
    except ValueError as error:
        if diagnose:
            runtime.report.increment("community.channels", "skipped")
            runtime.report.add(
                DiagnosticSeverity.WARNING,
                "channel_malformed",
                str(error),
                entity="channels",
                source_id=row.get("id"),
            )
        return None


def _permission_code(
    runtime: MigrationRuntime,
    channel: _PreparedChannel,
    mask: int,
    *,
    operation: str,
) -> str | None:
    if mask == 0:
        return None
    if channel.slug == "announce" and operation == "write":
        return "chat.announce"
    if mask & _NORMAL_READ_BITS:
        return "chat.read" if operation == "read" else "chat.write"
    runtime.report.add(
        DiagnosticSeverity.WARNING,
        "channel_permission_narrowed",
        "legacy privilege mask has no exact canonical permission and was narrowed to administrators",
        entity="channels",
        source_id=channel.source_id,
        details={"operation": operation, "legacy_mask": mask},
    )
    return "admin.access"


async def _migrate_mail_batch(
    session: AsyncSession,
    runtime: MigrationRuntime,
    rows: list[SourceRow],
) -> None:
    prepared: list[_PreparedMail] = []
    for row in rows:
        try:
            source_id = _positive_int(row.get("id"), "mail id")
            if source_id > 0xFFFF_FFFF:
                raise ValueError("mail id exceeds the deterministic import range")
            source_sender = _positive_int(row.get("from_id"), "mail sender")
            source_recipient = _positive_int(row.get("to_id"), "mail recipient")
            sender = runtime.mappings.accounts[source_sender]
            recipient = runtime.mappings.accounts[source_recipient]
            if sender == recipient:
                raise ValueError("self-addressed mail cannot form a direct conversation")
            content = row.get("msg")
            if not isinstance(content, str) or not 1 <= len(content) <= 10_000:
                raise ValueError("mail content must contain between 1 and 10000 characters")
            low, high = sorted((sender, recipient))
            prepared.append(
                _PreparedMail(
                    source_id,
                    sender,
                    recipient,
                    low,
                    high,
                    content,
                    _source_timestamp(runtime, row.get("time")),
                    _legacy_bool(row.get("read"), "mail read flag"),
                )
            )
        except (KeyError, ValueError) as error:
            runtime.report.increment("community.mail", "skipped")
            runtime.report.add(
                DiagnosticSeverity.WARNING,
                "mail_malformed",
                str(error),
                entity="mail",
                source_id=row.get("id"),
            )
    if not prepared:
        return

    pairs = {(mail.low_account_id, mail.high_account_id) for mail in prepared}
    conversations = {
        (low, high): channel_id
        for low, high, channel_id in (
            await session.execute(
                select(
                    DirectConversation.low_account_id,
                    DirectConversation.high_account_id,
                    DirectConversation.channel_id,
                ).where(tuple_(DirectConversation.low_account_id, DirectConversation.high_account_id).in_(pairs))
            )
        ).all()
    }
    first_mail_by_pair: dict[tuple[int, int], _PreparedMail] = {}
    for mail in prepared:
        first_mail_by_pair.setdefault((mail.low_account_id, mail.high_account_id), mail)
    for pair, first_mail in first_mail_by_pair.items():
        if pair in conversations:
            continue
        channel_id = _negative_id(runtime, "direct-channel", f"{pair[0]}:{pair[1]}")
        resolved = await session.execute(
            text(
                """
                INSERT INTO community.channel
                    (id, kind, slug, name, description, owner_account_id, team_id,
                     read_permission_id, write_permission_id, manage_permission_id,
                     auto_join, message_length_limit, archived_at, created_at, updated_at)
                OVERRIDING SYSTEM VALUE
                VALUES
                    (:id, :kind, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                     false, 10000, NULL, :created_at, :created_at)
                ON CONFLICT (id) DO NOTHING
                RETURNING id, kind
                """
            ),
            {"id": channel_id, "kind": ChannelKind.PRIVATE.value, "created_at": first_mail.sent_at},
        )
        channel_row = resolved.one_or_none()
        if channel_row is None:
            channel_row = (
                await session.execute(select(Channel.id, Channel.kind).where(Channel.id == channel_id))
            ).one_or_none()
        if channel_row is None:
            raise RuntimeError("direct channel merge did not resolve an identity")
        if channel_row.kind != ChannelKind.PRIVATE.value:
            runtime.report.add(
                DiagnosticSeverity.ERROR,
                "direct_channel_id_collision",
                "deterministic direct channel ID belongs to a non-private target channel",
                entity="mail",
                source_id=first_mail.source_id,
            )
            continue
        occupied_pair = (
            await session.execute(
                select(DirectConversation.low_account_id, DirectConversation.high_account_id).where(
                    DirectConversation.channel_id == channel_id
                )
            )
        ).one_or_none()
        if occupied_pair is not None and (occupied_pair.low_account_id, occupied_pair.high_account_id) != pair:
            runtime.report.add(
                DiagnosticSeverity.ERROR,
                "direct_channel_id_collision",
                "deterministic direct channel ID belongs to another target conversation",
                entity="mail",
                source_id=first_mail.source_id,
            )
            continue
        await session.execute(
            insert(DirectConversation)
            .values(channel_id=channel_id, low_account_id=pair[0], high_account_id=pair[1])
            .on_conflict_do_nothing()
        )
        persisted_channel = await session.scalar(
            select(DirectConversation.channel_id).where(
                DirectConversation.low_account_id == pair[0], DirectConversation.high_account_id == pair[1]
            )
        )
        if persisted_channel is None:
            runtime.report.add(
                DiagnosticSeverity.ERROR,
                "direct_conversation_upsert_failed",
                "target data prevented resolution of a direct conversation",
                entity="mail",
                source_id=first_mail.source_id,
            )
            continue
        conversations[pair] = persisted_channel

    message_statement = text(
        """
        INSERT INTO community.message
            (id, channel_id, sender_account_id, client_message_id, reply_to_id,
             content, is_action, edited_at, deleted_at, created_at)
        OVERRIDING SYSTEM VALUE
        VALUES
            (:id, :channel_id, :sender_account_id, :client_message_id, NULL,
             :content, false, NULL, NULL, :created_at)
        ON CONFLICT DO NOTHING
        RETURNING id, channel_id
        """
    )
    states: dict[tuple[int, int], int | None] = {}
    imported = 0
    for mail in prepared:
        conversation_channel_id = conversations.get((mail.low_account_id, mail.high_account_id))
        if conversation_channel_id is None:
            runtime.report.increment("community.mail", "skipped")
            continue
        message_id = _mail_message_id(runtime, mail.source_id)
        client_message_id = runtime.ids.make("mail-client-message", mail.source_id)
        result = await session.execute(
            message_statement,
            {
                "id": message_id,
                "channel_id": conversation_channel_id,
                "sender_account_id": mail.sender_id,
                "client_message_id": client_message_id,
                "content": mail.content,
                "created_at": mail.sent_at,
            },
        )
        persisted = result.one_or_none()
        if persisted is None:
            persisted = (
                await session.execute(
                    select(Message.id, Message.channel_id).where(
                        Message.sender_account_id == mail.sender_id,
                        Message.client_message_id == client_message_id,
                    )
                )
            ).one_or_none()
        if persisted is None or persisted.channel_id != conversation_channel_id:
            runtime.report.add(
                DiagnosticSeverity.ERROR,
                "mail_message_conflict",
                "target data prevented deterministic resolution of the imported message",
                entity="mail",
                source_id=mail.source_id,
            )
            runtime.report.increment("community.mail", "skipped")
            continue
        _advance_state(states, conversation_channel_id, mail.sender_id, persisted.id)
        states.setdefault((conversation_channel_id, mail.recipient_id), None)
        if mail.read:
            _advance_state(states, conversation_channel_id, mail.recipient_id, persisted.id)
        imported += 1

    if states:
        state_statement = insert(ChannelUserState).values(
            [
                {
                    "channel_id": channel_id,
                    "account_id": account_id,
                    "last_read_message_id": cursor,
                    "hidden": False,
                    "notification_level": "all",
                    "created_at": runtime.started_at,
                    "updated_at": runtime.started_at,
                }
                for (channel_id, account_id), cursor in states.items()
            ]
        )
        await session.execute(
            state_statement.on_conflict_do_update(
                index_elements=(ChannelUserState.channel_id, ChannelUserState.account_id),
                set_={
                    "last_read_message_id": case(
                        (
                            ChannelUserState.last_read_message_id.is_(None),
                            state_statement.excluded.last_read_message_id,
                        ),
                        (
                            state_statement.excluded.last_read_message_id.is_(None),
                            ChannelUserState.last_read_message_id,
                        ),
                        else_=func.greatest(
                            ChannelUserState.last_read_message_id,
                            state_statement.excluded.last_read_message_id,
                        ),
                    ),
                    "updated_at": ChannelUserState.updated_at,
                },
            )
        )
    runtime.report.increment("community.mail", "imported", imported)


def _channel_slug(name: str) -> str:
    candidate = name.casefold().removeprefix("#")
    candidate = _SLUG_SEPARATOR.sub("-", candidate)
    candidate = _SLUG_UNSAFE.sub("", candidate).strip("-")
    candidate = _CANONICAL_CHANNELS.get(candidate, candidate)
    if not candidate or len(candidate) > 100:
        raise ValueError("channel name does not produce a valid slug")
    return candidate


def _privilege_mask(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a nonnegative integer")
    return value


def _legacy_bool(value: object, name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in {0, 1}:
        return bool(value)
    raise ValueError(f"{name} must be zero or one")


def _positive_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _source_timestamp(runtime: MigrationRuntime, value: object) -> datetime:
    if isinstance(value, datetime):
        return aware_datetime(value, runtime.config.source_timezone, fallback=runtime.started_at)
    return unix_datetime(value, fallback=runtime.started_at)


def _negative_id(runtime: MigrationRuntime, entity: str, source_id: object) -> int:
    return -((runtime.ids.make(entity, source_id).int % ((1 << 63) - 1)) + 1)


def _mail_message_id(runtime: MigrationRuntime, source_id: int) -> int:
    namespace = runtime.ids.make("mail-message-range", "mail").int & ((1 << 29) - 1)
    return -(1 << 62) + (namespace << 32) + source_id


def _advance_state(states: dict[tuple[int, int], int | None], channel_id: int, account_id: int, cursor: int) -> None:
    key = (channel_id, account_id)
    current = states.get(key)
    states[key] = cursor if current is None else max(current, cursor)
