"""Persist social relationships and achievements in caller-owned transactions."""

import uuid
from collections.abc import Mapping
from datetime import datetime
from typing import cast

from sqlalchemy import delete, func, literal, select, union_all
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from perfcho.infra.db.enums import AccountStatus
from perfcho.infra.db.enums import Ruleset as DbRuleset
from perfcho.infra.db.locks import acquire_transaction_lock
from perfcho.infra.db.models.core import Account, AccountName
from perfcho.infra.db.models.social import (
    AchievementDefinition,
    AchievementTranslation,
    AchievementUnlock,
    Block,
    Follow,
)
from perfcho.modules.social.errors import AchievementUnlockConflict
from perfcho.modules.social.models import (
    AccountIdentityView,
    Achievement,
    AchievementDefinitionRecord,
    AchievementEvaluationDefinition,
    AchievementUnlockResult,
    BlockRecord,
    BlockView,
    FollowRecord,
    FollowView,
    PairRelationship,
)
from perfcho.modules.social.models import (
    AchievementUnlock as AchievementUnlockValue,
)


class SqlAlchemySocialRepository:
    """Query and mutate canonical social facts through an AsyncSession."""

    def __init__(self, session: AsyncSession) -> None:
        """Bind all operations to the caller-owned session."""
        self._session = session

    async def acquire_pair_lock(self, first_account_id: int, second_account_id: int) -> None:
        """Acquire the shared ordered account-pair transaction lock."""
        low_account_id, high_account_id = sorted((first_account_id, second_account_id))
        await acquire_transaction_lock(
            self._session,
            "social-community-account-pair",
            low_account_id,
            high_account_id,
        )

    async def existing_account_ids(self, account_ids: tuple[int, ...]) -> frozenset[int]:
        """Return existing account identifiers in one scalar batch query."""
        if not account_ids:
            return frozenset()
        identifiers = await self._session.scalars(select(Account.id).where(Account.id.in_(set(account_ids))))
        return frozenset(identifiers)

    async def resolve_account_by_name(self, name_key: str) -> AccountIdentityView | None:
        """Resolve one active account and its current display name."""
        row = (
            await self._session.execute(
                select(Account.id, AccountName.display_name)
                .join(AccountName, AccountName.account_id == Account.id)
                .where(
                    Account.status == AccountStatus.ACTIVE,
                    AccountName.name_key == name_key,
                    AccountName.ended_at.is_(None),
                )
            )
        ).one_or_none()
        return AccountIdentityView(row.id, row.display_name) if row is not None else None

    async def get_pair_relationship(self, first_account_id: int, second_account_id: int) -> PairRelationship:
        """Load all directional follow and block facts for one pair in one query."""
        low_account_id, high_account_id = sorted((first_account_id, second_account_id))
        pair_filter = ((Follow.actor_account_id == low_account_id) & (Follow.target_account_id == high_account_id)) | (
            (Follow.actor_account_id == high_account_id) & (Follow.target_account_id == low_account_id)
        )
        block_pair_filter = (
            (Block.actor_account_id == low_account_id) & (Block.target_account_id == high_account_id)
        ) | ((Block.actor_account_id == high_account_id) & (Block.target_account_id == low_account_id))
        statement = union_all(
            select(Follow.actor_account_id, Follow.target_account_id, literal("follow").label("kind")).where(
                pair_filter
            ),
            select(Block.actor_account_id, Block.target_account_id, literal("block").label("kind")).where(
                block_pair_filter
            ),
        )
        rows = (await self._session.execute(statement)).all()
        relations = {(row.actor_account_id, row.target_account_id, row.kind) for row in rows}
        return PairRelationship(
            low_account_id=low_account_id,
            high_account_id=high_account_id,
            low_follows_high=(low_account_id, high_account_id, "follow") in relations,
            high_follows_low=(high_account_id, low_account_id, "follow") in relations,
            low_blocks_high=(low_account_id, high_account_id, "block") in relations,
            high_blocks_low=(high_account_id, low_account_id, "block") in relations,
        )

    async def get_follow(self, actor_account_id: int, target_account_id: int) -> FollowRecord | None:
        """Return one outgoing follow as an immutable projection."""
        row = (
            await self._session.execute(
                select(Follow.actor_account_id, Follow.target_account_id, Follow.remark, Follow.created_at).where(
                    Follow.actor_account_id == actor_account_id,
                    Follow.target_account_id == target_account_id,
                )
            )
        ).one_or_none()
        return _follow_record(row)

    async def upsert_follow(
        self,
        actor_account_id: int,
        target_account_id: int,
        *,
        remark: str | None,
        now: datetime,
    ) -> FollowRecord:
        """Create a follow or update its remark while preserving first-follow time."""
        statement = (
            insert(Follow)
            .values(
                actor_account_id=actor_account_id,
                target_account_id=target_account_id,
                remark=remark,
                created_at=now,
            )
            .on_conflict_do_update(
                index_elements=(Follow.actor_account_id, Follow.target_account_id),
                set_={"remark": remark},
            )
            .returning(Follow.actor_account_id, Follow.target_account_id, Follow.remark, Follow.created_at)
        )
        row = (await self._session.execute(statement)).one()
        record = _follow_record(row)
        if record is None:
            raise RuntimeError("database did not return the follow")
        return record

    async def delete_follow(self, actor_account_id: int, target_account_id: int) -> bool:
        """Delete one outgoing follow and report whether a row changed."""
        identifier = await self._session.scalar(
            delete(Follow)
            .where(
                Follow.actor_account_id == actor_account_id,
                Follow.target_account_id == target_account_id,
            )
            .returning(Follow.actor_account_id)
        )
        return identifier is not None

    async def list_friends(self, account_id: int) -> tuple[FollowView, ...]:
        """List outgoing follows, current names, and reciprocal state in one query."""
        reverse_follow = aliased(Follow)
        rows = (
            await self._session.execute(
                select(
                    Follow.target_account_id,
                    AccountName.display_name,
                    Follow.remark,
                    Follow.created_at,
                    reverse_follow.actor_account_id.is_not(None).label("mutual"),
                )
                .join(
                    AccountName,
                    (AccountName.account_id == Follow.target_account_id) & AccountName.ended_at.is_(None),
                )
                .outerjoin(
                    reverse_follow,
                    (reverse_follow.actor_account_id == Follow.target_account_id)
                    & (reverse_follow.target_account_id == account_id),
                )
                .where(Follow.actor_account_id == account_id)
                .order_by(Follow.target_account_id)
            )
        ).all()
        return tuple(
            FollowView(row.target_account_id, row.display_name, row.remark, row.created_at, row.mutual) for row in rows
        )

    async def list_incoming_follower_account_ids(
        self,
        target_account_id: int,
        candidate_actor_account_ids: tuple[int, ...],
    ) -> frozenset[int]:
        """Return candidate actor identifiers with an incoming follow to the target."""
        if not candidate_actor_account_ids:
            return frozenset()
        values = await self._session.scalars(
            select(Follow.actor_account_id).where(
                Follow.target_account_id == target_account_id,
                Follow.actor_account_id.in_(candidate_actor_account_ids),
            )
        )
        return frozenset(values)

    async def get_block(self, actor_account_id: int, target_account_id: int) -> BlockRecord | None:
        """Return one outgoing block as an immutable projection."""
        row = (
            await self._session.execute(
                select(Block.actor_account_id, Block.target_account_id, Block.reason, Block.created_at).where(
                    Block.actor_account_id == actor_account_id,
                    Block.target_account_id == target_account_id,
                )
            )
        ).one_or_none()
        return _block_record(row)

    async def upsert_block(
        self,
        actor_account_id: int,
        target_account_id: int,
        *,
        reason: str | None,
        now: datetime,
    ) -> BlockRecord:
        """Create a block or update its reason while preserving first-block time."""
        statement = (
            insert(Block)
            .values(
                actor_account_id=actor_account_id,
                target_account_id=target_account_id,
                reason=reason,
                created_at=now,
            )
            .on_conflict_do_update(
                index_elements=(Block.actor_account_id, Block.target_account_id),
                set_={"reason": reason},
            )
            .returning(Block.actor_account_id, Block.target_account_id, Block.reason, Block.created_at)
        )
        row = (await self._session.execute(statement)).one()
        record = _block_record(row)
        if record is None:
            raise RuntimeError("database did not return the block")
        return record

    async def delete_block(self, actor_account_id: int, target_account_id: int) -> bool:
        """Delete one outgoing block and report whether a row changed."""
        identifier = await self._session.scalar(
            delete(Block)
            .where(
                Block.actor_account_id == actor_account_id,
                Block.target_account_id == target_account_id,
            )
            .returning(Block.actor_account_id)
        )
        return identifier is not None

    async def delete_pair_follows(self, first_account_id: int, second_account_id: int) -> int:
        """Delete both directional follows for a pair in one statement."""
        low_account_id, high_account_id = sorted((first_account_id, second_account_id))
        rows = (
            await self._session.execute(
                delete(Follow)
                .where(
                    ((Follow.actor_account_id == low_account_id) & (Follow.target_account_id == high_account_id))
                    | ((Follow.actor_account_id == high_account_id) & (Follow.target_account_id == low_account_id))
                )
                .returning(Follow.actor_account_id)
            )
        ).all()
        return len(rows)

    async def list_blocks(self, account_id: int) -> tuple[BlockView, ...]:
        """List outgoing blocks and current names in one query."""
        rows = (
            await self._session.execute(
                select(Block.target_account_id, AccountName.display_name, Block.reason, Block.created_at)
                .join(
                    AccountName,
                    (AccountName.account_id == Block.target_account_id) & AccountName.ended_at.is_(None),
                )
                .where(Block.actor_account_id == account_id)
                .order_by(Block.target_account_id)
            )
        ).all()
        return tuple(BlockView(row.target_account_id, row.display_name, row.reason, row.created_at) for row in rows)

    async def list_blocking_account_ids(
        self,
        target_account_id: int,
        actor_account_ids: tuple[int, ...],
    ) -> frozenset[int]:
        """Return candidate recipients who block the message sender in one query."""
        if not actor_account_ids:
            return frozenset()
        identifiers = await self._session.scalars(
            select(Block.actor_account_id).where(
                Block.target_account_id == target_account_id,
                Block.actor_account_id.in_(actor_account_ids),
            )
        )
        return frozenset(identifiers)

    async def list_achievements(
        self,
        *,
        account_id: int | None,
        locale: str,
        active_only: bool,
    ) -> tuple[Achievement, ...]:
        """List definitions with translation fallback and optional unlocks in one query."""
        requested_translation = aliased(AchievementTranslation)
        fallback_translation = aliased(AchievementTranslation)
        statement = (
            select(
                AchievementDefinition.id,
                AchievementDefinition.slug,
                func.coalesce(requested_translation.name, fallback_translation.name, AchievementDefinition.slug).label(
                    "name"
                ),
                func.coalesce(requested_translation.description, fallback_translation.description, "").label(
                    "description"
                ),
                AchievementDefinition.evaluator_code,
                AchievementDefinition.evaluator_version,
                AchievementDefinition.parameters,
                AchievementDefinition.ruleset,
                AchievementDefinition.icon_asset_id,
                AchievementDefinition.active,
                AchievementUnlock.created_at.label("unlocked_at"),
            )
            .outerjoin(
                requested_translation,
                (requested_translation.achievement_id == AchievementDefinition.id)
                & (requested_translation.locale == locale),
            )
            .outerjoin(
                fallback_translation,
                (fallback_translation.achievement_id == AchievementDefinition.id)
                & (fallback_translation.locale == "en"),
            )
            .outerjoin(
                AchievementUnlock,
                (AchievementUnlock.achievement_id == AchievementDefinition.id)
                & (AchievementUnlock.account_id == account_id),
            )
            .order_by(AchievementDefinition.id)
        )
        if active_only:
            statement = statement.where(AchievementDefinition.active.is_(True))
        rows = (await self._session.execute(statement)).all()
        return tuple(
            Achievement(
                achievement_id=row.id,
                slug=row.slug,
                name=row.name,
                description=row.description,
                evaluator_code=row.evaluator_code,
                evaluator_version=row.evaluator_version,
                parameters=row.parameters,
                ruleset=row.ruleset.value if row.ruleset is not None else None,
                icon_asset_id=row.icon_asset_id,
                active=row.active,
                unlocked_at=row.unlocked_at,
            )
            for row in rows
        )

    async def get_achievement_definition(self, achievement_id: int) -> AchievementDefinitionRecord | None:
        """Return the definition version and lifecycle state for an unlock."""
        row = (
            await self._session.execute(
                select(
                    AchievementDefinition.id,
                    AchievementDefinition.evaluator_version,
                    AchievementDefinition.active,
                ).where(AchievementDefinition.id == achievement_id)
            )
        ).one_or_none()
        if row is None:
            return None
        return AchievementDefinitionRecord(row.id, row.evaluator_version, row.active)

    async def list_score_achievement_definitions(
        self,
        *,
        account_id: int,
        ruleset: str,
    ) -> tuple[AchievementEvaluationDefinition, ...]:
        """Return active, still-locked score definitions with English display text."""
        translation = aliased(AchievementTranslation)
        rows = (
            await self._session.execute(
                select(
                    AchievementDefinition.id,
                    AchievementDefinition.slug,
                    func.coalesce(translation.name, AchievementDefinition.slug).label("name"),
                    func.coalesce(translation.description, "").label("description"),
                    AchievementDefinition.evaluator_code,
                    AchievementDefinition.evaluator_version,
                    AchievementDefinition.parameters,
                    AchievementDefinition.ruleset,
                )
                .outerjoin(
                    translation,
                    (translation.achievement_id == AchievementDefinition.id) & (translation.locale == "en"),
                )
                .outerjoin(
                    AchievementUnlock,
                    (AchievementUnlock.achievement_id == AchievementDefinition.id)
                    & (AchievementUnlock.account_id == account_id),
                )
                .where(
                    AchievementDefinition.active.is_(True),
                    AchievementUnlock.achievement_id.is_(None),
                    (AchievementDefinition.ruleset.is_(None)) | (AchievementDefinition.ruleset == DbRuleset(ruleset)),
                )
                .order_by(AchievementDefinition.id)
            )
        ).all()
        return tuple(
            AchievementEvaluationDefinition(
                achievement_id=row.id,
                slug=row.slug,
                name=row.name,
                description=row.description,
                evaluator_code=row.evaluator_code,
                evaluator_version=row.evaluator_version,
                parameters=row.parameters,
                ruleset=row.ruleset.value if row.ruleset is not None else None,
            )
            for row in rows
        )

    async def unlock_achievement(
        self,
        *,
        account_id: int,
        definition: AchievementDefinitionRecord,
        score_id: int | None,
        source_event_id: uuid.UUID | None,
        snapshot: Mapping[str, object],
        now: datetime,
    ) -> AchievementUnlockResult:
        """Insert a first unlock or return the exact account-definition unlock."""
        statement = (
            insert(AchievementUnlock)
            .values(
                account_id=account_id,
                achievement_id=definition.achievement_id,
                definition_version=definition.evaluator_version,
                score_id=score_id,
                source_event_id=source_event_id,
                snapshot=dict(snapshot),
                created_at=now,
            )
            .on_conflict_do_nothing()
            .returning(
                AchievementUnlock.account_id,
                AchievementUnlock.achievement_id,
                AchievementUnlock.definition_version,
                AchievementUnlock.score_id,
                AchievementUnlock.source_event_id,
                AchievementUnlock.snapshot,
                AchievementUnlock.created_at,
            )
        )
        try:
            row = (await self._session.execute(statement)).one_or_none()
        except IntegrityError as error:
            raise AchievementUnlockConflict("achievement unlock conflicts with existing evidence") from error
        if row is not None:
            return AchievementUnlockResult(_achievement_unlock(row), created=True)

        existing = (
            await self._session.execute(
                select(
                    AchievementUnlock.account_id,
                    AchievementUnlock.achievement_id,
                    AchievementUnlock.definition_version,
                    AchievementUnlock.score_id,
                    AchievementUnlock.source_event_id,
                    AchievementUnlock.snapshot,
                    AchievementUnlock.created_at,
                ).where(
                    AchievementUnlock.account_id == account_id,
                    AchievementUnlock.achievement_id == definition.achievement_id,
                )
            )
        ).one_or_none()
        if existing is None:
            raise AchievementUnlockConflict("source event is already assigned to another achievement unlock")
        return AchievementUnlockResult(_achievement_unlock(existing), created=False)


def _follow_record(row: object | None) -> FollowRecord | None:
    if row is None:
        return None
    return FollowRecord(row.actor_account_id, row.target_account_id, row.remark, row.created_at)  # type: ignore[attr-defined]


def _block_record(row: object | None) -> BlockRecord | None:
    if row is None:
        return None
    return BlockRecord(row.actor_account_id, row.target_account_id, row.reason, row.created_at)  # type: ignore[attr-defined]


def _achievement_unlock(row: object) -> AchievementUnlockValue:
    return AchievementUnlockValue(
        account_id=row.account_id,  # type: ignore[attr-defined]
        achievement_id=row.achievement_id,  # type: ignore[attr-defined]
        definition_version=row.definition_version,  # type: ignore[attr-defined]
        score_id=row.score_id,  # type: ignore[attr-defined]
        source_event_id=row.source_event_id,  # type: ignore[attr-defined]
        snapshot=cast(dict[str, object], row.snapshot),  # type: ignore[attr-defined]
        created_at=row.created_at,  # type: ignore[attr-defined]
    )
