"""Define transaction-bound ports consumed by social services."""

import uuid
from collections.abc import Mapping
from datetime import datetime
from typing import Protocol

from perfcho.modules.common.ports import UnitOfWork
from perfcho.modules.social.models import (
    AccountIdentityView,
    Achievement,
    AchievementDefinitionRecord,
    AchievementEvaluationDefinition,
    AchievementUnlockResult,
    AchievementUnlockView,
    BlockRecord,
    BlockView,
    FollowRecord,
    FollowView,
    PairRelationship,
    ScoreAchievementContext,
)


class SocialUnitOfWork(UnitOfWork, Protocol):
    """Expose the transaction resource used to bind social adapters."""

    @property
    def session(self) -> object:
        """Return the active transaction resource."""
        ...


class SocialRepository(Protocol):
    """Persist and query social facts without exposing ORM entities."""

    async def acquire_pair_lock(self, first_account_id: int, second_account_id: int) -> None:
        """Serialize changes for one canonically ordered account pair."""
        ...

    async def existing_account_ids(self, account_ids: tuple[int, ...]) -> frozenset[int]:
        """Return existing account IDs in one batch query."""
        ...

    async def resolve_account_by_name(self, name_key: str) -> AccountIdentityView | None:
        """Resolve an active account by a normalized current name key."""
        ...

    async def get_pair_relationship(self, first_account_id: int, second_account_id: int) -> PairRelationship:
        """Return directional follows and blocks for one pair in one query."""
        ...

    async def get_follow(self, actor_account_id: int, target_account_id: int) -> FollowRecord | None:
        """Return one outgoing follow when it exists."""
        ...

    async def upsert_follow(
        self,
        actor_account_id: int,
        target_account_id: int,
        *,
        remark: str | None,
        now: datetime,
    ) -> FollowRecord:
        """Create or update an outgoing follow."""
        ...

    async def delete_follow(self, actor_account_id: int, target_account_id: int) -> bool:
        """Delete one outgoing follow idempotently."""
        ...

    async def list_friends(self, account_id: int) -> tuple[FollowView, ...]:
        """List outgoing follows and derive mutual friendship in one query."""
        ...

    async def list_follower_account_ids(self, account_id: int) -> frozenset[int]:
        """Return accounts whose presence-friend filter includes this account."""
        ...

    async def get_block(self, actor_account_id: int, target_account_id: int) -> BlockRecord | None:
        """Return one outgoing block when it exists."""
        ...

    async def upsert_block(
        self,
        actor_account_id: int,
        target_account_id: int,
        *,
        reason: str | None,
        now: datetime,
    ) -> BlockRecord:
        """Create or update an outgoing block."""
        ...

    async def delete_block(self, actor_account_id: int, target_account_id: int) -> bool:
        """Delete one outgoing block idempotently."""
        ...

    async def delete_pair_follows(self, first_account_id: int, second_account_id: int) -> int:
        """Delete both directional follows for an account pair."""
        ...

    async def list_blocks(self, account_id: int) -> tuple[BlockView, ...]:
        """List outgoing blocks with current account names in one query."""
        ...

    async def list_blocking_account_ids(
        self,
        target_account_id: int,
        actor_account_ids: tuple[int, ...],
    ) -> frozenset[int]:
        """Return candidate actors who block the target in one batch query."""
        ...

    async def list_achievements(
        self,
        *,
        account_id: int | None,
        locale: str,
        active_only: bool,
    ) -> tuple[Achievement, ...]:
        """List localized definitions and optional unlocks in one query."""
        ...

    async def list_score_achievement_definitions(
        self,
        *,
        account_id: int,
        ruleset: str,
    ) -> tuple[AchievementEvaluationDefinition, ...]:
        """Return active, still-locked definitions applicable to one score."""
        ...

    async def get_achievement_definition(self, achievement_id: int) -> AchievementDefinitionRecord | None:
        """Return the definition facts needed for an unlock."""
        ...

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
        """Insert an achievement unlock idempotently."""
        ...


class SocialRepositoryFactory(Protocol):
    """Bind a social repository to the current transaction resource."""

    def __call__(self, session: object) -> SocialRepository:
        """Return a transaction-bound repository."""
        ...


class AchievementAwarder(Protocol):
    """Award deterministic social achievements inside a caller-owned transaction."""

    async def award_for_score(
        self,
        context: ScoreAchievementContext,
        *,
        at: datetime,
    ) -> tuple[AchievementUnlockView, ...]:
        """Evaluate definitions and return only unlocks created by this invocation."""
        ...


class AchievementAwarderFactory(Protocol):
    """Bind achievement evaluation to the current scoring transaction."""

    def __call__(self, session: object) -> AchievementAwarder:
        """Return an awarder sharing the scoring session."""
        ...
