"""Define immutable social relationship and achievement values."""

import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType


def _require_positive_id(name: str, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{name} must be a positive integer")


def _require_aware(name: str, value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")


@dataclass(frozen=True, slots=True)
class PairRelationship:
    """Describe all directional follow and block facts for an account pair."""

    low_account_id: int
    high_account_id: int
    low_follows_high: bool
    high_follows_low: bool
    low_blocks_high: bool
    high_blocks_low: bool

    def __post_init__(self) -> None:
        """Require a canonical pair order."""
        _require_positive_id("low_account_id", self.low_account_id)
        _require_positive_id("high_account_id", self.high_account_id)
        if self.low_account_id >= self.high_account_id:
            raise ValueError("account pair must be ordered")

    @property
    def mutual_friends(self) -> bool:
        """Return whether both accounts currently follow each other."""
        return self.low_follows_high and self.high_follows_low

    @property
    def blocked(self) -> bool:
        """Return whether either account currently blocks the other."""
        return self.low_blocks_high or self.high_blocks_low


@dataclass(frozen=True, slots=True)
class FollowView:
    """Describe one outgoing follow using current Stable-facing account identity."""

    account_id: int
    display_name: str
    remark: str | None
    followed_at: datetime
    mutual: bool

    def __post_init__(self) -> None:
        """Validate the returned account and timestamp."""
        _require_positive_id("account_id", self.account_id)
        _require_aware("followed_at", self.followed_at)


@dataclass(frozen=True, slots=True)
class BlockView:
    """Describe one outgoing block using current Stable-facing account identity."""

    account_id: int
    display_name: str
    reason: str | None
    blocked_at: datetime

    def __post_init__(self) -> None:
        """Validate the returned account and timestamp."""
        _require_positive_id("account_id", self.account_id)
        _require_aware("blocked_at", self.blocked_at)


@dataclass(frozen=True, slots=True)
class FollowRecord:
    """Carry one persisted follow without exposing an ORM entity."""

    actor_account_id: int
    target_account_id: int
    remark: str | None
    created_at: datetime

    def __post_init__(self) -> None:
        """Validate the persisted relation."""
        _require_positive_id("actor_account_id", self.actor_account_id)
        _require_positive_id("target_account_id", self.target_account_id)
        _require_aware("created_at", self.created_at)


@dataclass(frozen=True, slots=True)
class BlockRecord:
    """Carry one persisted block without exposing an ORM entity."""

    actor_account_id: int
    target_account_id: int
    reason: str | None
    created_at: datetime

    def __post_init__(self) -> None:
        """Validate the persisted relation."""
        _require_positive_id("actor_account_id", self.actor_account_id)
        _require_positive_id("target_account_id", self.target_account_id)
        _require_aware("created_at", self.created_at)


@dataclass(frozen=True, slots=True)
class BlockResult:
    """Return the block fact and how many conflicting follows were removed."""

    block: BlockRecord
    removed_follow_count: int
    changed: bool

    def __post_init__(self) -> None:
        """Reject invalid deletion counts."""
        if self.removed_follow_count < 0:
            raise ValueError("removed_follow_count must not be negative")


@dataclass(frozen=True, slots=True)
class AchievementDefinitionRecord:
    """Carry the version and lifecycle state needed to unlock an achievement."""

    achievement_id: int
    evaluator_version: int
    active: bool


@dataclass(frozen=True, slots=True)
class Achievement:
    """Describe a localized achievement and an optional account unlock."""

    achievement_id: int
    slug: str
    name: str
    description: str
    evaluator_code: str
    evaluator_version: int
    parameters: Mapping[str, object]
    ruleset: str | None
    icon_asset_id: uuid.UUID | None
    active: bool
    unlocked_at: datetime | None = None

    def __post_init__(self) -> None:
        """Defensively freeze parameters and validate an unlock timestamp."""
        _require_positive_id("achievement_id", self.achievement_id)
        object.__setattr__(self, "parameters", MappingProxyType(dict(self.parameters)))
        if self.unlocked_at is not None:
            _require_aware("unlocked_at", self.unlocked_at)


@dataclass(frozen=True, slots=True)
class AchievementUnlock:
    """Describe the immutable first unlock of an achievement."""

    account_id: int
    achievement_id: int
    definition_version: int
    score_id: int | None
    source_event_id: uuid.UUID | None
    snapshot: Mapping[str, object]
    created_at: datetime

    def __post_init__(self) -> None:
        """Validate identifiers, freeze evidence, and require an aware timestamp."""
        _require_positive_id("account_id", self.account_id)
        _require_positive_id("achievement_id", self.achievement_id)
        if self.score_id is not None:
            _require_positive_id("score_id", self.score_id)
        object.__setattr__(self, "snapshot", MappingProxyType(dict(self.snapshot)))
        _require_aware("created_at", self.created_at)


@dataclass(frozen=True, slots=True)
class AchievementUnlockResult:
    """Return an unlock and whether this invocation created it."""

    unlock: AchievementUnlock
    created: bool
