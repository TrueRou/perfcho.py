"""Expose protocol-neutral social and achievement operations."""

from perfcho.modules.social.errors import (
    AchievementNotFound,
    AchievementUnlockConflict,
    SocialAccountNotFound,
    SocialInteractionBlocked,
    SocialRelationRejected,
)
from perfcho.modules.social.models import (
    Achievement,
    AchievementUnlock,
    AchievementUnlockResult,
    BlockResult,
    BlockView,
    FollowView,
    PairRelationship,
)
from perfcho.modules.social.ports import SocialRepository
from perfcho.modules.social.services import SocialService

__all__ = (
    "Achievement",
    "AchievementNotFound",
    "AchievementUnlock",
    "AchievementUnlockConflict",
    "AchievementUnlockResult",
    "BlockResult",
    "BlockView",
    "FollowView",
    "PairRelationship",
    "SocialAccountNotFound",
    "SocialInteractionBlocked",
    "SocialRelationRejected",
    "SocialRepository",
    "SocialService",
)
