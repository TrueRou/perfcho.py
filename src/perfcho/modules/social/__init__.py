"""Expose protocol-neutral social and achievement operations."""

from perfcho.modules.social.commands import build_clan_commands
from perfcho.modules.social.errors import (
    AchievementNotFound,
    AchievementUnlockConflict,
    SocialAccountNotFound,
    SocialInteractionBlocked,
    SocialRelationRejected,
)
from perfcho.modules.social.models import (
    AccountIdentityView,
    Achievement,
    AchievementEvaluationDefinition,
    AchievementUnlock,
    AchievementUnlockResult,
    AchievementUnlockView,
    BlockResult,
    BlockView,
    FollowView,
    PairRelationship,
    ScoreAchievementContext,
)
from perfcho.modules.social.ports import AchievementAwarder, SocialRepository
from perfcho.modules.social.queries import SocialQueryService
from perfcho.modules.social.services import SocialService, TransactionAchievementAwarder

__all__ = (
    "Achievement",
    "AchievementAwarder",
    "AchievementEvaluationDefinition",
    "AccountIdentityView",
    "AchievementNotFound",
    "AchievementUnlock",
    "AchievementUnlockConflict",
    "AchievementUnlockResult",
    "AchievementUnlockView",
    "BlockResult",
    "BlockView",
    "build_clan_commands",
    "FollowView",
    "PairRelationship",
    "SocialAccountNotFound",
    "SocialInteractionBlocked",
    "SocialRelationRejected",
    "SocialRepository",
    "SocialService",
    "SocialQueryService",
    "ScoreAchievementContext",
    "TransactionAchievementAwarder",
)
