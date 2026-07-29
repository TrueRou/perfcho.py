"""Define protocol-neutral social and achievement failures."""

from perfcho.modules.common.errors import AuthorizationDenied, InputRejected, ResourceConflict, ResourceNotFound


class SocialRelationRejected(InputRejected):
    """Reject a malformed or self-referential social relation."""

    code = "social_relation_rejected"


class SocialAccountNotFound(ResourceNotFound):
    """Indicate that an account in a social operation does not exist."""

    code = "social_account_not_found"


class SocialInteractionBlocked(AuthorizationDenied):
    """Reject a follow while either account blocks the other."""

    code = "social_interaction_blocked"


class AchievementNotFound(ResourceNotFound):
    """Indicate that an achievement definition is unavailable."""

    code = "achievement_not_found"


class AchievementUnlockConflict(ResourceConflict):
    """Indicate reuse of an achievement source event for another unlock."""

    code = "achievement_unlock_conflict"
