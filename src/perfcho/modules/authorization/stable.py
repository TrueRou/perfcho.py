"""Project canonical authorization into Stable client privilege bits."""

from enum import IntFlag

from perfcho.modules.authorization.models import EffectiveAuthorization

_PLAYER_PERMISSION = "account.login"
_MODERATOR_PERMISSION = "moderation.enforce"
_DEVELOPER_PERMISSION = "admin.access"
_OWNER_ROLE = "administrator"
_SUPPORTER_ENTITLEMENT = "supporter"


class StablePrivilege(IntFlag):
    """Represent privilege bits understood by the Stable client."""

    NONE = 0
    PLAYER = 1
    MODERATOR = 2
    SUPPORTER = 4
    OWNER = 8
    DEVELOPER = 16


def project_stable_privileges(authorization: EffectiveAuthorization) -> StablePrivilege:
    """Map canonical effective codes to Stable's presentation-only bit field."""
    privileges = StablePrivilege.NONE
    if authorization.allows(_PLAYER_PERMISSION):
        privileges |= StablePrivilege.PLAYER
    if _SUPPORTER_ENTITLEMENT in authorization.entitlement_codes:
        privileges |= StablePrivilege.SUPPORTER
    if authorization.allows(_MODERATOR_PERMISSION):
        privileges |= StablePrivilege.MODERATOR
    if authorization.allows(_DEVELOPER_PERMISSION):
        privileges |= StablePrivilege.DEVELOPER
    if _OWNER_ROLE in authorization.role_codes:
        privileges |= StablePrivilege.OWNER
    return privileges
