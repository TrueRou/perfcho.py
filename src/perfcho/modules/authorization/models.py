"""Define immutable effective authorization values."""

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class EffectiveAuthorization:
    """Describe the canonical roles, permissions, and entitlements active for an account."""

    account_id: int
    evaluated_at: datetime
    permission_codes: frozenset[str]
    role_codes: frozenset[str]
    entitlement_codes: frozenset[str]

    def __post_init__(self) -> None:
        """Defensively freeze code collections and require an authoritative instant."""
        if self.evaluated_at.tzinfo is None or self.evaluated_at.utcoffset() is None:
            raise ValueError("evaluated_at must be timezone-aware")
        object.__setattr__(self, "permission_codes", frozenset(self.permission_codes))
        object.__setattr__(self, "role_codes", frozenset(self.role_codes))
        object.__setattr__(self, "entitlement_codes", frozenset(self.entitlement_codes))

    def allows(self, permission_code: str) -> bool:
        """Return whether a permission remains effective after explicit denies."""
        return permission_code in self.permission_codes
