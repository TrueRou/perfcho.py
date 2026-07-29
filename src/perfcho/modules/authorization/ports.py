"""Define persistence ports consumed by authorization queries."""

from datetime import datetime
from typing import Protocol, runtime_checkable

from perfcho.modules.authorization.models import EffectiveAuthorization


@runtime_checkable
class AuthorizationRepository(Protocol):
    """Load protocol-neutral effective authorization from authoritative grants."""

    async def get_effective(self, account_id: int, *, at: datetime) -> EffectiveAuthorization:
        """Return grants effective for an account at the supplied instant."""
        ...
