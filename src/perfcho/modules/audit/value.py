"""Define immutable audit facts without persistence dependencies."""

import uuid
from dataclasses import dataclass, field

from perfcho.modules.common.models import JsonValue


@dataclass(frozen=True, slots=True)
class AuditEventValue:
    """Describe one sensitive operation to be durably recorded."""

    actor_account_id: int | None
    action: str
    target_type: str
    target_id: str
    request_id: uuid.UUID | None
    ip_address: str | None
    reason: str | None = None
    before: dict[str, JsonValue] | None = None
    after: dict[str, JsonValue] | None = None
    metadata: dict[str, JsonValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Reject malformed audit identity fields before persistence."""
        if self.actor_account_id is not None and (
            isinstance(self.actor_account_id, bool) or self.actor_account_id <= 0
        ):
            raise ValueError("actor_account_id must be positive or None")
        if not self.action or not self.target_type or not self.target_id:
            raise ValueError("audit action and target identity must not be empty")


AuditEvent = AuditEventValue
