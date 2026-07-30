"""Project account lifecycle events into the user activity stream."""

from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra.db.models.events import OutboxEvent
from perfcho.infra.db.projectors.common import (
    advance_checkpoint,
    payload_datetime,
    payload_integer,
    payload_string,
    project_activity,
    require_accounts,
    require_event_context,
)
from perfcho.infra.outbox import register_consumer

_CONSUMER = "account-projection.v1"


@register_consumer(_CONSUMER, ("account.registered.v1",))
async def project_account_event(session: AsyncSession, event: OutboxEvent, partition_key: str) -> None:
    """Project a new account into its public activity history."""
    account_id = payload_integer(event.payload, "account_id")
    require_event_context(
        event,
        partition_key,
        aggregate_type="account",
        aggregate_id=str(account_id),
        expected_partition_key=f"account:{account_id}",
    )
    await require_accounts(session, (account_id,))
    await project_activity(
        session,
        event,
        subject_account_id=account_id,
        actor_account_id=account_id,
        event_type="account_registered",
        visibility="public",
        occurred_at=payload_datetime(event.payload, "registered_at"),
        snapshot={
            "account_id": account_id,
            "display_name": payload_string(event.payload, "display_name"),
            "status": payload_string(event.payload, "status"),
        },
    )
    await advance_checkpoint(session, event, projector=_CONSUMER, partition_key=partition_key)
