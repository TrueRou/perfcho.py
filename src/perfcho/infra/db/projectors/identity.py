"""Project identity session events into private account activity."""

from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra.db.models.events import OutboxEvent
from perfcho.infra.db.projectors.common import (
    advance_checkpoint,
    payload_boolean,
    payload_datetime,
    payload_integer,
    payload_optional_string,
    payload_string,
    payload_uuid,
    project_activity,
    require_accounts,
    require_event_context,
)

CONSUMER_NAME = "identity-projector.v1"
EVENT_TYPES = frozenset({"identity.session-opened.v1", "identity.session-closed.v1"})


async def project_identity_event(session: AsyncSession, event: OutboxEvent, partition_key: str) -> None:
    """Project one opened or closed authentication session without changing Presence."""
    account_id = payload_integer(event.payload, "account_id")
    session_id = payload_uuid(event.payload, "session_id")
    require_event_context(
        event,
        partition_key,
        aggregate_type="identity_session",
        aggregate_id=str(session_id),
        expected_partition_key=f"account:{account_id}",
    )
    await require_accounts(session, (account_id,))
    if event.event_type == "identity.session-opened.v1":
        occurred_at = payload_datetime(event.payload, "opened_at")
        activity_type = "identity_session_opened"
        snapshot: dict[str, object] = {
            "session_id": str(session_id),
            "device_id": str(payload_uuid(event.payload, "device_id")),
            "client_family": payload_string(event.payload, "client_family"),
            "client_version": payload_string(event.payload, "client_version"),
            "client_variant": payload_optional_string(event.payload, "client_variant"),
            "expires_at": payload_datetime(event.payload, "expires_at").isoformat(),
        }
    else:
        occurred_at = payload_datetime(event.payload, "closed_at")
        activity_type = "identity_session_closed"
        snapshot = {
            "session_id": str(session_id),
            "reason": payload_string(event.payload, "reason"),
            "revoked": payload_boolean(event.payload, "revoked"),
        }
    await project_activity(
        session,
        event,
        subject_account_id=account_id,
        actor_account_id=account_id,
        event_type=activity_type,
        visibility="private",
        occurred_at=occurred_at,
        snapshot=snapshot,
    )
    await advance_checkpoint(session, event, projector=CONSUMER_NAME, partition_key=partition_key)
