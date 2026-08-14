"""Adapt osu!lazer notification endpoints onto the community query service.

Notifications are projected durably by the community-message outbox consumer
(``record_notification``) and read back through :class:`CommunityQueryService`.
"""

from typing import Annotated

from fastapi import APIRouter, Body, Query
from fastapi.responses import JSONResponse

from perfcho.api.canonical.dependencies import CanonicalAccountDependency, CanonicalServicesDependency
from perfcho.api.canonical.router._shared import error
from perfcho.modules.community import NotificationView

router = APIRouter()


@router.get("/notifications", response_model=None, tags=["Notifications"])
async def list_notifications(
    services: CanonicalServicesDependency,
    account: CanonicalAccountDependency,
    max_id: Annotated[int | None, Query(gt=0)] = None,
) -> dict[str, object] | JSONResponse:
    """Return the authenticated account's notifications and unread count."""
    if services.community_query is None:
        return error(503, "service_unavailable", "Community service is unavailable.")
    page = await services.community_query.list_notifications(
        account.account_id,
        before_notification_id=max_id,
        limit=50,
    )
    return {
        "has_more": page.has_more,
        "notifications": [_notification(view) for view in page.notifications],
        "unread_count": page.unread_count,
        "notification_endpoint": services.settings.notifications_ws_path,
    }


@router.post("/notifications/mark-read", response_model=None, tags=["Notifications"])
async def mark_read(
    services: CanonicalServicesDependency,
    account: CanonicalAccountDependency,
    body: Annotated[dict[str, object] | None, Body()] = None,
) -> JSONResponse:
    """Mark notifications read by identity."""
    if services.community_query is None:
        return error(503, "service_unavailable", "Community service is unavailable.")
    identities = (body or {}).get("identities", [])
    if not isinstance(identities, list):
        return error(422, "invalid_request", "identities must be a list.")
    notification_ids: list[int] = []
    for identity in identities:
        if not isinstance(identity, dict):
            return error(422, "invalid_request", "notification identities must be objects.")
        raw_id = identity.get("id")
        if raw_id is None:
            continue
        try:
            notification_ids.append(int(raw_id))
        except (TypeError, ValueError):
            return error(422, "invalid_request", "notification identity id must be an integer.")
    await services.community_query.mark_notifications_read(account.account_id, tuple(notification_ids))
    return JSONResponse(status_code=204, content=None)


def _notification(view: NotificationView) -> dict[str, object]:
    return {
        "id": view.notification_id,
        "name": view.kind,
        "created_at": view.created_at.isoformat(),
        "object_type": view.resource_type or "",
        "object_id": view.resource_id or "",
        "source_user_id": view.actor_account_id,
        "is_read": view.read,
        "details": None,
    }
