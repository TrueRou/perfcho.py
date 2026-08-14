"""Adapt osu!lazer notification endpoints.

Notifications are delivered transiently through the realtime event bus and
projected durably by the notification outbox. This adapter exposes the HTTP
read/mark-read surface; durable history projection is wired in a later phase.
"""

from typing import Annotated

from fastapi import APIRouter, Body
from fastapi.responses import JSONResponse

from perfcho.api.canonical.dependencies import CanonicalAccountDependency, CanonicalServicesDependency

router = APIRouter()


@router.get("/notifications", response_model=None, tags=["Notifications"])
async def list_notifications(
    services: CanonicalServicesDependency,
    account: CanonicalAccountDependency,
) -> dict[str, object]:
    """Return the authenticated account's notifications."""
    del services, account
    return {
        "notifications": [],
        "unread_count": 0,
        "notification_endpoint": "/signalr/metadata",
    }


@router.post("/notifications/mark-read", response_model=None, tags=["Notifications"])
async def mark_read(
    services: CanonicalServicesDependency,
    account: CanonicalAccountDependency,
    body: Annotated[dict[str, object] | None, Body()] = None,
) -> JSONResponse:
    """Mark notifications as read (durable projection lands in a later phase)."""
    del services, account, body
    return JSONResponse(status_code=204, content=None)
