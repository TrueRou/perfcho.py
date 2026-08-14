"""Compose canonical osu! API protocol adapters."""

from fastapi import APIRouter

from .router.beatmaps import router as beatmaps_router
from .router.beatmapsets import router as beatmapsets_router
from .router.chat import router as chat_router
from .router.comments import router as comments_router
from .router.misc import router as misc_router
from .router.notifications import router as notifications_router
from .router.oauth import router as oauth_router
from .router.rankings import router as rankings_router
from .router.relationship import router as relationship_router
from .router.rooms import router as rooms_router
from .router.scoring import router as scoring_router
from .router.users import router as users_router

router = APIRouter(tags=["osu! Canonical API"])
router.include_router(oauth_router)
router.include_router(scoring_router, prefix="/api/v2")
router.include_router(beatmapsets_router, prefix="/api/v2")
router.include_router(users_router, prefix="/api/v2")
router.include_router(rankings_router, prefix="/api/v2")
router.include_router(relationship_router, prefix="/api/v2")
router.include_router(beatmaps_router, prefix="/api/v2")
router.include_router(chat_router, prefix="/api/v2")
router.include_router(comments_router, prefix="/api/v2")
router.include_router(rooms_router, prefix="/api/v2")
router.include_router(notifications_router, prefix="/api/v2")
router.include_router(misc_router, prefix="/api/v2")

__all__ = ("router",)
