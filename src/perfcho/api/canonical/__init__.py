"""Compose canonical osu! API protocol adapters."""

from fastapi import APIRouter

from .router.oauth import router as oauth_router
from .router.scoring import router as scoring_router

router = APIRouter(tags=["osu! Canonical API"])
router.include_router(oauth_router)
router.include_router(scoring_router, prefix="/api/v2")

__all__ = ("router",)
