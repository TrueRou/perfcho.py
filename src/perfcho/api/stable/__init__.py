"""Compose unversioned osu! Stable protocol adapters."""

from fastapi import APIRouter

from perfcho.api.stable.router.cho import router as bancho_router
from perfcho.api.stable.router.web import router as web_router

router = APIRouter(tags=["osu! Stable Protocol"])
router.include_router(web_router)
router.include_router(bancho_router)

__all__ = ("router",)
