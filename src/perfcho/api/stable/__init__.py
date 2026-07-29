"""Compose unversioned osu! Stable protocol adapters."""

from fastapi import APIRouter

from perfcho.api.stable.router.bancho import router as bancho_router

router = APIRouter()
router.include_router(bancho_router)

__all__ = ("router",)
