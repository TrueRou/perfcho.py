"""Compose public HTTP API routers."""

from fastapi import APIRouter

from .canonical import router as canonical_router
from .notifications import router as notifications_router
from .stable import router as stable_router

router = APIRouter()
router.include_router(stable_router)
router.include_router(canonical_router)
router.include_router(notifications_router)
