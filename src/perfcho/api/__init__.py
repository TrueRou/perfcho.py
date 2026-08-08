"""Compose public HTTP API routers."""

from fastapi import APIRouter

from .cho import router as stable_router

router = APIRouter()
router.include_router(stable_router)
