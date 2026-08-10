"""Compose public HTTP API routers."""

from fastapi import APIRouter

from .cho import router as stable_router
from .oauth import router as oauth_router

router = APIRouter()
router.include_router(stable_router)
router.include_router(oauth_router)
