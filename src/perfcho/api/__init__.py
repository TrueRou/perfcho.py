"""Compose public HTTP API routers."""

from fastapi import APIRouter

from .cho import router as stable_router
from .oauth import router as oauth_router
from .v2 import router as v2_router

router = APIRouter()
router.include_router(stable_router)
router.include_router(oauth_router)
router.include_router(v2_router)
