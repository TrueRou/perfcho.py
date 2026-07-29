"""Compose public HTTP API routers."""

from fastapi import APIRouter

from .stable import router as stable_router
from .v1.router import router as v1_router

router = APIRouter()
router.include_router(stable_router)
router.include_router(v1_router)
