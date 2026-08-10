"""Compose osu! API v2 protocol adapters."""

from fastapi import APIRouter

from perfcho.api.v2.scoring import router as scoring_router

router = APIRouter(prefix="/api/v2")
router.include_router(scoring_router)
