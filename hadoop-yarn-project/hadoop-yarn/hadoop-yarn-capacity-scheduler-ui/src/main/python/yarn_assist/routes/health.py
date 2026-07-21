"""Health endpoint."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from yarn_assist.config import Settings, get_settings

router = APIRouter()


@router.get("/health")
async def health(settings: Settings = Depends(get_settings)) -> dict:
    return {
        "status": "ok",
        "model_configured": settings.yarn_assist_model is not None,
        "service": "yarn-assist",
        "version": "0.0.1",
    }
