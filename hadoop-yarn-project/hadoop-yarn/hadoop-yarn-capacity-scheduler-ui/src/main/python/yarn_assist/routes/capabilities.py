"""Capabilities endpoint - reports feature flags to the UI."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from yarn_assist.auth import get_current_user
from yarn_assist.config import Settings, get_settings

router = APIRouter()


@router.get("/capabilities")
async def capabilities(
    settings: Settings = Depends(get_settings),
    _username: str = Depends(get_current_user),
) -> dict:
    return {
        "chat_enabled": True,
        "mcp_read_enabled": True,
        "staging_proposals_enabled": True,
        "write_enabled": False,
        "model_configured": settings.yarn_assist_model is not None,
    }
