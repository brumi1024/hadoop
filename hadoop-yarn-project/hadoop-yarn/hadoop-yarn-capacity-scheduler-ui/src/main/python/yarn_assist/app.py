"""Application factory for the YARN Assist service."""

from __future__ import annotations

import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from yarn_assist.config import Settings

logger = logging.getLogger(__name__)


def create_app(settings: Settings | None = None) -> FastAPI:
    from yarn_assist.config import get_settings
    from yarn_assist.routes.capabilities import router as capabilities_router
    from yarn_assist.routes.chat import router as chat_router
    from yarn_assist.routes.conversations import router as conversations_router
    from yarn_assist.routes.health import router as health_router

    resolved = settings or get_settings()

    app = FastAPI(
        title="YARN Capacity Scheduler AI Assist",
        description="PoC standalone service - not for production use.",
        version="0.0.1",
        docs_url="/yarn-assist/docs",
        redoc_url=None,
    )

    # Override the settings dependency with the resolved instance so tests
    # and callers that pass an explicit settings object get consistent behavior.
    app.dependency_overrides[get_settings] = lambda: resolved

    # SECURITY: CORS is only enabled when YARN_ASSIST_ALLOW_DEV_CORS=true.
    # The trusted-identity headers must NOT appear in allow_headers because a
    # CORS preflight from any origin could then set them directly.
    # In production, CORS must be handled by the fronting proxy, not the service.
    if resolved.yarn_assist_allow_dev_cors:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["http://localhost:5173"],
            allow_credentials=True,
            allow_methods=["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
            # Trusted-identity headers are deliberately excluded; they must
            # be injected by the reverse proxy, never by browser JavaScript.
            allow_headers=["Content-Type", "Accept", "Authorization"],
        )

    prefix = "/yarn-assist/api/v1"
    app.include_router(health_router, prefix=prefix)
    app.include_router(capabilities_router, prefix=prefix)
    app.include_router(conversations_router, prefix=prefix)
    app.include_router(chat_router, prefix=prefix)

    # SECURITY: The MCP endpoint (/yarn-mcp) is mounted here for PoC
    # convenience. It does not enforce get_current_user because FastMCP's
    # stateless HTTP transport does not natively support FastAPI Depends.
    # PRODUCTION GAP: Before exposing this to a network, wrap the mounted
    # app in authentication middleware or restrict it to loopback-only access
    # so that Pydantic AI can reach it in-process without network exposure.
    try:
        from yarn_assist.mcp.server import create_mcp_app

        mcp_app = create_mcp_app(resolved)
        app.mount("/yarn-mcp", mcp_app)
    except ImportError:
        logger.warning("MCP server not yet available - /yarn-mcp routes are disabled")

    return app
