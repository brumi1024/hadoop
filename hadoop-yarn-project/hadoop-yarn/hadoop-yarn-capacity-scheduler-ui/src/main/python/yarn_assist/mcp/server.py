"""FastMCP server factory with YARN read-only tools."""

from __future__ import annotations

import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from yarn_assist.config import Settings

logger = logging.getLogger(__name__)


def create_mcp_app(settings: Settings) -> FastAPI:
    from fastmcp import FastMCP

    from yarn_assist.mcp.tools import register_tools
    from yarn_assist.yarn.adapter import YarnAdapter

    adapter = YarnAdapter(
        base_url=settings.yarn_assist_rm_base_url,
        timeout=settings.yarn_assist_request_timeout_seconds,
    )
    mcp = FastMCP(name="yarn-mcp-v1")
    register_tools(mcp, adapter, max_chars=settings.yarn_assist_max_response_chars)
    mcp_app = mcp.http_app(path="/v1", stateless_http=True)

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncGenerator[None, None]:
        async with mcp_app.router.lifespan_context(_app):
            yield
        await adapter.aclose()

    app = FastAPI(lifespan=lifespan)
    app.mount("/v1", mcp_app)
    return app
