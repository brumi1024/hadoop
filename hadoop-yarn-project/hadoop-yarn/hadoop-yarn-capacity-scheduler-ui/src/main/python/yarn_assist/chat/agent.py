"""YARN Assist Pydantic AI agent construction."""

from __future__ import annotations

import logging
import os

from pydantic_ai import Agent
from pydantic_ai.mcp import MCPToolset

from yarn_assist.config import Settings

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = """You are an AI assistant for the YARN Capacity Scheduler.
You help administrators understand queue configuration, cluster health, resource usage, and applications.

Your capabilities:
- Explain the current queue hierarchy and capacity settings.
- Investigate saturated or underutilized queues.
- Summarize cluster health using metrics and node information.
- Investigate specific YARN applications.
- Propose structured configuration changes for explicit user review.

Guidelines:
- Ground answers in current data from MCP tools before responding.
- Be concise and specific. Avoid padding or generic advice.
- For configuration changes, always use the propose_scheduler_changes tool to produce a typed proposal.
  Never describe changes informally as if they are already applied.
- Never claim that a change has been applied - the user applies changes through the UI.
- In read-only mode, explain why changes cannot be staged.
- When a tool call fails, explain what information is unavailable and continue with what you have.
"""

_FORWARDED_HEADERS = ("x-awc-username", "x-awc-userid", "x-awc-roles", "x-awc-requestid")


def build_model(settings: Settings) -> str:
    """Return the configured model string, raising clearly if not set."""
    model = settings.yarn_assist_model
    if not model:
        raise RuntimeError(
            "No model configured. Set YARN_ASSIST_MODEL to a Pydantic AI model string "
            "(e.g. openai:gpt-4o, anthropic:claude-3-5-sonnet-latest) and the corresponding "
            "provider API key environment variable."
        )
    return model


def build_mcp_toolset(request_headers: dict[str, str], settings: Settings) -> MCPToolset:
    """Build an MCPToolset pointing at the in-process /yarn-mcp/v1 endpoint."""
    forwarded = {k: request_headers[k] for k in _FORWARDED_HEADERS if k in request_headers}
    endpoint = f"http://localhost:{_service_port()}/yarn-mcp/v1"
    return MCPToolset(
        endpoint,
        headers=forwarded,
        tool_error_behavior="error",
        cache_tools=False,
        cache_resources=False,
        cache_prompts=False,
    )


def build_agent(model: str, toolset: MCPToolset) -> Agent:
    return Agent(
        model,
        system_prompt=_SYSTEM_PROMPT,
        toolsets=[toolset],
    )


def _service_port() -> int:
    port_str = os.environ.get("YARN_ASSIST_PORT", "8090")
    try:
        return int(port_str)
    except ValueError:
        return 8090
