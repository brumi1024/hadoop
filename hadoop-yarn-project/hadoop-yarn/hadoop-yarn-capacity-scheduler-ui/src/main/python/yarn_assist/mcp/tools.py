"""YARN MCP tool registrations - all read-only for this PoC."""

from __future__ import annotations

import json
import logging
from typing import Annotated, Any

from mcp.types import ToolAnnotations

from yarn_assist.safety.bounds import truncate_text
from yarn_assist.yarn.adapter import YarnAdapter
from yarn_assist.yarn.errors import YarnAdapterError

logger = logging.getLogger(__name__)

_READ_ONLY_ANNOTATIONS = ToolAnnotations(
    readOnlyHint=True,
    destructiveHint=False,
    idempotentHint=True,
    openWorldHint=False,
)


def _safe_result(data: Any, max_chars: int = 64_000) -> str:
    raw = json.dumps(data, default=str)
    bounded = truncate_text(raw, max_chars)
    if bounded.truncated:
        logger.info("MCP tool response truncated: %d -> %d chars", bounded.original_chars, bounded.returned_chars)
    return bounded.text


def _adapter_error(exc: YarnAdapterError) -> str:
    return json.dumps(exc.as_dict())


def register_tools(mcp: Any, adapter: YarnAdapter, max_chars: int = 64_000) -> None:
    """Register all YARN read-only tools on the FastMCP server."""

    @mcp.tool(annotations=_READ_ONLY_ANNOTATIONS)
    async def get_cluster_info() -> str:
        """Return YARN cluster identity, state, HA status, and ResourceManager version."""
        try:
            return _safe_result(await adapter.get_cluster_info(), max_chars)
        except YarnAdapterError as exc:
            return _adapter_error(exc)

    @mcp.tool(annotations=_READ_ONLY_ANNOTATIONS)
    async def get_cluster_metrics() -> str:
        """Return aggregate cluster resource metrics: memory, vCores, container counts, app counts."""
        try:
            return _safe_result(await adapter.get_cluster_metrics(), max_chars)
        except YarnAdapterError as exc:
            return _adapter_error(exc)

    @mcp.tool(annotations=_READ_ONLY_ANNOTATIONS)
    async def get_scheduler() -> str:
        """Return the current Capacity Scheduler state including all queue capacities and usage."""
        try:
            return _safe_result(await adapter.get_scheduler(), max_chars)
        except YarnAdapterError as exc:
            return _adapter_error(exc)

    @mcp.tool(annotations=_READ_ONLY_ANNOTATIONS)
    async def get_scheduler_configuration() -> str:
        """Return the mutable Capacity Scheduler configuration (allowlisted properties only, secrets redacted)."""
        try:
            return _safe_result(await adapter.get_scheduler_configuration(), max_chars)
        except YarnAdapterError as exc:
            return _adapter_error(exc)

    @mcp.tool(annotations=_READ_ONLY_ANNOTATIONS)
    async def list_nodes(
        state: Annotated[str | None, "Node state filter: NEW, RUNNING, UNHEALTHY, DECOMMISSIONED, LOST, REBOOTED"] = None,
    ) -> str:
        """List cluster nodes with resource capacities, states, and labels."""
        try:
            return _safe_result(await adapter.list_nodes(state=state), max_chars)
        except YarnAdapterError as exc:
            return _adapter_error(exc)

    @mcp.tool(annotations=_READ_ONLY_ANNOTATIONS)
    async def list_node_labels() -> str:
        """List all node labels defined in the cluster."""
        try:
            return _safe_result(await adapter.get_node_labels(), max_chars)
        except YarnAdapterError as exc:
            return _adapter_error(exc)

    @mcp.tool(annotations=_READ_ONLY_ANNOTATIONS)
    async def list_applications(
        state: Annotated[str | None, "Application state: NEW, SUBMITTED, ACCEPTED, RUNNING, FINISHED, FAILED, KILLED"] = None,
        queue: Annotated[str | None, "Filter by queue path, e.g. root.analytics"] = None,
        user: Annotated[str | None, "Filter by submitting user"] = None,
        app_type: Annotated[str | None, "Filter by application type, e.g. MAPREDUCE, SPARK"] = None,
        limit: Annotated[int, "Max number of applications to return (max 200)"] = 50,
    ) -> str:
        """List YARN applications with resource usage, state, queue, and progress."""
        try:
            return _safe_result(
                await adapter.list_applications(state=state, queue=queue, user=user, app_type=app_type, limit=limit),
                max_chars,
            )
        except YarnAdapterError as exc:
            return _adapter_error(exc)

    @mcp.tool(annotations=_READ_ONLY_ANNOTATIONS)
    async def get_application(
        application_id: Annotated[str, "YARN application ID, e.g. application_1234567890_0001"],
    ) -> str:
        """Get detailed information for a single YARN application."""
        try:
            return _safe_result(await adapter.get_application(application_id), max_chars)
        except YarnAdapterError as exc:
            return _adapter_error(exc)

    @mcp.tool(
        annotations=ToolAnnotations(
            readOnlyHint=True,
            destructiveHint=False,
            idempotentHint=False,
            openWorldHint=False,
        )
    )
    async def propose_scheduler_changes(
        summary: Annotated[str, "One-line summary of the proposed change"],
        rationale: Annotated[str, "Explanation of why this change is beneficial"],
        changes: Annotated[list[dict], "List of typed change objects (see schema)"],
        warnings: Annotated[list[str] | None, "Optional warnings about the proposal"] = None,
    ) -> str:
        """Produce a typed scheduler change proposal for user review.

        This tool does NOT modify YARN or browser state.
        The proposal must be explicitly staged by the user through the UI.

        Each change in `changes` must be one of:
        - {"kind": "queue_property", "queue_path": "root.x", "property": "...", "new_value": "...", "old_value": "...", "reason": "..."}
        - {"kind": "global_property", "property": "...", "new_value": "...", "old_value": "...", "reason": "..."}
        - {"kind": "queue_addition", "parent_path": "root.x", "queue_name": "y", "config": {...}, "reason": "..."}
        - {"kind": "queue_removal", "queue_path": "root.x", "reason": "..."}
        - {"kind": "label_queue_property", "queue_path": "root.x", "label": "gpu", "property": "...", "new_value": "...", "old_value": "...", "reason": "..."}
        """
        import uuid
        proposal = {
            "proposal_id": uuid.uuid4().hex[:16],
            "summary": summary,
            "rationale": rationale,
            "changes": changes or [],
            "warnings": warnings or [],
        }
        return _safe_result(proposal, max_chars)
