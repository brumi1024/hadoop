"""P2 tests: YarnAdapter, MCP tools, and absence of write tools."""

from __future__ import annotations

import json

import httpx
import pytest

from yarn_assist.yarn.adapter import YarnAdapter, _filter_conf
from yarn_assist.yarn.errors import YarnAdapterError

# ---------------------------------------------------------------------------
# MockTransport helpers
# ---------------------------------------------------------------------------

def make_fixed_transport(body: dict | list, status: int = 200) -> httpx.MockTransport:
    encoded = json.dumps(body).encode()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, content=encoded, headers={"content-type": "application/json"})

    return httpx.MockTransport(handler=handler)


def make_error_transport() -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("simulated connection failure")

    return httpx.MockTransport(handler=handler)


def make_status_transport(status: int, body: dict | None = None) -> httpx.MockTransport:
    encoded = json.dumps(body or {"message": "error"}).encode()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, content=encoded, headers={"content-type": "application/json"})

    return httpx.MockTransport(handler=handler)


# ---------------------------------------------------------------------------
# Adapter happy-path tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_cluster_info():
    payload = {"clusterInfo": {"id": 1, "state": "STARTED"}}
    adapter = YarnAdapter(transport=make_fixed_transport(payload))
    result = await adapter.get_cluster_info()
    assert result == payload


@pytest.mark.asyncio
async def test_get_cluster_metrics():
    payload = {"clusterMetrics": {"totalMB": 65536, "totalVirtualCores": 32}}
    adapter = YarnAdapter(transport=make_fixed_transport(payload))
    result = await adapter.get_cluster_metrics()
    assert result == payload


@pytest.mark.asyncio
async def test_get_scheduler():
    payload = {"scheduler": {"schedulerInfo": {"type": "capacityScheduler"}}}
    adapter = YarnAdapter(transport=make_fixed_transport(payload))
    result = await adapter.get_scheduler()
    assert result["scheduler"]["schedulerInfo"]["type"] == "capacityScheduler"


@pytest.mark.asyncio
async def test_list_nodes_default():
    payload = {"nodes": {"node": [{"id": "node1"}, {"id": "node2"}]}}
    adapter = YarnAdapter(transport=make_fixed_transport(payload))
    result = await adapter.list_nodes()
    assert len(result["nodes"]["node"]) == 2


@pytest.mark.asyncio
async def test_list_nodes_truncation():
    nodes = [{"id": f"node{i}"} for i in range(600)]
    payload = {"nodes": {"node": nodes}}
    adapter = YarnAdapter(transport=make_fixed_transport(payload))
    result = await adapter.list_nodes(max_results=10)
    assert len(result["nodes"]["node"]) == 10
    assert result["nodes"]["_truncated"] is True


@pytest.mark.asyncio
async def test_list_applications_limit_capped():
    payload = {"apps": {"app": []}}
    captured_params: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured_params["params"] = dict(request.url.params)
        return httpx.Response(200, content=json.dumps(payload).encode(), headers={"content-type": "application/json"})

    adapter = YarnAdapter(transport=httpx.MockTransport(handler=handler))
    await adapter.list_applications(limit=999)
    assert int(captured_params["params"]["limit"]) == 200


@pytest.mark.asyncio
async def test_get_application_invalid_id():
    adapter = YarnAdapter(transport=make_fixed_transport({}))
    with pytest.raises(YarnAdapterError) as exc_info:
        await adapter.get_application("bad/id")
    assert exc_info.value.code == "validation_error"


# ---------------------------------------------------------------------------
# Error mapping
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_404_raises_not_found():
    adapter = YarnAdapter(transport=make_status_transport(404, {"message": "not found"}))
    with pytest.raises(YarnAdapterError) as exc_info:
        await adapter.get_cluster_info()
    assert exc_info.value.code == "not_found"
    assert exc_info.value.status_code == 404


@pytest.mark.asyncio
async def test_500_raises_unavailable_retryable():
    adapter = YarnAdapter(transport=make_status_transport(500))
    with pytest.raises(YarnAdapterError) as exc_info:
        await adapter.get_cluster_info()
    assert exc_info.value.code == "unavailable"
    assert exc_info.value.retryable is True


@pytest.mark.asyncio
async def test_connection_error_raises_unavailable():
    adapter = YarnAdapter(transport=make_error_transport())
    with pytest.raises(YarnAdapterError) as exc_info:
        await adapter.get_cluster_info()
    assert exc_info.value.code == "unavailable"
    assert exc_info.value.retryable is True


# ---------------------------------------------------------------------------
# Configuration filtering
# ---------------------------------------------------------------------------

def test_filter_conf_removes_sensitive():
    data = {
        "property": [
            {"name": "yarn.scheduler.capacity.maximum-applications", "value": "10000"},
            {"name": "hadoop.security.authentication.password", "value": "s3cr3t"},
            {"name": "some.service.token", "value": "abc123"},
        ]
    }
    result = _filter_conf(data)
    names = [p["name"] for p in result["property"]]
    assert "yarn.scheduler.capacity.maximum-applications" in names
    assert "hadoop.security.authentication.password" not in names
    assert "some.service.token" not in names


# ---------------------------------------------------------------------------
# MCP tools registration and annotations
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_mcp_tool_registration():
    from fastmcp import FastMCP

    from yarn_assist.mcp.tools import register_tools

    mcp = FastMCP(name="test")
    adapter = YarnAdapter(transport=make_fixed_transport({}))
    register_tools(mcp, adapter)

    tools = await mcp.list_tools()
    tool_names = {t.name for t in tools}
    expected = {
        "get_cluster_info",
        "get_cluster_metrics",
        "get_scheduler",
        "get_scheduler_configuration",
        "list_nodes",
        "list_node_labels",
        "list_applications",
        "get_application",
        "propose_scheduler_changes",
    }
    assert expected.issubset(tool_names)


@pytest.mark.asyncio
async def test_no_write_tools():
    from fastmcp import FastMCP

    from yarn_assist.mcp.tools import register_tools

    mcp = FastMCP(name="test")
    adapter = YarnAdapter(transport=make_fixed_transport({}))
    register_tools(mcp, adapter)

    tools = await mcp.list_tools()
    tool_names = {t.name for t in tools}
    forbidden = {"trigger_dag_run", "apply_scheduler_configuration", "put_scheduler_conf", "applyChanges"}
    assert tool_names.isdisjoint(forbidden), f"Write tools found: {tool_names & forbidden}"


@pytest.mark.asyncio
async def test_all_tools_non_destructive():
    from fastmcp import FastMCP

    from yarn_assist.mcp.tools import register_tools

    mcp = FastMCP(name="test")
    adapter = YarnAdapter(transport=make_fixed_transport({}))
    register_tools(mcp, adapter)

    tools = await mcp.list_tools()
    for tool in tools:
        ann = tool.annotations
        if ann is not None:
            assert ann.destructiveHint is not True, f"Tool {tool.name} has destructiveHint=True"


@pytest.mark.asyncio
async def test_propose_scheduler_changes_returns_proposal():
    from fastmcp import FastMCP

    from yarn_assist.mcp.tools import register_tools

    mcp = FastMCP(name="test")
    adapter = YarnAdapter(transport=make_fixed_transport({}))
    register_tools(mcp, adapter)

    result = await mcp.call_tool(
        "propose_scheduler_changes",
        {
            "summary": "Test proposal",
            "rationale": "Testing",
            "changes": [
                {
                    "kind": "queue_property",
                    "queue_path": "root.a",
                    "property": "capacity",
                    "new_value": "20",
                    "old_value": "10",
                    "reason": "test",
                }
            ],
        },
    )
    # FastMCP 3.x returns a ToolResult with .content list
    raw = result.content[0].text if result.content else result.structured_content.get("result", "")
    data = json.loads(raw)
    assert data["summary"] == "Test proposal"
    assert "proposal_id" in data
    assert len(data["changes"]) == 1
