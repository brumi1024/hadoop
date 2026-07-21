"""Async YARN ResourceManager REST adapter.

All path knowledge, query encoding, timeouts, bounds, and HTTP error mapping
live here.  Callers receive normalized dicts; they never see raw HTTP objects.

Error mapping:
  401 -> unauthenticated (503 to caller)
  403 -> permission_denied
  404 -> not_found
  4xx -> bad_request
  5xx -> unavailable (retryable)
  connection error -> unavailable (retryable)
  timeout -> timeout (retryable)
"""

from __future__ import annotations

import logging
from typing import Any
from urllib.parse import quote

import httpx

from yarn_assist.safety.redaction import redact_text
from yarn_assist.yarn.errors import YarnAdapterError

logger = logging.getLogger(__name__)

_CONF_ALLOWLIST = frozenset({
    "yarn.scheduler.capacity.maximum-applications",
    "yarn.scheduler.capacity.maximum-am-resource-percent",
    "yarn.scheduler.capacity.resource-calculator",
    "yarn.scheduler.capacity.node-locality-delay",
    "yarn.scheduler.capacity.rack-locality-additional-delay",
})
_CONF_SENSITIVE_RE = frozenset({"password", "passwd", "token", "key", "secret", "keytab"})


def _safe_app_id(app_id: str) -> str:
    """Validate and encode an application ID path segment."""
    if not app_id or any(c in app_id for c in ("/", "\\", "\x00", " ")):
        raise YarnAdapterError("validation_error", f"Invalid application id: {app_id!r}", 400)
    return quote(app_id, safe="")


def _raise_for_status(response: httpx.Response) -> None:
    code = response.status_code
    if code < 400:
        return
    try:
        detail = response.json()
        msg = detail.get("RemoteException", {}).get("message", str(detail)) if isinstance(detail, dict) else str(detail)
    except Exception:
        msg = response.text
    safe_msg = redact_text(str(msg)[:500])

    if code == 401:
        raise YarnAdapterError("unauthenticated", "ResourceManager rejected the request.", 401)
    if code == 403:
        raise YarnAdapterError("permission_denied", f"RM API 403: {safe_msg}", 403)
    if code == 404:
        raise YarnAdapterError("not_found", f"RM API 404: {safe_msg}", 404)
    if 400 <= code < 500:
        raise YarnAdapterError("bad_request", f"RM API {code}: {safe_msg}", code)
    raise YarnAdapterError("unavailable", f"RM API {code}: {safe_msg}", code, retryable=True)


class YarnAdapter:
    """Async adapter for YARN ResourceManager REST v1 APIs."""

    def __init__(
        self,
        base_url: str = "http://localhost:8088",
        timeout: float = 15.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._base = base_url.rstrip("/")
        self._timeout = timeout
        self._client = httpx.AsyncClient(
            base_url=self._base,
            timeout=timeout,
            transport=transport,
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> YarnAdapter:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.aclose()

    # ------------------------------------------------------------------
    # Public read methods
    # ------------------------------------------------------------------

    async def get_cluster_info(self) -> dict:
        return await self._get("/ws/v1/cluster/info")

    async def get_cluster_metrics(self) -> dict:
        return await self._get("/ws/v1/cluster/metrics")

    async def get_scheduler(self) -> dict:
        return await self._get("/ws/v1/cluster/scheduler")

    async def get_scheduler_configuration(self) -> dict:
        data = await self._get("/ws/v1/cluster/scheduler-conf")
        return _filter_conf(data)

    async def list_nodes(
        self,
        state: str | None = None,
        max_results: int = 500,
    ) -> dict:
        params: dict[str, Any] = {}
        if state:
            params["states"] = state
        data = await self._get("/ws/v1/cluster/nodes", params=params)
        nodes = (data.get("nodes") or {}).get("node") or []
        if isinstance(nodes, list) and len(nodes) > max_results:
            nodes = nodes[:max_results]
            data = {**data, "nodes": {"node": nodes, "_truncated": True}}
        return data

    async def get_node_labels(self) -> dict:
        return await self._get("/ws/v1/cluster/get-node-labels")

    async def list_applications(
        self,
        state: str | None = None,
        queue: str | None = None,
        user: str | None = None,
        app_type: str | None = None,
        limit: int = 200,
    ) -> dict:
        params: dict[str, Any] = {"limit": min(limit, 200)}
        if state:
            params["states"] = state
        if queue:
            params["queue"] = queue
        if user:
            params["user"] = user
        if app_type:
            params["applicationTypes"] = app_type
        return await self._get("/ws/v1/cluster/apps", params=params)

    async def get_application(self, application_id: str) -> dict:
        safe_id = _safe_app_id(application_id)
        return await self._get(f"/ws/v1/cluster/apps/{safe_id}")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _get(self, path: str, params: dict | None = None) -> dict:
        try:
            response = await self._client.get(path, params=params)
            _raise_for_status(response)
            return response.json()
        except YarnAdapterError:
            raise
        except httpx.TimeoutException as exc:
            raise YarnAdapterError(
                "timeout", f"ResourceManager request timed out: {exc}", 504, retryable=True
            ) from exc
        except httpx.ConnectError as exc:
            raise YarnAdapterError(
                "unavailable", f"Could not connect to ResourceManager: {exc}", 503, retryable=True
            ) from exc
        except httpx.HTTPError as exc:
            raise YarnAdapterError(
                "unavailable", f"ResourceManager HTTP error: {exc}", 503, retryable=True
            ) from exc


def _filter_conf(data: dict) -> dict:
    """Return only allowlisted configuration keys with values redacted for secrets.

    Uses an explicit allowlist (not a blocklist) so that unknown keys are
    excluded by default rather than exposed. Values of allowed keys are still
    run through redact_text in case a secret-shaped string appears in them.
    """
    from yarn_assist.safety.redaction import redact_text

    props = data.get("property") or []
    if not isinstance(props, list):
        props = [props]

    filtered = []
    for prop in props:
        name = prop.get("name", "")
        if name not in _CONF_ALLOWLIST:
            continue
        safe_value = redact_text(str(prop.get("value", "")))
        filtered.append({**prop, "value": safe_value})

    return {"property": filtered, "_total": len(props), "_returned": len(filtered)}
