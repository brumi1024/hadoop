# YARN Capacity Scheduler AI Assist - Python Service

A standalone FastAPI service providing AI-assisted YARN Capacity Scheduler management.
This is a proof of concept - not for production use without the gaps listed below.

## Prerequisites

- Python 3.11+
- All dependencies are listed in `pyproject.toml`.
- Install with `pip install -e ".[dev]"` from this directory.

## Running the service

```bash
cd src/main/python
pip install -e ".[dev]"

# Development mode (no model required for health/capabilities)
# ALLOW_DEV_CORS is required only when the Vite dev server (port 5173) calls this service.
YARN_ASSIST_ALLOW_DEV_AUTH=true YARN_ASSIST_ALLOW_DEV_CORS=true uvicorn yarn_assist.app:app --factory --port 8090 --reload
```

The service starts at http://localhost:8090 with docs at `/yarn-assist/docs`.

## Configuration

All settings are environment variables (can also be placed in a `.env` file):

| Variable | Default | Description |
|---|---|---|
| `YARN_ASSIST_RM_BASE_URL` | `http://localhost:8088` | ResourceManager base URL |
| `YARN_ASSIST_MODEL` | _(none)_ | Pydantic AI model string, e.g. `openai:gpt-4o` |
| `YARN_ASSIST_ALLOW_DEV_AUTH` | `false` | Allow requests without identity headers (dev only) |
| `YARN_ASSIST_DEV_USER` | `dev-user` | Username used when dev auth is active |
| `YARN_ASSIST_BIND_HOST` | `127.0.0.1` | Network interface to bind - do not change to 0.0.0.0 without a trusted proxy |
| `YARN_ASSIST_BIND_PORT` | `8090` | Port to listen on |
| `YARN_ASSIST_ALLOW_DEV_CORS` | `false` | Enable CORS for the local Vite dev server (dev only) |
| `YARN_ASSIST_REQUEST_TIMEOUT_SECONDS` | `15.0` | ResourceManager request timeout |
| `YARN_ASSIST_MAX_RESPONSE_CHARS` | `64000` | Max chars in a single tool response |
| `YARN_ASSIST_MAX_TOOL_CALLS_PER_MINUTE` | `60` | Per-user tool call rate limit |
| `YARN_ASSIST_MAX_APPS_RETURNED` | `200` | Max application list size |
| `YARN_ASSIST_MAX_NODES_RETURNED` | `500` | Max node list size |
| `YARN_ASSIST_MAX_MESSAGE_CHARS` | `50000` | Max user message length |
| `YARN_ASSIST_MAX_CONVERSATIONS` | `100` | Max in-memory conversations |
| `YARN_ASSIST_CONVERSATION_TTL_SECONDS` | `604800` | Conversation TTL (7 days) |

Provider API keys are consumed from standard Pydantic AI environment variables
(e.g. `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`).

## Routes

| Method | Path | Description |
|---|---|---|
| `GET` | `/yarn-assist/api/v1/health` | Process health and model status |
| `GET` | `/yarn-assist/api/v1/capabilities` | Feature flags |
| `GET` | `/yarn-assist/api/v1/conversations` | List user conversations |
| `POST` | `/yarn-assist/api/v1/conversations` | Create conversation |
| `GET` | `/yarn-assist/api/v1/conversations/{id}` | Get one conversation |
| `PATCH` | `/yarn-assist/api/v1/conversations/{id}` | Rename conversation |
| `DELETE` | `/yarn-assist/api/v1/conversations/{id}` | Delete conversation |
| `POST` | `/yarn-assist/api/v1/chat/stream` | Stream AI SDK chat response |
| `POST` | `/yarn-mcp/v1` | Streamable HTTP MCP |

## Running tests

```bash
cd src/main/python
python -m pytest
python -m ruff check .
```

## Connecting to the Vite development server

Set `VITE_YARN_ASSIST_PROXY_TARGET=http://localhost:8090` in
`src/main/webapp/.env` and the Vite server will proxy `/yarn-assist` and
`/yarn-mcp` to this service.
Start the service first, then `npm run dev` from `src/main/webapp/`.

## Production gaps

This PoC does not address:

- Knox route and trusted-header overwrite contract.
- Kerberos, delegation, or service-identity for ResourceManager calls.
- Persistent conversation storage and retention policy.
- Model secret delivery and provider policy.
- High availability, health probes, observability, audit retention, rate policy.
- Packaging and lifecycle outside the ResourceManager JVM.
- Formal authorization for configuration visibility and future operational writes.
- Upgrade, compatibility, and rollback strategy.
- Apache dependency, licensing, and release review for new dependencies.
