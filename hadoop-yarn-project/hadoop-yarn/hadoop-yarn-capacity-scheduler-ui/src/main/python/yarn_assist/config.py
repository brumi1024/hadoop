"""Environment-backed settings for the YARN Assist service."""

from __future__ import annotations

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # ResourceManager
    yarn_assist_rm_base_url: str = "http://localhost:8088"

    # Model - intentionally no default so misconfiguration fails loudly
    yarn_assist_model: str | None = None

    # Development auth - disabled by default
    yarn_assist_allow_dev_auth: bool = False
    yarn_assist_dev_user: str = "dev-user"

    # Network binding - defaults to loopback; set to 0.0.0.0 only behind a trusted proxy
    yarn_assist_bind_host: str = "127.0.0.1"
    yarn_assist_bind_port: int = 8090

    # Development CORS - disabled by default; enable only with a local Vite server
    yarn_assist_allow_dev_cors: bool = False

    # Request/response bounds
    yarn_assist_request_timeout_seconds: float = 15.0
    yarn_assist_max_response_chars: int = 64_000
    yarn_assist_max_tool_calls_per_minute: int = 60
    yarn_assist_max_apps_returned: int = 200
    yarn_assist_max_nodes_returned: int = 500
    yarn_assist_max_message_chars: int = 50_000

    # Conversation store
    yarn_assist_max_conversations: int = 100
    yarn_assist_conversation_ttl_seconds: int = 604_800  # 7 days

    model_config = {"env_file": ".env", "extra": "ignore"}


_settings: Settings | None = None


def get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings
