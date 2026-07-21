"""P3 tests: chat API, model config, conversation ownership, message validation."""

from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient


def _ai_sdk_body(conv_id: str, text: str = "Hello") -> bytes:
    """Build a minimal AI SDK v6 request body."""
    return json.dumps({
        "id": conv_id,
        "messages": [
            {
                "id": "msg_001",
                "role": "user",
                "parts": [{"type": "text", "text": text}],
            }
        ],
        "trigger": "submit-message",
    }).encode()


@pytest.fixture
def dev_settings():
    from yarn_assist.config import Settings
    return Settings(yarn_assist_allow_dev_auth=True, yarn_assist_dev_user="test-user")


@pytest.fixture
def app(dev_settings):
    from yarn_assist.app import create_app
    return create_app(dev_settings)


@pytest.fixture
def client(app):
    return TestClient(app, raise_server_exceptions=True)


@pytest.fixture
def conv_id(client):
    r = client.post(
        "/yarn-assist/api/v1/conversations",
        json={"page_context": {"page_kind": "queues"}},
    )
    assert r.status_code == 201
    return r.json()["conversation_id"]


# --- Model not configured ---

def test_chat_stream_503_when_no_model(client, conv_id):
    r = client.post(
        "/yarn-assist/api/v1/chat/stream",
        content=_ai_sdk_body(conv_id),
        headers={"content-type": "application/json"},
    )
    assert r.status_code == 503
    assert r.json()["detail"]["code"] == "model_not_configured"


# --- Conversation not found ---

def test_chat_stream_404_unknown_conversation():
    from yarn_assist.app import create_app
    from yarn_assist.config import Settings
    app2 = create_app(Settings(
        yarn_assist_allow_dev_auth=True,
        yarn_assist_dev_user="test-user",
        yarn_assist_model="test",
    ))
    c = TestClient(app2, raise_server_exceptions=True)
    r = c.post(
        "/yarn-assist/api/v1/chat/stream",
        content=_ai_sdk_body("nonexistent:abc123"),
        headers={"content-type": "application/json"},
    )
    assert r.status_code == 404


# --- Message validation ---

def test_chat_stream_422_empty_message():
    from yarn_assist.app import create_app
    from yarn_assist.config import Settings
    app2 = create_app(Settings(
        yarn_assist_allow_dev_auth=True,
        yarn_assist_dev_user="test-user",
        yarn_assist_model="test",
    ))
    c = TestClient(app2, raise_server_exceptions=True)
    r = c.post("/yarn-assist/api/v1/conversations", json={})
    cid = r.json()["conversation_id"]
    r = c.post(
        "/yarn-assist/api/v1/chat/stream",
        content=_ai_sdk_body(cid, text=""),
        headers={"content-type": "application/json"},
    )
    assert r.status_code == 422


# --- Conversation ownership ---

def test_chat_stream_404_wrong_user(conv_id):
    """conv_id was created by test-user; other-user must get 404."""
    from yarn_assist.app import create_app
    from yarn_assist.config import Settings
    app = create_app(Settings(
        yarn_assist_allow_dev_auth=True,
        yarn_assist_dev_user="other-user",
        yarn_assist_model="test",
    ))
    c = TestClient(app, raise_server_exceptions=True)
    r = c.post(
        "/yarn-assist/api/v1/chat/stream",
        content=_ai_sdk_body(conv_id),
        headers={"content-type": "application/json"},
    )
    assert r.status_code == 404


# --- Message history serialization roundtrip ---

def test_message_history_roundtrip():
    from yarn_assist.chat.message_history import deserialize_messages, serialize_messages
    assert serialize_messages([]) == []
    assert deserialize_messages([]) == []


# --- No Airflow import ---

def test_no_airflow_in_chat():
    import sys


    airflow_mods = [k for k in sys.modules if k.startswith("airflow")]
    assert airflow_mods == [], f"Unexpected Airflow modules: {airflow_mods}"
