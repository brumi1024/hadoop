"""Serialize/deserialize pydantic-ai message histories."""

from __future__ import annotations

import json
from typing import Any

from pydantic_ai.messages import ModelMessagesTypeAdapter


def serialize_messages(messages: list) -> list[dict[str, Any]]:
    if not messages:
        return []
    raw: bytes = ModelMessagesTypeAdapter.dump_json(messages)
    return json.loads(raw)


def deserialize_messages(data: list[dict[str, Any]]) -> list:
    if not data:
        return []
    raw: bytes = json.dumps(data).encode()
    return ModelMessagesTypeAdapter.validate_json(raw)
