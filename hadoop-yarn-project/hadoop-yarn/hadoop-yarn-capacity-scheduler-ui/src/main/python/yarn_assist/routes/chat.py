"""Chat streaming route backed by Pydantic AI and the Vercel AI SDK adapter."""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import ValidationError
from pydantic_ai.ui.vercel_ai import VercelAIAdapter

from yarn_assist.auth import get_current_user
from yarn_assist.chat.agent import build_agent, build_mcp_toolset, build_model
from yarn_assist.chat.message_history import deserialize_messages, serialize_messages
from yarn_assist.config import Settings, get_settings
from yarn_assist.conversations.store import conversation_store

logger = logging.getLogger(__name__)

router = APIRouter()

MAX_MESSAGE_CHARS = 50_000


@router.post("/chat/stream")
async def chat_stream(
    request: Request,
    username: str = Depends(get_current_user),
    settings: Settings = Depends(get_settings),
):
    """Stream AI SDK UI-message events using server-side conversation history."""
    # Build model - fail clearly if not configured
    try:
        model = build_model(settings)
    except RuntimeError as exc:
        raise HTTPException(
            status_code=503,
            detail={"code": "model_not_configured", "message": str(exc)},
        ) from exc

    # Parse the AI SDK request body
    try:
        run_input = VercelAIAdapter.build_run_input(await request.body())
    except (ValidationError, Exception) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    # Look up the conversation
    conversation = conversation_store.get(run_input.id, username)
    if conversation is None:
        raise HTTPException(status_code=404, detail="Conversation not found.")

    # Validate the latest user message
    latest_user = next(
        (m for m in reversed(run_input.messages) if m.role == "user"), None
    )
    if run_input.trigger == "submit-message":
        if latest_user is None:
            raise HTTPException(status_code=422, detail="No user message found.")
        text = _message_text(latest_user)
        if not text or len(text) > MAX_MESSAGE_CHARS:
            raise HTTPException(
                status_code=422,
                detail=f"Message must be between 1 and {MAX_MESSAGE_CHARS} characters.",
            )
        # Only send the latest user message to the model; history is server-side
        run_input = run_input.model_copy(update={"messages": [latest_user]})

    # Load server-side history
    history = _load_history(conversation)

    # Build agent with MCP toolset
    request_headers = dict(request.headers)
    toolset = build_mcp_toolset(request_headers, settings)
    agent = build_agent(model, toolset)

    def on_complete(messages: list) -> None:
        all_messages = history + messages
        conversation_store.set_messages(conversation.id, _stored_history(all_messages))

    return VercelAIAdapter.dispatch_request(
        request,
        agent=agent,
        sdk_version=6,
        message_history=history,
        conversation_id=conversation.id,
        on_complete=lambda _, m: on_complete(m),
    )


def _message_text(message) -> str:
    from pydantic_ai.ui.vercel_ai.request_types import TextUIPart
    return "".join(
        part.text for part in (message.parts or []) if isinstance(part, TextUIPart)
    ).strip()


def _load_history(conversation) -> list:
    batches = [m["_pai_batch"] for m in conversation.messages if m.get("_pai_batch")]
    flat = [msg for batch in batches for msg in batch]
    return deserialize_messages(flat) if flat else []


def _stored_history(history: list) -> list[dict]:
    serialized = serialize_messages(history)
    return [{"_pai_batch": serialized}] if serialized else []
