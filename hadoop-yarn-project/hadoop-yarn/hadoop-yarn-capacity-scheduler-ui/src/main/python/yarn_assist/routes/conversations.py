"""User-scoped conversation management routes."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from yarn_assist.auth import get_current_user
from yarn_assist.conversations.store import Conversation, conversation_store

router = APIRouter()


class CreateConversationRequest(BaseModel):
    page_context: dict[str, Any] = Field(default_factory=dict)


class UpdateConversationRequest(BaseModel):
    title: str = Field(min_length=1, max_length=120)


def _summary(conv: Conversation) -> dict:
    return {
        "conversation_id": conv.id,
        "created_at": conv.created_at.isoformat(),
        "updated_at": conv.updated_at.isoformat(),
        "title": conv.title or _derive_title(conv),
        "message_count": len(conv.messages),
    }


def _derive_title(conv: Conversation, max_length: int = 60) -> str:
    for msg in conv.messages:
        if msg.get("role") == "user":
            parts = msg.get("parts") or msg.get("content") or []
            text = ""
            if isinstance(parts, str):
                text = parts
            elif isinstance(parts, list):
                for part in parts:
                    if isinstance(part, dict) and part.get("type") == "text":
                        text = part.get("text", "")
                        break
            text = text.strip().replace("\n", " ")
            if text:
                return text[:max_length]
    return "New conversation"


@router.get("/conversations")
async def list_conversations(username: str = Depends(get_current_user)) -> dict:
    convs = conversation_store.list_user(username)
    return {"conversations": [_summary(c) for c in reversed(convs)]}


@router.post("/conversations", status_code=201)
async def create_conversation(
    body: CreateConversationRequest,
    username: str = Depends(get_current_user),
) -> dict:
    conv = conversation_store.create(username, page_context=body.page_context)
    return {"conversation_id": conv.id}


@router.get("/conversations/{conv_id}")
async def get_conversation(
    conv_id: str,
    username: str = Depends(get_current_user),
) -> dict:
    conv = conversation_store.get(conv_id, username)
    if conv is None:
        raise HTTPException(status_code=404, detail="Conversation not found.")
    return {**_summary(conv), "messages": conv.messages}


@router.patch("/conversations/{conv_id}")
async def rename_conversation(
    conv_id: str,
    body: UpdateConversationRequest,
    username: str = Depends(get_current_user),
) -> dict:
    ok = conversation_store.rename(conv_id, username, body.title)
    if not ok:
        raise HTTPException(status_code=404, detail="Conversation not found.")
    return {"ok": True}


@router.delete("/conversations/{conv_id}", status_code=204)
async def delete_conversation(
    conv_id: str,
    username: str = Depends(get_current_user),
) -> None:
    ok = conversation_store.delete(conv_id, username)
    if not ok:
        raise HTTPException(status_code=404, detail="Conversation not found.")
