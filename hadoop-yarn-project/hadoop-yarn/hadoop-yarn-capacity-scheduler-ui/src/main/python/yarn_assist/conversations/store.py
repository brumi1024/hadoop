"""Thread-safe in-memory conversation store with LRU eviction and TTL."""

from __future__ import annotations

import threading
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any


@dataclass
class Conversation:
    id: str
    username: str
    created_at: datetime
    updated_at: datetime
    title: str | None = None
    page_context: dict[str, Any] = field(default_factory=dict)
    messages: list[dict[str, Any]] = field(default_factory=list)


class ConversationStore:
    """Thread-safe LRU store for process-local user-scoped conversations."""

    def __init__(self, max_size: int = 100, ttl_seconds: int = 604_800) -> None:
        self._lock = threading.Lock()
        self._store: OrderedDict[str, Conversation] = OrderedDict()
        self._max_size = max_size
        self._ttl = timedelta(seconds=ttl_seconds)

    def create(self, username: str, page_context: dict[str, Any] | None = None) -> Conversation:
        now = datetime.now(UTC)
        conv_id = f"{username}:{uuid.uuid4().hex[:12]}"
        conv = Conversation(
            id=conv_id,
            username=username,
            created_at=now,
            updated_at=now,
            page_context=page_context or {},
        )
        with self._lock:
            self._evict_expired()
            while len(self._store) >= self._max_size:
                self._store.popitem(last=False)
            self._store[conv_id] = conv
            self._store.move_to_end(conv_id)
        return conv

    def get(self, conv_id: str, username: str) -> Conversation | None:
        with self._lock:
            conv = self._store.get(conv_id)
            if conv is None or conv.username != username:
                return None
            self._store.move_to_end(conv_id)
            return conv

    def list_user(self, username: str) -> list[Conversation]:
        with self._lock:
            self._evict_expired()
            return [c for c in self._store.values() if c.username == username]

    def rename(self, conv_id: str, username: str, title: str) -> bool:
        with self._lock:
            conv = self._store.get(conv_id)
            if conv is None or conv.username != username:
                return False
            conv.title = title
            conv.updated_at = datetime.now(UTC)
            return True

    def delete(self, conv_id: str, username: str) -> bool:
        with self._lock:
            conv = self._store.get(conv_id)
            if conv is None or conv.username != username:
                return False
            del self._store[conv_id]
            return True

    def set_messages(self, conv_id: str, messages: list[dict[str, Any]]) -> None:
        with self._lock:
            conv = self._store.get(conv_id)
            if conv is not None:
                conv.messages = messages
                conv.updated_at = datetime.now(UTC)

    def _evict_expired(self) -> None:
        cutoff = datetime.now(UTC) - self._ttl
        expired = [k for k, v in self._store.items() if v.updated_at < cutoff]
        for k in expired:
            del self._store[k]


conversation_store = ConversationStore()
