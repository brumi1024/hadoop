"""Response size bounds."""

from __future__ import annotations

from pydantic import BaseModel


class BoundedText(BaseModel):
    text: str
    truncated: bool
    original_chars: int
    returned_chars: int


def truncate_text(text: str, max_chars: int) -> BoundedText:
    original = len(text)
    if original <= max_chars:
        return BoundedText(text=text, truncated=False, original_chars=original, returned_chars=original)
    return BoundedText(
        text=text[:max_chars] + "\n[...truncated...]",
        truncated=True,
        original_chars=original,
        returned_chars=max_chars,
    )
