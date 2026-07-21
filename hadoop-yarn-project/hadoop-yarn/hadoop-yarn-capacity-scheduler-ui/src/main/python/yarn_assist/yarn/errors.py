"""Stable error codes for YARN adapter failures."""

from __future__ import annotations


class YarnAdapterError(Exception):
    def __init__(
        self,
        code: str,
        message: str,
        status_code: int = 500,
        retryable: bool = False,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code
        self.retryable = retryable

    def as_dict(self) -> dict:
        return {
            "error": self.code,
            "message": self.message,
            "retryable": self.retryable,
        }
