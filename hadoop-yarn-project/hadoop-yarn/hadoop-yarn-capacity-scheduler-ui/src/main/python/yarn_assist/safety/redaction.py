"""Sensitive-value redaction for YARN adapter responses."""

from __future__ import annotations

import json as _json
import re

REDACTED = "[REDACTED]"

_SENSITIVE_KEYS = frozenset(
    {"password", "passwd", "token", "api_key", "api_token", "secret", "secret_key", "access_key", "keytab"}
)
_URI_CREDS_RE = re.compile(r"//[^:@/\s]+:[^@\s]+@")
_KEYS_PATTERN = "|".join(re.escape(k) for k in sorted(_SENSITIVE_KEYS, key=len, reverse=True))
_SENSITIVE_KV_RE = re.compile(
    rf"((?:{_KEYS_PATTERN})\s*[=:]\s*)(?:['\"][^'\"]*['\"]|\S+)",
    re.IGNORECASE,
)
_MAX_REDACT_BYTES = 65_536
_MAX_REDACT_DEPTH = 50


def redact_text(text: str) -> str:
    text = _URI_CREDS_RE.sub("//[REDACTED]@", text)
    return _SENSITIVE_KV_RE.sub(r"\g<1>" + REDACTED, text)


def redact_mapping(data: dict) -> dict:
    return _redact_depth(data, 0)


def _redact_depth(data: dict, depth: int) -> dict:
    result = {}
    for key, value in data.items():
        if key.lower() in _SENSITIVE_KEYS:
            result[key] = REDACTED
        elif isinstance(value, dict):
            result[key] = _redact_depth(value, depth + 1) if depth < _MAX_REDACT_DEPTH else REDACTED
        elif isinstance(value, list):
            result[key] = [_redact_depth(v, depth + 1) if isinstance(v, dict) else v for v in value]
        elif isinstance(value, str):
            result[key] = _try_redact_json(value, depth)
        else:
            result[key] = value
    return result


def _try_redact_json(value: str, depth: int) -> str:
    if len(value) > _MAX_REDACT_BYTES or depth >= _MAX_REDACT_DEPTH:
        return redact_text(value)
    stripped = value.strip()
    if stripped.startswith(("{", "[")):
        try:
            parsed = _json.loads(stripped)
            if isinstance(parsed, dict):
                return _json.dumps(_redact_depth(parsed, depth + 1))
            if isinstance(parsed, list):
                return _json.dumps(
                    [_redact_depth(i, depth + 1) if isinstance(i, dict) else i for i in parsed]
                )
        except (ValueError, TypeError, RecursionError):
            pass
    return redact_text(value)
