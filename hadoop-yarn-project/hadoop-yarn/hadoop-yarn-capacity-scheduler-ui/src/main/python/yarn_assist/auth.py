"""Trusted identity header extraction and development auth."""

from __future__ import annotations

from fastapi import Depends, HTTPException, Request

from yarn_assist.config import Settings, get_settings

# SECURITY NOTE - trusted identity headers (PoC limitation):
# This service trusts x-awc-username (and siblings) unconditionally when they
# arrive in a request. This is safe ONLY when the service is reachable
# exclusively through a reverse proxy that:
#   1. Overwrites these headers before forwarding (stripping any client-supplied values), AND
#   2. Authenticates the end user and injects the authoritative header values.
#
# PRODUCTION GAP: Before deploying, add one of:
#   - A shared proxy-secret header validated here (e.g. X-Proxy-Auth HMAC).
#   - mTLS: the service only accepts connections whose client cert matches the proxy.
#   - Binding to a UNIX socket or loopback address that only the local proxy can reach.
#
# The default bind is 127.0.0.1 (see __main__.py / YARN_ASSIST_BIND_HOST), which
# reduces the attack surface for direct forgery, but does not eliminate it on
# multi-tenant hosts. Do not change the bind host without also adding proxy provenance.

_TRUSTED_HEADERS = (
    "x-awc-username",
    "x-awc-userid",
    "x-awc-roles",
    "x-awc-requestid",
)


def get_trusted_headers(request: Request) -> dict[str, str]:
    return {name: request.headers[name] for name in _TRUSTED_HEADERS if name in request.headers}


async def get_current_user(
    request: Request,
    settings: Settings = Depends(get_settings),
) -> str:
    username = request.headers.get("x-awc-username")
    if username:
        return username

    if settings.yarn_assist_allow_dev_auth:
        return settings.yarn_assist_dev_user

    raise HTTPException(
        status_code=401,
        detail="Missing trusted identity header x-awc-username. "
        "Set YARN_ASSIST_ALLOW_DEV_AUTH=true for local development.",
    )
