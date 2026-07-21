"""Entry point: python -m yarn_assist"""

from __future__ import annotations

import uvicorn

from yarn_assist.app import create_app
from yarn_assist.config import get_settings

# SECURITY NOTE: The service defaults to loopback (127.0.0.1). Set
# YARN_ASSIST_BIND_HOST=0.0.0.0 only when deployed behind a reverse proxy
# that enforces trusted-identity header overwrite (x-awc-username etc.).
# Never expose this service to untrusted networks without that proxy in place.

app = create_app()

if __name__ == "__main__":
    settings = get_settings()
    uvicorn.run(
        "yarn_assist.__main__:app",
        host=settings.yarn_assist_bind_host,
        port=settings.yarn_assist_bind_port,
        reload=False,
    )
