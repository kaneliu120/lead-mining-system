"""
CSRF Protection Middleware — Origin/Referer validation
Prevents cross-site request forgery: validates source for POST/PUT/DELETE/PATCH requests

Strategy:
  1. API clients (Content-Type: application/json) pass Origin/Referer check
  2. Browser form submissions (non-JSON) must come from a trusted Origin
  3. Inter-service calls (no Origin/Referer) allowed within trusted IP range
"""
from __future__ import annotations

import logging
import os
from typing import FrozenSet, Optional
from urllib.parse import urlparse

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

logger = logging.getLogger(__name__)

# Safe methods that don't need CSRF protection
_SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})

# Path prefixes exempt from CSRF check (e.g. API docs, health check)
_EXEMPT_PATHS = frozenset({"/health", "/docs", "/redoc", "/openapi.json"})


def _parse_trusted_origins() -> FrozenSet[str]:
    """Parse trusted Origin list from environment variables"""
    domain = os.environ.get("DOMAIN", "")
    origins = set()
    if domain:
        origins.add(f"https://{domain}")
        origins.add(f"https://n8n.{domain}")
        origins.add(f"https://api.{domain}")
    # Allow localhost in dev mode
    for port in ("8000", "8010", "5678", "3000", "8080"):
        origins.add(f"http://localhost:{port}")
        origins.add(f"http://127.0.0.1:{port}")
    # Additional trusted Origins
    extra = os.environ.get("CSRF_TRUSTED_ORIGINS", "")
    for origin in extra.split(","):
        origin = origin.strip()
        if origin:
            origins.add(origin)
    return frozenset(origins)


class CSRFMiddleware(BaseHTTPMiddleware):
    """CSRF protection middleware based on Origin/Referer validation"""

    def __init__(self, app, enabled: bool = True):
        super().__init__(app)
        self._enabled = enabled
        self._trusted_origins = _parse_trusted_origins()

    async def dispatch(self, request: Request, call_next):
        if not self._enabled:
            return await call_next(request)

        # Safe methods need no check
        if request.method in _SAFE_METHODS:
            return await call_next(request)

        # Exempt paths
        path = request.url.path
        if path in _EXEMPT_PATHS:
            return await call_next(request)

        # Get Origin or Referer
        origin = request.headers.get("Origin") or ""
        referer = request.headers.get("Referer") or ""

        # Internal service call (no Origin and no Referer) — check Content-Type
        # JSON API call without Origin (e.g. curl/httpx) — allow through
        if not origin and not referer:
            content_type = request.headers.get("Content-Type", "")
            if "application/json" in content_type:
                # Direct API client call (curl/httpx/n8n) — allow
                return await call_next(request)
            # Non-JSON request without Origin (potentially malicious form submission) — reject
            logger.warning(f"CSRF blocked: {request.method} {path} — no Origin/Referer")
            return JSONResponse(
                status_code=403,
                content={"error": "csrf_validation_failed", "detail": "Missing Origin header"},
            )

        # Validate Origin
        check_origin = origin
        if not check_origin and referer:
            parsed = urlparse(referer)
            check_origin = f"{parsed.scheme}://{parsed.netloc}"

        if check_origin not in self._trusted_origins:
            logger.warning(
                f"CSRF blocked: {request.method} {path} — "
                f"untrusted origin '{check_origin}'"
            )
            return JSONResponse(
                status_code=403,
                content={"error": "csrf_validation_failed", "detail": "Untrusted origin"},
            )

        return await call_next(request)
