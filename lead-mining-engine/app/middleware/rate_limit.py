"""
Rate Limit Middleware — token-bucket-based API rate limiting
P2-6: Prevent single-IP over-speed, protect upstream API quota

Token bucket parameters:
  rate     = tokens refilled per second (= requests_per_minute / 60)
  capacity = max bucket capacity (allowed burst size)
"""
from __future__ import annotations

import asyncio
import ipaddress
import os
import time
from typing import Dict, FrozenSet, Optional, Tuple

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse


def _parse_trusted_proxies() -> FrozenSet[ipaddress.IPv4Network | ipaddress.IPv6Network]:
    """Parse trusted proxy IP/CIDR list from TRUSTED_PROXY_IPS env var"""
    raw = os.environ.get("TRUSTED_PROXY_IPS", "127.0.0.1,172.16.0.0/12")
    networks = set()
    for entry in raw.split(","):
        entry = entry.strip()
        if not entry:
            continue
        try:
            networks.add(ipaddress.ip_network(entry, strict=False))
        except ValueError:
            pass
    return frozenset(networks)


def _is_trusted_proxy(client_ip: str, trusted: FrozenSet) -> bool:
    """Check if client_ip is within a trusted proxy subnet"""
    try:
        addr = ipaddress.ip_address(client_ip)
        return any(addr in net for net in trusted)
    except ValueError:
        return False


# ── Token bucket ─────────────────────────────────────────────────────────────
class _TokenBucket:
    """Async token bucket (sliding refill), supports concurrent access"""

    __slots__ = ("rate", "capacity", "_tokens", "_last_ts", "_lock")

    def __init__(self, rate: float, capacity: int):
        self.rate     = rate          # tokens / second
        self.capacity = capacity
        self._tokens  = float(capacity)
        self._last_ts = time.monotonic()
        self._lock    = asyncio.Lock()

    async def consume(self, tokens: int = 1) -> bool:
        async with self._lock:
            now = time.monotonic()
            # Refill tokens
            self._tokens = min(
                self.capacity,
                self._tokens + (now - self._last_ts) * self.rate,
            )
            self._last_ts = now
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False


# ── Middleware ───────────────────────────────────────────────────────────────
class RateLimitMiddleware(BaseHTTPMiddleware):
    """
    Global IP-level API rate limiting.

    Rules (match by endpoint prefix, more specific = higher priority):
      /mine      → 10 req/min, burst 10   (heavy mining tasks)
      /enrich    → 5  req/min, burst 5    (AI enrichment, protect Gemini quota)
      /rag/query → 20 req/min, burst 20
      others     → 60 req/min, burst 60

    Unrestricted paths: /health /docs /redoc /openapi.json /dashboard /stats
    """

    # (rate_per_sec, capacity)
    _RULES: Tuple[Tuple[str, float, int], ...] = (
        ("/mine",       10 / 60, 10),
        ("/enrich",      5 / 60,  5),
        ("/rag/query",  20 / 60, 20),
    )
    _DEFAULT: Tuple[float, int] = (60 / 60, 60)

    _EXEMPT = frozenset({
        "/health", "/docs", "/redoc", "/openapi.json",
        "/dashboard", "/stats",
    })

    def __init__(self, app, enabled: bool = True):
        super().__init__(app)
        self._enabled  = enabled
        self._trusted_proxies = _parse_trusted_proxies()
        # (client_ip, path_key) → TokenBucket
        self._buckets:   Dict[Tuple[str, str], _TokenBucket] = {}
        # Clean up inactive buckets every 10 minutes (prevent memory leaks)
        self._last_gc    = time.monotonic()
        self._gc_interval = 600

    async def dispatch(self, request: Request, call_next):
        if not self._enabled:
            return await call_next(request)

        path = request.url.path
        if path in self._EXEMPT or path.startswith("/docs") or path.startswith("/redoc"):
            return await call_next(request)

        ip = self._get_client_ip(request)
        rate, cap, path_key = self._match_rule(path)
        key  = (ip, path_key)

        if key not in self._buckets:
            self._buckets[key] = _TokenBucket(rate=rate, capacity=cap)

        allowed = await self._buckets[key].consume()

        # Periodic GC
        now = time.monotonic()
        if now - self._last_gc > self._gc_interval:
            self._gc()
            self._last_gc = now

        if not allowed:
            limit_per_min = int(rate * 60)
            return JSONResponse(
                status_code=429,
                content={
                    "error":   "rate_limit_exceeded",
                    "detail":  f"Too many requests — limit is {limit_per_min} req/min for {path_key}",
                    "limit":   limit_per_min,
                    "retry_after_seconds": max(1, int(1 / max(rate, 1e-6))),
                },
                headers={"Retry-After": str(max(1, int(1 / max(rate, 1e-6))))},
            )

        return await call_next(request)

    # ── Helpers ─────────────────────────────────────────────────────────────────

    def _get_client_ip(self, request: Request) -> str:
        """
        Safely retrieve client IP: trust X-Forwarded-For only when the direct IP belongs to a trusted proxy,
        otherwise use the real TCP connection IP to prevent IP spoofing from bypassing rate limiting.
        """
        direct_ip = request.client.host if request.client else ""
        if direct_ip and _is_trusted_proxy(direct_ip, self._trusted_proxies):
            forwarded = request.headers.get("X-Forwarded-For", "")
            if forwarded:
                return forwarded.split(",")[0].strip()
        return direct_ip or "anonymous"

    def _match_rule(self, path: str) -> Tuple[float, int, str]:
        """Return (rate, capacity, path_key)"""
        for prefix, rate, cap in self._RULES:
            if path.startswith(prefix):
                return rate, cap, prefix.lstrip("/")
        return self._DEFAULT[0], self._DEFAULT[1], "default"

    def _gc(self) -> None:
        """Remove long-inactive token buckets (save memory)"""
        cutoff = time.monotonic() - self._gc_interval
        stale  = [k for k, b in self._buckets.items() if b._last_ts < cutoff]
        for k in stale:
            del self._buckets[k]
        if stale:
            import logging
            logging.getLogger("rate_limit").debug(
                f"GC: removed {len(stale)} stale rate-limit buckets"
            )
