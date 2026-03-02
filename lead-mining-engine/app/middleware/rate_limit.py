"""
Rate Limit Middleware — 基于令牌桶算法的 API 速率限制
P2-6：防止单个 IP 超速，保护上游 API 配额

令牌桶参数说明：
  rate     = 每秒补充的令牌数（= requests_per_minute / 60）
  capacity = 桶的最大容量（允许的突发量）
"""
from __future__ import annotations

import asyncio
import time
from typing import Dict, Optional, Tuple

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse


# ── 令牌桶 ────────────────────────────────────────────────────────────────────
class _TokenBucket:
    """异步令牌桶（滑动填充），支持并发访问"""

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
            # 补充令牌
            self._tokens = min(
                self.capacity,
                self._tokens + (now - self._last_ts) * self.rate,
            )
            self._last_ts = now
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False


# ── 中间件 ────────────────────────────────────────────────────────────────────
class RateLimitMiddleware(BaseHTTPMiddleware):
    """
    全局 IP 级 API 速率限制。

    规则（按端点前缀匹配，越精确越优先）：
      /mine      → 10 req/min，burst 10   （重型采集任务）
      /enrich    → 5  req/min，burst 5    （AI 富化，保护 Gemini 配额）
      /rag/query → 20 req/min，burst 20
      其他        → 60 req/min，burst 60

    不受限路径：/health /docs /redoc /openapi.json /dashboard /stats
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
        # (client_ip, path_key) → TokenBucket
        self._buckets:   Dict[Tuple[str, str], _TokenBucket] = {}
        # 每 10 分钟清理一次不活跃的桶（避免内存泄漏）
        self._last_gc    = time.monotonic()
        self._gc_interval = 600

    async def dispatch(self, request: Request, call_next):
        if not self._enabled:
            return await call_next(request)

        path = request.url.path
        if path in self._EXEMPT or path.startswith("/docs") or path.startswith("/redoc"):
            return await call_next(request)

        ip = (
            request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
            or (request.client.host if request.client else "")
            or "anonymous"
        )
        rate, cap, path_key = self._match_rule(path)
        key  = (ip, path_key)

        if key not in self._buckets:
            self._buckets[key] = _TokenBucket(rate=rate, capacity=cap)

        allowed = await self._buckets[key].consume()

        # 定期 GC
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

    # ── 辅助 ──────────────────────────────────────────────────────────────────

    def _match_rule(self, path: str) -> Tuple[float, int, str]:
        """返回 (rate, capacity, path_key)"""
        for prefix, rate, cap in self._RULES:
            if path.startswith(prefix):
                return rate, cap, prefix.lstrip("/")
        return self._DEFAULT[0], self._DEFAULT[1], "default"

    def _gc(self) -> None:
        """移除长时间不活跃的令牌桶（节省内存）"""
        cutoff = time.monotonic() - self._gc_interval
        stale  = [k for k, b in self._buckets.items() if b._last_ts < cutoff]
        for k in stale:
            del self._buckets[k]
        if stale:
            import logging
            logging.getLogger("rate_limit").debug(
                f"GC: removed {len(stale)} stale rate-limit buckets"
            )
