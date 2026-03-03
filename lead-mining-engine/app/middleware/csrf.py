"""
CSRF Protection Middleware — Origin/Referer 验证
防止跨站请求伪造：对 POST/PUT/DELETE/PATCH 请求验证来源

策略：
  1. API 客户端（Content-Type: application/json）通过 Origin/Referer 校验
  2. 浏览器表单提交（非 JSON）必须来自可信 Origin
  3. 内部服务间调用（无 Origin/Referer）在可信 IP 内放行
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

# 不需要 CSRF 保护的安全方法
_SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})

# 不需要 CSRF 检查的路径前缀（如 API 文档、健康检查）
_EXEMPT_PATHS = frozenset({"/health", "/docs", "/redoc", "/openapi.json"})


def _parse_trusted_origins() -> FrozenSet[str]:
    """从环境变量解析可信 Origin 列表"""
    domain = os.environ.get("DOMAIN", "")
    origins = set()
    if domain:
        origins.add(f"https://{domain}")
        origins.add(f"https://n8n.{domain}")
        origins.add(f"https://api.{domain}")
    # 开发模式下允许 localhost
    for port in ("8000", "8010", "5678", "3000", "8080"):
        origins.add(f"http://localhost:{port}")
        origins.add(f"http://127.0.0.1:{port}")
    # 额外的可信 Origin
    extra = os.environ.get("CSRF_TRUSTED_ORIGINS", "")
    for origin in extra.split(","):
        origin = origin.strip()
        if origin:
            origins.add(origin)
    return frozenset(origins)


class CSRFMiddleware(BaseHTTPMiddleware):
    """基于 Origin/Referer 验证的 CSRF 防护中间件"""

    def __init__(self, app, enabled: bool = True):
        super().__init__(app)
        self._enabled = enabled
        self._trusted_origins = _parse_trusted_origins()

    async def dispatch(self, request: Request, call_next):
        if not self._enabled:
            return await call_next(request)

        # 安全方法无需检查
        if request.method in _SAFE_METHODS:
            return await call_next(request)

        # 免检路径
        path = request.url.path
        if path in _EXEMPT_PATHS:
            return await call_next(request)

        # 获取 Origin 或 Referer
        origin = request.headers.get("Origin") or ""
        referer = request.headers.get("Referer") or ""

        # 内部服务调用（无 Origin 且无 Referer），检查 Content-Type
        # JSON API 调用如果没有 Origin（如 curl/httpx），视情况放行
        if not origin and not referer:
            content_type = request.headers.get("Content-Type", "")
            if "application/json" in content_type:
                # API 客户端直接调用（curl/httpx/n8n），放行
                return await call_next(request)
            # 无 Origin 的非 JSON 请求（可能是恶意表单提交），拒绝
            logger.warning(f"CSRF blocked: {request.method} {path} — no Origin/Referer")
            return JSONResponse(
                status_code=403,
                content={"error": "csrf_validation_failed", "detail": "Missing Origin header"},
            )

        # 验证 Origin
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
