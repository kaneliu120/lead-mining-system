"""
RateLimitMiddleware 单元测试
验证令牌桶算法、IP 提取（含 X-Forwarded-For）、路径匹配
"""
from __future__ import annotations

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock

from app.middleware.rate_limit import _TokenBucket, RateLimitMiddleware


# ── TokenBucket ──────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_token_bucket_allows_within_capacity():
    """未超过容量时应允许请求"""
    bucket = _TokenBucket(rate=1.0, capacity=5)
    results = [await bucket.consume() for _ in range(5)]
    assert all(results), "前 5 次请求都应被允许"


@pytest.mark.asyncio
async def test_token_bucket_blocks_when_exhausted():
    """令牌耗尽后应拒绝请求"""
    bucket = _TokenBucket(rate=0.01, capacity=3)
    for _ in range(3):
        await bucket.consume()
    # 第 4 次应被拒绝（rate 极低，令牌来不及补充）
    result = await bucket.consume()
    assert result is False


@pytest.mark.asyncio
async def test_token_bucket_refills_over_time():
    """等待一段时间后令牌应自动补充"""
    import time
    bucket = _TokenBucket(rate=10.0, capacity=10)
    # 消耗全部令牌
    for _ in range(10):
        await bucket.consume()
    assert await bucket.consume() is False

    # 等待足够时间让令牌补充（rate=10/s，等 0.2s 应能补充 2 个）
    await asyncio.sleep(0.25)
    assert await bucket.consume() is True


@pytest.mark.asyncio
async def test_token_bucket_concurrent_safe():
    """并发消耗时不应超发（线程安全）"""
    bucket = _TokenBucket(rate=0.01, capacity=5)
    tasks = [bucket.consume() for _ in range(10)]
    results = await asyncio.gather(*tasks)
    # 最多 5 次被允许
    assert sum(results) == 5


# ── RateLimitMiddleware IP 提取（Bug 修复验证）──────────────────────────────

@pytest.mark.asyncio
async def test_ip_extraction_from_x_forwarded_for():
    """X-Forwarded-For 头应优先于 request.client.host"""
    middleware = RateLimitMiddleware(app=AsyncMock(), enabled=True)

    call_next = AsyncMock(return_value=MagicMock(status_code=200))
    request = MagicMock()
    request.url.path = "/leads"
    request.headers.get = lambda key, default="": \
        "10.0.0.1, 10.0.0.2" if key == "X-Forwarded-For" else default
    request.client = MagicMock()
    request.client.host = "172.0.0.1"

    await middleware.dispatch(request, call_next)

    # IP 应该是 X-Forwarded-For 的第一个值，而不是 request.client.host
    # 验证方式：确认 bucket 的 key 是 ("10.0.0.1", "default")
    bucket_keys = list(middleware._buckets.keys())
    assert len(bucket_keys) == 1
    ip_used = bucket_keys[0][0]
    assert ip_used == "10.0.0.1", f"应提取 XFF 中的第一个 IP，实际得到: {ip_used}"


@pytest.mark.asyncio
async def test_ip_falls_back_to_client_host():
    """无 X-Forwarded-For 时，应使用 request.client.host"""
    middleware = RateLimitMiddleware(app=AsyncMock(), enabled=True)

    call_next = AsyncMock(return_value=MagicMock(status_code=200))
    request = MagicMock()
    request.url.path = "/leads"
    request.headers.get = lambda key, default="": default  # 无 XFF
    request.client = MagicMock()
    request.client.host = "192.168.1.100"

    await middleware.dispatch(request, call_next)

    bucket_keys = list(middleware._buckets.keys())
    ip_used = bucket_keys[0][0]
    assert ip_used == "192.168.1.100"


@pytest.mark.asyncio
async def test_ip_anonymous_when_no_client():
    """request.client 为 None 且无 XFF 时，IP 应为 'anonymous'"""
    middleware = RateLimitMiddleware(app=AsyncMock(), enabled=True)

    call_next = AsyncMock(return_value=MagicMock(status_code=200))
    request = MagicMock()
    request.url.path = "/leads"
    request.headers.get = lambda key, default="": default
    request.client = None

    await middleware.dispatch(request, call_next)

    bucket_keys = list(middleware._buckets.keys())
    ip_used = bucket_keys[0][0]
    assert ip_used == "anonymous"


# ── 路径匹配规则 ──────────────────────────────────────────────────────────────

def test_rule_matching_mine():
    middleware = RateLimitMiddleware(app=MagicMock(), enabled=True)
    rate, cap, key = middleware._match_rule("/mine")
    assert key == "mine"
    assert cap == 10


def test_rule_matching_enrich():
    middleware = RateLimitMiddleware(app=MagicMock(), enabled=True)
    rate, cap, key = middleware._match_rule("/enrich")
    assert key == "enrich"
    assert cap == 5


def test_rule_matching_rag():
    middleware = RateLimitMiddleware(app=MagicMock(), enabled=True)
    rate, cap, key = middleware._match_rule("/rag/query")
    assert key == "rag/query"
    assert cap == 20


def test_rule_matching_default():
    middleware = RateLimitMiddleware(app=MagicMock(), enabled=True)
    rate, cap, key = middleware._match_rule("/leads")
    assert key == "default"
    assert cap == 60


# ── 豁免路径不产生 bucket ─────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_exempt_paths_bypass_rate_limit():
    """健康检查和文档路径不应创建 bucket"""
    middleware = RateLimitMiddleware(app=AsyncMock(), enabled=True)
    call_next = AsyncMock(return_value=MagicMock(status_code=200))

    for exempt_path in ["/health", "/docs", "/redoc", "/openapi.json", "/stats"]:
        request = MagicMock()
        request.url.path = exempt_path
        request.headers.get = lambda k, d="": d
        request.client = None
        middleware._buckets.clear()
        await middleware.dispatch(request, call_next)
        assert len(middleware._buckets) == 0, f"{exempt_path} 不应创建 bucket"


@pytest.mark.asyncio
async def test_disabled_middleware_passes_all():
    """enabled=False 时，所有请求都应直接通过"""
    middleware = RateLimitMiddleware(app=AsyncMock(), enabled=False)
    call_next = AsyncMock(return_value=MagicMock(status_code=200))

    request = MagicMock()
    request.url.path = "/mine"
    request.headers.get = lambda k, d="": d
    request.client = None

    for _ in range(20):
        resp = await middleware.dispatch(request, call_next)
        assert resp.status_code == 200
