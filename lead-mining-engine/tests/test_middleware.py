"""
RateLimitMiddleware Unit Tests
Validates token bucket algorithm, IP extraction (including X-Forwarded-For), path matching
"""
from __future__ import annotations

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock

from app.middleware.rate_limit import _TokenBucket, RateLimitMiddleware


# ── TokenBucket ──────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_token_bucket_allows_within_capacity():
    """Requests should be allowed when capacity is not exceeded"""
    bucket = _TokenBucket(rate=1.0, capacity=5)
    results = [await bucket.consume() for _ in range(5)]
    assert all(results), "First 5 requests should all be allowed"


@pytest.mark.asyncio
async def test_token_bucket_blocks_when_exhausted():
    """Requests should be rejected after tokens are exhausted"""
    bucket = _TokenBucket(rate=0.01, capacity=3)
    for _ in range(3):
        await bucket.consume()
    # The 4th request should be rejected (rate is extremely low, tokens cannot replenish in time)
    result = await bucket.consume()
    assert result is False


@pytest.mark.asyncio
async def test_token_bucket_refills_over_time():
    """Tokens should auto-refill after a waiting period"""
    import time
    bucket = _TokenBucket(rate=10.0, capacity=10)
    # Consume all tokens
    for _ in range(10):
        await bucket.consume()
    assert await bucket.consume() is False

    # Wait sufficient time for token replenishment (rate=10/s, waiting 0.2s should replenish 2 tokens)
    await asyncio.sleep(0.25)
    assert await bucket.consume() is True


@pytest.mark.asyncio
async def test_token_bucket_concurrent_safe():
    """Should not over-issue when consumed concurrently (thread-safe)"""
    bucket = _TokenBucket(rate=0.01, capacity=5)
    tasks = [bucket.consume() for _ in range(10)]
    results = await asyncio.gather(*tasks)
    # Maximum 5 allowed
    assert sum(results) == 5


# ── RateLimitMiddleware IP Extraction (Bug Fix Verification) ──────────────────────────────

@pytest.mark.asyncio
async def test_ip_extraction_from_x_forwarded_for():
    """X-Forwarded-For header should take priority over request.client.host"""
    middleware = RateLimitMiddleware(app=AsyncMock(), enabled=True)

    call_next = AsyncMock(return_value=MagicMock(status_code=200))
    request = MagicMock()
    request.url.path = "/leads"
    request.headers.get = lambda key, default="": \
        "10.0.0.1, 10.0.0.2" if key == "X-Forwarded-For" else default
    request.client = MagicMock()
    request.client.host = "172.0.0.1"

    await middleware.dispatch(request, call_next)

    # IP should be the first value of X-Forwarded-For, not request.client.host
    # Verification: confirm bucket key is ("10.0.0.1", "default")
    bucket_keys = list(middleware._buckets.keys())
    assert len(bucket_keys) == 1
    ip_used = bucket_keys[0][0]
    assert ip_used == "10.0.0.1", f"Should extract first IP from XFF, got: {ip_used}"


@pytest.mark.asyncio
async def test_ip_falls_back_to_client_host():
    """When no X-Forwarded-For, should use request.client.host"""
    middleware = RateLimitMiddleware(app=AsyncMock(), enabled=True)

    call_next = AsyncMock(return_value=MagicMock(status_code=200))
    request = MagicMock()
    request.url.path = "/leads"
    request.headers.get = lambda key, default="": default  # No XFF
    request.client = MagicMock()
    request.client.host = "192.168.1.100"

    await middleware.dispatch(request, call_next)

    bucket_keys = list(middleware._buckets.keys())
    ip_used = bucket_keys[0][0]
    assert ip_used == "192.168.1.100"


@pytest.mark.asyncio
async def test_ip_anonymous_when_no_client():
    """When request.client is None and no XFF, IP should be 'anonymous'"""
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


# ── Path Matching Rules ──────────────────────────────────────────────────────────────────────

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


# ── Exempt paths should not create buckets ─────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_exempt_paths_bypass_rate_limit():
    """Health check and documentation paths should not create buckets"""
    middleware = RateLimitMiddleware(app=AsyncMock(), enabled=True)
    call_next = AsyncMock(return_value=MagicMock(status_code=200))

    for exempt_path in ["/health", "/docs", "/redoc", "/openapi.json", "/stats"]:
        request = MagicMock()
        request.url.path = exempt_path
        request.headers.get = lambda k, d="": d
        request.client = None
        middleware._buckets.clear()
        await middleware.dispatch(request, call_next)
        assert len(middleware._buckets) == 0, f"{exempt_path} should not create a bucket"


@pytest.mark.asyncio
async def test_disabled_middleware_passes_all():
    """When enabled=False, all requests should pass through directly"""
    middleware = RateLimitMiddleware(app=AsyncMock(), enabled=False)
    call_next = AsyncMock(return_value=MagicMock(status_code=200))

    request = MagicMock()
    request.url.path = "/mine"
    request.headers.get = lambda k, d="": d
    request.client = None

    for _ in range(20):
        resp = await middleware.dispatch(request, call_next)
        assert resp.status_code == 200
