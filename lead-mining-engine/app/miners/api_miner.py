"""
APIBasedMiner — HTTP-API-based data source plugin base class
Wraps httpx.AsyncClient lifecycle, rate limiting, exponential backoff retry
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

import httpx

from app.miners.base import BaseMiner, MinerConfig


class _AsyncTokenBucket:
    """Async token bucket rate limiter, ensures API requests do not exceed configured rate"""

    __slots__ = ("_rate", "_capacity", "_tokens", "_last_ts", "_lock")

    def __init__(self, rate_per_minute: int):
        self._rate = rate_per_minute / 60.0  # tokens/second
        self._capacity = max(rate_per_minute, 1)
        self._tokens = float(self._capacity)
        self._last_ts = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Wait until a token is acquired"""
        while True:
            async with self._lock:
                now = time.monotonic()
                self._tokens = min(
                    self._capacity,
                    self._tokens + (now - self._last_ts) * self._rate,
                )
                self._last_ts = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
            # Insufficient tokens — wait briefly and retry
            await asyncio.sleep(1.0 / max(self._rate, 0.1))


class APIBasedMiner(BaseMiner):
    """
    HTTP API-based data source plugin base class.

    Subclasses call the API via self.client,
    and get automatic retry via self._request_with_retry().
    Automatically enforces the rate limit defined in config.rate_limit_per_minute at runtime.
    """

    def __init__(
        self,
        config: MinerConfig,
        api_key: str = "",
        base_url: str = "",
    ):
        super().__init__(config)
        self.api_key = api_key
        self.base_url = base_url
        self._client: Optional[httpx.AsyncClient] = None
        self._rate_limiter = _AsyncTokenBucket(config.rate_limit_per_minute)

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def on_startup(self) -> None:
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=httpx.Timeout(self.config.timeout_seconds),
            limits=httpx.Limits(
                max_connections=10,
                max_keepalive_connections=5,
            ),
        )
        self.logger.info(
            f"{self.__class__.__name__} HTTP client initialized "
            f"(base_url={self.base_url!r})"
        )

    async def on_shutdown(self) -> None:
        if self._client:
            await self._client.aclose()
            self.logger.info(f"{self.__class__.__name__} HTTP client closed")

    # ── Properties ──────────────────────────────────────────────────────────────

    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError(
                f"{self.__class__.__name__}: client not initialized, "
                "call on_startup() first"
            )
        return self._client

    # ── Utility methods ─────────────────────────────────────────────────────────

    async def _request_with_retry(
        self,
        method: str,
        url: str,
        retries: Optional[int] = None,
        **kwargs,
    ) -> httpx.Response:
        """
        HTTP request with exponential backoff retry.

        Retry trigger conditions: HTTP 4xx/5xx, connection error, read timeout.
        Exponential backoff: 1s → 2s → 4s.
        """
        max_retries = retries if retries is not None else self.config.max_retries
        last_error: Exception = RuntimeError("No attempts made")

        for attempt in range(max_retries + 1):
            try:
                # Execute rate limit wait before sending request
                await self._rate_limiter.acquire()
                response = await self.client.request(method, url, **kwargs)
                response.raise_for_status()
                return response
            except httpx.HTTPStatusError as exc:
                last_error = exc
                status = exc.response.status_code
                # 4xx client errors (except 429) are permanent — do not retry, report immediately
                if 400 <= status < 500 and status != 429:
                    hint = ""
                    if status == 403:
                        hint = (
                            " [Hint: Serper Maps 403 may mean the Maps API is not enabled."
                            " Please log in to https://serper.dev and check your subscription plan and API Key permissions]"
                        )
                    self.logger.error(
                        f"{self.__class__.__name__} HTTP {status} (non-retryable): {exc}{hint}"
                    )
                    raise
                if attempt < max_retries:
                    wait = 2 ** attempt          # 1s → 2s → 4s
                    self.logger.warning(
                        f"{self.__class__.__name__} request failed "
                        f"(attempt {attempt + 1}/{max_retries + 1}), "
                        f"retry in {wait}s: {exc}"
                    )
                    await asyncio.sleep(wait)
                else:
                    self.logger.error(
                        f"{self.__class__.__name__} all retries exhausted: {exc}"
                    )
            except (
                httpx.ConnectError,
                httpx.TimeoutException,      # ReadTimeout / ConnectTimeout / WriteTimeout / PoolTimeout
                httpx.RemoteProtocolError,
            ) as exc:
                last_error = exc
                if attempt < max_retries:
                    wait = 2 ** attempt          # 1s → 2s → 4s
                    self.logger.warning(
                        f"{self.__class__.__name__} request failed "
                        f"(attempt {attempt + 1}/{max_retries + 1}), "
                        f"retry in {wait}s: {exc}"
                    )
                    await asyncio.sleep(wait)
                else:
                    self.logger.error(
                        f"{self.__class__.__name__} all retries exhausted: {exc}"
                    )

        raise last_error
