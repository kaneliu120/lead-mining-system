"""
APIBasedMiner — 基于 HTTP API 的数据源插件基类
封装 httpx.AsyncClient 生命周期、速率限制、指数退避重试
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

import httpx

from app.miners.base import BaseMiner, MinerConfig


class _AsyncTokenBucket:
    """异步令牌桶速率限制器，确保 API 请求不超过配置的速率"""

    __slots__ = ("_rate", "_capacity", "_tokens", "_last_ts", "_lock")

    def __init__(self, rate_per_minute: int):
        self._rate = rate_per_minute / 60.0  # tokens/second
        self._capacity = max(rate_per_minute, 1)
        self._tokens = float(self._capacity)
        self._last_ts = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """等待直到获取一个令牌"""
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
            # 令牌不足，等待一小段时间后重试
            await asyncio.sleep(1.0 / max(self._rate, 0.1))


class APIBasedMiner(BaseMiner):
    """
    基于 HTTP API 的数据源插件基类。

    子类通过 self.client 调用 API，
    通过 self._request_with_retry() 获得自动重试。
    运行时自动执行 config.rate_limit_per_minute 定义的速率限制。
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

    # ── 生命周期 ──────────────────────────────────────────────────────────────

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

    # ── 属性 ─────────────────────────────────────────────────────────────────

    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError(
                f"{self.__class__.__name__}: client not initialized, "
                "call on_startup() first"
            )
        return self._client

    # ── 工具方法 ──────────────────────────────────────────────────────────────

    async def _request_with_retry(
        self,
        method: str,
        url: str,
        retries: Optional[int] = None,
        **kwargs,
    ) -> httpx.Response:
        """
        带指数退避重试的 HTTP 请求。

        重试触发条件：HTTP 4xx/5xx、连接错误、读取超时。
        指数退避：1s → 2s → 4s。
        """
        max_retries = retries if retries is not None else self.config.max_retries
        last_error: Exception = RuntimeError("No attempts made")

        for attempt in range(max_retries + 1):
            try:
                # 在发送请求前执行速率限制等待
                await self._rate_limiter.acquire()
                response = await self.client.request(method, url, **kwargs)
                response.raise_for_status()
                return response
            except httpx.HTTPStatusError as exc:
                last_error = exc
                status = exc.response.status_code
                # 4xx 客户端错误（429 除外）是永久性错误，不重试，立即上报
                if 400 <= status < 500 and status != 429:
                    hint = ""
                    if status == 403:
                        hint = (
                            " [Hint: Serper Maps 403 可能是 Maps API 未开通。"
                            "请登录 https://serper.dev 检查订阅计划及 API Key 权限]"
                        )
                    self.logger.error(
                        f"{self.__class__.__name__} HTTP {status} (不可重试): {exc}{hint}"
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
