"""
APIBasedMiner — 基于 HTTP API 的数据源插件基类
封装 httpx.AsyncClient 生命周期、速率限制、指数退避重试
"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

import httpx

from app.miners.base import BaseMiner, MinerConfig


class APIBasedMiner(BaseMiner):
    """
    基于 HTTP API 的数据源插件基类。

    子类通过 self.client 调用 API，
    通过 self._request_with_retry() 获得自动重试。
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
                response = await self.client.request(method, url, **kwargs)
                response.raise_for_status()
                return response
            except (
                httpx.HTTPStatusError,
                httpx.ConnectError,
                httpx.TimeoutException,      # 覆盖所有超时类型：ReadTimeout / ConnectTimeout / WriteTimeout / PoolTimeout
                httpx.RemoteProtocolError,
            ) as exc:
                last_error = exc
                if attempt < max_retries:
                    wait = 2 ** attempt          # 1 → 2 → 4 秒
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
