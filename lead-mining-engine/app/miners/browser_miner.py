"""
BrowserBasedMiner — 基于 Playwright 的爬虫插件基类
用于没有 API 的数据源（PhilGEPS、DTI BNRS、黄页等）
"""
from __future__ import annotations

from typing import Optional

from app.miners.base import BaseMiner, MinerConfig


class BrowserBasedMiner(BaseMiner):
    """
    基于 Playwright 的数据源插件基类。

    必须以 headless=True 运行，兼容 Docker 容器环境。
    子类通过 self._new_page() 获取新的浏览器页面。
    """

    def __init__(self, config: MinerConfig):
        super().__init__(config)
        self._browser = None
        self._playwright = None

    # ── 生命周期 ──────────────────────────────────────────────────────────────

    async def on_startup(self) -> None:
        from playwright.async_api import async_playwright

        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(
            headless=True,                          # Docker 必须 headless
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--disable-setuid-sandbox",
            ],
        )
        self.logger.info(f"{self.__class__.__name__} browser launched (headless)")

    async def on_shutdown(self) -> None:
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()
        self.logger.info(f"{self.__class__.__name__} browser closed")

    # ── 工具方法 ──────────────────────────────────────────────────────────────

    async def _new_page(self):
        """
        创建新的浏览器页面。

        返回 (context, page)调用方注意关闭 context（关闭 context 会同时关闭其下所有 page）：

            context, page = await self._new_page()
            try:
                ...
            finally:
                await context.close()
        """
        if self._browser is None:
            raise RuntimeError(
                f"{self.__class__.__name__}: browser not started, "
                "call on_startup() first"
            )
        context = await self._browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/121.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 720},
            java_script_enabled=True,
        )
        page = await context.new_page()
        page.set_default_timeout(self.config.timeout_seconds * 1000)
        return context, page
