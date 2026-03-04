"""
BrowserBasedMiner — Playwright-based crawler plugin base class
For data sources without an API (PhilGEPS, DTI BNRS, Yellow Pages, etc.)
"""
from __future__ import annotations

from typing import Optional

from app.miners.base import BaseMiner, MinerConfig


class BrowserBasedMiner(BaseMiner):
    """
    Playwright-based data source plugin base class.

    Must run with headless=True for Docker container compatibility.
    Subclasses obtain a new browser page via self._new_page().
    """

    def __init__(self, config: MinerConfig):
        super().__init__(config)
        self._browser = None
        self._playwright = None

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def on_startup(self) -> None:
        from playwright.async_api import async_playwright

        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(
            headless=True,                          # Docker requires headless
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

    # ── Utility methods ─────────────────────────────────────────────────────────

    async def _new_page(self):
        """
        Create a new browser page.

        Returns (context, page). Callers must close the context (closing context also closes all its pages):

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
