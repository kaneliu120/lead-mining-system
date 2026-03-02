"""
YellowPagesMiner — Phase 2 菲律宾黄页爬虫插件
从 www.yellowpages.ph 抓取商家名称、电话、地址和网站（Playwright）
"""
from __future__ import annotations

import asyncio
import re
import time
from dataclasses import dataclass
from typing import AsyncIterator, Optional

from app.miners.browser_miner import BrowserBasedMiner
from app.miners.base import MinerConfig, MinerHealth
from app.models.lead import LeadRaw, LeadSource

_BASE_URL = "https://www.yellowpages.ph"

# 各字段可能的 CSS 选择器（多候选，增强鲁棒性）
_ITEM_SELECTORS = [
    "div.listing-item",
    ".company-item",
    "article.business-listing",
    ".biz-listing-item",
    ".search-result",
    ".sp-default-result",
]
_NAME_SELECTORS = [
    "h2.listing-name", "h2.company-name", "h3.biz-name",
    "a.listing-name", ".listing-title", "h2 a",
]
_PHONE_SELECTORS = [
    "span.phone", ".telephone", ".contact-phone",
    "a[href^='tel:']", ".phone-number",
]
_ADDRESS_SELECTORS = [
    "span.address", ".street-address", ".biz-address",
    ".listing-address", "address",
]
_CATEGORY_SELECTORS = [
    "span.category", ".listing-category", ".business-category",
    ".categories", "span.categories",
]


@dataclass
class YellowPagesConfig(MinerConfig):
    base_url:              str = _BASE_URL
    timeout_seconds:       int = 30
    rate_limit_per_minute: int = 10         # 礼貌爬取，避免 IP 被封
    page_delay_seconds:    float = 2.5       # 翻页间隔


class YellowPagesMiner(BrowserBasedMiner):
    """
    菲律宾黄页（www.yellowpages.ph）爬虫插件。

    基于 Playwright headless 浏览器采集商家信息：
    - 商家名称、电话、地址
    - 行业分类
    - 官网 URL（如有）

    注意：无需 API Key，但需遵守网站 robots.txt 和合理使用频率。
    """

    def __init__(self, config: YellowPagesConfig):
        super().__init__(config)
        self._cfg = config

    @property
    def source_name(self) -> LeadSource:
        return LeadSource.YELLOW_PAGES

    async def mine(
        self,
        keyword: str,
        location: str = "",
        lat: Optional[float] = None,
        lng: Optional[float] = None,
        limit: int = 50,
    ) -> AsyncIterator[LeadRaw]:
        context, page = await self._new_page()
        try:
            page_num  = 1
            collected = 0

            while collected < limit:
                # 构建搜索 URL
                kw_enc  = keyword.replace(" ", "+")
                loc_enc = location.replace(" ", "+") if location else ""
                url = f"{self._cfg.base_url}/search?keyword={kw_enc}"
                if loc_enc:
                    url += f"&location={loc_enc}"
                if page_num > 1:
                    url += f"&page={page_num}"

                self.logger.info(f"YellowPages: fetching page {page_num} → {url}")

                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=self._cfg.timeout_seconds * 1000)
                    await page.wait_for_timeout(2000)  # 等待 JS 渲染
                except Exception as e:
                    self.logger.warning(f"YellowPages: navigation failed: {e}")
                    break

                # 等待任意匹配的商家列表容器出现
                found_container = False
                for sel in _ITEM_SELECTORS:
                    try:
                        await page.wait_for_selector(sel, timeout=8000)
                        found_container = True
                        break
                    except Exception:
                        continue

                if not found_container:
                    self.logger.info(
                        f"YellowPages: no listing containers found for "
                        f"'{keyword}' page {page_num}"
                    )
                    break

                # 抓取所有商家条目
                items = await page.query_selector_all(
                    ", ".join(_ITEM_SELECTORS)
                )
                if not items:
                    break

                for item in items:
                    if collected >= limit:
                        break

                    name     = await self._first_text(item, _NAME_SELECTORS)
                    phone    = await self._first_text(item, _PHONE_SELECTORS)
                    address  = await self._first_text(item, _ADDRESS_SELECTORS)
                    category = await self._first_text(item, _CATEGORY_SELECTORS)

                    # 清洗电话号码
                    if phone:
                        phone = re.sub(r"[^\d\+\-\(\) ]", "", phone)[:20].strip()
                    # tel: 链接中直接取号码
                    if not phone:
                        tel_el = await item.query_selector("a[href^='tel:']")
                        if tel_el:
                            href = await tel_el.get_attribute("href") or ""
                            phone = href.replace("tel:", "").strip()[:20]

                    # 邮箱（mailto: 链接）
                    email = ""
                    mailto_el = await item.query_selector("a[href^='mailto:']")
                    if mailto_el:
                        href = await mailto_el.get_attribute("href") or ""
                        email = href.replace("mailto:", "").split("?")[0].strip()

                    # 官网链接（排除 yellowpages.ph 自身）
                    website = ""
                    for ws in await item.query_selector_all("a[href^='http']"):
                        href = await ws.get_attribute("href") or ""
                        if (
                            "yellowpages.ph" not in href
                            and "google.com" not in href
                            and "facebook.com" not in href
                        ):
                            website = href.split("?")[0][:300]
                            break

                    if not name or name.lower() in ("yellow pages", "yp", "home"):
                        continue

                    yield LeadRaw(
                        source=LeadSource.YELLOW_PAGES,
                        business_name=name[:200],
                        industry_keyword=keyword,
                        phone=phone,
                        address=address[:500] if address else "",
                        email=email,
                        website=website,
                        metadata={
                            "category":   category,
                            "source_url": url,
                            "page":       page_num,
                        },
                    )
                    collected += 1

                # 判断是否有下一页
                if not await self._has_next_page(page):
                    break

                page_num += 1
                # 礼貌间隔
                await asyncio.sleep(self._cfg.page_delay_seconds)

        finally:
            await context.close()

    # ── 辅助方法 ───────────────────────────────────────────────────────────────

    @staticmethod
    async def _first_text(element, selectors: list[str]) -> str:
        """依次尝试多个 CSS 选择器，返回第一个非空文本"""
        for sel in selectors:
            try:
                el = await element.query_selector(sel)
                if el:
                    t = (await el.inner_text()).strip()
                    if t:
                        return t
            except Exception:
                continue
        return ""

    @staticmethod
    async def _has_next_page(page) -> bool:
        """检测是否存在（且未被禁用的）下一页按钮"""
        try:
            for sel in [
                "a.next-page:not(.disabled)",
                "a[aria-label='Next']:not([disabled])",
                ".pagination .next:not(.disabled)",
                "li.next:not(.disabled) a",
            ]:
                el = await page.query_selector(sel)
                if el:
                    return True
        except Exception:
            pass
        return False

    async def validate_config(self) -> bool:
        return True     # 无需 API Key

    async def health_check(self) -> MinerHealth:
        context, page = await self._new_page()
        try:
            start = time.monotonic()
            await page.goto(_BASE_URL, wait_until="domcontentloaded",
                            timeout=self._cfg.timeout_seconds * 1000)
            latency = (time.monotonic() - start) * 1000
            return MinerHealth(healthy=True, message="OK", latency_ms=latency)
        except Exception as exc:
            return MinerHealth(healthy=False, message=str(exc))
        finally:
            await context.close()
