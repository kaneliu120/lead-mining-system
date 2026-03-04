"""
YellowPagesMiner — Phase 2 Philippines Yellow Pages scraping plugin
Scrapes business name, phone, address and website from www.yellowpages.ph (Playwright)
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

# Possible CSS selectors for each field (multiple candidates, enhanced robustness)
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
    rate_limit_per_minute: int = 10         # Polite scraping, avoid IP being banned
    page_delay_seconds:    float = 2.5       # Page turn interval


class YellowPagesMiner(BrowserBasedMiner):
    """
    Philippines Yellow Pages (www.yellowpages.ph) scraping plugin.

    Mines business information using Playwright headless browser:
    - Business name, phone, address
    - Industry classification
    - Website URL (if available)

    Note: No API Key required, but must comply with website robots.txt and reasonable usage frequency.
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
                # Build search URL
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
                    await page.wait_for_timeout(2000)  # Wait for JS rendering
                except Exception as e:
                    self.logger.warning(f"YellowPages: navigation failed: {e}")
                    break

                # Wait for any matching business listing container to appear
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

                # Scrape all business entries
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

                    # Clean phone number
                    if phone:
                        phone = re.sub(r"[^\d\+\-\(\) ]", "", phone)[:20].strip()
                    # Extract number directly from tel: link
                    if not phone:
                        tel_el = await item.query_selector("a[href^='tel:']")
                        if tel_el:
                            href = await tel_el.get_attribute("href") or ""
                            phone = href.replace("tel:", "").strip()[:20]

                    # Email (mailto: link)
                    email = ""
                    mailto_el = await item.query_selector("a[href^='mailto:']")
                    if mailto_el:
                        href = await mailto_el.get_attribute("href") or ""
                        email = href.replace("mailto:", "").split("?")[0].strip()

                    # Website link (excluding yellowpages.ph itself)
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

                # Check if there is a next page
                if not await self._has_next_page(page):
                    break

                page_num += 1
                # Polite interval
                await asyncio.sleep(self._cfg.page_delay_seconds)

        finally:
            await context.close()

    # ── Helper methods ───────────────────────────────────────────────────────────────

    @staticmethod
    async def _first_text(element, selectors: list[str]) -> str:
        """Try multiple CSS selectors in order, return first non-empty text"""
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
        """Detect if a (non-disabled) next page button exists"""
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
        return True     # No API Key required

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
