"""
DTIBNRSMiner — Phase 3 Philippines Department of Trade and Industry Business Name Registration System Mining Plugin
Department of Trade and Industry — Business Name Registration System

Target website: https://bnrs.dti.gov.ph
Data content: Philippines legally registered business names, registrant address, industry classification

Data quality notes:
  - 100% legally registered businesses (official government database)
  - Covers sole proprietorships and small businesses
  - Includes business name, registrant name, address, business description
  - No API Key required, fully publicly accessible

Limitations:
  - Maximum 20 results per search
  - Requires Playwright rendering (dynamic forms)
  - Polite rate limiting: 3 req/min
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import AsyncIterator, Optional

from app.miners.browser_miner import BrowserBasedMiner
from app.miners.base import MinerConfig, MinerHealth
from app.models.lead import LeadRaw, LeadSource


@dataclass
class DTIBNRSConfig(MinerConfig):
    rate_limit_per_minute: int = 3        # Government website, extremely conservative
    timeout_seconds: int = 60
    max_pages: int = 5                    # Maximum 5 pages per search


class DTIBNRSMiner(BrowserBasedMiner):
    """
    Philippines Department of Trade and Industry (DTI) Business Name Registration System mining plugin.

    Search entry: https://bnrs.dti.gov.ph/web/guest/search
    Function: Search registered business names, filter active status (REGISTERED) businesses.
    Uses Playwright to render JavaScript forms, automatically fills in keywords and scrapes result tables.

    Field mapping:
      business_name   ← Business Name column
      address         ← Address column
      phone           ← (DTI BNRS does not expose phone numbers, enrich from other sources)
      metadata        ← {owner_name, registration_no, status, business_type}
    """

    BASE_URL   = "https://bnrs.dti.gov.ph"
    SEARCH_URL = "https://bnrs.dti.gov.ph/web/guest/search"

    def __init__(self, config: DTIBNRSConfig):
        super().__init__(config)

    @property
    def source_name(self) -> LeadSource:
        return LeadSource.DTI_BNRS

    async def mine(
        self,
        keyword: str,
        location: str = "",
        lat: Optional[float] = None,
        lng: Optional[float] = None,
        limit: int = 100,
    ) -> AsyncIterator[LeadRaw]:
        context, page = await self._new_page()
        collected = 0

        try:
            # ── Open search page ──────────────────────────────────────────────────
            await page.goto(
                self.SEARCH_URL,
                wait_until="networkidle",
                timeout=self.config.timeout_seconds * 1000,
            )
            await asyncio.sleep(2)

            # ── Fill in business name search keyword ────────────────────────────────────────────
            # Common ID / name attributes for DTI BNRS search box (multiple selectors as fallback)
            kw_selectors = [
                'input[name*="businessName"]',
                'input[id*="businessName"]',
                'input[placeholder*="Business Name"]',
                'input[placeholder*="business name"]',
                '#businessNameSearch',
                'input[type="text"]:first-of-type',
            ]
            filled = False
            for sel in kw_selectors:
                try:
                    await page.fill(sel, keyword, timeout=5000)
                    filled = True
                    self.logger.debug(f"DTI BNRS: filled '{sel}' with '{keyword}'")
                    break
                except Exception:
                    continue

            if not filled:
                self.logger.warning("DTI BNRS: could not find keyword input, trying fallback")
                # Fallback: construct GET query URL directly
                async for lead in self._mine_via_direct_url(keyword, location, limit, page):
                    yield lead
                    collected += 1
                return

            # ── Click search button ──────────────────────────────────────────────────
            btn_selectors = [
                'button[type="submit"]',
                'input[type="submit"]',
                'button:has-text("Search")',
                'button:has-text("SEARCH")',
                'a:has-text("Search")',
            ]
            for sel in btn_selectors:
                try:
                    await page.click(sel, timeout=5000)
                    break
                except Exception:
                    continue

            await page.wait_for_load_state("networkidle", timeout=30000)
            await asyncio.sleep(1)

            # ── Iterate paginated results ──────────────────────────────────────────────────
            current_page = 0
            while collected < limit and current_page < self.config.max_pages:
                async for lead in self._parse_results_page(page, keyword):
                    if collected >= limit:
                        break
                    yield lead
                    collected += 1

                # Next page
                next_btn = await page.query_selector(
                    "a:has-text('Next'), "
                    "a[aria-label='Next'], "
                    ".pagination li:last-child a:not(.disabled), "
                    "li.next a"
                )
                if not next_btn:
                    break

                await next_btn.click()
                await page.wait_for_load_state("networkidle", timeout=20000)
                await asyncio.sleep(2)
                current_page += 1

        except Exception as exc:
            self.logger.error(f"DTI BNRS mining failed: {exc}")
        finally:
            await context.close()

    async def _mine_via_direct_url(
        self,
        keyword: str,
        location: str,
        limit: int,
        page,
    ) -> AsyncIterator[LeadRaw]:
        """
        Fallback: attempt to directly call BNRS API endpoint via URL parameters (supported by some versions).
        https://bnrs.dti.gov.ph/api/businesses/search?query=<keyword>&status=REGISTERED
        """
        import json
        api_url = (
            f"https://bnrs.dti.gov.ph/api/businesses/search"
            f"?query={keyword}&status=REGISTERED&size=20"
        )
        try:
            resp = await page.goto(api_url, wait_until="networkidle", timeout=30000)
            content = await page.content()
            # Attempt to parse JSON (API endpoint returns JSON)
            if resp and "application/json" in (resp.headers.get("content-type", "")):
                data = json.loads(await resp.text())
                items = data.get("content", data.get("data", []))
                for item in items[:limit]:
                    yield self._item_to_lead(item, keyword)
        except Exception as exc:
            self.logger.warning(f"DTI BNRS API fallback failed: {exc}")

    def _item_to_lead(self, item: dict, keyword: str) -> LeadRaw:
        """Convert API JSON object to LeadRaw."""
        loc = item.get("address") or item.get("businessAddress") or {}
        if isinstance(loc, dict):
            address_parts = filter(None, [
                loc.get("streetNo", ""),
                loc.get("bldg", ""),
                loc.get("barangay", ""),
                loc.get("city", ""),
                loc.get("province", ""),
            ])
            address = ", ".join(address_parts)
        else:
            address = str(loc)

        return LeadRaw(
            source=LeadSource.DTI_BNRS,
            business_name=item.get("businessName", item.get("name", "")),
            industry_keyword=keyword,
            address=address,
            phone="",    # DTI BNRS does not expose phone numbers
            metadata={
                "owner_name":        item.get("ownerName", ""),
                "registration_no":   item.get("registrationNo", item.get("bnrsNo", "")),
                "status":            item.get("status", "REGISTERED"),
                "business_type":     item.get("businessType", "Sole Proprietorship"),
                "line_of_business":  item.get("lineOfBusiness", ""),
                "registration_date": item.get("registrationDate", ""),
            },
        )

    async def _parse_results_page(
        self,
        page,
        keyword: str,
    ) -> AsyncIterator[LeadRaw]:
        """
        Parse search results table on current page, extract business information and yield LeadRaw.
        Compatible with different versions of BNRS page structure (table-based and list-based).
        """
        # Option A: Standard HTML table
        rows = await page.query_selector_all(
            "table tbody tr, "
            ".search-results tr:not(:first-child), "
            ".results-table tr:not(.header)"
        )

        if rows:
            for row in rows:
                cells = await row.query_selector_all("td")
                if len(cells) < 2:
                    continue

                texts = []
                for cell in cells:
                    t = await cell.inner_text()
                    texts.append(t.strip())

                # Skip header rows and empty rows
                biz_name = texts[0] if texts else ""
                if not biz_name or biz_name.lower() in (
                    "business name", "name", "", "no results found"
                ):
                    continue

                # Column order: [business_name, owner/reg_no, address, status, ...]
                owner_or_reg = texts[1] if len(texts) > 1 else ""
                address      = texts[2] if len(texts) > 2 else ""
                status       = texts[3] if len(texts) > 3 else ""

                # Filter inactive businesses
                if status and status.upper() not in ("REGISTERED", "ACTIVE", ""):
                    continue

                yield LeadRaw(
                    source=LeadSource.DTI_BNRS,
                    business_name=biz_name,
                    industry_keyword=keyword,
                    address=address,
                    phone="",
                    metadata={
                        "owner_or_reg":  owner_or_reg,
                        "status":        status,
                        "raw_col_count": len(texts),
                    },
                )
            return

        # Option B: Card layout (new BNRS version)
        cards = await page.query_selector_all(
            ".business-card, .result-card, .search-result-item"
        )
        for card in cards:
            name_el = await card.query_selector("h4, h3, .business-name, strong")
            addr_el = await card.query_selector(".address, .location, p:last-of-type")

            name = (await name_el.inner_text()).strip() if name_el else ""
            addr = (await addr_el.inner_text()).strip() if addr_el else ""

            if not name:
                continue

            yield LeadRaw(
                source=LeadSource.DTI_BNRS,
                business_name=name,
                industry_keyword=keyword,
                address=addr,
                phone="",
                metadata={"layout": "card"},
            )

    async def validate_config(self) -> bool:
        return True    # No configuration needed, publicly accessible

    async def health_check(self) -> MinerHealth:
        try:
            context, page = await self._new_page()
            start = time.monotonic()
            resp = await page.goto(
                self.BASE_URL,
                wait_until="domcontentloaded",
                timeout=20000,
            )
            latency = (time.monotonic() - start) * 1000
            await context.close()
            ok = resp is not None and resp.ok
            return MinerHealth(
                healthy=ok,
                message="OK" if ok else f"HTTP {resp.status if resp else 'N/A'}",
                latency_ms=latency,
            )
        except Exception as exc:
            return MinerHealth(healthy=False, message=str(exc))
