"""
PhilGEPSMiner — Phase 3 Government Procurement System Mining Plugin
Philippine Government Electronic Procurement System
High supplier data quality: includes name, address, contact information, all legally operating businesses
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
class PhilGEPSConfig(MinerConfig):
    rate_limit_per_minute: int = 5          # Government website, extremely conservative rate limiting
    timeout_seconds: int = 60


class PhilGEPSMiner(BrowserBasedMiner):
    """
    Philippines Government Electronic Procurement System (PhilGEPS) mining plugin.
    Entry point: https://www.philgeps.gov.ph
    Mine registered supplier directory, highest data quality (all legally registered businesses).
    """

    BASE_URL = "https://www.philgeps.gov.ph"

    def __init__(self, config: PhilGEPSConfig):
        super().__init__(config)

    @property
    def source_name(self) -> LeadSource:
        return LeadSource.PHILGEPS

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
            # ── Navigate to supplier search page ────────────────────────────────────────
            await page.goto(
                f"{self.BASE_URL}/GEPS/Central/SearchBuyer/tabid/80/Default.aspx",
                wait_until="networkidle",
                timeout=self.config.timeout_seconds * 1000,
            )
            await asyncio.sleep(1)

            # ── Fill in search form ────────────────────────────────────────────────
            kw_selectors = [
                'input[id*="Keyword"]',
                'input[name*="keyword"]',
                'input[id*="txtKeyword"]',
                '#ctl00_ContentPlaceHolder1_txtKeyword',
            ]
            for sel in kw_selectors:
                try:
                    await page.fill(sel, keyword)
                    break
                except Exception:
                    continue

            btn_selectors = [
                'input[value="Search"]',
                'input[id*="btnSearch"]',
                'button:has-text("Search")',
            ]
            for sel in btn_selectors:
                try:
                    await page.click(sel)
                    break
                except Exception:
                    continue

            await page.wait_for_load_state("networkidle", timeout=30000)

            # ── Iterate paginated results ────────────────────────────────────────────────
            while collected < limit:
                rows = await page.query_selector_all(
                    "table tr:not(:first-child), "
                    ".grid-results tr:not(:first-child)"
                )

                if not rows:
                    self.logger.info(f"PhilGEPS: no more rows on current page")
                    break

                for row in rows:
                    if collected >= limit:
                        break
                    cells = await row.query_selector_all("td")
                    if len(cells) < 2:
                        continue

                    texts = []
                    for cell in cells:
                        t = await cell.inner_text()
                        texts.append(t.strip())

                    name = texts[0] if texts else ""
                    if not name or name.lower() in ("company name", "name", ""):
                        continue

                    yield LeadRaw(
                        source=LeadSource.PHILGEPS,
                        business_name=name,
                        industry_keyword=keyword,
                        address=texts[2] if len(texts) > 2 else "",
                        phone=texts[3] if len(texts) > 3 else "",
                        metadata={
                            "philgeps_ref":  texts[1] if len(texts) > 1 else "",
                            "category":      keyword,
                            "raw_row_count": len(texts),
                        },
                    )
                    collected += 1

                # ── Next page ────────────────────────────────────────────────────
                next_btn = await page.query_selector(
                    "a:has-text('Next'), "
                    "a[title='Next page'], "
                    ".pager-next a"
                )
                if not next_btn:
                    break

                await next_btn.click()
                await page.wait_for_load_state("networkidle", timeout=20000)
                await asyncio.sleep(2)          # Polite delay

        except Exception as exc:
            self.logger.error(f"PhilGEPS mining failed: {exc}")
        finally:
            await context.close()

    async def validate_config(self) -> bool:
        return True                             # No configuration needed

    async def health_check(self) -> MinerHealth:
        try:
            context, page = await self._new_page()
            start = time.monotonic()
            resp = await page.goto(self.BASE_URL, wait_until="domcontentloaded",
                                   timeout=20000)
            latency = (time.monotonic() - start) * 1000
            await context.close()
            ok = resp is not None and resp.ok
            return MinerHealth(healthy=ok, message="OK" if ok else "non-2xx",
                               latency_ms=latency)
        except Exception as exc:
            return MinerHealth(healthy=False, message=str(exc))
