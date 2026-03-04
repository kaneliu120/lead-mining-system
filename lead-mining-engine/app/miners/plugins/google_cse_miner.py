"""
GoogleCSEMiner — Phase 2 Search Engine Supplementary Data Plugin
Google Custom Search Engine API, 100 requests/day free
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import AsyncIterator, Optional

from app.miners.api_miner import APIBasedMiner
from app.miners.base import MinerConfig, MinerHealth
from app.models.lead import LeadRaw, LeadSource


@dataclass
class GoogleCSEConfig(MinerConfig):
    api_key: str = ""
    cx: str = ""                            # Custom Search Engine ID
    country: str = "countryPH"
    rate_limit_per_minute: int = 10         # Free quota: 100 requests/day


class GoogleCSEMiner(APIBasedMiner):
    """
    Google Custom Search Engine Miner.
    Supplements Philippines business directory data not covered by Serper.
    Can perform targeted searches on specific sites (e.g. yellowpages.ph).
    """

    def __init__(self, config: GoogleCSEConfig):
        super().__init__(
            config=config,
            api_key=config.api_key,
            base_url="https://www.googleapis.com",
        )
        self._cfg = config

    @property
    def source_name(self) -> LeadSource:
        return LeadSource.GOOGLE_CSE

    async def mine(
        self,
        keyword: str,
        location: str = "",
        lat: Optional[float] = None,
        lng: Optional[float] = None,
        limit: int = 50,
    ) -> AsyncIterator[LeadRaw]:
        query = f"{keyword} {location} Philippines contact".strip()
        start_index = 1
        collected = 0
        max_google_results = 100            # Google CSE returns at most 100 results

        while collected < limit and start_index <= max_google_results:
            batch = min(10, limit - collected)
            response = await self._request_with_retry(
                "GET",
                "/customsearch/v1",
                params={
                    "key":   self.api_key,
                    "cx":    self._cfg.cx,
                    "q":     query,
                    "cr":    self._cfg.country,
                    "num":   batch,
                    "start": start_index,
                },
            )
            data = response.json()
            items = data.get("items", [])
            if not items:
                break

            for item in items:
                if collected >= limit:
                    break
                snippet  = item.get("snippet", "")
                page_map = item.get("pagemap", {})
                orgs = page_map.get("organization") or page_map.get("localbusiness") or [{}]
                org  = orgs[0] if orgs else {}

                # Extract structured fields from JSON-LD / pagemap
                name = (
                    org.get("name")
                    or item.get("title", "").split(" - ")[0].split("|")[0].strip()[:200]
                )
                yield LeadRaw(
                    source=LeadSource.GOOGLE_CSE,
                    business_name=name,
                    industry_keyword=keyword,
                    website=item.get("link", ""),
                    phone=org.get("telephone") or org.get("phone", ""),
                    address=org.get("address") or org.get("streetAddress", ""),
                    email=org.get("email", ""),
                    metadata={
                        "search_snippet":  snippet[:400],
                        "display_link":    item.get("displayLink", ""),
                        "html_title":      item.get("htmlTitle", ""),
                        "image_url":       (
                            item.get("pagemap", {})
                                .get("cse_thumbnail", [{}])[0]
                                .get("src", "")
                        ),
                    },
                )
                collected += 1

            start_index += 10

    async def validate_config(self) -> bool:
        return bool(self._cfg.api_key and self._cfg.cx)

    async def health_check(self) -> MinerHealth:
        try:
            start = time.monotonic()
            await self._request_with_retry(
                "GET",
                "/customsearch/v1",
                params={"key": self.api_key, "cx": self._cfg.cx,
                        "q": "restaurant Philippines", "num": 1},
                retries=1,
            )
            latency = (time.monotonic() - start) * 1000
            return MinerHealth(healthy=True, message="OK", latency_ms=latency)
        except Exception as exc:
            return MinerHealth(healthy=False, message=str(exc))
