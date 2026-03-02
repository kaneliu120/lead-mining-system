"""
GoogleCSEMiner — Phase 2 搜索引擎补充数据插件
Google Custom Search Engine API，100 次/天免费
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
    rate_limit_per_minute: int = 10         # 免费 100 次/天


class GoogleCSEMiner(APIBasedMiner):
    """
    Google Custom Search Engine Miner。
    补充 Serper 未覆盖的菲律宾商业目录数据。
    可针对特定网站（如 yellowpages.ph）进行定向搜索。
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
        max_google_results = 100            # Google CSE 最多返回 100 条

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

                # 从 JSON-LD / pagemap 中提取结构化字段
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
