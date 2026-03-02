"""
SerperMiner — Phase 1 主力商户数据插件
用 Serper google_maps API 替代 SerpAPI，成本降低 90%（$1/1K vs $10/1K）
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import AsyncIterator, Optional

from app.miners.api_miner import APIBasedMiner
from app.miners.base import MinerConfig, MinerHealth
from app.models.lead import LeadRaw, LeadSource


@dataclass
class SerperConfig(MinerConfig):
    api_key: str = ""
    country_code: str = "ph"
    language: str = "en"
    results_per_page: int = 20


class SerperMiner(APIBasedMiner):
    """
    Serper Google Maps API 插件。
    文档: https://serper.dev/
    免费额度: 2,500 次/月
    付费: $1/1K 次（SerpAPI 的 1/10 价格）
    """

    def __init__(self, config: SerperConfig):
        super().__init__(
            config=config,
            api_key=config.api_key,
            base_url="https://google.serper.dev",
        )
        self._cfg = config

    @property
    def source_name(self) -> LeadSource:
        return LeadSource.SERPER

    async def mine(
        self,
        keyword: str,
        location: str = "",
        lat: Optional[float] = None,
        lng: Optional[float] = None,
        limit: int = 100,
    ) -> AsyncIterator[LeadRaw]:
        query = f"{keyword} {location}".strip()
        collected = 0
        page = 1

        while collected < limit:
            payload: dict = {
                "q": query,
                "gl": self._cfg.country_code,
                "hl": self._cfg.language,
                "num": min(self._cfg.results_per_page, limit - collected),
            }
            if page > 1:
                payload["page"] = page

            response = await self._request_with_retry(
                "POST",
                "/maps",
                headers={
                    "X-API-KEY": self.api_key,
                    "Content-Type": "application/json",
                },
                json=payload,
            )
            data = response.json()
            places = data.get("places", [])
            if not places:
                break

            for place in places:
                if collected >= limit:
                    break
                yield LeadRaw(
                    source=LeadSource.SERPER,
                    business_name=place.get("title", ""),
                    industry_keyword=keyword,
                    address=place.get("address", ""),
                    phone=place.get("phoneNumber") or place.get("phone", ""),
                    website=place.get("website", ""),
                    rating=place.get("rating"),
                    review_count=place.get("ratingCount") or place.get("reviews"),
                    lat=place.get("latitude") or place.get("gps_coordinates", {}).get("latitude"),
                    lng=place.get("longitude") or place.get("gps_coordinates", {}).get("longitude"),
                    google_maps_url=self._build_maps_url(place),
                    metadata={
                        "place_id":   place.get("placeId", ""),
                        "cid":        place.get("cid", ""),
                        "category":   place.get("category", ""),
                        "thumbnail":  place.get("thumbnailUrl", ""),
                        "open_state": place.get("openState", ""),
                    },
                )
                collected += 1

            if len(places) < self._cfg.results_per_page:
                break           # 没有更多结果了
            page += 1

    async def validate_config(self) -> bool:
        return bool(self._cfg.api_key and len(self._cfg.api_key) > 10)

    async def health_check(self) -> MinerHealth:
        try:
            start = time.monotonic()
            await self._request_with_retry(
                "POST",
                "/maps",
                headers={"X-API-KEY": self.api_key, "Content-Type": "application/json"},
                json={"q": "test restaurant", "gl": "ph", "num": 1},
                retries=1,
            )
            latency = (time.monotonic() - start) * 1000
            return MinerHealth(healthy=True, message="OK", latency_ms=latency)
        except Exception as exc:
            return MinerHealth(healthy=False, message=str(exc))

    @staticmethod
    def _build_maps_url(place: dict) -> str:
        place_id = place.get("placeId", "")
        if place_id:
            return f"https://www.google.com/maps/place/?q=place_id:{place_id}"
        return ""
