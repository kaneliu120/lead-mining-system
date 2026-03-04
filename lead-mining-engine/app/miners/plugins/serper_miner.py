"""
SerperMiner — Phase 1 Primary Business Data Plugin
Uses Serper google_maps API to replace SerpAPI, 90% cost reduction ($1/1K vs $10/1K)
"""
from __future__ import annotations

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
    Serper Google Maps API plugin.
    Documentation: https://serper.dev/
    Free quota: 2,500 calls/month
    Paid: $1/1K calls (1/10 the price of SerpAPI)
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
                break           # No more results
            page += 1

    async def validate_config(self) -> bool:
        return bool(self._cfg.api_key and len(self._cfg.api_key) > 10)

    async def health_check(self) -> MinerHealth:
        """
        Config-only check — avoids consuming API quota on every Docker
        health probe (which fires every 10 s).
        Use /mine to do a real connectivity test.
        """
        if not await self.validate_config():
            return MinerHealth(healthy=False, message="SERPER_API_KEY not configured")
        return MinerHealth(healthy=True, message="configured (key present)")

    @staticmethod
    def _build_maps_url(place: dict) -> str:
        place_id = place.get("placeId", "")
        if place_id:
            return f"https://www.google.com/maps/place/?q=place_id:{place_id}"
        return ""
