"""
FacebookMiner — Phase 3 Facebook Business Page Data Mining Plugin
Uses Facebook Graph API v22.0 to search for Philippines SME business information

Data Quality Notes:
  - Covers businesses registered as Facebook Business Pages
  - Includes phone, website, email (if publicly set)
  - Requires a Facebook App + user access token

Prerequisites:
  1. Create an App at https://developers.facebook.com (type: Business)
  2. Enable permissions: pages_read_engagement, pages_search_engagement (after business verification)
  3. Generate a long-lived User Access Token
  4. Set the FACEBOOK_ACCESS_TOKEN environment variable
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import AsyncIterator, Optional

from app.miners.api_miner import APIBasedMiner
from app.miners.base import MinerConfig, MinerHealth
from app.models.lead import LeadRaw, LeadSource

# Metro Manila center coordinates (Mandaluyong)
_DEFAULT_LAT = 14.5794
_DEFAULT_LNG = 121.0359
_DEFAULT_RADIUS_M = 30000        # 30 km covers Metro Manila


@dataclass
class FacebookConfig(MinerConfig):
    api_key: str = ""             # access_token stored in api_key field
    api_version: str = "v22.0"
    radius_meters: int = _DEFAULT_RADIUS_M
    rate_limit_per_minute: int = 30     # Graph API has a relatively relaxed rate limit
    timeout_seconds: int = 20
    # Max results per page (Graph API limit: 100)
    page_limit: int = 50

    @property
    def access_token(self) -> str:
        return self.api_key


class FacebookMiner(APIBasedMiner):
    """
    Facebook Graph API v22.0 business location search plugin.
    
    Endpoint: GET /search
      type=place       — search physical places (businesses, institutions)
      q=<keyword>      — keyword
      center=lat,lng   — search center coordinates
      distance=<m>     — search radius
      fields=...       — list of requested fields
    
    Free quota: 200 requests/hour (user token)
    Cost: $0 (public search API is free; rate limit can be increased after Business verification)
    """

    GRAPH_BASE = "https://graph.facebook.com"

    # Requested fields list (get as much info as possible)
    _FIELDS = (
        "id,name,location,phone,website,emails,"
        "category_list,fan_count,overall_star_rating,rating_count"
    )

    def __init__(self, config: FacebookConfig):
        super().__init__(
            config=config,
            api_key=config.access_token,
            base_url=self.GRAPH_BASE,
        )
        self._cfg = config

    @property
    def source_name(self) -> LeadSource:
        return LeadSource.FACEBOOK

    async def mine(
        self,
        keyword: str,
        location: str = "",
        lat: Optional[float] = None,
        lng: Optional[float] = None,
        limit: int = 100,
    ) -> AsyncIterator[LeadRaw]:
        """Search Facebook for Philippines business pages matching the keyword."""
        center_lat = lat or _DEFAULT_LAT
        center_lng = lng or _DEFAULT_LNG
        collected = 0
        after_cursor: Optional[str] = None      # Pagination cursor

        while collected < limit:
            params = {
                "type":          "place",
                "q":             keyword,
                "center":        f"{center_lat},{center_lng}",
                "distance":      self._cfg.radius_meters,
                "fields":        self._FIELDS,
                "limit":         min(self._cfg.page_limit, limit - collected),
                "access_token":  self._cfg.access_token,
            }
            if after_cursor:
                params["after"] = after_cursor

            try:
                resp = await self._request_with_retry(
                    "GET",
                    f"/{self._cfg.api_version}/search",
                    params=params,
                )
                data = resp.json()
            except Exception as exc:
                self.logger.error(f"Facebook search failed: {exc}")
                break

            # Error handling (token expired, insufficient permissions, etc.)
            if "error" in data:
                err = data["error"]
                self.logger.error(
                    f"Facebook API error {err.get('code')}: {err.get('message')}"
                )
                break

            places = data.get("data", [])
            if not places:
                break

            for place in places:
                if collected >= limit:
                    break

                loc = place.get("location") or {}
                emails = place.get("emails") or []
                cats = place.get("category_list") or []
                cat_names = ", ".join(c.get("name", "") for c in cats)

                # Assemble address
                address_parts = filter(None, [
                    loc.get("street", ""),
                    loc.get("city", ""),
                    loc.get("state", ""),
                    loc.get("country", ""),
                ])
                address = ", ".join(address_parts)

                yield LeadRaw(
                    source=LeadSource.FACEBOOK,
                    business_name=place.get("name", ""),
                    industry_keyword=keyword,
                    address=address,
                    phone=place.get("phone", ""),
                    email=emails[0] if emails else "",
                    website=place.get("website", ""),
                    rating=float(place.get("overall_star_rating", 0)) or None,
                    review_count=place.get("rating_count"),  # Preserve None semantics (no rating data vs. 0 ratings)
                    metadata={
                        "facebook_id":    place.get("id", ""),
                        "category":       cat_names,
                        "fan_count":      place.get("fan_count", 0),
                        "latitude":       loc.get("latitude"),
                        "longitude":      loc.get("longitude"),
                        "zip":            loc.get("zip", ""),
                    },
                )
                collected += 1

            # Pagination cursor
            paging = data.get("paging", {})
            cursors = paging.get("cursors", {})
            after_cursor = cursors.get("after")
            if not after_cursor or not paging.get("next"):
                break

    async def validate_config(self) -> bool:
        return bool(self._cfg.access_token)

    async def health_check(self) -> MinerHealth:
        """Attempt to call /me endpoint to verify token validity."""
        if not self._cfg.access_token:
            return MinerHealth(
                healthy=False,
                message="FACEBOOK_ACCESS_TOKEN not set",
            )
        try:
            start = time.monotonic()
            resp = await self._request_with_retry(
                "GET",
                f"/{self._cfg.api_version}/me",
                params={"access_token": self._cfg.access_token, "fields": "id,name"},
                retries=1,
            )
            latency = (time.monotonic() - start) * 1000
            data = resp.json()
            if "error" in data:
                err = data["error"]
                return MinerHealth(
                    healthy=False,
                    message=f"Token error {err.get('code')}: {err.get('message', '')}",
                    latency_ms=latency,
                )
            return MinerHealth(
                healthy=True,
                message=f"OK — user: {data.get('name', data.get('id', '?'))}",
                latency_ms=latency,
            )
        except Exception as exc:
            return MinerHealth(healthy=False, message=str(exc))
