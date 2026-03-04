"""
HunterMiner — Contact Enrichment Plugin (Apollo.io replacement)
Hunter.io free tier: 25 domain-search/month, no credit card registration required
Registration: https://hunter.io
API documentation: https://hunter.io/api-documentation

Feature mapping:
  Apollo.io enrich_contacts()  →  Hunter.io /v2/domain-search
  Apollo.io people search      →  Hunter.io /v2/email-finder
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import AsyncIterator, List, Optional
from urllib.parse import urlparse

from app.miners.api_miner import APIBasedMiner
from app.miners.base import MinerConfig, MinerHealth
from app.models.lead import ContactLead, LeadRaw, LeadSource


@dataclass
class HunterConfig(MinerConfig):
    api_key: str = ""
    # Free tier: 25 domain-search/month
    # Basic paid $34/month = 500 calls, suitable for Philippines SME scale


class HunterMiner(APIBasedMiner):
    """
    Hunter.io contact enrichment plugin.

    mine() logic:
      - If keyword is in domain format (contains "." and no spaces), directly perform domain-search on that domain,
        return all known emails and corresponding contacts for that company → LeadRaw.
      - Otherwise do not yield (company discovery is handled by SerperMiner).

    enrich_contacts() logic:
      - Call /v2/domain-search on discovered company domains,
        return decision-maker email list → List[ContactLead].
    """

    def __init__(self, config: HunterConfig):
        super().__init__(
            config=config,
            api_key=config.api_key,
            base_url="https://api.hunter.io",
        )

    @property
    def source_name(self) -> LeadSource:
        return LeadSource.HUNTER

    # ── mine() ───────────────────────────────────────────────────────────────
    async def mine(
        self,
        keyword: str,
        location: str = "",
        lat: Optional[float] = None,
        lng: Optional[float] = None,
        limit: int = 100,
    ) -> AsyncIterator[LeadRaw]:
        """
        If keyword is in domain format (e.g. "example.com"), use Hunter domain-search to discover contacts.
        Otherwise silently skip, let SerperMiner handle keyword discovery.
        """
        # Only process domain-type keywords
        if "." not in keyword or " " in keyword:
            return  # Hand off to Serper to process regular keywords

        # Use urlparse to correctly extract domain (lstrip removes characters one by one, which can incorrectly remove characters like h/t/p/s at the start)
        kw = keyword.strip()
        parsed = urlparse(kw if kw.startswith("http") else f"https://{kw}")
        domain = parsed.netloc or parsed.path.split("/")[0]
        collected = 0
        offset = 0
        per_page = min(10, limit)   # Hunter max 100 per page, but use small batches to save quota

        while collected < limit:
            response = await self._request_with_retry(
                "GET",
                "/v2/domain-search",
                params={
                    "domain": domain,
                    "api_key": self.api_key,
                    "limit": per_page,
                    "offset": offset,
                },
            )
            data = response.json().get("data", {})
            emails = data.get("emails", [])
            if not emails:
                break

            # Use first email as company representative contact
            org_name = data.get("organization", "") or domain
            phone = data.get("phone_number", "") or ""

            # Merge into one company-level LeadRaw (with emails metadata)
            yield LeadRaw(
                source=LeadSource.HUNTER,
                business_name=org_name,
                industry_keyword=keyword,
                website=f"https://{domain}",
                phone=phone,
                address="",
                metadata={
                    "domain":        domain,
                    "emails_found":  len(emails),
                    "decision_makers": [
                        {
                            "email":     e.get("value", ""),
                            "first_name": e.get("first_name", ""),
                            "last_name":  e.get("last_name", ""),
                            "position":   e.get("position", ""),
                            "linkedin":   e.get("linkedin", ""),
                            "confidence": e.get("confidence", 0),
                        }
                        for e in emails
                        if e.get("type") in ("personal", None)
                    ][:5],
                    "twitter":       data.get("twitter", ""),
                    "description":   data.get("description", ""),
                },
            )
            collected += 1  # Hunter domain-search counts one domain as one lead
            break           # Only retrieve one LeadRaw per domain

    # ── enrich_contacts() ────────────────────────────────────────────────────
    async def enrich_contacts(
        self,
        domain: str,
        limit: int = 5,
    ) -> List[ContactLead]:
        """
        Find decision-maker emails based on company domain.
        Calls Hunter.io GET /v2/domain-search.
        """
        response = await self._request_with_retry(
            "GET",
            "/v2/domain-search",
            params={
                "domain":  domain,
                "api_key": self.api_key,
                "limit":   min(limit, 10),
                # Hunter supports filtering by job title (comma-separated)
                "seniority": "senior,executive,director",
                "department": "executive,management",
            },
        )
        data = response.json().get("data", {})
        contacts: List[ContactLead] = []

        for person in data.get("emails", [])[:limit]:
            full_name = f"{person.get('first_name', '')} {person.get('last_name', '')}".strip()
            contacts.append(
                ContactLead(
                    lead_ref=f"domain:{domain}",
                    full_name=full_name,
                    job_title=person.get("position", ""),
                    email=person.get("value", ""),
                    email_verified=person.get("verification", {}).get("status") == "valid",
                    linkedin_url=person.get("linkedin", ""),
                    company_size="",
                    source=LeadSource.HUNTER,
                    metadata={
                        "confidence":    person.get("confidence", 0),
                        "email_type":    person.get("type", ""),
                        "first_name":    person.get("first_name", ""),
                        "last_name":     person.get("last_name", ""),
                        "phone_number":  person.get("phone_number", ""),
                        "twitter":       person.get("twitter", ""),
                        "seniority":     person.get("seniority", ""),
                        "department":    person.get("department", ""),
                    },
                )
            )
        return contacts

    async def validate_config(self) -> bool:
        return bool(self.api_key)

    async def health_check(self) -> MinerHealth:
        """Check API Key validity and remaining quota via /v2/account."""
        try:
            start = time.monotonic()
            resp = await self._request_with_retry(
                "GET",
                "/v2/account",
                params={"api_key": self.api_key},
                retries=1,
            )
            latency = (time.monotonic() - start) * 1000
            data = resp.json().get("data", {})
            calls_used = data.get("calls", {}).get("used", "?")
            calls_left = data.get("calls", {}).get("available", "?")
            return MinerHealth(
                healthy=True,
                message=f"OK — calls used: {calls_used}, remaining: {calls_left}",
                latency_ms=latency,
            )
        except Exception as exc:
            return MinerHealth(healthy=False, message=str(exc))
