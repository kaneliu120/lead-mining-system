"""
Unified lead data model — Lead Mining System
Output contract for all Miner plugins (Pydantic v2)
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Enum: data source identifiers
# ---------------------------------------------------------------------------

class LeadSource(str, Enum):
    SERPER          = "serper"
    SERPAPI         = "serpapi"
    OUTSCRAPER      = "outscraper"
    HUNTER          = "hunter"   # Replaces Apollo.io — uses Hunter.io for contact enrichment
    APOLLO          = "apollo"    # Kept for backwards-compatibility, not used in new installs
    SEC_PH          = "sec_ph"
    GOOGLE_CSE      = "google_cse"
    REDDIT          = "reddit"
    PHILGEPS        = "philgeps"
    FACEBOOK        = "facebook"
    DTI_BNRS        = "dti_bnrs"
    YELLOW_PAGES    = "yellow_pages"
    GEMINI_ENRICHED = "gemini_enriched"


# ---------------------------------------------------------------------------
# Core model: LeadRaw
# ---------------------------------------------------------------------------

# Timezone-aware UTC now factory (datetime.utcnow() deprecated since Python 3.12)
def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class LeadRaw(BaseModel):
    """Unified lead data model — output contract for all Miner plugins"""

    model_config = {"populate_by_name": True}

    # ── Required ─────────────────────────────────────────────────────────────
    source: LeadSource
    business_name: str

    # ── Core business fields ─────────────────────────────────────────────────
    industry_keyword: str = ""
    address: str = ""
    phone: str = ""
    website: str = ""
    email: str = ""

    # ── Google Maps exclusive ──────────────────────────────────────────────────
    rating: Optional[float] = None
    review_count: Optional[int] = None
    google_maps_url: str = ""
    lat: Optional[float] = None
    lng: Optional[float] = None

    # ── Extended metadata (source-specific fields, stored as JSONB) ──────────
    metadata: dict = Field(default_factory=dict)
    # Examples:
    #   Serper:      {"place_id": "...", "cid": "...", "category": "..."}
    #   Apollo:      {"apollo_org_id": "...", "industry": "...", "linkedin_url": "..."}
    #   SEC PH:      {"sec_registration_no": "...", "company_type": "...", "status": "Active"}
    #   Reddit:      {"subreddit": "phinvest", "post_url": "...", "post_score": 42}
    #   PhilGEPS:    {"philgeps_ref": "...", "category": "..."}
    #   Google CSE:  {"search_snippet": "...", "display_link": "..."}

    # ── System fields ─────────────────────────────────────────────────────────
    collected_at: datetime = Field(default_factory=_utcnow)
    enriched: bool = False
    score: Optional[float] = None

    # ── Methods ──────────────────────────────────────────────────────────────

    def dedup_key(self) -> str:
        """Generate dedup key: priority phone → google_maps_url → name+address"""
        if self.phone:
            return f"phone:{self.phone.strip()}"
        if self.google_maps_url:
            return f"gmaps:{self.google_maps_url}"
        return f"name_addr:{self.business_name}:{self.address}"

    def to_chroma_document(self) -> str:
        """Generate text representation for ChromaDB vector retrieval"""
        parts = [
            f"Business: {self.business_name}",
            f"Industry: {self.industry_keyword}",
        ]
        if self.address:
            parts.append(f"Address: {self.address}")
        if self.phone:
            parts.append(f"Phone: {self.phone}")
        if self.website:
            parts.append(f"Website: {self.website}")
        if self.email:
            parts.append(f"Email: {self.email}")
        if self.rating is not None:
            parts.append(f"Rating: {self.rating} ({self.review_count or 0} reviews)")
        if self.metadata.get("nature_of_business"):
            parts.append(f"Nature: {self.metadata['nature_of_business']}")
        if self.metadata.get("industry"):
            parts.append(f"Industry: {self.metadata['industry']}")
        return "\n".join(parts)

    def to_summary(self) -> str:
        """Single-line summary for logging and debugging"""
        return (
            f"[{self.source.value}] {self.business_name} | "
            f"{self.phone or 'no-phone'} | "
            f"{self.website or 'no-website'} | "
            f"score={self.score}"
        )


# ---------------------------------------------------------------------------
# Contact model: ContactLead (Apollo enrichment output)
# ---------------------------------------------------------------------------

class ContactLead(BaseModel):
    """Contact data model — output of ApolloMiner and other contact enrichment plugins"""

    lead_ref: str                           # Reference to LeadRaw.dedup_key()
    full_name: str = ""
    job_title: str = ""
    email: str = ""
    email_verified: bool = False
    phone_direct: str = ""
    linkedin_url: str = ""
    company_size: str = ""                  # "1-10", "11-50", "51-200" ...
    source: LeadSource = LeadSource.HUNTER
    metadata: dict = Field(default_factory=dict)
    collected_at: datetime = Field(default_factory=_utcnow)


# ---------------------------------------------------------------------------
# Extended model: EnrichedLead (after Gemini AI enrichment)
# ---------------------------------------------------------------------------

class EnrichedLead(LeadRaw):
    """Lead data after AI enrichment (superset of LeadRaw)"""

    # ── AI analysis fields ──────────────────────────────────────────────────
    industry_category: str = ""            # Industry category, e.g. "Food & Beverage"
    business_size: str = ""                # "micro" | "small" | "medium"
    pain_points: list[str] = Field(default_factory=list)
    value_proposition: str = ""
    recommended_product: str = ""
    outreach_angle: str = ""               # Outreach email angle
    score_reason: str = ""                 # Score rationale
    best_contact_time: str = ""            # Recommended contact time
    enriched_at: Optional[datetime] = None

    def to_outreach_context(self) -> str:
        """Generate context string for the outreach engine"""
        lines = [
            f"Business Name: {self.business_name}",
            f"Industry: {self.industry_keyword}",
            f"Address: {self.address}",
            f"Rating: {self.rating} ({self.review_count} reviews)",
            f"Website: {self.website}",
            f"Value Proposition: {self.value_proposition}",
            f"Outreach Angle: {self.outreach_angle}",
            f"Recommended Product/Service: {self.recommended_product}",
            f"Pain Points: {', '.join(self.pain_points)}",
        ]
        return "\n".join(lines)
