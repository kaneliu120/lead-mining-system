"""
CRM Sync — Phase 3 Sales CRM Integration
Supports HubSpot CRM API v3 and Pipedrive API v1

Usage:
1. Set environment variables (see each Adapter's description)
2. Call POST /crm/sync to automatically push enriched leads
3. After successful outreach, automatically log contact activity in CRM

Environment variables:
  CRM_PROVIDER=hubspot|pipedrive|none  (default: none)
  HUBSPOT_ACCESS_TOKEN=<token>          (HubSpot Private App Token)
  PIPEDRIVE_API_TOKEN=<token>           (Pipedrive API Token)
  PIPEDRIVE_COMPANY_DOMAIN=<domain>     (e.g. mycompany.pipedrive.com)
"""
from __future__ import annotations

import logging
import os
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import httpx

logger = logging.getLogger(__name__)

# ── Environment variables ─────────────────────────────────────────────────────
CRM_PROVIDER             = os.environ.get("CRM_PROVIDER", "none").lower()
HUBSPOT_ACCESS_TOKEN     = os.environ.get("HUBSPOT_ACCESS_TOKEN", "")
PIPEDRIVE_API_TOKEN      = os.environ.get("PIPEDRIVE_API_TOKEN", "")
PIPEDRIVE_COMPANY_DOMAIN = os.environ.get("PIPEDRIVE_COMPANY_DOMAIN", "api.pipedrive.com")


# ─────────────────────────────────────────────────────────────────────────────
# Common CRM Lead data structure (aligned with LeadEnriched)
# ─────────────────────────────────────────────────────────────────────────────
class CRMLead:
    """Standardize CRM input format, abstracting away field differences between CRM platforms."""

    def __init__(
        self,
        lead_id: int,
        business_name: str,
        email: str = "",
        phone: str = "",
        address: str = "",
        website: str = "",
        industry: str = "",
        score: float = 0,
        contact_name: str = "",
        contact_title: str = "",
        pain_points: Optional[List[str]] = None,
        value_proposition: str = "",
        outreach_angle: str = "",
        source: str = "",
        extra: Optional[Dict[str, Any]] = None,
    ):
        self.lead_id           = lead_id
        self.business_name     = business_name
        self.email             = email
        self.phone             = phone
        self.address           = address
        self.website           = website
        self.industry          = industry
        self.score             = score
        self.contact_name      = contact_name
        self.contact_title     = contact_title
        self.pain_points       = pain_points or []
        self.value_proposition = value_proposition
        self.outreach_angle    = outreach_angle
        self.source            = source
        self.extra             = extra or {}


class CRMSyncResult:
    def __init__(
        self,
        success: bool,
        crm_id: str = "",
        crm_url: str = "",
        message: str = "",
    ):
        self.success = success
        self.crm_id  = crm_id
        self.crm_url = crm_url
        self.message = message

    def to_dict(self) -> Dict[str, Any]:
        return {
            "success": self.success,
            "crm_id":  self.crm_id,
            "crm_url": self.crm_url,
            "message": self.message,
        }


# ─────────────────────────────────────────────────────────────────────────────
# Abstract base class
# ─────────────────────────────────────────────────────────────────────────────
class BaseCRMAdapter(ABC):
    """Abstract base class for CRM adapters — enforces interface consistency."""

    @abstractmethod
    async def push_lead(self, lead: CRMLead) -> CRMSyncResult:
        """Push a single lead to the CRM (create or update company + contact)."""

    @abstractmethod
    async def log_activity(
        self,
        crm_id: str,
        subject: str,
        email_body: str,
        contact_email: str,
    ) -> CRMSyncResult:
        """Log an outreach email activity in the CRM (send record)."""

    @abstractmethod
    async def health_check(self) -> bool:
        """Check CRM API connectivity."""


# ─────────────────────────────────────────────────────────────────────────────
# HubSpot Adapter
# ─────────────────────────────────────────────────────────────────────────────
class HubSpotAdapter(BaseCRMAdapter):
    """
    HubSpot CRM API v3 adapter.

    Required permissions (Private App):
      crm.objects.contacts.write
      crm.objects.companies.write
      crm.objects.deals.write
      crm.objects.engagements.write
      crm.objects.associations.write

    Docs: https://developers.hubspot.com/docs/api/crm/contacts
    """

    BASE_URL = "https://api.hubapi.com"

    def __init__(self, access_token: str):
        self._token = access_token
        self._headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type":  "application/json",
        }

    async def push_lead(self, lead: CRMLead) -> CRMSyncResult:
        """Create/update Company first, then create/update Contact and associate."""
        async with httpx.AsyncClient(timeout=15) as client:

            # ── Step 1: Upsert Company ─────────────────────────────────────
            company_id = await self._upsert_company(client, lead)
            if not company_id:
                return CRMSyncResult(
                    success=False, message="Failed to create/update company in HubSpot"
                )

            # ── Step 2: Upsert Contact ─────────────────────────────────────
            if lead.email:
                contact_id = await self._upsert_contact(client, lead, company_id)
            else:
                contact_id = None

            crm_url = f"https://app.hubspot.com/contacts/{company_id}/company/{company_id}"
            return CRMSyncResult(
                success=True,
                crm_id=company_id,
                crm_url=crm_url,
                message=f"HubSpot: company {company_id}"
                + (f", contact {contact_id}" if contact_id else ""),
            )

    async def _upsert_company(self, client: httpx.AsyncClient, lead: CRMLead) -> Optional[str]:
        """Search for existing company (by name), create if not found."""
        # Search
        resp = await client.post(
            f"{self.BASE_URL}/crm/v3/objects/companies/search",
            headers=self._headers,
            json={
                "filterGroups": [{
                    "filters": [{
                        "propertyName": "name",
                        "operator":     "EQ",
                        "value":        lead.business_name,
                    }]
                }],
                "limit": 1,
            },
        )
        if resp.status_code == 200:
            results = resp.json().get("results", [])
            if results:
                return results[0]["id"]    # Already exists, return ID

        # Create
        properties = {
            "name":         lead.business_name,
            "phone":        lead.phone,
            "website":      lead.website,
            "address":      lead.address,
            "industry":     lead.industry,
            "description":  lead.value_proposition[:500] if lead.value_proposition else "",
            # Custom fields (must be pre-created in HubSpot)
            "lead_score":          str(int(lead.score)),
            "lead_source":         lead.source,
            "pain_points":         "; ".join(lead.pain_points[:3]),
            "outreach_angle":      lead.outreach_angle[:500] if lead.outreach_angle else "",
            "internal_lead_id":    str(lead.lead_id),
        }
        # Filter empty values (HubSpot rejects empty strings)
        properties = {k: v for k, v in properties.items() if v}

        resp = await client.post(
            f"{self.BASE_URL}/crm/v3/objects/companies",
            headers=self._headers,
            json={"properties": properties},
        )
        if resp.status_code in (200, 201):
            return resp.json().get("id")
        logger.error(f"HubSpot create company failed: {resp.status_code} {resp.text[:200]}")
        return None

    async def _upsert_contact(
        self,
        client: httpx.AsyncClient,
        lead: CRMLead,
        company_id: str,
    ) -> Optional[str]:
        """Create or update contact and associate with company."""
        # Split name
        name_parts = (lead.contact_name or "").split(" ", 1)
        first_name = name_parts[0]
        last_name  = name_parts[1] if len(name_parts) > 1 else ""

        properties = {
            "email":      lead.email,
            "firstname":  first_name,
            "lastname":   last_name,
            "jobtitle":   lead.contact_title,
            "phone":      lead.phone,
            "company":    lead.business_name,
        }
        properties = {k: v for k, v in properties.items() if v}

        # Attempt email-based upsert (HubSpot auto-deduplicates)
        resp = await client.patch(
            f"{self.BASE_URL}/crm/v3/objects/contacts/{quote(lead.email, safe='')}?idProperty=email",
            headers=self._headers,
            json={"properties": properties},
        )
        if resp.status_code == 404:
            # Does not exist, create
            resp = await client.post(
                f"{self.BASE_URL}/crm/v3/objects/contacts",
                headers=self._headers,
                json={"properties": properties},
            )
        if resp.status_code not in (200, 201):
            logger.warning(f"HubSpot contact upsert failed: {resp.status_code}")
            return None

        contact_id = resp.json().get("id")

        # Associate contact → company
        await client.put(
            f"{self.BASE_URL}/crm/v3/associations/contacts/{contact_id}"
            f"/companies/{company_id}/contact_to_company",
            headers=self._headers,
        )
        return contact_id

    async def log_activity(
        self,
        crm_id: str,
        subject: str,
        email_body: str,
        contact_email: str,
    ) -> CRMSyncResult:
        """Create an Email Engagement in HubSpot (outreach record)."""
        async with httpx.AsyncClient(timeout=15) as client:
            payload = {
                "engagement": {
                    "active": True,
                    "type":   "EMAIL",
                },
                "associations": {
                    "companyIds": [int(crm_id)],
                },
                "metadata": {
                    "subject":   subject,
                    "text":      email_body[:5000],
                    "to":        [{"email": contact_email}],
                    "status":    "SENT",
                },
            }
            resp = await client.post(
                f"{self.BASE_URL}/engagements/v1/engagements",
                headers=self._headers,
                json=payload,
            )
            if resp.status_code in (200, 201):
                eng_id = resp.json().get("engagement", {}).get("id", "")
                return CRMSyncResult(
                    success=True,
                    crm_id=str(eng_id),
                    message="HubSpot email engagement logged",
                )
            logger.warning(f"HubSpot log_activity failed: {resp.status_code}")
            return CRMSyncResult(success=False, message=f"Status {resp.status_code}")

    async def health_check(self) -> bool:
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                resp = await client.get(
                    f"{self.BASE_URL}/crm/v3/objects/contacts?limit=1",
                    headers=self._headers,
                )
                return resp.status_code == 200
        except Exception:
            return False


# ─────────────────────────────────────────────────────────────────────────────
# Pipedrive Adapter
# ─────────────────────────────────────────────────────────────────────────────
class PipedriveAdapter(BaseCRMAdapter):
    """
    Pipedrive API v1 adapter.

    Docs: https://developers.pipedrive.com/docs/api/v1
    Free tier (Essential): 14-day trial before billing + 3 free user seats (2024)

    Use personal API Token (Settings → API).
    Company domain from https://yourcompany.pipedrive.com.
    """

    def __init__(self, api_token: str, company_domain: str = "api.pipedrive.com"):
        # domain format: api.pipedrive.com or yourcompany.pipedrive.com
        base = company_domain if "pipedrive.com" in company_domain else f"{company_domain}.pipedrive.com"
        self._base = f"https://{base}/api/v1"
        self._token = api_token
        # Use Authorization header instead of URL param to prevent token leaking into logs/Referer
        self._headers = {"Authorization": f"Bearer {api_token}"}

    async def push_lead(self, lead: CRMLead) -> CRMSyncResult:
        """Create/update Organization (company) + Person (contact) + Lead."""
        async with httpx.AsyncClient(timeout=15) as client:

            # ── Step 1: Upsert Organization ────────────────────────────────
            org_id = await self._upsert_organization(client, lead)
            if not org_id:
                return CRMSyncResult(success=False, message="Failed to create org in Pipedrive")

            # ── Step 2: Upsert Person ──────────────────────────────────────
            person_id = None
            if lead.email or lead.contact_name:
                person_id = await self._upsert_person(client, lead, org_id)

            # ── Step 3: Create Lead ────────────────────────────────────────
            lead_pipedrive_id = await self._create_lead(client, lead, org_id, person_id)

            crm_url = f"https://{PIPEDRIVE_COMPANY_DOMAIN}/leads/list"
            return CRMSyncResult(
                success=True,
                crm_id=str(org_id),
                crm_url=crm_url,
                message=f"Pipedrive: org {org_id}"
                + (f", person {person_id}" if person_id else "")
                + (f", lead {lead_pipedrive_id}" if lead_pipedrive_id else ""),
            )

    async def _upsert_organization(self, client: httpx.AsyncClient, lead: CRMLead) -> Optional[int]:
        """Search for organization by name, create if not found."""
        # Search
        resp = await client.get(
            f"{self._base}/organizations/search",
            headers=self._headers,
            params={"term": lead.business_name, "exact_match": True, "limit": 1},
        )
        if resp.status_code == 200:
            items = resp.json().get("data", {}).get("items", [])
            if items:
                return items[0]["item"]["id"]

        # Create
        resp = await client.post(
            f"{self._base}/organizations",
            headers=self._headers,
            json={
                "name":    lead.business_name,
                "address": lead.address,
            },
        )
        if resp.status_code in (200, 201):
            return resp.json().get("data", {}).get("id")
        logger.error(f"Pipedrive create org failed: {resp.status_code} {resp.text[:200]}")
        return None

    async def _upsert_person(
        self,
        client: httpx.AsyncClient,
        lead: CRMLead,
        org_id: int,
    ) -> Optional[int]:
        """Search for contact by email, create if not found."""
        if lead.email:
            resp = await client.get(
                f"{self._base}/persons/search",
                headers=self._headers,
                params={"term": lead.email, "exact_match": True, "limit": 1},
            )
            if resp.status_code == 200:
                items = resp.json().get("data", {}).get("items", [])
                if items:
                    return items[0]["item"]["id"]

        payload: Dict[str, Any] = {
            "name":    lead.contact_name or lead.business_name,
            "org_id":  org_id,
        }
        if lead.email:
            payload["email"] = [{"value": lead.email, "primary": True}]
        if lead.phone:
            payload["phone"] = [{"value": lead.phone, "primary": True}]
        if lead.contact_title:
            payload["job_title"] = lead.contact_title

        resp = await client.post(
            f"{self._base}/persons",
            headers=self._headers,
            json=payload,
        )
        if resp.status_code in (200, 201):
            return resp.json().get("data", {}).get("id")
        logger.warning(f"Pipedrive create person failed: {resp.status_code}")
        return None

    async def _create_lead(
        self,
        client: httpx.AsyncClient,
        lead: CRMLead,
        org_id: int,
        person_id: Optional[int],
    ) -> Optional[str]:
        """Create a Pipedrive Lead (different from a Deal)."""
        payload: Dict[str, Any] = {
            "title":          f"{lead.business_name} — {lead.industry}",
            "organization_id": org_id,
            "expected_close_date": None,
        }
        if person_id:
            payload["person_id"] = person_id

        resp = await client.post(
            f"{self._base}/leads",
            headers=self._headers,
            json=payload,
        )
        if resp.status_code in (200, 201):
            data = resp.json().get("data", {})
            return str(data.get("id", ""))
        logger.warning(f"Pipedrive create lead failed: {resp.status_code}")
        return None

    async def log_activity(
        self,
        crm_id: str,
        subject: str,
        email_body: str,
        contact_email: str,
    ) -> CRMSyncResult:
        """Log an outreach Email Activity in Pipedrive."""
        async with httpx.AsyncClient(timeout=15) as client:
            payload = {
                "subject":   subject,
                "type":      "email",
                "note":      f"To: {contact_email}\n\n{email_body[:2000]}",
                "done":      1,
                "org_id":    int(crm_id) if crm_id.isdigit() else None,
            }
            payload = {k: v for k, v in payload.items() if v is not None}

            resp = await client.post(
                f"{self._base}/activities",
                headers=self._headers,
                json=payload,
            )
            if resp.status_code in (200, 201):
                act_id = resp.json().get("data", {}).get("id", "")
                return CRMSyncResult(
                    success=True,
                    crm_id=str(act_id),
                    message="Pipedrive activity logged",
                )
            logger.warning(f"Pipedrive log_activity failed: {resp.status_code}")
            return CRMSyncResult(success=False, message=f"Status {resp.status_code}")

    async def health_check(self) -> bool:
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                resp = await client.get(
                    f"{self._base}/users/me",
                    headers=self._headers,
                )
                return resp.status_code == 200
        except Exception:
            return False


# ─────────────────────────────────────────────────────────────────────────────
# Factory function: create the appropriate CRM Adapter based on environment variable
# ─────────────────────────────────────────────────────────────────────────────
def get_crm_adapter() -> Optional[BaseCRMAdapter]:
    """
    Return the appropriate adapter instance based on the CRM_PROVIDER environment variable.
    Returns None when CRM_PROVIDER=none (default), performing no CRM operations.
    """
    if CRM_PROVIDER == "hubspot":
        if not HUBSPOT_ACCESS_TOKEN:
            logger.warning("CRM_PROVIDER=hubspot but HUBSPOT_ACCESS_TOKEN is not set")
            return None
        return HubSpotAdapter(access_token=HUBSPOT_ACCESS_TOKEN)

    elif CRM_PROVIDER == "pipedrive":
        if not PIPEDRIVE_API_TOKEN:
            logger.warning("CRM_PROVIDER=pipedrive but PIPEDRIVE_API_TOKEN is not set")
            return None
        return PipedriveAdapter(
            api_token=PIPEDRIVE_API_TOKEN,
            company_domain=PIPEDRIVE_COMPANY_DOMAIN,
        )

    # CRM_PROVIDER=none or not set
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Batch sync utility function (for use by API endpoints)
# ─────────────────────────────────────────────────────────────────────────────
async def sync_leads_to_crm(
    leads: List[Dict[str, Any]],
    adapter: Optional[BaseCRMAdapter] = None,
) -> Dict[str, Any]:
    """
    Batch push leads_enriched leads to CRM.
    leads format: list of dicts returned from PostgreSQL leads_enriched query.

    Returns:
      {synced: int, skipped: int, failed: int, results: [...]}
    """
    if adapter is None:
        adapter = get_crm_adapter()
    if adapter is None:
        return {
            "synced": 0,
            "skipped": len(leads),
            "failed": 0,
            "message": "No CRM provider configured (CRM_PROVIDER=none)",
        }

    synced = skipped = failed = 0
    results = []

    for raw in leads:
        metadata = raw.get("metadata") or {}
        if isinstance(metadata, str):
            import json
            try:
                metadata = json.loads(metadata)
            except Exception:
                metadata = {}

        crm_lead = CRMLead(
            lead_id=raw.get("id", 0),
            business_name=raw.get("business_name", ""),
            email=raw.get("email", ""),
            phone=raw.get("phone", ""),
            address=raw.get("address", ""),
            website=raw.get("website", ""),
            industry=raw.get("industry_category") or raw.get("industry_keyword", ""),
            score=float(raw.get("score", 0)),
            contact_name=metadata.get("contact_name", ""),
            contact_title=metadata.get("contact_title", ""),
            pain_points=raw.get("pain_points") or [],
            value_proposition=raw.get("value_proposition", ""),
            outreach_angle=raw.get("outreach_angle", ""),
            source=raw.get("source", ""),
            extra=metadata,
        )

        try:
            result = await adapter.push_lead(crm_lead)
            if result.success:
                synced += 1
                results.append({
                    "lead_id": crm_lead.lead_id,
                    "status":  "synced",
                    "crm_id":  result.crm_id,
                    "message": result.message,
                })
            else:
                failed += 1
                results.append({
                    "lead_id": crm_lead.lead_id,
                    "status":  "failed",
                    "message": result.message,
                })
        except Exception as exc:
            logger.error(f"CRM sync failed for lead {crm_lead.lead_id}: {exc}")
            failed += 1
            results.append({
                "lead_id": crm_lead.lead_id,
                "status":  "error",
                "message": str(exc),
            })

    return {
        "synced":   synced,
        "skipped":  skipped,
        "failed":   failed,
        "provider": CRM_PROVIDER,
        "results":  results[:50],    # Limit returned records
    }
