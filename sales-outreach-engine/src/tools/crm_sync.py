"""
CRM Sync — Phase 3 销售 CRM 对接
支持 HubSpot CRM API v3 和 Pipedrive API v1

使用方式：
1. 设置环境变量（见各 Adapter 说明）
2. 调用 POST /crm/sync 自动推送富化线索
3. 外展成功后自动在 CRM 记录联系活动

环境变量：
  CRM_PROVIDER=hubspot|pipedrive|none  (默认: none)
  HUBSPOT_ACCESS_TOKEN=<token>          (HubSpot Private App Token)
  PIPEDRIVE_API_TOKEN=<token>           (Pipedrive API Token)
  PIPEDRIVE_COMPANY_DOMAIN=<domain>     (如 mycompany.pipedrive.com)
"""
from __future__ import annotations

import logging
import os
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import httpx

logger = logging.getLogger(__name__)

# ── 环境变量 ──────────────────────────────────────────────────────────────────
CRM_PROVIDER             = os.environ.get("CRM_PROVIDER", "none").lower()
HUBSPOT_ACCESS_TOKEN     = os.environ.get("HUBSPOT_ACCESS_TOKEN", "")
PIPEDRIVE_API_TOKEN      = os.environ.get("PIPEDRIVE_API_TOKEN", "")
PIPEDRIVE_COMPANY_DOMAIN = os.environ.get("PIPEDRIVE_COMPANY_DOMAIN", "api.pipedrive.com")


# ─────────────────────────────────────────────────────────────────────────────
# 通用 CRM Lead 数据结构（与 LeadEnriched 对齐）
# ─────────────────────────────────────────────────────────────────────────────
class CRMLead:
    """统一 CRM 输入格式，屏蔽不同 CRM 的字段差异。"""

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
# 抽象基类
# ─────────────────────────────────────────────────────────────────────────────
class BaseCRMAdapter(ABC):
    """CRM 适配器抽象基类，强约束接口一致性。"""

    @abstractmethod
    async def push_lead(self, lead: CRMLead) -> CRMSyncResult:
        """将单条线索推送到 CRM（创建或更新公司 + 联系人）。"""

    @abstractmethod
    async def log_activity(
        self,
        crm_id: str,
        subject: str,
        email_body: str,
        contact_email: str,
    ) -> CRMSyncResult:
        """在 CRM 中记录外展邮件活动（发送记录）。"""

    @abstractmethod
    async def health_check(self) -> bool:
        """检查 CRM API 连通性。"""


# ─────────────────────────────────────────────────────────────────────────────
# HubSpot Adapter
# ─────────────────────────────────────────────────────────────────────────────
class HubSpotAdapter(BaseCRMAdapter):
    """
    HubSpot CRM API v3 适配器。

    所需权限（Private App）：
      crm.objects.contacts.write
      crm.objects.companies.write
      crm.objects.deals.write
      crm.objects.engagements.write
      crm.objects.associations.write

    文档：https://developers.hubspot.com/docs/api/crm/contacts
    """

    BASE_URL = "https://api.hubapi.com"

    def __init__(self, access_token: str):
        self._token = access_token
        self._headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type":  "application/json",
        }

    async def push_lead(self, lead: CRMLead) -> CRMSyncResult:
        """先创建/更新 Company，再创建/更新 Contact 并关联。"""
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
        """搜索已有公司（按 name），不存在则创建。"""
        # 搜索
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
                return results[0]["id"]    # 已存在，返回 ID

        # 创建
        properties = {
            "name":         lead.business_name,
            "phone":        lead.phone,
            "website":      lead.website,
            "address":      lead.address,
            "industry":     lead.industry,
            "description":  lead.value_proposition[:500] if lead.value_proposition else "",
            # 自定义字段（需在 HubSpot 中预先创建）
            "lead_score":          str(int(lead.score)),
            "lead_source":         lead.source,
            "pain_points":         "; ".join(lead.pain_points[:3]),
            "outreach_angle":      lead.outreach_angle[:500] if lead.outreach_angle else "",
            "internal_lead_id":    str(lead.lead_id),
        }
        # 过滤空值（HubSpot 拒绝空字符串）
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
        """创建或更新联系人，并关联到公司。"""
        # 拆分姓名
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

        # 尝试按 email upsert（HubSpot 会自动去重）
        resp = await client.patch(
            f"{self.BASE_URL}/crm/v3/objects/contacts/{quote(lead.email, safe='')}?idProperty=email",
            headers=self._headers,
            json={"properties": properties},
        )
        if resp.status_code == 404:
            # 不存在，创建
            resp = await client.post(
                f"{self.BASE_URL}/crm/v3/objects/contacts",
                headers=self._headers,
                json={"properties": properties},
            )
        if resp.status_code not in (200, 201):
            logger.warning(f"HubSpot contact upsert failed: {resp.status_code}")
            return None

        contact_id = resp.json().get("id")

        # 关联联系人 → 公司
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
        """在 HubSpot 创建 Email Engagement（外展记录）。"""
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
    Pipedrive API v1 适配器。

    文档：https://developers.pipedrive.com/docs/api/v1
    免费层（Essential）：开票前 14 天试用 + 3 用户免费席位（2024）

    使用个人 API Token（Settings → API）。
    公司域名可从 https://yourcompany.pipedrive.com 获取。
    """

    def __init__(self, api_token: str, company_domain: str = "api.pipedrive.com"):
        # domain 格式：api.pipedrive.com 或 yourcompany.pipedrive.com
        base = company_domain if "pipedrive.com" in company_domain else f"{company_domain}.pipedrive.com"
        self._base = f"https://{base}/api/v1"
        self._token = api_token
        self._params = {"api_token": api_token}    # 所有请求附加此参数

    async def push_lead(self, lead: CRMLead) -> CRMSyncResult:
        """创建/更新 Organization（公司）+ Person（联系人）+ Lead（线索）。"""
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
        """按 name 搜索组织，不存在则创建。"""
        # 搜索
        resp = await client.get(
            f"{self._base}/organizations/search",
            params={**self._params, "term": lead.business_name, "exact_match": True, "limit": 1},
        )
        if resp.status_code == 200:
            items = resp.json().get("data", {}).get("items", [])
            if items:
                return items[0]["item"]["id"]

        # 创建
        resp = await client.post(
            f"{self._base}/organizations",
            params=self._params,
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
        """按 email 搜索联系人，不存在则创建。"""
        if lead.email:
            resp = await client.get(
                f"{self._base}/persons/search",
                params={**self._params, "term": lead.email, "exact_match": True, "limit": 1},
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
            params=self._params,
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
        """创建 Pipedrive Lead（线索，不同于 Deal）。"""
        payload: Dict[str, Any] = {
            "title":          f"{lead.business_name} — {lead.industry}",
            "organization_id": org_id,
            "expected_close_date": None,
        }
        if person_id:
            payload["person_id"] = person_id

        resp = await client.post(
            f"{self._base}/leads",
            params=self._params,
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
        """在 Pipedrive 记录外展 Email Activity。"""
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
                params=self._params,
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
                    params=self._params,
                )
                return resp.status_code == 200
        except Exception:
            return False


# ─────────────────────────────────────────────────────────────────────────────
# 工厂函数：根据环境变量创建对应的 CRM Adapter
# ─────────────────────────────────────────────────────────────────────────────
def get_crm_adapter() -> Optional[BaseCRMAdapter]:
    """
    根据 CRM_PROVIDER 环境变量返回对应适配器实例。
    CRM_PROVIDER=none（默认）时返回 None，不执行任何 CRM 操作。
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

    # CRM_PROVIDER=none 或未设置
    return None


# ─────────────────────────────────────────────────────────────────────────────
# 批量同步工具函数（供 API 端点调用）
# ─────────────────────────────────────────────────────────────────────────────
async def sync_leads_to_crm(
    leads: List[Dict[str, Any]],
    adapter: Optional[BaseCRMAdapter] = None,
) -> Dict[str, Any]:
    """
    批量将 leads_enriched 线索推送到 CRM。
    leads 格式：从 PostgreSQL leads_enriched 查询返回的字典列表。

    返回：
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
        "results":  results[:50],    # 限制返回条数
    }
