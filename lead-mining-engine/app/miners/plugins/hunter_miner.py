"""
HunterMiner — 联系人富化插件（替代 Apollo.io）
Hunter.io 免费层: 25 次 domain-search/月，无需信用卡注册
注册地址: https://hunter.io
API 文档: https://hunter.io/api-documentation

功能对应关系：
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
    # 免费层：25 次 domain-search/月
    # 基础付费 $34/月 = 500次，适合菲律宾 SME 规模


class HunterMiner(APIBasedMiner):
    """
    Hunter.io 联系人富化插件。

    mine() 逻辑：
      - 若 keyword 形如域名（含"."且无空格），则直接对该域名做 domain-search，
        返回该公司所有已知邮箱及对应联系人 → LeadRaw。
      - 否则不 yield（公司发现由 SerperMiner 负责）。

    enrich_contacts() 逻辑：
      - 对已发现的公司域名调用 /v2/domain-search，
        返回决策者邮箱列表 → List[ContactLead]。
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
        若 keyword 是域名格式（如 "example.com"），用 Hunter domain-search 发现联系人。
        否则静默跳过，由 SerperMiner 执行关键词发现。
        """
        # 只处理域名类型的关键词
        if "." not in keyword or " " in keyword:
            return  # 交给 Serper 处理普通关键词

        # 使用 urlparse 正确提取域名（lstrip 是逐字符剔除，会误删 h/t/p/s 开头的字符）
        kw = keyword.strip()
        parsed = urlparse(kw if kw.startswith("http") else f"https://{kw}")
        domain = parsed.netloc or parsed.path.split("/")[0]
        collected = 0
        offset = 0
        per_page = min(10, limit)   # Hunter 每页最多 100，但节省配额用小批次

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

            # 使用第一条邮箱作为公司代表联系人
            org_name = data.get("organization", "") or domain
            phone = data.get("phone_number", "") or ""

            # 合并为一个公司级 LeadRaw（附 emails 元数据）
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
            collected += 1  # Hunter domain-search 一个域名算一条 lead
            break           # 每个域名只取一条 LeadRaw

    # ── enrich_contacts() ────────────────────────────────────────────────────
    async def enrich_contacts(
        self,
        domain: str,
        limit: int = 5,
    ) -> List[ContactLead]:
        """
        根据企业域名查找决策者邮箱。
        调用 Hunter.io GET /v2/domain-search。
        """
        response = await self._request_with_retry(
            "GET",
            "/v2/domain-search",
            params={
                "domain":  domain,
                "api_key": self.api_key,
                "limit":   min(limit, 10),
                # Hunter 支持按职位过滤（逗号分隔）
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
        """通过 /v2/account 检测 API Key 有效性和剩余配额。"""
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
