"""
ApolloMiner — Phase 1 联系人富化插件
Apollo.io 免费层 10K credits/月，270M+ 联系人数据库
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import AsyncIterator, List, Optional

from app.miners.api_miner import APIBasedMiner
from app.miners.base import MinerConfig, MinerHealth
from app.models.lead import ContactLead, LeadRaw, LeadSource


@dataclass
class ApolloConfig(MinerConfig):
    api_key: str = ""
    # 免费层：10K credits/月，每次 mixed_companies/search 消耗 1 credit


class ApolloMiner(APIBasedMiner):
    """
    Apollo.io 联系人富化插件。
    文档: https://apolloio.github.io/apollo-api-docs/
    免费层: 10K credits/月（足够菲律宾 SME 市场使用）
    """

    def __init__(self, config: ApolloConfig):
        super().__init__(
            config=config,
            api_key=config.api_key,
            base_url="https://api.apollo.io",
        )

    @property
    def source_name(self) -> LeadSource:
        return LeadSource.APOLLO

    async def mine(
        self,
        keyword: str,
        location: str = "",
        lat: Optional[float] = None,
        lng: Optional[float] = None,
        limit: int = 100,
    ) -> AsyncIterator[LeadRaw]:
        """通过组织关键词搜索返回公司级别 LeadRaw"""
        page = 1
        collected = 0
        per_page = min(25, limit)           # Apollo 单次最多 25 条

        while collected < limit:
            payload = {
                "api_key": self.api_key,
                "q_organization_keyword_tags": [keyword],
                "per_page": min(per_page, limit - collected),
                "page": page,
            }
            if location:
                payload["organization_locations"] = [location]

            response = await self._request_with_retry(
                "POST",
                "/v1/mixed_companies/search",
                headers={
                    "Content-Type": "application/json",
                    "Cache-Control": "no-cache",
                },
                json=payload,
            )
            data = response.json()
            orgs = data.get("organizations", [])
            if not orgs:
                break

            for org in orgs:
                if collected >= limit:
                    break
                website = org.get("website_url", "")
                # 清理 protocal prefix
                yield LeadRaw(
                    source=LeadSource.APOLLO,
                    business_name=org.get("name", ""),
                    industry_keyword=keyword,
                    website=website,
                    phone=org.get("phone", ""),
                    address=self._build_address(org),
                    metadata={
                        "apollo_org_id":    org.get("id", ""),
                        "industry":         org.get("industry", ""),
                        "employee_count":   org.get("estimated_num_employees"),
                        "linkedin_url":     org.get("linkedin_url", ""),
                        "founded_year":     org.get("founded_year"),
                        "technologies":     org.get("technologies", [])[:5],
                    },
                )
                collected += 1

            total_pages = data.get("pagination", {}).get("total_pages", 1)
            if page >= total_pages:
                break
            page += 1

    async def enrich_contacts(
        self,
        domain: str,
        limit: int = 5,
    ) -> List[ContactLead]:
        """
        根据企业域名查找联系人（决策者邮箱、职位、LinkedIn）。
        调用 /v1/mixed_people/search，消耗 credits。
        """
        response = await self._request_with_retry(
            "POST",
            "/v1/mixed_people/search",
            headers={
                "Content-Type": "application/json",
                "Cache-Control": "no-cache",
            },
            json={
                "api_key": self.api_key,
                "q_organization_domains": domain,
                "per_page": min(limit, 25),
                # 优先搜索 C-level / 决策者
                "person_titles": [
                    "CEO", "Founder", "Owner", "Manager",
                    "Director", "President", "VP",
                ],
            },
        )
        data = response.json()
        contacts: List[ContactLead] = []

        for person in data.get("people", []):
            org = person.get("organization") or {}
            contacts.append(
                ContactLead(
                    lead_ref=f"domain:{domain}",
                    full_name=person.get("name", ""),
                    job_title=person.get("title", ""),
                    email=person.get("email", ""),
                    email_verified=person.get("email_status") == "verified",
                    linkedin_url=person.get("linkedin_url", ""),
                    company_size=str(org.get("estimated_num_employees", "")),
                    source=LeadSource.APOLLO,
                    metadata={
                        "apollo_person_id": person.get("id", ""),
                        "city":             person.get("city", ""),
                        "country":          person.get("country", ""),
                    },
                )
            )
        return contacts

    async def validate_config(self) -> bool:
        return bool(self.api_key)

    async def health_check(self) -> MinerHealth:
        try:
            start = time.monotonic()
            await self._request_with_retry(
                "POST",
                "/v1/mixed_companies/search",
                headers={"Content-Type": "application/json"},
                json={"api_key": self.api_key, "per_page": 1,
                      "q_organization_keyword_tags": ["test"]},
                retries=1,
            )
            latency = (time.monotonic() - start) * 1000
            return MinerHealth(healthy=True, message="OK", latency_ms=latency)
        except Exception as exc:
            return MinerHealth(healthy=False, message=str(exc))

    @staticmethod
    def _build_address(org: dict) -> str:
        parts = [
            org.get("city", ""),
            org.get("state", ""),
            org.get("country", ""),
        ]
        return ", ".join(p for p in parts if p)
