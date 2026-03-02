"""
SECPhilippinesMiner — Phase 2 政府数据插件
菲律宾证券交易委员会（SEC）9 个公开 API，完全免费
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import AsyncIterator, Optional

from app.miners.api_miner import APIBasedMiner
from app.miners.base import MinerConfig, MinerHealth
from app.models.lead import LeadRaw, LeadSource


@dataclass
class SECPhConfig(MinerConfig):
    # 无需 API Key，完全免费
    rate_limit_per_minute: int = 20         # 政府 API，保守限速
    timeout_seconds: int = 30


class SECPhilippinesMiner(APIBasedMiner):
    """
    菲律宾证券交易委员会（SEC）公开 API 插件。
    可获取：公司注册信息、类型、状态、地址、董事名单等。
    文档: https://services.sec.gov.ph/online/api/
    """

    def __init__(self, config: SECPhConfig):
        super().__init__(
            config=config,
            base_url="https://efiling.sec.gov.ph",
        )

    @property
    def source_name(self) -> LeadSource:
        return LeadSource.SEC_PH

    async def mine(
        self,
        keyword: str,
        location: str = "",
        lat: Optional[float] = None,
        lng: Optional[float] = None,
        limit: int = 100,
    ) -> AsyncIterator[LeadRaw]:
        """按公司名称关键词搜索 SEC 注册企业"""
        try:
            response = await self._request_with_retry(
                "GET",
                "/efs/api/online/seccsvdownloadreport",
                params={
                    "companyName": keyword,
                    "companyType": "",
                    "keyword":     keyword,
                },
            )
            data = response.json()
        except Exception as exc:
            self.logger.warning(f"SEC PH search failed for '{keyword}': {exc}")
            return

        # SEC API 返回格式：list 或 {"companyList": [...]}
        if isinstance(data, list):
            companies = data
        elif isinstance(data, dict):
            companies = (
                data.get("companyList")
                or data.get("results")
                or data.get("data")
                or []
            )
        else:
            return

        for company in companies[:limit]:
            sec_no = (
                company.get("secRegistrationNo")
                or company.get("secNo")
                or company.get("id", "")
            )

            # 尝试获取详细信息
            details: dict = {}
            if sec_no:
                try:
                    detail_resp = await self._request_with_retry(
                        "GET",
                        f"/efs/api/online/seccsvdownloadreport/{sec_no}",
                        retries=1,
                    )
                    details = detail_resp.json() or {}
                except Exception:
                    pass

            yield LeadRaw(
                source=LeadSource.SEC_PH,
                business_name=(
                    company.get("companyName")
                    or company.get("name", "Unknown")
                ),
                industry_keyword=keyword,
                address=(
                    details.get("businessAddress")
                    or company.get("address", "")
                ),
                metadata={
                    "sec_registration_no": sec_no,
                    "company_type":        company.get("companyType", ""),
                    "registration_date":   company.get("dateRegistered", ""),
                    "status":              company.get("status", ""),
                    "sec_status":          details.get("secStatus", ""),
                    "nature_of_business":  details.get("natureOfBusiness", ""),
                    "region":              company.get("region", ""),
                },
            )

    async def validate_config(self) -> bool:
        return True     # 无需 API Key

    async def health_check(self) -> MinerHealth:
        try:
            start = time.monotonic()
            await self._request_with_retry(
                "GET",
                "/efs/api/online/seccsvdownloadreport",
                params={"companyName": "test"},
                retries=1,
            )
            latency = (time.monotonic() - start) * 1000
            return MinerHealth(healthy=True, message="OK", latency_ms=latency)
        except Exception as exc:
            return MinerHealth(healthy=False, message=str(exc))
