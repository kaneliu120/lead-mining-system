"""
DTIBNRSMiner — Phase 3 菲律宾贸工部商号注册系统爬取插件
Department of Trade and Industry — Business Name Registration System

目标网站：https://bnrs.dti.gov.ph
数据内容：菲律宾合法注册商号、注册人地址、行业分类

数据质量说明：
  - 100% 合法注册企业（政府官方数据库）
  - 覆盖个体商户（sole proprietorship）和小企业
  - 含商号、注册人姓名、地址、业务描述
  - 无需 API Key，完全公开可访问

限制说明：
  - 每次搜索最多返回 20 条
  - 需 Playwright 渲染（动态表单）
  - 礼貌限速：3 req/min
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import AsyncIterator, Optional

from app.miners.browser_miner import BrowserBasedMiner
from app.miners.base import MinerConfig, MinerHealth
from app.models.lead import LeadRaw, LeadSource


@dataclass
class DTIBNRSConfig(MinerConfig):
    rate_limit_per_minute: int = 3        # 政府网站，极保守
    timeout_seconds: int = 60
    max_pages: int = 5                    # 每次搜索最多翻 5 页


class DTIBNRSMiner(BrowserBasedMiner):
    """
    菲律宾贸工部（DTI）商号注册系统爬取插件。

    搜索入口：https://bnrs.dti.gov.ph/web/guest/search
    功能：搜索已注册商号，筛选活跃状态（REGISTERED）企业。
    使用 Playwright 渲染 JavaScript 表单，自动填写关键词并抓取结果表格。

    字段映射：
      business_name   ← Business Name 列
      address         ← Address 列
      phone           ← （DTI BNRS 不公示手机，从其他来源富化）
      metadata        ← {owner_name, registration_no, status, business_type}
    """

    BASE_URL   = "https://bnrs.dti.gov.ph"
    SEARCH_URL = "https://bnrs.dti.gov.ph/web/guest/search"

    def __init__(self, config: DTIBNRSConfig):
        super().__init__(config)

    @property
    def source_name(self) -> LeadSource:
        return LeadSource.DTI_BNRS

    async def mine(
        self,
        keyword: str,
        location: str = "",
        lat: Optional[float] = None,
        lng: Optional[float] = None,
        limit: int = 100,
    ) -> AsyncIterator[LeadRaw]:
        context, page = await self._new_page()
        collected = 0

        try:
            # ── 打开搜索页面 ──────────────────────────────────────────────────
            await page.goto(
                self.SEARCH_URL,
                wait_until="networkidle",
                timeout=self.config.timeout_seconds * 1000,
            )
            await asyncio.sleep(2)

            # ── 填写商号搜索关键词 ────────────────────────────────────────────
            # DTI BNRS 搜索框的常见 ID / name 属性（多选择器兜底）
            kw_selectors = [
                'input[name*="businessName"]',
                'input[id*="businessName"]',
                'input[placeholder*="Business Name"]',
                'input[placeholder*="business name"]',
                '#businessNameSearch',
                'input[type="text"]:first-of-type',
            ]
            filled = False
            for sel in kw_selectors:
                try:
                    await page.fill(sel, keyword, timeout=5000)
                    filled = True
                    self.logger.debug(f"DTI BNRS: filled '{sel}' with '{keyword}'")
                    break
                except Exception:
                    continue

            if not filled:
                self.logger.warning("DTI BNRS: could not find keyword input, trying fallback")
                # 回退：直接构造 GET 查询 URL
                async for lead in self._mine_via_direct_url(keyword, location, limit, page):
                    yield lead
                    collected += 1
                return

            # ── 点击搜索按钮 ──────────────────────────────────────────────────
            btn_selectors = [
                'button[type="submit"]',
                'input[type="submit"]',
                'button:has-text("Search")',
                'button:has-text("SEARCH")',
                'a:has-text("Search")',
            ]
            for sel in btn_selectors:
                try:
                    await page.click(sel, timeout=5000)
                    break
                except Exception:
                    continue

            await page.wait_for_load_state("networkidle", timeout=30000)
            await asyncio.sleep(1)

            # ── 遍历分页结果 ──────────────────────────────────────────────────
            current_page = 0
            while collected < limit and current_page < self.config.max_pages:
                async for lead in self._parse_results_page(page, keyword):
                    if collected >= limit:
                        break
                    yield lead
                    collected += 1

                # 翻页
                next_btn = await page.query_selector(
                    "a:has-text('Next'), "
                    "a[aria-label='Next'], "
                    ".pagination li:last-child a:not(.disabled), "
                    "li.next a"
                )
                if not next_btn:
                    break

                await next_btn.click()
                await page.wait_for_load_state("networkidle", timeout=20000)
                await asyncio.sleep(2)
                current_page += 1

        except Exception as exc:
            self.logger.error(f"DTI BNRS mining failed: {exc}")
        finally:
            await context.close()

    async def _mine_via_direct_url(
        self,
        keyword: str,
        location: str,
        limit: int,
        page,
    ) -> AsyncIterator[LeadRaw]:
        """
        回退方案：尝试通过 URL 参数直接调用 BNRS API 端点（某些版本支持）。
        https://bnrs.dti.gov.ph/api/businesses/search?query=<keyword>&status=REGISTERED
        """
        import json
        api_url = (
            f"https://bnrs.dti.gov.ph/api/businesses/search"
            f"?query={keyword}&status=REGISTERED&size=20"
        )
        try:
            resp = await page.goto(api_url, wait_until="networkidle", timeout=30000)
            content = await page.content()
            # 尝试解析 JSON（API 端点返回 JSON）
            if resp and "application/json" in (resp.headers.get("content-type", "")):
                data = json.loads(await resp.text())
                items = data.get("content", data.get("data", []))
                for item in items[:limit]:
                    yield self._item_to_lead(item, keyword)
        except Exception as exc:
            self.logger.warning(f"DTI BNRS API fallback failed: {exc}")

    def _item_to_lead(self, item: dict, keyword: str) -> LeadRaw:
        """将 API JSON 对象转换为 LeadRaw。"""
        loc = item.get("address") or item.get("businessAddress") or {}
        if isinstance(loc, dict):
            address_parts = filter(None, [
                loc.get("streetNo", ""),
                loc.get("bldg", ""),
                loc.get("barangay", ""),
                loc.get("city", ""),
                loc.get("province", ""),
            ])
            address = ", ".join(address_parts)
        else:
            address = str(loc)

        return LeadRaw(
            source=LeadSource.DTI_BNRS,
            business_name=item.get("businessName", item.get("name", "")),
            industry_keyword=keyword,
            address=address,
            phone="",    # DTI BNRS 不公示手机号
            metadata={
                "owner_name":        item.get("ownerName", ""),
                "registration_no":   item.get("registrationNo", item.get("bnrsNo", "")),
                "status":            item.get("status", "REGISTERED"),
                "business_type":     item.get("businessType", "Sole Proprietorship"),
                "line_of_business":  item.get("lineOfBusiness", ""),
                "registration_date": item.get("registrationDate", ""),
            },
        )

    async def _parse_results_page(
        self,
        page,
        keyword: str,
    ) -> AsyncIterator[LeadRaw]:
        """
        解析当前页面的搜索结果表格，提取业务信息并 yield LeadRaw。
        兼容不同版本 BNRS 页面结构（table-based 和 list-based）。
        """
        # 方案 A：标准 HTML 表格
        rows = await page.query_selector_all(
            "table tbody tr, "
            ".search-results tr:not(:first-child), "
            ".results-table tr:not(.header)"
        )

        if rows:
            for row in rows:
                cells = await row.query_selector_all("td")
                if len(cells) < 2:
                    continue

                texts = []
                for cell in cells:
                    t = await cell.inner_text()
                    texts.append(t.strip())

                # 跳过表头行和空行
                biz_name = texts[0] if texts else ""
                if not biz_name or biz_name.lower() in (
                    "business name", "name", "", "no results found"
                ):
                    continue

                # 列顺序：[business_name, owner/reg_no, address, status, ...]
                owner_or_reg = texts[1] if len(texts) > 1 else ""
                address      = texts[2] if len(texts) > 2 else ""
                status       = texts[3] if len(texts) > 3 else ""

                # 过滤非活跃企业
                if status and status.upper() not in ("REGISTERED", "ACTIVE", ""):
                    continue

                yield LeadRaw(
                    source=LeadSource.DTI_BNRS,
                    business_name=biz_name,
                    industry_keyword=keyword,
                    address=address,
                    phone="",
                    metadata={
                        "owner_or_reg":  owner_or_reg,
                        "status":        status,
                        "raw_col_count": len(texts),
                    },
                )
            return

        # 方案 B：卡片式布局（新版 BNRS）
        cards = await page.query_selector_all(
            ".business-card, .result-card, .search-result-item"
        )
        for card in cards:
            name_el = await card.query_selector("h4, h3, .business-name, strong")
            addr_el = await card.query_selector(".address, .location, p:last-of-type")

            name = (await name_el.inner_text()).strip() if name_el else ""
            addr = (await addr_el.inner_text()).strip() if addr_el else ""

            if not name:
                continue

            yield LeadRaw(
                source=LeadSource.DTI_BNRS,
                business_name=name,
                industry_keyword=keyword,
                address=addr,
                phone="",
                metadata={"layout": "card"},
            )

    async def validate_config(self) -> bool:
        return True    # 无需配置，公开访问

    async def health_check(self) -> MinerHealth:
        try:
            context, page = await self._new_page()
            start = time.monotonic()
            resp = await page.goto(
                self.BASE_URL,
                wait_until="domcontentloaded",
                timeout=20000,
            )
            latency = (time.monotonic() - start) * 1000
            await context.close()
            ok = resp is not None and resp.ok
            return MinerHealth(
                healthy=ok,
                message="OK" if ok else f"HTTP {resp.status if resp else 'N/A'}",
                latency_ms=latency,
            )
        except Exception as exc:
            return MinerHealth(healthy=False, message=str(exc))
