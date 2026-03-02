"""
统一线索数据模型 — Lead Mining System
所有 Miner 插件的输出契约（Pydantic v2）
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# 枚举：数据源标识
# ---------------------------------------------------------------------------

class LeadSource(str, Enum):
    SERPER          = "serper"
    SERPAPI         = "serpapi"
    OUTSCRAPER      = "outscraper"
    HUNTER          = "hunter"   # 替代 Apollo.io，使用 Hunter.io联系人富化
    APOLLO          = "apollo"    # 保留向后兼容，新安装不使用
    SEC_PH          = "sec_ph"
    GOOGLE_CSE      = "google_cse"
    REDDIT          = "reddit"
    PHILGEPS        = "philgeps"
    FACEBOOK        = "facebook"
    DTI_BNRS        = "dti_bnrs"
    YELLOW_PAGES    = "yellow_pages"
    GEMINI_ENRICHED = "gemini_enriched"


# ---------------------------------------------------------------------------
# 核心模型：LeadRaw
# ---------------------------------------------------------------------------

# 时区感知的当前 UTC 时间工厂（datetime.utcnow() 在 Python 3.12 已废弃）
def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class LeadRaw(BaseModel):
    """统一线索数据模型 — 所有 Miner 插件的输出契约"""

    model_config = {"populate_by_name": True}

    # ── 必填 ────────────────────────────────────────────────────────────────
    source: LeadSource
    business_name: str

    # ── 核心业务字段 ─────────────────────────────────────────────────────────
    industry_keyword: str = ""
    address: str = ""
    phone: str = ""
    website: str = ""
    email: str = ""

    # ── Google Maps 专属 ────────────────────────────────────────────────────
    rating: Optional[float] = None
    review_count: Optional[int] = None
    google_maps_url: str = ""
    lat: Optional[float] = None
    lng: Optional[float] = None

    # ── 扩展元数据（源特有字段，存 JSONB）───────────────────────────────────
    metadata: dict = Field(default_factory=dict)
    # 示例：
    #   Serper:      {"place_id": "...", "cid": "...", "category": "..."}
    #   Apollo:      {"apollo_org_id": "...", "industry": "...", "linkedin_url": "..."}
    #   SEC PH:      {"sec_registration_no": "...", "company_type": "...", "status": "Active"}
    #   Reddit:      {"subreddit": "phinvest", "post_url": "...", "post_score": 42}
    #   PhilGEPS:    {"philgeps_ref": "...", "category": "..."}
    #   Google CSE:  {"search_snippet": "...", "display_link": "..."}

    # ── 系统字段 ─────────────────────────────────────────────────────────────
    collected_at: datetime = Field(default_factory=_utcnow)
    enriched: bool = False
    score: Optional[float] = None

    # ── 方法 ─────────────────────────────────────────────────────────────────

    def dedup_key(self) -> str:
        """生成去重键：优先 phone → google_maps_url → name+address"""
        if self.phone:
            return f"phone:{self.phone.strip()}"
        if self.google_maps_url:
            return f"gmaps:{self.google_maps_url}"
        return f"name_addr:{self.business_name}:{self.address}"

    def to_chroma_document(self) -> str:
        """生成用于 ChromaDB 向量检索的文本表示"""
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
        """单行摘要，用于日志和调试"""
        return (
            f"[{self.source.value}] {self.business_name} | "
            f"{self.phone or 'no-phone'} | "
            f"{self.website or 'no-website'} | "
            f"score={self.score}"
        )


# ---------------------------------------------------------------------------
# 联系人模型：ContactLead（Apollo 富化输出）
# ---------------------------------------------------------------------------

class ContactLead(BaseModel):
    """联系人数据模型 — ApolloMiner 等联系人富化插件的输出"""

    lead_ref: str                           # 关联的 LeadRaw.dedup_key()
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
# 扩展模型：EnrichedLead（Gemini AI 富化后）
# ---------------------------------------------------------------------------

class EnrichedLead(LeadRaw):
    """AI 富化后的线索数据（LeadRaw 的超集）"""

    # ── AI 分析字段 ─────────────────────────────────────────────────────────
    industry_category: str = ""            # 行业分类，如 "Food & Beverage"
    business_size: str = ""                # "micro" | "small" | "medium"
    pain_points: list[str] = Field(default_factory=list)
    value_proposition: str = ""
    recommended_product: str = ""
    outreach_angle: str = ""               # 外展邮件切入角度
    score_reason: str = ""                 # 评分原因说明
    best_contact_time: str = ""            # 建议联系时间
    enriched_at: Optional[datetime] = None

    def to_outreach_context(self) -> str:
        """生成外展引擎使用的上下文字符串"""
        lines = [
            f"企业名称: {self.business_name}",
            f"行业: {self.industry_keyword}",
            f"地址: {self.address}",
            f"评分: {self.rating}（{self.review_count} 条评论）",
            f"网站: {self.website}",
            f"价值主张: {self.value_proposition}",
            f"外展切入点: {self.outreach_angle}",
            f"推荐产品/服务: {self.recommended_product}",
            f"潜在痛点: {', '.join(self.pain_points)}",
        ]
        return "\n".join(lines)
