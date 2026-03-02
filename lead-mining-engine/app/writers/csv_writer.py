"""
CsvWriter — CSV 导出工具
轻量无依赖的 CSV 写入，适合批量数据导出供销售团队使用
"""
from __future__ import annotations

import csv
import io
import logging
from pathlib import Path
from typing import List, Union

from app.models.lead import EnrichedLead, LeadRaw

logger = logging.getLogger(__name__)

# CSV 列顺序
_RAW_FIELDS = [
    "source", "business_name", "industry_keyword",
    "address", "phone", "website", "email",
    "rating", "review_count",
    "lat", "lng", "google_maps_url",
]

_ENRICHED_EXTRA = [
    "industry_category", "business_size",
    "score", "value_proposition",
    "recommended_product", "outreach_angle",
    "pain_points", "score_reason",
]


class CsvWriter:
    """
    将 LeadRaw / EnrichedLead 列表写为 UTF-8 BOM CSV（兼容 Excel 直接打开）。
    """

    @staticmethod
    def write_raw(
        leads: List[LeadRaw],
        output: Union[str, Path, None] = None,
    ) -> str:
        """
        写原始线索 CSV。
        output=None 时返回 CSV 字符串；否则写入文件。
        """
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=_RAW_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for lead in leads:
            writer.writerow({
                "source":           lead.source.value,
                "business_name":    lead.business_name,
                "industry_keyword": lead.industry_keyword,
                "address":          lead.address or "",
                "phone":            lead.phone or "",
                "website":          lead.website or "",
                "email":            lead.email or "",
                "rating":           lead.rating if lead.rating is not None else "",
                "review_count":     lead.review_count or "",
                "lat":              lead.lat if lead.lat is not None else "",
                "lng":              lead.lng if lead.lng is not None else "",
                "google_maps_url":  lead.google_maps_url or "",
            })

        content = "\ufeff" + buf.getvalue()     # UTF-8 BOM（Excel 友好）
        if output:
            Path(output).write_text(content, encoding="utf-8-sig")
            logger.info(f"CsvWriter: wrote {len(leads)} raw leads to {output}")
        return content

    @staticmethod
    def write_enriched(
        leads: List[EnrichedLead],
        output: Union[str, Path, None] = None,
    ) -> str:
        """写富化线索 CSV（包含 AI 评分和外展建议）"""
        fields = _RAW_FIELDS + _ENRICHED_EXTRA
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()

        for lead in leads:
            writer.writerow({
                "source":               lead.source.value,
                "business_name":        lead.business_name,
                "industry_keyword":     lead.industry_keyword,
                "address":              lead.address or "",
                "phone":                lead.phone or "",
                "website":              lead.website or "",
                "email":                lead.email or "",
                "rating":               lead.rating if lead.rating is not None else "",
                "review_count":         lead.review_count or "",
                "lat":                  lead.lat if lead.lat is not None else "",
                "lng":                  lead.lng if lead.lng is not None else "",
                "google_maps_url":      lead.google_maps_url or "",
                "industry_category":    lead.industry_category or "",
                "business_size":        lead.business_size or "",
                "score":                lead.score,
                "value_proposition":    lead.value_proposition or "",
                "recommended_product":  lead.recommended_product or "",
                "outreach_angle":       lead.outreach_angle or "",
                "pain_points":          "; ".join(lead.pain_points or []),
                "score_reason":         lead.score_reason or "",
            })

        content = "\ufeff" + buf.getvalue()
        if output:
            Path(output).write_text(content, encoding="utf-8-sig")
            logger.info(f"CsvWriter: wrote {len(leads)} enriched leads to {output}")
        return content
