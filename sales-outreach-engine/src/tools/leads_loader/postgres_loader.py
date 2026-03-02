"""
PostgresLeadLoader — 从 PostgreSQL 读取已富化线索
供 sales-outreach LangGraph 使用
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import List, Optional

import asyncpg

logger = logging.getLogger(__name__)


class PostgresLeadLoader:
    """
    从 leads DB 加载已富化、尚未外展的线索。
    供 LangGraph nodes.py 中的 get_new_leads 节点调用。
    """

    def __init__(self, dsn: str):
        self.dsn = dsn
        self._pool: Optional[asyncpg.Pool] = None

    async def connect(self) -> None:
        self._pool = await asyncpg.create_pool(dsn=self.dsn, min_size=1, max_size=5)
        logger.info("PostgresLeadLoader pool created")

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()

    async def get_pending_leads(
        self,
        limit: int = 20,
        min_score: int = 60,
        industry_keyword: Optional[str] = None,
    ) -> List[dict]:
        """
        查询尚未外展（outreach_log 中无记录）且评分达标的线索。
        """
        if self._pool is None:
            raise RuntimeError("PostgresLeadLoader not connected")

        conditions = [
            "e.score >= $1",
            "r.id NOT IN (SELECT DISTINCT lead_id FROM outreach_log WHERE lead_id IS NOT NULL)",
        ]
        params: list = [min_score]
        idx = 2

        if industry_keyword:
            conditions.append(f"r.industry_kw ILIKE ${idx}")
            params.append(f"%{industry_keyword}%")
            idx += 1

        params.append(limit)
        sql = f"""
            SELECT
                r.id, r.business_name, r.industry_kw AS industry_keyword,
                r.address, r.phone, r.website, r.email, r.rating, r.gmaps_url,
                e.industry_category, e.business_size, e.score,
                e.pain_points, e.value_proposition,
                e.recommended_product, e.outreach_angle
            FROM leads_raw r
            JOIN leads_enriched e ON e.id = r.id
            WHERE {' AND '.join(conditions)}
            ORDER BY e.score DESC
            LIMIT ${idx}
        """
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)

        result = []
        for row in rows:
            d = dict(row)
            # pain_points 存为 JSONB，需要解析
            if isinstance(d.get("pain_points"), str):
                try:
                    d["pain_points"] = json.loads(d["pain_points"])
                except Exception:
                    d["pain_points"] = []
            result.append(d)
        return result

    async def mark_outreached(self, lead_id: int, email_sent_to: str) -> None:
        """记录外展日志（防止重复发送）"""
        if self._pool is None:
            return
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO outreach_log (lead_id, email, status, sent_at)
                VALUES ($1, $2, 'sent', NOW())
                ON CONFLICT (lead_id, email) DO NOTHING
                """,
                lead_id, email_sent_to,
            )

    async def get_enriched_leads(
        self,
        min_score: float = 0.0,
        industry_keyword: Optional[str] = None,
        limit: int = 50,
        lead_ids: Optional[List[int]] = None,
    ) -> List[dict]:
        """
        P3-5: 查询所有已富化线索（含已外展），用于推送到 CRM。
        与 get_pending_leads 的区别：不过滤 outreach_log。
        支持 lead_ids 指定列表过滤。
        """
        if self._pool is None:
            raise RuntimeError("PostgresLeadLoader not connected")

        conditions: list = ["e.score >= $1"]
        params: list = [min_score]
        idx = 2

        if industry_keyword:
            conditions.append(f"r.industry_kw ILIKE ${idx}")
            params.append(f"%{industry_keyword}%")
            idx += 1

        if lead_ids:
            placeholders = ", ".join(f"${i}" for i in range(idx, idx + len(lead_ids)))
            conditions.append(f"r.id IN ({placeholders})")
            params.extend(lead_ids)
            idx += len(lead_ids)

        params.append(limit)
        sql = f"""
            SELECT
                r.id, r.business_name, r.industry_kw AS industry_keyword,
                r.source, r.address, r.phone, r.website, r.email,
                e.industry_category, e.business_size, e.score,
                e.pain_points, e.value_proposition,
                e.recommended_product, e.outreach_angle,
                r.metadata
            FROM leads_raw r
            JOIN leads_enriched e ON e.id = r.id
            WHERE {' AND '.join(conditions)}
            ORDER BY e.score DESC
            LIMIT ${idx}
        """
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)

        result = []
        for row in rows:
            d = dict(row)
            for field in ("pain_points", "metadata"):
                if isinstance(d.get(field), str):
                    try:
                        d[field] = json.loads(d[field])
                    except Exception:
                        d[field] = [] if field == "pain_points" else {}
                elif d.get(field) is None:
                    d[field] = [] if field == "pain_points" else {}
            result.append(d)
        return result
