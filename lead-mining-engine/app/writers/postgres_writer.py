"""
PostgresWriter — 持久化存储层
使用 asyncpg 高性能异步写入，支持 UPSERT 去重
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import List, Optional, Tuple

import asyncpg

from app.models.lead import EnrichedLead, LeadRaw

logger = logging.getLogger(__name__)

# ── DDL，首次运行自动建表 ──────────────────────────────────────────────────────
_CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS leads_raw (
    id              BIGSERIAL PRIMARY KEY,
    dedup_key       TEXT         NOT NULL UNIQUE,
    source          TEXT         NOT NULL,
    business_name   TEXT         NOT NULL,
    industry_kw     TEXT,
    address         TEXT,
    phone           TEXT,
    website         TEXT,
    email           TEXT,
    rating          FLOAT,
    review_count    INT,
    lat             FLOAT,
    lng             FLOAT,
    gmaps_url       TEXT,
    metadata        JSONB,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_leads_raw_source   ON leads_raw(source);
CREATE INDEX IF NOT EXISTS idx_leads_raw_kw       ON leads_raw(industry_kw);
CREATE INDEX IF NOT EXISTS idx_leads_raw_created  ON leads_raw(created_at DESC);

CREATE TABLE IF NOT EXISTS leads_enriched (
    id                  BIGINT       PRIMARY KEY REFERENCES leads_raw(id),
    industry_category   TEXT,
    business_size       TEXT,
    pain_points         JSONB,
    value_proposition   TEXT,
    recommended_product TEXT,
    outreach_angle      TEXT,
    score               INT,
    score_reason        TEXT,
    best_contact_time   TEXT,
    enriched            BOOLEAN      NOT NULL DEFAULT TRUE,
    enriched_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_leads_enriched_score  ON leads_enriched(score DESC);

CREATE TABLE IF NOT EXISTS contacts (
    id              BIGSERIAL PRIMARY KEY,
    lead_id         BIGINT       REFERENCES leads_raw(id),
    full_name       TEXT,
    job_title       TEXT,
    email           TEXT,
    email_verified  BOOLEAN      DEFAULT FALSE,
    linkedin_url    TEXT,
    company_size    TEXT,
    source          TEXT,
    metadata        JSONB,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS outreach_log (
    id          BIGSERIAL PRIMARY KEY,
    lead_id     BIGINT       REFERENCES leads_raw(id),
    email       TEXT         NOT NULL,
    subject     TEXT,
    status      TEXT         NOT NULL DEFAULT 'sent',
    sent_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (lead_id, email)
);

CREATE INDEX IF NOT EXISTS idx_outreach_log_lead ON outreach_log(lead_id);
CREATE INDEX IF NOT EXISTS idx_outreach_log_sent ON outreach_log(sent_at DESC);

-- P2-5：外展反馈表，用于评分模型优化
CREATE TABLE IF NOT EXISTS score_feedback (
    id          BIGSERIAL    PRIMARY KEY,
    lead_id     BIGINT       NOT NULL REFERENCES leads_raw(id) ON DELETE CASCADE,
    outcome     TEXT         NOT NULL
                             CHECK (outcome IN ('replied','converted','bounced','ignored','unsubscribed')),
    notes       TEXT,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_score_feedback_lead    ON score_feedback(lead_id);
CREATE INDEX IF NOT EXISTS idx_score_feedback_outcome ON score_feedback(outcome);

-- 面板配置持久化表（API Key、Miner 开关等）
CREATE TABLE IF NOT EXISTS app_settings (
    key         TEXT        PRIMARY KEY,
    value       TEXT        NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


class PostgresWriter:
    """
    asyncpg 连接池封装，提供 leads_raw / leads_enriched / contacts 三表的 CRUD。
    """

    def __init__(self, dsn: str, min_size: int = 2, max_size: int = 10):
        self.dsn = dsn
        self.min_size = min_size
        self.max_size = max_size
        self._pool: Optional[asyncpg.Pool] = None

    async def connect(self) -> None:
        self._pool = await asyncpg.create_pool(
            dsn=self.dsn,
            min_size=self.min_size,
            max_size=self.max_size,
            command_timeout=30,
        )
        logger.info(f"PostgresWriter pool created (min={self.min_size}, max={self.max_size})")

    async def init_tables(self) -> None:
        """首次启动建表（幂等）"""
        if self._pool is None:
            raise RuntimeError("PostgresWriter not connected. Call connect() first.")
        async with self._pool.acquire() as conn:
            await conn.execute(_CREATE_TABLES_SQL)
        logger.info("PostgresWriter: tables initialized")

    async def upsert_leads(self, leads: List[LeadRaw]) -> Tuple[int, int]:
        """
        批量 UPSERT leads_raw（去重键：dedup_key）。
        返回 (inserted, skipped)
        """
        if not leads or self._pool is None:
            return 0, 0

        upsert_sql = """
            INSERT INTO leads_raw
                (dedup_key, source, business_name, industry_kw, address,
                 phone, website, email, rating, review_count, lat, lng,
                 gmaps_url, metadata)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
            ON CONFLICT (dedup_key)
            DO UPDATE SET
                updated_at    = NOW(),
                phone         = COALESCE(EXCLUDED.phone, leads_raw.phone),
                website       = COALESCE(EXCLUDED.website, leads_raw.website),
                email         = COALESCE(EXCLUDED.email, leads_raw.email),
                rating        = COALESCE(EXCLUDED.rating, leads_raw.rating),
                review_count  = COALESCE(EXCLUDED.review_count, leads_raw.review_count),
                metadata      = leads_raw.metadata || EXCLUDED.metadata
            RETURNING (xmax = 0) AS inserted
        """
        inserted = 0
        skipped = 0

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                for lead in leads:
                    row = await conn.fetchrow(
                        upsert_sql,
                        lead.dedup_key(),
                        lead.source.value,
                        lead.business_name,
                        lead.industry_keyword,
                        lead.address,
                        lead.phone,
                        lead.website,
                        lead.email,
                        lead.rating,
                        lead.review_count,
                        lead.lat,
                        lead.lng,
                        lead.google_maps_url,
                        json.dumps(lead.metadata or {}),
                    )
                    if row and row["inserted"]:
                        inserted += 1
                    else:
                        skipped += 1

        logger.info(f"PostgresWriter upsert: {inserted} inserted, {skipped} skipped")
        return inserted, skipped

    async def upsert_enriched(self, leads: List[EnrichedLead]) -> int:
        """更新富化字段"""
        if not leads or self._pool is None:
            return 0

        count = 0
        async with self._pool.acquire() as conn:
            for lead in leads:
                raw_id = await conn.fetchval(
                    "SELECT id FROM leads_raw WHERE dedup_key = $1",
                    lead.dedup_key(),
                )
                if raw_id is None:
                    continue

                await conn.execute(
                    """
                    INSERT INTO leads_enriched
                        (id, industry_category, business_size, pain_points,
                         value_proposition, recommended_product, outreach_angle,
                         score, score_reason, best_contact_time, enriched)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                    ON CONFLICT (id) DO UPDATE SET
                        industry_category   = EXCLUDED.industry_category,
                        business_size       = EXCLUDED.business_size,
                        pain_points         = EXCLUDED.pain_points,
                        value_proposition   = EXCLUDED.value_proposition,
                        recommended_product = EXCLUDED.recommended_product,
                        outreach_angle      = EXCLUDED.outreach_angle,
                        score               = EXCLUDED.score,
                        score_reason        = EXCLUDED.score_reason,
                        best_contact_time   = EXCLUDED.best_contact_time,
                        enriched_at         = NOW()
                    """,
                    raw_id,
                    lead.industry_category,
                    lead.business_size,
                    json.dumps(lead.pain_points or []),
                    lead.value_proposition,
                    lead.recommended_product,
                    lead.outreach_angle,
                    lead.score,
                    lead.score_reason,
                    lead.best_contact_time,
                    lead.enriched,
                )
                count += 1
        return count

    async def query_leads(
        self,
        industry_keyword: Optional[str] = None,
        min_score: int = 0,
        limit: int = 50,
        offset: int = 0,
        source: Optional[str] = None,
        lead_id: Optional[int] = None,
    ) -> List[dict]:
        """查询已富化线索"""
        if self._pool is None:
            return []

        # 输入校验（纵深防御）
        limit = max(1, min(limit, 500))
        offset = max(0, offset)
        min_score = max(0, min(min_score, 100))
        if industry_keyword and len(industry_keyword) > 200:
            industry_keyword = industry_keyword[:200]
        if source and len(source) > 50:
            source = source[:50]

        conditions = ["1=1"]
        params: list = []
        idx = 1

        if lead_id is not None:
            conditions.append(f"r.id = ${idx}")
            params.append(lead_id)
            idx += 1
        if industry_keyword:
            conditions.append(f"r.industry_kw ILIKE ${idx}")
            params.append(f"%{industry_keyword}%")
            idx += 1
        if source:
            conditions.append(f"r.source = ${idx}")
            params.append(source)
            idx += 1
        if min_score > 0:
            conditions.append(f"COALESCE(e.score, 0) >= ${idx}")
            params.append(min_score)
            idx += 1

        params += [limit, offset]
        sql = f"""
            SELECT
                r.id, r.dedup_key, r.source, r.business_name, r.industry_kw,
                r.address, r.phone, r.website, r.email, r.rating, r.review_count,
                r.lat, r.lng, r.gmaps_url, r.metadata, r.created_at,
                e.industry_category, e.business_size, e.pain_points,
                e.value_proposition, e.recommended_product, e.outreach_angle,
                e.score, e.score_reason, e.best_contact_time, e.enriched
            FROM leads_raw r
            LEFT JOIN leads_enriched e ON e.id = r.id
            WHERE {' AND '.join(conditions)}
            ORDER BY COALESCE(e.score, 0) DESC, r.created_at DESC
            LIMIT ${idx} OFFSET ${idx+1}
        """
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)
        return [dict(row) for row in rows]

    async def query_unenriched_leads(
        self,
        limit: int = 50,
        industry_keyword: Optional[str] = None,
    ) -> List[dict]:
        """
        查询尚未经过 AI 富化的 leads_raw 记录（leads_enriched 中无对应行）。
        供 /enrich 端点使用，避免重复富化。
        """
        if self._pool is None:
            return []

        conditions = ["e.id IS NULL"]
        params: list = []
        idx = 1

        if industry_keyword:
            conditions.append(f"r.industry_kw ILIKE ${idx}")
            params.append(f"%{industry_keyword}%")
            idx += 1

        params.append(limit)
        sql = f"""
            SELECT
                r.id, r.source, r.business_name, r.industry_kw,
                r.address, r.phone, r.website, r.email,
                r.rating, r.review_count, r.lat, r.lng,
                r.gmaps_url, r.metadata
            FROM leads_raw r
            LEFT JOIN leads_enriched e ON e.id = r.id
            WHERE {' AND '.join(conditions)}
            ORDER BY r.created_at DESC
            LIMIT ${idx}
        """
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)
        return [dict(row) for row in rows]

    async def update_outreach_status(
        self,
        email: str,
        status: str,
    ) -> int:
        """
        根据邮箱地址更新 outreach_log 状态（如：interested / not_interested）。
        供 n8n 回复检测工作流调用。
        返回受影响的行数。
        """
        if self._pool is None:
            return 0

        valid_statuses = {"interested", "not_interested", "auto_reply", "unsubscribe", "other", "sent"}
        if status not in valid_statuses:
            logger.warning(f"update_outreach_status: invalid status '{status}', defaulting to 'other'")
            status = "other"

        async with self._pool.acquire() as conn:
            result = await conn.execute(
                """
                UPDATE outreach_log
                SET status = $1
                WHERE email = $2
                """,
                status,
                email,
            )
        # asyncpg execute() 返回 "UPDATE N" 字符串
        try:
            affected = int(result.split()[-1])
        except Exception:
            affected = 0
        logger.info(f"update_outreach_status: {affected} rows updated for email={email}, status={status}")
        return affected

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
            logger.info("PostgresWriter pool closed")

    # ── 管理面板：app_settings CRUD ─────────────────────────────────────────

    async def get_setting(self, key: str, default: str = "") -> str:
        """读取单个配置项"""
        if self._pool is None:
            return default
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT value FROM app_settings WHERE key = $1", key
            )
        return row["value"] if row else default

    async def set_setting(self, key: str, value: str) -> None:
        """写入/更新单个配置项（UPSERT）"""
        if self._pool is None:
            raise RuntimeError("PostgresWriter not connected")
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO app_settings (key, value, updated_at)
                VALUES ($1, $2, NOW())
                ON CONFLICT (key) DO UPDATE
                    SET value = EXCLUDED.value, updated_at = NOW()
                """,
                key,
                value,
            )

    async def get_all_settings(self) -> dict:
        """读取所有配置项，返回 {key: value} 字典"""
        if self._pool is None:
            return {}
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("SELECT key, value FROM app_settings")
        return {row["key"]: row["value"] for row in rows}

    # ── P2-5 反馈与评分统计 ─────────────────────────────────────────────────

    async def insert_feedback(self, lead_id: int, outcome: str, notes: str = "") -> None:
        """记录外展结果反馈（replies / converted / bounced 等）"""
        if self._pool is None:
            raise RuntimeError("PostgresWriter not connected")
        await self._pool.execute(
            "INSERT INTO score_feedback (lead_id, outcome, notes) VALUES ($1, $2, $3)",
            lead_id, outcome, notes or None,
        )

    async def fetch_funnel_stats(self) -> dict:
        """读取漏斗统计指标（raw → enriched → outreached），供 /stats 端点使用"""
        if self._pool is None:
            raise RuntimeError("PostgresWriter not connected")
        row = await self._pool.fetchrow("""
            SELECT
                (SELECT COUNT(*)  FROM leads_raw)                                   AS raw_total,
                (SELECT COUNT(*)  FROM leads_raw WHERE email IS NOT NULL AND email != '') AS raw_with_email,
                (SELECT COUNT(*)  FROM leads_enriched WHERE enriched = TRUE)        AS enriched_total,
                (SELECT AVG(score)::NUMERIC(5,1) FROM leads_enriched WHERE enriched = TRUE) AS avg_score,
                (SELECT COUNT(*)  FROM leads_enriched WHERE enriched = TRUE AND score >= 70)  AS high_score,
                (SELECT COUNT(*)  FROM outreach_log)                                AS outreached_total,
                (SELECT COUNT(*)  FROM outreach_log WHERE status = 'sent')          AS emails_sent,
                (SELECT COUNT(DISTINCT status) FROM outreach_log WHERE status NOT IN ('sent','skipped')) AS replies
        """)
        return dict(row)

    async def fetch_scoring_stats(self) -> tuple:
        """按行业统计转化率，供 /scoring/stats 端点使用"""
        if self._pool is None:
            raise RuntimeError("PostgresWriter not connected")
        rows = await self._pool.fetch("""
            SELECT
                COALESCE(NULLIF(le.industry_category,''), lr.industry_kw, 'Unknown') AS industry,
                COUNT(DISTINCT le.id)                                    AS total_leads,
                ROUND(AVG(le.score)::NUMERIC, 1)                        AS avg_score,
                COUNT(DISTINCT f.lead_id)
                    FILTER (WHERE f.outcome IN ('replied','converted'))  AS positive_responses,
                ROUND(
                    100.0 * COUNT(DISTINCT f.lead_id)
                        FILTER (WHERE f.outcome IN ('replied','converted'))
                    / NULLIF(COUNT(DISTINCT le.id), 0)
                , 1)                                                     AS conversion_rate_pct
            FROM leads_enriched le
            JOIN  leads_raw     lr ON lr.id = le.id
            LEFT JOIN score_feedback f ON f.lead_id = le.id
            WHERE le.enriched = TRUE
            GROUP BY COALESCE(NULLIF(le.industry_category,''), lr.industry_kw, 'Unknown')
            ORDER BY positive_responses DESC, avg_score DESC
            LIMIT 30
        """)
        summary_row = await self._pool.fetchrow(
            "SELECT COUNT(*) AS total, "
            "COUNT(*) FILTER (WHERE outcome IN ('replied','converted')) AS positive "
            "FROM score_feedback"
        )
        return [dict(r) for r in rows], dict(summary_row)

    async def fetch_low_score_positive_leads(self, min_positives: int = 3) -> list:
        """找出有正向反馈但评分偏低（< 65）的线索，供评分校正使用"""
        if self._pool is None:
            raise RuntimeError("PostgresWriter not connected")
        rows = await self._pool.fetch(
            """
            SELECT lr.id, lr.business_name, lr.industry_kw, lr.website, lr.email,
                   lr.address, lr.phone, lr.rating, lr.review_count,
                   lr.lat, lr.lng, lr.gmaps_url, lr.source, lr.metadata, le.score
            FROM leads_raw lr
            JOIN leads_enriched le ON le.id = lr.id
            JOIN score_feedback  sf ON sf.lead_id = lr.id
                AND sf.outcome IN ('replied', 'converted')
            WHERE le.score < 65
            GROUP BY lr.id, lr.business_name, lr.industry_kw, lr.website, lr.email,
                     lr.address, lr.phone, lr.rating, lr.review_count,
                     lr.lat, lr.lng, lr.gmaps_url, lr.source, lr.metadata, le.score
            HAVING COUNT(sf.id) >= $1
            LIMIT 20
            """,
            min_positives,
        )
        return [dict(r) for r in rows]
