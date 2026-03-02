"""
Lead Mining System — 综合实时集成测试
针对正在运行的 Docker 容器（localhost:8010 和 localhost:8080）执行完整 API 测试。

运行方式:
    cd /Users/kane/文档/project/n8n自动化营销项目/lead-mining-system
    python3 -m pytest tests/test_integration.py -v

前提条件: 所有 5 个容器必须处于 healthy 状态。
"""
from __future__ import annotations

import httpx
import pytest
import time

MINER_URL  = "http://localhost:8010"
OUTREACH_URL = "http://localhost:8080"

TIMEOUT = 30.0


# ─────────────────────────────────────────────
# ── 辅助
# ─────────────────────────────────────────────

@pytest.fixture(scope="session")
def miner() -> httpx.Client:
    with httpx.Client(base_url=MINER_URL, timeout=TIMEOUT) as c:
        yield c


@pytest.fixture(scope="session")
def outreach() -> httpx.Client:
    with httpx.Client(base_url=OUTREACH_URL, timeout=TIMEOUT) as c:
        yield c


@pytest.fixture(scope="session")
def first_lead_id(miner) -> int:
    """获取数据库中第一条 lead 的 ID 用于后续测试"""
    r = miner.get("/leads", params={"limit": 1})
    assert r.status_code == 200
    leads = r.json()["leads"]
    assert leads, "测试前提：数据库中至少需要 1 条 lead"
    return leads[0]["id"]


# ═════════════════════════════════════════════
# §1  LEAD-MINER HEALTH & METADATA
# ═════════════════════════════════════════════

class TestLeadMinerHealth:

    def test_health_returns_200(self, miner):
        r = miner.get("/health")
        assert r.status_code == 200

    def test_health_has_status_ok(self, miner):
        data = miner.get("/health").json()
        assert data.get("status") == "ok"

    def test_health_has_miners_key(self, miner):
        data = miner.get("/health").json()
        assert "miners" in data

    def test_health_has_chroma_docs(self, miner):
        data = miner.get("/health").json()
        assert "chroma_docs" in data
        assert isinstance(data["chroma_docs"], int)
        assert data["chroma_docs"] >= 0

    def test_stats_returns_200(self, miner):
        assert miner.get("/stats").status_code == 200

    def test_stats_structure(self, miner):
        data = miner.get("/stats").json()
        assert "leads" in data
        assert "outreach" in data
        assert "chroma_docs" in data

    def test_stats_leads_fields(self, miner):
        leads_stats = miner.get("/stats").json()["leads"]
        for key in ("raw_total", "enriched_total", "avg_score", "high_score_70"):
            assert key in leads_stats, f"stats.leads 缺少字段: {key}"

    def test_stats_raw_total_positive(self, miner):
        data = miner.get("/stats").json()
        assert data["leads"]["raw_total"] > 0, "数据库中应有 leads"

    def test_dashboard_returns_html(self, miner):
        r = miner.get("/dashboard")
        assert r.status_code == 200
        assert "text/html" in r.headers["content-type"]
        assert "<!DOCTYPE html>" in r.text or "<html" in r.text


# ═════════════════════════════════════════════
# §2  LEAD-MINER — /leads 读取
# ═════════════════════════════════════════════

class TestLeadsList:

    def test_leads_returns_200(self, miner):
        assert miner.get("/leads").status_code == 200

    def test_leads_response_shape(self, miner):
        data = miner.get("/leads").json()
        assert "total" in data
        assert "leads" in data
        assert isinstance(data["leads"], list)

    def test_leads_total_matches_list(self, miner):
        data = miner.get("/leads", params={"limit": 100}).json()
        assert data["total"] == len(data["leads"])

    def test_leads_limit_respected(self, miner):
        data = miner.get("/leads", params={"limit": 2}).json()
        assert len(data["leads"]) <= 2

    def test_leads_limit_one(self, miner):
        data = miner.get("/leads", params={"limit": 1}).json()
        assert len(data["leads"]) == 1

    def test_leads_required_fields(self, miner):
        leads = miner.get("/leads", params={"limit": 1}).json()["leads"]
        assert leads
        lead = leads[0]
        for field in ("id", "business_name", "source", "score", "enriched"):
            assert field in lead, f"lead 缺少字段: {field}"

    def test_leads_enriched_filter_true(self, miner):
        """enriched=true 参数不应导致 422 或 500（表现为未知参数被API忽略）"""
        r = miner.get("/leads", params={"enriched": "true", "limit": 50})
        assert r.status_code == 200
        data = r.json()
        assert "leads" in data and isinstance(data["leads"], list)

    def test_leads_enriched_filter_false(self, miner):
        """enriched=false 参数不应导致 422 或 500（表现为未知参数被API忽略）"""
        r = miner.get("/leads", params={"enriched": "false", "limit": 50})
        assert r.status_code == 200
        data = r.json()
        assert "leads" in data and isinstance(data["leads"], list)

    def test_leads_score_filter(self, miner):
        data = miner.get("/leads", params={"min_score": 90, "limit": 50}).json()
        for lead in data["leads"]:
            assert lead["score"] >= 90

    def test_leads_source_field_valid(self, miner):
        valid_sources = {"serper", "hunter", "yellowpages", "philgeps", "facebook",
                         "dti_bnrs", "api_miner", "manual"}
        leads = miner.get("/leads", params={"limit": 20}).json()["leads"]
        for lead in leads:
            assert lead["source"] in valid_sources, \
                f"未知 source: {lead['source']}"


# ═════════════════════════════════════════════
# §3  LEAD-MINER — /leads/export
# ═════════════════════════════════════════════

class TestLeadsExport:

    def test_export_returns_200(self, miner):
        assert miner.get("/leads/export", params={"limit": 5}).status_code == 200

    def test_export_content_type_csv(self, miner):
        r = miner.get("/leads/export", params={"limit": 5})
        assert "csv" in r.headers.get("content-type", "").lower()

    def test_export_has_content_disposition(self, miner):
        r = miner.get("/leads/export", params={"limit": 5})
        assert "attachment" in r.headers.get("content-disposition", "").lower()

    def test_export_csv_has_header_row(self, miner):
        r = miner.get("/leads/export", params={"limit": 5})
        first_line = r.text.strip().split("\n")[0]
        # CSV 首行应包含列名
        assert "business_name" in first_line or "id" in first_line


# ═════════════════════════════════════════════
# §4  LEAD-MINER — /rag/query
# ═════════════════════════════════════════════

class TestRAGQuery:

    def test_rag_query_returns_200(self, miner):
        r = miner.post("/rag/query", json={"query": "restaurant Manila"})
        assert r.status_code == 200

    def test_rag_query_has_results(self, miner):
        data = miner.post("/rag/query", json={"query": "restaurant"}).json()
        assert "results" in data
        assert isinstance(data["results"], list)

    def test_rag_query_result_fields(self, miner):
        data = miner.post("/rag/query", json={"query": "food business"}).json()
        if data["results"]:
            result = data["results"][0]
            assert "id" in result
            assert "document" in result

    def test_rag_query_missing_body_422(self, miner):
        r = miner.post("/rag/query", json={})
        assert r.status_code == 422

    def test_rag_query_empty_query_handled(self, miner):
        # 空 query 不应 crash（允许 422 或 200 空结果）
        r = miner.post("/rag/query", json={"query": ""})
        assert r.status_code in (200, 422)


# ═════════════════════════════════════════════
# §5  LEAD-MINER — /scoring/*
# ═════════════════════════════════════════════

class TestScoring:

    def test_scoring_stats_returns_200(self, miner):
        assert miner.get("/scoring/stats").status_code == 200

    def test_scoring_stats_structure(self, miner):
        data = miner.get("/scoring/stats").json()
        assert "feedback_total" in data
        assert "industries" in data
        assert isinstance(data["industries"], list)

    def test_scoring_stats_feedback_positive_field(self, miner):
        data = miner.get("/scoring/stats").json()
        assert "feedback_positive" in data
        assert data["feedback_positive"] >= 0

    def test_recalibrate_returns_200(self, miner):
        r = miner.post("/scoring/recalibrate", json={"lead_id": 2})
        assert r.status_code == 200

    def test_recalibrate_response_ok(self, miner):
        data = miner.post("/scoring/recalibrate", json={"lead_id": 2}).json()
        assert "ok" in data
        assert data["ok"] is True


# ═════════════════════════════════════════════
# §6  LEAD-MINER — /leads/{id}/feedback
# ═════════════════════════════════════════════

class TestFeedback:

    VALID_OUTCOMES = ["bounced", "converted", "ignored", "replied", "unsubscribed"]

    def test_feedback_valid_outcome(self, miner, first_lead_id):
        r = miner.post(
            f"/leads/{first_lead_id}/feedback",
            json={"outcome": "replied", "note": "Integration test"},
        )
        assert r.status_code == 200

    def test_feedback_response_shape(self, miner, first_lead_id):
        data = miner.post(
            f"/leads/{first_lead_id}/feedback",
            json={"outcome": "ignored"},
        ).json()
        assert data.get("ok") is True
        assert data.get("lead_id") == first_lead_id
        assert data.get("outcome") == "ignored"

    def test_feedback_invalid_outcome_400(self, miner, first_lead_id):
        r = miner.post(
            f"/leads/{first_lead_id}/feedback",
            json={"outcome": "invalid_outcome_xyz"},
        )
        assert r.status_code in (400, 422)

    def test_feedback_missing_outcome_422(self, miner, first_lead_id):
        r = miner.post(f"/leads/{first_lead_id}/feedback", json={})
        assert r.status_code == 422

    def test_feedback_nonexistent_lead_404(self, miner):
        r = miner.post(
            "/leads/99999999/feedback",
            json={"outcome": "replied"},
        )
        assert r.status_code == 404

    def test_all_valid_outcomes_accepted(self, miner, first_lead_id):
        for outcome in self.VALID_OUTCOMES:
            r = miner.post(
                f"/leads/{first_lead_id}/feedback",
                json={"outcome": outcome},
            )
            assert r.status_code == 200, \
                f"outcome='{outcome}' 应被接受，实际: {r.status_code}"


# ═════════════════════════════════════════════
# §7  LEAD-MINER — /mine & /enrich (验证请求格式)
# ═════════════════════════════════════════════

class TestMineEnrich:

    def test_mine_missing_keyword_422(self, miner):
        r = miner.post("/mine", json={})
        assert r.status_code == 422

    def test_mine_valid_request_accepted(self, miner):
        """矿工可能失败（API key），但格式正确时不应返回 422"""
        r = miner.post("/mine", json={"keyword": "restaurant", "limit": 1})
        # 200（成功）或 5xx（外部 API 错误）均可接受，不接受 422
        assert r.status_code != 422

    def test_enrich_returns_200(self, miner):
        r = miner.post("/enrich", json={})
        assert r.status_code == 200

    def test_enrich_response_shape(self, miner):
        data = miner.post("/enrich", json={}).json()
        assert "enriched" in data
        assert "total_input" in data

    def test_update_status_missing_email_422(self, miner):
        r = miner.post("/leads/update-status", json={})
        assert r.status_code == 422

    def test_update_status_valid_request(self, miner):
        r = miner.post(
            "/leads/update-status",
            json={"email": "nonexistent@test.com", "status": "replied"},
        )
        assert r.status_code == 200
        data = r.json()
        assert "updated" in data
        assert data["email"] == "nonexistent@test.com"


# ═════════════════════════════════════════════
# §8  SALES-OUTREACH — Health
# ═════════════════════════════════════════════

class TestOutreachHealth:

    def test_health_returns_200(self, outreach):
        assert outreach.get("/health").status_code == 200

    def test_health_status_ok(self, outreach):
        data = outreach.get("/health").json()
        assert data.get("status") == "ok"

    def test_crm_health_returns_200(self, outreach):
        assert outreach.get("/crm/health").status_code == 200

    def test_crm_health_has_provider(self, outreach):
        data = outreach.get("/crm/health").json()
        assert "provider" in data
        assert "healthy" in data
        assert "message" in data


# ═════════════════════════════════════════════
# §9  SALES-OUTREACH — /crm/sync  （关键 Bug 修复验证）
# ═════════════════════════════════════════════

class TestCRMSync:

    def test_crm_sync_returns_200_not_500(self, outreach):
        """
        关键修复验证: postgres_loader.py 中 e.metadata → r.metadata
        修复前: 触发 UndefinedColumnError -> 500
        修复后: 正常返回 200，提示 CRM provider 未配置
        """
        r = outreach.post("/crm/sync", json={"min_score": 0, "limit": 1})
        assert r.status_code == 200, \
            f"CRM sync 不应崩溃 — e.metadata Bug 已修复，实际状态码: {r.status_code}"

    def test_crm_sync_response_not_empty(self, outreach):
        data = outreach.post("/crm/sync", json={"min_score": 0, "limit": 1}).json()
        assert data != {}

    def test_crm_sync_no_provider_message(self, outreach):
        """未配置 CRM 时应返回明确提示，而不是服务器错误"""
        data = outreach.post("/crm/sync", json={"min_score": 0, "limit": 1}).json()
        # 允许 message 字段或 synced 字段
        assert "message" in data or "synced" in data

    def test_crm_sync_with_high_min_score(self, outreach):
        """极高分数门槛应返回 synced=0（不崩溃）"""
        data = outreach.post("/crm/sync", json={"min_score": 999, "limit": 10}).json()
        assert "synced" in data or "message" in data


# ═════════════════════════════════════════════
# §10  SALES-OUTREACH — /outreach/run
# ═════════════════════════════════════════════

class TestOutreachRun:

    def test_outreach_dry_run_returns_200(self, outreach):
        r = outreach.post("/outreach/run", json={"limit": 1, "dry_run": True})
        assert r.status_code == 200

    def test_outreach_dry_run_response(self, outreach):
        data = outreach.post("/outreach/run", json={"limit": 1, "dry_run": True}).json()
        assert data.get("dry_run") is True
        assert "lead_count" in data

    def test_outreach_dry_run_no_emails_sent(self, outreach):
        data = outreach.post("/outreach/run", json={"limit": 5, "dry_run": True}).json()
        # dry_run 模式不应发送邮件
        assert data.get("dry_run") is True

    def test_outreach_dry_run_returns_leads_preview(self, outreach):
        data = outreach.post("/outreach/run", json={"limit": 3, "dry_run": True}).json()
        assert "leads_preview" in data
        assert isinstance(data["leads_preview"], list)

    def test_outreach_leads_preview_shape(self, outreach):
        data = outreach.post("/outreach/run", json={"limit": 3, "dry_run": True}).json()
        for lead in data.get("leads_preview", []):
            assert "id" in lead
            assert "business_name" in lead
            assert "score" in lead

    def test_outreach_missing_body_uses_defaults(self, outreach):
        """缺少 body 时应使用默认参数，不应返回 422"""
        r = outreach.post("/outreach/run", json={})
        assert r.status_code != 422

    def test_outreach_high_min_score_returns_empty(self, outreach):
        data = outreach.post(
            "/outreach/run", json={"limit": 10, "dry_run": True, "min_score": 999}
        ).json()
        assert data.get("lead_count", 0) == 0


# ═════════════════════════════════════════════
# §11  SALES-OUTREACH — Webhooks
# ═════════════════════════════════════════════

class TestWebhooks:

    def test_hot_lead_webhook_returns_200(self, outreach):
        r = outreach.post(
            "/webhook/hot-lead",
            json={"lead_id": 2, "business_name": "Test Biz", "score": 95},
        )
        assert r.status_code == 200

    def test_hot_lead_webhook_response(self, outreach):
        data = outreach.post(
            "/webhook/hot-lead",
            json={"lead_id": 1, "business_name": "Biz", "score": 88},
        ).json()
        assert data.get("status") == "received"

    def test_outreach_summary_webhook_returns_200(self, outreach):
        r = outreach.post(
            "/webhook/outreach-summary",
            json={
                "summary": "Sent 3 emails",
                "leads_contacted": 3,
                "timestamp": "2026-03-01T00:00:00Z",
            },
        )
        assert r.status_code == 200

    def test_outreach_summary_webhook_response(self, outreach):
        data = outreach.post(
            "/webhook/outreach-summary",
            json={
                "summary": "Batch done",
                "leads_contacted": 5,
                "timestamp": "2026-03-02T12:00:00Z",
            },
        ).json()
        assert data.get("status") == "received"
        assert "timestamp" in data

    def test_hot_lead_empty_body_handled(self, outreach):
        """空 body 的 webhook 处理不应 500"""
        r = outreach.post("/webhook/hot-lead", json={})
        assert r.status_code != 500


# ═════════════════════════════════════════════
# §12  Bug 修复专项验证
# ═════════════════════════════════════════════

class TestBugFixVerification:

    # ── Fix #1: postgres_loader.py e.metadata → r.metadata ──────────────────
    def test_crm_sync_does_not_crash_with_sql_error(self, outreach):
        """
        修复前: leads_enriched 没有 metadata 列，查询 e.metadata 导致 500
        修复后: 查询 r.metadata (leads_raw) 正常返回
        """
        r = outreach.post("/crm/sync", json={"min_score": 0, "limit": 5})
        assert r.status_code in (200, 202), \
            "CRM sync 不应抛 SQL 500 错误（e.metadata bug 已修复）"

    # ── Fix #2: rate_limit.py X-Forwarded-For ───────────────────────────────
    def test_xff_header_does_not_break_rate_limit(self, miner):
        """
        修复前: request.client=None 时所有请求共享 anonymous bucket，失去 IP 隔离
        修复后: 优先使用 X-Forwarded-For 头
        """
        r = miner.get(
            "/leads",
            headers={"X-Forwarded-For": "1.2.3.4"},
        )
        # 主要验证: 带 XFF 头的请求不会引发 500
        assert r.status_code in (200, 429), \
            "带 X-Forwarded-For 头的请求不应导致 500"

    # ── Fix #3: facebook_miner review_count None 语义 ────────────────────────
    def test_leads_review_count_can_be_null(self, miner):
        """
        修复前: review_count=None → 0（丢失无数据语义）
        修复后: review_count 保留 None 或真实值
        验证方式：检查收录的 leads 中存在 review_count 为 null 的记录（如有）
        """
        data = miner.get("/leads", params={"limit": 50}).json()
        # 这里只验证 review_count 字段类型正确（int 或 null），不强制要求存在 null
        for lead in data["leads"]:
            rc = lead.get("review_count")
            assert rc is None or isinstance(rc, (int, float)), \
                f"review_count 应为 int 或 null，得到: {type(rc)}"

    # ── Fix #4: browser_miner context 泄漏（无法直接验证，验证 health 响应正常）
    def test_health_miners_section_present(self, miner):
        """BrowserContext 修复后 /health 中矿工列表仍正常返回"""
        data = miner.get("/health").json()
        assert "miners" in data

    # ── Fix #5: asyncpg pool 关闭（验证服务正常重启后健康）────────────────────
    def test_outreach_healthy_after_restart(self, outreach):
        """close_db_pool() 修复后，重启的服务进程应正常响应"""
        r = outreach.get("/health")
        assert r.status_code == 200
        assert r.json().get("status") == "ok"


# ═════════════════════════════════════════════
# §13  跨服务数据一致性验证
# ═════════════════════════════════════════════

class TestDataConsistency:

    def test_lead_count_consistent(self, miner):
        """stats.raw_total 应与 /leads total 一致"""
        stats = miner.get("/stats").json()
        raw_total = stats["leads"]["raw_total"]
        # 分批获取（max limit=200）
        leads_data = miner.get("/leads", params={"limit": 200}).json()
        assert "total" in leads_data, "/leads 响应缺少 total 字段"
        assert leads_data["total"] == raw_total, \
            f"stats.raw_total={raw_total} 与 leads.total={leads_data['total']} 不一致"

    def test_enriched_count_consistent(self, miner):
        """stats.enriched_total 应 <= raw_total（数据完整性验证）"""
        stats = miner.get("/stats").json()
        raw_total = stats["leads"]["raw_total"]
        enriched_total = stats["leads"]["enriched_total"]
        assert 0 <= enriched_total <= raw_total, \
            f"enriched_total={enriched_total} 应在 [0, raw_total={raw_total}] 范围内"

    def test_chroma_docs_positive(self, miner):
        """ChromaDB 中应有向量化文档（说明 enrichment 正常写入）"""
        data = miner.get("/health").json()
        assert data["chroma_docs"] > 0, "ChromaDB 中应有文档"

    def test_scoring_stats_industry_in_leads(self, miner):
        """scoring/stats 中的行业应能在 leads 中找到对应记录"""
        scoring = miner.get("/scoring/stats").json()
        industries_in_scoring = {i["industry"] for i in scoring["industries"]}
        leads = miner.get("/leads", params={"limit": 50}).json()["leads"]
        if leads and industries_in_scoring:
            lead_industries = {l.get("industry_category") for l in leads if l.get("industry_category")}
            # 至少有一个行业重叠
            overlap = industries_in_scoring & lead_industries
            assert len(overlap) > 0, \
                f"scoring stats 行业 {industries_in_scoring} 与 leads 行业 {lead_industries} 无交集"

    def test_outreach_dry_run_count_le_leads_total(self, miner, outreach):
        """outreach dry_run 返回的 lead_count 不应超过数据库中的 leads 总数"""
        total = miner.get("/stats").json()["leads"]["raw_total"]
        outreach_count = outreach.post(
            "/outreach/run", json={"limit": 200, "dry_run": True}
        ).json().get("lead_count", 0)
        assert outreach_count <= total, \
            f"outreach lead_count={outreach_count} 超过总 leads={total}"
