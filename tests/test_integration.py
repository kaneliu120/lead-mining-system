"""
Lead Mining System — Comprehensive live integration tests
Runs full API tests against running Docker containers (localhost:8010 and localhost:8080).

Run with:
    cd /path/to/lead-mining-system
    python3 -m pytest tests/test_integration.py -v

Prerequisite: all 5 containers must be in healthy state.
"""
from __future__ import annotations

import httpx
import pytest
import time

MINER_URL  = "http://localhost:8010"
OUTREACH_URL = "http://localhost:8080"

TIMEOUT = 30.0


# ─────────────────────────────────────────────
# ── Helpers
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
    """Get the ID of the first lead in the database for subsequent tests"""
    r = miner.get("/leads", params={"limit": 1})
    assert r.status_code == 200
    leads = r.json()["leads"]
    assert leads, "Test prerequisite: at least 1 lead must exist in the database"
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
            assert key in leads_stats, f"stats.leads missing field: {key}"

    def test_stats_raw_total_positive(self, miner):
        data = miner.get("/stats").json()
        assert data["leads"]["raw_total"] > 0, "Database should have leads"

    def test_dashboard_returns_html(self, miner):
        r = miner.get("/dashboard")
        assert r.status_code == 200
        assert "text/html" in r.headers["content-type"]
        assert "<!DOCTYPE html>" in r.text or "<html" in r.text


# ═════════════════════════════════════════════
# §2  LEAD-MINER — /leads read
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
            assert field in lead, f"lead missing field: {field}"

    def test_leads_enriched_filter_true(self, miner):
        """enriched=true parameter should not cause 422 or 500 (unknown param ignored by API)"""
        r = miner.get("/leads", params={"enriched": "true", "limit": 50})
        assert r.status_code == 200
        data = r.json()
        assert "leads" in data and isinstance(data["leads"], list)

    def test_leads_enriched_filter_false(self, miner):
        """enriched=false parameter should not cause 422 or 500 (unknown param ignored by API)"""
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
                f"Unknown source: {lead['source']}"


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
        # CSV first row should contain column names
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
        # Empty query should not crash (allow 422 or 200 empty result)
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
                f"outcome='{outcome}' should be accepted, actual: {r.status_code}"


# ═════════════════════════════════════════════
# §7  LEAD-MINER — /mine & /enrich (validate request format)
# ═════════════════════════════════════════════

class TestMineEnrich:

    def test_mine_missing_keyword_422(self, miner):
        r = miner.post("/mine", json={})
        assert r.status_code == 422

    def test_mine_valid_request_accepted(self, miner):
        """Miner may fail (API key), but should not return 422 with correct format"""
        r = miner.post("/mine", json={"keyword": "restaurant", "limit": 1})
        # 200 (success) or 5xx (external API error) are acceptable, 422 is not
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
# §9  SALES-OUTREACH — /crm/sync  (critical bug fix verification)
# ═════════════════════════════════════════════

class TestCRMSync:

    def test_crm_sync_returns_200_not_500(self, outreach):
        """
        Critical fix verification: postgres_loader.py e.metadata → r.metadata
        Before fix: triggered UndefinedColumnError -> 500
        After fix: returns 200 normally, indicating CRM provider not configured
        """
        r = outreach.post("/crm/sync", json={"min_score": 0, "limit": 1})
        assert r.status_code == 200, \
            f"CRM sync should not crash — e.metadata bug fixed, actual status: {r.status_code}"

    def test_crm_sync_response_not_empty(self, outreach):
        data = outreach.post("/crm/sync", json={"min_score": 0, "limit": 1}).json()
        assert data != {}

    def test_crm_sync_no_provider_message(self, outreach):
        """When CRM is not configured, should return a clear message rather than a server error"""
        data = outreach.post("/crm/sync", json={"min_score": 0, "limit": 1}).json()
        # Allow either message or synced field
        assert "message" in data or "synced" in data

    def test_crm_sync_with_high_min_score(self, outreach):
        """Very high score threshold should return synced=0 (no crash)"""
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
        # dry_run mode should not send emails
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
        """Missing body should use default parameters, should not return 422"""
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
        """Empty body webhook should not 500"""
        r = outreach.post("/webhook/hot-lead", json={})
        assert r.status_code != 500


# ═════════════════════════════════════════════
# §12  Bug fix targeted verification
# ═════════════════════════════════════════════

class TestBugFixVerification:

    # ── Fix #1: postgres_loader.py e.metadata → r.metadata ──────────────────
    def test_crm_sync_does_not_crash_with_sql_error(self, outreach):
        """
        Before fix: leads_enriched had no metadata column, querying e.metadata caused 500
        After fix: querying r.metadata (leads_raw) returns normally
        """
        r = outreach.post("/crm/sync", json={"min_score": 0, "limit": 5})
        assert r.status_code in (200, 202), \
            "CRM sync should not throw SQL 500 error (e.metadata bug fixed)"

    # ── Fix #2: rate_limit.py X-Forwarded-For ───────────────────────────────
    def test_xff_header_does_not_break_rate_limit(self, miner):
        """
        Before fix: all requests shared 'anonymous' bucket when request.client=None, losing IP isolation
        After fix: X-Forwarded-For header is preferred
        """
        r = miner.get(
            "/leads",
            headers={"X-Forwarded-For": "1.2.3.4"},
        )
        # Main assertion: requests with XFF header should not cause 500
        assert r.status_code in (200, 429), \
            "Requests with X-Forwarded-For header should not cause 500"

    # ── Fix #3: facebook_miner review_count None semantics ───────────────────
    def test_leads_review_count_can_be_null(self, miner):
        """
        Before fix: review_count=None → 0 (losing 'no data' semantics)
        After fix: review_count preserves None or real value
        Verify: check that collected leads may have review_count as null (if any)
        """
        data = miner.get("/leads", params={"limit": 50}).json()
        # Only verify review_count field type is correct (int or null), not required to have null
        for lead in data["leads"]:
            rc = lead.get("review_count")
            assert rc is None or isinstance(rc, (int, float)), \
                f"review_count should be int or null, got: {type(rc)}"

    # ── Fix #4: browser_miner context leak (cannot verify directly, verify health response is normal)
    def test_health_miners_section_present(self, miner):
        """After BrowserContext fix, miners list in /health should still return normally"""
        data = miner.get("/health").json()
        assert "miners" in data

    # ── Fix #5: asyncpg pool close (verify service responds normally after healthy restart) ──
    def test_outreach_healthy_after_restart(self, outreach):
        """After close_db_pool() fix, restarted service process should respond normally"""
        r = outreach.get("/health")
        assert r.status_code == 200
        assert r.json().get("status") == "ok"


# ═════════════════════════════════════════════
# §13  Cross-service data consistency verification
# ═════════════════════════════════════════════

class TestDataConsistency:

    def test_lead_count_consistent(self, miner):
        """stats.raw_total should match /leads total"""
        stats = miner.get("/stats").json()
        raw_total = stats["leads"]["raw_total"]
        # Fetch in batches (max limit=200)
        leads_data = miner.get("/leads", params={"limit": 200}).json()
        assert "total" in leads_data, "/leads response missing total field"
        assert leads_data["total"] == raw_total, \
            f"stats.raw_total={raw_total} inconsistent with leads.total={leads_data['total']}"

    def test_enriched_count_consistent(self, miner):
        """stats.enriched_total should be <= raw_total (data integrity check)"""
        stats = miner.get("/stats").json()
        raw_total = stats["leads"]["raw_total"]
        enriched_total = stats["leads"]["enriched_total"]
        assert 0 <= enriched_total <= raw_total, \
            f"enriched_total={enriched_total} should be in range [0, raw_total={raw_total}]"

    def test_chroma_docs_positive(self, miner):
        """ChromaDB should have vectorized documents (indicating enrichment wrote successfully)"""
        data = miner.get("/health").json()
        assert data["chroma_docs"] > 0, "ChromaDB should have documents"

    def test_scoring_stats_industry_in_leads(self, miner):
        """Industries in scoring/stats should have corresponding records in leads"""
        scoring = miner.get("/scoring/stats").json()
        industries_in_scoring = {i["industry"] for i in scoring["industries"]}
        leads = miner.get("/leads", params={"limit": 50}).json()["leads"]
        if leads and industries_in_scoring:
            lead_industries = {l.get("industry_category") for l in leads if l.get("industry_category")}
            # At least one industry should overlap
            overlap = industries_in_scoring & lead_industries
            assert len(overlap) > 0, \
                f"scoring stats industries {industries_in_scoring} have no overlap with leads industries {lead_industries}"

    def test_outreach_dry_run_count_le_leads_total(self, miner, outreach):
        """outreach dry_run returned lead_count should not exceed total leads in database"""
        total = miner.get("/stats").json()["leads"]["raw_total"]
        outreach_count = outreach.post(
            "/outreach/run", json={"limit": 200, "dry_run": True}
        ).json().get("lead_count", 0)
        assert outreach_count <= total, \
            f"outreach lead_count={outreach_count} exceeds total leads={total}"
