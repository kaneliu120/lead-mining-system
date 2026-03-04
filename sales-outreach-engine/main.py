"""
Sales Outreach Engine — FastAPI Entry Point
Adapted from sales-outreach-automation-langgraph, integrated with PostgresLeadLoader
"""
from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from pydantic import BaseModel
from typing import Optional, List

from src.tools.leads_loader.postgres_loader import PostgresLeadLoader
from src.tools.crm_sync import get_crm_adapter, sync_leads_to_crm, CRM_PROVIDER
from src.nodes import close_db_pool

logger = logging.getLogger(__name__)


class AppState:
    lead_loader: PostgresLeadLoader


state = AppState()


@asynccontextmanager
async def lifespan(app: FastAPI):
    state.lead_loader = PostgresLeadLoader(
        dsn=os.environ.get(
            "DATABASE_URL",
            "postgresql://postgres:postgres@postgres:5432/leads",
        )
    )
    await state.lead_loader.connect()
    logger.info("Sales Outreach Engine ready")
    yield
    await state.lead_loader.close()
    await close_db_pool()     # Close the global connection pool used by log_outreach


app = FastAPI(
    title="Sales Outreach Engine",
    version="2.0.0",
    lifespan=lifespan,
)


class OutreachRequest(BaseModel):
    limit:            int           = 10
    min_score:        int           = 60
    industry_keyword: Optional[str] = None
    dry_run:          bool          = False
    language:         str           = "mixed"   # P3-4: 'en' | 'fil' | 'mixed'


@app.post("/outreach/run")
async def run_outreach(req: OutreachRequest):
    """
    Trigger a round of outreach tasks:
    1. Read pending outreach leads from PostgreSQL
    2. Call LangGraph workflow to generate personalized emails
    3. Send emails and record outreach_log
    """
    leads = await state.lead_loader.get_pending_leads(
        limit=req.limit,
        min_score=req.min_score,
        industry_keyword=req.industry_keyword,
    )

    if not leads:
        return {"message": "No pending leads found", "processed": 0}

    if req.dry_run:
        return {
            "message": "Dry run — no emails sent",
            "lead_count": len(leads),
            "dry_run": True,
            "leads_preview": [
                {
                    "id":             l["id"],
                    "business_name":  l["business_name"],
                    "score":          l["score"],
                    "outreach_angle": l.get("outreach_angle", ""),
                }
                for l in leads[:5]
            ],
        }

    # Execute LangGraph outreach workflow
    from src.graph import get_graph
    graph = get_graph()

    sent = skipped = failed = 0
    results = []

    for lead in leads:
        initial_state = {
            "lead":        lead,
            "language":    req.language,    # P3-4: pass language option
            "rag_context": None,
            "contacts":    [],
            "email_draft": None,
            "send_result": None,
            "error":       None,
            "skipped":     False,
            "messages":    [],
        }
        try:
            final_state = await graph.ainvoke(initial_state)
        except Exception as exc:
            logger.error(f"Graph failed for lead {lead['id']}: {exc}")
            failed += 1
            results.append({"lead_id": lead["id"], "status": "error", "error": str(exc)})
            continue

        if final_state.get("skipped"):
            skipped += 1
            results.append({"lead_id": lead["id"], "status": "skipped"})
        elif final_state.get("send_result", {}).get("status") == "sent":
            sent += 1
            results.append({
                "lead_id": lead["id"],
                "status":  "sent",
                "to":      final_state["send_result"]["to"],
            })
        else:
            failed += 1
            results.append({
                "lead_id": lead["id"],
                "status":  "failed",
                "error":   final_state.get("error", "unknown"),
            })

    return {
        "message":    "Outreach completed",
        "lead_count": len(leads),
        "sent":       sent,
        "skipped":    skipped,
        "failed":     failed,
        "results":    results[:20],
    }


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/webhook/outreach-summary")
async def webhook_outreach_summary(request: Request):
    """Receive outreach summary notification from n8n workflow 03"""
    body = await request.json()
    logger.info(f"[webhook] outreach-summary: {body}")
    return {
        "status": "received",
        "sent":      body.get("sent", 0),
        "skipped":   body.get("skipped", 0),
        "failed":    body.get("failed", 0),
        "timestamp": body.get("timestamp", ""),
    }


@app.post("/webhook/hot-lead")
async def webhook_hot_lead(request: Request):
    """Receive hot lead alert from n8n workflow 04"""
    body = await request.json()
    logger.info(f"[webhook] HOT LEAD ALERT: {body}")
    return {
        "status":           "received",
        "from":             body.get("from", ""),
        "intent":           body.get("intent", ""),
        "suggested_action": body.get("suggested_action", ""),
    }


# ─────────────────────────────────────────────────────────────────────────────
# P3-5: CRM integration endpoints (HubSpot / Pipedrive)
# ─────────────────────────────────────────────────────────────────────────────
class CRMSyncRequest(BaseModel):
    min_score:        float          = 60.0
    industry_keyword: Optional[str]  = None
    limit:            int            = 50
    lead_ids:         Optional[List[int]] = None    # Optional specified ID list


@app.post("/crm/sync")
async def crm_sync(req: CRMSyncRequest):
    """
    P3-5: Batch push enriched leads to configured CRM (HubSpot / Pipedrive).

    Configuration (.env):
      CRM_PROVIDER=hubspot            # or pipedrive
      HUBSPOT_ACCESS_TOKEN=<token>    # HubSpot Private App Token
      PIPEDRIVE_API_TOKEN=<token>     # Pipedrive API Token
      PIPEDRIVE_COMPANY_DOMAIN=<dom>  # e.g. mycompany.pipedrive.com
    """
    adapter = get_crm_adapter()
    if adapter is None:
        return {
            "message": f"CRM provider not configured (CRM_PROVIDER={CRM_PROVIDER})",
            "synced": 0,
            "hint": "Set CRM_PROVIDER=hubspot or CRM_PROVIDER=pipedrive in .env",
        }

    # Load leads to sync from database
    leads = await state.lead_loader.get_enriched_leads(
        min_score=req.min_score,
        industry_keyword=req.industry_keyword,
        limit=req.limit,
        lead_ids=req.lead_ids,
    )

    if not leads:
        return {"message": "No leads to sync", "synced": 0}

    result = await sync_leads_to_crm(leads, adapter=adapter)
    return {"message": "CRM sync completed", **result}


@app.get("/crm/health")
async def crm_health():
    """Check CRM connectivity."""
    adapter = get_crm_adapter()
    if adapter is None:
        return {
            "provider": CRM_PROVIDER,
            "healthy":  False,
            "message":  f"CRM_PROVIDER={CRM_PROVIDER} (not configured)",
        }
    ok = await adapter.health_check()
    return {
        "provider": CRM_PROVIDER,
        "healthy":  ok,
        "message":  "Connected" if ok else "Connection failed",
    }
