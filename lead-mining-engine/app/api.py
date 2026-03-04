"""
FastAPI service layer — Lead Mining Engine external interface
Provides /mine, /health, /leads, /rag/query, /export endpoints
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import os
import secrets
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query, BackgroundTasks, Body, Request, Response, Cookie
from fastapi.responses import StreamingResponse, HTMLResponse, RedirectResponse
from pydantic import BaseModel, Field
from app.middleware import RateLimitMiddleware, CSRFMiddleware

from app.config import (
    build_orchestrator,
    build_postgres_writer,
    build_chroma_writer,
    build_gemini_enricher,
    FACTORY,
)
from app.enrichers.gemini_enricher import GeminiEnricher
from app.orchestrator import MiningOrchestrator, MiningTask
from app.writers.postgres_writer import PostgresWriter
from app.writers.chroma_writer import ChromaWriter
from app.writers.csv_writer import CsvWriter

logger = logging.getLogger(__name__)

# ── Application state container ──────────────────────────────────────────
class AppState:
    orchestrator: MiningOrchestrator
    pg: PostgresWriter
    chroma: ChromaWriter
    enricher: Optional[GeminiEnricher] = None


state = AppState()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Startup ──
    logger.info("Lead Mining Engine starting up...")

    state.pg = build_postgres_writer()
    await state.pg.connect()
    await state.pg.init_tables()

    state.chroma = build_chroma_writer()
    state.chroma.connect()

    try:
        state.enricher = build_gemini_enricher()
    except ValueError as exc:
        logger.warning(f"GeminiEnricher disabled: {exc}")

    state.orchestrator = build_orchestrator()
    await state.orchestrator.startup()

    logger.info("Lead Mining Engine ready")
    yield

    # ── Shutdown ──
    await state.orchestrator.shutdown()
    await state.pg.close()
    logger.info("Lead Mining Engine shut down")


app = FastAPI(
    title="Lead Mining Engine",
    version="2.0.0",
    description="Philippines SME Lead Mining & Enrichment API",
    lifespan=lifespan,
)

# P2-6: Rate limiting middleware (token bucket, prevent single-IP abuse)
app.add_middleware(RateLimitMiddleware, enabled=True)
# CSRF protection middleware (Origin/Referer validation, prevent CSRF)
app.add_middleware(CSRFMiddleware, enabled=True)

# ══════════════════════════════════════════════════════════════════════════════
# Admin auth — HMAC-signed cookie
# Set ADMIN_PASSWORD env var to change login password (required, no default)
# Set ADMIN_SECRET env var to change signing key (required, no default)
# ══════════════════════════════════════════════════════════════════════════════
_ADMIN_SECRET   = os.environ.get("ADMIN_SECRET",   "")
_ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "")

if not _ADMIN_SECRET:
    raise RuntimeError("ADMIN_SECRET env var not set — startup refused")
if not _ADMIN_PASSWORD:
    raise RuntimeError("ADMIN_PASSWORD env var not set — startup refused")
_SESSION_COOKIE   = "admin_session"
_SESSION_TTL      = 86400  # 24 hours


def _make_session_token() -> str:
    """Generate a timestamped HMAC-signed session token"""
    ts  = str(int(time.time()))
    sig = hmac.new(_ADMIN_SECRET.encode(), ts.encode(), hashlib.sha256).hexdigest()
    return f"{ts}.{sig}"


def _verify_session(token: str | None) -> bool:
    """Verify session token signature and expiry"""
    if not token:
        return False
    try:
        ts_str, sig = token.split(".", 1)
        expected = hmac.new(_ADMIN_SECRET.encode(), ts_str.encode(), hashlib.sha256).hexdigest()
        if not secrets.compare_digest(sig, expected):
            return False
        if time.time() - int(ts_str) > _SESSION_TTL:
            return False
        return True
    except Exception:
        return False


_LOGIN_HTML = """<!DOCTYPE html>
<html lang="en" id="login-root">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Lead Mining Console — Login</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
  background:#0f172a;color:#e2e8f0;display:flex;align-items:center;
  justify-content:center;min-height:100vh}}
.card{{background:#1e293b;border-radius:14px;padding:40px 36px;width:100%;max-width:380px;
  box-shadow:0 20px 60px rgba(0,0,0,.5)}}
.logo{{font-size:1.6rem;font-weight:700;color:#38bdf8;text-align:center;margin-bottom:6px}}
.sub{{font-size:.82rem;color:#64748b;text-align:center;margin-bottom:28px}}
label{{display:block;font-size:.75rem;color:#94a3b8;margin-bottom:6px}}
input[type=password]{{width:100%;background:#0f172a;border:1px solid #334155;
  border-radius:8px;padding:10px 14px;color:#e2e8f0;font-size:.9rem;margin-bottom:20px}}
input[type=password]:focus{{outline:none;border-color:#38bdf8;box-shadow:0 0 0 2px rgba(56,189,248,.2)}}
.btn-login{{width:100%;background:#1d4ed8;color:#fff;border:none;border-radius:8px;
  padding:11px;font-size:.9rem;font-weight:600;cursor:pointer;transition:background .2s;margin-bottom:10px}}
.btn-login:hover{{background:#1e40af}}
.err{{background:#3b1f1f;color:#fca5a5;border-radius:8px;padding:10px 14px;
  font-size:.82rem;margin-bottom:16px;display:none}}
.lang-row{{display:flex;justify-content:flex-end;margin-bottom:18px;gap:6px}}
.lang-btn{{background:#0f172a;border:1px solid #334155;color:#64748b;border-radius:6px;
  padding:3px 10px;font-size:.72rem;cursor:pointer;transition:all .2s}}
.lang-btn.active{{background:#1d4ed8;border-color:#1d4ed8;color:#fff}}
.lang-btn:hover:not(.active){{background:#334155;color:#e2e8f0}}
</style>
</head>
<body>
<div class="card">
  <div class="lang-row">
    <button class="lang-btn active" id="login-btn-en" onclick="loginSetLang('en')">EN</button>
    <button class="lang-btn" id="login-btn-zh" onclick="loginSetLang('zh')">Chinese</button>
  </div>
  <div class="logo">&#9889; Lead Mining</div>
  <div class="sub" id="login-sub">Console Login</div>
  {err_block}
  <form method="POST" action="/login">
    <label id="pw-label">Admin Password</label>
    <input type="password" name="password" id="pw-input" placeholder="Enter password…" autofocus required>
    <button type="submit" class="btn-login" id="btn-submit">Log In</button>
  </form>
</div>
<script>
const _LL={{en:{{sub:'Console Login',pwLabel:'Admin Password',pwPlaceholder:'Enter password\u2026',submit:'Log In',errWrong:'Incorrect password, please try again'}},zh:{{sub:'Console Login',pwLabel:'Admin Password',pwPlaceholder:'Enter password\u2026',submit:'Log In',errWrong:'Incorrect password, please try again'}}}};
let _loginLang=localStorage.getItem('lang')||'en';
function loginSetLang(l){{_loginLang=l;localStorage.setItem('lang',l);const d=_LL[l]||_LL.en;document.getElementById('login-sub').textContent=d.sub;document.getElementById('pw-label').textContent=d.pwLabel;document.getElementById('pw-input').placeholder=d.pwPlaceholder;document.getElementById('btn-submit').textContent=d.submit;document.getElementById('login-btn-en').classList.toggle('active',l==='en');document.getElementById('login-btn-zh').classList.toggle('active',l==='zh');const errEl=document.getElementById('loginErr');if(errEl&&errEl.dataset.show)errEl.textContent=d.errWrong;}}
loginSetLang(_loginLang);
</script>
</body></html>"""



# ── Request / Response models ─────────────────────────────────────────────────
class MineRequest(BaseModel):
    keyword:  str    = Field(..., example="restaurant")
    location: str    = Field("Philippines", example="Manila, Philippines")
    limit:    int    = Field(100, ge=1, le=500)
    sources:  Optional[List[str]] = None
    enrich:   bool   = Field(False, description="Whether to immediately trigger Gemini enrichment")
    min_score: int   = Field(0, ge=0, le=100, description="Filter score after enrichment")


class MineResponse(BaseModel):
    task_id:        str
    total:          int
    dedup_removed:  int
    source_counts:  Dict[str, int]
    duration_sec:   float
    errors:         Dict[str, str]
    leads:          List[Dict[str, Any]]


class RagQueryRequest(BaseModel):
    query:   str
    n:       int = Field(10, ge=1, le=50)
    where:   Optional[Dict[str, Any]] = None


# ── Routes ─────────────────────────────────────────────────────────────────────
@app.post("/mine", response_model=MineResponse, tags=["Mining"])
async def mine(req: MineRequest, background_tasks: BackgroundTasks):
    """
    Trigger a single mining task.
    - Concurrently calls all enabled Miners
    - Auto-deduplicate
    - Write to PostgreSQL + ChromaDB
    - Optional immediate enrichment (synchronous)
    """
    task = MiningTask(
        keyword=req.keyword,
        location=req.location,
        limit=req.limit,
        sources=req.sources,
    )
    result = await state.orchestrator.run_task(task)

    # Write to PostgreSQL
    if result.leads:
        inserted, skipped = await state.pg.upsert_leads(result.leads)
        logger.info(f"/mine: pg upsert {inserted} new, {skipped} skipped")

    # Write to ChromaDB (background task, non-blocking)
    background_tasks.add_task(state.chroma.upsert_leads, result.leads)

    # Optional enrichment
    enriched_leads = []
    if req.enrich and state.enricher and result.leads:
        enriched_leads = await state.enricher.enrich_batch(
            result.leads, skip_below_score=req.min_score
        )
        if enriched_leads:
            await state.pg.upsert_enriched(enriched_leads)
            background_tasks.add_task(state.chroma.upsert_leads, enriched_leads)

    final_leads = enriched_leads if req.enrich else result.leads
    return MineResponse(
        task_id=str(uuid.uuid4()),
        total=result.total,
        dedup_removed=result.dedup_removed,
        source_counts=result.source_counts,
        duration_sec=round(result.duration_seconds, 2),
        errors=result.errors,
        leads=[_lead_to_dict(l) for l in final_leads],
    )


@app.get("/leads", tags=["Leads"])
async def list_leads(
    keyword:    Optional[str] = Query(None),
    min_score:  int           = Query(0, ge=0, le=100),
    source:     Optional[str] = Query(None),
    lead_id:    Optional[int] = Query(None, description="Exact lookup of a single lead by ID"),
    limit:      int           = Query(50, ge=1, le=200),
    offset:     int           = Query(0, ge=0),
):
    """Query stored leads (supports id / keyword / source / min_score filtering)"""
    rows = await state.pg.query_leads(
        industry_keyword=keyword,
        min_score=min_score,
        source=source,
        lead_id=lead_id,
        limit=limit,
        offset=offset,
    )
    return {"total": len(rows), "leads": rows}


class EnrichRequest(BaseModel):
    limit:            int           = Field(50, ge=1, le=200, description="Maximum number of un-enriched leads to process")
    min_score:        int           = Field(0, ge=0, le=100, description="Minimum score to keep after enrichment")
    industry_keyword: Optional[str] = None


@app.post("/enrich", tags=["Enrichment"])
async def enrich_leads(req: EnrichRequest, background_tasks: BackgroundTasks):
    """
    Batch-enrich leads in the database that have not yet been AI-enriched.
    - No re-mining, does not consume Serper/Apollo quota
    - Called periodically by n8n workflow 02
    """
    if not state.enricher:
        raise HTTPException(status_code=503, detail="GeminiEnricher not configured (GEMINI_API_KEY missing)")

    raw_rows = await state.pg.query_unenriched_leads(
        limit=req.limit,
        industry_keyword=req.industry_keyword,
    )
    if not raw_rows:
        return {"enriched": 0, "total_input": 0, "message": "No unenriched leads found"}

    leads = []
    for r in raw_rows:
        lead = _row_to_lead_raw(r)
        if lead is not None:
            leads.append(lead)
        else:
            logger.warning(f"/enrich: skip row id={r.get('id')}: conversion failed")

    enriched = await state.enricher.enrich_batch(leads, skip_below_score=req.min_score)
    if enriched:
        await state.pg.upsert_enriched(enriched)
        background_tasks.add_task(state.chroma.upsert_leads, enriched)

    logger.info(f"/enrich: {len(enriched)}/{len(leads)} leads enriched")
    return {
        "enriched":    len(enriched),
        "total_input": len(leads),
        "message":     f"Enriched {len(enriched)} of {len(leads)} leads",
    }


class UpdateStatusRequest(BaseModel):
    email:  str
    status: str = Field(..., description="interested|not_interested|auto_reply|unsubscribe|other")


@app.post("/leads/update-status", tags=["Leads"])
async def update_lead_status(req: UpdateStatusRequest):
    """
    Update the reply status of an email address in outreach_log.
    Called by n8n reply-detection workflow (04).
    """
    affected = await state.pg.update_outreach_status(
        email=req.email,
        status=req.status,
    )
    return {"updated": affected, "email": req.email, "status": req.status}


@app.post("/rag/query", tags=["RAG"])
async def rag_query(req: RagQueryRequest):
    """
    Semantic vector search via ChromaDB, returns most relevant lead documents.
    ChromaDB HTTP Client uses synchronous I/O; use asyncio.to_thread to avoid blocking the event loop.
    """
    try:
        results = await asyncio.to_thread(
            state.chroma.query_similar,
            req.query,
            req.n,
            req.where,
        )
    except Exception as exc:
        import logging
        logging.getLogger(__name__).error("RAG query error: %s", exc)
        return {"results": [], "error": str(exc)}
    return {"results": results}


@app.get("/leads/export", tags=["Export"])
async def export_leads(
    keyword:       Optional[str] = Query(None),
    min_score:     int           = Query(50, ge=0, le=100),
    export_format: str           = Query("csv", regex="^(csv|json)$"),
):
    """Export leads as CSV or JSON (for direct use by sales team)"""
    from app.models.lead import EnrichedLead
    rows = await state.pg.query_leads(
        industry_keyword=keyword,
        min_score=min_score,
        limit=1000,
    )

    if export_format == "json":
        return rows

    # CSV streaming response (UTF-8 BOM, Excel-compatible)
    leads = [EnrichedLead(**_row_to_enriched_kwargs(r)) for r in rows if r.get("enriched")]
    csv_content = CsvWriter.write_enriched(leads)

    return StreamingResponse(
        content=iter([csv_content.encode("utf-8-sig")]),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="leads.csv"'},
    )


@app.get("/health", tags=["System"])
async def health():
    """System health check (including all Miner plugin statuses)"""
    miners_health = await state.orchestrator.health_check_all()
    chroma_count  = await asyncio.to_thread(state.chroma.count)
    return {
        "status": "ok",
        "miners": {
            name: {"healthy": h.healthy, "message": h.message, "latency_ms": h.latency_ms}
            for name, h in miners_health.items()
        },
        "chroma_docs": chroma_count,
    }


@app.get("/stats", tags=["System"])
async def stats():
    """Funnel stats: raw → enriched → outreached"""
    d = await state.pg.fetch_funnel_stats()
    raw    = int(d["raw_total"] or 0)
    enr    = int(d["enriched_total"] or 0)
    out    = int(d["outreached_total"] or 0)
    return {
        "leads": {
            "raw_total":       raw,
            "raw_with_email":  int(d["raw_with_email"] or 0),
            "enriched_total":  enr,
            "enrichment_rate": f"{round(enr/raw*100, 1) if raw else 0}%",
            "avg_score":       float(d["avg_score"] or 0),
            "high_score_70":   int(d["high_score"] or 0),
        },
        "outreach": {
            "outreached_total":  out,
            "emails_sent":       int(d["emails_sent"] or 0),
            "conversion_rate":   f"{round(out/enr*100, 1) if enr else 0}%",
        },
        "chroma_docs": await asyncio.to_thread(state.chroma.count),
    }


@app.get("/dashboard", response_class=HTMLResponse, tags=["System"])
async def dashboard_legacy():
    """Legacy dashboard — redirect to new /admin"""    
    return RedirectResponse(url="/admin", status_code=301)


@app.get("/_old_dashboard_html", include_in_schema=False)
async def dashboard_html_unused():
    """(Deprecated — kept for historical reference)"""  # noqa
    return """<!DOCTYPE html>\n<!-- UNUSED LEGACY -->
<html lang="zh">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Lead Mining Dashboard</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js"></script>
  <style>
    *{box-sizing:border-box;margin:0;padding:0}
    body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0f172a;color:#e2e8f0;padding:24px}
    h1{font-size:1.5rem;margin-bottom:4px;color:#38bdf8}
    .sub{font-size:.85rem;color:#64748b;margin-bottom:24px}
    .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:14px;margin-bottom:24px}
    .card{background:#1e293b;border-radius:10px;padding:18px;text-align:center}
    .card .val{font-size:2rem;font-weight:700;color:#38bdf8}
    .card .lbl{font-size:.78rem;color:#94a3b8;margin-top:4px}
    .card .sub2{font-size:.72rem;color:#475569;margin-top:2px}
    .charts{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:24px}
    .chart-box{background:#1e293b;border-radius:10px;padding:18px}
    .chart-box h3{font-size:.85rem;color:#94a3b8;margin-bottom:10px}
    table{width:100%;border-collapse:collapse;background:#1e293b;border-radius:10px;overflow:hidden}
    th{background:#0f172a;color:#64748b;font-size:.72rem;text-transform:uppercase;padding:10px 12px;text-align:left}
    td{padding:9px 12px;font-size:.83rem;border-bottom:1px solid #0f172a}
    tr:last-child td{border-bottom:none}
    .badge{display:inline-block;padding:2px 8px;border-radius:999px;font-size:.7rem;font-weight:600}
    .g{background:#064e3b;color:#34d399}.b{background:#1e3a5f;color:#60a5fa}.r{background:#3b1f1f;color:#fca5a5}
    .ts{font-size:.72rem;color:#475569;text-align:right;margin-bottom:6px}
    .sec{font-size:.95rem;color:#94a3b8;margin:20px 0 10px;font-weight:600}
    @media(max-width:640px){.charts{grid-template-columns:1fr}}
  </style>
</head>
<body>
  <h1>&#127919; Lead Mining Dashboard</h1>
  <p class="sub">Philippines SME &#8226; Auto-refresh every 60s</p>
  <p class="ts" id="ts">Loading...</p>
  <div class="grid" id="cards"></div>
  <div class="charts">
    <div class="chart-box"><h3>&#128200; Funnel Conversion</h3><canvas id="fc" height="200"></canvas></div>
    <div class="chart-box"><h3>&#11088; Top Leads Score</h3><canvas id="sc" height="200"></canvas></div>
  </div>
  <p class="sec">&#128229; Top 15 Enriched Leads</p>
  <table><thead><tr><th>#</th><th>Company</th><th>Industry</th><th>Address</th><th>Score</th><th>Status</th></tr></thead>
  <tbody id="tb"></tbody></table>
<script>
async function load(){
  const [stats,leads]=await Promise.all([
    fetch('/stats').then(r=>r.json()).catch(()=>({})),
    fetch('/leads?min_score=0&limit=15').then(r=>r.json()).catch(()=>({leads:[]}))
  ]);
  const s=stats.leads||{},o=stats.outreach||{};
  document.getElementById('ts').textContent=t('lastUpd')+new Date().toLocaleTimeString(_lang==='zh'?'zh-CN':'en-US');
  document.getElementById('cards').innerHTML=[
    {v:s.raw_total||0,l:t('cardRaw'),s2:t('cardRawSub')},
    {v:s.enriched_total||0,l:t('cardEnr'),s2:t('cardEnrPre')+(s.enrichment_rate||'0%')},
    {v:+(s.avg_score||0).toFixed(1),l:'Avg Score',s2:'Out of 100'},
    {v:s.high_score_70||0,l:t('cardHigh'),s2:t('cardHighSub')},
    {v:o.emails_sent||0,l:t('cardEmails'),s2:''},
    {v:s.raw_with_email||0,l:'Leads w/ Email',s2:'Ready for outreach'},
  ].map(c=>`<div class="card"><div class="val">${c.v}</div><div class="lbl">${c.l}</div><div class="sub2">${c.s2}</div></div>`).join('');
  const fc=document.getElementById('fc');
  if(window._fc)window._fc.destroy();
  window._fc=new Chart(fc,{type:'bar',data:{labels:['Raw','Enriched','70+','Outreached'],
    datasets:[{data:[s.raw_total||0,s.enriched_total||0,s.high_score_70||0,o.emails_sent||0],
    backgroundColor:['#1d4ed8','#7c3aed','#059669','#d97706'],borderRadius:6,borderSkipped:false}]},
    options:{plugins:{legend:{display:false}},scales:{y:{grid:{color:'#1e293b'},ticks:{color:'#64748b'}},x:{ticks:{color:'#94a3b8'}}}}});
  const top=(leads.leads||[]).filter(l=>l.score>0).sort((a,b)=>b.score-a.score).slice(0,15);
  const sc=document.getElementById('sc');
  if(window._sc)window._sc.destroy();
  window._sc=new Chart(sc,{type:'bar',data:{
    labels:top.map(l=>(l.business_name||'').substring(0,14)),
    datasets:[{data:top.map(l=>l.score),
    backgroundColor:top.map(l=>l.score>=70?'#059669':l.score>=50?'#1d4ed8':'#991b1b'),
    borderRadius:4,borderSkipped:false}]},
    options:{indexAxis:'y',plugins:{legend:{display:false}},
    scales:{x:{max:100,grid:{color:'#0f172a'},ticks:{color:'#64748b'}},y:{ticks:{color:'#94a3b8',font:{size:10}}}}}});
  const tb=document.getElementById('tb');
  if(!top.length){tb.innerHTML='<tr><td colspan=6 style="text-align:center;color:#475569;padding:28px">No enriched data</td></tr>';return;}
  tb.innerHTML=top.map((l,i)=>{
    const cls=l.score>=70?'g':l.score>=50?'b':'r';
    return`<tr><td>${i+1}</td><td>${l.business_name||''}</td><td>${l.industry_category||l.industry_keyword||''}</td><td style="color:#64748b;font-size:.78rem">${(l.address||'').substring(0,28)}</td><td><span class="badge ${cls}">${l.score}</span></td><td><span class="badge ${l.outreached?'g':'b'}">${l.outreached?'Outreached':'Pending'}</span></td></tr>`;
  }).join('');
}
load();setInterval(load,60000);
</script></body></html>"""


# ── P2-5: Score feedback & model calibration ──────────────────────────────────

_VALID_OUTCOMES = frozenset({"replied", "converted", "bounced", "ignored", "unsubscribed"})


@app.post("/leads/{lead_id}/feedback", tags=["Scoring"])
async def submit_lead_feedback(
    lead_id: int,
    outcome: str = Body(..., embed=True,
                       description="replied | converted | bounced | ignored | unsubscribed"),
    notes: str   = Body("", embed=True),
):
    """
    P2-5: Record outreach result for scoring model optimization.
    - replied / converted → positive signal (score weight for this lead type should increase)
    - bounced / ignored   → negative signal
    - unsubscribed        → must be removed from the list
    """
    if outcome not in _VALID_OUTCOMES:
        raise HTTPException(400, f"outcome must be one of {sorted(_VALID_OUTCOMES)}")
    try:
        await state.pg.insert_feedback(lead_id, outcome, notes)
    except Exception as exc:
        err_msg = str(exc)
        # FK constraint violation → lead_id does not exist
        if "foreign key" in err_msg.lower() or "ForeignKeyViolationError" in type(exc).__name__:
            raise HTTPException(404, f"Lead {lead_id} not found")
        raise HTTPException(500, f"Failed to write feedback: {exc}")
    return {"ok": True, "lead_id": lead_id, "outcome": outcome}


@app.get("/scoring/stats", tags=["Scoring"])
async def scoring_stats():
    """
    P2-5: Statistics on score and conversion rate by industry, to optimize minimum outreach score threshold.
    Returns avg score, positive reply count, conversion rate per industry to identify high-value segments.
    """
    summary, total_feedback = await state.pg.fetch_scoring_stats()
    return {
        "industries":        summary,
        "feedback_total":    total_feedback.get("total", 0),
        "feedback_positive": total_feedback.get("positive", 0),
        "hint": "Use conversion_rate_pct to calibrate min_score per industry.",
    }


@app.post("/scoring/recalibrate", tags=["Scoring"])
async def scoring_recalibrate(min_positives: int = Body(3, embed=True)):
    """
    P2-5: Re-enrich leads with low scores but positive outreach feedback, correcting score bias.
    Trigger condition: >= min_positives positive feedback entries and current score < 65.
    """
    if state.enricher is None:
        raise HTTPException(503, "GeminiEnricher not initialized — check GEMINI_API_KEY")

    # Find leads with positive feedback but low scores
    rows = await state.pg.fetch_low_score_positive_leads(min_positives=min_positives)
    if not rows:
        return {"ok": True, "recalibrated": 0, "message": "No leads need recalibration"}

    # Convert rows to LeadRaw objects and re-enrich
    leads_to_recalibrate = [
        lead for r in rows
        if (lead := _row_to_lead_raw(r)) is not None
        or logger.warning(f"/scoring/recalibrate: skip lead id={r.get('id')}: conversion failed")
    ]

    if not leads_to_recalibrate:
        return {"ok": True, "recalibrated": 0, "message": "Lead data conversion failed"}

    enriched = await state.enricher.enrich_batch(leads_to_recalibrate, skip_below_score=0)
    if enriched:
        await state.pg.upsert_enriched(enriched)

    return {
        "ok":           True,
        "recalibrated": len(enriched),
        "total_found":  len(rows),
        "message":      f"Re-calibrated {len(enriched)} leads with Gemini scoring (original avg score < 65, positive feedback >= {min_positives})",
    }


# ── Helper functions ─────────────────────────────────────────────────────────
def _lead_to_dict(lead) -> dict:
    try:
        return lead.model_dump(mode="json")
    except AttributeError:
        return lead.__dict__


def _row_to_enriched_kwargs(row: dict) -> dict:
    """Convert PostgreSQL row to EnrichedLead constructor kwargs"""
    import json as _json
    from app.models.lead import LeadSource
    return {
        "source":               LeadSource(row.get("source", "serper")),
        "business_name":        row.get("business_name", ""),
        "industry_keyword":     row.get("industry_kw", ""),
        "address":              row.get("address"),
        "phone":                row.get("phone"),
        "website":              row.get("website"),
        "email":                row.get("email"),
        "rating":               row.get("rating"),
        "review_count":         row.get("review_count"),
        "lat":                  row.get("lat"),
        "lng":                  row.get("lng"),
        "google_maps_url":      row.get("gmaps_url"),
        "metadata":             _json.loads(row.get("metadata") or "{}") if isinstance(row.get("metadata"), str) else (row.get("metadata") or {}),
        "industry_category":    row.get("industry_category", ""),
        "business_size":        row.get("business_size", ""),
        "pain_points":          _json.loads(row.get("pain_points") or "[]"),
        "value_proposition":    row.get("value_proposition", ""),
        "recommended_product":  row.get("recommended_product", ""),
        "outreach_angle":       row.get("outreach_angle", ""),
        "score":                row.get("score", 0) or 0,
        "score_reason":         row.get("score_reason", ""),
        "best_contact_time":    row.get("best_contact_time", ""),
        "enriched":             row.get("enriched", False),
    }

# ══════════════════════════════════════════════════════════════════════════════
# Admin panel API — /admin/*
# ══════════════════════════════════════════════════════════════════════════════

# Descriptors for all configurable API Keys
_API_KEY_DEFS = [
    {"key": "GEMINI_API_KEY",           "label": "Gemini API Key",          "hint": "AI Enrichment Engine — Required",           "hint_en": "AI Enrichment Engine — Required",              "url": "https://aistudio.google.com/app/apikey"},
    {"key": "SERPER_API_KEY",           "label": "Serper API Key",          "hint": "Google Search Proxy (Phase 1)",   "hint_en": "Google Search Proxy (Phase 1)",                 "url": "https://serper.dev"},
    {"key": "HUNTER_API_KEY",           "label": "Hunter.io API Key",       "hint": "Email Discovery (Phase 1)",          "hint_en": "Email Discovery (Phase 1)",                     "url": "https://hunter.io"},
    {"key": "GOOGLE_API_KEY",           "label": "Google API Key",          "hint": "Google CSE Search (Phase 2)",   "hint_en": "Google CSE Search (Phase 2)",                   "url": "https://console.cloud.google.com"},
    {"key": "GOOGLE_CSE_CX",            "label": "Google CSE CX",           "hint": "Custom Search Engine ID",     "hint_en": "Custom Search Engine ID",                       "url": "https://programmablesearchengine.google.com"},
    {"key": "REDDIT_CLIENT_ID",         "label": "Reddit Client ID",        "hint": "Reddit Mining (Phase 2)",       "hint_en": "Reddit Mining (Phase 2)",                       "url": "https://www.reddit.com/prefs/apps"},
    {"key": "REDDIT_CLIENT_SECRET",     "label": "Reddit Client Secret",    "hint": "Reddit Authentication Secret",             "hint_en": "Reddit Authentication Secret",                  "url": "https://www.reddit.com/prefs/apps"},
    {"key": "FACEBOOK_ACCESS_TOKEN",    "label": "Facebook Access Token",   "hint": "Facebook Business Search (Phase 3)", "hint_en": "Facebook Business Search (Phase 3)",            "url": "https://developers.facebook.com"},
]

# Miner display names and descriptions
_MINER_DISPLAY = {
    "serper":       {"label": "Serper",       "desc": "Google Search Proxy — Best for Philippines SME",     "desc_en": "Google Search Proxy — Best for Philippines SME",        "phase": 1},
    "hunter":       {"label": "Hunter.io",    "desc": "Email Verification & Discovery",                       "desc_en": "Email Verification & Discovery",                        "phase": 1},
    "google_cse":   {"label": "Google CSE",   "desc": "Google Custom Search Engine",                "desc_en": "Google Custom Search Engine",                           "phase": 2},
    "sec_ph":       {"label": "SEC PH",       "desc": "Philippines SEC Company Registry (No API Key)",       "desc_en": "Philippines SEC Company Registry (No API Key)",         "phase": 2},
    "reddit":       {"label": "Reddit",       "desc": "Reddit Community Lead Mining",                  "desc_en": "Reddit Community Lead Mining",                          "phase": 2},
    "yellow_pages": {"label": "Yellow Pages", "desc": "Yellow Pages Business Data (No API Key)",             "desc_en": "Yellow Pages Business Data (No API Key)",               "phase": 2},
    "philgeps":     {"label": "PhilGEPS",     "desc": "Government Procurement Announcements (No API Key)",             "desc_en": "Government Procurement Announcements (No API Key)",     "phase": 3},
    "facebook":     {"label": "Facebook",     "desc": "Facebook Business Page Search",                "desc_en": "Facebook Business Page Search",                         "phase": 3},
    "dti_bnrs":     {"label": "DTI BNRS",     "desc": "Philippines DTI Business Registration (No API Key)",    "desc_en": "Philippines DTI Business Registration (No API Key)",    "phase": 3},
}


class MinerToggleRequest(BaseModel):
    name:    str
    enabled: bool


class ApiKeyRequest(BaseModel):
    key:   str
    value: str


@app.get("/admin/settings", tags=["Admin"])
async def admin_get_settings():
    """Get all Miner statuses and API Key presence (does not return plaintext keys)"""
    db_settings = await state.pg.get_all_settings()

    miners_out = {}
    for name, meta in FACTORY.items():
        miner = state.orchestrator._miners.get(name)
        if miner is None:
            continue
        # Runtime status or DB override
        db_enabled = db_settings.get(f"miner:{name}:enabled")
        if db_enabled is not None:
            enabled = db_enabled.lower() == "true"
        else:
            enabled = miner.config.enabled

        env_key = meta.get("env_key")
        has_key = bool(os.environ.get(env_key, "").strip()) if env_key else None
        key_in_db = bool(db_settings.get(f"apikey:{env_key}", "").strip()) if env_key else None

        miners_out[name] = {
            "enabled":  enabled,
            "phase":    meta.get("phase", 1),
            "env_key":  env_key,
            "has_key":  has_key or key_in_db,
            "display":  _MINER_DISPLAY.get(name, {"label": name, "desc": "", "phase": 1}),
        }

    api_keys_out = {}
    for d in _API_KEY_DEFS:
        raw = os.environ.get(d["key"], "") or db_settings.get(f"apikey:{d['key']}", "")
        if raw:
            masked = raw[:4] + "****" + raw[-4:] if len(raw) > 8 else "****"
        else:
            masked = ""
        api_keys_out[d["key"]] = {
            "label":    d["label"],
            "hint":     d["hint"],
            "hint_en":  d.get("hint_en", d["hint"]),
            "url":      d["url"],
            "masked":   masked,
            "has_key":  bool(raw),
        }

    return {"miners": miners_out, "api_keys": api_keys_out}


@app.post("/admin/settings/miner-toggle", tags=["Admin"])
async def admin_miner_toggle(req: MinerToggleRequest):
    """Toggle Miner enabled state at runtime (persisted to DB)"""
    miner = state.orchestrator._miners.get(req.name)
    if miner is None:
        raise HTTPException(404, f"Miner '{req.name}' not found")
    miner.config.enabled = req.enabled
    await state.pg.set_setting(f"miner:{req.name}:enabled", str(req.enabled).lower())
    return {"ok": True, "name": req.name, "enabled": req.enabled}


@app.post("/admin/settings/apikey", tags=["Admin"])
async def admin_set_apikey(req: ApiKeyRequest):
    """Update API Key (write to DB + inject into current process env, restored from DB after restart)"""
    allowed = {d["key"] for d in _API_KEY_DEFS}
    if req.key not in allowed:
        raise HTTPException(400, f"Key not allowed for update: {req.key}")
    if not req.value.strip():
        raise HTTPException(400, "value cannot be empty")

    # Inject into runtime env
    os.environ[req.key] = req.value.strip()
    # Persist to DB
    await state.pg.set_setting(f"apikey:{req.key}", req.value.strip())

    # If it's a required Miner Key, auto re-enable the corresponding miner
    for name, meta in FACTORY.items():
        if meta.get("env_key") == req.key:
            miner = state.orchestrator._miners.get(name)
            if miner and not miner.config.enabled:
                miner.config.api_key = req.value.strip()
                miner.config.enabled = True
                await state.pg.set_setting(f"miner:{name}:enabled", "true")

    # If it's the Gemini Key, re-initialize the enricher
    if req.key == "GEMINI_API_KEY" and state.enricher is None:
        try:
            state.enricher = build_gemini_enricher()
            logger.info("GeminiEnricher re-initialized after API key update")
        except Exception as exc:
            logger.warning(f"Failed to re-init GeminiEnricher: {exc}")

    logger.info(f"Admin: API key '{req.key}' updated")
    return {"ok": True, "key": req.key}


@app.on_event("startup")
async def _restore_db_settings():
    """Restore persisted Keys and Miner states from DB on startup (must run after lifespan completes)"""
    # Deferred execution — wait for state initialization in lifespan to complete
    await asyncio.sleep(2)
    try:
        db_settings = await state.pg.get_all_settings()
        for k, v in db_settings.items():
            if k.startswith("apikey:"):
                env_name = k[len("apikey:"):]
                if v and not os.environ.get(env_name):
                    os.environ[env_name] = v
            elif k.startswith("miner:") and k.endswith(":enabled"):
                miner_name = k.split(":")[1]
                miner = state.orchestrator._miners.get(miner_name)
                if miner:
                    miner.config.enabled = v.lower() == "true"
        if db_settings:
            logger.info(f"Admin: restored {len(db_settings)} settings from DB")
    except Exception as exc:
        logger.warning(f"Admin: could not restore DB settings: {exc}")


@app.get("/", include_in_schema=False)
async def root_redirect(
    session: Optional[str] = Cookie(None, alias=_SESSION_COOKIE),
):
    """Root path: authenticated → /admin, unauthenticated → /login"""
    if _verify_session(session):
        return RedirectResponse(url="/admin", status_code=302)
    return RedirectResponse(url="/login", status_code=302)


@app.get("/login", response_class=HTMLResponse, include_in_schema=False)
async def login_page():
    """Admin login page"""
    return _LOGIN_HTML.format(err_block='<div class="err" id="loginErr"></div>')


@app.post("/login", include_in_schema=False)
async def do_login(request: Request):
    """Verify password and set session cookie"""
    form = await request.form()
    password = str(form.get("password", ""))
    if not secrets.compare_digest(password, _ADMIN_PASSWORD):
        err = '<div class="err" id="loginErr" data-show="1" style="display:block"></div>'
        html = _LOGIN_HTML.format(err_block=err)
        return HTMLResponse(content=html, status_code=401)
    token = _make_session_token()
    resp = RedirectResponse(url="/admin", status_code=302)
    resp.set_cookie(
        _SESSION_COOKIE, token,
        httponly=True, samesite="lax",
        max_age=_SESSION_TTL, secure=True,
    )
    return resp


@app.post("/demo-login", include_in_schema=False)
async def demo_login_disabled():
    """Demo entry closed, redirect to login page"""
    return RedirectResponse(url="/login", status_code=302)


@app.get("/logout", include_in_schema=False)
async def logout():
    """Log out and clear session cookie"""
    resp = RedirectResponse(url="/login", status_code=302)
    resp.delete_cookie(_SESSION_COOKIE)
    return resp


@app.get("/admin", response_class=HTMLResponse, tags=["Admin"])
async def admin_panel(
    session: Optional[str] = Cookie(None, alias=_SESSION_COOKIE),
):
    """⚡ Lead Mining unified admin console"""
    is_admin = _verify_session(session)
    if not is_admin:
        return RedirectResponse(url="/login", status_code=302)
    _html = _ADMIN_HTML.replace("/*__DEMO_INIT__*/", "window.DEMO=false;")
    return _html


_ADMIN_HTML = """<!DOCTYPE html>
<html lang="en" id="html-root">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Lead Mining Console</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js"></script>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0f172a;color:#e2e8f0;min-height:100vh}
.header{background:#1e293b;border-bottom:1px solid #334155;padding:16px 24px;display:flex;align-items:center;justify-content:space-between}
.header h1{font-size:1.25rem;font-weight:700;color:#38bdf8;display:flex;align-items:center;gap:8px}
.status-dot{width:8px;height:8px;border-radius:50%;background:#22c55e;display:inline-block;box-shadow:0 0 6px #22c55e}
.tabs{display:flex;background:#1e293b;border-bottom:1px solid #334155;padding:0 24px}
.tab{padding:12px 20px;cursor:pointer;font-size:.875rem;font-weight:500;color:#64748b;border-bottom:2px solid transparent;transition:all .2s}
.tab:hover{color:#94a3b8}
.tab.active{color:#38bdf8;border-bottom-color:#38bdf8}
.content{padding:24px;max-width:1400px;margin:0 auto}
/* Cards */
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:12px;margin-bottom:20px}
.card{background:#1e293b;border-radius:10px;padding:16px;text-align:center}
.card .val{font-size:1.75rem;font-weight:700;color:#38bdf8}
.card .lbl{font-size:.75rem;color:#94a3b8;margin-top:4px}
.card .sub2{font-size:.68rem;color:#475569;margin-top:2px}
/* Charts */
.charts{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:20px}
@media(max-width:768px){.charts{grid-template-columns:1fr}}
.chart-box{background:#1e293b;border-radius:10px;padding:16px}
.chart-box h3{font-size:.8rem;color:#94a3b8;margin-bottom:10px}
/* Table */
table{width:100%;border-collapse:collapse;background:#1e293b;border-radius:10px;overflow:hidden;font-size:.82rem}
th{background:#0f172a;color:#64748b;font-size:.7rem;text-transform:uppercase;padding:10px 12px;text-align:left}
td{padding:8px 12px;border-bottom:1px solid #0f172a}
tr:last-child td{border-bottom:none}
tr:hover td{background:#1a2740}
/* Badges */
.badge{display:inline-block;padding:2px 8px;border-radius:999px;font-size:.68rem;font-weight:600}
.badge-green{background:#064e3b;color:#34d399}
.badge-blue{background:#1e3a5f;color:#60a5fa}
.badge-red{background:#3b1f1f;color:#fca5a5}
.badge-yellow{background:#3b2f00;color:#fbbf24}
.badge-gray{background:#1e293b;color:#64748b;border:1px solid #334155}
/* Miner Cards */
.miner-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:14px}
.miner-card{background:#1e293b;border-radius:10px;padding:16px;border:1px solid #334155;transition:border .2s}
.miner-card.enabled{border-color:#1d4ed8}
.miner-card .mc-header{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:10px}
.miner-card .mc-name{font-weight:600;color:#e2e8f0;font-size:.9rem}
.miner-card .mc-desc{font-size:.75rem;color:#64748b;margin-top:2px}
/* Toggle */
.toggle-wrap{display:flex;align-items:center;gap:8px;font-size:.75rem;color:#94a3b8}
.toggle{position:relative;display:inline-block;width:40px;height:22px}
.toggle input{opacity:0;width:0;height:0}
.slider{position:absolute;inset:0;background:#334155;border-radius:22px;cursor:pointer;transition:.3s}
.slider:before{content:"";position:absolute;height:16px;width:16px;left:3px;bottom:3px;background:#94a3b8;border-radius:50%;transition:.3s}
input:checked+.slider{background:#1d4ed8}
input:checked+.slider:before{transform:translateX(18px);background:#fff}
/* Key status */
.key-status{display:flex;align-items:center;gap:6px;margin-top:8px;font-size:.73rem}
.key-status .dot{width:6px;height:6px;border-radius:50%}
.dot-green{background:#22c55e}
.dot-red{background:#ef4444}
/* Key input row */
.key-row{display:flex;gap:6px;margin-top:8px}
.key-row input{flex:1;background:#0f172a;border:1px solid #334155;border-radius:6px;padding:6px 10px;color:#e2e8f0;font-size:.78rem;font-family:monospace}
.key-row input:focus{outline:none;border-color:#38bdf8}
/* Buttons */
.btn{padding:6px 14px;border-radius:6px;border:none;cursor:pointer;font-size:.78rem;font-weight:500;transition:all .2s}
.btn-primary{background:#1d4ed8;color:#fff}
.btn-primary:hover{background:#1e40af}
.btn-sm{padding:4px 10px;font-size:.72rem}
.btn-ghost{background:transparent;color:#64748b;border:1px solid #334155}
.btn-ghost:hover{background:#1e293b;color:#e2e8f0}
.btn-danger{background:#7f1d1d;color:#fca5a5}
.btn-danger:hover{background:#991b1b}
/* API Key Table */
.key-table-row{display:grid;grid-template-columns:1fr 2fr 1fr;gap:10px;padding:12px 0;border-bottom:1px solid #0f172a;align-items:center}
.key-table-row:last-child{border-bottom:none}
/* Mine Form */
.form-row{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:12px}
@media(max-width:640px){.form-row{grid-template-columns:1fr}}
.form-group label{display:block;font-size:.75rem;color:#94a3b8;margin-bottom:4px}
.form-group input[type=text],.form-group input[type=number],.form-group select{width:100%;background:#0f172a;border:1px solid #334155;border-radius:6px;padding:8px 12px;color:#e2e8f0;font-size:.85rem}
.form-group input:focus,.form-group select:focus{outline:none;border-color:#38bdf8}
.result-box{background:#020917;border:1px solid #1e3a5f;border-radius:8px;padding:16px;margin-top:16px;font-family:monospace;font-size:.78rem;color:#94a3b8;white-space:pre-wrap;max-height:300px;overflow-y:auto}
/* Timestamp */
.ts{font-size:.68rem;color:#475569;text-align:right;margin-bottom:8px}
/* Section title */
.sec{font-size:.9rem;color:#94a3b8;margin:16px 0 10px;font-weight:600;letter-spacing:.05em}
/* Toast */
#toast{position:fixed;bottom:20px;right:20px;background:#1e293b;border:1px solid #334155;border-radius:8px;padding:12px 18px;font-size:.82rem;transform:translateY(60px);opacity:0;transition:all .3s;z-index:999}
#toast.show{transform:translateY(0);opacity:1}
.tab-panel{display:none}.tab-panel.active{display:block}
/* Language switch */
.lang-sw{display:flex;gap:5px}
.lang-sw button{background:#0f172a;border:1px solid #334155;color:#64748b;border-radius:6px;padding:3px 10px;font-size:.72rem;cursor:pointer;transition:all .2s}
.lang-sw button.active{background:#1d4ed8;border-color:#1d4ed8;color:#fff}
.lang-sw button:hover:not(.active){background:#334155;color:#e2e8f0}
/* Demo banner */
.demo-banner{background:#1e1a0e;border-bottom:1px solid #713f12;padding:7px 24px;display:none;align-items:center;gap:10px;font-size:.78rem;color:#fbbf24}
.demo-banner.show{display:flex}
</style>
</head>
<body>
<div class="demo-banner" id="demo-banner">
  <span>&#128065;</span>
  <span id="demo-banner-text">Demo Mode — Read Only · Write operations are disabled</span>
  <a href="/logout" id="demo-exit-link" style="margin-left:auto;color:#fbbf24;opacity:.8;font-size:.72rem;text-decoration:underline">Exit Demo</a>
</div>
<div class="header">
  <h1><span id="hdr-title-text">⚡ Lead Mining Console</span> <span class="status-dot" id="htdot"></span></h1>
  <div style="display:flex;align-items:center;gap:12px">
    <div class="lang-sw">
      <button id="hdr-en" onclick="applyLang('en')">EN</button>
      <button id="hdr-zh" onclick="applyLang('zh')">Chinese</button>
    </div>
    <a href="/docs" target="_blank" id="hdr-apidocs" style="font-size:.78rem;color:#64748b;text-decoration:none">API Docs ↗</a>
    <a href="/logout" id="hdr-logout" style="font-size:.78rem;color:#94a3b8;text-decoration:none;background:#1e293b;border:1px solid #334155;padding:4px 12px;border-radius:6px;transition:all .2s" onmouseover="this.style.background='#334155'" onmouseout="this.style.background='#1e293b'">Log Out</a>
  </div>
</div>
<div class="tabs">
  <div class="tab active" id="tab0" onclick="switchTab(0)">&#128200; Monitor</div>
  <div class="tab" id="tab1" onclick="switchTab(1)">&#9881; Miners</div>
  <div class="tab" id="tab2" onclick="switchTab(2)">&#128273; API Keys</div>
  <div class="tab" id="tab3" onclick="switchTab(3)">&#128640; Mine Now</div>
</div>
<div class="content">
  <!-- TAB 0: Monitor Dashboard -->
  <div class="tab-panel active" id="tp0">
    <p class="ts" id="ts0">Loading...</p>
    <div class="grid" id="cards"></div>
    <div class="charts">
      <div class="chart-box"><h3 id="chart-funnel-title">&#128200; Mining Funnel</h3><canvas id="fc" height="200"></canvas></div>
      <div class="chart-box"><h3 id="chart-top-title">&#11088; Top Lead Scores</h3><canvas id="sc" height="200"></canvas></div>
    </div>
    <div class="sec" id="table-title">&#128229; Top 20 High-Intent Leads</div>
    <table>
      <thead><tr><th>#</th><th id="th-company">Company</th><th id="th-industry">Industry</th><th id="th-address">Address</th><th id="th-score">Score</th><th id="th-email">Email</th><th id="th-status">Status</th></tr></thead>
      <tbody id="ltb"></tbody>
    </table>
  </div>

  <!-- TAB 1: Miner Configuration -->
  <div class="tab-panel" id="tp1">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px">
      <span class="sec" id="miners-title" style="margin:0">Data Source Management</span>
      <button class="btn btn-ghost btn-sm" id="miners-refresh" onclick="loadSettings()">&#8635; Refresh</button>
    </div>
    <div class="miner-grid" id="miner-grid">Loading…</div>
  </div>

  <!-- TAB 2: API Keys -->
  <div class="tab-panel" id="tp2">
    <div style="margin-bottom:14px">
      <span class="sec" id="apikeys-title" style="margin:0">API Key Management</span>
      <p id="apikeys-sub" style="font-size:.75rem;color:#475569;margin-top:4px">Changes take effect immediately and persist to DB (restored after restart)</p>
    </div>
    <div id="key-list">Loading…</div>
  </div>

  <!-- TAB 3: Mine Now -->
  <div class="tab-panel" id="tp3">
    <div class="sec" id="mine-section-title">Trigger Mining Task</div>
    <div class="form-row">
      <div class="form-group"><label id="lbl-kw">Keyword (required)</label><input type="text" id="m-kw" placeholder="restaurant, IT services, retail…"></div>
      <div class="form-group"><label id="lbl-loc">Location</label><input type="text" id="m-loc" value="Philippines"></div>
    </div>
    <div class="form-row">
      <div class="form-group"><label id="lbl-limit">Max Count (1–500)</label><input type="number" id="m-limit" value="50" min="1" max="500"></div>
      <div class="form-group"><label id="lbl-src">Sources (blank=all)</label><input type="text" id="m-src" placeholder="serper,hunter…"></div>
    </div>
    <div style="display:flex;align-items:center;gap:16px;margin-bottom:14px">
      <label style="display:flex;align-items:center;gap:6px;font-size:.82rem;cursor:pointer;color:#94a3b8">
        <input type="checkbox" id="m-enrich" style="accent-color:#1d4ed8"> <span id="lbl-enrich">Gemini Enrich</span>
      </label>
      <label style="display:flex;align-items:center;gap:6px;font-size:.82rem;cursor:pointer;color:#94a3b8">
        <input type="number" id="m-score" value="0" min="0" max="100" style="width:56px;background:#0f172a;border:1px solid #334155;border-radius:4px;padding:3px 6px;color:#e2e8f0"> <span id="lbl-score">Min Score Filter</span>
      </label>
    </div>
    <button class="btn btn-primary" onclick="startMine()" id="mine-btn">&#128640; Start Mining</button>
    <div id="mine-result" class="result-box" style="display:none"></div>
  </div>
</div>
<div id="toast"></div>

<script>
/*__DEMO_INIT__*/
// ── i18n ──────────────────────────────────────────────────────────────────
const I18N={
en:{title:'\u26a1 Lead Mining Console',tab0:'\U0001F4C8 Monitor',tab1:'\u2699\ufe0f Miners',tab2:'\U0001F511 API Keys',tab3:'\U0001F680 Mine Now',logout:'Log Out',loading:'Loading\u2026',lastUpd:'Last updated: ',cardRaw:'Raw Leads',cardRawSub:'All Raw',cardEnr:'AI Enriched',cardEnrPre:'Rate ',cardAvg:'Avg Score',cardAvgSub:'/ 100',cardHigh:'High Intent 70+',cardHighSub:'Priority Outreach',cardEmails:'Emails Sent',cardEmail:'Has Email',cardEmailSub:'Direct Outreach',chartFunnel:'\U0001F4C8 Mining Funnel',chartTop:'\u2b50 Top Lead Scores',funnelLabels:['Raw','Enriched','70+','Outreached'],tableTitle:'\U0001F4E5 Top 20 High-Intent Leads',colCompany:'Company',colIndustry:'Industry',colAddress:'Address',colScore:'Score',colEmail2:'Email',colStatus:'Status',noData:'No enriched data yet',outreached:'Outreached',pending:'Pending',minersTitle:'Data Source Management',refreshBtn:'\u21bb Refresh',apiKeysTitle:'API Key Management',apiKeysSub:'Changes take effect immediately and persist to DB (restored after restart)',applyLink:'Apply \u2197',notConfigured:'Not configured',mineTitle:'Trigger Mining Task',kwLabel:'Keyword (required)',locLabel:'Location',limitLabel:'Max Count (1\u2013500)',srcLabel:'Sources (blank=all)',enrichLabel:'Gemini Enrich',scoreLabel:'Min Score Filter',mineBtn:'\U0001F680 Start Mining',mineBtnRunning:'Mining\u2026',mineWait:'\u23f3 Mining in progress, please wait\u2026',enterKw:'Please enter a keyword',enterVal:'Please enter a key value',enabled:'Enabled',disabled:'Disabled',noApiKey:'No API Key required',opFail:'Operation failed',saveFail:'Save failed',loadFail:'Load failed',configured:'\u2705 Configured',notConfiguredBadge:'\u274c Not Configured',saveBtn:'Save',demoBanner:'\U0001F441 Demo Mode \u2014 Read Only \u00b7 Write operations are disabled',demoExit:'Exit Demo'},
zh:{title:'\u26a1 Lead Mining Console',tab0:'\U0001F4C8 Monitor',tab1:'\u2699\ufe0f Miner Configuration',tab2:'\U0001F511 API Keys',tab3:'\U0001F680 Mine Now',logout:'Log Out',loading:'Loading\u2026',lastUpd:'Last updated: ',cardRaw:'Raw Leads',cardRawSub:'All Raw',cardEnr:'AI Enriched',cardEnrPre:'Rate ',cardAvg:'Avg Score',cardAvgSub:'Out of 100',cardHigh:'High Intent 70+',cardHighSub:'Priority Outreach',cardEmails:'Emails Sent',cardEmail:'Has Email',cardEmailSub:'Direct Outreach',chartFunnel:'\U0001F4C8 Mining Funnel',chartTop:'\u2b50 Top Lead Scores',funnelLabels:['Raw','Enriched','70+','Outreached'],tableTitle:'\U0001F4E5 Top 20 High-Intent Leads',colCompany:'Company',colIndustry:'Industry',colAddress:'Address',colScore:'Score',colEmail2:'Email',colStatus:'Status',noData:'No enriched data yet',outreached:'Outreached',pending:'Pending',minersTitle:'Data Source Management',refreshBtn:'\u21bb Refresh',apiKeysTitle:'API Key Management',apiKeysSub:'Changes take effect immediately and persist to DB (restored after restart)',applyLink:'Apply \u2197',notConfigured:'Not configured',mineTitle:'Trigger Mining Task',kwLabel:'Keyword (required)',locLabel:'Location',limitLabel:'Max Count (1\u2013500)',srcLabel:'Sources (blank=all)',enrichLabel:'Gemini Enrich',scoreLabel:'Min Score Filter',mineBtn:'\U0001F680 Start Mining',mineBtnRunning:'Mining\u2026',mineWait:'\u23f3 Mining in progress, please wait\u2026',enterKw:'Please enter a keyword',enterVal:'Please enter a key value',enabled:'Enabled',disabled:'Disabled',noApiKey:'No API Key required',opFail:'Operation failed',saveFail:'Save failed',loadFail:'Load failed',configured:'\u2705 Configured',notConfiguredBadge:'\u274c Not Configured',saveBtn:'Save',demoBanner:'\U0001F441 Demo Mode \u2014 Read Only \u00b7 Write operations are disabled',demoExit:'Exit Demo'}
};
let _lang=localStorage.getItem('lang')||'en';
function t(k){const d=I18N[_lang]||I18N.en;return d[k]!==undefined?d[k]:k;}

function applyLang(l){
  _lang=l;localStorage.setItem('lang',l);
  document.getElementById('html-root').lang=l==='zh'?'zh':'en';
  document.getElementById('hdr-title-text').textContent=t('title');
  document.getElementById('tab0').textContent=t('tab0');
  document.getElementById('tab1').textContent=t('tab1');
  document.getElementById('tab2').textContent=t('tab2');
  document.getElementById('tab3').textContent=t('tab3');
  document.getElementById('hdr-logout').textContent=t('logout');
  document.getElementById('hdr-en').classList.toggle('active',l==='en');
  document.getElementById('hdr-zh').classList.toggle('active',l==='zh');
  // Tab 0 labels
  document.getElementById('chart-funnel-title').textContent=t('chartFunnel');
  document.getElementById('chart-top-title').textContent=t('chartTop');
  document.getElementById('table-title').textContent=t('tableTitle');
  document.getElementById('th-company').textContent=t('colCompany');
  document.getElementById('th-industry').textContent=t('colIndustry');
  document.getElementById('th-address').textContent=t('colAddress');
  document.getElementById('th-score').textContent=t('colScore');
  document.getElementById('th-email').textContent=t('colEmail2');
  document.getElementById('th-status').textContent=t('colStatus');
  // Tab 1
  document.getElementById('miners-title').textContent=t('minersTitle');
  document.getElementById('miners-refresh').textContent=t('refreshBtn');
  // Tab 2
  document.getElementById('apikeys-title').textContent=t('apiKeysTitle');
  document.getElementById('apikeys-sub').textContent=t('apiKeysSub');
  // Tab 3
  document.getElementById('mine-section-title').textContent=t('mineTitle');
  document.getElementById('lbl-kw').textContent=t('kwLabel');
  document.getElementById('lbl-loc').textContent=t('locLabel');
  document.getElementById('lbl-limit').textContent=t('limitLabel');
  document.getElementById('lbl-src').textContent=t('srcLabel');
  document.getElementById('lbl-enrich').textContent=t('enrichLabel');
  document.getElementById('lbl-score').textContent=t('scoreLabel');
  document.getElementById('mine-btn').textContent=t('mineBtn');
  // Demo banner
  const banner=document.getElementById('demo-banner-text');
  if(banner)banner.textContent=t('demoBanner');
  const exitLink=document.getElementById('demo-exit-link');
  if(exitLink)exitLink.textContent=t('demoExit');
  // timestamp
  const ts0=document.getElementById('ts0');
  if(ts0&&ts0.textContent!==t('loading'))ts0.textContent=t('loading');
  // Re-render current active tab data
  const activeIdx=[...document.querySelectorAll('.tab')].findIndex(tab=>tab.classList.contains('active'));
  if(activeIdx===0)loadDashboard();
  else if(activeIdx===1)loadSettings();
  else if(activeIdx===2)loadKeys();
}

let _fc=null,_sc=null;
const tabs=document.querySelectorAll('.tab');
const panels=document.querySelectorAll('.tab-panel');

function switchTab(i){
  tabs.forEach((t,j)=>{t.classList.toggle('active',i===j)});
  panels.forEach((p,j)=>{p.classList.toggle('active',i===j)});
  if(i===0)loadDashboard();
  if(i===1)loadSettings();
  if(i===2)loadKeys();
}

function toast(msg,ok=true){
  const el=document.getElementById('toast');
  el.textContent=msg;
  el.style.borderColor=ok?'#1d4ed8':'#7f1d1d';
  el.classList.add('show');
  setTimeout(()=>el.classList.remove('show'),2800);
}

// ── TAB 0: Monitor Dashboard ────────────────────────────────────────────
async function loadDashboard(){
  try{
    const [stats,leads]=await Promise.all([
      fetch('/stats').then(r=>r.json()).catch(()=>({})),
      fetch('/leads?min_score=0&limit=20').then(r=>r.json()).catch(()=>({leads:[]}))
    ]);
    const s=stats.leads||{},o=stats.outreach||{};
    document.getElementById('ts0').textContent=t('lastUpd')+new Date().toLocaleTimeString(_lang==='zh'?'zh-CN':'en-US');
    document.getElementById('htdot').style.background='#22c55e';
    document.getElementById('cards').innerHTML=[
      {v:s.raw_total||0,l:t('cardRaw'),s2:t('cardRawSub')},
      {v:s.enriched_total||0,l:t('cardEnr'),s2:t('cardEnrPre')+(s.enrichment_rate||'0%')},
      {v:+(s.avg_score||0).toFixed(1),l:t('cardAvg'),s2:t('cardAvgSub')},
      {v:s.high_score_70||0,l:t('cardHigh'),s2:t('cardHighSub')},
      {v:o.emails_sent||0,l:t('cardEmails'),s2:''},
      {v:s.raw_with_email||0,l:t('cardEmail'),s2:t('cardEmailSub')},
    ].map(c=>`<div class="card"><div class="val">${c.v}</div><div class="lbl">${c.l}</div><div class="sub2">${c.s2}</div></div>`).join('');

    const fc=document.getElementById('fc');
    if(_fc)_fc.destroy();
    _fc=new Chart(fc,{type:'bar',data:{labels:t('funnelLabels'),
      datasets:[{data:[s.raw_total||0,s.enriched_total||0,s.high_score_70||0,o.emails_sent||0],
      backgroundColor:['#1d4ed8','#7c3aed','#059669','#d97706'],borderRadius:6,borderSkipped:false}]},
      options:{plugins:{legend:{display:false}},scales:{y:{grid:{color:'#1e293b'},ticks:{color:'#64748b'}},x:{ticks:{color:'#94a3b8'}}}}});

    const top=(leads.leads||[]).filter(l=>l.score>0).sort((a,b)=>b.score-a.score).slice(0,20);
    const sc=document.getElementById('sc');
    if(_sc)_sc.destroy();
    _sc=new Chart(sc,{type:'bar',data:{
      labels:top.map(l=>(l.business_name||'').substring(0,14)),
      datasets:[{data:top.map(l=>l.score),
      backgroundColor:top.map(l=>l.score>=70?'#059669':l.score>=50?'#1d4ed8':'#991b1b'),
      borderRadius:4,borderSkipped:false}]},
      options:{indexAxis:'y',plugins:{legend:{display:false}},
      scales:{x:{max:100,grid:{color:'#0f172a'},ticks:{color:'#64748b'}},y:{ticks:{color:'#94a3b8',font:{size:10}}}}}});

    const tb=document.getElementById('ltb');
    if(!top.length){tb.innerHTML=`<tr><td colspan=7 style="text-align:center;color:#475569;padding:24px">${t('noData')}</td></tr>`;return;}
    tb.innerHTML=top.map((l,i)=>{
      const cls=l.score>=70?'badge-green':l.score>=50?'badge-blue':'badge-red';
      return`<tr><td>${i+1}</td><td>${l.business_name||''}</td><td style="color:#64748b">${l.industry_category||l.industry_keyword||''}</td><td style="color:#475569;font-size:.75rem">${(l.address||'').substring(0,30)}</td><td><span class="badge ${cls}">${l.score}</span></td><td style="color:#64748b;font-size:.75rem">${l.email||'—'}</td><td><span class="badge ${l.outreached?'badge-green':'badge-blue'}">${l.outreached?t('outreached'):t('pending')}</span></td></tr>`;
    }).join('');
  }catch(e){console.error(e);document.getElementById('htdot').style.background='#ef4444';}
}

// ── TAB 1: Miner Configuration ─────────────────────────────────────────
async function loadSettings(){
  const res=await fetch('/admin/settings').then(r=>r.json()).catch(()=>null);
  if(!res){document.getElementById('miner-grid').innerHTML=`<p style="color:#fca5a5">${t('loadFail')}</p>`;return;}
  const order=['serper','hunter','google_cse','sec_ph','reddit','yellow_pages','philgeps','facebook','dti_bnrs'];
  const phaseColors={1:'badge-blue',2:'badge-yellow',3:'badge-gray'};
  document.getElementById('miner-grid').innerHTML=order.filter(n=>res.miners[n]).map(name=>{
    const m=res.miners[name];
    const d=m.display;
    const desc=(_lang==='zh'?(d.desc||d.desc_en):(d.desc_en||d.desc));
    const keyInfo=m.env_key?`<div class="key-status"><span class="dot ${m.has_key?'dot-green':'dot-red'}"></span><span>${m.env_key}: ${m.has_key?t('configured'):t('notConfiguredBadge')}</span></div>`:`<div class="key-status"><span class="dot dot-green"></span><span>${t('noApiKey')}</span></div>`;
    const keyInput=(!m.has_key&&m.env_key)?`<div class="key-row"><input type="password" id="ki-${name}" placeholder="${m.env_key}…"><button class="btn btn-primary btn-sm" onclick="saveKey('${m.env_key}','ki-${name}')">${t('saveBtn')}</button></div>`:'';
    return`<div class="miner-card ${m.enabled?'enabled':''}" id="mc-${name}">
      <div class="mc-header">
        <div>
          <div class="mc-name">${d.label}</div>
          <div class="mc-desc">${desc}</div>
        </div>
        <span class="badge ${phaseColors[d.phase]||'badge-gray'}">P${d.phase}</span>
      </div>
      ${keyInfo}${keyInput}
      <div style="margin-top:10px;display:flex;justify-content:space-between;align-items:center">
        <div class="toggle-wrap">
          <label class="toggle"><input type="checkbox" ${m.enabled?'checked':''} onchange="toggleMiner('${name}',this.checked)"><span class="slider"></span></label>
          <span id="mtxt-${name}">${m.enabled?t('enabled'):t('disabled')}</span>
        </div>
      </div>
    </div>`;
  }).join('');
  // Sync update for API Keys tab
  _lastSettings=res;
}
let _lastSettings=null;

async function toggleMiner(name,enabled){
  const r=await fetch('/admin/settings/miner-toggle',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name,enabled})}).then(r=>r.json()).catch(()=>null);
  if(r?.ok){
    document.getElementById(`mc-${name}`).classList.toggle('enabled',enabled);
    document.getElementById(`mtxt-${name}`).textContent=enabled?t('enabled'):t('disabled');
    toast(`${name} ${enabled?t('enabled')+' ✅':t('disabled')}`);
  }else{toast(t('opFail'),false);}
}

// ── TAB 2: API Keys ──────────────────────────────────────────────────────
async function loadKeys(){
  const res=_lastSettings||await fetch('/admin/settings').then(r=>r.json()).catch(()=>null);
  if(!res){document.getElementById('key-list').innerHTML=`<p style="color:#fca5a5">${t('loadFail')}</p>`;return;}
  document.getElementById('key-list').innerHTML=`
  <div style="background:#1e293b;border-radius:10px;padding:16px">
    ${Object.entries(res.api_keys).map(([k,info])=>`
    <div class="key-table-row">
      <div>
        <div style="font-weight:600;font-size:.85rem">${info.label}</div>
        <div style="font-size:.72rem;color:#475569;margin-top:2px">${_lang==='zh'?info.hint:(info.hint_en||info.hint)} · <a href="${info.url}" target="_blank" style="color:#38bdf8;text-decoration:none">${t('applyLink')}</a></div>
      </div>
      <div>
        <div style="font-family:monospace;font-size:.78rem;color:${info.has_key?'#34d399':'#f87171'};margin-bottom:4px">${info.has_key?info.masked:t('notConfigured')}</div>
        <div class="key-row" style="margin-top:0">
          <input type="password" id="kf-${k}" placeholder="${k}…" style="font-size:.78rem">
          <button class="btn btn-primary btn-sm" onclick="saveKey('${k}','kf-${k}')">${t('saveBtn')}</button>
        </div>
      </div>
      <div><span class="badge ${info.has_key?'badge-green':'badge-red'}">${info.has_key?t('configured'):t('notConfiguredBadge')}</span></div>
    </div>`).join('')}
  </div>`;
}

async function saveKey(envKey,inputId){
  const val=document.getElementById(inputId)?.value?.trim();
  if(!val){toast(t('enterVal'),false);return;}
  const r=await fetch('/admin/settings/apikey',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({key:envKey,value:val})}).then(r=>r.json()).catch(()=>null);
  if(r?.ok){
    toast(`${envKey} ${t('configured')} ✅`);
    document.getElementById(inputId).value='';
    _lastSettings=null;
    await loadSettings();
    await loadKeys();
  }else{toast(t('saveFail'),false);}
}

// ── TAB 3: Mine Now ─────────────────────────────────────────────────────
async function startMine(){
  const kw=document.getElementById('m-kw').value.trim();
  if(!kw){toast(t('enterKw'),false);return;}
  const loc=document.getElementById('m-loc').value.trim()||'Philippines';
  const limit=parseInt(document.getElementById('m-limit').value)||50;
  const srcRaw=document.getElementById('m-src').value.trim();
  const sources=srcRaw?srcRaw.split(',').map(s=>s.trim()).filter(Boolean):null;
  const enrich=document.getElementById('m-enrich').checked;
  const minScore=parseInt(document.getElementById('m-score').value)||0;
  const body={keyword:kw,location:loc,limit,enrich,min_score:minScore};
  if(sources)body.sources=sources;

  const btn=document.getElementById('mine-btn');
  btn.disabled=true;btn.textContent=t('mineBtnRunning');
  const box=document.getElementById('mine-result');
  box.style.display='block';box.textContent=t('mineWait');

  try{
    const t0=Date.now();
    const resp=await fetch('/mine',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    const data=await resp.json();
    const elapsed=((Date.now()-t0)/1000).toFixed(1);
    if(!resp.ok){box.textContent='❌ Error: '+JSON.stringify(data,null,2);return;}
    const summary=`✅ Mining complete! ${elapsed}s
─────────────────────────────────────
Task ID  : ${data.task_id}
Total    : ${data.total}
Deduped  : ${data.dedup_removed}
Net New  : ${data.total - data.dedup_removed}
Duration : ${data.duration_sec}s
─────────────────────────────────────
Sources:
${Object.entries(data.source_counts||{}).map(([k,v])=>`  ${k}: ${v}`).join('\\n')||'  (none)'}
${Object.keys(data.errors||{}).length?'\\nErrors:\\n'+Object.entries(data.errors||{}).map(([k,v])=>`  ${k}: ${v}`).join('\\n'):''}
─────────────────────────────────────
Top 5 Leads:
${(data.leads||[]).slice(0,5).map(l=>`  [${l.score||0}] ${l.business_name||''} | ${l.email||'no email'}`).join('\\n')||'(none)'}`;
    box.textContent=summary;
    toast(`Mining done: ${data.total} leads ✅`);
  }catch(e){box.textContent='❌ Error: '+e.message;toast(t('saveFail'),false);}
  finally{btn.disabled=false;btn.textContent=t('mineBtn');}
}

// Initialize
if(window.DEMO){
  document.getElementById('demo-banner').classList.add('show');
  // Disable all write operation buttons and inputs
  setTimeout(()=>{
    document.querySelectorAll('input[type=password],input[type=text],input[type=number]').forEach(el=>el.disabled=true);
    document.querySelectorAll('input[type=checkbox]').forEach(el=>el.disabled=true);
    document.querySelectorAll('#mine-btn,#miners-refresh').forEach(el=>el.disabled=true);
    document.querySelectorAll('.miner-card .toggle input').forEach(el=>el.disabled=true);
    // disable all save buttons
    document.querySelectorAll('.btn-primary').forEach(el=>{if(el.id!=='mine-btn')el.disabled=true;else el.disabled=true;});
  },500);
}
applyLang(_lang);
setInterval(loadDashboard,60000);
</script>
</body>
</html>"""