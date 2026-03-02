"""
FastAPI 服务层 — Lead Mining Engine 对外接口
提供 /mine, /health, /leads, /rag/query, /export 端点
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query, BackgroundTasks, Body
from fastapi.responses import StreamingResponse, HTMLResponse, RedirectResponse
from pydantic import BaseModel, Field
from app.middleware import RateLimitMiddleware

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

# ── 应用状态容器 ─────────────────────────────────────────────────────────────
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

# P2-6：速率限制中间件（令牌桶，防止单 IP 滥用）
app.add_middleware(RateLimitMiddleware, enabled=True)


# ── Request / Response 模型 ────────────────────────────────────────────────────
class MineRequest(BaseModel):
    keyword:  str    = Field(..., example="restaurant")
    location: str    = Field("Philippines", example="Manila, Philippines")
    limit:    int    = Field(100, ge=1, le=500)
    sources:  Optional[List[str]] = None
    enrich:   bool   = Field(False, description="是否立即触发 Gemini 富化")
    min_score: int   = Field(0, ge=0, le=100, description="富化后过滤分数")


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


# ── 路由 ──────────────────────────────────────────────────────────────────────
@app.post("/mine", response_model=MineResponse, tags=["Mining"])
async def mine(req: MineRequest, background_tasks: BackgroundTasks):
    """
    触发一次采集任务。
    - 并发调用所有已启用 Miner
    - 自动去重
    - 写入 PostgreSQL + ChromaDB
    - 可选立即富化（同步）
    """
    task = MiningTask(
        keyword=req.keyword,
        location=req.location,
        limit=req.limit,
        sources=req.sources,
    )
    result = await state.orchestrator.run_task(task)

    # 写入 PostgreSQL
    if result.leads:
        inserted, skipped = await state.pg.upsert_leads(result.leads)
        logger.info(f"/mine: pg upsert {inserted} new, {skipped} skipped")

    # 写入 ChromaDB（背景任务，非阻塞）
    background_tasks.add_task(state.chroma.upsert_leads, result.leads)

    # 可选富化
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
    lead_id:    Optional[int] = Query(None, description="按 ID 精确查询单条线索"),
    limit:      int           = Query(50, ge=1, le=200),
    offset:     int           = Query(0, ge=0),
):
    """查询已存储的线索（支持 id / keyword / source / min_score 过滤）"""
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
    limit:            int           = Field(50, ge=1, le=200, description="最多处理多少条未富化线索")
    min_score:        int           = Field(0, ge=0, le=100, description="富化后保留的最低评分")
    industry_keyword: Optional[str] = None


@app.post("/enrich", tags=["Enrichment"])
async def enrich_leads(req: EnrichRequest, background_tasks: BackgroundTasks):
    """
    对数据库中尚未 AI 富化的线索批量调用 Gemini 富化。
    - 不重新采集，不消耗 Serper/Apollo 配额
    - 供 n8n 工作流 02 定期调用
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
    更新 outreach_log 中某邮箱地址的回复状态。
    供 n8n 回复检测工作流（04）调用。
    """
    affected = await state.pg.update_outreach_status(
        email=req.email,
        status=req.status,
    )
    return {"updated": affected, "email": req.email, "status": req.status}


@app.post("/rag/query", tags=["RAG"])
async def rag_query(req: RagQueryRequest):
    """
    语义相似度查询（供 sales-outreach LangGraph 使用）。
    基于 ChromaDB 向量搜索，返回最相关的线索文档。
    ChromaDB HTTP Client 为同步 I/O，用 asyncio.to_thread 避免阻塞事件循环。
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
    """导出线索为 CSV 或 JSON（用于销售团队直接使用）"""
    from app.models.lead import EnrichedLead
    rows = await state.pg.query_leads(
        industry_keyword=keyword,
        min_score=min_score,
        limit=1000,
    )

    if export_format == "json":
        return rows

    # CSV 流式返回（UTF-8 BOM，兼容 Excel）
    leads = [EnrichedLead(**_row_to_enriched_kwargs(r)) for r in rows if r.get("enriched")]
    csv_content = CsvWriter.write_enriched(leads)

    return StreamingResponse(
        content=iter([csv_content.encode("utf-8-sig")]),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="leads.csv"'},
    )


@app.get("/health", tags=["System"])
async def health():
    """系统健康检查（含各 Miner 插件状态）"""
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
    """漏斗统计：raw → enriched → outreached"""
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
async def dashboard():
    """可视化监控 Dashboard — 漏斗数据 + Top Leads"""
    return """<!DOCTYPE html>
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
  <p class="sub">Philippines SME &#8226; 自动刷新每60秒</p>
  <p class="ts" id="ts">加载中…</p>
  <div class="grid" id="cards"></div>
  <div class="charts">
    <div class="chart-box"><h3>&#128200; 漏斗转化率</h3><canvas id="fc" height="200"></canvas></div>
    <div class="chart-box"><h3>&#11088; Top Leads 评分</h3><canvas id="sc" height="200"></canvas></div>
  </div>
  <p class="sec">&#128229; Top 15 Enriched Leads</p>
  <table><thead><tr><th>#</th><th>公司</th><th>行业</th><th>地址</th><th>评分</th><th>状态</th></tr></thead>
  <tbody id="tb"></tbody></table>
<script>
async function load(){
  const [stats,leads]=await Promise.all([
    fetch('/stats').then(r=>r.json()).catch(()=>({})),
    fetch('/leads?min_score=0&limit=15').then(r=>r.json()).catch(()=>({leads:[]}))
  ]);
  const s=stats.leads||{},o=stats.outreach||{};
  document.getElementById('ts').textContent='最后更新: '+new Date().toLocaleTimeString('zh-CN');
  document.getElementById('cards').innerHTML=[
    {v:s.raw_total||0,l:'采集线索',s2:'全部 Raw'},
    {v:s.enriched_total||0,l:'AI 富化',s2:'率 '+(s.enrichment_rate||'0%')},
    {v:+(s.avg_score||0).toFixed(1),l:'平均分数',s2:'满分 100'},
    {v:s.high_score_70||0,l:'高意向 70+',s2:'优先外展'},
    {v:o.emails_sent||0,l:'已发邮件',s2:'转化 '+(o.conversion_rate||'0%')},
    {v:s.raw_with_email||0,l:'含邮箱线索',s2:'可直接外展'},
  ].map(c=>`<div class="card"><div class="val">${c.v}</div><div class="lbl">${c.l}</div><div class="sub2">${c.s2}</div></div>`).join('');
  const fc=document.getElementById('fc');
  if(window._fc)window._fc.destroy();
  window._fc=new Chart(fc,{type:'bar',data:{labels:['Raw','富化','70+','已外展'],
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
  if(!top.length){tb.innerHTML='<tr><td colspan=6 style="text-align:center;color:#475569;padding:28px">暂无富化数据</td></tr>';return;}
  tb.innerHTML=top.map((l,i)=>{
    const cls=l.score>=70?'g':l.score>=50?'b':'r';
    return`<tr><td>${i+1}</td><td>${l.business_name||''}</td><td>${l.industry_category||l.industry_keyword||''}</td><td style="color:#64748b;font-size:.78rem">${(l.address||'').substring(0,28)}</td><td><span class="badge ${cls}">${l.score}</span></td><td><span class="badge ${l.outreached?'g':'b'}">${l.outreached?'已外展':'待外展'}</span></td></tr>`;
  }).join('');
}
load();setInterval(load,60000);
</script></body></html>"""


# ── P2-5：评分反馈与模型优化 ─────────────────────────────────────────────────

_VALID_OUTCOMES = frozenset({"replied", "converted", "bounced", "ignored", "unsubscribed"})


@app.post("/leads/{lead_id}/feedback", tags=["Scoring"])
async def submit_lead_feedback(
    lead_id: int,
    outcome: str = Body(..., embed=True,
                       description="replied | converted | bounced | ignored | unsubscribed"),
    notes: str   = Body("", embed=True),
):
    """
    P2-5：记录外展结果，用于评分模型优化。
    - replied / converted → 正向信号（该类线索评分权重应提升）
    - bounced / ignored   → 负向信号
    - unsubscribed        → 需从名单移除
    """
    if outcome not in _VALID_OUTCOMES:
        raise HTTPException(400, f"outcome 必须是 {sorted(_VALID_OUTCOMES)} 之一")
    try:
        await state.pg.insert_feedback(lead_id, outcome, notes)
    except Exception as exc:
        err_msg = str(exc)
        # FK 约束违反 → lead_id 不存在
        if "foreign key" in err_msg.lower() or "ForeignKeyViolationError" in type(exc).__name__:
            raise HTTPException(404, f"Lead {lead_id} 不存在")
        raise HTTPException(500, f"写入反馈失败: {exc}")
    return {"ok": True, "lead_id": lead_id, "outcome": outcome}


@app.get("/scoring/stats", tags=["Scoring"])
async def scoring_stats():
    """
    P2-5：按行业维度统计评分与转化率，用于优化最低外展分阈值。
    返回各行业的平均分、正向回复数、转化率，帮助识别高价值行业。
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
    P2-5：对评分偏低但外展正向的线索重新进行 Gemini 富化，修正评分偏差。
    触发条件：有 >= min_positives 条正向反馈，且对应线索当前评分 < 65。
    """
    if state.enricher is None:
        raise HTTPException(503, "GeminiEnricher 未初始化，请检查 GEMINI_API_KEY")

    # 找出有正向反馈但评分偏低的线索
    rows = await state.pg.fetch_low_score_positive_leads(min_positives=min_positives)
    if not rows:
        return {"ok": True, "recalibrated": 0, "message": "暂无需要校正的线索"}

    # 将这些行转成 LeadRaw 对象并重新富化
    leads_to_recalibrate = [
        lead for r in rows
        if (lead := _row_to_lead_raw(r)) is not None
        or logger.warning(f"/scoring/recalibrate: skip lead id={r.get('id')}: conversion failed")
    ]

    if not leads_to_recalibrate:
        return {"ok": True, "recalibrated": 0, "message": "线索数据转换失败"}

    enriched = await state.enricher.enrich_batch(leads_to_recalibrate, skip_below_score=0)
    if enriched:
        await state.pg.upsert_enriched(enriched)

    return {
        "ok":           True,
        "recalibrated": len(enriched),
        "total_found":  len(rows),
        "message":      f"已重新对 {len(enriched)} 条线索进行 Gemini 评分校正（原均分 < 65，有正向反馈 ≥ {min_positives}）",
    }


# ── 辅助函数 ──────────────────────────────────────────────────────────────────
def _lead_to_dict(lead) -> dict:
    try:
        return lead.model_dump(mode="json")
    except AttributeError:
        return lead.__dict__


def _row_to_enriched_kwargs(row: dict) -> dict:
    """把 PostgreSQL 行转为 EnrichedLead 构造参数"""
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
# 管理面板 API — /admin/*
# ══════════════════════════════════════════════════════════════════════════════

# 所有可配置 API Key 的描述信息
_API_KEY_DEFS = [
    {"key": "GEMINI_API_KEY",           "label": "Gemini API Key",          "hint": "AI 富化引擎，必填", "url": "https://aistudio.google.com/app/apikey"},
    {"key": "SERPER_API_KEY",           "label": "Serper API Key",          "hint": "Google 搜索代理 (Phase 1)", "url": "https://serper.dev"},
    {"key": "HUNTER_API_KEY",           "label": "Hunter.io API Key",       "hint": "邮箱发现 (Phase 1)", "url": "https://hunter.io"},
    {"key": "GOOGLE_API_KEY",           "label": "Google API Key",          "hint": "Google CSE 搜索 (Phase 2)", "url": "https://console.cloud.google.com"},
    {"key": "GOOGLE_CSE_CX",            "label": "Google CSE CX",           "hint": "Custom Search Engine ID", "url": "https://programmablesearchengine.google.com"},
    {"key": "REDDIT_CLIENT_ID",         "label": "Reddit Client ID",        "hint": "Reddit 采集 (Phase 2)", "url": "https://www.reddit.com/prefs/apps"},
    {"key": "REDDIT_CLIENT_SECRET",     "label": "Reddit Client Secret",    "hint": "Reddit 认证密钥", "url": "https://www.reddit.com/prefs/apps"},
    {"key": "FACEBOOK_ACCESS_TOKEN",    "label": "Facebook Access Token",   "hint": "Facebook 商家搜索 (Phase 3)", "url": "https://developers.facebook.com"},
]

# Miner 展示名和相关说明
_MINER_DISPLAY = {
    "serper":       {"label": "Serper",       "desc": "Google 搜索代理，菲律宾 SME 首选",  "phase": 1},
    "hunter":       {"label": "Hunter.io",    "desc": "邮箱验证与发现",                   "phase": 1},
    "google_cse":   {"label": "Google CSE",   "desc": "Google 自定义搜索引擎",             "phase": 2},
    "sec_ph":       {"label": "SEC PH",       "desc": "菲律宾企业注册查询（无需 Key）",    "phase": 2},
    "reddit":       {"label": "Reddit",       "desc": "Reddit 社区线索挖掘",               "phase": 2},
    "yellow_pages": {"label": "Yellow Pages", "desc": "黄页商家数据（无需 Key）",          "phase": 2},
    "philgeps":     {"label": "PhilGEPS",     "desc": "政府采购公告（无需 Key）",          "phase": 3},
    "facebook":     {"label": "Facebook",     "desc": "Facebook 商家页面搜索",             "phase": 3},
    "dti_bnrs":     {"label": "DTI BNRS",     "desc": "菲律宾贸工部企业注册（无需 Key）",  "phase": 3},
}


class MinerToggleRequest(BaseModel):
    name:    str
    enabled: bool


class ApiKeyRequest(BaseModel):
    key:   str
    value: str


@app.get("/admin/settings", tags=["Admin"])
async def admin_get_settings():
    """获取所有 Miner 状态和 API Key 存在性（不返回明文 Key）"""
    db_settings = await state.pg.get_all_settings()

    miners_out = {}
    for name, meta in FACTORY.items():
        miner = state.orchestrator._miners.get(name)
        if miner is None:
            continue
        # 运行时状态 or DB 覆盖
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
            "label":   d["label"],
            "hint":    d["hint"],
            "url":     d["url"],
            "masked":  masked,
            "has_key": bool(raw),
        }

    return {"miners": miners_out, "api_keys": api_keys_out}


@app.post("/admin/settings/miner-toggle", tags=["Admin"])
async def admin_miner_toggle(req: MinerToggleRequest):
    """运行时切换 Miner 启用状态（持久化到 DB）"""
    miner = state.orchestrator._miners.get(req.name)
    if miner is None:
        raise HTTPException(404, f"Miner '{req.name}' 不存在")
    miner.config.enabled = req.enabled
    await state.pg.set_setting(f"miner:{req.name}:enabled", str(req.enabled).lower())
    return {"ok": True, "name": req.name, "enabled": req.enabled}


@app.post("/admin/settings/apikey", tags=["Admin"])
async def admin_set_apikey(req: ApiKeyRequest):
    """更新 API Key（写入 DB + 注入当前进程 env，重启后从 DB 恢复）"""
    allowed = {d["key"] for d in _API_KEY_DEFS}
    if req.key not in allowed:
        raise HTTPException(400, f"不允许更新的 Key: {req.key}")
    if not req.value.strip():
        raise HTTPException(400, "value 不能为空")

    # 注入运行时 env
    os.environ[req.key] = req.value.strip()
    # 持久化到 DB
    await state.pg.set_setting(f"apikey:{req.key}", req.value.strip())

    # 如果是 Miner 的必要 Key，自动重新启用对应 miner
    for name, meta in FACTORY.items():
        if meta.get("env_key") == req.key:
            miner = state.orchestrator._miners.get(name)
            if miner and not miner.config.enabled:
                miner.config.api_key = req.value.strip()
                miner.config.enabled = True
                await state.pg.set_setting(f"miner:{name}:enabled", "true")

    # 如果是 Gemini Key，重新初始化 enricher
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
    """启动时从 DB 恢复持久化的 Key 和 Miner 状态（需在 lifespan 完成后执行）"""
    # 延迟执行，等待 lifespan 中的 state 初始化完成
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


@app.get("/dashboard", response_class=HTMLResponse, tags=["System"])
async def dashboard_redirect():
    """旧版 dashboard，已升级为 /admin"""
    return RedirectResponse(url="/admin", status_code=301)


@app.get("/admin", response_class=HTMLResponse, tags=["Admin"])
async def admin_panel():
    """⚡ Lead Mining 综合管理控制台"""
    return """<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Lead Mining 控制台</title>
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
</style>
</head>
<body>
<div class="header">
  <h1>&#9889; Lead Mining 控制台 <span class="status-dot" id="htdot"></span></h1>
  <a href="/docs" target="_blank" style="font-size:.78rem;color:#64748b;text-decoration:none">API Docs ↗</a>
</div>
<div class="tabs">
  <div class="tab active" onclick="switchTab(0)">&#128200; 监控面板</div>
  <div class="tab" onclick="switchTab(1)">&#9881; Miner 配置</div>
  <div class="tab" onclick="switchTab(2)">&#128273; API Keys</div>
  <div class="tab" onclick="switchTab(3)">&#128640; 立即采集</div>
</div>
<div class="content">
  <!-- TAB 0: 监控面板 -->
  <div class="tab-panel active" id="tp0">
    <p class="ts" id="ts0">加载中…</p>
    <div class="grid" id="cards"></div>
    <div class="charts">
      <div class="chart-box"><h3>&#128200; 采集漏斗</h3><canvas id="fc" height="200"></canvas></div>
      <div class="chart-box"><h3>&#11088; Top Leads 评分</h3><canvas id="sc" height="200"></canvas></div>
    </div>
    <div class="sec">&#128229; Top 20 高意向线索</div>
    <table>
      <thead><tr><th>#</th><th>公司</th><th>行业</th><th>地址</th><th>评分</th><th>邮箱</th><th>状态</th></tr></thead>
      <tbody id="ltb"></tbody>
    </table>
  </div>

  <!-- TAB 1: Miner 配置 -->
  <div class="tab-panel" id="tp1">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px">
      <span class="sec" style="margin:0">数据源管理</span>
      <button class="btn btn-ghost btn-sm" onclick="loadSettings()">&#8635; 刷新</button>
    </div>
    <div class="miner-grid" id="miner-grid">加载中…</div>
  </div>

  <!-- TAB 2: API Keys -->
  <div class="tab-panel" id="tp2">
    <div style="margin-bottom:14px">
      <span class="sec" style="margin:0">API Key 管理</span>
      <p style="font-size:.75rem;color:#475569;margin-top:4px">保存后立即生效，并持久化到数据库（重启后自动恢复）</p>
    </div>
    <div id="key-list">加载中…</div>
  </div>

  <!-- TAB 3: 立即采集 -->
  <div class="tab-panel" id="tp3">
    <div class="sec">手动触发采集任务</div>
    <div class="form-row">
      <div class="form-group"><label>关键词 (必填)</label><input type="text" id="m-kw" placeholder="restaurant, IT services, retail…"></div>
      <div class="form-group"><label>地区</label><input type="text" id="m-loc" value="Philippines"></div>
    </div>
    <div class="form-row">
      <div class="form-group"><label>最多条数 (1–500)</label><input type="number" id="m-limit" value="50" min="1" max="500"></div>
      <div class="form-group"><label>数据源（留空=全部启用）</label><input type="text" id="m-src" placeholder="serper,hunter （逗号分隔）"></div>
    </div>
    <div style="display:flex;align-items:center;gap:16px;margin-bottom:14px">
      <label style="display:flex;align-items:center;gap:6px;font-size:.82rem;cursor:pointer;color:#94a3b8">
        <input type="checkbox" id="m-enrich" style="accent-color:#1d4ed8"> 立即 Gemini 富化
      </label>
      <label style="display:flex;align-items:center;gap:6px;font-size:.82rem;cursor:pointer;color:#94a3b8">
        <input type="number" id="m-score" value="0" min="0" max="100" style="width:56px;background:#0f172a;border:1px solid #334155;border-radius:4px;padding:3px 6px;color:#e2e8f0"> 最低评分过滤
      </label>
    </div>
    <button class="btn btn-primary" onclick="startMine()" id="mine-btn">&#128640; 开始采集</button>
    <div id="mine-result" class="result-box" style="display:none"></div>
  </div>
</div>
<div id="toast"></div>

<script>
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

// ── TAB 0: 监控面板 ───────────────────────────────────────────────────────
async function loadDashboard(){
  try{
    const [stats,leads]=await Promise.all([
      fetch('/stats').then(r=>r.json()).catch(()=>({})),
      fetch('/leads?min_score=0&limit=20').then(r=>r.json()).catch(()=>({leads:[]}))
    ]);
    const s=stats.leads||{},o=stats.outreach||{};
    document.getElementById('ts0').textContent='最后更新: '+new Date().toLocaleTimeString('zh-CN');
    document.getElementById('htdot').style.background='#22c55e';
    document.getElementById('cards').innerHTML=[
      {v:s.raw_total||0,l:'采集线索',s2:'全部 Raw'},
      {v:s.enriched_total||0,l:'AI 富化',s2:'率 '+(s.enrichment_rate||'0%')},
      {v:+(s.avg_score||0).toFixed(1),l:'平均评分',s2:'满分 100'},
      {v:s.high_score_70||0,l:'高意向 70+',s2:'优先外展'},
      {v:o.emails_sent||0,l:'已发邮件',s2:'转化 '+(o.conversion_rate||'0%')},
      {v:s.raw_with_email||0,l:'含邮箱',s2:'可直接外展'},
    ].map(c=>`<div class="card"><div class="val">${c.v}</div><div class="lbl">${c.l}</div><div class="sub2">${c.s2}</div></div>`).join('');

    const fc=document.getElementById('fc');
    if(_fc)_fc.destroy();
    _fc=new Chart(fc,{type:'bar',data:{labels:['Raw','富化','70+','已外展'],
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
    if(!top.length){tb.innerHTML='<tr><td colspan=7 style="text-align:center;color:#475569;padding:24px">暂无富化数据</td></tr>';return;}
    tb.innerHTML=top.map((l,i)=>{
      const cls=l.score>=70?'badge-green':l.score>=50?'badge-blue':'badge-red';
      return`<tr><td>${i+1}</td><td>${l.business_name||''}</td><td style="color:#64748b">${l.industry_category||l.industry_keyword||''}</td><td style="color:#475569;font-size:.75rem">${(l.address||'').substring(0,30)}</td><td><span class="badge ${cls}">${l.score}</span></td><td style="color:#64748b;font-size:.75rem">${l.email||'—'}</td><td><span class="badge ${l.outreached?'badge-green':'badge-blue'}">${l.outreached?'已外展':'待外展'}</span></td></tr>`;
    }).join('');
  }catch(e){console.error(e);document.getElementById('htdot').style.background='#ef4444';}
}

// ── TAB 1: Miner 配置 ────────────────────────────────────────────────────
async function loadSettings(){
  const res=await fetch('/admin/settings').then(r=>r.json()).catch(()=>null);
  if(!res){document.getElementById('miner-grid').innerHTML='<p style="color:#fca5a5">加载失败</p>';return;}
  const order=['serper','hunter','google_cse','sec_ph','reddit','yellow_pages','philgeps','facebook','dti_bnrs'];
  const phaseColors={1:'badge-blue',2:'badge-yellow',3:'badge-gray'};
  document.getElementById('miner-grid').innerHTML=order.filter(n=>res.miners[n]).map(name=>{
    const m=res.miners[name];
    const d=m.display;
    const keyInfo=m.env_key?`<div class="key-status"><span class="dot ${m.has_key?'dot-green':'dot-red'}"></span><span>${m.env_key}: ${m.has_key?'✅ 已配置':'❌ 未配置'}</span></div>`:'<div class="key-status"><span class="dot dot-green"></span><span>无需 API Key</span></div>';
    const keyInput=(!m.has_key&&m.env_key)?`<div class="key-row"><input type="password" id="ki-${name}" placeholder="${m.env_key}…"><button class="btn btn-primary btn-sm" onclick="saveKey('${m.env_key}','ki-${name}')">保存</button></div>`:'';
    return`<div class="miner-card ${m.enabled?'enabled':''}" id="mc-${name}">
      <div class="mc-header">
        <div>
          <div class="mc-name">${d.label}</div>
          <div class="mc-desc">${d.desc}</div>
        </div>
        <span class="badge ${phaseColors[d.phase]||'badge-gray'}">P${d.phase}</span>
      </div>
      ${keyInfo}${keyInput}
      <div style="margin-top:10px;display:flex;justify-content:space-between;align-items:center">
        <div class="toggle-wrap">
          <label class="toggle"><input type="checkbox" ${m.enabled?'checked':''} onchange="toggleMiner('${name}',this.checked)"><span class="slider"></span></label>
          <span id="mtxt-${name}">${m.enabled?'已启用':'已禁用'}</span>
        </div>
      </div>
    </div>`;
  }).join('');
  // 同步更新 API Keys tab
  _lastSettings=res;
}
let _lastSettings=null;

async function toggleMiner(name,enabled){
  const r=await fetch('/admin/settings/miner-toggle',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name,enabled})}).then(r=>r.json()).catch(()=>null);
  if(r?.ok){
    document.getElementById(`mc-${name}`).classList.toggle('enabled',enabled);
    document.getElementById(`mtxt-${name}`).textContent=enabled?'已启用':'已禁用';
    toast(`${name} ${enabled?'已启用 ✅':'已禁用'}`);
  }else{toast('操作失败',false);}
}

// ── TAB 2: API Keys ──────────────────────────────────────────────────────
async function loadKeys(){
  const res=_lastSettings||await fetch('/admin/settings').then(r=>r.json()).catch(()=>null);
  if(!res){document.getElementById('key-list').innerHTML='<p style="color:#fca5a5">加载失败</p>';return;}
  document.getElementById('key-list').innerHTML=`
  <div style="background:#1e293b;border-radius:10px;padding:16px">
    ${Object.entries(res.api_keys).map(([k,info])=>`
    <div class="key-table-row">
      <div>
        <div style="font-weight:600;font-size:.85rem">${info.label}</div>
        <div style="font-size:.72rem;color:#475569;margin-top:2px">${info.hint} · <a href="${info.url}" target="_blank" style="color:#38bdf8;text-decoration:none">申请 ↗</a></div>
      </div>
      <div>
        <div style="font-family:monospace;font-size:.78rem;color:${info.has_key?'#34d399':'#f87171'};margin-bottom:4px">${info.has_key?info.masked:'未配置'}</div>
        <div class="key-row" style="margin-top:0">
          <input type="password" id="kf-${k}" placeholder="粘贴新的 ${k}…" style="font-size:.78rem">
          <button class="btn btn-primary btn-sm" onclick="saveKey('${k}','kf-${k}')">保存</button>
        </div>
      </div>
      <div><span class="badge ${info.has_key?'badge-green':'badge-red'}">${info.has_key?'✅ 已配置':'❌ 未配置'}</span></div>
    </div>`).join('')}
  </div>`;
}

async function saveKey(envKey,inputId){
  const val=document.getElementById(inputId)?.value?.trim();
  if(!val){toast('请输入 Key 值',false);return;}
  const r=await fetch('/admin/settings/apikey',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({key:envKey,value:val})}).then(r=>r.json()).catch(()=>null);
  if(r?.ok){
    toast(`${envKey} 保存成功 ✅`);
    document.getElementById(inputId).value='';
    _lastSettings=null;
    await loadSettings();
    await loadKeys();
  }else{toast('保存失败',false);}
}

// ── TAB 3: 立即采集 ──────────────────────────────────────────────────────
async function startMine(){
  const kw=document.getElementById('m-kw').value.trim();
  if(!kw){toast('请输入关键词',false);return;}
  const loc=document.getElementById('m-loc').value.trim()||'Philippines';
  const limit=parseInt(document.getElementById('m-limit').value)||50;
  const srcRaw=document.getElementById('m-src').value.trim();
  const sources=srcRaw?srcRaw.split(',').map(s=>s.trim()).filter(Boolean):null;
  const enrich=document.getElementById('m-enrich').checked;
  const minScore=parseInt(document.getElementById('m-score').value)||0;
  const body={keyword:kw,location:loc,limit,enrich,min_score:minScore};
  if(sources)body.sources=sources;

  const btn=document.getElementById('mine-btn');
  btn.disabled=true;btn.textContent='采集中…';
  const box=document.getElementById('mine-result');
  box.style.display='block';box.textContent='⏳ 正在采集，请稍候…';

  try{
    const t0=Date.now();
    const resp=await fetch('/mine',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    const data=await resp.json();
    const elapsed=((Date.now()-t0)/1000).toFixed(1);
    if(!resp.ok){box.textContent='❌ 错误: '+JSON.stringify(data,null,2);return;}
    const summary=`✅ 采集完成！耗时 ${elapsed}s
─────────────────────────────────────
任务 ID  : ${data.task_id}
采集总数 : ${data.total} 条
去重删除 : ${data.dedup_removed} 条
净新增   : ${data.total - data.dedup_removed} 条
耗时     : ${data.duration_sec}s
─────────────────────────────────────
各数据源：
${Object.entries(data.source_counts||{}).map(([k,v])=>`  ${k}: ${v}`).join('\\n')||'  （无）'}
${Object.keys(data.errors||{}).length?'\\n错误:\\n'+Object.entries(data.errors||{}).map(([k,v])=>`  ${k}: ${v}`).join('\\n'):''}
─────────────────────────────────────
前5条线索预览：
${(data.leads||[]).slice(0,5).map(l=>`  [${l.score||0}] ${l.business_name||''} | ${l.email||'无邮箱'}`).join('\\n')||'（无）'}`;
    box.textContent=summary;
    toast(`采集完成：${data.total} 条 ✅`);
  }catch(e){box.textContent='❌ 请求失败: '+e.message;toast('采集失败',false);}
  finally{btn.disabled=false;btn.textContent='🚀 开始采集';}
}

// 初始化
loadDashboard();
setInterval(loadDashboard,60000);
</script>
</body>
</html>"""