"""
FastAPI 服务层 — Lead Mining Engine 对外接口
提供 /mine, /health, /leads, /rag/query, /export 端点
"""
from __future__ import annotations

import asyncio
import json
import logging
import uuid
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query, BackgroundTasks, Body
from fastapi.responses import StreamingResponse, HTMLResponse
from pydantic import BaseModel, Field
from app.middleware import RateLimitMiddleware

from app.config import (
    build_orchestrator,
    build_postgres_writer,
    build_chroma_writer,
    build_gemini_enricher,
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
