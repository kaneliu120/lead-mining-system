"""
LangGraph Nodes - 外展流程各处理节点
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Dict, List, Optional

import asyncpg
import httpx
from urllib.parse import urlparse

from src.state import EmailDraft, OutreachState

logger = logging.getLogger(__name__)

# ── 环境变量 ──────────────────────────────────────────────────────────────────
LEAD_MINER_URL  = os.environ.get("LEAD_MINER_URL",  "http://lead-miner:8000")
GEMINI_API_KEY  = os.environ.get("GEMINI_API_KEY",  "")
GEMINI_MODEL    = os.environ.get("GEMINI_MODEL",     "gemini-2.5-flash")
SMTP_HOST       = os.environ.get("SMTP_HOST",        "smtp.gmail.com")
SMTP_PORT       = int(os.environ.get("SMTP_PORT",   "587"))
SMTP_USER       = os.environ.get("SMTP_USER",        "")
SMTP_PASSWORD   = os.environ.get("SMTP_PASSWORD",    "")
SENDER_NAME     = os.environ.get("SENDER_NAME",      "Sales Team")
HUNTER_API_KEY  = os.environ.get("HUNTER_API_KEY",   "")


# ── Gemini 模型懒加载单例（避免每次调用重新初始化 + 全局竞态）─────────────────
_gemini_model = None


def _get_gemini_model():
    """返回全局 Gemini 模型单例，首次调用时初始化。"""
    global _gemini_model
    if _gemini_model is None:
        if not GEMINI_API_KEY:
            return None
        import google.generativeai as genai
        genai.configure(api_key=GEMINI_API_KEY)
        _gemini_model = genai.GenerativeModel(
            model_name=GEMINI_MODEL,
            generation_config=genai.types.GenerationConfig(
                temperature=0.7,
                max_output_tokens=8192,   # gemini-2.5-flash 思考模型需要较大空间
                response_mime_type="application/json",
            ),
        )
        logger.info(f"Gemini model (outreach) initialized: {GEMINI_MODEL}")
    return _gemini_model


# ── asyncpg 连接池懒加载单例（避免 log_outreach 每次创建新连接）──────────────
_db_pool = None


async def _get_db_pool():
    """返回全局 asyncpg 连接池单例，首次调用时初始化。"""
    global _db_pool
    if _db_pool is None:
        import asyncpg
        db_url = os.environ.get(
            "DATABASE_URL",
            "postgresql://postgres:postgres@postgres:5432/leads",
        )
        _db_pool = await asyncpg.create_pool(db_url, min_size=1, max_size=5)
        logger.info("asyncpg pool (outreach) initialized")
    return _db_pool


async def close_db_pool() -> None:
    """关闭全局 asyncpg 连接池（在应用 lifespan shutdown 阶段调用）。"""
    global _db_pool
    if _db_pool is not None:
        await _db_pool.close()
        _db_pool = None
        logger.info("asyncpg pool (outreach) closed")


# ─────────────────────────────────────────────────────────────────────────────
# Node 1: RAG 检索节点
# 从 Lead Mining Engine 的 /rag/query 接口查询相似案例
# ─────────────────────────────────────────────────────────────────────────────
async def retrieve_rag_context(state: OutreachState) -> OutreachState:
    lead = state["lead"]
    query = (
        f"{lead.get('industry_category', '')} "
        f"{lead.get('pain_points', [''])[0] if lead.get('pain_points') else ''} "
        f"{lead.get('recommended_product', '')} Philippines SME"
    ).strip()

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                f"{LEAD_MINER_URL}/rag/query",
                json={"query": query, "n": 3},
            )
            resp.raise_for_status()
            data = resp.json()
            results = data.get("results", [])
    except Exception as exc:
        logger.warning(f"RAG query failed for '{lead['business_name']}': {exc}")
        results = []

    # 将相似案例构建为 context 字符串
    context_parts = []
    for r in results:
        meta = r.get("metadata", {})
        context_parts.append(
            f"- {meta.get('business_name', 'Business')}: {r.get('document', '')[:200]}"
        )
    rag_context = "\n".join(context_parts) if context_parts else ""

    return {
        **state,
        "rag_context": rag_context,
        "messages": [f"RAG: retrieved {len(results)} similar leads"],
    }


# ─────────────────────────────────────────────────────────────────────────────
# Node 2: 联系人查询节点
# 从 Hunter.io API 根据域名查询决策者联系方式（替代 Apollo.io）
# ─────────────────────────────────────────────────────────────────────────────
async def find_contacts(state: OutreachState) -> OutreachState:
    lead    = state["lead"]
    website = lead.get("website", "")

    if not website or not HUNTER_API_KEY:
        return {
            **state,
            "contacts": [],
            "messages": ["Contacts: skipped (no website or Hunter.io key)"],
        }

    # 从 URL 中安全提取域名（urlparse 正确处理协议前缀，避免 lstrip/replace 的字符误删问题）
    parsed = urlparse(website if website.startswith("http") else f"https://{website}")
    domain = parsed.netloc or parsed.path.split("/")[0]

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                "https://api.hunter.io/v2/domain-search",
                params={
                    "domain":     domain,
                    "api_key":    HUNTER_API_KEY,
                    "limit":      3,
                    "seniority":  "senior,executive,director",
                    "department": "executive,management",
                },
            )
            resp.raise_for_status()
            data   = resp.json().get("data", {})
            emails = data.get("emails", [])
    except Exception as exc:
        logger.warning(f"Hunter contacts lookup failed for {domain}: {exc}")
        emails = []

    contacts = [
        {
            "name":         f"{e.get('first_name', '')} {e.get('last_name', '')}".strip(),
            "title":        e.get("position", ""),
            "email":        e.get("value", ""),
            "email_status": "verified" if e.get("verification", {}).get("status") == "valid" else "unverified",
            "linkedin":     e.get("linkedin", ""),
        }
        for e in emails
        if e.get("value")  # 只保留有邮箱的联系人
    ]

    return {
        **state,
        "contacts": contacts,
        "messages": [f"Contacts: found {len(contacts)} contacts for {domain} via Hunter.io"],
    }


# ─────────────────────────────────────────────────────────────────────────────
# Node 3: 邮件生成节点（Gemini 2.5 Flash）— 支持多语言 (P3-4)
# 语言选项: 'en'（纯英语）| 'fil'（菲律宾语为主）| 'mixed'（英菲混合 默认）
# ─────────────────────────────────────────────────────────────────────────────

# ── 英语版 Prompt ────────────────────────────────────────────────────────────
_EMAIL_PROMPT_EN = """\
You are a professional B2B sales email writer specializing in Philippine SME market.
Write a SHORT, personalized cold email entirely in English.

Business information:
- Name: {business_name}
- Industry: {industry_category}
- Location: {address}
- Size: {business_size}
- Pain Points: {pain_points}
- Value Proposition: {value_proposition}
- Recommended Product: {recommended_product}
- Outreach Angle: {outreach_angle}
- Score: {score}/100

Contact Person: {contact_name} ({contact_title})

Similar businesses we've helped (context):
{rag_context}

Requirements:
1. Subject line: specific, no spam words, mention their {industry_category} business
2. Opening: reference something specific about their business (location, industry, rating)
3. Body: 2-3 short paragraphs, focus on ONE specific problem and solution
4. CTA: one clear call-to-action (book a 15-min call or reply)
5. Length: max 150 words for body
6. Tone: friendly, professional, NOT pushy

Return ONLY valid JSON:
{{
  "subject": "...",
  "body": "..."
}}
"""

# ── 菲律宾语为主版 Prompt ─────────────────────────────────────────────────────
_EMAIL_PROMPT_FIL = """\
Ikaw ay isang propesyonal na manunulat ng B2B sales email para sa Philippine SME market.
Sumulat ng MAIKLING, personalized na cold email na pangunahing nakasulat sa Filipino/Tagalog.
Pwede kang gumamit ng ilang English na termino para sa mga teknikal na salita.

Impormasyon ng negosyo:
- Pangalan: {business_name}
- Industriya: {industry_category}
- Lokasyon: {address}
- Sukat ng Negosyo: {business_size}
- Mga Hamon (Pain Points): {pain_points}
- Value Proposition: {value_proposition}
- Inirerekomendang Produkto: {recommended_product}
- Diskarte sa Outreach: {outreach_angle}
- Score: {score}/100

Contact Person: {contact_name} ({contact_title})

Mga katulad na negosyo na aming natulungan (konteksto):
{rag_context}

Mga Kinakailangan:
1. Subject line: tiyak, walang spam na salita, banggitin ang kanilang {industry_category} negosyo
2. Pagbubukas: banggitin ang isang tiyak na detalye tungkol sa kanilang negosyo
3. Katawan: 2-3 maikling talata, fokus sa ISANG tiyak na problema at solusyon
4. CTA: isang malinaw na call-to-action (mag-book ng 15-min na tawag o sumagot)
5. Haba: max 120 salita para sa katawan
6. Tono: magiliw, propesyonal, HINDI mapanghikayat nang labis

Ibalik lamang ang valid na JSON:
{{
  "subject": "...",
  "body": "..."
}}
"""

# ── 英菲混合版 Prompt（默认，最自然） ─────────────────────────────────────────
_EMAIL_PROMPT_MIXED = """\
You are a professional B2B sales email writer specializing in Philippine SME market.
Write a SHORT, personalized cold email mixing natural English and Filipino/Tagalog
in the way Filipino business professionals actually communicate.
Use English for technical/business terms, Filipino for warmth and personal connection.

Business information:
- Name: {business_name}
- Industry: {industry_category}
- Location: {address}
- Size: {business_size}
- Pain Points: {pain_points}
- Value Proposition: {value_proposition}
- Recommended Product: {recommended_product}
- Outreach Angle: {outreach_angle}
- Score: {score}/100

Contact Person: {contact_name} ({contact_title})

Similar businesses we've helped (context):
{rag_context}

Requirements:
1. Subject line: in English (more professional for email)
2. Opening: use a Filipino greeting (e.g. "Magandang araw po") then switch naturally
3. Body: 2-3 short paragraphs, mix English and Filipino naturally (code-switching)
4. CTA: "Puwede po ba tayong mag-usap ng 15 minuto?" or similar warm Filipino phrasing
5. Length: max 150 words for body
6. Tone: "Malapit na kaibigan" — warm, respectful (with "po/opo"), NOT pushy

Language mixing example:
  "Hi [Name], I noticed na ang [business] ninyo sa [location] ay..."
  "Nakita namin na maraming [industry] businesses ang nag-struggle with [pain point]..."

Return ONLY valid JSON:
{{
  "subject": "...",
  "body": "..."
}}
"""

# 语言 → Prompt 映射
_PROMPT_BY_LANG = {
    "en":    _EMAIL_PROMPT_EN,
    "fil":   _EMAIL_PROMPT_FIL,
    "mixed": _EMAIL_PROMPT_MIXED,
}


async def generate_email(state: OutreachState) -> OutreachState:
    lead     = state["lead"]
    contacts = state.get("contacts", [])
    rag_ctx  = state.get("rag_context", "")
    language = state.get("language", "mixed")   # P3-4: 默认英菲混合

    # 选择最佳联系人
    contact = contacts[0] if contacts else {}
    contact_name  = contact.get("name", "Business Owner")
    contact_title = contact.get("title", "")
    to_email      = contact.get("email", lead.get("email", ""))

    if not to_email:
        return {
            **state,
            "email_draft": None,
            "skipped": True,
            "messages": ["Email: skipped (no valid email address)"],
        }

    if not GEMINI_API_KEY:
        return {
            **state,
            "email_draft": None,
            "error": "GEMINI_API_KEY not configured",
            "messages": ["Email: failed (no Gemini API key)"],
        }

    model = _get_gemini_model()
    if model is None:
        return {
            **state,
            "email_draft": None,
            "error": "Gemini model unavailable",
            "messages": ["Email: failed (Gemini model unavailable)"],
        }

    # P3-4: 根据语言选择对应 Prompt 模板（fallback 到 mixed）
    prompt_template = _PROMPT_BY_LANG.get(language, _EMAIL_PROMPT_MIXED)

    prompt = prompt_template.format(
        business_name=lead["business_name"],
        industry_category=lead.get("industry_category", "Business"),
        address=lead.get("address", "Philippines"),
        business_size=lead.get("business_size", "small"),
        pain_points=", ".join(lead.get("pain_points", [])[:3]),
        value_proposition=lead.get("value_proposition", ""),
        recommended_product=lead.get("recommended_product", ""),
        outreach_angle=lead.get("outreach_angle", ""),
        score=lead.get("score", 0),
        contact_name=contact_name,
        contact_title=contact_title,
        rag_context=rag_ctx[:500] if rag_ctx else "No similar cases available",
    )

    try:
        response = await asyncio.to_thread(model.generate_content, prompt)
        raw = response.text.strip()
        # 去除 markdown code block
        if raw.startswith("```"):
            raw = raw.split("```")[1].removeprefix("json").strip()
        # 思考模型可能在 JSON 前附加思考文本，用 regex 提取第一个 JSON 对象
        if not raw.startswith("{"):
            m = re.search(r'\{.*\}', raw, re.DOTALL)
            if m:
                raw = m.group(0)
        data = json.loads(raw)
        subject = data.get("subject", f"Quick question about {lead['business_name']}")
        body    = data.get("body", "")
    except Exception as exc:
        logger.error(f"Email generation failed for '{lead['business_name']}': {exc}")
        return {
            **state,
            "email_draft": None,
            "error": str(exc),
            "messages": [f"Email: generation failed — {exc}"],
        }

    draft = EmailDraft(
        subject=subject,
        body=body,
        to_email=to_email,
        to_name=contact_name,
    )
    return {
        **state,
        "email_draft": draft,
        "messages": [f"Email: draft generated for {to_email} [{language}]"],
    }


# ─────────────────────────────────────────────────────────────────────────────
# Node 4: 邮件发送节点（SMTP / Gmail）
# ─────────────────────────────────────────────────────────────────────────────
async def send_email(state: OutreachState) -> OutreachState:
    draft = state.get("email_draft")
    lead  = state["lead"]

    if state.get("skipped") or not draft:
        return {**state, "messages": ["Send: skipped"]}

    if not SMTP_USER or not SMTP_PASSWORD:
        return {
            **state,
            "error": "SMTP credentials not configured",
            "messages": ["Send: failed (no SMTP credentials)"],
        }

    # 构建邮件
    msg = MIMEMultipart("alternative")
    msg["Subject"] = draft["subject"]
    msg["From"]    = f"{SENDER_NAME} <{SMTP_USER}>"
    msg["To"]      = f"{draft['to_name']} <{draft['to_email']}>"

    # 纯文本 + HTML（简单包装）
    text_part = MIMEText(draft["body"], "plain", "utf-8")
    html_body = draft["body"].replace("\n", "<br>")
    html_part = MIMEText(
        f"<html><body><p>{html_body}</p></body></html>", "html", "utf-8"
    )
    msg.attach(text_part)
    msg.attach(html_part)

    try:
        # SMTP 是同步的，放到线程池；设置连接超时防止永久挂起
        def _send():
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
                server.ehlo()
                server.starttls()
                server.login(SMTP_USER, SMTP_PASSWORD)
                server.sendmail(SMTP_USER, [draft["to_email"]], msg.as_string())

        await asyncio.to_thread(_send)
        logger.info(f"Email sent to {draft['to_email']} for '{lead['business_name']}'")

        return {
            **state,
            "send_result": {
                "status":   "sent",
                "to":       draft["to_email"],
                "subject":  draft["subject"],
                "lead_id":  lead["id"],
            },
            "messages": [f"Send: email sent to {draft['to_email']}"],
        }

    except Exception as exc:
        logger.error(f"Email send failed to {draft['to_email']}: {exc}")
        return {
            **state,
            "error": str(exc),
            "messages": [f"Send: SMTP error — {exc}"],
        }


# ─────────────────────────────────────────────────────────────────────────────
# Node 5: 记录发送日志（写回 PostgreSQL outreach_log）
# ─────────────────────────────────────────────────────────────────────────────
async def log_outreach(state: OutreachState) -> OutreachState:
    result = state.get("send_result")
    if not result or result.get("status") != "sent":
        return {**state, "messages": ["Log: skipped (not sent)"]}

    try:
        pool = await _get_db_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO outreach_log (lead_id, email, subject, status, sent_at)
                VALUES ($1, $2, $3, 'sent', NOW())
                ON CONFLICT (lead_id, email) DO NOTHING
                """,
                result["lead_id"],
                result["to"],
                result["subject"],
            )
        return {**state, "messages": ["Log: recorded in outreach_log"]}
    except Exception as exc:
        logger.error(f"outreach_log write failed: {exc}")
        return {**state, "messages": [f"Log: DB write failed — {exc}"]}


# ─────────────────────────────────────────────────────────────────────────────
# 条件路由函数
# ─────────────────────────────────────────────────────────────────────────────
def should_send(state: OutreachState) -> str:
    """email_draft 存在且无错误才发送，否则结束"""
    if state.get("skipped"):
        return "end"
    if state.get("error"):
        return "end"
    if not state.get("email_draft"):
        return "end"
    return "send"
