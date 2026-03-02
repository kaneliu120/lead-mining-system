"""
RAG Tool — 供 LangGraph Agent 调用的 RAG 工具
通过 Lead Mining Engine /rag/query 接口查询相似线索
"""
from __future__ import annotations

import os
from typing import Optional

import httpx
from langchain_core.tools import tool

LEAD_MINER_URL = os.environ.get("LEAD_MINER_URL", "http://lead-miner:8000")


@tool
async def query_similar_leads(query: str, n: int = 5) -> str:
    """
    Query the lead database for businesses similar to the given description.
    Returns a formatted string of similar leads for context.
    
    Args:
        query: Natural language description of the business type or need
        n: Number of similar leads to retrieve (default: 5)
    """
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                f"{LEAD_MINER_URL}/rag/query",
                json={"query": query, "n": min(n, 10)},
            )
            resp.raise_for_status()
            results = resp.json().get("results", [])
    except Exception as exc:
        return f"RAG query failed: {exc}"

    if not results:
        return "No similar leads found in the database."

    lines = ["Similar businesses in our database:"]
    for i, r in enumerate(results, 1):
        meta = r.get("metadata", {})
        dist = r.get("distance")
        similarity = f" (similarity: {1 - dist:.2f})" if dist is not None else ""
        lines.append(
            f"{i}. {meta.get('business_name', 'Unknown')} — "
            f"{meta.get('industry_keyword', '')} in {meta.get('address', 'Philippines')}"
            f"{similarity}"
        )
        doc = r.get("document", "")
        if doc:
            lines.append(f"   {doc[:150]}")

    return "\n".join(lines)


@tool
async def get_lead_details(lead_id: int) -> str:
    """
    Get detailed information about a specific lead by ID.
    
    Args:
        lead_id: The database ID of the lead
    """
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                f"{LEAD_MINER_URL}/leads",
                params={"limit": 200},     # fetch and filter locally by id
            )
            resp.raise_for_status()
            data = resp.json()
            all_leads = data.get("leads", [])
            leads = [l for l in all_leads if l.get("id") == lead_id]
    except Exception as exc:
        return f"Failed to get lead details: {exc}"

    if not leads:
        return f"Lead with ID {lead_id} not found."

    lead = leads[0]
    return (
        f"Business: {lead.get('business_name')}\n"
        f"Industry: {lead.get('industry_category', 'N/A')}\n"
        f"Score: {lead.get('score', 0)}/100\n"
        f"Pain Points: {', '.join(lead.get('pain_points') or [])}\n"
        f"Value Prop: {lead.get('value_proposition', 'N/A')}\n"
        f"Outreach Angle: {lead.get('outreach_angle', 'N/A')}"
    )
