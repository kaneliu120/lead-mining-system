"""
LangGraph State — Shared state definition for the outreach workflow
"""
from __future__ import annotations

from typing import Annotated, Any, Dict, List, Optional
from typing_extensions import TypedDict
import operator


class LeadData(TypedDict):
    """Structure of enriched leads loaded from PostgreSQL"""
    id:                 int
    business_name:      str
    industry_keyword:   str
    address:            Optional[str]
    phone:              Optional[str]
    website:            Optional[str]
    email:              Optional[str]
    rating:             Optional[float]
    gmaps_url:          Optional[str]
    # AI enrichment fields
    industry_category:  Optional[str]
    business_size:      Optional[str]
    score:              int
    pain_points:        List[str]
    value_proposition:  Optional[str]
    recommended_product: Optional[str]
    outreach_angle:     Optional[str]


class EmailDraft(TypedDict):
    subject:    str
    body:       str
    to_email:   str
    to_name:    str


class OutreachState(TypedDict):
    """Complete outreach workflow state for a single lead"""
    # ── Input ─────────────────────────────────────────────────────────────────
    lead:               LeadData
    language:           str           # 'en' | 'fil' | 'mixed' (default 'mixed')

    # ── RAG retrieval results ─────────────────────────────────────────────────
    rag_context:        Optional[str]

    # ── Contacts (Apollo query) ───────────────────────────────────────────────
    contacts:           List[Dict[str, Any]]

    # ── Email draft ───────────────────────────────────────────────────────────
    email_draft:        Optional[EmailDraft]

    # ── Execution results ─────────────────────────────────────────────────────
    send_result:        Optional[Dict[str, Any]]     # SMTP send result
    error:              Optional[str]                # Node exception info
    skipped:            bool                         # Whether skipped due to unmet conditions

    # ── Process tracking ──────────────────────────────────────────────────────
    messages:           Annotated[List[str], operator.add]  # Execution log


class BatchOutreachState(TypedDict):
    """Top-level state for batch outreach tasks"""
    leads:              List[LeadData]
    completed:          Annotated[List[OutreachState], operator.add]
    total:              int
    sent:               int
    skipped:            int
    failed:             int
