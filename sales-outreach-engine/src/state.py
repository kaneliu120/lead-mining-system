"""
LangGraph State — 外展工作流的共享状态定义
"""
from __future__ import annotations

from typing import Annotated, Any, Dict, List, Optional
from typing_extensions import TypedDict
import operator


class LeadData(TypedDict):
    """从 PostgreSQL 加载的富化线索结构"""
    id:                 int
    business_name:      str
    industry_keyword:   str
    address:            Optional[str]
    phone:              Optional[str]
    website:            Optional[str]
    email:              Optional[str]
    rating:             Optional[float]
    gmaps_url:          Optional[str]
    # AI 富化字段
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
    """单条线索的完整外展工作流状态"""
    # ── 输入 ──────────────────────────────────────────────────────────────────
    lead:               LeadData
    language:           str           # 'en' | 'fil' | 'mixed' (默认 'mixed')

    # ── RAG 检索结果 ──────────────────────────────────────────────────────────
    rag_context:        Optional[str]

    # ── 联系人（Apollo 查询） ───────────────────────────────────────────────────
    contacts:           List[Dict[str, Any]]

    # ── 邮件草稿 ──────────────────────────────────────────────────────────────
    email_draft:        Optional[EmailDraft]

    # ── 执行结果 ──────────────────────────────────────────────────────────────
    send_result:        Optional[Dict[str, Any]]     # SMTP 发送结果
    error:              Optional[str]                # 节点异常信息
    skipped:            bool                         # 是否因条件不满足而跳过

    # ── 流程追踪 ─────────────────────────────────────────────────────────────
    messages:           Annotated[List[str], operator.add]  # 执行日志


class BatchOutreachState(TypedDict):
    """批量外展任务的顶层状态"""
    leads:              List[LeadData]
    completed:          Annotated[List[OutreachState], operator.add]
    total:              int
    sent:               int
    skipped:            int
    failed:             int
