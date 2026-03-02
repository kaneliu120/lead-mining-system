"""
LangGraph Graph — 外展流程图构建
"""
from __future__ import annotations

from langgraph.graph import END, StateGraph

from src.state import OutreachState
from src.nodes import (
    find_contacts,
    generate_email,
    log_outreach,
    retrieve_rag_context,
    send_email,
    should_send,
)


def build_graph() -> StateGraph:
    """
    构建外展工作流图：
    
    start → rag_retrieve ──→ find_contacts ──→ generate_email
                                                      │
                                              should_send()
                                              ┌────┴────┐
                                           "send"    "end"
                                              │         │
                                          send_email   END
                                              │
                                          log_outreach
                                              │
                                             END
    """
    workflow = StateGraph(OutreachState)

    # ── 添加节点 ──
    workflow.add_node("rag_retrieve",   retrieve_rag_context)
    workflow.add_node("find_contacts",  find_contacts)
    workflow.add_node("generate_email", generate_email)
    workflow.add_node("send_email",     send_email)
    workflow.add_node("log_outreach",   log_outreach)

    # ── 入口 ──
    workflow.set_entry_point("rag_retrieve")

    # ── 顺序边 ──
    workflow.add_edge("rag_retrieve",  "find_contacts")
    workflow.add_edge("find_contacts", "generate_email")

    # ── 条件分支 ──
    workflow.add_conditional_edges(
        "generate_email",
        should_send,
        {
            "send": "send_email",
            "end":  END,
        },
    )

    # ── 发送后记录日志 ──
    workflow.add_edge("send_email", "log_outreach")
    workflow.add_edge("log_outreach", END)

    return workflow.compile()


# 单例图实例（避免重复编译）
_graph = None


def get_graph():
    global _graph
    if _graph is None:
        _graph = build_graph()
    return _graph
