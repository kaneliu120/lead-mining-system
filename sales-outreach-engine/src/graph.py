"""
LangGraph Graph — Outreach workflow graph construction
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
    Build the outreach workflow graph:
    
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

    # ── Add nodes ──
    workflow.add_node("rag_retrieve",   retrieve_rag_context)
    workflow.add_node("find_contacts",  find_contacts)
    workflow.add_node("generate_email", generate_email)
    workflow.add_node("send_email",     send_email)
    workflow.add_node("log_outreach",   log_outreach)

    # ── Entry point ──
    workflow.set_entry_point("rag_retrieve")

    # ── Sequential edges ──
    workflow.add_edge("rag_retrieve",  "find_contacts")
    workflow.add_edge("find_contacts", "generate_email")

    # ── Conditional branch ──
    workflow.add_conditional_edges(
        "generate_email",
        should_send,
        {
            "send": "send_email",
            "end":  END,
        },
    )

    # ── Log after sending ──
    workflow.add_edge("send_email", "log_outreach")
    workflow.add_edge("log_outreach", END)

    return workflow.compile()


# Singleton graph instance (avoid repeated compilation)
_graph = None


def get_graph():
    global _graph
    if _graph is None:
        _graph = build_graph()
    return _graph
