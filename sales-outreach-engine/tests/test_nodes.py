"""
OutreachState node unit tests
Using mocks to isolate external dependencies (DB, Gemini, SMTP)
"""
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.state import OutreachState, LeadData
from src.nodes import should_send


# ── Test helpers ──────────────────────────────────────────────────────────────

def make_state(
    score: int = 80,
    email: str = "owner@biz.ph",
    contacts: list | None = None,
    error: str | None = None,
    skipped: bool = False,
) -> OutreachState:
    lead: LeadData = {
        "id": 1,
        "business_name": "Test Business",
        "industry_keyword": "restaurant",
        "address": "123 Roxas Blvd, Manila",
        "phone": "+63 2 8888 8888",
        "website": "https://testbiz.ph",
        "email": email,
        "rating": 4.5,
        "gmaps_url": "https://goo.gl/maps/xyz",
        "industry_category": "Food & Beverage",
        "business_size": "small",
        "score": score,
        "pain_points": ["No website", "Low online presence"],
        "value_proposition": "We can help you grow online.",
        "recommended_product": "Website Package",
        "outreach_angle": "You have great reviews but no website.",
    }
    return OutreachState(
        lead=lead,
        language="mixed",
        rag_context=None,
        contacts=contacts or [],
        email_draft=None,
        send_result=None,
        error=error,
        skipped=skipped,
        messages=[],
    )


# ── should_send routing logic ─────────────────────────────────────────────────

def test_should_send_returns_send_when_contact_found():
    """Should route to send when email_draft is generated without errors"""
    state = make_state(contacts=[{"email": "mgr@biz.ph", "first_name": "Maria"}])
    # Set email_draft, simulating successful generation
    state["email_draft"] = {
        "subject": "Hi Maria!",
        "body": "We'd like to help your business...",
        "to_email": "mgr@biz.ph",
        "to_name": "Maria",
    }
    result = should_send(state)
    assert result == "send"


def test_should_send_returns_skip_when_no_contacts():
    """Should route to end when no email_draft (often because no contact found)"""
    state = make_state(contacts=[])
    # email_draft is None (make_state default)
    result = should_send(state)
    assert result == "end"


def test_should_send_returns_skip_when_error():
    """Should route to end when an error occurs"""
    state = make_state(contacts=[{"email": "a@b.com"}], error="Gemini timeout")
    result = should_send(state)
    assert result == "end"


def test_should_send_returns_skip_when_already_skipped():
    """Should route to end when marked as skipped"""
    state = make_state(contacts=[{"email": "a@b.com"}], skipped=True)
    result = should_send(state)
    assert result == "end"


# ── OutreachState structure ───────────────────────────────────────────────────

def test_state_messages_are_appendable():
    """messages field should support operator.add accumulation"""
    import operator
    s1 = make_state()
    s1["messages"] = ["step 1"]
    s2 = make_state()
    s2["messages"] = ["step 2"]
    combined = operator.add(s1["messages"], s2["messages"])
    assert combined == ["step 1", "step 2"]


def test_state_lead_data_fields():
    """LeadData should contain all required fields"""
    state = make_state(score=95)
    lead = state["lead"]
    assert lead["score"] == 95
    assert lead["business_name"] == "Test Business"
    assert isinstance(lead["pain_points"], list)
    assert len(lead["pain_points"]) > 0


# ── close_db_pool close logic (bug fix verification) ─────────────────────────

@pytest.mark.asyncio
async def test_close_db_pool_idempotent():
    """Multiple calls to close_db_pool should not raise exceptions (idempotent)"""
    from src.nodes import close_db_pool
    # Ensure safe when pool is not initialized
    await close_db_pool()
    await close_db_pool()  # Second call should also be safe


@pytest.mark.asyncio
async def test_close_db_pool_closes_real_pool():
    """After pool initialization, calling close_db_pool should properly close it"""
    import src.nodes as nodes_module

    # Replace global _db_pool with mock pool
    mock_pool = AsyncMock()
    mock_pool.close = AsyncMock()
    nodes_module._db_pool = mock_pool

    from src.nodes import close_db_pool
    await close_db_pool()

    mock_pool.close.assert_called_once()
    assert nodes_module._db_pool is None
