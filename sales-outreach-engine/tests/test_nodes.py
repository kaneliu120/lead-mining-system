"""
OutreachState 节点单元测试
使用 mock 隔离外部依赖（DB、Gemini、SMTP）
"""
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.state import OutreachState, LeadData
from src.nodes import should_send


# ── 测试辅助 ──────────────────────────────────────────────────────────────────

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


# ── should_send 路由逻辑 ──────────────────────────────────────────────────────

def test_should_send_returns_send_when_contact_found():
    """生成了 email_draft 且无错误时应路由至 send"""
    state = make_state(contacts=[{"email": "mgr@biz.ph", "first_name": "Maria"}])
    # 设置 email_draft，模拟生成成功的情况
    state["email_draft"] = {
        "subject": "Hi Maria!",
        "body": "We'd like to help your business...",
        "to_email": "mgr@biz.ph",
        "to_name": "Maria",
    }
    result = should_send(state)
    assert result == "send"


def test_should_send_returns_skip_when_no_contacts():
    """无 email_draft（常因无联系人而未生成）应路由至 end"""
    state = make_state(contacts=[])
    # email_draft 为 None（make_state 默认）
    result = should_send(state)
    assert result == "end"


def test_should_send_returns_skip_when_error():
    """发生错误时应路由至 end"""
    state = make_state(contacts=[{"email": "a@b.com"}], error="Gemini timeout")
    result = should_send(state)
    assert result == "end"


def test_should_send_returns_skip_when_already_skipped():
    """已标记 skipped 时应路由至 end"""
    state = make_state(contacts=[{"email": "a@b.com"}], skipped=True)
    result = should_send(state)
    assert result == "end"


# ── OutreachState 结构 ───────────────────────────────────────────────────────

def test_state_messages_are_appendable():
    """messages 字段应支持 operator.add 累积"""
    import operator
    s1 = make_state()
    s1["messages"] = ["step 1"]
    s2 = make_state()
    s2["messages"] = ["step 2"]
    combined = operator.add(s1["messages"], s2["messages"])
    assert combined == ["step 1", "step 2"]


def test_state_lead_data_fields():
    """LeadData 应包含所有必要字段"""
    state = make_state(score=95)
    lead = state["lead"]
    assert lead["score"] == 95
    assert lead["business_name"] == "Test Business"
    assert isinstance(lead["pain_points"], list)
    assert len(lead["pain_points"]) > 0


# ── close_db_pool 关闭逻辑（Bug 修复验证）───────────────────────────────────

@pytest.mark.asyncio
async def test_close_db_pool_idempotent():
    """多次调用 close_db_pool 不应抛异常（幂等）"""
    from src.nodes import close_db_pool
    # 确保调用时 pool 未初始化的情况下也安全
    await close_db_pool()
    await close_db_pool()  # 二次调用应安全


@pytest.mark.asyncio
async def test_close_db_pool_closes_real_pool():
    """初始化 pool 后调用 close_db_pool 应正确关闭"""
    import src.nodes as nodes_module

    # 用 mock pool 替换全局 _db_pool
    mock_pool = AsyncMock()
    mock_pool.close = AsyncMock()
    nodes_module._db_pool = mock_pool

    from src.nodes import close_db_pool
    await close_db_pool()

    mock_pool.close.assert_called_once()
    assert nodes_module._db_pool is None
