"""
LeadRaw / LeadEnriched 模型单元测试
验证字段校验、dedup_key、序列化等
"""
from __future__ import annotations

import pytest
from datetime import datetime, timezone

from app.models.lead import LeadRaw, LeadSource


# ── LeadRaw 基础构造 ──────────────────────────────────────────────────────────

def test_leadraw_minimal_fields():
    """只需 source + business_name + industry_keyword 即可创建"""
    lead = LeadRaw(
        source=LeadSource.SERPER,
        business_name="Test Biz",
        industry_keyword="restaurant",
    )
    assert lead.business_name == "Test Biz"
    assert lead.source == LeadSource.SERPER
    assert lead.enriched is False
    assert lead.score is None


def test_leadraw_defaults():
    """可选字段应有合理默认值"""
    lead = LeadRaw(
        source=LeadSource.SERPER,
        business_name="Biz",
        industry_keyword="kw",
    )
    assert lead.address == ""
    assert lead.phone == ""
    assert lead.website == ""
    assert lead.email == ""
    assert lead.metadata == {}
    assert isinstance(lead.collected_at, datetime)


def test_leadraw_with_all_fields():
    """完整字段赋值应正确保存"""
    lead = LeadRaw(
        source=LeadSource.HUNTER,
        business_name="Full Corp",
        industry_keyword="finance",
        address="123 Main St, Manila",
        phone="+63 2 1234 5678",
        website="https://fullcorp.ph",
        email="hello@fullcorp.ph",
        rating=4.7,
        review_count=120,
        lat=14.5995,
        lng=120.9842,
        google_maps_url="https://goo.gl/maps/abc",
        metadata={"cid": "12345"},
    )
    assert lead.phone == "+63 2 1234 5678"
    assert lead.rating == 4.7
    assert lead.metadata["cid"] == "12345"


# ── dedup_key 逻辑 ────────────────────────────────────────────────────────────

def test_dedup_key_phone_priority():
    """有电话时，dedup_key 应以 phone: 为前缀"""
    lead = LeadRaw(
        source=LeadSource.SERPER,
        business_name="Biz",
        industry_keyword="food",
        phone="+63-001-111",
        email="a@b.com",
    )
    key = lead.dedup_key()
    assert key.startswith("phone:")
    assert "+63-001-111" in key


def test_dedup_key_email_fallback():
    """没有电话但有邮件时，dedup_key 应包含业务名称（name_addr: 前缀）"""
    lead = LeadRaw(
        source=LeadSource.SERPER,
        business_name="Biz",
        industry_keyword="food",
        email="contact@biz.com",
    )
    key = lead.dedup_key()
    # 实现使用 name_addr: 前缀作为通用 fallback
    assert key.startswith("name_addr:") or key.startswith("email:")
    assert len(key) > 5


def test_dedup_key_name_fallback():
    """无电话无邮件时，dedup_key 应包含业务名称（name_addr: 前缀）"""
    lead = LeadRaw(
        source=LeadSource.SERPER,
        business_name="Unique Restaurant",
        industry_keyword="food",
    )
    key = lead.dedup_key()
    assert key.startswith("name_addr:") or key.startswith("name:")
    assert "unique" in key.lower() or "restaurant" in key.lower()


def test_dedup_key_same_phone_different_source():
    """相同电话不同 source 的线索应有相同 dedup_key（用于跨 Miner 去重）"""
    lead_a = LeadRaw(source=LeadSource.SERPER, business_name="A", industry_keyword="kw", phone="+63-111")
    lead_b = LeadRaw(source=LeadSource.HUNTER, business_name="B", industry_keyword="kw", phone="+63-111")
    assert lead_a.dedup_key() == lead_b.dedup_key()


def test_dedup_key_not_empty():
    """任意线索的 dedup_key 都不应为空"""
    lead = LeadRaw(source=LeadSource.SERPER, business_name="X", industry_keyword="y")
    assert lead.dedup_key() != ""
    assert len(lead.dedup_key()) > 3


# ── LeadSource 枚举 ───────────────────────────────────────────────────────────

def test_lead_source_values():
    """LeadSource 枚举值应存在"""
    assert LeadSource.SERPER.value == "serper"
    assert LeadSource.HUNTER.value == "hunter"


def test_lead_source_from_string():
    """可通过字符串构造 LeadSource"""
    assert LeadSource("serper") == LeadSource.SERPER


# ── review_count Optional 语义（facebook_miner Bug 修复验证）────────────────

def test_review_count_none_preserved():
    """review_count=None 不应被强制转为 0，Optional 语义必须保留"""
    lead = LeadRaw(
        source=LeadSource.SERPER,
        business_name="No Reviews Yet",
        industry_keyword="food",
        review_count=None,
    )
    assert lead.review_count is None   # None ≠ 0：表示"无数据"而非"零评论"


def test_review_count_zero_kept():
    """review_count=0 应保留为 0（真实的零评论）"""
    lead = LeadRaw(
        source=LeadSource.SERPER,
        business_name="New Biz",
        industry_keyword="food",
        review_count=0,
    )
    assert lead.review_count == 0
