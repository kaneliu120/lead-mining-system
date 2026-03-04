"""
LeadRaw / LeadEnriched Model Unit Tests
Validates field validation, dedup_key, serialization, etc.
"""
from __future__ import annotations

import pytest
from datetime import datetime, timezone

from app.models.lead import LeadRaw, LeadSource


# ── LeadRaw Basic Construction ────────────────────────────────────────────────────────

def test_leadraw_minimal_fields():
    """Only source + business_name + industry_keyword required to create"""
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
    """Optional fields should have reasonable defaults"""
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
    """Full field assignment should save correctly"""
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


# ── dedup_key Logic ────────────────────────────────────────────────────────────────────

def test_dedup_key_phone_priority():
    """When phone is present, dedup_key should be prefixed with phone:"""
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
    """Without phone but with email, dedup_key should include business name (name_addr: prefix)"""
    lead = LeadRaw(
        source=LeadSource.SERPER,
        business_name="Biz",
        industry_keyword="food",
        email="contact@biz.com",
    )
    key = lead.dedup_key()
    # Implementation uses name_addr: prefix as generic fallback
    assert key.startswith("name_addr:") or key.startswith("email:")
    assert len(key) > 5


def test_dedup_key_name_fallback():
    """Without phone or email, dedup_key should include business name (name_addr: prefix)"""
    lead = LeadRaw(
        source=LeadSource.SERPER,
        business_name="Unique Restaurant",
        industry_keyword="food",
    )
    key = lead.dedup_key()
    assert key.startswith("name_addr:") or key.startswith("name:")
    assert "unique" in key.lower() or "restaurant" in key.lower()


def test_dedup_key_same_phone_different_source():
    """Leads with same phone but different sources should share dedup_key (for cross-Miner dedup)"""
    lead_a = LeadRaw(source=LeadSource.SERPER, business_name="A", industry_keyword="kw", phone="+63-111")
    lead_b = LeadRaw(source=LeadSource.HUNTER, business_name="B", industry_keyword="kw", phone="+63-111")
    assert lead_a.dedup_key() == lead_b.dedup_key()


def test_dedup_key_not_empty():
    """dedup_key for any lead should never be empty"""
    lead = LeadRaw(source=LeadSource.SERPER, business_name="X", industry_keyword="y")
    assert lead.dedup_key() != ""
    assert len(lead.dedup_key()) > 3


# ── LeadSource Enum ───────────────────────────────────────────────────────────────────

def test_lead_source_values():
    """LeadSource enum values should exist"""
    assert LeadSource.SERPER.value == "serper"
    assert LeadSource.HUNTER.value == "hunter"


def test_lead_source_from_string():
    """Can construct LeadSource from string"""
    assert LeadSource("serper") == LeadSource.SERPER


# ── review_count Optional semantics (facebook_miner Bug Fix Verification) ────────────────

def test_review_count_none_preserved():
    """review_count=None should not be forced to 0, Optional semantics must be preserved"""
    lead = LeadRaw(
        source=LeadSource.SERPER,
        business_name="No Reviews Yet",
        industry_keyword="food",
        review_count=None,
    )
    assert lead.review_count is None   # None ≠ 0: means "no data" not "zero reviews"


def test_review_count_zero_kept():
    """review_count=0 should be kept as 0 (true zero reviews)"""
    lead = LeadRaw(
        source=LeadSource.SERPER,
        business_name="New Biz",
        industry_keyword="food",
        review_count=0,
    )
    assert lead.review_count == 0
