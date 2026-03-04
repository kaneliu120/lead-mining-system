"""
SerperMiner Unit Tests
Uses httpx mock to avoid real API calls
"""
from __future__ import annotations

import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from app.miners.plugins.serper_miner import SerperConfig, SerperMiner
from app.models.lead import LeadSource


MOCK_SERPER_RESPONSE = {
    "places": [
        {
            "title":        "Jollibee Makati",
            "address":      "123 Ayala Ave, Makati, Metro Manila",
            "phoneNumber":  "+63 2 1234 5678",
            "website":      "https://www.jollibee.com.ph",
            "rating":       4.3,
            "ratingCount":  892,
            "placeId":      "ChIJabc123",
            "latitude":     14.5547,
            "longitude":    121.0244,
            "category":     "Fast Food Restaurant",
        },
        {
            "title":    "Manila Bistro",
            "address":  "456 Roxas Blvd, Manila",
            "rating":   4.0,
            "ratingCount": 321,
            "placeId":  "ChIJdef456",
        },
    ]
}


@pytest.fixture
def serper_config():
    return SerperConfig(
        api_key="test_key_12345",
        country_code="ph",
    )


@pytest.fixture
def serper_miner(serper_config):
    return SerperMiner(serper_config)


@pytest.mark.asyncio
async def test_mine_returns_leads(serper_miner):
    """mine() should convert API response into LeadRaw objects"""
    mock_response = MagicMock()
    mock_response.json.return_value = MOCK_SERPER_RESPONSE

    with patch.object(
        serper_miner, "_request_with_retry", new=AsyncMock(return_value=mock_response)
    ):
        await serper_miner.on_startup()
        leads = []
        async for lead in serper_miner.mine("restaurant", "Manila", limit=10):
            leads.append(lead)

    assert len(leads) == 2
    assert leads[0].business_name == "Jollibee Makati"
    assert leads[0].source == LeadSource.SERPER
    assert leads[0].phone == "+63 2 1234 5678"
    assert leads[0].rating == 4.3
    assert leads[0].review_count == 892
    assert leads[0].metadata["place_id"] == "ChIJabc123"


@pytest.mark.asyncio
async def test_mine_dedup_key_not_empty(serper_miner):
    """dedup_key should not be empty"""
    mock_response = MagicMock()
    mock_response.json.return_value = MOCK_SERPER_RESPONSE

    with patch.object(
        serper_miner, "_request_with_retry", new=AsyncMock(return_value=mock_response)
    ):
        await serper_miner.on_startup()
        async for lead in serper_miner.mine("restaurant", "Manila", limit=5):
            assert lead.dedup_key() != ""
            assert len(lead.dedup_key()) > 5


@pytest.mark.asyncio
async def test_mine_respects_limit(serper_miner):
    """mine() should not exceed the limit count"""
    mock_response = MagicMock()
    mock_response.json.return_value = MOCK_SERPER_RESPONSE   # 2 results

    with patch.object(
        serper_miner, "_request_with_retry", new=AsyncMock(return_value=mock_response)
    ):
        await serper_miner.on_startup()
        leads = []
        async for lead in serper_miner.mine("restaurant", limit=1):
            leads.append(lead)

    assert len(leads) == 1


@pytest.mark.asyncio
async def test_mine_empty_response(serper_miner):
    """Empty response should not raise an exception"""
    mock_response = MagicMock()
    mock_response.json.return_value = {"places": []}

    with patch.object(
        serper_miner, "_request_with_retry", new=AsyncMock(return_value=mock_response)
    ):
        await serper_miner.on_startup()
        leads = []
        async for lead in serper_miner.mine("xyz_nonexistent"):
            leads.append(lead)

    assert leads == []


@pytest.mark.asyncio
async def test_validate_config_valid(serper_miner):
    assert await serper_miner.validate_config() is True


@pytest.mark.asyncio
async def test_validate_config_empty_key():
    cfg = SerperConfig(api_key="")
    miner = SerperMiner(cfg)
    assert await miner.validate_config() is False
