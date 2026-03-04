"""
MiningOrchestrator Unit Tests
Validates deduplication, concurrency, and Fallback logic
"""
from __future__ import annotations

import pytest
from typing import AsyncIterator, Optional
from unittest.mock import AsyncMock, MagicMock, patch

from app.miners.base import BaseMiner, MinerConfig, MinerHealth
from app.models.lead import LeadRaw, LeadSource
from app.orchestrator import MiningOrchestrator, MiningTask


# ── Mock Miner ────────────────────────────────────────────────────────────────
class MockMiner(BaseMiner):
    def __init__(self, source: LeadSource, leads: list, fail: bool = False):
        cfg = MinerConfig(enabled=True)
        super().__init__(cfg)
        self._source = source
        self._leads = leads
        self._fail = fail

    @property
    def source_name(self) -> LeadSource:
        return self._source

    async def mine(self, keyword, location="", lat=None, lng=None, limit=100) -> AsyncIterator[LeadRaw]:
        if self._fail:
            raise RuntimeError("Mock miner failure")
        for lead in self._leads[:limit]:
            yield lead

    async def validate_config(self) -> bool:
        return True

    async def health_check(self) -> MinerHealth:
        return MinerHealth(healthy=not self._fail, message="mock")


def make_lead(name: str, source: LeadSource = LeadSource.SERPER, phone: str = "") -> LeadRaw:
    return LeadRaw(
        source=source,
        business_name=name,
        industry_keyword="restaurant",
        phone=phone or f"+63-{name[:6]}",
    )


# ── Tests ──────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_dedup_across_miners():
    """Duplicate leads across Miners should be deduplicated"""
    lead_a = make_lead("Jollibee", LeadSource.SERPER, phone="+63-001")
    lead_b = make_lead("Jollibee", LeadSource.HUNTER, phone="+63-001")  # same phone, same dedup_key
    lead_c = make_lead("Manila Bistro", LeadSource.SERPER, phone="+63-002")

    orch = MiningOrchestrator()
    orch.register(MockMiner(LeadSource.SERPER, [lead_a, lead_c]))
    orch.register(MockMiner(LeadSource.HUNTER, [lead_b]))
    await orch.startup()

    result = await orch.run_task(MiningTask(keyword="restaurant", limit=10))

    assert result.total == 2                # lead_a and lead_b are duplicates, keep one
    assert result.dedup_removed == 1
    names = {l.business_name for l in result.leads}
    assert "Jollibee" in names
    assert "Manila Bistro" in names


@pytest.mark.asyncio
async def test_fallback_triggered_on_failure():
    """When primary Miner fails, Fallback Miner should be called"""
    fallback_lead = make_lead("Fallback Result", LeadSource.HUNTER)

    failing_miner  = MockMiner(LeadSource.SERPER, [], fail=True)
    fallback_miner = MockMiner(LeadSource.HUNTER, [fallback_lead])

    orch = MiningOrchestrator()
    orch.register(failing_miner)
    orch.register(fallback_miner, fallback_for="serper")
    await orch.startup()

    result = await orch.run_task(MiningTask(keyword="restaurant", limit=10))

    assert result.total == 1
    assert "serper" in result.errors
    assert result.leads[0].business_name == "Fallback Result"


@pytest.mark.asyncio
async def test_limit_respected():
    """Results should not exceed task.limit"""
    leads = [make_lead(f"Business {i}", phone=f"+63-00{i}") for i in range(50)]

    orch = MiningOrchestrator()
    orch.register(MockMiner(LeadSource.SERPER, leads))
    await orch.startup()

    result = await orch.run_task(MiningTask(keyword="restaurant", limit=10))
    assert result.total <= 10


@pytest.mark.asyncio
async def test_health_check_all():
    """health_check_all should return health status for each Miner"""
    orch = MiningOrchestrator()
    orch.register(MockMiner(LeadSource.SERPER, []))
    orch.register(MockMiner(LeadSource.HUNTER, [], fail=True))
    await orch.startup()

    health = await orch.health_check_all()
    assert health["serper"].healthy is True
    assert health["hunter"].healthy is False


@pytest.mark.asyncio
async def test_disabled_miner_not_called():
    """disabled Miners should not be called"""
    cfg = MinerConfig(enabled=False)
    miner = MockMiner(LeadSource.SERPER, [make_lead("Should Not Appear")])
    miner._config = cfg

    orch = MiningOrchestrator()
    orch._miners["serper"] = miner
    # Manually mark as disabled
    miner.config.enabled = False
    await orch.startup()

    result = await orch.run_task(MiningTask(keyword="restaurant", limit=10))
    assert result.total == 0
