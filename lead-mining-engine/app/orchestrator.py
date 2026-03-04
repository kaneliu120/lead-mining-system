"""
MiningOrchestrator — core scheduler
Manages lifecycle, dedup, fallback, and concurrency control for all Miner plugins
"""
from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from app.miners.base import BaseMiner, MinerHealth
from app.models.lead import LeadRaw

logger = logging.getLogger(__name__)


@dataclass
class MiningTask:
    keyword: str
    location: str = ""
    limit: int = 100
    sources: Optional[List[str]] = None     # None = all enabled miners


@dataclass
class MiningResult:
    task: MiningTask
    leads: List[LeadRaw]
    source_counts: Dict[str, int]
    dedup_removed: int
    errors: Dict[str, str] = field(default_factory=dict)
    duration_seconds: float = 0.0

    @property
    def total(self) -> int:
        return len(self.leads)


class MiningOrchestrator:
    """
    Plugin-based Miner scheduler.
    - Register/start/stop Miner plugins
    - Concurrently call multiple Miners, collect results
    - Deduplicate based on dedup_key
    - Source-level fallback (switch to backup if primary plugin fails)
    """

    def __init__(self, max_concurrent_miners: int = 4, miner_timeout_seconds: int = 120):
        self._miners: Dict[str, BaseMiner] = {}         # source_name → miner
        self._fallbacks: Dict[str, str] = {}            # source → fallback_source
        self._semaphore = asyncio.Semaphore(max_concurrent_miners)
        self._miner_timeout_seconds = miner_timeout_seconds
        self._started = False

    def register(
        self,
        miner: BaseMiner,
        fallback_for: Optional[str] = None,
    ) -> None:
        """
        Register a Miner plugin.
        fallback_for: when the specified source fails, enable this Miner as backup.
        """
        name = miner.source_name.value
        self._miners[name] = miner
        if fallback_for:
            self._fallbacks[fallback_for] = name
        logger.info(f"Orchestrator: registered miner '{name}' (fallback_for={fallback_for})")

    async def startup(self) -> None:
        """Concurrently start all registered Miners"""
        if self._started:
            return

        tasks = []
        for name, miner in self._miners.items():
            if miner.config.enabled:
                tasks.append(self._startup_miner(name, miner))

        await asyncio.gather(*tasks)
        self._started = True
        logger.info(f"Orchestrator started with {len(self._miners)} miners")

    async def _startup_miner(self, name: str, miner: BaseMiner) -> None:
        try:
            await miner.on_startup()
        except Exception as exc:
            logger.error(f"Orchestrator: failed to start '{name}': {exc}")

    async def shutdown(self) -> None:
        """Concurrently shut down all Miners"""
        tasks = [miner.on_shutdown() for miner in self._miners.values()]
        await asyncio.gather(*tasks, return_exceptions=True)
        self._started = False
        logger.info("Orchestrator: all miners shut down")

    async def run_task(self, task: MiningTask) -> MiningResult:
        """
        Execute a mining task:
        1. Determine which Miners to call
        2. Execute concurrently with timeout protection
        3. Merge results and deduplicate
        4. Trigger fallback (if any)
        """
        import time
        start = time.monotonic()

        active_miners = self._get_active_miners(task.sources)
        if not active_miners:
            logger.warning("Orchestrator: no active miners for task")
            return MiningResult(task=task, leads=[], source_counts={}, dedup_removed=0)

        # Per-Miner limit
        per_miner_limit = max(task.limit // max(len(active_miners), 1), 10)

        tasks = [
            self._run_single_miner(miner, task, per_miner_limit)
            for miner in active_miners
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_leads: List[LeadRaw] = []
        source_counts: Dict[str, int] = {}
        errors: Dict[str, str] = {}

        for miner, result in zip(active_miners, results):
            name = miner.source_name.value
            if isinstance(result, Exception):
                errors[name] = str(result)
                # Trigger fallback
                fallback = await self._try_fallback(name, task, per_miner_limit)
                if fallback:
                    all_leads.extend(fallback)
                    source_counts[f"{name}_fallback"] = len(fallback)
            else:
                all_leads.extend(result)
                source_counts[name] = len(result)

        # Deduplicate
        seen = set()
        deduped: List[LeadRaw] = []
        for lead in all_leads:
            key = lead.dedup_key()
            if key not in seen:
                seen.add(key)
                deduped.append(lead)

        dedup_removed = len(all_leads) - len(deduped)
        duration = time.monotonic() - start

        logger.info(
            f"Orchestrator task done: {len(deduped)} leads "
            f"(dedup removed {dedup_removed}) in {duration:.1f}s"
        )
        return MiningResult(
            task=task,
            leads=deduped[: task.limit],
            source_counts=source_counts,
            dedup_removed=dedup_removed,
            errors=errors,
            duration_seconds=duration,
        )

    async def _run_single_miner(
        self,
        miner: BaseMiner,
        task: MiningTask,
        limit: int,
    ) -> List[LeadRaw]:
        async with self._semaphore:
            name = miner.source_name.value
            try:
                return await asyncio.wait_for(
                    self._collect_leads(miner, task, limit),
                    timeout=self._miner_timeout_seconds,
                )
            except asyncio.TimeoutError:
                logger.error(
                    f"Miner '{name}' timed out after {self._miner_timeout_seconds}s "
                    f"(keyword='{task.keyword}')"
                )
                raise
            except Exception as exc:
                logger.error(f"Miner '{name}' failed: {exc}")
                raise

    async def _collect_leads(
        self,
        miner: BaseMiner,
        task: MiningTask,
        limit: int,
    ) -> List[LeadRaw]:
        """Wrap an async generator as a single coroutine manageable by wait_for"""
        leads: List[LeadRaw] = []
        gen = miner.mine(
            keyword=task.keyword,
            location=task.location,
            limit=limit,
        )
        async for lead in gen:
            leads.append(lead)
            if len(leads) >= limit:
                break
        return leads

    async def _try_fallback(
        self,
        failed_source: str,
        task: MiningTask,
        limit: int,
    ) -> Optional[List[LeadRaw]]:
        fallback_name = self._fallbacks.get(failed_source)
        if not fallback_name:
            return None
        fallback_miner = self._miners.get(fallback_name)
        if not fallback_miner or not fallback_miner.config.enabled:
            return None

        logger.info(f"Orchestrator: using fallback '{fallback_name}' for '{failed_source}'")
        try:
            return await self._run_single_miner(fallback_miner, task, limit)
        except Exception:
            return None

    def _get_active_miners(self, sources: Optional[List[str]]) -> List[BaseMiner]:
        if sources:
            return [
                self._miners[s]
                for s in sources
                if s in self._miners and self._miners[s].config.enabled
            ]
        return [m for m in self._miners.values() if m.config.enabled]

    async def health_check_all(self) -> Dict[str, MinerHealth]:
        """Concurrently check health status of all Miners"""
        tasks = {
            name: miner.health_check()
            for name, miner in self._miners.items()
            if miner.config.enabled
        }
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        health: Dict[str, MinerHealth] = {}
        for name, result in zip(tasks.keys(), results):
            if isinstance(result, Exception):
                health[name] = MinerHealth(healthy=False, message=str(result))
            else:
                health[name] = result
        return health
