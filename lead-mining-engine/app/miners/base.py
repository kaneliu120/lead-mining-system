"""
BaseMiner — abstract base class for all data source plugins
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import AsyncIterator, Optional

from app.models.lead import LeadRaw, LeadSource


@dataclass
class MinerConfig:
    """Common plugin configuration"""
    enabled: bool = True
    priority: int = 50                      # 0 = highest priority, 100 = lowest
    rate_limit_per_minute: int = 60
    max_retries: int = 3
    timeout_seconds: int = 30
    batch_size: int = 20


@dataclass
class MinerHealth:
    """Plugin health status"""
    healthy: bool
    message: str
    latency_ms: Optional[float] = None
    remaining_quota: Optional[int] = None


class BaseMiner(ABC):
    """
    Abstract base class for all data source plugins.

    Each plugin must implement:
      - source_name  : return LeadSource enum value
      - mine()       : async generator, yield LeadRaw
      - validate_config(): validate API Key and other config
      - health_check(): return MinerHealth
    """

    def __init__(self, config: MinerConfig):
        self.config = config
        self.logger = logging.getLogger(f"miner.{self.__class__.__name__}")

    # ── Must implement ───────────────────────────────────────────────────────

    @property
    @abstractmethod
    def source_name(self) -> LeadSource:
        """Return the data source identifier enum value"""
        ...

    @abstractmethod
    async def mine(
        self,
        keyword: str,
        location: str = "",
        lat: Optional[float] = None,
        lng: Optional[float] = None,
        limit: int = 100,
    ) -> AsyncIterator[LeadRaw]:
        """
        Execute data mining.

        Args:
            keyword:  industry/search keyword (e.g. "restaurant", "salon")
            location: location text (e.g. "Makati, Metro Manila")
            lat/lng:  optional coordinates (required by some APIs)
            limit:    maximum number of results to return

        Yields:
            LeadRaw objects (unified output format)
        """
        ...

    @abstractmethod
    async def validate_config(self) -> bool:
        """
        Validate that the plugin configuration is valid (API Key exists and is correctly formatted, etc.).
        Called at startup; if it fails, this plugin is skipped.
        """
        ...

    @abstractmethod
    async def health_check(self) -> MinerHealth:
        """
        Runtime health check, returns latency, available quota, etc.
        Called by the Orchestrator on demand to decide whether to enable fallback.
        """
        ...

    # ── Optional overrides (lifecycle hooks) ─────────────────────────────────

    async def on_startup(self) -> None:
        """Plugin startup hook, used to initialize HTTP clients, browsers, and other resources"""
        pass

    async def on_shutdown(self) -> None:
        """Plugin shutdown hook, used to release connection pools and other resources"""
        pass
