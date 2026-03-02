"""
BaseMiner — 所有数据源插件的抽象基类
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import AsyncIterator, Optional

from app.models.lead import LeadRaw, LeadSource


@dataclass
class MinerConfig:
    """插件通用配置"""
    enabled: bool = True
    priority: int = 50                      # 0 = 最高优先级，100 = 最低
    rate_limit_per_minute: int = 60
    max_retries: int = 3
    timeout_seconds: int = 30
    batch_size: int = 20


@dataclass
class MinerHealth:
    """插件健康状态"""
    healthy: bool
    message: str
    latency_ms: Optional[float] = None
    remaining_quota: Optional[int] = None


class BaseMiner(ABC):
    """
    所有数据源插件的抽象基类。

    每个插件必须实现:
      - source_name  : 返回 LeadSource 枚举值
      - mine()       : 异步生成器，yield LeadRaw
      - validate_config(): 验证 API Key 等配置
      - health_check(): 返回 MinerHealth
    """

    def __init__(self, config: MinerConfig):
        self.config = config
        self.logger = logging.getLogger(f"miner.{self.__class__.__name__}")

    # ── 必须实现 ──────────────────────────────────────────────────────────────

    @property
    @abstractmethod
    def source_name(self) -> LeadSource:
        """返回数据源标识枚举值"""
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
        执行数据采集。

        Args:
            keyword:  行业/搜索关键词（如 "restaurant"、"salon"）
            location: 地点文字（如 "Makati, Metro Manila"）
            lat/lng:  可选坐标（部分 API 需要）
            limit:    最大返回条数

        Yields:
            LeadRaw 对象（统一输出格式）
        """
        ...

    @abstractmethod
    async def validate_config(self) -> bool:
        """
        验证插件配置是否有效（API Key 存在且格式正确等）。
        启动时调用，失败则跳过此插件。
        """
        ...

    @abstractmethod
    async def health_check(self) -> MinerHealth:
        """
        运行时健康检查，返回延迟、可用配额等。
        由 Orchestrator 按需调用，决定是否启用 fallback。
        """
        ...

    # ── 可选覆写（生命周期钩子）──────────────────────────────────────────────

    async def on_startup(self) -> None:
        """插件启动钩子，用于初始化 HTTP 客户端、浏览器等资源"""
        pass

    async def on_shutdown(self) -> None:
        """插件关闭钩子，用于释放连接池等资源"""
        pass
