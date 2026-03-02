"""
Config — 配置加载与插件工厂
从 YAML + 环境变量构建所有 Miner 实例
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import yaml
except ImportError:
    yaml = None  # type: ignore

from app.miners.base import MinerConfig
from app.miners.plugins.serper_miner import SerperConfig, SerperMiner
from app.miners.plugins.hunter_miner import HunterConfig, HunterMiner
from app.miners.plugins.sec_ph_miner import SECPhConfig, SECPhilippinesMiner
from app.miners.plugins.google_cse_miner import GoogleCSEConfig, GoogleCSEMiner
from app.miners.plugins.reddit_miner import RedditConfig, RedditMiner
from app.miners.plugins.philgeps_miner import PhilGEPSConfig, PhilGEPSMiner
from app.miners.plugins.facebook_miner import FacebookConfig, FacebookMiner
from app.miners.plugins.dti_bnrs_miner import DTIBNRSConfig, DTIBNRSMiner
from app.miners.plugins.yellowpages_miner import YellowPagesConfig, YellowPagesMiner
from app.enrichers.gemini_enricher import GeminiEnricher
from app.orchestrator import MiningOrchestrator
from app.writers.postgres_writer import PostgresWriter
from app.writers.chroma_writer import ChromaWriter


# ── 环境变量快捷读取 ────────────────────────────────────────────────────────────
def _env(key: str, default: str = "") -> str:
    return os.environ.get(key, default)


def _env_bool(key: str, default: bool = True) -> bool:
    v = os.environ.get(key, "").lower()
    if v in ("0", "false", "no"):
        return False
    if v in ("1", "true", "yes"):
        return True
    return default


# ── 插件工厂字典 ────────────────────────────────────────────────────────────────
FACTORY: Dict[str, Any] = {
    "serper": {
        "config_cls":  SerperConfig,
        "miner_cls":   SerperMiner,
        "env_key":     "SERPER_API_KEY",
        "phase":       1,
    },
    "hunter": {
        "config_cls":  HunterConfig,
        "miner_cls":   HunterMiner,
        "env_key":     "HUNTER_API_KEY",   # https://hunter.io 免费注册
        "phase":       1,
    },
    "google_cse": {
        "config_cls":  GoogleCSEConfig,
        "miner_cls":   GoogleCSEMiner,
        "env_key":     "GOOGLE_API_KEY",
        "phase":       2,
    },
    "sec_ph": {
        "config_cls":  SECPhConfig,
        "miner_cls":   SECPhilippinesMiner,
        "env_key":     None,            # 无需 API Key
        "phase":       2,
    },
    "reddit": {
        "config_cls":  RedditConfig,
        "miner_cls":   RedditMiner,
        "env_key":     "REDDIT_CLIENT_ID",
        "phase":       2,
    },
    "philgeps": {
        "config_cls":  PhilGEPSConfig,
        "miner_cls":   PhilGEPSMiner,
        "env_key":     None,
        "phase":       3,
    },
    "facebook": {
        "config_cls":  FacebookConfig,
        "miner_cls":   FacebookMiner,
        "env_key":     "FACEBOOK_ACCESS_TOKEN",
        "phase":       3,
    },
    "dti_bnrs": {
        "config_cls":  DTIBNRSConfig,
        "miner_cls":   DTIBNRSMiner,
        "env_key":     None,      # 无需 API Key，公开爬虫
        "phase":       3,
    },
    "yellow_pages": {
        "config_cls":  YellowPagesConfig,
        "miner_cls":   YellowPagesMiner,
        "env_key":     None,        # 无需 API Key，基于 Playwright
        "phase":       2,
    },
}


def build_miners_from_config(config_path: Optional[str] = None) -> List:
    """
    从 config/miners.yaml（可选）+ 环境变量构建 Miner 实例列表。
    YAML 提供细粒度参数，环境变量提供 API Key 和启用开关。
    """
    # 加载 YAML（可选）
    yaml_data: Dict[str, Any] = {}
    if config_path and yaml:
        p = Path(config_path)
        if p.exists():
            with p.open() as f:
                yaml_data = yaml.safe_load(f) or {}

    miners = []
    for name, meta in FACTORY.items():
        section: Dict[str, Any] = yaml_data.get("miners", {}).get(name, {})

        # 从 YAML 或环境变量读取 enabled
        enabled_from_yaml = section.get("enabled", None)
        enabled_env = os.environ.get(f"{name.upper()}_ENABLED", "").lower()
        if enabled_env in ("0", "false"):
            enabled = False
        elif enabled_env in ("1", "true"):
            enabled = True
        elif enabled_from_yaml is not None:
            enabled = bool(enabled_from_yaml)
        else:
            # Phase 1 默认启用，Phase 2-3 默认禁用
            enabled = meta["phase"] == 1

        # API Key
        api_key = ""
        if meta["env_key"]:
            api_key = _env(meta["env_key"])
            # 如果需要 API Key 但没有提供，禁用并跳过
            if not api_key:
                enabled = False

        # 构建 Config 实例
        config_kwargs: Dict[str, Any] = {
            "name":    name,
            "enabled": enabled,
            **section,                   # YAML 中所有字段会 override
        }
        if api_key:
            config_kwargs["api_key"] = api_key

        # 特殊字段处理
        if name == "google_cse":
            config_kwargs.setdefault("cx", _env("GOOGLE_CSE_CX"))
        if name == "reddit":
            config_kwargs.setdefault("client_id",     _env("REDDIT_CLIENT_ID"))
            config_kwargs.setdefault("client_secret", _env("REDDIT_CLIENT_SECRET"))

        # 过滤 Config 不支持的字段
        config_cls  = meta["config_cls"]
        valid_keys  = {f.name for f in config_cls.__dataclass_fields__.values()} \
                      if hasattr(config_cls, "__dataclass_fields__") else set()
        if valid_keys:
            config_kwargs = {k: v for k, v in config_kwargs.items() if k in valid_keys}

        cfg    = config_cls(**config_kwargs)
        miner  = meta["miner_cls"](cfg)
        miners.append(miner)

    return miners


def build_orchestrator(config_path: Optional[str] = None) -> MiningOrchestrator:
    """工厂函数：构建并注册所有 Miner 的 Orchestrator"""
    orchestrator = MiningOrchestrator(max_concurrent_miners=4)
    miners = build_miners_from_config(config_path)

    for miner in miners:
        # Serper 作为 google_cse 的 Fallback
        if miner.source_name.value == "google_cse":
            orchestrator.register(miner, fallback_for="serper")
        else:
            orchestrator.register(miner)

    return orchestrator


def build_postgres_writer() -> PostgresWriter:
    dsn = _env(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/leads",
    )
    return PostgresWriter(dsn=dsn)


def build_chroma_writer() -> ChromaWriter:
    return ChromaWriter(
        host=_env("CHROMA_HOST", "localhost"),
        port=int(_env("CHROMA_PORT", "8001")),
        collection_name=_env("CHROMA_COLLECTION", "all_leads"),
    )


def build_gemini_enricher() -> GeminiEnricher:
    api_key = _env("GEMINI_API_KEY")
    if not api_key:
        raise ValueError("GEMINI_API_KEY environment variable is required")
    return GeminiEnricher(
        api_key=api_key,
        model=_env("GEMINI_MODEL", "gemini-2.5-flash"),
        max_concurrent=int(_env("GEMINI_MAX_CONCURRENT", "10")),
    )
