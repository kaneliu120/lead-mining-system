"""
RedditMiner — Phase 2 社交媒体数据插件
PRAW SDK，免费 60 req/min，从菲律宾商业 subreddit 挖掘线索
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import AsyncIterator, List, Optional

from app.miners.base import BaseMiner, MinerConfig, MinerHealth
from app.models.lead import LeadRaw, LeadSource

# 菲律宾商业相关关键词（用于帖子过滤）
_BIZ_KEYWORDS = [
    "business", "store", "shop", "restaurant", "service", "company",
    "startup", "negosyo", "tindahan", "sari-sari", "franchise",
    "online selling", "small business",
]


@dataclass
class RedditConfig(MinerConfig):
    client_id: str = ""
    client_secret: str = ""
    user_agent: str = "ph-lead-miner/2.0 (by /u/leadbot)"
    target_subreddits: List[str] = field(
        default_factory=lambda: [
            "Philippines", "phinvest", "Entrepreneur", "smallbusiness",
        ]
    )
    rate_limit_per_minute: int = 55         # 官方 60/min，留安全余量


class RedditMiner(BaseMiner):
    """
    Reddit API 插件（PRAW）。
    从菲律宾商业相关 subreddit 中提取 SME 线索。

    ⚠️ 注意：Reddit ToS 禁止大规模商业采集。
    建议仅用于市场调研和趋势分析，而非直接联系人获取。
    """

    def __init__(self, config: RedditConfig):
        super().__init__(config)
        self._cfg = config
        self._reddit = None

    @property
    def source_name(self) -> LeadSource:
        return LeadSource.REDDIT

    async def on_startup(self) -> None:
        import praw
        self._reddit = praw.Reddit(
            client_id=self._cfg.client_id,
            client_secret=self._cfg.client_secret,
            user_agent=self._cfg.user_agent,
        )
        self.logger.info("Reddit (PRAW) client initialized")

    async def on_shutdown(self) -> None:
        self._reddit = None

    async def mine(
        self,
        keyword: str,
        location: str = "",
        lat: Optional[float] = None,
        lng: Optional[float] = None,
        limit: int = 100,
    ) -> AsyncIterator[LeadRaw]:
        if self._reddit is None:
            return

        per_sub = max(limit // max(len(self._cfg.target_subreddits), 1), 10)
        collected = 0

        for sub_name in self._cfg.target_subreddits:
            if collected >= limit:
                break

            # PRAW 是同步 SDK，在线程池中运行
            try:
                submissions = await asyncio.to_thread(
                    lambda sn=sub_name: list(
                        self._reddit.subreddit(sn).search(
                            f"{keyword} Philippines",
                            limit=per_sub,
                            sort="relevance",
                            time_filter="year",
                        )
                    )
                )
            except Exception as exc:
                self.logger.warning(f"Reddit subreddit {sub_name} search failed: {exc}")
                continue

            for submission in submissions:
                if collected >= limit:
                    break

                business_name = self._extract_business_name(submission, keyword)
                if not business_name:
                    continue

                yield LeadRaw(
                    source=LeadSource.REDDIT,
                    business_name=business_name,
                    industry_keyword=keyword,
                    website=self._extract_url(submission),
                    metadata={
                        "subreddit":          sub_name,
                        "post_title":         submission.title[:200],
                        "post_url":           f"https://reddit.com{submission.permalink}",
                        "post_score":         submission.score,
                        "comment_count":      submission.num_comments,
                        "author":             str(submission.author or ""),
                        "created_utc":        submission.created_utc,
                        "post_text_preview":  (submission.selftext or "")[:300],
                    },
                )
                collected += 1

    @staticmethod
    def _extract_business_name(submission, keyword: str) -> str:
        """从帖子中提取商业名称（启发式规则，可后续换成 Gemini NER）"""
        title_lower = submission.title.lower()
        if any(kw in title_lower for kw in _BIZ_KEYWORDS):
            return submission.title[:150]
        if keyword.lower() in title_lower:
            return submission.title[:150]
        return ""

    @staticmethod
    def _extract_url(submission) -> str:
        """从帖子中提取可能的商业网站 URL"""
        if submission.url and not submission.is_self:
            if "reddit.com" not in submission.url:
                return submission.url
        return ""

    async def validate_config(self) -> bool:
        return bool(self._cfg.client_id and self._cfg.client_secret)

    async def health_check(self) -> MinerHealth:
        if self._reddit is None:
            return MinerHealth(healthy=False, message="Reddit client not initialized")
        try:
            start = time.monotonic()
            await asyncio.to_thread(
                lambda: list(self._reddit.subreddit("Philippines").hot(limit=1))
            )
            latency = (time.monotonic() - start) * 1000
            return MinerHealth(healthy=True, message="OK", latency_ms=latency)
        except Exception as exc:
            return MinerHealth(healthy=False, message=str(exc))
