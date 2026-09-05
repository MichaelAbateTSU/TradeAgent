from __future__ import annotations

from datetime import UTC, datetime, timedelta
from enum import StrEnum
from hashlib import sha256
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import insert, select

from tradeagent.persistence import Database, market_news


class NewsCategory(StrEnum):
    MACRO = "macro"
    HALT = "halt"
    FILING = "filing"
    EARNINGS = "earnings"
    ISSUER = "issuer"
    GENERAL = "general"


class SourceReliability(StrEnum):
    OFFICIAL = "official"
    LICENSED = "licensed"
    SECONDARY = "secondary"


class MarketNewsItem(BaseModel):
    model_config = ConfigDict(frozen=True)

    source: str = Field(min_length=1)
    source_url: str = Field(min_length=1)
    category: NewsCategory
    symbols: tuple[str, ...] = ()
    headline: str = Field(min_length=1)
    published_at: datetime
    received_at: datetime
    updated_at: datetime | None = None
    reliability: SourceReliability
    revision_of: UUID | None = None

    @field_validator("symbols")
    @classmethod
    def normalize_symbols(cls, symbols: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(sorted({symbol.strip().upper() for symbol in symbols if symbol.strip()}))

    @property
    def content_hash(self) -> str:
        canonical = "|".join(
            [self.source, self.headline, self.published_at.astimezone(UTC).isoformat()]
        )
        return sha256(canonical.encode()).hexdigest()


class NewsRepository:
    def __init__(self, database: Database) -> None:
        self._database = database

    def store(self, item: MarketNewsItem) -> tuple[UUID, bool]:
        if item.published_at > item.received_at:
            raise ValueError("news publication cannot be after first receipt")
        with self._database.begin() as connection:
            existing = connection.scalar(
                select(market_news.c.news_id).where(
                    market_news.c.source == item.source,
                    market_news.c.content_hash == item.content_hash,
                )
            )
            if existing is not None:
                return UUID(str(existing)), False
            news_id = uuid4()
            connection.execute(
                insert(market_news).values(
                    news_id=str(news_id),
                    source=item.source,
                    source_url=item.source_url,
                    category=item.category.value,
                    symbols=list(item.symbols),
                    headline=item.headline,
                    content_hash=item.content_hash,
                    published_at=item.published_at,
                    received_at=item.received_at,
                    updated_at=item.updated_at,
                    revision_of=str(item.revision_of) if item.revision_of else None,
                    reliability=item.reliability.value,
                )
            )
        return news_id, True


class NewsBlackoutPolicy:
    def __init__(self, *, maximum_feed_age: timedelta = timedelta(minutes=15)) -> None:
        self._maximum_feed_age = maximum_feed_age

    def permits_entry(
        self,
        items: list[MarketNewsItem],
        *,
        symbol: str,
        decision_at: datetime,
        latest_feed_at: datetime | None,
    ) -> tuple[bool, str]:
        if latest_feed_at is None or decision_at - latest_feed_at > self._maximum_feed_age:
            return False, "NEWS_FEED_STALE"
        normalized = symbol.upper()
        for item in items:
            if item.received_at > decision_at:
                continue
            age = decision_at - item.received_at
            relevant = not item.symbols or normalized in item.symbols
            if relevant and item.category is NewsCategory.HALT and age <= timedelta(days=1):
                return False, "UNRESOLVED_TRADING_HALT"
            if (
                relevant
                and item.category
                in {
                    NewsCategory.MACRO,
                    NewsCategory.FILING,
                    NewsCategory.EARNINGS,
                }
                and age <= timedelta(minutes=30)
            ):
                return False, "HIGH_IMPACT_NEWS_WINDOW"
        return True, "NEWS_CLEAR"
