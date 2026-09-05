from __future__ import annotations

from collections.abc import Callable
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

    @field_validator("published_at", "received_at", "updated_at")
    @classmethod
    def normalize_timestamp(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

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

    def recent(
        self,
        *,
        since: datetime,
        until: datetime,
    ) -> list[MarketNewsItem]:
        with self._database.begin() as connection:
            rows = connection.execute(
                select(market_news)
                .where(
                    market_news.c.received_at >= since,
                    market_news.c.received_at <= until,
                )
                .order_by(market_news.c.received_at.desc())
            ).mappings()
            return [
                MarketNewsItem(
                    source=str(row["source"]),
                    source_url=str(row["source_url"]),
                    category=NewsCategory(str(row["category"])),
                    symbols=tuple(row["symbols"]),
                    headline=str(row["headline"]),
                    published_at=row["published_at"],
                    received_at=row["received_at"],
                    updated_at=row["updated_at"],
                    reliability=SourceReliability(str(row["reliability"])),
                    revision_of=UUID(str(row["revision_of"])) if row["revision_of"] else None,
                )
                for row in rows
            ]


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


class NewsContext(BaseModel):
    model_config = ConfigDict(frozen=True)

    permits_entry: bool
    reason: str
    article_count: int
    official_count: int
    high_impact_count: int
    latest_received_at: datetime | None
    citations: tuple[str, ...]


class NewsContextService:
    def __init__(
        self,
        repository: NewsRepository,
        policy: NewsBlackoutPolicy,
        *,
        latest_feed_at: Callable[[], datetime | None],
    ) -> None:
        self._repository = repository
        self._policy = policy
        self._latest_feed_at = latest_feed_at

    def context(self, symbol: str, decision_at: datetime) -> NewsContext:
        items = self._repository.recent(
            since=decision_at - timedelta(hours=24),
            until=decision_at,
        )
        relevant = [item for item in items if not item.symbols or symbol.upper() in item.symbols]
        permitted, reason = self._policy.permits_entry(
            relevant,
            symbol=symbol,
            decision_at=decision_at,
            latest_feed_at=self._latest_feed_at(),
        )
        return NewsContext(
            permits_entry=permitted,
            reason=reason,
            article_count=len(relevant),
            official_count=sum(item.reliability is SourceReliability.OFFICIAL for item in relevant),
            high_impact_count=sum(
                item.category
                in {
                    NewsCategory.MACRO,
                    NewsCategory.HALT,
                    NewsCategory.FILING,
                    NewsCategory.EARNINGS,
                }
                for item in relevant
            ),
            latest_received_at=max(
                (item.received_at for item in relevant),
                default=None,
            ),
            citations=tuple(item.source_url for item in relevant[:5]),
        )
