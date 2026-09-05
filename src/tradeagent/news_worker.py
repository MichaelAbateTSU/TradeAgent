from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterable, Sequence
from datetime import UTC, datetime, timedelta
from typing import Protocol

import httpx
from pydantic import BaseModel, ConfigDict, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from tradeagent.news import MarketNewsItem, NewsRepository
from tradeagent.persistence import ProductionRepository


class NewsWorkerSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="NEWS_",
        env_file=".env",
        env_file_encoding="utf-8",
        frozen=True,
        extra="ignore",
    )

    contact_email: str
    symbols: str = "SPY,QQQ,IWM,TLT,GLD"
    poll_seconds: int = Field(default=60, gt=0)
    stale_seconds: int = Field(default=300, gt=0)
    overlap_seconds: int = Field(default=120, ge=0)

    @property
    def symbol_list(self) -> tuple[str, ...]:
        return tuple(symbol.strip().upper() for symbol in self.symbols.split(",") if symbol.strip())


class NewsSource(Protocol):
    def articles(
        self,
        *,
        symbols: Sequence[str],
        start: datetime,
        end: datetime,
    ) -> Iterable[MarketNewsItem]: ...


class NewsPollResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    received: int
    inserted: int
    duplicates: int
    latest_article_at: datetime | None
    polled_at: datetime


class NewsWorker:
    def __init__(
        self,
        settings: NewsWorkerSettings,
        source: NewsSource,
        news_repository: NewsRepository,
        production_repository: ProductionRepository,
        *,
        instance_id: str,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._settings = settings
        self._source = source
        self._news = news_repository
        self._production = production_repository
        self._instance_id = instance_id
        self._clock = clock

    def poll_once(self) -> NewsPollResult:
        now = self._clock()
        watermark_value = self._production.get_control("alpaca_news_watermark")
        watermark = (
            datetime.fromisoformat(watermark_value)
            if watermark_value
            else now - timedelta(minutes=15)
        )
        start = watermark - timedelta(seconds=self._settings.overlap_seconds)
        articles = list(
            self._source.articles(
                symbols=self._settings.symbol_list,
                start=start,
                end=now,
            )
        )
        inserted = 0
        for article in articles:
            _, created = self._news.store(article)
            inserted += int(created)
        latest = max(
            (article.updated_at or article.published_at for article in articles),
            default=None,
        )
        next_watermark = max(watermark, latest) if latest else now
        self._production.set_control(
            "alpaca_news_watermark",
            next_watermark.astimezone(UTC).isoformat(),
        )
        result = NewsPollResult(
            received=len(articles),
            inserted=inserted,
            duplicates=len(articles) - inserted,
            latest_article_at=latest,
            polled_at=now,
        )
        self._production.heartbeat(
            "tradeagent-news-worker",
            self._instance_id,
            {
                "state": "healthy",
                **result.model_dump(mode="json"),
            },
            observed_at=now,
        )
        self._production.refresh_worker_lock(
            "tradeagent-news-worker",
            self._instance_id,
            observed_at=now,
        )
        return result

    async def run(self, stop_event: asyncio.Event | None = None) -> None:
        stop = stop_event or asyncio.Event()
        if not self._production.acquire_worker_lock(
            "tradeagent-news-worker",
            self._instance_id,
            stale_after_seconds=self._settings.stale_seconds,
        ):
            raise RuntimeError("another news worker owns the polling lease")
        backoff = self._settings.poll_seconds
        try:
            while not stop.is_set():
                try:
                    await asyncio.to_thread(self.poll_once)
                    backoff = self._settings.poll_seconds
                except httpx.HTTPStatusError as error:
                    if error.response.status_code in {400, 401, 403}:
                        raise
                    self._production.heartbeat(
                        "tradeagent-news-worker",
                        self._instance_id,
                        {"state": "provider_error", "status": error.response.status_code},
                        observed_at=self._clock(),
                    )
                    backoff = min(backoff * 2, 60)
                try:
                    await asyncio.wait_for(stop.wait(), timeout=backoff)
                except TimeoutError:
                    continue
        finally:
            self._production.release_worker_lock("tradeagent-news-worker", self._instance_id)
