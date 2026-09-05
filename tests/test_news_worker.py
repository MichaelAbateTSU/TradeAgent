from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from tradeagent.news import MarketNewsItem, NewsCategory, NewsRepository, SourceReliability
from tradeagent.news_worker import NewsWorker, NewsWorkerSettings
from tradeagent.persistence import Database, ProductionRepository

NOW = datetime(2026, 9, 5, 15, tzinfo=UTC)


class FakeSource:
    def articles(self, *, symbols, start, end):
        return [
            MarketNewsItem(
                source="Alpaca:test",
                source_url="https://example.test/news",
                category=NewsCategory.GENERAL,
                symbols=("SPY",),
                headline="Test headline",
                published_at=NOW,
                received_at=NOW,
                reliability=SourceReliability.LICENSED,
            )
        ]


def test_news_worker_watermark_deduplication_and_heartbeat(tmp_path: Path) -> None:
    with Database(f"sqlite:///{tmp_path / 'news-worker.db'}") as database:
        database.initialize()
        production = ProductionRepository(database)
        worker = NewsWorker(
            NewsWorkerSettings(contact_email="owner@example.com"),
            FakeSource(),
            NewsRepository(database),
            production,
            instance_id="news-1",
            clock=lambda: NOW,
        )

        first = worker.poll_once()
        second = worker.poll_once()

        assert first.inserted == 1
        assert second.inserted == 0
        assert second.duplicates == 1
        assert production.get_control("alpaca_news_watermark") is not None
        heartbeat = production.latest_heartbeat("tradeagent-news-worker")
        assert heartbeat is not None
        assert heartbeat[2]["state"] == "healthy"
