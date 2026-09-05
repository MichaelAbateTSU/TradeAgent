from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from tradeagent.news import (
    MarketNewsItem,
    NewsBlackoutPolicy,
    NewsCategory,
    NewsRepository,
    SourceReliability,
)
from tradeagent.persistence import Database

NOW = datetime(2026, 9, 4, 15, tzinfo=UTC)


def _item(category: NewsCategory = NewsCategory.GENERAL) -> MarketNewsItem:
    return MarketNewsItem(
        source="SEC",
        source_url="https://www.sec.gov/example",
        category=category,
        symbols=("spy",),
        headline="Example market event",
        published_at=NOW,
        received_at=NOW,
        reliability=SourceReliability.OFFICIAL,
    )


def test_news_repository_deduplicates_point_in_time_content(tmp_path: Path) -> None:
    with Database(f"sqlite:///{tmp_path / 'news.db'}") as database:
        database.initialize()
        repository = NewsRepository(database)

        first_id, first_created = repository.store(_item())
        second_id, second_created = repository.store(_item())

        assert first_created
        assert not second_created
        assert first_id == second_id
        assert _item().symbols == ("SPY",)
        assert len(_item().content_hash) == 64


def test_news_repository_rejects_future_publication(tmp_path: Path) -> None:
    with Database(f"sqlite:///{tmp_path / 'news.db'}") as database:
        database.initialize()
        repository = NewsRepository(database)
        future = _item().model_copy(update={"published_at": NOW + timedelta(minutes=1)})

        with pytest.raises(ValueError, match="after first receipt"):
            repository.store(future)


def test_news_blackout_is_fail_closed_and_point_in_time() -> None:
    policy = NewsBlackoutPolicy()

    assert policy.permits_entry([], symbol="SPY", decision_at=NOW, latest_feed_at=None) == (
        False,
        "NEWS_FEED_STALE",
    )
    halt = _item(NewsCategory.HALT)
    assert policy.permits_entry(
        [halt],
        symbol="SPY",
        decision_at=NOW + timedelta(minutes=1),
        latest_feed_at=NOW,
    ) == (False, "UNRESOLVED_TRADING_HALT")
    future_macro = _item(NewsCategory.MACRO).model_copy(
        update={"received_at": NOW + timedelta(hours=1)}
    )
    assert policy.permits_entry(
        [future_macro],
        symbol="SPY",
        decision_at=NOW,
        latest_feed_at=NOW,
    ) == (True, "NEWS_CLEAR")
