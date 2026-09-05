from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from tradeagent.config import AppConfig, IntradayConfig
from tradeagent.domain import MarketBar
from tradeagent.intraday_strategy import (
    IntradayStrategyConfig,
    RegimeFilteredMomentumStrategy,
)
from tradeagent.live_shadow import (
    LiveShadowDecisionProcessor,
    SynchronizedFiveMinuteBuilder,
)
from tradeagent.news import NewsBlackoutPolicy, NewsContextService, NewsRepository
from tradeagent.persistence import Database, ProductionRepository


def _minute(symbol: str, timestamp: datetime, price: Decimal) -> MarketBar:
    return MarketBar(
        symbol=symbol,
        timestamp=timestamp,
        open=price,
        high=price + Decimal("0.1"),
        low=price - Decimal("0.1"),
        close=price,
        volume=Decimal("1000"),
    )


def test_synchronized_builder_emits_only_complete_universe_frame() -> None:
    builder = SynchronizedFiveMinuteBuilder(("SPY", "QQQ"))
    start = datetime(2026, 9, 4, 14, 0, tzinfo=UTC)
    frame = None
    for index in range(6):
        for symbol in ("SPY", "QQQ"):
            emitted = builder.add(
                _minute(
                    symbol,
                    start + timedelta(minutes=index),
                    Decimal(100 + index),
                )
            )
            frame = emitted or frame

    assert frame is not None
    assert frame.timestamp == start + timedelta(minutes=5)
    assert {bar.symbol for bar in frame.bars} == {"SPY", "QQQ"}


def test_live_shadow_processor_records_decisions_and_outcomes(tmp_path: Path) -> None:
    database = Database(f"sqlite:///{tmp_path / 'shadow.db'}")
    database.initialize()
    repository = ProductionRepository(database)
    config = AppConfig(intraday=IntradayConfig(enabled=True))
    processor = LiveShadowDecisionProcessor(
        config,
        repository,
        RegimeFilteredMomentumStrategy(
            IntradayStrategyConfig(),
            config.intraday,
        ),
        symbols=("SPY", "QQQ"),
    )
    start = datetime(2026, 9, 4, 14, 0, tzinfo=UTC)

    async def process() -> None:
        for index in range(11):
            for symbol in ("SPY", "QQQ"):
                await processor.on_bar(
                    _minute(
                        symbol,
                        start + timedelta(minutes=index),
                        Decimal(100 + index),
                    ),
                    can_enter=False,
                )

    asyncio.run(process())

    assert repository.market_data_counts() == (22, 0)
    assert repository.event_count() == 25
    database.dispose()


def test_stale_news_context_blocks_shadow_targets(tmp_path: Path) -> None:
    database = Database(f"sqlite:///{tmp_path / 'news-shadow.db'}")
    database.initialize()
    repository = ProductionRepository(database)
    config = AppConfig(intraday=IntradayConfig(enabled=True))
    processor = LiveShadowDecisionProcessor(
        config,
        repository,
        RegimeFilteredMomentumStrategy(IntradayStrategyConfig(), config.intraday),
        symbols=("SPY",),
        news_context=NewsContextService(
            NewsRepository(database),
            NewsBlackoutPolicy(),
            latest_feed_at=lambda: None,
        ),
    )
    start = datetime(2026, 9, 4, 14, 0, tzinfo=UTC)

    async def process() -> None:
        for index in range(6):
            await processor.on_bar(
                _minute("SPY", start + timedelta(minutes=index), Decimal("100")),
                can_enter=False,
            )

    asyncio.run(process())
    decision = repository.latest_event_payload("shadow_decision")

    assert decision is not None
    assert decision["news_context"]["SPY"]["reason"] == "NEWS_FEED_STALE"
