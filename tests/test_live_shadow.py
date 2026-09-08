from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from sqlalchemy import select

from tradeagent.alpaca_stream import MarketQuote, ReceivedStreamEvent
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
from tradeagent.persistence import Database, ProductionRepository, market_bars
from tradeagent.portfolio import PortfolioIntent
from tradeagent.shadow_recorder import ShadowRecorderSettings, persist_shadow_batch
from tradeagent.worker import AutonomousPaperWorker, RecordedShadowBatchProcessor, WorkerMode


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

    assert repository.market_data_counts() == (22, 0, 0)
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


def test_shadow_signal_records_multi_frame_decay(tmp_path: Path) -> None:
    class OneShotStrategy:
        strategy_id = "one-shot"

        def __init__(self) -> None:
            self.frames = 0

        def on_quote(self, quote) -> None:
            return None

        def on_frame(self, frame):
            self.frames += 1
            return PortfolioIntent(
                strategy_id=self.strategy_id,
                timestamp=frame.timestamp,
                target_weights={"SPY": Decimal("0.0025") if self.frames == 1 else Decimal(0)},
                rationale="test signal",
            )

    database = Database(f"sqlite:///{tmp_path / 'decay.db'}")
    database.initialize()
    repository = ProductionRepository(database)
    config = AppConfig(intraday=IntradayConfig(enabled=True))
    processor = LiveShadowDecisionProcessor(
        config,
        repository,
        OneShotStrategy(),
        symbols=("SPY",),
    )
    start = datetime(2026, 9, 4, 14, 0, tzinfo=UTC)

    async def process() -> None:
        for index in range(36):
            await processor.on_bar(
                _minute("SPY", start + timedelta(minutes=index), Decimal(100 + index)),
                can_enter=False,
            )

    asyncio.run(process())
    decay = repository.latest_event_payload("shadow_signal_decay")

    assert decay is not None
    assert decay["horizon_frames"] == 6
    assert Decimal(decay["gross_return_bps"]) > 0


def test_midbucket_start_is_unusable_then_recovers_without_invented_minutes() -> None:
    issues = []
    builder = SynchronizedFiveMinuteBuilder(("SPY", "QQQ"), on_unusable=issues.append)
    start = datetime(2026, 9, 8, 14, 0, tzinfo=UTC)
    frames = []
    for index in range(2, 10):
        for symbol in ("SPY", "QQQ"):
            frame = builder.add(
                _minute(symbol, start + timedelta(minutes=index), Decimal(index + 100))
            )
            if frame:
                frames.append(frame)
    assert len(issues) == 1
    assert issues[0]["reason"] == "incomplete_five_minute_bucket"
    assert issues[0]["usable"] is False
    assert issues[0]["missing_minutes"]["SPY"] == [
        start.isoformat(),
        (start + timedelta(minutes=1)).isoformat(),
    ]
    assert len(frames) == 1
    assert frames[0].timestamp == start + timedelta(minutes=10)
    assert frames[0].bar_for("SPY").open == Decimal(105)
    assert frames[0].bar_for("SPY").close == Decimal(109)
    assert frames[0].bar_for("SPY").volume == Decimal(5000)


def test_missing_symbol_keeps_derived_buffer_bounded_and_marks_every_closed_bucket() -> None:
    issues = []
    builder = SynchronizedFiveMinuteBuilder(("SPY", "QQQ"), on_unusable=issues.append)
    start = datetime(2026, 9, 8, 14, 0, tzinfo=UTC)
    for index in range(61):
        assert builder.add(_minute("SPY", start + timedelta(minutes=index), Decimal(100))) is None
        assert builder.pending_minutes <= 5
    assert len(issues) == 12
    assert all(len(issue["missing_minutes"]["QQQ"]) == 5 for issue in issues)


def test_shutdown_marks_pending_partial_frame_unusable_without_a_successor_bar() -> None:
    issues = []
    builder = SynchronizedFiveMinuteBuilder(("SPY",), on_unusable=issues.append)
    start = datetime(2026, 9, 8, 14, 0, tzinfo=UTC)
    assert builder.add(_minute("SPY", start, Decimal(100))) is None
    assert builder.add(_minute("SPY", start + timedelta(minutes=1), Decimal(101))) is None
    builder.finish(observed_at=start + timedelta(minutes=3))
    assert builder.pending_minutes == 0
    assert len(issues) == 1
    assert issues[0]["reason"] == "incomplete_bucket_at_stop"
    assert issues[0]["stopped_at"] == (start + timedelta(minutes=3)).isoformat()
    assert len(issues[0]["missing_minutes"]["SPY"]) == 3


def test_duplicate_conflicting_and_late_minutes_do_not_crash_or_reopen_a_frame() -> None:
    issues = []
    builder = SynchronizedFiveMinuteBuilder(("SPY",), on_unusable=issues.append)
    start = datetime(2026, 9, 8, 14, 0, tzinfo=UTC)
    first = _minute("SPY", start, Decimal(100))
    assert builder.add(first) is None
    assert builder.add(first) is None
    assert builder.duplicate_minutes == 1
    assert builder.add(_minute("SPY", start, Decimal(101))) is None
    assert issues[-1]["reason"] == "conflicting_minute_bar"
    assert issues[-1]["incoming_bar"]["close"] == "101"
    for index in range(1, 5):
        assert builder.add(_minute("SPY", start + timedelta(minutes=index), Decimal(100))) is None
    frames = []
    for index in range(5, 10):
        frame = builder.add(_minute("SPY", start + timedelta(minutes=index), Decimal(100)))
        if frame:
            frames.append(frame)
    assert len(frames) == 1
    assert builder.add(first) is None
    assert issues[-1]["reason"] == "late_bar_after_bucket_closed"
    assert builder.add(_minute("SPY", start + timedelta(minutes=9), Decimal(100))) is None


def test_gaps_reset_derived_outcomes_decay_and_feature_history(tmp_path: Path) -> None:
    class AlwaysInvested:
        strategy_id = "test"

        def __init__(self):
            self.frame_timestamps = []

        def on_quote(self, quote):
            return None

        def on_frame(self, frame):
            self.frame_timestamps.append(frame.timestamp)
            return PortfolioIntent(
                strategy_id="test",
                timestamp=frame.timestamp,
                target_weights={"SPY": Decimal("0.0025")},
                rationale="test",
            )

    database = Database(f"sqlite:///{tmp_path / 'derived-gap.db'}")
    database.initialize()
    repository = ProductionRepository(database)
    processor = LiveShadowDecisionProcessor(
        AppConfig(),
        repository,
        AlwaysInvested(),
        symbols=("SPY",),
    )
    start = datetime(2026, 9, 8, 14, 0, tzinfo=UTC)

    async def run():
        for index in range(15):
            if index != 7:
                await processor.on_bar(
                    _minute("SPY", start + timedelta(minutes=index), Decimal(100 + index)),
                    can_enter=False,
                )

    asyncio.run(run())
    assert repository.market_data_counts() == (14, 0, 0)
    assert repository.latest_event_payload("shadow_outcome") is None
    assert repository.latest_event_payload("shadow_signal_decay") is None
    issue = repository.latest_event_payload("shadow_frame_unusable")
    decision = repository.latest_event_payload("shadow_decision")
    assert issue["missing_minutes"]["SPY"] == [(start + timedelta(minutes=7)).isoformat()]
    assert decision["signal_at"] == (start + timedelta(minutes=15)).isoformat()
    assert decision["evidence_epoch"] == issue["next_evidence_epoch"]
    assert decision["frame_usable"] is True
    assert decision["cumulative_evidence_complete"] is False
    assert processor._strategy.frame_timestamps == [start + timedelta(minutes=15)]
    assert processor.derived_health(observed_at=start + timedelta(minutes=15))["state"] == "ready"
    assert processor.derived_health(observed_at=start + timedelta(minutes=30))["healthy"] is False
    database.dispose()


def test_out_of_order_quote_is_recorded_but_not_passed_to_chronological_features(tmp_path: Path):
    database = Database(f"sqlite:///{tmp_path / 'late-quote.db'}")
    database.initialize()
    repository = ProductionRepository(database)
    config = AppConfig()
    processor = LiveShadowDecisionProcessor(
        config,
        repository,
        RegimeFilteredMomentumStrategy(IntradayStrategyConfig(), config.intraday),
        symbols=("SPY",),
    )
    now = datetime(2026, 9, 8, 14, 0, tzinfo=UTC)
    fresh = MarketQuote(
        symbol="SPY",
        timestamp=now,
        bid_price=Decimal(100),
        ask_price=Decimal(101),
        bid_size=Decimal(10),
        ask_size=Decimal(20),
    )
    late = fresh.model_copy(update={"timestamp": now - timedelta(seconds=1)})

    async def run():
        await processor.on_quote(fresh, can_enter=False)
        await processor.on_quote(late, can_enter=False)

    asyncio.run(run())
    assert repository.market_data_counts() == (0, 2, 0)
    assert processor.derived_health(observed_at=now)["ignored_out_of_order_quotes"] == 1
    assert repository.latest_event_payload("shadow_quote_unusable")["raw_data_preserved"] is True
    database.dispose()


def test_recorded_batch_contract_preserves_raw_timestamps_and_does_not_duplicate_audits(
    tmp_path: Path,
):
    database = Database(f"sqlite:///{tmp_path / 'recorded-batch.db'}")
    database.initialize()
    repository = ProductionRepository(database)
    config = AppConfig()
    processor = LiveShadowDecisionProcessor(
        config,
        repository,
        RegimeFilteredMomentumStrategy(IntradayStrategyConfig(), config.intraday),
        symbols=("SPY",),
    )
    assert isinstance(processor, RecordedShadowBatchProcessor)
    start = datetime(2026, 9, 8, 14, 0, tzinfo=UTC)
    receipts = [
        ReceivedStreamEvent(
            _minute("SPY", start + timedelta(minutes=index), Decimal(100)),
            start + timedelta(minutes=index + 1, seconds=1),
        )
        for index in range(2, 10)
    ]
    persist_shadow_batch(
        repository,
        receipts,
        [],
        "offline-batch",
        clock=lambda: start + timedelta(minutes=12),
    )
    result = asyncio.run(processor.on_recorded_batch(receipts))
    assert result.bars == 8
    assert not result.errors
    assert repository.market_data_counts() == (8, 0, 0)
    assert repository.event_count() == 3  # One raw batch, one unusable bucket, one valid decision.
    with database.begin() as connection:
        rows = (
            connection.execute(select(market_bars).order_by(market_bars.c.event_at))
            .mappings()
            .all()
        )
    assert rows[0]["event_at"].replace(tzinfo=UTC) == start + timedelta(minutes=2)
    assert rows[0]["received_at"].replace(tzinfo=UTC) == start + timedelta(minutes=3, seconds=1)
    assert rows[0]["processed_at"].replace(tzinfo=UTC) == start + timedelta(minutes=12)
    database.dispose()


def test_out_of_order_quote_burst_uses_one_derived_audit_not_per_quote_commits(tmp_path: Path):
    database = Database(f"sqlite:///{tmp_path / 'out-of-order-batch.db'}")
    database.initialize()
    repository = ProductionRepository(database)
    config = AppConfig()
    processor = LiveShadowDecisionProcessor(
        config,
        repository,
        RegimeFilteredMomentumStrategy(IntradayStrategyConfig(), config.intraday),
        symbols=("SPY",),
    )
    now = datetime(2026, 9, 8, 14, 0, tzinfo=UTC)
    quote = MarketQuote(
        symbol="SPY",
        timestamp=now,
        bid_price=Decimal(100),
        ask_price=Decimal(101),
        bid_size=Decimal(10),
        ask_size=Decimal(20),
    )
    receipts = [
        ReceivedStreamEvent(
            quote.model_copy(update={"timestamp": now - timedelta(microseconds=index)}),
            now + timedelta(seconds=1),
        )
        for index in range(101)
    ]
    persist_shadow_batch(repository, receipts, [], "out-of-order-batch")
    result = asyncio.run(processor.on_recorded_batch(receipts))
    assert result.quotes == 101
    assert not result.errors
    assert repository.market_data_counts() == (0, 101, 0)
    assert repository.event_count() == 2
    rejected = repository.latest_event_payload("shadow_quote_unusable")
    assert rejected["count"] == 100
    assert len(rejected["quotes"]) == 100
    assert processor.derived_health(observed_at=now)["ignored_out_of_order_quotes"] == 100
    database.dispose()


def test_actual_shadow_worker_dispatches_live_processor_batches_and_survives_midbucket(
    tmp_path: Path,
):
    database = Database(f"sqlite:///{tmp_path / 'actual-worker.db'}")
    database.initialize()
    repository = ProductionRepository(database)
    repository.set_control("kill_switch", "operator-paused")
    config = AppConfig()
    start = datetime(2026, 9, 8, 14, 0, tzinfo=UTC)
    now = start
    processor = LiveShadowDecisionProcessor(
        config,
        repository,
        RegimeFilteredMomentumStrategy(IntradayStrategyConfig(), config.intraday),
        symbols=("SPY",),
    )

    class ForbiddenReconciler:
        def reconcile(self, **kwargs):
            raise AssertionError("shadow should never reconcile the trading role")

    worker = AutonomousPaperWorker(
        config,
        repository,
        ForbiddenReconciler(),
        processor,
        mode=WorkerMode.SHADOW,
        instance_id="derived-worker",
        strategy_authorized=lambda: False,
        clock=lambda: now,
        recorder_settings=ShadowRecorderSettings(
            batch_size=1,
            flush_interval_seconds=0.001,
            heartbeat_interval_seconds=0.01,
        ),
    )
    completed = 0
    original_batch = processor.on_recorded_batch

    async def counted_batch(receipts):
        nonlocal completed
        result = await original_batch(receipts)
        completed += len(receipts)
        return result

    processor.on_recorded_batch = counted_batch

    async def feed():
        nonlocal now
        for index in range(2, 10):
            now = start + timedelta(minutes=index + 1, seconds=1)
            yield ReceivedStreamEvent(
                _minute("SPY", start + timedelta(minutes=index), Decimal(100)), now
            )
            # Advance exchange time only after this real DB commit and derived processing finish.
            for _ in range(100):
                if completed == index - 1:
                    break
                await asyncio.sleep(0.005)
            assert completed == index - 1

    result = asyncio.run(worker.run(feed()))
    assert result.bars_processed == 8
    assert repository.market_data_counts() == (8, 0, 0)
    assert repository.latest_event_payload("shadow_frame_unusable")["usable"] is False
    heartbeat = repository.latest_heartbeat("tradeagent-shadow-recorder")[2]
    assert heartbeat["decision_errors"] == 0
    assert heartbeat["derived"]["state"] == "ready"
    assert heartbeat["committed"] == 8
    assert repository.get_control("kill_switch") == "operator-paused"
    database.dispose()
