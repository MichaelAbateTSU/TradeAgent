from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from tradeagent.config import IntradayConfig
from tradeagent.domain import MarketBar
from tradeagent.intraday_strategy import (
    IntradayEqualWeightBenchmark,
    IntradayStrategyConfig,
    OpeningRangeBreakoutStrategy,
    SessionVwapMeanReversionStrategy,
)
from tradeagent.universe import UniverseFrame


def _frame(timestamp: datetime, prices: dict[str, Decimal]) -> UniverseFrame:
    return UniverseFrame(
        timestamp=timestamp,
        bars=tuple(
            MarketBar(
                symbol=symbol,
                timestamp=timestamp,
                open=price,
                high=price + Decimal("0.1"),
                low=price - Decimal("0.1"),
                close=price,
                volume=Decimal("1000"),
            )
            for symbol, price in prices.items()
        ),
    )


def test_opening_range_enters_breakout_and_flattens() -> None:
    intraday = IntradayConfig(enabled=True)
    strategy = OpeningRangeBreakoutStrategy(
        IntradayStrategyConfig(opening_range_minutes=30, breakout_buffer_bps=0),
        intraday,
    )
    start = datetime(2026, 9, 4, 13, 30, tzinfo=UTC)
    for index in range(6):
        strategy.on_frame(
            _frame(
                start + timedelta(minutes=index * 5),
                {"SPY": Decimal("100")},
            )
        )

    breakout = strategy.on_frame(_frame(start + timedelta(minutes=30), {"SPY": Decimal("101")}))
    failed = strategy.on_frame(_frame(start + timedelta(minutes=35), {"SPY": Decimal("99")}))
    flattened = strategy.on_frame(
        _frame(
            datetime(2026, 9, 4, 19, 50, tzinfo=UTC),
            {"SPY": Decimal("102")},
        )
    )

    assert breakout.target_weights["SPY"] > 0
    assert failed.target_weights["SPY"] == 0
    assert flattened.target_weights["SPY"] == 0


def test_vwap_reversion_enters_deviation_and_exits_recovery() -> None:
    intraday = IntradayConfig(enabled=True)
    strategy = SessionVwapMeanReversionStrategy(
        IntradayStrategyConfig(
            vwap_minimum_observations=5,
            vwap_entry_z=Decimal("-0.5"),
            vwap_exit_z=Decimal("-0.1"),
        ),
        intraday,
    )
    start = datetime(2026, 9, 4, 14, 0, tzinfo=UTC)
    prices = [100, 100, 100, 100, 95, 100]
    intents = [
        strategy.on_frame(
            _frame(
                start + timedelta(minutes=index * 5),
                {"SPY": Decimal(price)},
            )
        )
        for index, price in enumerate(prices)
    ]

    assert intents[4].target_weights["SPY"] > 0
    assert intents[5].target_weights["SPY"] == 0


def test_intraday_benchmark_holds_only_during_session() -> None:
    benchmark = IntradayEqualWeightBenchmark(
        Decimal("0.01"),
        IntradayConfig(enabled=True),
    )
    pre_entry = benchmark.on_frame(
        _frame(
            datetime(2026, 9, 4, 13, 30, tzinfo=UTC),
            {"SPY": Decimal("100")},
        )
    )
    entry = benchmark.on_frame(
        _frame(
            datetime(2026, 9, 4, 13, 35, tzinfo=UTC),
            {"SPY": Decimal("100")},
        )
    )
    flatten = benchmark.on_frame(
        _frame(
            datetime(2026, 9, 4, 19, 50, tzinfo=UTC),
            {"SPY": Decimal("100")},
        )
    )

    assert pre_entry.target_weights["SPY"] == 0
    assert entry.target_weights["SPY"] == Decimal("0.01")
    assert flatten.target_weights["SPY"] == 0
