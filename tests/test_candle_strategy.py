from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from tradeagent.candle_strategy import CandlePattern, CandleTrackingStrategy
from tradeagent.config import IntradayConfig
from tradeagent.domain import MarketBar
from tradeagent.universe import UniverseFrame


def _frame(index: int, open_price: str, high: str, low: str, close: str) -> UniverseFrame:
    timestamp = datetime(2026, 9, 4, 14, 0, tzinfo=UTC) + timedelta(minutes=5 * index)
    bar = MarketBar(
        symbol="SPY",
        timestamp=timestamp,
        open=Decimal(open_price),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
        volume=Decimal("1000"),
    )
    return UniverseFrame(timestamp=timestamp, bars=(bar,))


def test_bullish_engulfing_signal() -> None:
    strategy = CandleTrackingStrategy(CandlePattern.ENGULFING, IntradayConfig(enabled=True))
    strategy.on_frame(_frame(0, "101", "102", "99", "100"))
    signal = strategy.on_frame(_frame(1, "99.5", "102", "99", "101.5"))
    assert signal.target_weights["SPY"] > 0


def test_inside_bar_breakout_signal() -> None:
    strategy = CandleTrackingStrategy(CandlePattern.INSIDE_BREAKOUT, IntradayConfig(enabled=True))
    strategy.on_frame(_frame(0, "100", "102", "98", "101"))
    strategy.on_frame(_frame(1, "100", "101", "99", "100.5"))
    signal = strategy.on_frame(_frame(2, "101", "103", "100", "102.5"))
    assert signal.target_weights["SPY"] > 0


def test_heikin_ashi_requires_confirmation() -> None:
    strategy = CandleTrackingStrategy(CandlePattern.HEIKIN_ASHI, IntradayConfig(enabled=True))
    intents = [
        strategy.on_frame(
            _frame(index, str(100 + index), str(102 + index), str(99 + index), str(101 + index))
        )
        for index in range(3)
    ]
    assert intents[0].target_weights["SPY"] == 0
    assert intents[-1].target_weights["SPY"] > 0
