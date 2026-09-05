from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from tradeagent.alpaca_stream import MarketQuote
from tradeagent.config import IntradayConfig
from tradeagent.domain import MarketBar
from tradeagent.features import IntradayFeatureEngine, MarketRegime


def _bar(timestamp: datetime, price: Decimal, volume: Decimal = Decimal("1000")) -> MarketBar:
    return MarketBar(
        symbol="SPY",
        timestamp=timestamp,
        open=price,
        high=price + Decimal("0.1"),
        low=price - Decimal("0.1"),
        close=price,
        volume=volume,
    )


def test_features_use_only_current_and_prior_events() -> None:
    engine = IntradayFeatureEngine(IntradayConfig(enabled=True))
    start = datetime(2026, 9, 4, 13, 35, tzinfo=UTC)
    engine.on_quote(
        MarketQuote(
            symbol="SPY",
            timestamp=start,
            bid_price=Decimal("99.9"),
            ask_price=Decimal("100.1"),
            bid_size=Decimal("10"),
            ask_size=Decimal("10"),
        )
    )
    vectors = [
        engine.on_bar(
            _bar(
                start + timedelta(minutes=index * 5),
                Decimal(100 + index),
            )
        )
        for index in range(13)
    ]

    assert vectors[0].spread_bps == Decimal("20")
    assert vectors[0].regime is MarketRegime.WARMUP
    assert vectors[-1].momentum_15m is not None
    assert vectors[-1].momentum_30m is not None
    assert vectors[-1].momentum_60m == Decimal("0.12")
    assert vectors[-1].realized_volatility is not None
    assert vectors[-1].regime in {
        MarketRegime.TRENDING,
        MarketRegime.HIGH_VOLATILITY,
    }


def test_relative_volume_uses_prior_sessions_only() -> None:
    engine = IntradayFeatureEngine(IntradayConfig(enabled=True))
    first = datetime(2026, 9, 3, 14, 0, tzinfo=UTC)
    second = datetime(2026, 9, 4, 14, 0, tzinfo=UTC)

    first_vector = engine.on_bar(_bar(first, Decimal("100"), Decimal("1000")))
    second_vector = engine.on_bar(_bar(second, Decimal("100"), Decimal("2000")))

    assert first_vector.relative_volume is None
    assert second_vector.relative_volume == Decimal("2")


def test_future_and_out_of_order_quotes_are_not_used() -> None:
    engine = IntradayFeatureEngine(IntradayConfig(enabled=True))
    timestamp = datetime(2026, 9, 4, 14, 0, tzinfo=UTC)
    future = MarketQuote(
        symbol="SPY",
        timestamp=timestamp + timedelta(seconds=1),
        bid_price=Decimal("99.9"),
        ask_price=Decimal("100.1"),
        bid_size=Decimal("10"),
        ask_size=Decimal("10"),
    )
    engine.on_quote(future)

    vector = engine.on_bar(_bar(timestamp, Decimal("100")))

    assert vector.spread_bps is None
    with pytest.raises(ValueError, match="chronological"):
        engine.on_quote(future.model_copy(update={"timestamp": timestamp}))
