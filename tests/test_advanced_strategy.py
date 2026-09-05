from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from tradeagent.advanced_strategy import (
    DonchianAtrBreakoutStrategy,
    NoiseAreaMomentumStrategy,
    VolatilitySqueezeBreakoutStrategy,
)
from tradeagent.config import IntradayConfig
from tradeagent.domain import MarketBar
from tradeagent.universe import UniverseFrame


def _frame(index: int, price: Decimal, volume: Decimal = Decimal("1000")) -> UniverseFrame:
    timestamp = datetime(2026, 9, 4, 14, 0, tzinfo=UTC) + timedelta(minutes=index * 5)
    bar = MarketBar(
        symbol="SPY",
        timestamp=timestamp,
        open=price,
        high=price + 1,
        low=price - 1,
        close=price,
        volume=volume,
    )
    return UniverseFrame(timestamp=timestamp, bars=(bar,))


def test_advanced_strategies_are_deterministic_and_bounded() -> None:
    strategies = [
        NoiseAreaMomentumStrategy(IntradayConfig(enabled=True)),
        DonchianAtrBreakoutStrategy(IntradayConfig(enabled=True)),
        VolatilitySqueezeBreakoutStrategy(IntradayConfig(enabled=True)),
    ]
    for strategy in strategies:
        intents = [strategy.on_frame(_frame(i, Decimal(100 + i) / 1)) for i in range(25)]
        assert all(sum(intent.target_weights.values()) <= Decimal("0.0025") for intent in intents)
        assert all(weight >= 0 for intent in intents for weight in intent.target_weights.values())
