from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

from tradeagent.config import StrategyConfig
from tradeagent.domain import MarketBar
from tradeagent.strategy import SmaCrossoverStrategy, VolatilityTargetTrendStrategy


def test_sma_strategy_waits_for_complete_window(bar: MarketBar) -> None:
    strategy = SmaCrossoverStrategy(StrategyConfig(fast_window=2, slow_window=3))

    assert strategy.on_bar(bar) is None
    second_bar = bar.model_copy(update={"timestamp": bar.timestamp + timedelta(days=1)})
    assert strategy.on_bar(second_bar) is None


def test_sma_strategy_emits_long_and_flat_targets(bar: MarketBar) -> None:
    strategy = SmaCrossoverStrategy(
        StrategyConfig(fast_window=2, slow_window=3, target_weight=Decimal("0.02"))
    )
    prices = [Decimal("100"), Decimal("101"), Decimal("104"), Decimal("95")]
    intents = []
    for offset, price in enumerate(prices):
        current = bar.model_copy(
            update={"timestamp": bar.timestamp + timedelta(days=offset), "close": price}
        )
        intents.append(strategy.on_bar(current))

    assert intents[2] is not None
    assert intents[2].target_weight == Decimal("0.02")
    assert intents[3] is not None
    assert intents[3].target_weight == Decimal(0)


def test_volatility_target_trend_scales_and_flattens(bar: MarketBar) -> None:
    config = StrategyConfig(
        fast_window=2,
        slow_window=3,
        volatility_window=2,
        target_annual_volatility=Decimal("0.01"),
        target_weight=Decimal("0.02"),
    )
    strategy = VolatilityTargetTrendStrategy(config)
    prices = [Decimal("100"), Decimal("101"), Decimal("110"), Decimal("90")]
    intents = []
    for offset, price in enumerate(prices):
        current = bar.model_copy(
            update={"timestamp": bar.timestamp + timedelta(days=offset), "close": price}
        )
        intents.append(strategy.on_bar(current))

    assert intents[2] is not None
    assert Decimal(0) < intents[2].target_weight < config.target_weight
    assert intents[3] is not None
    assert intents[3].target_weight == Decimal(0)
