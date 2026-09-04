from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

import pytest

from tradeagent.config import StrategyConfig
from tradeagent.domain import MarketBar
from tradeagent.strategy import (
    DelayedStrategy,
    MeanReversionStrategy,
    SmaCrossoverStrategy,
    VolatilityTargetTrendStrategy,
)


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


def test_delayed_strategy_never_emits_current_bar_signal(bar: MarketBar) -> None:
    base = SmaCrossoverStrategy(StrategyConfig(fast_window=2, slow_window=3))
    strategy = DelayedStrategy(base, delay_bars=1)
    bars = [
        bar.model_copy(
            update={
                "timestamp": bar.timestamp + timedelta(days=offset),
                "close": price,
            }
        )
        for offset, price in enumerate(
            [Decimal("100"), Decimal("101"), Decimal("104"), Decimal("105")]
        )
    ]

    assert strategy.on_bar(bars[0]) is None
    assert strategy.on_bar(bars[1]) is None
    assert strategy.on_bar(bars[2]) is None
    delayed = strategy.on_bar(bars[3])
    assert delayed is not None
    assert delayed.generated_at == bars[3].timestamp
    assert "1-bar delay" in delayed.rationale


def test_delayed_strategy_validates_delay(bar: MarketBar) -> None:
    base = SmaCrossoverStrategy(StrategyConfig())
    with pytest.raises(ValueError, match="cannot be negative"):
        DelayedStrategy(base, delay_bars=-1)
    immediate = DelayedStrategy(base, delay_bars=0)
    assert immediate.strategy_id == base.strategy_id
    assert immediate.on_bar(bar) is None


def test_mean_reversion_uses_entry_exit_hysteresis(bar: MarketBar) -> None:
    config = StrategyConfig(
        mean_reversion_window=3,
        mean_reversion_entry_z=Decimal("-0.5"),
        mean_reversion_exit_z=Decimal("0"),
    )
    strategy = MeanReversionStrategy(config)
    prices = [
        Decimal("100"),
        Decimal("100"),
        Decimal("95"),
        Decimal("96"),
        Decimal("105"),
    ]
    intents = []
    for offset, price in enumerate(prices):
        intents.append(
            strategy.on_bar(
                bar.model_copy(
                    update={
                        "timestamp": bar.timestamp + timedelta(days=offset),
                        "close": price,
                    }
                )
            )
        )

    assert intents[2] is not None
    assert intents[2].target_weight == config.target_weight
    assert intents[3] is not None
    assert intents[3].target_weight == config.target_weight
    assert intents[4] is not None
    assert intents[4].target_weight == Decimal(0)


def test_mean_reversion_handles_zero_volatility(bar: MarketBar) -> None:
    strategy = MeanReversionStrategy(StrategyConfig(mean_reversion_window=3))

    strategy.on_bar(bar)
    strategy.on_bar(bar.model_copy(update={"timestamp": bar.timestamp + timedelta(days=1)}))
    intent = strategy.on_bar(
        bar.model_copy(update={"timestamp": bar.timestamp + timedelta(days=2)})
    )

    assert intent is not None
    assert intent.target_weight == Decimal(0)
