from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from tradeagent.domain import MarketBar
from tradeagent.lower_turnover import (
    RELATIVE_STRENGTH_CONFIGS,
    SWING_MEAN_REVERSION_CONFIGS,
    TIME_SERIES_MOMENTUM_CONFIGS,
    RegimeConditionedSwingMeanReversionStrategy,
    RelativeStrengthConfig,
    RelativeStrengthRotationStrategy,
    SwingMeanReversionConfig,
    TimeSeriesMomentumConfig,
    TimeSeriesMomentumStrategy,
)
from tradeagent.universe import UniverseFrame


def _frame(day: int, spy: Decimal, qqq: Decimal) -> UniverseFrame:
    timestamp = datetime(2020, 1, 1, tzinfo=UTC) + timedelta(days=day)

    def bar(symbol: str, price: Decimal) -> MarketBar:
        return MarketBar(
            symbol=symbol,
            timestamp=timestamp,
            open=price,
            high=price,
            low=price,
            close=price,
            volume=Decimal("1000000"),
        )

    return UniverseFrame(timestamp=timestamp, bars=(bar("SPY", spy), bar("QQQ", qqq)))


def test_research_budget_is_predefined_and_capped() -> None:
    assert len(TIME_SERIES_MOMENTUM_CONFIGS) == 10
    assert len(RELATIVE_STRENGTH_CONFIGS) == 10
    assert len(SWING_MEAN_REVERSION_CONFIGS) == 10


def test_time_series_momentum_holds_only_positive_assets() -> None:
    strategy = TimeSeriesMomentumStrategy(
        TimeSeriesMomentumConfig(lookback_days=20, skip_days=0, rebalance_days=1)
    )
    intent = None
    for day in range(22):
        intent = strategy.on_frame(
            _frame(
                day,
                Decimal(100 + day),
                Decimal(100 - day),
            )
        )

    assert intent is not None
    assert intent.target_weights["SPY"] > 0
    assert intent.target_weights["QQQ"] == 0


def test_relative_strength_rotates_into_leader() -> None:
    strategy = RelativeStrengthRotationStrategy(
        RelativeStrengthConfig(
            lookback_days=20,
            skip_days=0,
            rebalance_days=1,
            top_n=1,
        )
    )
    intent = None
    for day in range(22):
        intent = strategy.on_frame(
            _frame(
                day,
                Decimal(100 + day),
                Decimal(100 + day * 2),
            )
        )

    assert intent is not None
    assert intent.target_weights["SPY"] == 0
    assert intent.target_weights["QQQ"] > 0


def test_swing_mean_reversion_requires_risk_on_regime() -> None:
    strategy = RegimeConditionedSwingMeanReversionStrategy(
        SwingMeanReversionConfig(
            zscore_window=10,
            entry_zscore=Decimal("-1"),
            exit_zscore=Decimal("0"),
            maximum_holding_days=5,
            regime_lookback_days=50,
        )
    )
    intent = None
    for day in range(55):
        qqq = Decimal(100 + day) if day < 54 else Decimal("80")
        intent = strategy.on_frame(_frame(day, Decimal(100 + day), qqq))

    assert intent is not None
    assert intent.target_weights["QQQ"] > 0
