from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

import pytest

from tradeagent.data import synthetic_bars
from tradeagent.portfolio_strategy import (
    CrossSectionalMomentumStrategy,
    DelayedPortfolioStrategy,
    EqualWeightPortfolioStrategy,
    PortfolioStrategyConfig,
)
from tradeagent.universe import UniverseFrame


def _momentum_frames() -> list[UniverseFrame]:
    base = next(synthetic_bars(symbol="SPY", count=1))
    prices = {
        "SPY": [100, 102, 105, 110],
        "QQQ": [100, 105, 115, 130],
        "TLT": [100, 98, 95, 90],
    }
    frames = []
    for index in range(4):
        timestamp = base.timestamp + timedelta(days=index)
        bars = tuple(
            base.model_copy(
                update={
                    "symbol": symbol,
                    "timestamp": timestamp,
                    "open": Decimal(price),
                    "high": Decimal(price),
                    "low": Decimal(price),
                    "close": Decimal(price),
                }
            )
            for symbol, values in prices.items()
            for price in [values[index]]
        )
        frames.append(UniverseFrame(timestamp=timestamp, bars=bars))
    return frames


def test_cross_sectional_momentum_selects_strongest_positive_assets() -> None:
    strategy = CrossSectionalMomentumStrategy(
        PortfolioStrategyConfig(
            lookback_frames=3,
            top_n=2,
            gross_target=Decimal("0.04"),
        )
    )
    intents = [strategy.on_frame(frame) for frame in _momentum_frames()]

    assert intents[:3] == [None, None, None]
    intent = intents[3]
    assert intent is not None
    assert intent.target_weights == {
        "SPY": Decimal("0.02"),
        "QQQ": Decimal("0.02"),
        "TLT": Decimal(0),
    }


def test_equal_weight_benchmark_matches_requested_gross() -> None:
    frame = _momentum_frames()[0]
    intent = EqualWeightPortfolioStrategy(Decimal("0.09")).on_frame(frame)

    assert sum(intent.target_weights.values(), Decimal(0)) == Decimal("0.09")
    assert set(intent.target_weights.values()) == {Decimal("0.03")}


def test_delayed_portfolio_strategy_defers_intent() -> None:
    frames = _momentum_frames()
    base = EqualWeightPortfolioStrategy(Decimal("0.09"))
    strategy = DelayedPortfolioStrategy(base, delay_frames=1)

    assert strategy.on_frame(frames[0]) is None
    delayed = strategy.on_frame(frames[1])

    assert delayed is not None
    assert delayed.timestamp == frames[1].timestamp
    assert "1-frame delay" in delayed.rationale
    assert strategy.strategy_id == base.strategy_id


def test_portfolio_strategy_configuration_fails_closed() -> None:
    with pytest.raises(ValueError, match="two percent"):
        PortfolioStrategyConfig(top_n=1, gross_target=Decimal("0.10"))
    with pytest.raises(ValueError, match="between zero and one"):
        EqualWeightPortfolioStrategy(Decimal(0))
    with pytest.raises(ValueError, match="cannot be negative"):
        DelayedPortfolioStrategy(
            EqualWeightPortfolioStrategy(Decimal("0.10")),
            delay_frames=-1,
        )
