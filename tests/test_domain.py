from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from tradeagent.config import AppConfig, RiskLimits, StrategyConfig
from tradeagent.domain import MarketBar


def test_market_bar_normalizes_symbol_and_timestamp(bar: MarketBar) -> None:
    assert bar.symbol == "SPY"
    assert bar.timestamp.utcoffset() == bar.timestamp.utcoffset()


def test_market_bar_rejects_naive_timestamp() -> None:
    with pytest.raises(ValidationError, match="timezone-aware"):
        MarketBar(
            symbol="SPY",
            timestamp=datetime(2025, 1, 1),
            open=Decimal("100"),
            high=Decimal("101"),
            low=Decimal("99"),
            close=Decimal("100"),
            volume=Decimal("10"),
        )


def test_market_bar_rejects_inconsistent_range() -> None:
    with pytest.raises(ValidationError, match="inconsistent"):
        MarketBar(
            symbol="SPY",
            timestamp="2025-01-01T00:00:00Z",
            open=Decimal("100"),
            high=Decimal("99"),
            low=Decimal("98"),
            close=Decimal("100"),
            volume=Decimal("10"),
        )


def test_configuration_rejects_inconsistent_limits() -> None:
    with pytest.raises(ValidationError, match="max_order_exposure"):
        RiskLimits(
            max_order_exposure=Decimal("0.10"),
            max_position_exposure=Decimal("0.05"),
        )

    with pytest.raises(ValidationError, match="fast_window"):
        StrategyConfig(fast_window=20, slow_window=20)

    with pytest.raises(ValidationError, match="target_weight"):
        AppConfig(
            strategy=StrategyConfig(target_weight=Decimal("0.03")),
            risk=RiskLimits(max_order_exposure=Decimal("0.02")),
        )
