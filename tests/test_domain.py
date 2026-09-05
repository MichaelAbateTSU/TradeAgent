from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from tradeagent.config import (
    AppConfig,
    IntradayConfig,
    RiskLimits,
    StrategyConfig,
    config_fingerprint,
)
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

    with pytest.raises(ValidationError, match="mean_reversion_entry_z"):
        StrategyConfig(
            mean_reversion_entry_z=Decimal("-0.25"),
            mean_reversion_exit_z=Decimal("-0.5"),
        )

    with pytest.raises(ValidationError, match="target_weight"):
        AppConfig(
            strategy=StrategyConfig(target_weight=Decimal("0.03")),
            risk=RiskLimits(max_order_exposure=Decimal("0.02")),
        )


def test_configuration_fingerprint_changes_with_runtime_policy() -> None:
    default = AppConfig()
    changed = AppConfig(trading_enabled=False)

    assert len(config_fingerprint(default)) == 64
    assert config_fingerprint(default) != config_fingerprint(changed)


def test_intraday_mandate_defaults_are_small_and_low_turnover() -> None:
    config = IntradayConfig()

    assert not config.enabled
    assert config.primary_bar_minutes == 5
    assert config.context_bar_minutes == 15
    assert config.maximum_order_notional == Decimal("25")
    assert config.maximum_round_trips_per_day == 2
    assert config.flatten_start < config.hard_flatten_deadline


def test_intraday_mandate_rejects_inconsistent_policy() -> None:
    with pytest.raises(ValidationError, match="multiple"):
        IntradayConfig(primary_bar_minutes=7, context_bar_minutes=15)
    with pytest.raises(ValidationError, match="strictly increasing"):
        IntradayConfig(no_new_entries_after="09:30:00")
    with pytest.raises(ValidationError, match="minimum_order_notional"):
        IntradayConfig(
            minimum_order_notional=Decimal("26"),
            maximum_order_notional=Decimal("25"),
        )
    with pytest.raises(ValidationError, match="hard risk limit"):
        AppConfig(
            intraday=IntradayConfig(maximum_gross_exposure=Decimal("0.10")),
            risk=RiskLimits(max_gross_exposure=Decimal("0.05")),
        )
