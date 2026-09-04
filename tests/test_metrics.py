from __future__ import annotations

from decimal import Decimal

import pytest

from tradeagent.metrics import performance_metrics


def test_performance_metrics_measure_return_drawdown_and_turnover() -> None:
    metrics = performance_metrics(
        [
            Decimal("100"),
            Decimal("102"),
            Decimal("99"),
            Decimal("105"),
        ],
        Decimal("50"),
    )

    assert metrics["total_return"] == Decimal("0.05")
    assert metrics["max_drawdown"] == Decimal("99") / Decimal("102") - Decimal(1)
    assert metrics["turnover"] > 0
    assert metrics["annualized_volatility"] > 0
    assert metrics["sharpe_ratio"] is not None
    assert metrics["sortino_ratio"] is not None
    assert metrics["calmar_ratio"] is not None


def test_performance_metrics_reject_empty_curve() -> None:
    with pytest.raises(ValueError, match="equity observation"):
        performance_metrics([], Decimal(0))


def test_flat_curve_has_no_ratio_metrics() -> None:
    metrics = performance_metrics(
        [Decimal("100"), Decimal("100"), Decimal("100")],
        Decimal(0),
    )

    assert metrics["sharpe_ratio"] is None
    assert metrics["sortino_ratio"] is None
    assert metrics["calmar_ratio"] is None
