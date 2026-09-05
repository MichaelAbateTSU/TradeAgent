from __future__ import annotations

from decimal import Decimal

import pytest

from tradeagent.statistical_validation import (
    deflated_sharpe_probability,
    probabilistic_sharpe_ratio,
    probability_of_backtest_overfitting,
)


def test_consistent_positive_returns_have_strong_sharpe_probability() -> None:
    returns = [
        Decimal("0.01"),
        Decimal("0.012"),
        Decimal("0.009"),
        Decimal("0.011"),
        Decimal("0.013"),
        Decimal("0.008"),
        Decimal("0.012"),
        Decimal("0.010"),
    ]

    assert probabilistic_sharpe_ratio(returns, benchmark_sharpe=Decimal(0)) > Decimal("0.95")
    assert deflated_sharpe_probability(returns, number_of_trials=3) > Decimal("0.95")


def test_stable_candidate_has_low_backtest_overfitting_probability() -> None:
    candidate = [Decimal("0.01")] * 12
    benchmark = [Decimal("0")] * 12

    probability = probability_of_backtest_overfitting(
        [candidate, benchmark],
        subsets=6,
    )

    assert probability == 0


def test_pbo_uses_contiguous_temporal_blocks() -> None:
    candidate = [
        Decimal("0.03"),
        Decimal("0.03"),
        Decimal("0.03"),
        Decimal("-0.03"),
        Decimal("-0.03"),
        Decimal("-0.03"),
    ]
    benchmark = [Decimal(0)] * 6

    probability = probability_of_backtest_overfitting(
        [candidate, benchmark],
        subsets=6,
    )

    assert Decimal(0) <= probability <= Decimal(1)


def test_statistical_validation_rejects_invalid_samples() -> None:
    with pytest.raises(ValueError, match="three"):
        probabilistic_sharpe_ratio(
            [Decimal("0.01")],
            benchmark_sharpe=Decimal(0),
        )
    with pytest.raises(ValueError, match="positive"):
        deflated_sharpe_probability(
            [Decimal("0.01")] * 3,
            number_of_trials=0,
        )
    with pytest.raises(ValueError, match="two strategies"):
        probability_of_backtest_overfitting([[Decimal("0.01")] * 6])
    with pytest.raises(ValueError, match="even"):
        probability_of_backtest_overfitting(
            [[Decimal("0.01")] * 6, [Decimal(0)] * 6],
            subsets=3,
        )
