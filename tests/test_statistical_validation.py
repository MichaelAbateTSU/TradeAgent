from __future__ import annotations

from decimal import Decimal

import pytest

from tradeagent.statistical_validation import (
    combinatorially_symmetric_cross_validation,
    deflated_sharpe_probability,
    effective_number_of_independent_trials,
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
    assert deflated_sharpe_probability(
        returns,
        number_of_trials=3,
        trial_sharpes=(Decimal("-0.1"), Decimal(0), Decimal("0.1")),
    ) > Decimal("0.95")


def test_cscv_pbo_is_near_zero_for_stable_ordering() -> None:
    candidates = [
        [Decimal("0.03")] * 12,
        [Decimal("0.02")] * 12,
        [Decimal("0.01")] * 12,
    ]

    result = combinatorially_symmetric_cross_validation(
        candidates,
        subsets=6,
    )

    assert result.probability == 0
    assert all(logit > 0 for logit in result.logits)
    assert result.combinations == 20


def test_cscv_pbo_is_one_for_perfect_regime_overfit() -> None:
    candidates = [
        [Decimal(value) for value in (1, 1, 1, -1, -1, -1)],
        [Decimal(value) for value in (-1, -1, -1, 1, 1, 1)],
    ]

    result = combinatorially_symmetric_cross_validation(
        candidates,
        subsets=6,
    )

    assert result.probability == 1
    assert all(logit < 0 for logit in result.logits)


def test_cscv_pbo_is_half_for_mixed_out_of_sample_ranks() -> None:
    candidates = [
        [Decimal(value) for value in (1, 1, 1, -1, -3, -1, -1, -2, -2, -4, -1, 3)],
        [Decimal(value) for value in (1, -3, -3, 2, -1, 3, -3, 2, -2, -1, 2, -1)],
    ]

    result = combinatorially_symmetric_cross_validation(
        candidates,
        subsets=6,
    )

    assert result.probability == Decimal("0.5")
    assert sum(logit <= 0 for logit in result.logits) == 10


def test_effective_trials_deduplicates_reruns_and_discounts_correlation() -> None:
    first = [Decimal(value) for value in (1, 2, 3, 4, 5, 6)]
    duplicate_rerun = list(first)
    correlated = [Decimal(value) for value in (1, 2, 3, 4, 5, 7)]
    independent = [Decimal(value) for value in (1, -1, 1, -1, 1, -1)]

    effective = effective_number_of_independent_trials(
        [first, duplicate_rerun, correlated, independent]
    )

    assert Decimal(1) < effective < Decimal(3)


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
    with pytest.raises(ValueError, match="trial Sharpes"):
        deflated_sharpe_probability(
            [Decimal("0.01")] * 3,
            number_of_trials=2,
        )
    with pytest.raises(ValueError, match="two strategies"):
        probability_of_backtest_overfitting([[Decimal("0.01")] * 6])
    with pytest.raises(ValueError, match="even"):
        probability_of_backtest_overfitting(
            [[Decimal("0.01")] * 6, [Decimal(0)] * 6],
            subsets=3,
        )
    with pytest.raises(ValueError, match="divide evenly"):
        probability_of_backtest_overfitting(
            [[Decimal("0.01")] * 5, [Decimal(0)] * 5],
            subsets=4,
        )
