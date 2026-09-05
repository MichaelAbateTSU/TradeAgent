from __future__ import annotations

import itertools
import math
from collections.abc import Sequence
from decimal import Decimal
from statistics import NormalDist, mean, stdev


def probabilistic_sharpe_ratio(
    returns: Sequence[Decimal],
    *,
    benchmark_sharpe: Decimal,
) -> Decimal:
    if len(returns) < 3:
        raise ValueError("probabilistic Sharpe requires at least three returns")
    values = [float(value) for value in returns]
    standard_deviation = stdev(values)
    if standard_deviation == 0:
        return Decimal(1) if mean(values) > float(benchmark_sharpe) else Decimal(0)
    observed_sharpe = mean(values) / standard_deviation
    sample_mean = mean(values)
    centered = [value - sample_mean for value in values]
    second_moment = mean(value**2 for value in centered)
    skewness = mean(value**3 for value in centered) / second_moment**1.5 if second_moment > 0 else 0
    kurtosis = mean(value**4 for value in centered) / second_moment**2 if second_moment > 0 else 3
    denominator = math.sqrt(
        max(
            1e-12,
            1 - skewness * observed_sharpe + ((kurtosis - 1) / 4) * observed_sharpe**2,
        )
    )
    statistic = (
        (observed_sharpe - float(benchmark_sharpe)) * math.sqrt(len(values) - 1) / denominator
    )
    return Decimal(str(NormalDist().cdf(statistic)))


def deflated_sharpe_probability(
    returns: Sequence[Decimal],
    *,
    number_of_trials: int,
) -> Decimal:
    if number_of_trials < 1:
        raise ValueError("number_of_trials must be positive")
    if len(returns) < 3:
        raise ValueError("deflated Sharpe requires at least three returns")
    if number_of_trials == 1:
        benchmark = Decimal(0)
    else:
        normal = NormalDist()
        euler_gamma = 0.5772156649015329
        trials = float(number_of_trials)
        expected_maximum = (1 - euler_gamma) * normal.inv_cdf(
            1 - 1 / trials
        ) + euler_gamma * normal.inv_cdf(1 - 1 / (trials * math.e))
        benchmark = Decimal(str(expected_maximum / math.sqrt(len(returns) - 1)))
    return probabilistic_sharpe_ratio(
        returns,
        benchmark_sharpe=benchmark,
    )


def probability_of_backtest_overfitting(
    strategy_returns: Sequence[Sequence[Decimal]],
    *,
    subsets: int = 6,
) -> Decimal:
    if len(strategy_returns) < 2:
        raise ValueError("PBO requires at least two strategies")
    periods = {len(values) for values in strategy_returns}
    if len(periods) != 1 or not periods or next(iter(periods)) < subsets:
        raise ValueError("PBO strategies must have equal history covering all subsets")
    if subsets < 4 or subsets % 2:
        raise ValueError("PBO subsets must be an even number of at least four")

    period_count = next(iter(periods))
    partitions = [
        [index for index in range(period_count) if index % subsets == subset]
        for subset in range(subsets)
    ]
    overfit = 0
    combinations = 0
    for training_subsets in itertools.combinations(range(subsets), subsets // 2):
        training_set = set(training_subsets)
        training_indices = [
            index
            for subset, indices in enumerate(partitions)
            if subset in training_set
            for index in indices
        ]
        testing_indices = [
            index
            for subset, indices in enumerate(partitions)
            if subset not in training_set
            for index in indices
        ]
        training_scores = [
            _score([returns[index] for index in training_indices]) for returns in strategy_returns
        ]
        selected = max(
            range(len(strategy_returns)),
            key=lambda index: (training_scores[index], -index),
        )
        testing_scores = [
            _score([returns[index] for index in testing_indices]) for returns in strategy_returns
        ]
        selected_rank = sum(score <= testing_scores[selected] for score in testing_scores) / len(
            testing_scores
        )
        if selected_rank <= 0.5:
            overfit += 1
        combinations += 1
    return Decimal(overfit) / Decimal(combinations)


def _score(values: Sequence[Decimal]) -> float:
    floats = [float(value) for value in values]
    if len(floats) < 2:
        return mean(floats)
    deviation = stdev(floats)
    return mean(floats) / deviation if deviation > 0 else mean(floats)
