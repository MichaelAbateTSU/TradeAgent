from __future__ import annotations

import itertools
import math
from collections.abc import Sequence
from dataclasses import dataclass
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
    number_of_trials: int | float | Decimal,
    trial_sharpes: Sequence[Decimal] | None = None,
) -> Decimal:
    trials = float(number_of_trials)
    if trials < 1:
        raise ValueError("number_of_trials must be positive")
    if len(returns) < 3:
        raise ValueError("deflated Sharpe requires at least three returns")
    if trials == 1:
        benchmark = Decimal(0)
    else:
        if trial_sharpes is None or len(trial_sharpes) < 2:
            raise ValueError("multiple-trial DSR requires all comparable trial Sharpes")
        normal = NormalDist()
        euler_gamma = 0.5772156649015329
        expected_maximum = (1 - euler_gamma) * normal.inv_cdf(
            1 - 1 / trials
        ) + euler_gamma * normal.inv_cdf(1 - 1 / (trials * math.e))
        sharpe_dispersion = stdev(float(value) for value in trial_sharpes)
        benchmark = Decimal(str(sharpe_dispersion * expected_maximum))
    return probabilistic_sharpe_ratio(
        returns,
        benchmark_sharpe=benchmark,
    )


@dataclass(frozen=True)
class PboDiagnostics:
    probability: Decimal
    logits: tuple[Decimal, ...]
    selected_indices: tuple[int, ...]
    out_of_sample_relative_ranks: tuple[Decimal, ...]
    combinations: int
    subsets: int


def probability_of_backtest_overfitting(
    strategy_returns: Sequence[Sequence[Decimal]],
    *,
    subsets: int = 6,
) -> Decimal:
    return combinatorially_symmetric_cross_validation(
        strategy_returns,
        subsets=subsets,
    ).probability


def combinatorially_symmetric_cross_validation(
    strategy_returns: Sequence[Sequence[Decimal]],
    *,
    subsets: int = 6,
) -> PboDiagnostics:
    if len(strategy_returns) < 2:
        raise ValueError("PBO requires at least two strategies")
    periods = {len(values) for values in strategy_returns}
    if len(periods) != 1 or not periods or next(iter(periods)) < subsets:
        raise ValueError("PBO strategies must have equal history covering all subsets")
    if subsets < 4 or subsets % 2:
        raise ValueError("PBO subsets must be an even number of at least four")
    if next(iter(periods)) % subsets:
        raise ValueError("PBO history must divide evenly into contiguous temporal subsets")

    period_count = next(iter(periods))
    partitions = [
        list(
            range(
                subset * period_count // subsets,
                (subset + 1) * period_count // subsets,
            )
        )
        for subset in range(subsets)
    ]
    logits: list[Decimal] = []
    selected_indices: list[int] = []
    relative_ranks: list[Decimal] = []
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
        selected_score = testing_scores[selected]
        lower = sum(score < selected_score for score in testing_scores)
        equal = sum(score == selected_score for score in testing_scores)
        rank = Decimal(lower) + (Decimal(equal) + Decimal(1)) / Decimal(2)
        relative_rank = rank / Decimal(len(testing_scores) + 1)
        logit = Decimal(str(math.log(float(relative_rank / (Decimal(1) - relative_rank)))))
        logits.append(logit)
        selected_indices.append(selected)
        relative_ranks.append(relative_rank)
    overfit = sum(logit <= 0 for logit in logits)
    return PboDiagnostics(
        probability=Decimal(overfit) / Decimal(len(logits)),
        logits=tuple(logits),
        selected_indices=tuple(selected_indices),
        out_of_sample_relative_ranks=tuple(relative_ranks),
        combinations=len(logits),
        subsets=subsets,
    )


def effective_number_of_independent_trials(
    strategy_returns: Sequence[Sequence[Decimal]],
) -> Decimal:
    if not strategy_returns:
        raise ValueError("effective trials require at least one strategy")
    periods = {len(values) for values in strategy_returns}
    if len(periods) != 1 or next(iter(periods)) < 2:
        raise ValueError("effective-trial strategies must have equal nontrivial history")
    unique = list(dict.fromkeys(tuple(values) for values in strategy_returns))
    if len(unique) == 1:
        return Decimal(1)
    correlations = [_correlation(left, right) for left, right in itertools.combinations(unique, 2)]
    average_correlation = max(0.0, min(1.0, mean(correlations)))
    effective = 1 + (len(unique) - 1) * (1 - average_correlation)
    return Decimal(str(effective))


def _score(values: Sequence[Decimal]) -> float:
    floats = [float(value) for value in values]
    if len(floats) < 2:
        return mean(floats)
    deviation = stdev(floats)
    return mean(floats) / deviation if deviation > 0 else mean(floats)


def _correlation(left: Sequence[Decimal], right: Sequence[Decimal]) -> float:
    left_values = [float(value) for value in left]
    right_values = [float(value) for value in right]
    left_deviation = stdev(left_values)
    right_deviation = stdev(right_values)
    if left_deviation == 0 or right_deviation == 0:
        return 1.0 if left_values == right_values else 0.0
    left_mean = mean(left_values)
    right_mean = mean(right_values)
    covariance = sum(
        (left_value - left_mean) * (right_value - right_mean)
        for left_value, right_value in zip(left_values, right_values, strict=True)
    ) / (len(left_values) - 1)
    return covariance / (left_deviation * right_deviation)
