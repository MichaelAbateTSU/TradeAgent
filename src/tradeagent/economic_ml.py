from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from datetime import date, datetime
from decimal import Decimal
from hashlib import sha256
from itertools import pairwise
from math import sqrt
from statistics import mean, stdev

from pydantic import BaseModel, ConfigDict
from sklearn.linear_model import Ridge
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from tradeagent.research import temporal_block_bootstrap_mean_confidence_interval
from tradeagent.statistical_validation import (
    deflated_sharpe_probability,
    probability_of_backtest_overfitting,
)
from tradeagent.universe import UniverseFrame

MINIMUM_EVENTS = 1_000
MINIMUM_POSITIVE_EVENTS = 100
MINIMUM_TRAINING_POSITIVES_PER_FOLD = 100
RIDGE_ALPHAS = (Decimal("0.1"), Decimal("1"), Decimal("10"))
ESTIMATED_ROUND_TRIP_COST_BPS = Decimal("7")
UNCERTAINTY_BUFFER_BPS = Decimal("3")
HOLDING_DAYS = 5


class EconomicCandidateEvent(BaseModel):
    model_config = ConfigDict(frozen=True)

    symbol: str
    signal_at: datetime
    outcome_at: datetime
    features: tuple[float, ...]
    expected_move_bps: Decimal
    realized_gross_return_bps: Decimal
    realized_net_return_bps: Decimal


class EconomicMlFold(BaseModel):
    model_config = ConfigDict(frozen=True)

    fold: int
    training_started: date
    training_ended: date
    testing_started: date
    testing_ended: date
    training_events: int
    training_positive_events: int
    testing_events: int
    selected_events: int
    mean_selected_net_bps: Decimal


class EconomicMlAttempt(BaseModel):
    model_config = ConfigDict(frozen=True)

    model_name: str
    hyperparameters: dict[str, str]
    folds: tuple[EconomicMlFold, ...]
    selected_events: int
    mean_net_return_bps: Decimal
    date_clustered_returns_bps: dict[date, Decimal]
    date_clustered_sharpe: Decimal | None
    deflated_sharpe_probability: Decimal | None


class EconomicMlReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    total_candidate_events: int
    positive_net_edge_events: int
    independent_years: tuple[int, ...]
    eligible: bool
    eligibility_reasons: tuple[str, ...]
    target: str
    decision_rule: str
    attempted_models: tuple[EconomicMlAttempt, ...]
    baseline: EconomicMlAttempt | None
    best_model: EconomicMlAttempt | None
    improvement_ci_lower_bps: Decimal | None
    probability_backtest_overfitting: Decimal | None
    qualified: bool
    qualification_reasons: tuple[str, ...]


def build_economic_events(frames: Sequence[UniverseFrame]) -> tuple[EconomicCandidateEvent, ...]:
    if len(frames) < 70:
        raise ValueError("economic event generation requires at least 70 daily frames")
    events: list[EconomicCandidateEvent] = []
    symbols = tuple(bar.symbol for bar in frames[0].bars)
    for index in range(63, len(frames) - HOLDING_DAYS - 1):
        current = frames[index]
        execution = frames[index + 1]
        outcome = frames[index + 1 + HOLDING_DAYS]
        cross_sectional_momentum = {
            symbol: current.bar_for(symbol).close / frames[index - 63].bar_for(symbol).close - 1
            for symbol in symbols
        }
        ranked = {
            symbol: rank / max(1, len(symbols) - 1)
            for rank, (symbol, _) in enumerate(
                sorted(cross_sectional_momentum.items(), key=lambda item: item[1])
            )
        }
        for symbol in symbols:
            closes = [
                frames[offset].bar_for(symbol).close for offset in range(index - 63, index + 1)
            ]
            returns = [
                float(current_close / prior_close - 1)
                for prior_close, current_close in pairwise(closes)
            ]
            momentum_63 = cross_sectional_momentum[symbol]
            expected_move = momentum_63 * Decimal(HOLDING_DAYS) / Decimal(63) * Decimal(10_000)
            if expected_move <= ESTIMATED_ROUND_TRIP_COST_BPS + UNCERTAINTY_BUFFER_BPS:
                continue
            momentum_21 = closes[-1] / closes[-22] - 1
            average_20 = sum(closes[-20:], Decimal(0)) / Decimal(20)
            volatility_20 = stdev(returns[-20:]) * sqrt(252)
            gross_bps = (
                outcome.bar_for(symbol).close / execution.bar_for(symbol).close - 1
            ) * Decimal(10_000)
            events.append(
                EconomicCandidateEvent(
                    symbol=symbol,
                    signal_at=current.timestamp,
                    outcome_at=outcome.timestamp,
                    features=(
                        float(momentum_21),
                        float(momentum_63),
                        volatility_20,
                        float(closes[-1] / average_20 - 1),
                        ranked[symbol],
                    ),
                    expected_move_bps=expected_move,
                    realized_gross_return_bps=gross_bps,
                    realized_net_return_bps=gross_bps - ESTIMATED_ROUND_TRIP_COST_BPS,
                )
            )
    return tuple(events)


def evaluate_economic_ml(
    events: Sequence[EconomicCandidateEvent],
) -> EconomicMlReport:
    positives = sum(event.realized_net_return_bps > 0 for event in events)
    years = tuple(sorted({event.signal_at.year for event in events}))
    eligibility_reasons: list[str] = []
    if len(events) < MINIMUM_EVENTS:
        eligibility_reasons.append("INSUFFICIENT_CANDIDATE_EVENTS")
    if positives < MINIMUM_POSITIVE_EVENTS:
        eligibility_reasons.append("INSUFFICIENT_POSITIVE_NET_EVENTS")
    if len(years) < 3:
        eligibility_reasons.append("INSUFFICIENT_INDEPENDENT_PERIODS")
    folds = _event_folds(events)
    if len(folds) < 3:
        eligibility_reasons.append("INSUFFICIENT_TEMPORAL_FOLDS")
    if any(
        sum(event.realized_net_return_bps > 0 for event in training)
        < MINIMUM_TRAINING_POSITIVES_PER_FOLD
        for training, _ in folds
    ):
        eligibility_reasons.append("INSUFFICIENT_FOLD_POSITIVES")
    if eligibility_reasons:
        return _ineligible_report(events, positives, years, eligibility_reasons)

    attempts = tuple(_ridge_attempt(alpha, folds) for alpha in RIDGE_ALPHAS)
    baseline = _baseline_attempt(folds)
    best = max(attempts, key=lambda attempt: attempt.mean_net_return_bps)
    best_daily = best.date_clustered_returns_bps
    baseline_daily = baseline.date_clustered_returns_bps
    common_dates = sorted(set(best_daily) | set(baseline_daily))
    differences = [
        best_daily.get(day, Decimal(0)) - baseline_daily.get(day, Decimal(0))
        for day in common_dates
    ]
    improvement_lower = (
        temporal_block_bootstrap_mean_confidence_interval(
            differences,
            samples=2_000,
            confidence_level=Decimal("0.95"),
            random_seed=9200,
            block_size=min(20, len(differences)),
        )[0]
        if differences
        else None
    )
    fold_matrix = [
        [fold.mean_selected_net_bps for fold in attempt.folds] for attempt in (*attempts, baseline)
    ]
    pbo = probability_of_backtest_overfitting(
        fold_matrix,
        subsets=min(4, len(fold_matrix[0]) // 2 * 2),
    )
    reasons: list[str] = []
    if best.mean_net_return_bps <= baseline.mean_net_return_bps:
        reasons.append("NO_BASELINE_IMPROVEMENT")
    if improvement_lower is None or improvement_lower <= 0:
        reasons.append("IMPROVEMENT_CONFIDENCE_INTERVAL_FAILED")
    if best.deflated_sharpe_probability is None or best.deflated_sharpe_probability < Decimal(
        "0.95"
    ):
        reasons.append("DEFLATED_SHARPE_FAILED")
    if pbo > Decimal("0.20"):
        reasons.append("BACKTEST_OVERFITTING_FAILED")
    return EconomicMlReport(
        total_candidate_events=len(events),
        positive_net_edge_events=positives,
        independent_years=years,
        eligible=True,
        eligibility_reasons=(),
        target="realized five-day net return in basis points after 7 bps estimated round-trip cost",
        decision_rule="predicted net return > 3 bps uncertainty buffer",
        attempted_models=attempts,
        baseline=baseline,
        best_model=best,
        improvement_ci_lower_bps=improvement_lower,
        probability_backtest_overfitting=pbo,
        qualified=not reasons,
        qualification_reasons=tuple(reasons),
    )


def _event_folds(
    events: Sequence[EconomicCandidateEvent],
) -> list[tuple[list[EconomicCandidateEvent], list[EconomicCandidateEvent]]]:
    dates = sorted({event.signal_at.date() for event in events})
    by_date: defaultdict[date, list[EconomicCandidateEvent]] = defaultdict(list)
    for event in events:
        by_date[event.signal_at.date()].append(event)
    folds: list[tuple[list[EconomicCandidateEvent], list[EconomicCandidateEvent]]] = []
    training_days = 504
    testing_days = 126
    embargo_days = HOLDING_DAYS
    start = 0
    while True:
        training_end = start + training_days
        testing_start = training_end + embargo_days
        testing_end = testing_start + testing_days
        if testing_end > len(dates):
            break
        training = [event for day in dates[start:training_end] for event in by_date[day]]
        testing = [event for day in dates[testing_start:testing_end] for event in by_date[day]]
        folds.append((training, testing))
        start += testing_days
    return folds


def _ridge_attempt(
    alpha: Decimal,
    folds: Sequence[tuple[list[EconomicCandidateEvent], list[EconomicCandidateEvent]]],
) -> EconomicMlAttempt:
    fold_reports: list[EconomicMlFold] = []
    selected_by_date: defaultdict[date, list[Decimal]] = defaultdict(list)
    for index, (training, testing) in enumerate(folds, start=1):
        model = make_pipeline(StandardScaler(), Ridge(alpha=float(alpha)))
        model.fit(
            [event.features for event in training],
            [float(event.realized_net_return_bps) for event in training],
        )
        predictions = model.predict([event.features for event in testing])
        for event in testing:
            selected_by_date.setdefault(event.signal_at.date(), [])
        selected = [
            event
            for event, prediction in zip(testing, predictions, strict=True)
            if prediction > float(UNCERTAINTY_BUFFER_BPS)
        ]
        for event in selected:
            selected_by_date[event.signal_at.date()].append(event.realized_net_return_bps)
        fold_reports.append(_fold_report(index, training, testing, selected))
    return _attempt(
        "ridge-net-return-regression",
        {"alpha": str(alpha)},
        fold_reports,
        selected_by_date,
    )


def _baseline_attempt(
    folds: Sequence[tuple[list[EconomicCandidateEvent], list[EconomicCandidateEvent]]],
) -> EconomicMlAttempt:
    fold_reports: list[EconomicMlFold] = []
    selected_by_date: defaultdict[date, list[Decimal]] = defaultdict(list)
    for index, (training, testing) in enumerate(folds, start=1):
        for event in testing:
            selected_by_date.setdefault(event.signal_at.date(), [])
        selected = [
            event
            for event in testing
            if event.expected_move_bps > ESTIMATED_ROUND_TRIP_COST_BPS + UNCERTAINTY_BUFFER_BPS
        ]
        for event in selected:
            selected_by_date[event.signal_at.date()].append(event.realized_net_return_bps)
        fold_reports.append(_fold_report(index, training, testing, selected))
    return _attempt(
        "simple-cost-aware-momentum-threshold",
        {
            "estimated_round_trip_cost_bps": str(ESTIMATED_ROUND_TRIP_COST_BPS),
            "uncertainty_buffer_bps": str(UNCERTAINTY_BUFFER_BPS),
        },
        fold_reports,
        selected_by_date,
    )


def _fold_report(
    index: int,
    training: Sequence[EconomicCandidateEvent],
    testing: Sequence[EconomicCandidateEvent],
    selected: Sequence[EconomicCandidateEvent],
) -> EconomicMlFold:
    selected_values = [event.realized_net_return_bps for event in selected]
    return EconomicMlFold(
        fold=index,
        training_started=training[0].signal_at.date(),
        training_ended=training[-1].signal_at.date(),
        testing_started=testing[0].signal_at.date(),
        testing_ended=testing[-1].signal_at.date(),
        training_events=len(training),
        training_positive_events=sum(event.realized_net_return_bps > 0 for event in training),
        testing_events=len(testing),
        selected_events=len(selected),
        mean_selected_net_bps=(
            sum(selected_values, Decimal(0)) / len(selected_values)
            if selected_values
            else Decimal(0)
        ),
    )


def _attempt(
    model_name: str,
    hyperparameters: dict[str, str],
    folds: Sequence[EconomicMlFold],
    selected_by_date: dict[date, list[Decimal]],
) -> EconomicMlAttempt:
    daily = [
        sum(values, Decimal(0)) / len(values) if values else Decimal(0)
        for _, values in sorted(selected_by_date.items())
    ]
    float_daily = [float(value) for value in daily]
    daily_deviation = stdev(float_daily) if len(float_daily) > 1 else 0.0
    sharpe = (
        Decimal(str(mean(float_daily) / daily_deviation * sqrt(252)))
        if daily_deviation > 0
        else None
    )
    return EconomicMlAttempt(
        model_name=model_name,
        hyperparameters=hyperparameters,
        folds=tuple(folds),
        selected_events=sum(fold.selected_events for fold in folds),
        mean_net_return_bps=(sum(daily, Decimal(0)) / len(daily) if daily else Decimal(0)),
        date_clustered_returns_bps={
            day: (sum(values, Decimal(0)) / len(values) if values else Decimal(0))
            for day, values in selected_by_date.items()
        },
        date_clustered_sharpe=sharpe,
        deflated_sharpe_probability=(
            deflated_sharpe_probability(daily, number_of_trials=len(RIDGE_ALPHAS) + 1)
            if len(daily) >= 3
            else None
        ),
    )


def _ineligible_report(
    events: Sequence[EconomicCandidateEvent],
    positives: int,
    years: tuple[int, ...],
    reasons: Sequence[str],
) -> EconomicMlReport:
    return EconomicMlReport(
        total_candidate_events=len(events),
        positive_net_edge_events=positives,
        independent_years=years,
        eligible=False,
        eligibility_reasons=tuple(reasons),
        target="realized five-day net return in basis points after 7 bps estimated round-trip cost",
        decision_rule="disabled because eligibility minimums were not met",
        attempted_models=(),
        baseline=None,
        best_model=None,
        improvement_ci_lower_bps=None,
        probability_backtest_overfitting=None,
        qualified=False,
        qualification_reasons=("ML_DISABLED",),
    )


def report_hash(report: EconomicMlReport) -> str:
    return sha256(report.model_dump_json().encode()).hexdigest()
