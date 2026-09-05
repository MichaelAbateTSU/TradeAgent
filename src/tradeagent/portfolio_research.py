from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Callable, Sequence
from datetime import date, datetime
from decimal import Decimal
from hashlib import sha256
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field

from tradeagent.broker import PaperBroker
from tradeagent.config import AppConfig, BrokerConfig
from tradeagent.ledger import SQLiteLedger
from tradeagent.portfolio import (
    PortfolioBacktestReport,
    PortfolioEngine,
    PortfolioStrategy,
)
from tradeagent.portfolio_strategy import DelayedPortfolioStrategy
from tradeagent.research import (
    WalkForwardConfig,
    bootstrap_mean_confidence_interval,
    current_git_sha,
)
from tradeagent.risk import RiskEngine
from tradeagent.statistical_validation import (
    deflated_sharpe_probability,
    probability_of_backtest_overfitting,
)
from tradeagent.universe import UniverseFrame, UniverseManifest

PortfolioStrategyFactory = Callable[[], PortfolioStrategy]


class PortfolioWalkForwardFold(BaseModel):
    model_config = ConfigDict(frozen=True)

    fold: int = Field(ge=1)
    training_started_at: datetime
    training_ended_at: datetime
    testing_started_at: datetime
    testing_ended_at: datetime
    report: PortfolioBacktestReport


class PortfolioWalkForwardReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    strategy_id: str
    cost_multiplier: Decimal = Field(ge=0)
    execution_delay_frames: int = Field(ge=0)
    folds: tuple[PortfolioWalkForwardFold, ...]
    positive_fold_ratio: Decimal = Field(ge=0, le=1)
    average_sharpe: Decimal | None
    worst_drawdown: Decimal
    qualified: bool
    qualification_reasons: tuple[str, ...]


class PortfolioBenchmarkComparison(BaseModel):
    model_config = ConfigDict(frozen=True)

    benchmark_strategy_id: str
    cost_multiplier: Decimal = Field(ge=0)
    execution_delay_frames: int = Field(ge=0)
    average_excess_return: Decimal
    excess_return_ci_lower: Decimal
    excess_return_ci_upper: Decimal
    confidence_level: Decimal = Field(gt=0, lt=1)
    bootstrap_samples: int = Field(ge=100)
    beat_fold_ratio: Decimal = Field(ge=0, le=1)
    passed: bool


class PortfolioResearchReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    dataset: UniverseManifest
    config_hash: str = Field(min_length=64, max_length=64)
    git_sha: str
    random_seed: int
    scenarios: tuple[PortfolioWalkForwardReport, ...]
    benchmark_comparisons: tuple[PortfolioBenchmarkComparison, ...]
    closed_trade_estimate: int = Field(ge=0)
    minimum_closed_trades: int = Field(ge=0)
    deflated_sharpe_probability: Decimal | None = None
    probability_backtest_overfitting: Decimal | None = None
    qualified: bool
    qualification_reasons: tuple[str, ...]


def portfolio_config_hash(
    app_config: AppConfig,
    walk_forward: WalkForwardConfig,
    strategy_config: BaseModel,
    strategy_id: str,
) -> str:
    payload = json.dumps(
        {
            "app": app_config.model_dump(mode="json"),
            "walk_forward": walk_forward.model_dump(mode="json"),
            "strategy": strategy_config.model_dump(mode="json"),
            "strategy_id": strategy_id,
        },
        sort_keys=True,
    )
    return sha256(payload.encode()).hexdigest()


def evaluate_portfolio_walk_forward(
    frames: Sequence[UniverseFrame],
    app_config: AppConfig,
    walk_forward: WalkForwardConfig,
    strategy_factory: PortfolioStrategyFactory,
    *,
    cost_multiplier: Decimal,
    execution_delay_frames: int,
) -> PortfolioWalkForwardReport:
    required = walk_forward.training_bars + walk_forward.embargo_bars + walk_forward.testing_bars
    if len(frames) < required:
        raise ValueError(f"portfolio walk-forward evaluation requires at least {required} frames")
    broker_config = BrokerConfig(
        starting_cash=app_config.broker.starting_cash,
        slippage_bps=app_config.broker.slippage_bps * cost_multiplier,
        spread_bps=app_config.broker.spread_bps * cost_multiplier,
        commission_bps=app_config.broker.commission_bps * cost_multiplier,
        max_volume_participation=app_config.broker.max_volume_participation,
    )
    scenario_config = app_config.model_copy(update={"broker": broker_config})
    folds: list[PortfolioWalkForwardFold] = []
    for fold_index, (training, testing) in enumerate(
        _fold_windows(frames, walk_forward, scenario_config),
        start=1,
    ):
        base_strategy = strategy_factory()
        if walk_forward.warmup_bars:
            for frame in training[-walk_forward.warmup_bars :]:
                base_strategy.on_frame(frame)
        strategy = DelayedPortfolioStrategy(
            base_strategy,
            delay_frames=execution_delay_frames,
            intraday=(scenario_config.intraday if scenario_config.intraday.enabled else None),
        )
        with SQLiteLedger(":memory:") as ledger:
            report = PortfolioEngine(
                scenario_config,
                strategy,
                PaperBroker(scenario_config.broker),
                RiskEngine(scenario_config.risk),
                ledger,
            ).run(testing)
        folds.append(
            PortfolioWalkForwardFold(
                fold=fold_index,
                training_started_at=training[0].timestamp,
                training_ended_at=training[-1].timestamp,
                testing_started_at=testing[0].timestamp,
                testing_ended_at=testing[-1].timestamp,
                report=report,
            )
        )

    positive = sum(fold.report.total_return > 0 for fold in folds)
    positive_ratio = Decimal(positive) / Decimal(len(folds))
    sharpes = [fold.report.sharpe_ratio for fold in folds if fold.report.sharpe_ratio is not None]
    average_sharpe = sum(sharpes, Decimal(0)) / Decimal(len(sharpes)) if sharpes else None
    worst_drawdown = min(fold.report.max_drawdown for fold in folds)
    reasons: list[str] = []
    if positive_ratio < walk_forward.minimum_positive_fold_ratio:
        reasons.append("INSUFFICIENT_POSITIVE_FOLDS")
    if average_sharpe is None or average_sharpe <= walk_forward.minimum_average_sharpe:
        reasons.append("INSUFFICIENT_AVERAGE_SHARPE")
    return PortfolioWalkForwardReport(
        strategy_id=strategy_factory().strategy_id,
        cost_multiplier=cost_multiplier,
        execution_delay_frames=execution_delay_frames,
        folds=tuple(folds),
        positive_fold_ratio=positive_ratio,
        average_sharpe=average_sharpe,
        worst_drawdown=worst_drawdown,
        qualified=not reasons,
        qualification_reasons=tuple(reasons),
    )


def evaluate_portfolio_suite(
    frames: Sequence[UniverseFrame],
    manifest: UniverseManifest,
    app_config: AppConfig,
    walk_forward: WalkForwardConfig,
    strategy_config: BaseModel,
    strategy_factory: PortfolioStrategyFactory,
    benchmark_factory: PortfolioStrategyFactory,
    *,
    random_seed: int,
    minimum_closed_trades: int = 0,
    minimum_deflated_sharpe_probability: Decimal | None = None,
    maximum_backtest_overfitting_probability: Decimal | None = None,
    number_of_trials: int = 2,
    git_sha: str | None = None,
) -> PortfolioResearchReport:
    strategy_id = strategy_factory().strategy_id
    scenario_specs = (
        (Decimal(1), 1),
        (Decimal(2), 1),
        (Decimal(3), 1),
        (Decimal(1), 2),
        (Decimal(2), 2),
        (Decimal(3), 2),
        (Decimal(0), 1),
        (Decimal("1.5"), 1),
    )
    scenarios = tuple(
        evaluate_portfolio_walk_forward(
            frames,
            app_config,
            walk_forward,
            strategy_factory,
            cost_multiplier=cost,
            execution_delay_frames=delay,
        )
        for cost, delay in scenario_specs
    )
    benchmarks = tuple(
        evaluate_portfolio_walk_forward(
            frames,
            app_config,
            walk_forward,
            benchmark_factory,
            cost_multiplier=cost,
            execution_delay_frames=delay,
        )
        for cost, delay in scenario_specs
    )
    comparisons: list[PortfolioBenchmarkComparison] = []
    for scenario_index, (scenario, benchmark) in enumerate(zip(scenarios, benchmarks, strict=True)):
        excess_returns = [
            candidate.report.total_return - passive.report.total_return
            for candidate, passive in zip(
                scenario.folds,
                benchmark.folds,
                strict=True,
            )
        ]
        wins = sum(excess > 0 for excess in excess_returns)
        beat_ratio = Decimal(wins) / Decimal(len(excess_returns))
        average_excess = sum(excess_returns, Decimal(0)) / Decimal(len(excess_returns))
        lower, upper = bootstrap_mean_confidence_interval(
            excess_returns,
            samples=walk_forward.bootstrap_samples,
            confidence_level=walk_forward.confidence_level,
            random_seed=random_seed + scenario_index,
        )
        comparisons.append(
            PortfolioBenchmarkComparison(
                benchmark_strategy_id=benchmark.strategy_id,
                cost_multiplier=scenario.cost_multiplier,
                execution_delay_frames=scenario.execution_delay_frames,
                average_excess_return=average_excess,
                excess_return_ci_lower=lower,
                excess_return_ci_upper=upper,
                confidence_level=walk_forward.confidence_level,
                bootstrap_samples=walk_forward.bootstrap_samples,
                beat_fold_ratio=beat_ratio,
                passed=(
                    beat_ratio >= walk_forward.minimum_positive_fold_ratio
                    and average_excess > 0
                    and lower > 0
                ),
            )
        )
    reasons: list[str] = []
    if not scenarios[0].qualified:
        reasons.append("BASE_SCENARIO_FAILED")
    if any(not scenario.qualified for scenario in scenarios[1:]):
        reasons.append("EXECUTION_STRESS_FAILED")
    if not comparisons[0].passed:
        reasons.append("BENCHMARK_NOT_BEATEN")
    if any(not comparison.passed for comparison in comparisons[1:]):
        reasons.append("BENCHMARK_EXECUTION_STRESS_FAILED")
    closed_trade_estimate = sum(fold.report.fills for fold in scenarios[0].folds) // 2
    if closed_trade_estimate < minimum_closed_trades:
        reasons.append("INSUFFICIENT_CLOSED_TRADES")
    base_candidate_returns = [fold.report.total_return for fold in scenarios[0].folds]
    base_benchmark_returns = [fold.report.total_return for fold in benchmarks[0].folds]
    excess_returns = [
        candidate - benchmark
        for candidate, benchmark in zip(
            base_candidate_returns,
            base_benchmark_returns,
            strict=True,
        )
    ]
    dsr_probability = (
        deflated_sharpe_probability(
            excess_returns,
            number_of_trials=number_of_trials,
        )
        if len(excess_returns) >= 3
        else None
    )
    pbo_probability = (
        probability_of_backtest_overfitting(
            [base_candidate_returns, base_benchmark_returns],
            subsets=min(6, len(base_candidate_returns) // 2 * 2),
        )
        if len(base_candidate_returns) >= 4
        else None
    )
    if minimum_deflated_sharpe_probability is not None and (
        dsr_probability is None or dsr_probability < minimum_deflated_sharpe_probability
    ):
        reasons.append("DEFLATED_SHARPE_FAILED")
    if maximum_backtest_overfitting_probability is not None and (
        pbo_probability is None or pbo_probability > maximum_backtest_overfitting_probability
    ):
        reasons.append("BACKTEST_OVERFITTING_FAILED")
    return PortfolioResearchReport(
        dataset=manifest,
        config_hash=portfolio_config_hash(
            app_config,
            walk_forward,
            strategy_config,
            strategy_id,
        ),
        git_sha=git_sha or current_git_sha(),
        random_seed=random_seed,
        scenarios=scenarios,
        benchmark_comparisons=tuple(comparisons),
        closed_trade_estimate=closed_trade_estimate,
        minimum_closed_trades=minimum_closed_trades,
        deflated_sharpe_probability=dsr_probability,
        probability_backtest_overfitting=pbo_probability,
        qualified=not reasons,
        qualification_reasons=tuple(reasons),
    )


def _fold_windows(
    frames: Sequence[UniverseFrame],
    walk_forward: WalkForwardConfig,
    config: AppConfig,
) -> list[tuple[list[UniverseFrame], list[UniverseFrame]]]:
    if not config.intraday.enabled:
        windows: list[tuple[list[UniverseFrame], list[UniverseFrame]]] = []
        training_start = 0
        while True:
            training_end = training_start + walk_forward.training_bars
            testing_start = training_end + walk_forward.embargo_bars
            testing_end = testing_start + walk_forward.testing_bars
            if testing_end > len(frames):
                break
            windows.append(
                (
                    list(frames[training_start:training_end]),
                    list(frames[testing_start:testing_end]),
                )
            )
            training_start += walk_forward.step_bars
        return windows

    timezone = ZoneInfo(config.intraday.timezone)
    sessions: dict[date, list[UniverseFrame]] = defaultdict(list)
    for frame in frames:
        sessions[frame.timestamp.astimezone(timezone).date()].append(frame)
    ordered_sessions = [sessions[key] for key in sorted(sessions)]
    expected_per_day = 390 // config.intraday.primary_bar_minutes
    training_sessions = max(1, round(walk_forward.training_bars / expected_per_day))
    testing_sessions = max(1, round(walk_forward.testing_bars / expected_per_day))
    embargo_sessions = max(0, round(walk_forward.embargo_bars / expected_per_day))
    step_sessions = max(1, round(walk_forward.step_bars / expected_per_day))
    windows = []
    training_start = 0
    while True:
        training_end = training_start + training_sessions
        testing_start = training_end + embargo_sessions
        testing_end = testing_start + testing_sessions
        if testing_end > len(ordered_sessions):
            break
        training = [
            frame for session in ordered_sessions[training_start:training_end] for frame in session
        ]
        testing = [
            frame for session in ordered_sessions[testing_start:testing_end] for frame in session
        ]
        windows.append((training, testing))
        training_start += step_sessions
    return windows
