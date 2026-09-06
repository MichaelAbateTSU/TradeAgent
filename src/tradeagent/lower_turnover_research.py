from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import datetime
from decimal import Decimal
from hashlib import sha256
from itertools import pairwise
from pathlib import Path
from statistics import mean, stdev

from pydantic import BaseModel, ConfigDict, Field

from tradeagent.config import AppConfig
from tradeagent.diagnostics import StrategyDiagnostics, diagnose_strategy
from tradeagent.lower_turnover import (
    RELATIVE_STRENGTH_CONFIGS,
    SWING_MEAN_REVERSION_CONFIGS,
    TIME_SERIES_MOMENTUM_CONFIGS,
    RegimeConditionedSwingMeanReversionStrategy,
    RelativeStrengthConfig,
    RelativeStrengthRotationStrategy,
    SwingMeanReversionConfig,
    TimeSeriesMomentumConfig,
    TimeSeriesMomentumStrategy,
)
from tradeagent.portfolio import PortfolioStrategy
from tradeagent.portfolio_research import PortfolioResearchReport, evaluate_portfolio_suite
from tradeagent.portfolio_strategy import EqualWeightPortfolioStrategy
from tradeagent.research import ExperimentRegistry, WalkForwardConfig, current_git_sha
from tradeagent.statistical_validation import (
    combinatorially_symmetric_cross_validation,
    deflated_sharpe_probability,
    effective_number_of_independent_trials,
)
from tradeagent.universe import UniverseFrame, UniverseManifest, load_universe

StrategyFactory = Callable[[], PortfolioStrategy]
LowerTurnoverConfig = TimeSeriesMomentumConfig | RelativeStrengthConfig | SwingMeanReversionConfig


class RegimePerformance(BaseModel):
    model_config = ConfigDict(frozen=True)

    regime: str
    observations: int = Field(ge=0)
    mean_net_return_bps: Decimal


class EconomicAttribution(BaseModel):
    model_config = ConfigDict(frozen=True)

    gross_edge: Decimal
    spread_cost: Decimal
    delay_cost: Decimal
    slippage: Decimal
    fees: Decimal
    flattening_cost: Decimal
    net_edge: Decimal
    edge_to_cost_ratio: Decimal | None


class LowerTurnoverResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    family: str
    configuration_index: int = Field(ge=1, le=10)
    strategy_id: str
    config_hash: str
    experiment_id: int
    diagnostics: StrategyDiagnostics
    attribution: EconomicAttribution
    turnover: Decimal
    max_drawdown: Decimal
    walk_forward_positive_ratio: Decimal
    alpha_decay_two_frames: Decimal
    alpha_decay_five_frames: Decimal
    regime_performance: tuple[RegimePerformance, ...]
    qualified: bool
    qualification_reasons: tuple[str, ...]
    validation: PortfolioResearchReport


class LowerTurnoverFamilyReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    family: str
    configurations_tested: int
    adjacent_sign_stability: Decimal
    family_pbo: Decimal | None
    pbo_logits: tuple[Decimal, ...] = ()
    results: tuple[LowerTurnoverResult, ...]


class LowerTurnoverResearchReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    generated_at: datetime
    git_sha: str
    dataset: UniverseManifest
    experiment_count: int
    families: tuple[LowerTurnoverFamilyReport, ...]
    qualified_strategy_ids: tuple[str, ...]
    raw_hypothesis_count: int = 30
    effective_independent_trials: Decimal | None = None
    deterministic_reruns_counted_as_hypotheses: bool = False


def evaluate_lower_turnover_families(
    universe_directory: Path,
    symbols: Sequence[str],
    experiment_database: Path,
    *,
    generated_at: datetime,
) -> LowerTurnoverResearchReport:
    dataset = load_universe(universe_directory, symbols)
    frames = dataset.frames
    app_config = AppConfig()
    walk_forward = WalkForwardConfig(
        training_bars=504,
        testing_bars=126,
        step_bars=126,
        embargo_bars=21,
        warmup_bars=252,
    )
    family_specs: tuple[tuple[str, Sequence[LowerTurnoverConfig], Decimal], ...] = (
        (
            "multi-day-time-series-momentum",
            TIME_SERIES_MOMENTUM_CONFIGS,
            Decimal("0.20"),
        ),
        (
            "cross-sectional-relative-strength",
            RELATIVE_STRENGTH_CONFIGS,
            Decimal("0.10"),
        ),
        (
            "regime-conditioned-swing-mean-reversion",
            SWING_MEAN_REVERSION_CONFIGS,
            Decimal("0.06"),
        ),
    )
    git_sha = current_git_sha()
    preliminary_families: list[LowerTurnoverFamilyReport] = []
    with ExperimentRegistry(experiment_database) as registry:
        for family, configurations, benchmark_gross in family_specs:
            results: list[LowerTurnoverResult] = []
            for index, strategy_config in enumerate(configurations, start=1):
                strategy_factory = _strategy_factory(strategy_config)
                validation = evaluate_portfolio_suite(
                    frames,
                    dataset.manifest,
                    app_config,
                    walk_forward,
                    strategy_config,
                    strategy_factory,
                    _equal_weight_factory(benchmark_gross),
                    random_seed=7000 + index,
                    minimum_closed_trades=200,
                    minimum_deflated_sharpe_probability=None,
                    maximum_backtest_overfitting_probability=None,
                    number_of_trials=1,
                    git_sha=git_sha,
                )
                diagnostics = diagnose_strategy(
                    frames,
                    app_config,
                    strategy_factory(),
                    delay_frames=1,
                )
                delay_two = diagnose_strategy(
                    frames,
                    app_config,
                    strategy_factory(),
                    delay_frames=2,
                )
                delay_five = diagnose_strategy(
                    frames,
                    app_config,
                    strategy_factory(),
                    delay_frames=5,
                )
                config_hash = validation.config_hash
                total_cost = diagnostics.execution_cost
                result = LowerTurnoverResult(
                    family=family,
                    configuration_index=index,
                    strategy_id=validation.scenarios[0].strategy_id,
                    config_hash=config_hash,
                    experiment_id=0,
                    diagnostics=diagnostics,
                    attribution=EconomicAttribution(
                        gross_edge=diagnostics.gross_pnl,
                        spread_cost=diagnostics.spread_cost,
                        delay_cost=diagnostics.net_pnl - delay_two.net_pnl,
                        slippage=diagnostics.slippage_cost,
                        fees=diagnostics.fees,
                        flattening_cost=diagnostics.flattening_cost,
                        net_edge=diagnostics.net_pnl,
                        edge_to_cost_ratio=(
                            diagnostics.gross_pnl / total_cost if total_cost > 0 else None
                        ),
                    ),
                    turnover=sum(
                        (fold.report.turnover for fold in validation.scenarios[0].folds),
                        Decimal(0),
                    )
                    / len(validation.scenarios[0].folds),
                    max_drawdown=validation.scenarios[0].worst_drawdown,
                    walk_forward_positive_ratio=validation.scenarios[0].positive_fold_ratio,
                    alpha_decay_two_frames=delay_two.net_pnl - diagnostics.net_pnl,
                    alpha_decay_five_frames=delay_five.net_pnl - diagnostics.net_pnl,
                    regime_performance=_regime_performance(
                        frames,
                        strategy_factory(),
                        total_cost_bps=Decimal("7"),
                    ),
                    qualified=validation.qualified,
                    qualification_reasons=validation.qualification_reasons,
                    validation=validation,
                )
                results.append(result)
            preliminary_families.append(_family_report(family, results))

        trial_returns = [
            _active_return_series(result)
            for family_report in preliminary_families
            for result in family_report.results
        ]
        effective_trials = effective_number_of_independent_trials(trial_returns)
        trial_sharpes = tuple(_periodic_sharpe(values) for values in trial_returns)
        families: list[LowerTurnoverFamilyReport] = []
        for family_report in preliminary_families:
            family_returns = [_active_return_series(result) for result in family_report.results]
            pbo = combinatorially_symmetric_cross_validation(
                family_returns,
                subsets=_cscv_subsets(len(family_returns[0])),
            )
            corrected_results: list[LowerTurnoverResult] = []
            for result in family_report.results:
                active_returns = _active_return_series(result)
                dsr = deflated_sharpe_probability(
                    active_returns,
                    number_of_trials=effective_trials,
                    trial_sharpes=trial_sharpes,
                )
                reasons = list(result.validation.qualification_reasons)
                if dsr < Decimal("0.95"):
                    reasons.append("DEFLATED_SHARPE_FAILED")
                if pbo.probability > Decimal("0.20"):
                    reasons.append("BACKTEST_OVERFITTING_FAILED")
                validation = result.validation.model_copy(
                    update={
                        "deflated_sharpe_probability": dsr,
                        "probability_backtest_overfitting": pbo.probability,
                        "qualified": not reasons,
                        "qualification_reasons": tuple(reasons),
                    }
                )
                experiment_id = registry.record_model(
                    validation,
                    dataset_hash=dataset.manifest.dataset_hash,
                    config_hash_value=result.config_hash,
                    git_sha=git_sha,
                    random_seed=7000 + result.configuration_index,
                    strategy_id=result.strategy_id,
                    qualified=validation.qualified,
                )
                corrected_results.append(
                    result.model_copy(
                        update={
                            "experiment_id": experiment_id,
                            "qualified": validation.qualified,
                            "qualification_reasons": validation.qualification_reasons,
                            "validation": validation,
                        }
                    )
                )
            families.append(
                _family_report(
                    family_report.family,
                    corrected_results,
                    family_pbo=pbo.probability,
                    pbo_logits=pbo.logits,
                )
            )

    qualified = tuple(
        result.strategy_id
        for family_report in families
        for result in family_report.results
        if result.qualified
    )
    return LowerTurnoverResearchReport(
        generated_at=generated_at,
        git_sha=git_sha,
        dataset=dataset.manifest,
        experiment_count=sum(len(family.results) for family in families),
        families=tuple(families),
        qualified_strategy_ids=qualified,
        raw_hypothesis_count=len(trial_returns),
        effective_independent_trials=effective_trials,
        deterministic_reruns_counted_as_hypotheses=False,
    )


def _family_report(
    family: str,
    results: Sequence[LowerTurnoverResult],
    *,
    family_pbo: Decimal | None = None,
    pbo_logits: tuple[Decimal, ...] = (),
) -> LowerTurnoverFamilyReport:
    signs = [result.diagnostics.net_pnl > 0 for result in results]
    adjacent_matches = sum(left == right for left, right in pairwise(signs))
    stability = (
        Decimal(adjacent_matches) / Decimal(len(signs) - 1) if len(signs) > 1 else Decimal(0)
    )
    return LowerTurnoverFamilyReport(
        family=family,
        configurations_tested=len(results),
        adjacent_sign_stability=stability,
        family_pbo=family_pbo,
        pbo_logits=pbo_logits,
        results=tuple(results),
    )


def _regime_performance(
    frames: Sequence[UniverseFrame],
    strategy: PortfolioStrategy,
    *,
    total_cost_bps: Decimal,
) -> tuple[RegimePerformance, ...]:
    returns: dict[str, list[Decimal]] = {"risk-on": [], "risk-off": []}
    prior_targets: dict[str, Decimal] = {}
    spy_history: list[Decimal] = []
    for current, following in pairwise(frames):
        spy = current.bar_for("SPY").close
        spy_history.append(spy)
        intent = strategy.on_frame(current)
        targets = intent.target_weights if intent is not None else {}
        if len(spy_history) < 200:
            prior_targets = dict(targets)
            continue
        moving_average = sum(spy_history[-200:], Decimal(0)) / Decimal(200)
        regime = "risk-on" if spy > moving_average else "risk-off"
        gross_return = sum(
            (
                targets.get(bar.symbol, Decimal(0))
                * (following.bar_for(bar.symbol).close / bar.close - 1)
                for bar in current.bars
            ),
            Decimal(0),
        )
        turnover = sum(
            (
                abs(targets.get(symbol, Decimal(0)) - prior_targets.get(symbol, Decimal(0)))
                for symbol in set(targets) | set(prior_targets)
            ),
            Decimal(0),
        )
        returns[regime].append(gross_return - turnover * total_cost_bps / Decimal(10_000))
        prior_targets = dict(targets)
    return tuple(
        RegimePerformance(
            regime=regime,
            observations=len(values),
            mean_net_return_bps=(
                sum(values, Decimal(0)) / Decimal(len(values)) * Decimal(10_000)
                if values
                else Decimal(0)
            ),
        )
        for regime, values in returns.items()
    )


def report_hash(report: LowerTurnoverResearchReport) -> str:
    return sha256(report.model_dump_json().encode()).hexdigest()


def _strategy_factory(config: LowerTurnoverConfig) -> StrategyFactory:
    if isinstance(config, TimeSeriesMomentumConfig):
        return lambda: TimeSeriesMomentumStrategy(config)
    if isinstance(config, RelativeStrengthConfig):
        return lambda: RelativeStrengthRotationStrategy(config)
    return lambda: RegimeConditionedSwingMeanReversionStrategy(config)


def _equal_weight_factory(gross_target: Decimal) -> StrategyFactory:
    return lambda: EqualWeightPortfolioStrategy(gross_target)


def _active_return_series(result: LowerTurnoverResult) -> tuple[Decimal, ...]:
    candidate = [
        value
        for fold in result.validation.scenarios[0].folds
        for value in fold.report.period_returns
    ]
    if not candidate:
        raise ValueError("lower-turnover validation is missing daily returns")
    # The benchmark daily streams are attached by evaluate_portfolio_suite.
    benchmark_values = result.validation.benchmark_period_returns
    if len(candidate) != len(benchmark_values):
        raise ValueError("candidate and benchmark daily returns are not aligned")
    return tuple(
        candidate_value - benchmark_value
        for candidate_value, benchmark_value in zip(
            candidate,
            benchmark_values,
            strict=True,
        )
    )


def _periodic_sharpe(values: Sequence[Decimal]) -> Decimal:
    floats = [float(value) for value in values]
    deviation = stdev(floats)
    return Decimal(str(mean(floats) / deviation)) if deviation > 0 else Decimal(0)


def _cscv_subsets(periods: int) -> int:
    for subsets in (10, 8, 6, 4):
        if periods >= subsets and periods % subsets == 0:
            return subsets
    raise ValueError("daily return history cannot form equal CSCV blocks")
