from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import datetime
from decimal import Decimal
from statistics import mean, stdev

from pydantic import BaseModel, ConfigDict

from tradeagent.lower_execution_evidence import PointInTimeSnapshot
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
from tradeagent.observed_execution import (
    ExecutionStyle,
    ObservedExecutionReport,
    simulate_observed_execution,
)
from tradeagent.portfolio import PortfolioStrategy
from tradeagent.portfolio_strategy import EqualWeightPortfolioStrategy
from tradeagent.statistical_validation import (
    combinatorially_symmetric_cross_validation,
    deflated_sharpe_probability,
    effective_number_of_independent_trials,
)
from tradeagent.universe import UniverseFrame

LowerConfig = TimeSeriesMomentumConfig | RelativeStrengthConfig | SwingMeanReversionConfig
StrategyFactory = Callable[[], PortfolioStrategy]


class CalibratedConfiguration(BaseModel):
    model_config = ConfigDict(frozen=True)

    family: str
    configuration_index: int
    configuration: dict[str, object]
    scenarios: tuple[ObservedExecutionReport, ...]
    benchmark_scenarios: tuple[ObservedExecutionReport, ...]
    benchmark_relative_return: Decimal
    deflated_sharpe_probability: Decimal
    family_pbo: Decimal
    qualified: bool
    qualification_reasons: tuple[str, ...]


class CalibratedFamily(BaseModel):
    model_config = ConfigDict(frozen=True)

    family: str
    pbo: Decimal
    pbo_logits: tuple[Decimal, ...]
    configurations: tuple[CalibratedConfiguration, ...]


class LowerCalibrationReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    generated_at: datetime
    development_period: str
    evidence_manifest: str
    raw_hypotheses: int
    effective_independent_trials: Decimal
    cscv_periods: int
    cscv_subsets: int
    fee_schedule: dict[str, str]
    slippage_rule: str
    families: tuple[CalibratedFamily, ...]
    qualified_strategy_ids: tuple[str, ...]


def calibrate_lower_turnover_families(
    frames: Sequence[UniverseFrame],
    snapshots: dict[tuple[str, datetime], PointInTimeSnapshot],
    *,
    generated_at: datetime,
    evidence_manifest: str,
) -> LowerCalibrationReport:
    family_specs: tuple[tuple[str, Sequence[LowerConfig], Decimal], ...] = (
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
    preliminary: list[
        tuple[
            str,
            int,
            LowerConfig,
            tuple[ObservedExecutionReport, ...],
            tuple[ObservedExecutionReport, ...],
        ]
    ] = []
    benchmark_cache: dict[tuple[Decimal, Decimal], ObservedExecutionReport] = {}
    scenario_specs: tuple[tuple[ExecutionStyle, Decimal], ...] = (
        ("market", Decimal(1)),
        ("market", Decimal("1.5")),
        ("market", Decimal(2)),
        ("market", Decimal(3)),
        ("decision_marketable_limit", Decimal(1)),
    )
    for family, configurations, benchmark_gross in family_specs:
        for index, configuration in enumerate(configurations, start=1):
            scenarios = tuple(
                simulate_observed_execution(
                    frames,
                    _strategy_factory(configuration)(),
                    snapshots,
                    execution_style=style,
                    cost_multiplier=multiplier,
                )
                for style, multiplier in scenario_specs
            )
            benchmarks: list[ObservedExecutionReport] = []
            for multiplier in (Decimal(1), Decimal("1.5"), Decimal(2), Decimal(3)):
                key = benchmark_gross, multiplier
                if key not in benchmark_cache:
                    benchmark_cache[key] = simulate_observed_execution(
                        frames,
                        EqualWeightPortfolioStrategy(benchmark_gross),
                        snapshots,
                        execution_style="market",
                        cost_multiplier=multiplier,
                    )
                benchmarks.append(benchmark_cache[key])
            preliminary.append(
                (
                    family,
                    index,
                    configuration,
                    scenarios,
                    tuple(benchmarks),
                )
            )

    active_returns = [
        _active_returns(scenarios[0], benchmarks[0])
        for _, _, _, scenarios, benchmarks in preliminary
    ]
    effective_trials = effective_number_of_independent_trials(active_returns)
    trial_sharpes = tuple(_periodic_sharpe(values) for values in active_returns)
    cscv_subsets = 10
    cscv_periods = len(active_returns[0]) - len(active_returns[0]) % cscv_subsets
    families: list[CalibratedFamily] = []
    for family, _, _ in family_specs:
        family_rows = [row for row in preliminary if row[0] == family]
        family_returns = [
            _active_returns(scenarios[0], benchmarks[0])[-cscv_periods:]
            for _, _, _, scenarios, benchmarks in family_rows
        ]
        pbo = combinatorially_symmetric_cross_validation(
            family_returns,
            subsets=cscv_subsets,
        )
        calibrated_configurations: list[CalibratedConfiguration] = []
        for row in family_rows:
            _, index, configuration, scenarios, benchmark_scenarios = row
            active = _active_returns(scenarios[0], benchmark_scenarios[0])
            dsr = deflated_sharpe_probability(
                active,
                number_of_trials=effective_trials,
                trial_sharpes=trial_sharpes,
            )
            relative_return = _compound(active)
            reasons: list[str] = []
            if scenarios[0].total_return <= 0:
                reasons.append("NONPOSITIVE_NET_RETURN")
            if relative_return <= 0:
                reasons.append("BENCHMARK_NOT_BEATEN")
            for scenario, benchmark in zip(
                scenarios[1:4],
                benchmark_scenarios[1:],
                strict=True,
            ):
                if scenario.total_return <= 0:
                    reasons.append(f"COST_STRESS_{scenario.cost_multiplier}_FAILED")
                if _compound(_active_returns(scenario, benchmark)) <= 0:
                    reasons.append(f"BENCHMARK_STRESS_{scenario.cost_multiplier}_FAILED")
            if scenarios[0].full_fills // 2 < 200:
                reasons.append("INSUFFICIENT_CLOSED_TRADES")
            if scenarios[0].max_drawdown < Decimal("-0.15"):
                reasons.append("DRAWDOWN_FAILED")
            if dsr < Decimal("0.95"):
                reasons.append("DEFLATED_SHARPE_FAILED")
            if pbo.probability > Decimal("0.20"):
                reasons.append("BACKTEST_OVERFITTING_FAILED")
            calibrated_configurations.append(
                CalibratedConfiguration(
                    family=family,
                    configuration_index=index,
                    configuration=configuration.model_dump(mode="json"),
                    scenarios=scenarios,
                    benchmark_scenarios=benchmark_scenarios,
                    benchmark_relative_return=relative_return,
                    deflated_sharpe_probability=dsr,
                    family_pbo=pbo.probability,
                    qualified=not reasons,
                    qualification_reasons=tuple(dict.fromkeys(reasons)),
                )
            )
        families.append(
            CalibratedFamily(
                family=family,
                pbo=pbo.probability,
                pbo_logits=pbo.logits,
                configurations=tuple(calibrated_configurations),
            )
        )
    qualified = tuple(
        configuration.scenarios[0].strategy_id
        for family in families
        for configuration in family.configurations
        if configuration.qualified
    )
    return LowerCalibrationReport(
        generated_at=generated_at,
        development_period="2020-01-02 through 2024-12-31",
        evidence_manifest=evidence_manifest,
        raw_hypotheses=len(preliminary),
        effective_independent_trials=effective_trials,
        cscv_periods=cscv_periods,
        cscv_subsets=cscv_subsets,
        fee_schedule={
            "SEC_SECTION_31": "$20.60 per $1,000,000 sold",
            "FINRA_TAF": "$0.000195 per share sold, max $9.79 per trade",
            "CAT": "$0.000003 per share bought or sold",
            "rounding": "each fee type rounded up to $0.01 at end of day",
        },
        slippage_rule=(
            "max(0.5 bps, adverse quote movement across the submission timestamp); "
            "displayed-size cap; no OHLC touch fills"
        ),
        families=tuple(families),
        qualified_strategy_ids=qualified,
    )


def _strategy_factory(configuration: LowerConfig) -> StrategyFactory:
    if isinstance(configuration, TimeSeriesMomentumConfig):
        return lambda: TimeSeriesMomentumStrategy(configuration)
    if isinstance(configuration, RelativeStrengthConfig):
        return lambda: RelativeStrengthRotationStrategy(configuration)
    return lambda: RegimeConditionedSwingMeanReversionStrategy(configuration)


def _active_returns(
    candidate: ObservedExecutionReport,
    benchmark: ObservedExecutionReport,
) -> tuple[Decimal, ...]:
    if candidate.period_dates != benchmark.period_dates:
        raise ValueError("candidate and benchmark dates are not aligned")
    return tuple(
        candidate_return - benchmark_return
        for candidate_return, benchmark_return in zip(
            candidate.period_returns,
            benchmark.period_returns,
            strict=True,
        )
    )


def _periodic_sharpe(values: Sequence[Decimal]) -> Decimal:
    floats = [float(value) for value in values]
    deviation = stdev(floats)
    return Decimal(str(mean(floats) / deviation)) if deviation > 0 else Decimal(0)


def _compound(values: Sequence[Decimal]) -> Decimal:
    wealth = Decimal(1)
    for value in values:
        wealth *= Decimal(1) + value
    return wealth - Decimal(1)
