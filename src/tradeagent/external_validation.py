from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict

from tradeagent.frozen_candidate import (
    FrozenCandidateManifest,
    strategy_from_manifest,
)
from tradeagent.lower_calibration import LowerCalibrationReport
from tradeagent.lower_execution_evidence import PointInTimeSnapshot
from tradeagent.observed_execution import (
    ExecutionStyle,
    ObservedExecutionReport,
    simulate_observed_execution,
)
from tradeagent.portfolio_strategy import EqualWeightPortfolioStrategy
from tradeagent.research import temporal_block_bootstrap_mean_confidence_interval
from tradeagent.statistical_validation import deflated_sharpe_probability
from tradeagent.universe import UniverseFrame


class ExternalCandidateResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    era: str
    strategy_id: str
    family: str
    scenarios: tuple[ObservedExecutionReport, ...]
    benchmark_scenarios: tuple[ObservedExecutionReport, ...]
    benchmark_relative_return: Decimal
    benchmark_relative_ci_lower: Decimal
    benchmark_relative_ci_upper: Decimal
    deflated_sharpe_probability: Decimal
    family_pbo: Decimal
    trade_count: int
    positive_month_ratio: Decimal
    annual_returns: dict[str, Decimal]
    return_without_best_year: Decimal
    pnl_by_instrument: dict[str, Decimal]
    pnl_by_asset_class: dict[str, Decimal]
    best_instrument_positive_pnl_share: Decimal | None
    parameter_neighbor_positive_ratio: Decimal
    qualified: bool
    qualification_reasons: tuple[str, ...]


class ExternalEraReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    generated_at: datetime
    era: str
    dataset_manifest: str
    evidence_manifest: str
    candidates: tuple[ExternalCandidateResult, ...]


def evaluate_external_era(
    frames: Sequence[UniverseFrame],
    snapshots: dict[tuple[str, datetime], PointInTimeSnapshot],
    candidates: Sequence[FrozenCandidateManifest],
    development: LowerCalibrationReport,
    *,
    era: str,
    dataset_manifest: str,
    evidence_manifest: str,
    generated_at: datetime,
) -> ExternalEraReport:
    results: list[ExternalCandidateResult] = []
    benchmark_cache: dict[tuple[Decimal, Decimal], ObservedExecutionReport] = {}
    scenario_specs: tuple[tuple[ExecutionStyle, Decimal], ...] = (
        ("market", Decimal(1)),
        ("market", Decimal("1.5")),
        ("market", Decimal(2)),
        ("market", Decimal(3)),
        ("decision_marketable_limit", Decimal(1)),
    )
    for manifest in candidates:
        gross = (
            Decimal("0.20")
            if manifest.family == "multi-day-time-series-momentum"
            else Decimal("0.10")
        )
        scenarios = tuple(
            simulate_observed_execution(
                frames,
                strategy_from_manifest(manifest),
                snapshots,
                execution_style=style,
                cost_multiplier=multiplier,
            )
            for style, multiplier in scenario_specs
        )
        benchmarks: list[ObservedExecutionReport] = []
        for multiplier in (Decimal(1), Decimal("1.5"), Decimal(2), Decimal(3)):
            key = gross, multiplier
            if key not in benchmark_cache:
                benchmark_cache[key] = simulate_observed_execution(
                    frames,
                    EqualWeightPortfolioStrategy(gross),
                    snapshots,
                    execution_style="market",
                    cost_multiplier=multiplier,
                )
            benchmarks.append(benchmark_cache[key])
        benchmark_scenarios = tuple(benchmarks)
        active = _active_returns(scenarios[0], benchmark_scenarios[0])
        ci_lower, ci_upper = temporal_block_bootstrap_mean_confidence_interval(
            active,
            samples=5_000,
            confidence_level=Decimal("0.95"),
            random_seed=10_100,
            block_size=min(20, len(active)),
        )
        dsr = deflated_sharpe_probability(
            active,
            number_of_trials=development.effective_independent_trials,
            trial_sharpes=development.trial_periodic_sharpes,
        )
        monthly = _grouped_returns(scenarios[0], monthly=True)
        annual = _grouped_returns(scenarios[0], monthly=False)
        best_year = max(annual, key=lambda key: annual[key])
        without_best = [
            value
            for day, value in zip(
                scenarios[0].period_dates,
                scenarios[0].period_returns,
                strict=True,
            )
            if str(day.year) != best_year
        ]
        by_class = _asset_class_pnl(scenarios[0].pnl_by_symbol)
        positive_instruments = {
            symbol: pnl
            for symbol, pnl in scenarios[0].pnl_by_symbol.items()
            if symbol != "REGULATORY_FEES" and pnl > 0
        }
        positive_total = sum(positive_instruments.values(), Decimal(0))
        best_instrument_share = (
            max(positive_instruments.values()) / positive_total if positive_total > 0 else None
        )
        family = next(family for family in development.families if family.family == manifest.family)
        selected = [
            configuration
            for configuration in family.configurations
            if configuration.configuration_index
            in {member.configuration_index for member in manifest.members}
        ]
        neighbor_positive = Decimal(
            sum(configuration.scenarios[0].total_return > 0 for configuration in selected)
        ) / Decimal(len(selected))
        relative_return = _compound(active)
        reasons: list[str] = []
        if scenarios[0].total_return <= 0:
            reasons.append("NONPOSITIVE_NET_RETURN")
        if relative_return <= 0:
            reasons.append("NONPOSITIVE_BENCHMARK_RELATIVE_RETURN")
        if ci_lower <= 0:
            reasons.append("BLOCK_BOOTSTRAP_CONFIDENCE_FAILED")
        if dsr < Decimal("0.95"):
            reasons.append("DEFLATED_SHARPE_FAILED")
        if manifest.family_pbo > Decimal("0.20"):
            reasons.append("FAMILY_PBO_FAILED")
        for scenario, benchmark in zip(
            scenarios[1:4],
            benchmark_scenarios[1:],
            strict=True,
        ):
            if scenario.total_return <= 0 or _compound(_active_returns(scenario, benchmark)) <= 0:
                reasons.append(f"COST_STRESS_{scenario.cost_multiplier}_FAILED")
        if scenarios[0].full_fills // 2 < 200:
            reasons.append("INSUFFICIENT_TRADE_COUNT")
        if scenarios[0].max_drawdown < Decimal("-0.15"):
            reasons.append("DRAWDOWN_FAILED")
        if _compound(without_best) <= 0:
            reasons.append("BEST_YEAR_DEPENDENCE")
        if best_instrument_share is not None and best_instrument_share > Decimal("0.50"):
            reasons.append("BEST_INSTRUMENT_DEPENDENCE")
        if neighbor_positive < Decimal("0.75"):
            reasons.append("PARAMETER_NEIGHBOR_INSTABILITY")
        if manifest.external_data_acquired_before_freeze:
            reasons.append("CANDIDATE_NOT_FROZEN_BEFORE_DATA")
        results.append(
            ExternalCandidateResult(
                era=era,
                strategy_id=manifest.strategy_id,
                family=manifest.family,
                scenarios=scenarios,
                benchmark_scenarios=benchmark_scenarios,
                benchmark_relative_return=relative_return,
                benchmark_relative_ci_lower=ci_lower,
                benchmark_relative_ci_upper=ci_upper,
                deflated_sharpe_probability=dsr,
                family_pbo=manifest.family_pbo,
                trade_count=scenarios[0].full_fills // 2,
                positive_month_ratio=(
                    Decimal(sum(value > 0 for value in monthly.values())) / Decimal(len(monthly))
                ),
                annual_returns=annual,
                return_without_best_year=_compound(without_best),
                pnl_by_instrument=scenarios[0].pnl_by_symbol,
                pnl_by_asset_class=by_class,
                best_instrument_positive_pnl_share=best_instrument_share,
                parameter_neighbor_positive_ratio=neighbor_positive,
                qualified=not reasons,
                qualification_reasons=tuple(dict.fromkeys(reasons)),
            )
        )
    return ExternalEraReport(
        generated_at=generated_at,
        era=era,
        dataset_manifest=dataset_manifest,
        evidence_manifest=evidence_manifest,
        candidates=tuple(results),
    )


def _grouped_returns(
    report: ObservedExecutionReport,
    *,
    monthly: bool,
) -> dict[str, Decimal]:
    grouped: defaultdict[str, list[Decimal]] = defaultdict(list)
    for day, value in zip(report.period_dates, report.period_returns, strict=True):
        key = f"{day.year:04d}-{day.month:02d}" if monthly else str(day.year)
        grouped[key].append(value)
    return {key: _compound(values) for key, values in grouped.items()}


def _asset_class_pnl(pnl_by_symbol: dict[str, Decimal]) -> dict[str, Decimal]:
    broad = {"SPY", "QQQ", "IWM", "DIA", "EFA", "EEM"}
    treasury = {"SHY", "IEF", "TLT"}
    sectors = {
        "XLB",
        "XLC",
        "XLE",
        "XLF",
        "XLI",
        "XLK",
        "XLP",
        "XLRE",
        "XLU",
        "XLV",
        "XLY",
    }
    classes: defaultdict[str, Decimal] = defaultdict(Decimal)
    for symbol, pnl in pnl_by_symbol.items():
        if symbol in broad:
            classes["broad_equity"] += pnl
        elif symbol in treasury:
            classes["treasury"] += pnl
        elif symbol in sectors:
            classes["sector_equity"] += pnl
        elif symbol == "GLD":
            classes["gold"] += pnl
        elif symbol == "REGULATORY_FEES":
            classes["regulatory_fees"] += pnl
        else:
            classes["other"] += pnl
    return dict(classes)


def _active_returns(
    candidate: ObservedExecutionReport,
    benchmark: ObservedExecutionReport,
) -> tuple[Decimal, ...]:
    if candidate.period_dates != benchmark.period_dates:
        raise ValueError("external candidate and benchmark dates are not aligned")
    return tuple(
        candidate_value - benchmark_value
        for candidate_value, benchmark_value in zip(
            candidate.period_returns,
            benchmark.period_returns,
            strict=True,
        )
    )


def _compound(values: Sequence[Decimal]) -> Decimal:
    wealth = Decimal(1)
    for value in values:
        wealth *= Decimal(1) + value
    return wealth - Decimal(1)
