from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Sequence
from concurrent.futures import ProcessPoolExecutor
from datetime import UTC, date, datetime
from decimal import Decimal
from hashlib import sha256
from pathlib import Path

from pydantic import BaseModel, ConfigDict

from tradeagent.advanced_strategy import VolatilitySqueezeBreakoutStrategy
from tradeagent.config import AppConfig, IntradayConfig
from tradeagent.data import read_bars
from tradeagent.diagnostics import StrategyDiagnostics, diagnose_strategy
from tradeagent.domain import MarketBar
from tradeagent.intraday import NyseSessionCalendar
from tradeagent.portfolio import PortfolioStrategy
from tradeagent.portfolio_research import evaluate_portfolio_suite
from tradeagent.portfolio_strategy import EqualWeightPortfolioStrategy
from tradeagent.research import WalkForwardConfig, current_git_sha
from tradeagent.universe import align_universe


class SqueezeMatrixResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    symbol: str
    timeframe: str
    frames: int
    sessions: int
    dataset_hash: str
    gross_average_sharpe: Decimal | None
    net_average_sharpe: Decimal | None
    closed_trades: int
    gross_pnl: Decimal
    spread_cost: Decimal
    slippage: Decimal
    fees: Decimal
    flattening_cost: Decimal
    net_pnl: Decimal
    deflated_sharpe_probability: Decimal | None
    probability_backtest_overfitting: Decimal | None
    qualified: bool
    qualification_reasons: tuple[str, ...]
    diagnostics: StrategyDiagnostics


class FrozenSqueezeConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    lookback_frames: int = 20
    minimum_squeeze_frames: int = 3
    bollinger_width_deviations: Decimal = Decimal("4")
    keltner_width_atr: Decimal = Decimal("3")
    breakout_deviations: Decimal = Decimal("2")
    maximum_holding_frames: int = 12
    target_weight: Decimal = Decimal("0.0025")


class FrozenSqueezeExternalReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    generated_at: datetime
    frozen_specification: str
    frozen_git_commit: str
    evaluation_git_sha: str
    selection_policy: str
    instruments: tuple[str, ...]
    timeframes: tuple[str, ...]
    multiple_testing_trials: int
    results: tuple[SqueezeMatrixResult, ...]
    family_decision: str


def evaluate_frozen_squeeze_matrix(
    source_directory: Path,
    symbols: Sequence[str],
    *,
    generated_at: datetime,
    workers: int = 4,
) -> FrozenSqueezeExternalReport:
    if workers < 1:
        raise ValueError("workers must be positive")
    git_sha = current_git_sha()
    jobs = [
        (
            source_directory,
            symbol,
            timeframe,
            minutes,
            len(symbols) * 3,
            git_sha,
        )
        for timeframe, minutes in (("5Min", 5), ("30Min", 30), ("1Hour", 60))
        for symbol in symbols
    ]
    if workers == 1:
        results = [_evaluate_cell(*job) for job in jobs]
    else:
        with ProcessPoolExecutor(max_workers=min(workers, len(jobs))) as executor:
            results = list(executor.map(_evaluate_cell_from_job, jobs))
    qualified = [result for result in results if result.qualified]
    return FrozenSqueezeExternalReport(
        generated_at=generated_at,
        frozen_specification="research/freezes/v0.8.0.json",
        frozen_git_commit="4cbee33bb1a639cb5da2fa71ff4e50a41eb63059",
        evaluation_git_sha=git_sha,
        selection_policy=(
            "Complete predefined instrument/timeframe matrix; no post-result selection."
        ),
        instruments=tuple(symbols),
        timeframes=("5Min", "30Min", "1Hour"),
        multiple_testing_trials=len(symbols) * 3,
        results=tuple(results),
        family_decision=(
            "continue-investigation" if qualified else "retire-no-robust-external-validation"
        ),
    )


def _evaluate_cell_from_job(
    job: tuple[Path, str, str, int, int, str],
) -> SqueezeMatrixResult:
    return _evaluate_cell(*job)


def _evaluate_cell(
    source_directory: Path,
    symbol: str,
    timeframe: str,
    minutes: int,
    number_of_trials: int,
    git_sha: str,
) -> SqueezeMatrixResult:
    source_timeframe = "5min" if minutes == 5 else "30min"
    source = source_directory / source_timeframe / f"{symbol}.csv"
    source_bars = _regular_bars(tuple(read_bars(source)), minutes=min(minutes, 30))
    bars = _aggregate_hourly(source_bars) if minutes == 60 else source_bars
    dataset = align_universe({symbol: bars})
    intraday = IntradayConfig(
        enabled=True,
        primary_bar_minutes=minutes,
        context_bar_minutes=minutes,
    )
    app_config = AppConfig(intraday=intraday)
    expected_per_session = 390 // minutes
    walk_forward = WalkForwardConfig(
        training_bars=expected_per_session * 504,
        testing_bars=expected_per_session * 126,
        step_bars=expected_per_session * 126,
        embargo_bars=expected_per_session * 21,
        warmup_bars=expected_per_session * 20,
    )
    strategy_factory = _squeeze_factory(intraday)
    validation = evaluate_portfolio_suite(
        dataset.frames,
        dataset.manifest,
        app_config,
        walk_forward,
        FrozenSqueezeConfig(),
        strategy_factory,
        lambda: EqualWeightPortfolioStrategy(Decimal("0.0025")),
        random_seed=9100,
        minimum_closed_trades=200,
        minimum_deflated_sharpe_probability=Decimal("0.95"),
        maximum_backtest_overfitting_probability=Decimal("0.20"),
        number_of_trials=number_of_trials,
        git_sha=git_sha,
    )
    diagnostics = diagnose_strategy(
        dataset.frames,
        app_config,
        strategy_factory(),
        delay_frames=1,
    )
    gross_scenario = next(
        scenario
        for scenario in validation.scenarios
        if scenario.cost_multiplier == 0 and scenario.execution_delay_frames == 1
    )
    net_scenario = validation.scenarios[0]
    return SqueezeMatrixResult(
        symbol=symbol,
        timeframe=timeframe,
        frames=len(dataset.frames),
        sessions=len({frame.timestamp.date() for frame in dataset.frames}),
        dataset_hash=dataset.manifest.dataset_hash,
        gross_average_sharpe=gross_scenario.average_sharpe,
        net_average_sharpe=net_scenario.average_sharpe,
        closed_trades=validation.closed_trade_estimate,
        gross_pnl=diagnostics.gross_pnl,
        spread_cost=diagnostics.spread_cost,
        slippage=diagnostics.slippage_cost,
        fees=diagnostics.fees,
        flattening_cost=diagnostics.flattening_cost,
        net_pnl=diagnostics.net_pnl,
        deflated_sharpe_probability=validation.deflated_sharpe_probability,
        probability_backtest_overfitting=validation.probability_backtest_overfitting,
        qualified=validation.qualified,
        qualification_reasons=validation.qualification_reasons,
        diagnostics=diagnostics,
    )


def _regular_bars(bars: Sequence[MarketBar], *, minutes: int) -> tuple[MarketBar, ...]:
    config = IntradayConfig(
        enabled=True,
        primary_bar_minutes=minutes,
        context_bar_minutes=minutes,
    )
    calendar = NyseSessionCalendar(config)
    output: list[MarketBar] = []
    for bar in bars:
        bounds = calendar.session_bounds(bar.timestamp.astimezone(UTC).date())
        if bounds is not None and bounds[0] < bar.timestamp <= bounds[1]:
            output.append(bar)
    return tuple(output)


def _aggregate_hourly(bars: Sequence[MarketBar]) -> tuple[MarketBar, ...]:
    by_session: defaultdict[date, list[MarketBar]] = defaultdict(list)
    for bar in bars:
        by_session[bar.timestamp.date()].append(bar)
    output: list[MarketBar] = []
    for session_bars in by_session.values():
        for start in range(0, len(session_bars), 2):
            bucket = session_bars[start : start + 2]
            first = bucket[0]
            last = bucket[-1]
            output.append(
                MarketBar(
                    symbol=first.symbol,
                    timestamp=last.timestamp,
                    open=first.open,
                    high=max(bar.high for bar in bucket),
                    low=min(bar.low for bar in bucket),
                    close=last.close,
                    volume=sum((bar.volume for bar in bucket), Decimal(0)),
                )
            )
    return tuple(output)


def report_hash(report: FrozenSqueezeExternalReport) -> str:
    return sha256(report.model_dump_json().encode()).hexdigest()


def _squeeze_factory(intraday: IntradayConfig) -> Callable[[], PortfolioStrategy]:
    return lambda: VolatilitySqueezeBreakoutStrategy(intraday)
