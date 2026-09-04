from __future__ import annotations

import json
import sqlite3
import subprocess
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from decimal import Decimal
from hashlib import sha256
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, model_validator

from tradeagent.broker import PaperBroker
from tradeagent.config import AppConfig, BrokerConfig
from tradeagent.domain import (
    BenchmarkComparison,
    DatasetManifest,
    MarketBar,
    ResearchReport,
    WalkForwardFold,
    WalkForwardReport,
)
from tradeagent.engine import TradingEngine
from tradeagent.ledger import SQLiteLedger
from tradeagent.risk import RiskEngine
from tradeagent.strategy import ConstantWeightStrategy, DelayedStrategy, Strategy

StrategyFactory = Callable[[], Strategy]


class WalkForwardConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    training_bars: int = Field(default=252, ge=20)
    testing_bars: int = Field(default=63, ge=5)
    step_bars: int = Field(default=63, ge=1)
    embargo_bars: int = Field(default=5, ge=0)
    warmup_bars: int = Field(default=50, ge=0)
    minimum_positive_fold_ratio: Decimal = Field(default=Decimal("0.60"), ge=0, le=1)
    minimum_average_sharpe: Decimal = Decimal(0)

    @model_validator(mode="after")
    def validate_warmup(self) -> WalkForwardConfig:
        if self.warmup_bars > self.training_bars:
            raise ValueError("warmup_bars cannot exceed training_bars")
        return self


def dataset_manifest(bars: Sequence[MarketBar]) -> DatasetManifest:
    if not bars:
        raise ValueError("at least one market bar is required")
    canonical = "\n".join(bar.model_dump_json() for bar in bars)
    return DatasetManifest(
        dataset_hash=sha256(canonical.encode()).hexdigest(),
        rows=len(bars),
        symbols=tuple(sorted({bar.symbol for bar in bars})),
        started_at=bars[0].timestamp,
        ended_at=bars[-1].timestamp,
    )


def config_hash(config: BaseModel) -> str:
    payload = json.dumps(config.model_dump(mode="json"), sort_keys=True)
    return sha256(payload.encode()).hexdigest()


def evaluation_config_hash(
    app_config: AppConfig,
    walk_forward: WalkForwardConfig,
    strategy_id: str,
) -> str:
    payload = json.dumps(
        {
            "app": app_config.model_dump(mode="json"),
            "walk_forward": walk_forward.model_dump(mode="json"),
            "strategy_id": strategy_id,
        },
        sort_keys=True,
    )
    return sha256(payload.encode()).hexdigest()


def current_git_sha() -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        check=False,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip() if result.returncode == 0 else "unknown"


def evaluate_walk_forward(
    bars: Sequence[MarketBar],
    app_config: AppConfig,
    walk_forward: WalkForwardConfig,
    strategy_factory: StrategyFactory,
    *,
    cost_multiplier: Decimal = Decimal(1),
    execution_delay_bars: int = 0,
) -> WalkForwardReport:
    required = walk_forward.training_bars + walk_forward.embargo_bars + walk_forward.testing_bars
    if len(bars) < required:
        raise ValueError(f"walk-forward evaluation requires at least {required} bars")

    broker_config = BrokerConfig(
        starting_cash=app_config.broker.starting_cash,
        slippage_bps=app_config.broker.slippage_bps * cost_multiplier,
        commission_bps=app_config.broker.commission_bps * cost_multiplier,
    )
    scenario_config = app_config.model_copy(update={"broker": broker_config})
    folds: list[WalkForwardFold] = []
    training_start = 0
    fold_index = 1
    while True:
        training_end = training_start + walk_forward.training_bars
        testing_start = training_end + walk_forward.embargo_bars
        testing_end = testing_start + walk_forward.testing_bars
        if testing_end > len(bars):
            break

        training = bars[training_start:training_end]
        testing = bars[testing_start:testing_end]
        base_strategy = strategy_factory()
        if walk_forward.warmup_bars:
            for bar in training[-walk_forward.warmup_bars :]:
                base_strategy.on_bar(bar)
        strategy = DelayedStrategy(base_strategy, execution_delay_bars)
        with SQLiteLedger(":memory:") as ledger:
            report = TradingEngine(
                config=scenario_config,
                strategy=strategy,
                broker=PaperBroker(scenario_config.broker),
                risk=RiskEngine(scenario_config.risk),
                ledger=ledger,
            ).run(testing)
        folds.append(
            WalkForwardFold(
                fold=fold_index,
                training_started_at=training[0].timestamp,
                training_ended_at=training[-1].timestamp,
                testing_started_at=testing[0].timestamp,
                testing_ended_at=testing[-1].timestamp,
                report=report,
            )
        )
        fold_index += 1
        training_start += walk_forward.step_bars

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

    return WalkForwardReport(
        strategy_id=strategy_factory().strategy_id,
        cost_multiplier=cost_multiplier,
        execution_delay_bars=execution_delay_bars,
        folds=tuple(folds),
        positive_fold_ratio=positive_ratio,
        average_sharpe=average_sharpe,
        worst_drawdown=worst_drawdown,
        qualified=not reasons,
        qualification_reasons=tuple(reasons),
    )


def evaluate_research_suite(
    bars: Sequence[MarketBar],
    app_config: AppConfig,
    walk_forward: WalkForwardConfig,
    strategy_factory: StrategyFactory,
    *,
    random_seed: int,
    git_sha: str | None = None,
) -> ResearchReport:
    strategy_id = strategy_factory().strategy_id

    def benchmark_factory() -> Strategy:
        return ConstantWeightStrategy("buy-and-hold-v1", app_config.strategy.target_weight)

    scenario_specs = (
        (Decimal(1), 1),
        (Decimal(2), 1),
        (Decimal(3), 1),
        (Decimal(1), 2),
        (Decimal(2), 2),
        (Decimal(3), 2),
    )
    scenarios = tuple(
        evaluate_walk_forward(
            bars,
            app_config,
            walk_forward,
            strategy_factory,
            cost_multiplier=multiplier,
            execution_delay_bars=delay,
        )
        for multiplier, delay in scenario_specs
    )
    benchmark_scenarios = tuple(
        evaluate_walk_forward(
            bars,
            app_config,
            walk_forward,
            benchmark_factory,
            cost_multiplier=multiplier,
            execution_delay_bars=delay,
        )
        for multiplier, delay in scenario_specs
    )
    comparisons: list[BenchmarkComparison] = []
    for scenario, benchmark in zip(scenarios, benchmark_scenarios, strict=True):
        excess_returns = [
            candidate_fold.report.total_return - benchmark_fold.report.total_return
            for candidate_fold, benchmark_fold in zip(scenario.folds, benchmark.folds, strict=True)
        ]
        wins = sum(excess > 0 for excess in excess_returns)
        beat_fold_ratio = Decimal(wins) / Decimal(len(excess_returns))
        average_excess_return = sum(excess_returns, Decimal(0)) / Decimal(len(excess_returns))
        comparisons.append(
            BenchmarkComparison(
                benchmark_strategy_id=benchmark.strategy_id,
                cost_multiplier=scenario.cost_multiplier,
                execution_delay_bars=scenario.execution_delay_bars,
                average_excess_return=average_excess_return,
                beat_fold_ratio=beat_fold_ratio,
                passed=(
                    beat_fold_ratio >= walk_forward.minimum_positive_fold_ratio
                    and average_excess_return > 0
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
    return ResearchReport(
        dataset=dataset_manifest(bars),
        config_hash=evaluation_config_hash(app_config, walk_forward, strategy_id),
        git_sha=git_sha or current_git_sha(),
        random_seed=random_seed,
        scenarios=scenarios,
        benchmark_comparisons=tuple(comparisons),
        qualified=not reasons,
        qualification_reasons=tuple(reasons),
    )


class ExperimentRegistry:
    """Append-only registry for reproducible research trials."""

    def __init__(self, path: Path | str) -> None:
        raw_path = str(path)
        Path(raw_path).parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(raw_path)
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS experiments (
                experiment_id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                dataset_hash TEXT NOT NULL,
                config_hash TEXT NOT NULL,
                git_sha TEXT NOT NULL,
                random_seed INTEGER NOT NULL,
                strategy_id TEXT NOT NULL,
                qualified INTEGER NOT NULL,
                report TEXT NOT NULL
            )
            """
        )
        self._connection.commit()

    def record(self, report: ResearchReport) -> int:
        cursor = self._connection.execute(
            """
            INSERT INTO experiments (
                created_at, dataset_hash, config_hash, git_sha, random_seed,
                strategy_id, qualified, report
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                datetime.now(UTC).isoformat(),
                report.dataset.dataset_hash,
                report.config_hash,
                report.git_sha,
                report.random_seed,
                report.scenarios[0].strategy_id,
                int(report.qualified),
                report.model_dump_json(),
            ),
        )
        self._connection.commit()
        if cursor.lastrowid is None:
            raise RuntimeError("experiment insert did not return an identifier")
        return cursor.lastrowid

    def count(self) -> int:
        row = self._connection.execute("SELECT COUNT(*) FROM experiments").fetchone()
        return int(row[0])

    def qualified_count(self) -> int:
        row = self._connection.execute(
            "SELECT COUNT(*) FROM experiments WHERE qualified = 1"
        ).fetchone()
        return int(row[0])

    def recent(self, *, limit: int = 20) -> list[dict[str, object]]:
        self._connection.row_factory = sqlite3.Row
        rows = self._connection.execute(
            """
            SELECT experiment_id, created_at, dataset_hash, config_hash, git_sha,
                   random_seed, strategy_id, qualified, report
            FROM experiments ORDER BY experiment_id DESC LIMIT ?
            """,
            (limit,),
        )
        return [
            {
                "experiment_id": int(row["experiment_id"]),
                "created_at": str(row["created_at"]),
                "dataset_hash": str(row["dataset_hash"]),
                "config_hash": str(row["config_hash"]),
                "git_sha": str(row["git_sha"]),
                "random_seed": int(row["random_seed"]),
                "strategy_id": str(row["strategy_id"]),
                "qualified": bool(row["qualified"]),
                "report": json.loads(row["report"]),
            }
            for row in rows
        ]

    def close(self) -> None:
        self._connection.close()

    def __enter__(self) -> ExperimentRegistry:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
