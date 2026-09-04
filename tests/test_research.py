from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from tradeagent.config import AppConfig, StrategyConfig
from tradeagent.data import synthetic_bars
from tradeagent.research import (
    ExperimentRegistry,
    WalkForwardConfig,
    bootstrap_mean_confidence_interval,
    config_hash,
    dataset_manifest,
    evaluate_research_suite,
    evaluate_walk_forward,
    evaluation_config_hash,
)
from tradeagent.strategy import ConstantWeightStrategy, SmaCrossoverStrategy


def test_dataset_and_configuration_hashes_are_reproducible() -> None:
    bars = list(synthetic_bars(count=30, seed=5))
    config = AppConfig()

    assert dataset_manifest(bars) == dataset_manifest(bars)
    assert (
        dataset_manifest(bars).dataset_hash
        != dataset_manifest(list(synthetic_bars(count=30, seed=6))).dataset_hash
    )
    assert len(config_hash(config)) == 64
    assert len(evaluation_config_hash(config, WalkForwardConfig(), "sma-crossover-v1")) == 64


def test_bootstrap_confidence_interval_is_deterministic_and_conservative() -> None:
    values = [Decimal("0.01"), Decimal("0.02"), Decimal("0.03")]

    first = bootstrap_mean_confidence_interval(
        values,
        samples=500,
        confidence_level=Decimal("0.95"),
        random_seed=17,
    )
    second = bootstrap_mean_confidence_interval(
        values,
        samples=500,
        confidence_level=Decimal("0.95"),
        random_seed=17,
    )

    assert first == second
    assert first[0] > 0
    assert first[0] <= sum(values) / len(values) <= first[1]


def test_walk_forward_runs_disjoint_test_folds() -> None:
    bars = list(synthetic_bars(count=90, seed=9))
    config = AppConfig(strategy=StrategyConfig(fast_window=2, slow_window=3))
    walk_forward = WalkForwardConfig(
        training_bars=20,
        testing_bars=10,
        step_bars=10,
        embargo_bars=2,
        warmup_bars=3,
    )
    report = evaluate_walk_forward(
        bars,
        config,
        walk_forward,
        lambda: SmaCrossoverStrategy(config.strategy),
    )

    assert len(report.folds) == 6
    assert report.cost_multiplier == Decimal(1)
    assert report.execution_delay_bars == 0
    assert all(fold.training_ended_at < fold.testing_started_at for fold in report.folds)


def test_research_suite_stresses_costs_and_records_trial(tmp_path: Path) -> None:
    bars = list(synthetic_bars(count=70, seed=3))
    config = AppConfig()
    walk_forward = WalkForwardConfig(
        training_bars=20,
        testing_bars=10,
        step_bars=10,
        embargo_bars=0,
        warmup_bars=0,
    )
    report = evaluate_research_suite(
        bars,
        config,
        walk_forward,
        lambda: ConstantWeightStrategy("benchmark-v1", config.strategy.target_weight),
        random_seed=3,
        git_sha="abc123",
    )

    assert [scenario.cost_multiplier for scenario in report.scenarios] == [
        Decimal(1),
        Decimal(2),
        Decimal(3),
        Decimal(1),
        Decimal(2),
        Decimal(3),
    ]
    assert [scenario.execution_delay_bars for scenario in report.scenarios] == [
        1,
        1,
        1,
        2,
        2,
        2,
    ]
    assert len(report.benchmark_comparisons) == 6
    assert all(
        comparison.excess_return_ci_lower
        <= comparison.average_excess_return
        <= comparison.excess_return_ci_upper
        for comparison in report.benchmark_comparisons
    )
    assert all(
        comparison.benchmark_strategy_id == "buy-and-hold-v1"
        for comparison in report.benchmark_comparisons
    )
    assert not report.qualified
    assert "BENCHMARK_NOT_BEATEN" in report.qualification_reasons
    assert report.git_sha == "abc123"
    with ExperimentRegistry(tmp_path / "experiments.db") as registry:
        experiment_id = registry.record(report)
        assert experiment_id == 1
        assert registry.count() == 1
        assert not registry.is_strategy_qualified("benchmark-v1")
        assert not registry.is_strategy_qualified("missing-v1")


def test_walk_forward_rejects_short_dataset() -> None:
    bars = list(synthetic_bars(count=20))
    config = AppConfig()
    walk_forward = WalkForwardConfig(
        training_bars=20,
        testing_bars=5,
        embargo_bars=0,
        warmup_bars=0,
    )

    with pytest.raises(ValueError, match="requires at least"):
        evaluate_walk_forward(
            bars,
            config,
            walk_forward,
            lambda: ConstantWeightStrategy("cash-v1", Decimal(0)),
        )


def test_research_validates_empty_data_and_invalid_benchmarks() -> None:
    with pytest.raises(ValueError, match="at least one"):
        dataset_manifest([])
    with pytest.raises(ValueError, match="between zero and one"):
        ConstantWeightStrategy("invalid", Decimal("1.1"))
    with pytest.raises(ValueError, match="warmup_bars"):
        WalkForwardConfig(training_bars=20, warmup_bars=21)
    with pytest.raises(ValueError, match="at least one"):
        bootstrap_mean_confidence_interval(
            [],
            samples=100,
            confidence_level=Decimal("0.95"),
            random_seed=1,
        )
    with pytest.raises(ValueError, match="at least 100"):
        bootstrap_mean_confidence_interval(
            [Decimal(0)],
            samples=99,
            confidence_level=Decimal("0.95"),
            random_seed=1,
        )
    with pytest.raises(ValueError, match="between zero and one"):
        bootstrap_mean_confidence_interval(
            [Decimal(0)],
            samples=100,
            confidence_level=Decimal(1),
            random_seed=1,
        )
