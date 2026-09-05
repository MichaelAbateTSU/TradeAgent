from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from tradeagent.config import AppConfig
from tradeagent.data import synthetic_bars
from tradeagent.portfolio_research import (
    evaluate_portfolio_suite,
    evaluate_portfolio_walk_forward,
    portfolio_config_hash,
)
from tradeagent.portfolio_strategy import (
    CrossSectionalMomentumStrategy,
    EqualWeightPortfolioStrategy,
    PortfolioStrategyConfig,
)
from tradeagent.research import ExperimentRegistry, WalkForwardConfig
from tradeagent.universe import align_universe


def _dataset(frame_count: int = 100):
    return align_universe(
        {
            "SPY": list(synthetic_bars(symbol="SPY", count=frame_count, seed=1)),
            "QQQ": list(synthetic_bars(symbol="QQQ", count=frame_count, seed=2)),
            "TLT": list(synthetic_bars(symbol="TLT", count=frame_count, seed=3)),
        }
    )


def test_portfolio_walk_forward_and_suite_are_reproducible(tmp_path: Path) -> None:
    dataset = _dataset()
    app_config = AppConfig()
    strategy_config = PortfolioStrategyConfig(
        lookback_frames=3,
        top_n=2,
        gross_target=Decimal("0.04"),
    )
    walk_forward = WalkForwardConfig(
        training_bars=20,
        testing_bars=10,
        step_bars=10,
        embargo_bars=1,
        warmup_bars=4,
        bootstrap_samples=200,
    )

    def factory() -> CrossSectionalMomentumStrategy:
        return CrossSectionalMomentumStrategy(strategy_config)

    report = evaluate_portfolio_suite(
        dataset.frames,
        dataset.manifest,
        app_config,
        walk_forward,
        strategy_config,
        factory,
        lambda: EqualWeightPortfolioStrategy(strategy_config.gross_target),
        random_seed=9,
        git_sha="portfolio-test",
    )

    assert len(report.scenarios) == 6
    assert len(report.scenarios[0].folds) == 7
    assert len(report.benchmark_comparisons) == 6
    assert report.closed_trade_estimate >= 0
    assert report.minimum_closed_trades == 0
    assert report.git_sha == "portfolio-test"
    assert len(report.config_hash) == 64
    assert (
        portfolio_config_hash(
            app_config,
            walk_forward,
            strategy_config,
            factory().strategy_id,
        )
        == report.config_hash
    )
    with ExperimentRegistry(tmp_path / "experiments.db") as registry:
        experiment_id = registry.record_model(
            report,
            dataset_hash=report.dataset.dataset_hash,
            config_hash_value=report.config_hash,
            git_sha=report.git_sha,
            random_seed=report.random_seed,
            strategy_id=report.scenarios[0].strategy_id,
            qualified=report.qualified,
        )
        assert experiment_id == 1
        assert registry.count() == 1


def test_portfolio_walk_forward_rejects_short_history() -> None:
    dataset = _dataset(frame_count=20)
    strategy_config = PortfolioStrategyConfig(
        lookback_frames=3,
        top_n=2,
        gross_target=Decimal("0.04"),
    )
    with pytest.raises(ValueError, match="requires at least"):
        evaluate_portfolio_walk_forward(
            dataset.frames,
            AppConfig(),
            WalkForwardConfig(
                training_bars=20,
                testing_bars=5,
                embargo_bars=0,
                warmup_bars=4,
            ),
            lambda: CrossSectionalMomentumStrategy(strategy_config),
            cost_multiplier=Decimal(1),
            execution_delay_frames=1,
        )
