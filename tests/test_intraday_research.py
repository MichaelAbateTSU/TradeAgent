from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from tradeagent.config import AppConfig, IntradayConfig
from tradeagent.domain import MarketBar
from tradeagent.intraday_strategy import (
    IntradayEqualWeightBenchmark,
    IntradayStrategyConfig,
    OpeningRangeBreakoutStrategy,
)
from tradeagent.portfolio_research import evaluate_portfolio_suite
from tradeagent.research import WalkForwardConfig
from tradeagent.universe import UniverseFrame, UniverseManifest


def _frames(count: int = 70) -> tuple[UniverseFrame, ...]:
    start = datetime(2026, 9, 1, 13, 30, tzinfo=UTC)
    frames = []
    price = Decimal("100")
    for index in range(count):
        timestamp = start + timedelta(minutes=index * 5)
        price += Decimal("0.05") if index % 10 >= 6 else Decimal("0")
        bar = MarketBar(
            symbol="SPY",
            timestamp=timestamp,
            open=price,
            high=price + Decimal("0.1"),
            low=price - Decimal("0.1"),
            close=price,
            volume=Decimal("10000"),
        )
        frames.append(UniverseFrame(timestamp=timestamp, bars=(bar,)))
    return tuple(frames)


def test_intraday_suite_enforces_minimum_trade_evidence() -> None:
    frames = _frames()
    intraday = IntradayConfig(enabled=True)
    app_config = AppConfig(intraday=intraday)
    strategy_config = IntradayStrategyConfig(
        opening_range_minutes=30,
        breakout_buffer_bps=Decimal(0),
    )
    manifest = UniverseManifest(
        dataset_hash="a" * 64,
        symbols=("SPY",),
        frames=len(frames),
        rows=len(frames),
        started_at=frames[0].timestamp,
        ended_at=frames[-1].timestamp,
        dropped_rows={"SPY": 0},
    )
    report = evaluate_portfolio_suite(
        frames,
        manifest,
        app_config,
        WalkForwardConfig(
            training_bars=20,
            testing_bars=10,
            step_bars=10,
            embargo_bars=0,
            warmup_bars=10,
            bootstrap_samples=100,
        ),
        strategy_config,
        lambda: OpeningRangeBreakoutStrategy(strategy_config, intraday),
        lambda: IntradayEqualWeightBenchmark(
            strategy_config.target_weight,
            intraday,
        ),
        random_seed=1,
        minimum_closed_trades=200,
        minimum_deflated_sharpe_probability=Decimal("0.95"),
        maximum_backtest_overfitting_probability=Decimal("0.20"),
        number_of_trials=3,
        git_sha="intraday-test",
    )

    assert not report.qualified
    assert report.minimum_closed_trades == 200
    assert report.closed_trade_estimate < 200
    assert report.deflated_sharpe_probability is not None
    assert report.probability_backtest_overfitting is not None
    assert "INSUFFICIENT_CLOSED_TRADES" in report.qualification_reasons
