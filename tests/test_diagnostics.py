from __future__ import annotations

from decimal import Decimal

from tradeagent.config import AppConfig, BrokerConfig
from tradeagent.data import synthetic_bars
from tradeagent.diagnostics import diagnose_strategy
from tradeagent.portfolio_strategy import EqualWeightPortfolioStrategy
from tradeagent.universe import align_universe


def test_strategy_diagnostics_reconcile_gross_net_and_costs() -> None:
    dataset = align_universe({"SPY": list(synthetic_bars(symbol="SPY", count=10, seed=1))})
    config = AppConfig(
        broker=BrokerConfig(
            slippage_bps=Decimal("1"),
            spread_bps=Decimal("1"),
            commission_bps=Decimal("1"),
        )
    )

    report = diagnose_strategy(
        dataset.frames,
        config,
        EqualWeightPortfolioStrategy(Decimal("0.02")),
    )

    assert report.trades
    assert report.gross_pnl - report.execution_cost == report.net_pnl
    assert report.expectancy == report.net_pnl / len(report.trades)
    assert all(trade.mfe >= trade.mae for trade in report.trades)
