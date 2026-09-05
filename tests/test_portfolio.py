from __future__ import annotations

from decimal import Decimal

import pytest

from tradeagent.broker import PaperBroker
from tradeagent.config import AppConfig
from tradeagent.data import synthetic_bars
from tradeagent.ledger import SQLiteLedger
from tradeagent.portfolio import (
    PortfolioEngine,
    PortfolioIntent,
)
from tradeagent.risk import RiskEngine
from tradeagent.universe import UniverseFrame, align_universe


class FixedPortfolioStrategy:
    strategy_id = "fixed-portfolio-v1"

    def on_frame(self, frame: UniverseFrame) -> PortfolioIntent:
        return PortfolioIntent(
            strategy_id=self.strategy_id,
            timestamp=frame.timestamp,
            target_weights={bar.symbol: Decimal("0.01") for bar in frame.bars},
            rationale="fixed test allocation",
        )


def _frames() -> tuple[UniverseFrame, ...]:
    return align_universe(
        {
            "SPY": list(synthetic_bars(symbol="SPY", count=50, seed=1)),
            "QQQ": list(synthetic_bars(symbol="QQQ", count=50, seed=2)),
        }
    ).frames


def test_portfolio_engine_runs_synchronized_multi_asset_accounting() -> None:
    config = AppConfig()
    with SQLiteLedger(":memory:") as ledger:
        report = PortfolioEngine(
            config,
            FixedPortfolioStrategy(),
            PaperBroker(config.broker),
            RiskEngine(config.risk),
            ledger,
        ).run(_frames())

        assert report.mode == "paper"
        assert report.symbols == ("QQQ", "SPY")
        assert report.fills > 0
        assert len(report.final_positions) == 2
        assert report.rejected_orders == 0
        assert ledger.latest_event("portfolio_progress") is not None
        assert ledger.latest_event("broker_checkpoint") is not None


def test_portfolio_engine_rejects_empty_and_changing_universe() -> None:
    config = AppConfig()
    with SQLiteLedger(":memory:") as ledger:
        engine = PortfolioEngine(
            config,
            FixedPortfolioStrategy(),
            PaperBroker(config.broker),
            RiskEngine(config.risk),
            ledger,
        )
        with pytest.raises(ValueError, match="at least one"):
            engine.run([])

    frames = _frames()
    changed = UniverseFrame(
        timestamp=frames[1].timestamp,
        bars=(frames[1].bar_for("SPY"),),
    )
    with SQLiteLedger(":memory:") as ledger:
        engine = PortfolioEngine(
            config,
            FixedPortfolioStrategy(),
            PaperBroker(config.broker),
            RiskEngine(config.risk),
            ledger,
        )
        with pytest.raises(ValueError, match="symbols changed"):
            engine.run([frames[0], changed])


def test_portfolio_intent_validates_weights() -> None:
    frame = _frames()[0]
    with pytest.raises(ValueError, match="between zero and one"):
        PortfolioIntent(
            strategy_id="invalid",
            timestamp=frame.timestamp,
            target_weights={"SPY": Decimal("-0.1")},
            rationale="invalid",
        )
    with pytest.raises(ValueError, match="cannot exceed one"):
        PortfolioIntent(
            strategy_id="invalid",
            timestamp=frame.timestamp,
            target_weights={"SPY": Decimal("0.6"), "QQQ": Decimal("0.6")},
            rationale="invalid",
        )
