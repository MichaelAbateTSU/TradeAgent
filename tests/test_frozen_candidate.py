from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from tradeagent.domain import MarketBar
from tradeagent.frozen_candidate import TargetWeightEnsemble
from tradeagent.portfolio import PortfolioIntent
from tradeagent.universe import UniverseFrame


class FixedMember:
    def __init__(self, strategy_id: str, weight: str) -> None:
        self.strategy_id = strategy_id
        self.weight = Decimal(weight)

    def on_frame(self, frame: UniverseFrame) -> PortfolioIntent:
        return PortfolioIntent(
            strategy_id=self.strategy_id,
            timestamp=frame.timestamp,
            target_weights={"SPY": self.weight},
            rationale="fixed ensemble member",
        )


def test_target_weight_ensemble_averages_preregistered_members() -> None:
    timestamp = datetime(2024, 1, 2, tzinfo=UTC)
    frame = UniverseFrame(
        timestamp=timestamp,
        bars=(
            MarketBar(
                symbol="SPY",
                timestamp=timestamp,
                open=Decimal("100"),
                high=Decimal("100"),
                low=Decimal("100"),
                close=Decimal("100"),
                volume=Decimal("1000"),
            ),
        ),
    )
    ensemble = TargetWeightEnsemble(
        "frozen",
        (FixedMember("first", "0.02"), FixedMember("second", "0")),
    )

    intent = ensemble.on_frame(frame)

    assert intent.target_weights == {"SPY": Decimal("0.01")}
