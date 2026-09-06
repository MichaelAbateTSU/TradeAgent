from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from tradeagent.domain import MarketBar
from tradeagent.frozen_candidate import (
    EnsembleMember,
    FrozenCandidateManifest,
    TargetWeightEnsemble,
    strategy_from_manifest,
)
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


def test_frozen_manifest_reconstructs_exact_member_parameters() -> None:
    manifest = FrozenCandidateManifest(
        version="v0.10.0",
        family="multi-day-time-series-momentum",
        strategy_id="frozen-momentum-ensemble-v1",
        frozen_at=datetime(2026, 9, 6, tzinfo=UTC),
        code_commit="a" * 40,
        development_dataset_hash="b" * 64,
        development_calibration_report="report.json",
        development_calibration_sha256="c" * 64,
        selection_method="stable plateau",
        economic_rationale="diversify parameter timing",
        members=(
            EnsembleMember(
                configuration_index=4,
                strategy_id="time-series-momentum-126-21-5",
                parameters={
                    "lookback_days": 126,
                    "skip_days": 21,
                    "rebalance_days": 5,
                    "maximum_positions": 10,
                    "gross_target": "0.20",
                    "maximum_position_weight": "0.02",
                    "estimated_round_trip_cost_bps": "7",
                    "uncertainty_buffer_bps": "3",
                },
            ),
            EnsembleMember(
                configuration_index=8,
                strategy_id="time-series-momentum-126-21-21",
                parameters={
                    "lookback_days": 126,
                    "skip_days": 21,
                    "rebalance_days": 21,
                    "maximum_positions": 10,
                    "gross_target": "0.20",
                    "maximum_position_weight": "0.02",
                    "estimated_round_trip_cost_bps": "7",
                    "uncertainty_buffer_bps": "3",
                },
            ),
        ),
        raw_hypothesis_count=30,
        effective_independent_trials=Decimal("19.49"),
        family_pbo=Decimal("0.4"),
        qualification_gates={"DSR": ">=0.95"},
        external_eras=("pre-2020", "2025-latest"),
        external_data_acquired_before_freeze=False,
        existing_sealed_holdouts_used=False,
    )

    strategy = strategy_from_manifest(manifest)

    assert strategy.strategy_id == "frozen-momentum-ensemble-v1"
