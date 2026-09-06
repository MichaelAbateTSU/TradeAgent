from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

from tradeagent.lower_turnover import (
    RelativeStrengthConfig,
    RelativeStrengthRotationStrategy,
    TimeSeriesMomentumConfig,
    TimeSeriesMomentumStrategy,
)
from tradeagent.portfolio import PortfolioIntent, PortfolioStrategy
from tradeagent.universe import UniverseFrame


class EnsembleMember(BaseModel):
    model_config = ConfigDict(frozen=True)

    configuration_index: int = Field(ge=1, le=10)
    strategy_id: str
    parameters: dict[str, object]


class FrozenCandidateManifest(BaseModel):
    model_config = ConfigDict(frozen=True)

    version: str
    family: str
    strategy_id: str
    frozen_at: datetime
    code_commit: str
    development_dataset_hash: str
    development_calibration_report: str
    development_calibration_sha256: str
    selection_method: str
    economic_rationale: str
    members: tuple[EnsembleMember, ...]
    raw_hypothesis_count: int
    effective_independent_trials: Decimal
    family_pbo: Decimal
    qualification_gates: dict[str, str]
    external_eras: tuple[str, ...]
    external_data_acquired_before_freeze: bool
    existing_sealed_holdouts_used: bool


class TargetWeightEnsemble:
    def __init__(
        self,
        strategy_id: str,
        members: Sequence[PortfolioStrategy],
    ) -> None:
        if len(members) < 2:
            raise ValueError("a frozen ensemble requires at least two members")
        self._strategy_id = strategy_id
        self._members = tuple(members)

    @property
    def strategy_id(self) -> str:
        return self._strategy_id

    def on_frame(self, frame: UniverseFrame) -> PortfolioIntent:
        intents = [member.on_frame(frame) for member in self._members]
        return PortfolioIntent(
            strategy_id=self.strategy_id,
            timestamp=frame.timestamp,
            target_weights={
                bar.symbol: sum(
                    (
                        intent.target_weights.get(bar.symbol, Decimal(0))
                        for intent in intents
                        if intent is not None
                    ),
                    Decimal(0),
                )
                / Decimal(len(self._members))
                for bar in frame.bars
            },
            rationale=(
                "equal-weight target ensemble across preregistered stable parameter neighbors"
            ),
        )


def write_frozen_candidate_manifest(
    path: Path,
    manifest: FrozenCandidateManifest,
) -> None:
    if path.exists():
        raise FileExistsError(f"frozen candidate manifest already exists: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".partial")
    temporary.write_text(manifest.model_dump_json(indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def strategy_from_manifest(
    manifest: FrozenCandidateManifest,
) -> TargetWeightEnsemble:
    members: list[PortfolioStrategy] = []
    for member in manifest.members:
        strategy: PortfolioStrategy
        if manifest.family == "multi-day-time-series-momentum":
            strategy = TimeSeriesMomentumStrategy(
                TimeSeriesMomentumConfig.model_validate(member.parameters)
            )
        elif manifest.family == "cross-sectional-relative-strength":
            strategy = RelativeStrengthRotationStrategy(
                RelativeStrengthConfig.model_validate(member.parameters)
            )
        else:
            raise ValueError(f"retired or unsupported candidate family: {manifest.family}")
        if strategy.strategy_id != member.strategy_id:
            raise ValueError("candidate member parameters do not reproduce strategy ID")
        members.append(strategy)
    return TargetWeightEnsemble(manifest.strategy_id, members)
