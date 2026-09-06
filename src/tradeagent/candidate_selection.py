from __future__ import annotations

import json
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path

from pydantic import BaseModel, ConfigDict

from tradeagent.frozen_candidate import (
    EnsembleMember,
    FrozenCandidateManifest,
    write_frozen_candidate_manifest,
)
from tradeagent.lower_calibration import LowerCalibrationReport
from tradeagent.lower_turnover import (
    RELATIVE_STRENGTH_CONFIGS,
    TIME_SERIES_MOMENTUM_CONFIGS,
    RelativeStrengthRotationStrategy,
    TimeSeriesMomentumStrategy,
)

PLATEAU_INDICES = (7, 8, 9, 10)


class CandidateSelectionProtocol(BaseModel):
    model_config = ConfigDict(frozen=True)

    version: str
    created_at: datetime
    method: str
    time_series_plateau_indices: tuple[int, ...]
    relative_strength_plateau_indices: tuple[int, ...]
    retired_family: str
    retirement_reason: str
    unique_development_hypotheses: int
    corrected_reruns: int
    reruns_counted_as_hypotheses: bool
    external_data_present_at_selection: bool
    candidate_files: tuple[str, ...]


def freeze_two_candidates(
    calibration_path: Path,
    output_directory: Path,
    *,
    code_commit: str,
    development_dataset_hash: str,
    corrected_reruns: int,
    frozen_at: datetime | None = None,
    external_directory: Path = Path("data/v010/external"),
) -> CandidateSelectionProtocol:
    if external_directory.exists() and any(external_directory.rglob("*.csv")):
        raise RuntimeError("external data already exists; candidate freeze must happen first")
    calibration_bytes = calibration_path.read_bytes()
    calibration = LowerCalibrationReport.model_validate_json(calibration_bytes)
    calibration_sha = sha256(calibration_bytes).hexdigest()
    selected_at = frozen_at or datetime.now(UTC)
    manifests = (
        _manifest(
            calibration,
            family="multi-day-time-series-momentum",
            strategy_id="frozen-time-series-momentum-plateau-v1",
            member_indices=PLATEAU_INDICES,
            code_commit=code_commit,
            dataset_hash=development_dataset_hash,
            calibration_path=calibration_path,
            calibration_sha=calibration_sha,
            frozen_at=selected_at,
        ),
        _manifest(
            calibration,
            family="cross-sectional-relative-strength",
            strategy_id="frozen-relative-strength-plateau-v1",
            member_indices=PLATEAU_INDICES,
            code_commit=code_commit,
            dataset_hash=development_dataset_hash,
            calibration_path=calibration_path,
            calibration_sha=calibration_sha,
            frozen_at=selected_at,
        ),
    )
    output_directory.mkdir(parents=True, exist_ok=True)
    paths = (
        output_directory / "time-series-momentum-candidate.json",
        output_directory / "relative-strength-candidate.json",
    )
    for path, manifest in zip(paths, manifests, strict=True):
        write_frozen_candidate_manifest(path, manifest)
    protocol = CandidateSelectionProtocol(
        version="v0.10.0",
        created_at=selected_at,
        method=(
            "Equal-weight target ensemble of the four monthly medium/long-horizon "
            "parameter neighbors (indices 7-10); membership fixed by economic horizon "
            "and plateau adjacency, never by maximum historical P&L."
        ),
        time_series_plateau_indices=PLATEAU_INDICES,
        relative_strength_plateau_indices=PLATEAU_INDICES,
        retired_family="regime-conditioned-swing-mean-reversion",
        retirement_reason=(
            "Failed benchmark, execution-stress, trade-count, corrected DSR, and "
            "corrected family-PBO gates in development."
        ),
        unique_development_hypotheses=calibration.raw_hypotheses,
        corrected_reruns=corrected_reruns,
        reruns_counted_as_hypotheses=False,
        external_data_present_at_selection=False,
        candidate_files=tuple(path.as_posix() for path in paths),
    )
    protocol_path = output_directory / "selection-protocol.json"
    protocol_path.write_text(protocol.model_dump_json(indent=2) + "\n", encoding="utf-8")
    return protocol


def _manifest(
    calibration: LowerCalibrationReport,
    *,
    family: str,
    strategy_id: str,
    member_indices: tuple[int, ...],
    code_commit: str,
    dataset_hash: str,
    calibration_path: Path,
    calibration_sha: str,
    frozen_at: datetime,
) -> FrozenCandidateManifest:
    calibrated_family = next(item for item in calibration.families if item.family == family)
    members: list[EnsembleMember] = []
    if family == "multi-day-time-series-momentum":
        for index in member_indices:
            momentum_config = TIME_SERIES_MOMENTUM_CONFIGS[index - 1]
            members.append(
                EnsembleMember(
                    configuration_index=index,
                    strategy_id=TimeSeriesMomentumStrategy(momentum_config).strategy_id,
                    parameters=momentum_config.model_dump(mode="json"),
                )
            )
    else:
        for index in member_indices:
            relative_config = RELATIVE_STRENGTH_CONFIGS[index - 1]
            members.append(
                EnsembleMember(
                    configuration_index=index,
                    strategy_id=RelativeStrengthRotationStrategy(relative_config).strategy_id,
                    parameters=relative_config.model_dump(mode="json"),
                )
            )
    return FrozenCandidateManifest(
        version="v0.10.0",
        family=family,
        strategy_id=strategy_id,
        frozen_at=frozen_at,
        code_commit=code_commit,
        development_dataset_hash=dataset_hash,
        development_calibration_report=calibration_path.as_posix(),
        development_calibration_sha256=calibration_sha,
        selection_method=(
            "Indices 7-10 form the adjacent monthly-rebalance medium/long-horizon "
            "plateau. Equal-weight targets reduce dependence on one historical optimum."
        ),
        economic_rationale=(
            "Monthly turnover gives multi-day ETF movement more room to exceed observed "
            "spread, slippage, and regulatory costs; 126/252-day horizons capture "
            "persistent trends rather than short-lived bar patterns."
        ),
        members=tuple(members),
        raw_hypothesis_count=calibration.raw_hypotheses,
        effective_independent_trials=calibration.effective_independent_trials,
        family_pbo=calibrated_family.pbo,
        qualification_gates={
            "both_external_eras_net_return": "> 0",
            "both_external_eras_benchmark_relative_return": "> 0",
            "DSR": ">= 0.95",
            "family_PBO": "<= 0.20",
            "cost_stress": "positive through 3x",
            "trade_count": ">= 200 per era",
            "maximum_drawdown": ">= -0.15",
            "best_year_and_instrument_dependence": "must remain positive without either",
            "parameter_neighbor_positive_ratio": ">= 0.75",
        },
        external_eras=(
            "2016-01-01 through 2019-12-31",
            "2025-01-01 through 2026-09-04",
        ),
        external_data_acquired_before_freeze=False,
        existing_sealed_holdouts_used=False,
    )


def selection_protocol_hash(protocol: CandidateSelectionProtocol) -> str:
    return sha256(
        json.dumps(
            protocol.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
