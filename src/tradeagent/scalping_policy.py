from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Mapping
from datetime import datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from tradeagent.scalping_config import ScalpingConfig
from tradeagent.scalping_economics import EconomicModel, EconomicSample, calibrate_model


def model_file_sha256(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()


def load_economic_model(config: ScalpingConfig) -> EconomicModel | None:
    if config.decision_policy == "legacy-v30":
        if config.economic_model_path is not None:
            raise ValueError("legacy policy cannot load an action-economics artifact")
        return None
    if config.economic_model_path is None or config.economic_model_sha256 is None:
        return None
    path = Path(config.economic_model_path)
    if not path.is_file():
        raise ValueError("configured economic model artifact does not exist")
    if model_file_sha256(path) != config.economic_model_sha256:
        raise ValueError("economic model file hash does not match the reviewed configuration")
    try:
        model = EconomicModel.model_validate_json(path.read_text(encoding="utf-8"))
    except (OSError, ValidationError, ValueError) as error:
        raise ValueError("economic model artifact is invalid") from error
    provenance = model.provenance
    failures = []
    if provenance.account_digest != config.account_digest:
        failures.append("account")
    if provenance.horizon_seconds != config.feature_horizon_seconds:
        failures.append("prediction horizon")
    if provenance.cancellation_horizon_seconds != config.entry_order_ttl_seconds:
        failures.append("cancellation horizon")
    if provenance.maker_fee_bps != float(config.maker_fee_bps):
        failures.append("maker fee")
    if provenance.taker_fee_bps != float(config.taker_fee_bps):
        failures.append("taker fee")
    if model.status == "validated" and not set(config.symbols).issubset(provenance.universe):
        failures.append("symbol universe")
    if failures:
        raise ValueError(
            "economic model provenance disagrees with configured " + ", ".join(failures)
        )
    return model


def calibrate_from_diagnostics(
    report: Mapping[str, Any],
    *,
    horizon_seconds: float,
    maker_fee_bps: float,
    taker_fee_bps: float,
    calibrated_at: datetime,
    valid_until: datetime,
    minimum_fit_samples: int = 4,
    minimum_validation_samples: int = 4,
    minimum_filled_samples: int = 2,
) -> tuple[EconomicModel, dict[str, Any]]:
    contract = report.get("calibration_contract")
    if not isinstance(contract, Mapping) or contract.get("schema") != "scalp-calibration-row-v1":
        raise ValueError("a scalp-calibration-row-v1 diagnostics report is required")
    account = report.get("account_digest")
    source = contract.get("source_sha256")
    if not isinstance(account, str) or not isinstance(source, str):
        raise ValueError("diagnostic calibration provenance is incomplete")
    selected = [
        row
        for row in report.get("calibration_samples", [])
        if isinstance(row, Mapping) and float(row.get("horizon_seconds", -1)) == horizon_seconds
    ]
    if len({str(row.get("cycle_id")) for row in selected}) != len(selected):
        raise ValueError("each candidate must appear exactly once at the selected horizon")
    groups: dict[tuple[str, str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in selected:
        sample = row.get("sample")
        if not isinstance(sample, Mapping):
            raise ValueError("calibration row is missing its raw sample envelope")
        groups[
            (str(sample.get("symbol")), str(sample.get("family")), str(sample.get("action")))
        ].append(row)
    accepted: list[EconomicSample] = []
    blocked = []
    for key, rows in sorted(groups.items()):
        reasons = Counter(
            reason
            for row in rows
            for reason in (
                *row.get("eligibility_reasons", []),
                *row.get("unpriced_cost_components", []),
            )
        )
        if reasons or not all(row.get("training_eligible") is True for row in rows):
            blocked.append(
                {
                    "symbol": key[0],
                    "family": key[1],
                    "action": key[2],
                    "candidate_count": len(rows),
                    "reason_counts": dict(sorted(reasons.items())),
                    "complete_group_required": True,
                }
            )
            continue
        try:
            accepted.extend(EconomicSample.model_validate(row["sample"]) for row in rows)
        except ValidationError as error:
            blocked.append(
                {
                    "symbol": key[0],
                    "family": key[1],
                    "action": key[2],
                    "candidate_count": len(rows),
                    "reason_counts": {"economic_sample_validation_failed": len(rows)},
                    "validation_errors": error.errors(include_url=False),
                    "complete_group_required": True,
                }
            )
    model = calibrate_model(
        accepted,
        account_digest=account,
        source_sha256=source,
        maker_fee_bps=maker_fee_bps,
        taker_fee_bps=taker_fee_bps,
        horizon_seconds=horizon_seconds,
        cancellation_horizon_seconds=float(
            selected[0]["sample"]["cancellation_horizon_seconds"]
            if selected
            else report.get("calibration_contract", {}).get("cancellation_horizon_seconds", 3)
        ),
        calibrated_at=calibrated_at,
        valid_until=valid_until,
        minimum_fit_samples=minimum_fit_samples,
        minimum_validation_samples=minimum_validation_samples,
        minimum_filled_samples=minimum_filled_samples,
    )
    audit = {
        "schema": "scalp-calibration-audit-v1",
        "account_digest": account,
        "source_sha256": source,
        "selected_horizon_seconds": horizon_seconds,
        "raw_candidate_count": len(selected),
        "accepted_sample_count": len(accepted),
        "blocked_groups": blocked,
        "model_id": model.model_id,
        "model_status": model.status,
        "model_reason_codes": list(model.reason_codes),
        "complete_group_policy": (
            "a partially observed action group is excluded in full; "
            "unsupported coverage remains NO_TRADE"
        ),
        "no_complete_case_cherry_picking": True,
        "no_counterfactual_aggressive_samples_created": True,
        "profitability_validated": model.status == "validated",
    }
    return model, audit


def write_model_artifact(model: EconomicModel, path: Path) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = model.canonical_json() + "\n"
    path.write_text(payload, encoding="utf-8", newline="\n")
    return model_file_sha256(path)
