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
from tradeagent.scalping_shadow import (
    ShadowActionOutcome,
    ShadowPolicy,
    outcome_sample,
    validate_passive_simulation,
)


def _digest(value: object) -> str:
    import json

    return sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


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


def calibrate_from_shadow_report(
    report: Mapping[str, Any],
    *,
    config: ScalpingConfig,
    calibrated_at: datetime,
    valid_until: datetime,
    maximum_incomplete_fraction: float = 0.05,
    minimum_fit_samples: int = 30,
    minimum_validation_samples: int = 15,
    minimum_filled_samples: int = 5,
    minimum_observation_days: float = 7,
) -> tuple[EconomicModel, dict[str, Any]]:
    if report.get("schema") not in {"shadow-action-report-v1", "shadow-action-report-v2"}:
        raise ValueError("a shadow-action-report-v1 or v2 payload is required")
    validation = report.get("passive_validation")
    outcomes = [ShadowActionOutcome.model_validate(row) for row in report.get("outcomes", [])]
    keys = [(row.candidate_id, row.action) for row in outcomes]
    if len(keys) != len(set(keys)):
        raise ValueError("duplicate candidate/action outcomes cannot calibrate a model")
    policy_ids = sorted({row.simulation_policy.identity for row in outcomes})
    symbols = sorted({row.symbol for row in outcomes if row.action == "PASSIVE_BUY"})
    passive_ids = sorted(
        row.candidate_id
        for row in outcomes
        if row.action == "PASSIVE_BUY"
        and row.actual_passive_comparable
        and row.actual_filled is not None
    )
    validation_sha = None
    if isinstance(validation, Mapping):
        raw_validation = dict(validation)
        supplied_sha = raw_validation.pop("validation_sha256", None)
        try:
            validation_policy = ShadowPolicy.model_validate(validation.get("policy"))
        except ValidationError:
            validation_policy = None
        schema = validation.get("schema")
        exact_v1_population = (
            schema == "shadow-execution-validation-v1"
            and validation.get("symbols") == symbols
            and validation.get("candidate_ids_sha256") == _digest(passive_ids)
        )
        raw_per_symbol = validation.get("per_symbol")
        passive_by_symbol = {
            symbol: {
                row.candidate_id
                for row in outcomes
                if row.symbol == symbol
                and row.action == "PASSIVE_BUY"
                and row.actual_passive_comparable
                and row.actual_filled is not None
            }
            for symbol in symbols
        }
        verified_v2_subset = False
        if schema == "shadow-execution-validation-v2" and isinstance(raw_per_symbol, Mapping):
            verified_v2_subset = (
                bool(symbols)
                and set(symbols).issubset(set(validation.get("symbols") or []))
                and all(
                    (
                        isinstance(entry := raw_per_symbol.get(symbol), Mapping)
                        and isinstance(candidate_ids := entry.get("candidate_ids"), list)
                        and all(isinstance(candidate_id, str) for candidate_id in candidate_ids)
                        and bool(passive_by_symbol[symbol])
                        and entry.get("passed") is True
                        and entry.get("candidate_ids_sha256") == _digest(sorted(candidate_ids))
                        and passive_by_symbol[symbol].issubset(set(candidate_ids))
                    )
                    for symbol in symbols
                )
            )
        validation_sha = (
            supplied_sha
            if supplied_sha == _digest(raw_validation)
            and validation_policy is not None
            and validation.get("policy_ids") == [validation_policy.identity]
            and policy_ids == [validation_policy.identity]
            and ((validation.get("passed") is True and exact_v1_population) or verified_v2_subset)
            else None
        )
        if report.get("schema") == "shadow-action-report-v2":
            # Execution ground truth and economic training are separate datasets.
            # Recompute the full validation proof instead of requiring training
            # candidates to be old broker orders or trusting a supplied pass flag.
            evidence = [
                ShadowActionOutcome.model_validate(row)
                for row in report.get("validation_outcomes", [])
            ]
            evidence_keys = [(row.candidate_id, row.action) for row in evidence]
            if len(evidence_keys) != len(set(evidence_keys)):
                raise ValueError("duplicate execution-validation outcomes")
            if set(keys).intersection(evidence_keys):
                raise ValueError("execution validation and economic training must be disjoint")
            recomputed = (
                validate_passive_simulation(evidence, validation_policy)
                if validation_policy is not None
                else {}
            )
            validation_sha = (
                supplied_sha
                if recomputed == dict(validation)
                and recomputed.get("passed") is True
                and validation_policy is not None
                and policy_ids == [validation_policy.identity]
                and recomputed.get("policy_ids") == policy_ids
                and set(symbols).issubset(set(recomputed.get("symbols", [])))
                and all(row.account_digest == config.account_digest for row in evidence)
                else None
            )
    groups: dict[tuple[str, str, str], list[ShadowActionOutcome]] = defaultdict(list)
    for outcome in outcomes:
        groups[(outcome.symbol, outcome.family, outcome.action)].append(outcome)
    samples: list[EconomicSample] = []
    blocked = []
    # Verify exactly the supplied envelope; optional fields added in later
    # schema readers must not invalidate immutable historical hashes.
    source_sha = _digest(report.get("outcomes", []))
    if report.get("source_sha256") != source_sha:
        raise ValueError("shadow report outcome hash does not match its contents")
    for key, rows in sorted(groups.items()):
        incomplete = sum(not row.complete for row in rows)
        fraction = incomplete / len(rows)
        observation_days = (
            max(row.decision_at for row in rows) - min(row.decision_at for row in rows)
        ).total_seconds() / 86400
        reasons = []
        if key[2] != "PASSIVE_BUY":
            reasons.append("AGGRESSIVE_EXECUTION_NOT_HISTORICALLY_VALIDATED")
        if validation_sha is None:
            reasons.append("PASSIVE_SIMULATION_NOT_VALIDATED")
        if fraction > maximum_incomplete_fraction:
            reasons.append("INCOMPLETE_OUTCOME_RATE_ABOVE_PREDECLARED_LIMIT")
        if observation_days < minimum_observation_days:
            reasons.append("INSUFFICIENT_CALENDAR_REGIME_COVERAGE")
        if reasons:
            blocked.append(
                {
                    "symbol": key[0],
                    "family": key[1],
                    "action": key[2],
                    "candidates": len(rows),
                    "incomplete": incomplete,
                    "incomplete_fraction": fraction,
                    "observation_days": observation_days,
                    "reasons": reasons,
                }
            )
            continue
        for row in rows:
            if row.complete:
                samples.append(
                    outcome_sample(
                        row,
                        source_sha256=source_sha,
                        execution_validation_sha256=validation_sha,
                        maker_fee_bps=float(config.maker_fee_bps),
                        taker_fee_bps=float(config.taker_fee_bps),
                    )
                )
    model = calibrate_model(
        samples,
        account_digest=config.account_digest,
        source_sha256=source_sha,
        maker_fee_bps=float(config.maker_fee_bps),
        taker_fee_bps=float(config.taker_fee_bps),
        horizon_seconds=config.feature_horizon_seconds,
        cancellation_horizon_seconds=config.entry_order_ttl_seconds,
        calibrated_at=calibrated_at,
        valid_until=valid_until,
        minimum_fit_samples=minimum_fit_samples,
        minimum_validation_samples=minimum_validation_samples,
        minimum_filled_samples=minimum_filled_samples,
    )
    return model, {
        "schema": "shadow-model-calibration-audit-v1",
        "model_id": model.model_id,
        "model_status": model.status,
        "model_reason_codes": list(model.reason_codes),
        "outcomes": len(outcomes),
        "accepted_samples": len(samples),
        "blocked_groups": blocked,
        "maximum_incomplete_fraction": maximum_incomplete_fraction,
        "minimum_observation_days": minimum_observation_days,
        "passive_validation_sha256": validation_sha,
        "aggressive_execution_supported": False,
        "no_complete_case_cherry_picking": True,
        "deployment_ready": model.status == "validated",
        "deployment_requires_reviewed_artifact": True,
    }
