"""Read-only, reproducible evidence audit. No execution engine, broker or lease."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import select

from tradeagent.persistence import Database, events
from tradeagent.scalping_config import ScalpingConfig
from tradeagent.scalping_policy import calibrate_from_shadow_report, write_model_artifact
from tradeagent.scalping_shadow import (
    ShadowCandidate,
    ShadowPolicy,
    load_historical_candidates,
    shadow_report,
    simulate_database_candidates,
    validate_passive_simulation,
)
from tradeagent.scalping_store import scalping_runs


def audit_shadow_pipeline(
    database: Database,
    *,
    cohort_id: str,
    historical_start: datetime,
    historical_end: datetime,
    at: datetime,
    valid_until: datetime,
    output_dir: Path,
    maximum_candidate_bytes: int = 8 * 1024 * 1024,
) -> dict[str, Any]:
    if maximum_candidate_bytes < 1:
        raise ValueError("audit candidate byte budget must be positive")
    if any(
        value.utcoffset() is None for value in (historical_start, historical_end, at, valid_until)
    ):
        raise ValueError("audit timestamps must be timezone-aware")
    if not historical_start < historical_end <= at < valid_until:
        raise ValueError("audit windows must be chronological")
    # Refuse accidental evidence overwrite; every attempt retains its own folder.
    output_dir.mkdir(parents=True, exist_ok=False)
    with database.begin() as connection:
        runs = list(
            connection.execute(
                select(scalping_runs)
                .where(scalping_runs.c.cohort_id == cohort_id)
                .order_by(scalping_runs.c.created_at)
            ).mappings()
        )
        if not runs:
            raise ValueError("no frozen run exists for the requested cohort")
        if len({row["config_hash"] for row in runs}) != 1:
            raise ValueError("a calibration cohort must have one immutable configuration")
        config = ScalpingConfig.model_validate(runs[-1]["config"])
        candidates: list[ShadowCandidate] = []
        candidate_bytes = 0
        with connection.scalars(
            select(events.c.payload)
            .where(
                events.c.event_type == "scalp_shadow_action_candidate",
                events.c.payload["run_id"].as_string().in_([row["run_id"] for row in runs]),
                events.c.occurred_at <= at - timedelta(seconds=config.feature_horizon_seconds + 2),
            )
            .order_by(events.c.occurred_at, events.c.event_id)
            .limit(10001)
            .execution_options(stream_results=True, yield_per=1)
        ) as payloads:
            for payload in payloads:
                encoded = json.dumps(payload)
                candidate_bytes += len(encoded.encode("utf-8"))
                if candidate_bytes > maximum_candidate_bytes or len(candidates) >= 10000:
                    raise ValueError(
                        "cohort exceeds audit input budget; no truncated training is allowed. "
                        "Use an isolated audit process with a reviewed higher byte budget."
                    )
                candidates.append(ShadowCandidate.model_validate_json(encoded))
    historical_load = load_historical_candidates(
        database,
        account_digest=config.account_digest,
        start=historical_start,
        end=historical_end,
    )
    historical = historical_load.candidates
    policy = ShadowPolicy()
    validation_outcomes = simulate_database_candidates(database, historical, config, policy)
    validation = validate_passive_simulation(validation_outcomes, policy)
    outcomes = simulate_database_candidates(database, candidates, config, policy)
    report = shadow_report(
        outcomes,
        passive_validation=validation,
        validation_outcomes=validation_outcomes,
    )
    model, calibration = calibrate_from_shadow_report(
        report,
        config=config,
        calibrated_at=at,
        valid_until=valid_until,
    )
    model_path = output_dir / "model.json"
    model_sha = write_model_artifact(model, model_path)
    summary = {
        "schema": "scalping-pipeline-audit-v1",
        "cohort_id": cohort_id,
        "as_of": at.isoformat(),
        "simulation_policy": policy.model_dump(mode="json"),
        "simulation_policy_id": policy.identity,
        "historical_candidates": len(historical),
        "historical_candidate_exclusions": historical_load.exclusions,
        "current_candidates": len(candidates),
        "candidate_input_bytes": candidate_bytes,
        "maximum_candidate_bytes": maximum_candidate_bytes,
        "passive_validation": {
            key: value for key, value in validation.items() if key != "per_symbol"
        },
        "groups": report["groups"],
        "calibration": calibration,
        "model_path": str(model_path),
        "model_file_sha256": model_sha,
        "broker_called": False,
        "execution_lease_acquired": False,
        "database_modified": False,
        "model_deployed": False,
        "profitability_guaranteed": False,
    }
    for name, payload in (("report.json", report), ("summary.json", summary)):
        (output_dir / name).write_text(
            json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n",
            encoding="utf-8",
        )
    return summary
