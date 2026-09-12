import gzip
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from tradeagent.cli import main
from tradeagent.scalping_config import ScalpingConfig
from tradeagent.scalping_policy import (
    calibrate_from_diagnostics,
    load_economic_model,
    write_model_artifact,
)

NOW = datetime(2026, 9, 11, 22, tzinfo=UTC)
ACCOUNT = "a" * 64
SOURCE = "b" * 64


def config(path: Path | None = None, digest: str | None = None, **overrides):
    return ScalpingConfig(
        cohort_id="policy",
        account_digest=ACCOUNT,
        approved_at=NOW,
        symbols=("BTC/USD",),
        decision_policy="action-value-v1",
        catastrophic_stop_bps="100",
        economic_model_path=str(path) if path else None,
        economic_model_sha256=digest,
        **overrides,
    )


def report(*rows):
    return {
        "account_digest": ACCOUNT,
        "calibration_contract": {
            "schema": "scalp-calibration-row-v1",
            "source_sha256": SOURCE,
        },
        "calibration_samples": list(rows),
    }


def raw_row(index: int, *, eligible: bool, reasons=()):
    at = NOW - timedelta(minutes=10 - index)
    sample = {
        "sample_id": f"sample-{index}",
        "account_digest": ACCOUNT,
        "source_sha256": SOURCE,
        "symbol": "BTC/USD",
        "family": "momentum",
        "action": "PASSIVE_BUY",
        "decision_at": at.isoformat(),
        "feature_observed_at": at.isoformat(),
        "label_matured_at": (at + timedelta(seconds=6)).isoformat(),
        "return_end_at": (at + timedelta(seconds=5)).isoformat(),
        "horizon_seconds": 5.0,
        "cancellation_horizon_seconds": 3.0,
        "features": {"normalized_ofi_5s": 0.5},
        "quote_age_seconds": 0.1,
        "decision_latency_seconds": 0.1,
        "spread_bps": 2.0,
        "notional_usd": 100.0,
        "filled": True,
        "filled_fraction": 1.0,
        "fill_delay_seconds": 0.2,
        "gross_return_bps": 80.0,
        "directional_return_bps": 90.0,
        "return_basis": "mid_to_mid",
        "costs": {
            "fees": {"bps": 50.0, "kind": "estimated", "provenance": "configured"},
            "spread": {"bps": 2.0, "kind": "estimated", "provenance": "observed"},
            "slippage": {"bps": 1.0, "kind": "estimated", "provenance": "fixture"},
            "impact": {"bps": 1.0, "kind": "estimated", "provenance": "fixture"},
            "adverse_selection": {
                "bps": 9.0,
                "included_in_return": True,
                "kind": "embedded",
                "provenance": "fill-conditioned",
            },
        },
        "execution_evidence": "observed",
    }
    return {
        "cycle_id": f"cycle-{index}",
        "horizon_seconds": 5,
        "sample": sample,
        "training_eligible": eligible,
        "eligibility_reasons": list(reasons),
        "unpriced_cost_components": [],
    }


def test_incomplete_action_group_is_not_cherry_picked_into_a_profitable_model():
    rows = [raw_row(index, eligible=True) for index in range(8)]
    rows[-1] = raw_row(7, eligible=False, reasons=("missing:decision_latency_seconds",))
    model, audit = calibrate_from_diagnostics(
        report(*rows),
        horizon_seconds=5,
        maker_fee_bps=15,
        taker_fee_bps=25,
        calibrated_at=NOW,
        valid_until=NOW + timedelta(days=1),
    )
    assert model.status == "no_support"
    assert model.input_count == 0
    assert audit["accepted_sample_count"] == 0
    assert audit["raw_candidate_count"] == 8
    assert audit["blocked_groups"][0]["candidate_count"] == 8


def test_reviewed_model_file_is_hash_and_provenance_bound(tmp_path):
    rows = [raw_row(index, eligible=True) for index in range(12)]
    model, _ = calibrate_from_diagnostics(
        report(*rows),
        horizon_seconds=5,
        maker_fee_bps=15,
        taker_fee_bps=25,
        calibrated_at=NOW,
        valid_until=NOW + timedelta(days=1),
        minimum_fit_samples=2,
        minimum_validation_samples=2,
    )
    path = tmp_path / "model.json"
    digest = write_model_artifact(model, path)
    assert load_economic_model(config(path, digest)) == model
    path.write_text(json.dumps({"tampered": True}), encoding="utf-8")
    with pytest.raises(ValueError, match="hash"):
        load_economic_model(config(path, digest))


def test_missing_model_is_explicit_no_model_not_an_automatic_legacy_fallback():
    assert load_economic_model(config()) is None
    legacy = ScalpingConfig(
        cohort_id="legacy", account_digest=ACCOUNT, approved_at=NOW, symbols=("BTC/USD",)
    )
    assert load_economic_model(legacy) is None


@pytest.mark.parametrize("compressed", [False, True])
def test_cli_freezes_model_and_separate_calibration_audit(tmp_path, capsys, compressed):
    diagnostics = tmp_path / ("diagnostics.json.gz" if compressed else "diagnostics.json")
    model_path = tmp_path / f"model-{compressed}.json"
    audit_path = tmp_path / f"audit-{compressed}.json"
    payload = json.dumps(report(raw_row(0, eligible=False, reasons=("missing:latency",))))
    if compressed:
        diagnostics.write_bytes(gzip.compress(payload.encode(), mtime=0))
    else:
        diagnostics.write_text(payload, encoding="utf-8")
    main(
        [
            "scalp-calibrate",
            "--diagnostics",
            str(diagnostics),
            "--model-output",
            str(model_path),
            "--audit-output",
            str(audit_path),
            "--calibrated-at",
            NOW.isoformat(),
            "--valid-until",
            (NOW + timedelta(days=1)).isoformat(),
        ]
    )
    output = json.loads(capsys.readouterr().out)
    assert output["model_status"] == "no_support"
    assert output["raw_candidate_count"] == 1
    assert output["accepted_sample_count"] == 0
    assert model_path.exists() and audit_path.exists()
    assert load_economic_model(config(model_path, output["model_file_sha256"])).status == (
        "no_support"
    )


def test_replay_cli_requires_the_model_account_binding(tmp_path):
    events = tmp_path / "events.jsonl"
    events.write_text("", encoding="utf-8")
    with pytest.raises(ValueError, match="account digest"):
        main(
            [
                "scalp-replay",
                "--events",
                str(events),
                "--economic-model",
                str(tmp_path / "model.json"),
                "--model-sha256",
                "0" * 64,
            ]
        )
