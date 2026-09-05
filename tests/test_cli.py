from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pytest

from tradeagent.cli import main


def test_backtest_command_prints_paper_report(capsys: object) -> None:
    main(
        [
            "backtest",
            "--bars",
            "30",
            "--fast-window",
            "2",
            "--slow-window",
            "3",
            "--seed",
            "11",
        ]
    )
    output = capsys.readouterr().out  # type: ignore[attr-defined]
    report = json.loads(output)

    assert report["mode"] == "paper"
    assert report["symbol"] == "SPY"
    assert Decimal(report["starting_equity"]) == Decimal("100000")


def test_paper_and_status_commands_share_audit_ledger(tmp_path: Path, capsys: object) -> None:
    database = tmp_path / "paper.db"
    main(
        [
            "paper",
            "--synthetic-bars",
            "80",
            "--database",
            str(database),
            "--seed",
            "13",
        ]
    )
    paper_output = json.loads(capsys.readouterr().out)  # type: ignore[attr-defined]
    main(["status", "--database", str(database), "--limit", "2"])
    status_output = json.loads(capsys.readouterr().out)  # type: ignore[attr-defined]

    assert paper_output["mode"] == "paper"
    assert status_output["event_count"] > 0
    assert len(status_output["events"]) == 2


def test_evaluate_command_records_cost_stressed_research(tmp_path: Path, capsys: object) -> None:
    database = tmp_path / "experiments.db"
    main(
        [
            "evaluate",
            "--bars",
            "90",
            "--fast-window",
            "2",
            "--slow-window",
            "3",
            "--training-bars",
            "20",
            "--testing-bars",
            "10",
            "--step-bars",
            "10",
            "--embargo-bars",
            "0",
            "--database",
            str(database),
        ]
    )
    output = json.loads(capsys.readouterr().out)  # type: ignore[attr-defined]

    assert output["experiment_id"] == 1
    assert len(output["report"]["scenarios"]) == 8
    assert database.exists()


def test_volatility_trend_evaluation_is_available(tmp_path: Path, capsys: object) -> None:
    main(
        [
            "evaluate",
            "--strategy",
            "volatility-trend",
            "--bars",
            "50",
            "--fast-window",
            "2",
            "--slow-window",
            "3",
            "--training-bars",
            "20",
            "--testing-bars",
            "10",
            "--step-bars",
            "10",
            "--embargo-bars",
            "0",
            "--database",
            str(tmp_path / "volatility.db"),
        ]
    )
    output = json.loads(capsys.readouterr().out)  # type: ignore[attr-defined]

    assert output["report"]["scenarios"][0]["strategy_id"] == "volatility-target-trend-v1"


def test_paper_command_resumes_without_replaying_orders(tmp_path: Path, capsys: object) -> None:
    database = tmp_path / "resume.db"
    common = ["paper", "--database", str(database), "--seed", "21"]
    main([*common, "--synthetic-bars", "60"])
    first = json.loads(capsys.readouterr().out)  # type: ignore[attr-defined]
    main([*common, "--synthetic-bars", "80"])
    resumed = json.loads(capsys.readouterr().out)  # type: ignore[attr-defined]
    main([*common, "--synthetic-bars", "80"])
    up_to_date = json.loads(capsys.readouterr().out)  # type: ignore[attr-defined]

    assert first["ended_at"] < resumed["started_at"]
    assert up_to_date["status"] == "up_to_date"
    assert up_to_date["account"]["equity"] == resumed["ending_equity"]


def test_kill_switch_requires_reconciliation_to_reset(tmp_path: Path, capsys: object) -> None:
    database = tmp_path / "controls.db"

    main(["kill-switch", "status", "--database", str(database)])
    assert json.loads(capsys.readouterr().out)["kill_switch"] == "inactive"  # type: ignore[attr-defined]
    main(["kill-switch", "activate", "--database", str(database)])
    assert json.loads(capsys.readouterr().out)["kill_switch"] == "active"  # type: ignore[attr-defined]
    with pytest.raises(ValueError, match="confirm-reconciled"):
        main(["kill-switch", "reset", "--database", str(database)])
    main(
        [
            "kill-switch",
            "reset",
            "--confirm-reconciled",
            "--database",
            str(database),
        ]
    )
    assert json.loads(capsys.readouterr().out)["kill_switch"] == "inactive"  # type: ignore[attr-defined]


def test_active_kill_switch_blocks_new_paper_exposure(tmp_path: Path, capsys: object) -> None:
    database = tmp_path / "blocked.db"
    main(["kill-switch", "activate", "--database", str(database)])
    capsys.readouterr()  # type: ignore[attr-defined]

    main(
        [
            "paper",
            "--synthetic-bars",
            "150",
            "--database",
            str(database),
            "--seed",
            "7",
        ]
    )
    report = json.loads(capsys.readouterr().out)  # type: ignore[attr-defined]

    assert report["fills"] == 0
    assert report["rejected_orders"] > 0
    assert report["final_positions"] == []


def test_paper_resume_rejects_configuration_drift(
    tmp_path: Path,
    capsys: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = tmp_path / "drift.db"
    command = [
        "paper",
        "--synthetic-bars",
        "60",
        "--database",
        str(database),
    ]
    main(command)
    capsys.readouterr()  # type: ignore[attr-defined]
    monkeypatch.setenv("TRADEAGENT_STRATEGY__EXECUTION_DELAY_BARS", "2")

    with pytest.raises(ValueError, match="configuration differs"):
        main(command)
