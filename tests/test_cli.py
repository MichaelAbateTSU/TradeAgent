from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

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
    assert len(output["report"]["scenarios"]) == 3
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
