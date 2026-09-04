from __future__ import annotations

import json
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
    assert report["starting_equity"] == "100000"


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
