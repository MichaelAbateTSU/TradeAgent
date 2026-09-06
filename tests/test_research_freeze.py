from __future__ import annotations

import json
from pathlib import Path


def test_v080_research_freeze_is_complete_and_holdouts_are_unopened() -> None:
    freeze = json.loads(Path("research/freezes/v0.8.0.json").read_text(encoding="utf-8"))

    assert freeze["status"] == "immutable"
    assert len(freeze["frozen_git_commit"]) == 40
    assert len(freeze["strategies"]) >= 13
    assert freeze["results"]["qualification"] == "no qualified strategy"
    assert not freeze["results"]["autonomous_orders_enabled"]
    assert all(not dataset["holdout_opened"] for dataset in freeze["datasets"].values())
    assert freeze["experiment_ledger"]["record_count"] == 6
