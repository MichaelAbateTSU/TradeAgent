import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import insert

from tradeagent.api import create_app
from tradeagent.daily_status import build_daily_status
from tradeagent.persistence import Database, ProductionRepository, controls
from tradeagent.scalping_reporting import request_scalping_stop, scalping_status


def setup_status(database: Database, now: datetime, *, owner: str = "owner") -> None:
    repo = ProductionRepository(database)
    repo.acquire_worker_lock("tradeagent-event-worker", owner, observed_at=now)
    repo.heartbeat(
        "tradeagent-event-worker",
        owner,
        {
            "state": "running",
            "mode": "paper",
            "cohort_id": "v30-test",
            "code_sha": "c" * 40,
            "config_hash": "d" * 64,
            "entry_policy": "v30-paper-unrestricted",
        },
        observed_at=now,
    )
    snapshot = {
        "state": "running",
        "profile": "v30-paper-unrestricted",
        "mode": "paper",
        "cohort_id": "v30-test",
        "code_sha": "c" * 40,
        "config_hash": "d" * 64,
        "owner_id": owner,
        "trade_summary": {"closed_round_trips": 3, "pending_fee_reconciliation": 1},
        "execution": {"pending_orders": 0, "account_digest": "a" * 64},
    }
    with database.begin() as connection:
        connection.execute(
            insert(controls).values(
                control_key="scalping:v30-test:status",
                control_value=json.dumps(snapshot),
                updated_at=now,
            )
        )
    repo.set_control("kill_switch", "active")


def test_missing_snapshot_is_not_a_flat_account_claim(tmp_path: Path) -> None:
    with Database(f"sqlite:///{tmp_path / 'missing.db'}") as database:
        database.initialize()
        result = scalping_status(database)
        assert result["state"] == "not_started" and result["active"] is False
        assert "not proof of no broker exposure" in result["message"]


def test_legacy_kill_does_not_relabel_unrestricted_paper_as_blocked(tmp_path: Path) -> None:
    now = datetime.now(UTC)
    with Database(f"sqlite:///{tmp_path / 'active.db'}") as database:
        database.initialize()
        setup_status(database, now)
        result = scalping_status(database, now=now)
        assert result["state"] == "running" and result["active"] is True
        assert result["live_execution_available"] is False
        assert result["qualification_eligible"] is False
        stale = scalping_status(database, now=now + timedelta(seconds=31))
        assert stale["state"] == "stale_or_unowned" and stale["active"] is False


def test_stale_or_replaced_owner_is_not_reported_as_running(tmp_path: Path) -> None:
    now = datetime.now(UTC)
    with Database(f"sqlite:///{tmp_path / 'owner.db'}") as database:
        database.initialize()
        setup_status(database, now)
        repo = ProductionRepository(database)
        repo.heartbeat(
            "tradeagent-event-worker",
            "someone-else",
            {
                "entry_policy": "v30-paper-unrestricted",
                "cohort_id": "v30-test",
                "code_sha": "c" * 40,
                "config_hash": "d" * 64,
            },
            observed_at=now,
        )
        assert scalping_status(database, now=now)["state"] == "stale_or_unowned"


def test_operator_stop_is_scoped_and_leaves_recovery_and_old_controls(tmp_path: Path) -> None:
    now = datetime.now(UTC)
    with Database(f"sqlite:///{tmp_path / 'stop.db'}") as database:
        database.initialize()
        setup_status(database, now)
        result = request_scalping_stop(database, "v30-test", "Owner requested a stop")
        assert result["owned_recovery_continues"] is True
        repo = ProductionRepository(database)
        assert repo.get_control("kill_switch") == "active"
        assert json.loads(repo.get_control("scalping:v30-test:stop") or "{}") == result
        with pytest.raises(ValueError, match="unknown"):
            request_scalping_stop(database, "missing", "Stop")


def test_daily_email_uses_scalping_not_retired_equipment_policy(tmp_path: Path) -> None:
    now = datetime.now(UTC)
    with Database(f"sqlite:///{tmp_path / 'daily.db'}") as database:
        database.initialize()
        setup_status(database, now)
        report = build_daily_status(database, now, "America/New_York")
        assert report["profile"] == "v30-paper-unrestricted"
        assert report["cohort_id"] == "v30-test"
        assert len(report["text"].split("\n\n")) == 5
        assert report["completed_round_trips"] == 0
        assert "pending_fee_reconciliation" not in report["text"]
        assert "No completed buy-and-sell trades were recorded" in report["text"]
        assert "paper-preflight" not in report["text"]


def test_api_and_dashboard_expose_separate_v30_state(tmp_path: Path) -> None:
    now = datetime.now(UTC)
    url = f"sqlite:///{tmp_path / 'api.db'}"
    with Database(url) as database:
        database.initialize()
        setup_status(database, now)
    app = create_app(
        ledger_path=tmp_path / "local.db",
        experiments_path=tmp_path / "experiments.db",
        production_database_url=url,
    )
    with TestClient(app) as client:
        response = client.get("/api/scalping")
        assert response.status_code == 200
        assert response.json()["profile"] == "v30-paper-unrestricted"
        assert response.json()["active"] is True
        legacy = client.get("/api/event-product")
        assert legacy.status_code == 200
        assert legacy.json()["entry_policy"] == "v30-paper-unrestricted"
        assert legacy.json()["legacy_event_cohorts_retained"] is True
        page = client.get("/").text
        assert 'id="scalping-heading"' in page
        assert "Legacy strategy kill (not v30 policy)" in page
        assert client.get("/api/scalping/diagnostics").json()["records"] == []
        assert client.get("/api/scalping/diagnostics?limit=101").status_code == 422


def test_diagnostic_journal_selects_latest_revision_without_mixing_other_runs(tmp_path):
    from test_daily_email_summary import add_cycle

    from tradeagent.scalping_config import ScalpingConfig
    from tradeagent.scalping_reporting import scalping_diagnostic_journal
    from tradeagent.scalping_store import ScalpStore

    now = datetime.now(UTC)
    with Database(f"sqlite:///{tmp_path / 'journal.db'}") as database:
        database.initialize()
        setup_status(database, now)
        store = ScalpStore(database)
        config = ScalpingConfig(cohort_id="v30-test", account_digest="a" * 64, approved_at=now)
        run = store.freeze_run(config, "c" * 40, at=now)
        cycle = add_cycle(database, run, now - timedelta(seconds=20))
        other = store.freeze_run(
            config.model_copy(update={"cohort_id": "unrelated"}), "b" * 40, at=now
        )
        other_cycle = add_cycle(database, other, now - timedelta(seconds=10))
        for identity, when, revision in (
            (cycle, now - timedelta(seconds=5), "old"),
            (cycle, now, "new"),
            (other_cycle, now, "unrelated"),
            (cycle, now + timedelta(minutes=1), "future"),
        ):
            store.audit(
                "trade_diagnostics",
                {"cycle_id": identity, "report": {"revision": revision}},
                at=when,
            )
        result = scalping_diagnostic_journal(database)
        assert len(result["records"]) == 1
        assert result["records"][0]["payload"]["report"]["revision"] == "new"
        assert result["profitability_validated"] is False
        assert (
            scalping_diagnostic_journal(database, cohort_id="unrelated")["records"][0]["cycle_id"]
            == other_cycle
        )
