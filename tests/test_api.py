from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from fastapi.testclient import TestClient

from tradeagent.api import create_app
from tradeagent.broker import PaperBroker
from tradeagent.config import BrokerConfig
from tradeagent.data import synthetic_bars
from tradeagent.ledger import SQLiteLedger


def test_read_only_console_exposes_health_status_events_and_metrics(
    tmp_path: Path,
) -> None:
    ledger_path = tmp_path / "ledger.db"
    experiments_path = tmp_path / "experiments.db"
    with SQLiteLedger(ledger_path) as ledger:
        ledger.append(
            "health",
            {"status": "ok"},
            occurred_at=datetime(2025, 1, 1, tzinfo=UTC),
            trace_id="health-1",
        )
    client = TestClient(
        create_app(
            ledger_path=ledger_path,
            experiments_path=experiments_path,
        )
    )

    assert client.get("/health").json() == {
        "status": "ok",
        "mode": "paper",
        "live_trading_available": False,
    }
    status = client.get("/api/status").json()
    assert status["event_count"] == 1
    assert status["event_counts"] == {"health": 1}
    assert status["kill_switch"] == "inactive"
    assert status["account"] is None
    assert client.get("/api/events?limit=1").json()["items"][0]["event_type"] == "health"
    experiments = client.get("/api/experiments").json()
    assert experiments == {"total": 0, "qualified_total": 0, "items": []}
    metrics = client.get("/metrics").text
    assert 'tradeagent_events_total{event_type="health"} 1' in metrics
    assert "tradeagent_experiments_total 0" in metrics


def test_dashboard_is_paper_only(tmp_path: Path) -> None:
    client = TestClient(
        create_app(
            ledger_path=tmp_path / "ledger.db",
            experiments_path=tmp_path / "experiments.db",
        )
    )

    response = client.get("/")

    assert response.status_code == 200
    assert "PAPER ONLY" in response.text
    assert "No live broker is connected" in response.text


def test_console_exposes_latest_paper_account(tmp_path: Path) -> None:
    ledger_path = tmp_path / "ledger.db"
    bar = next(synthetic_bars(count=1))
    broker = PaperBroker(BrokerConfig())
    broker.mark(bar)
    with SQLiteLedger(ledger_path) as ledger:
        ledger.append(
            "broker_checkpoint",
            broker.export_state(),
            occurred_at=bar.timestamp,
            trace_id="checkpoint-1",
        )
    client = TestClient(
        create_app(
            ledger_path=ledger_path,
            experiments_path=tmp_path / "experiments.db",
        )
    )

    status = client.get("/api/status").json()
    metrics = client.get("/metrics").text

    assert status["account"]["equity"] == "100000.0000"
    assert Decimal(status["account"]["gross_exposure_ratio"]) == 0
    assert "tradeagent_nav 100000.0000" in metrics
