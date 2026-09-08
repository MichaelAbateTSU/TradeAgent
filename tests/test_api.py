from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from tradeagent.api import create_app
from tradeagent.broker import PaperBroker
from tradeagent.config import BrokerConfig
from tradeagent.data import synthetic_bars
from tradeagent.ledger import SQLiteLedger
from tradeagent.persistence import Database, ProductionRepository


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
        "check": "process_liveness_only",
        "dependencies": "/ready",
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


def test_console_exposes_production_runtime_state(tmp_path: Path) -> None:
    production_url = f"sqlite:///{tmp_path / 'production.db'}"
    now = datetime(2026, 9, 4, 15, tzinfo=UTC)
    with Database(production_url) as database:
        database.initialize()
        repository = ProductionRepository(database)
        repository.heartbeat(
            "tradeagent-shadow-recorder",
            "worker-1",
            {"state": "running"},
            observed_at=now,
        )
        repository.append_event(
            "shadow_outcome",
            {"shadow_nav": "100001.25"},
            occurred_at=now,
            trace_id="shadow-1",
        )
    client = TestClient(
        create_app(
            ledger_path=tmp_path / "ledger.db",
            experiments_path=tmp_path / "experiments.db",
            production_database_url=production_url,
        )
    )

    runtime = client.get("/api/runtime").json()

    assert runtime["connected"]
    assert runtime["market_bars"] == 0
    assert runtime["market_trades"] == 0
    assert runtime["shadow_nav"] == "100001.25"
    assert runtime["worker_heartbeat"] is not None
    assert client.get("/api/news").json() == {
        "items": [],
        "feed_heartbeat": None,
    }


def test_production_statistics_are_singleflight_but_controls_and_roles_are_live(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import time
    from concurrent.futures import ThreadPoolExecutor
    from threading import Lock

    import tradeagent.api as api

    url = f"sqlite:///{tmp_path / 'sampled-counts.db'}"
    clock = [100.0]
    monkeypatch.setattr(api, "monotonic", lambda: clock[0])
    calls = 0
    lock = Lock()
    original = ProductionRepository.market_data_counts

    def count(repository: ProductionRepository) -> tuple[int, int, int]:
        nonlocal calls
        with lock:
            calls += 1
        time.sleep(0.03)
        return original(repository)

    monkeypatch.setattr(ProductionRepository, "market_data_counts", count)
    with Database(url) as database:
        database.initialize()
        repository = ProductionRepository(database)
        repository.append_event("test", {}, occurred_at=datetime.now(UTC), trace_id="one")
        repository.heartbeat(
            "tradeagent-event-worker",
            "event",
            {"cohort_id": "current"},
            observed_at=datetime.now(UTC),
        )
        repository.heartbeat(
            "tradeagent-shadow-recorder",
            "recorder",
            {"state": "healthy"},
            observed_at=datetime.now(UTC),
        )
        repository.set_control("kill_switch", "inactive")
        with TestClient(create_app(production_database_url=url)) as client:
            with ThreadPoolExecutor(max_workers=8) as pool:
                responses = list(pool.map(client.get, ["/api/runtime", "/api/status"] * 12))
            assert all(response.status_code == 200 for response in responses)
            assert calls == 1
            observed = responses[0].json()["statistics_as_of"]
            assert {response.json()["statistics_as_of"] for response in responses} == {observed}
            repository.append_event("test", {}, occurred_at=datetime.now(UTC), trace_id="two")
            repository.set_control("kill_switch", "active")
            repository.set_control("current:pause", "OPERATOR_PAUSE")
            repository.heartbeat(
                "tradeagent-shadow-recorder",
                "recorder",
                {"state": "degraded"},
                observed_at=datetime.now(UTC),
            )
            status = client.get("/api/status").json()
            assert status["event_count"] == 1
            assert status["kill_switch"] == "active"
            assert status["cohort_pause"] == "OPERATOR_PAUSE"
            runtime = client.get("/api/runtime").json()
            assert runtime["worker_status"]["reported"]["state"] == "degraded"
            assert runtime["statistics_as_of"] == observed
            assert calls == 1
            clock[0] += 61
            assert client.get("/api/status").json()["event_count"] == 2
            assert calls == 2


@pytest.mark.parametrize("missing_totals", [False, True])
def test_statistics_failure_is_not_cached_as_healthy_or_retried_by_every_request(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, missing_totals: bool
) -> None:
    from sqlalchemy.exc import OperationalError

    import tradeagent.api as api
    from tradeagent.persistence import MarketDataTotalsUnavailableError

    url = f"sqlite:///{tmp_path / 'failed-counts.db'}"
    with Database(url) as database:
        database.initialize()
    clock = [100.0]
    monkeypatch.setattr(api, "monotonic", lambda: clock[0])
    attempts = 0

    def fail(_: ProductionRepository) -> tuple[int, int, int]:
        nonlocal attempts
        attempts += 1
        if missing_totals:
            raise MarketDataTotalsUnavailableError("Required exact totals are missing")
        raise OperationalError("count", {}, RuntimeError("database unavailable"))

    monkeypatch.setattr(ProductionRepository, "market_data_counts", fail)
    with TestClient(create_app(production_database_url=url)) as client:
        assert client.get("/api/runtime").status_code == 503
        assert client.get("/api/status").status_code == 503
        assert attempts == 1
        assert client.get("/ready").status_code == 200
        clock[0] += 6
        assert client.get("/api/runtime").status_code == 503
        assert attempts == 2


@pytest.mark.parametrize("error_kind", ["incomplete", "oversized", "busy"])
def test_incomplete_production_report_is_explicitly_unavailable_and_releases_singleflight(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, error_kind: str
) -> None:
    import tradeagent.api as api
    from tradeagent.reporting_reads import (
        ReportBusyError,
        ReportingReadModelIncomplete,
        ReportPayloadTooLargeError,
    )

    url = f"sqlite:///{tmp_path / 'incomplete-report.db'}"
    with Database(url) as database:
        database.initialize()

    def incomplete(*args, **kwargs):
        if error_kind == "busy":
            raise ReportBusyError("missing-original")
        if error_kind == "oversized":
            raise ReportPayloadTooLargeError("missing-original")
        raise ReportingReadModelIncomplete("missing-original")

    monkeypatch.setattr(api, "session_report", incomplete)
    with TestClient(create_app(production_database_url=url)) as client:
        for _ in range(2):
            response = client.get("/api/event-session-report?cohort_id=fixture")
            assert response.status_code == (429 if error_kind == "busy" else 503)
            assert response.headers["Retry-After"] == ("5" if error_kind == "busy" else "60")
            assert "missing-original" in response.json()["detail"]


def test_runtime_never_substitutes_legacy_heartbeat_for_recorder(tmp_path: Path) -> None:
    production_url = f"sqlite:///{tmp_path / 'role-names.db'}"
    now = datetime.now(UTC)
    with Database(production_url) as database:
        database.initialize()
        repository = ProductionRepository(database)
        for legacy in ("tradeagent-worker", "tradeagent-market-feed"):
            repository.heartbeat(legacy, "obsolete", {"state": "healthy"}, observed_at=now)
        repository.heartbeat(
            "tradeagent-shadow-recorder",
            "current",
            {
                "state": "degraded",
                "healthy": False,
                "last_committed_event_at": now.isoformat(),
                "derived": {
                    "state": "awaiting_complete_frame",
                    "healthy": False,
                    "cumulative_evidence_complete": False,
                },
            },
            observed_at=now,
        )
        repository.heartbeat(
            "tradeagent-shadow-market-feed",
            "monitor",
            {
                "state": "stale",
                "freshness_basis": "durable_market_batch",
                "freshness_reason": "durable_batch_owner_mismatch",
                "durable_batch": {"event_id": "actual-batch", "owner_matches": False},
                "recorder_heartbeat_at": now.isoformat(),
                "recorder_lease_age_seconds": 2.0,
            },
            observed_at=now,
        )
    client = TestClient(create_app(production_database_url=production_url))
    runtime = client.get("/api/runtime").json()
    assert runtime["worker_status"]["instance_id"] == "current"
    assert runtime["worker_status"]["reported"]["healthy"] is False
    assert runtime["worker_status"]["reported"]["derived"]["healthy"] is False
    assert runtime["market_feed_status"]["reported"]["state"] == "stale"
    assert runtime["market_feed_status"]["reported"]["freshness_basis"] == "durable_market_batch"
    assert runtime["market_feed_status"]["reported"]["freshness_reason"] == (
        "durable_batch_owner_mismatch"
    )
    assert runtime["market_feed_status"]["reported"]["recorder_heartbeat_at"] == now.isoformat()
    assert runtime["market_feed_status"]["reported"]["recorder_lease_age_seconds"] == 2.0
    assert runtime["market_feed_status"]["reported"]["durable_batch"] == {
        "event_id": "actual-batch",
        "owner_matches": False,
    }
    ready = client.get("/ready").json()["operational_status"]["roles"]
    assert "tradeagent-shadow-market-feed" in ready
    assert "tradeagent-market-feed" not in ready
    assert "tradeagent-worker" not in ready
    page = client.get("/").text
    assert 'id="service-observations"' in page
    assert "Heartbeat freshness is not proof of market-data coverage or entry permission" in page


def test_readiness_ages_heartbeats_at_read_completion(tmp_path: Path, monkeypatch) -> None:
    from datetime import timedelta

    from tradeagent.api import _operational_status

    start = datetime(2026, 9, 8, 15, tzinfo=UTC)
    clocks = iter((start, start + timedelta(seconds=1)))

    class AdvancingClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return next(clocks)

    with Database(f"sqlite:///{tmp_path / 'readiness-clock.db'}") as database:
        database.initialize()
        ProductionRepository(database).heartbeat(
            "tradeagent-notifier",
            "current",
            {"state": "running"},
            observed_at=start + timedelta(seconds=0.5),
        )
        monkeypatch.setattr("tradeagent.api.datetime", AdvancingClock)
        current = _operational_status(database)["roles"]["tradeagent-notifier"]
        assert current["fresh"] is True
        assert current["age_seconds"] == 0.5
        fixed = _operational_status(database, start)["roles"]["tradeagent-notifier"]
        assert fixed["fresh"] is False


def test_concurrent_dashboard_sections_share_bounded_database_pool(tmp_path: Path) -> None:
    from concurrent.futures import ThreadPoolExecutor
    from threading import Lock

    from sqlalchemy import event

    url = f"sqlite:///{tmp_path / 'bounded-pool.db'}"
    with Database(url) as database:
        database.initialize()
    app = create_app(production_database_url=url)
    engine = app.state.production_database.engine
    connections = 0
    lock = Lock()

    @event.listens_for(engine, "connect")
    def connected(*_) -> None:
        nonlocal connections
        with lock:
            connections += 1

    with TestClient(app) as client, ThreadPoolExecutor(max_workers=12) as executor:
        paths = ["/ready", "/api/runtime", "/api/status"] * 12
        responses = list(executor.map(lambda path: client.get(path), paths))
        assert all(response.status_code == 200 for response in responses)
        assert 1 <= connections <= 2
        assert engine.pool.size() == 2
