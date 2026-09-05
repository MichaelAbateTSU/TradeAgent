from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from tradeagent.persistence import Database, ProductionRepository
from tradeagent.scheduler import (
    HeartbeatStaleError,
    HeartbeatWatchdog,
    ReconciliationFailureError,
    ReconciliationScheduler,
)

NOW = datetime(2026, 9, 4, 15, tzinfo=UTC)


class Status:
    def __init__(self, healthy: bool) -> None:
        self.healthy = healthy


class FakeReconciler:
    def __init__(self, healthy: bool) -> None:
        self.healthy = healthy
        self.calls = 0

    def reconcile(self, *, observed_at: datetime) -> Status:
        self.calls += 1
        return Status(self.healthy)


def _repository(tmp_path: Path) -> tuple[Database, ProductionRepository]:
    database = Database(f"sqlite:///{tmp_path / 'scheduler.db'}")
    database.initialize()
    return database, ProductionRepository(database)


def test_scheduled_reconciliation_records_health(tmp_path: Path) -> None:
    database, repository = _repository(tmp_path)
    reconciler = FakeReconciler(True)
    scheduler = ReconciliationScheduler(
        repository,
        reconciler,
        interval_seconds=60,
        instance_id="reconciler-1",
        clock=lambda: NOW,
    )

    result = scheduler.run_once()

    assert result.healthy
    assert reconciler.calls == 1
    assert repository.event_count() == 1
    heartbeat = repository.latest_heartbeat("tradeagent-reconciler")
    assert heartbeat is not None
    assert heartbeat[0] == "reconciler-1"
    database.dispose()


def test_reconciliation_failure_activates_kill_switch(tmp_path: Path) -> None:
    database, repository = _repository(tmp_path)
    scheduler = ReconciliationScheduler(
        repository,
        FakeReconciler(False),
        interval_seconds=60,
        instance_id="reconciler-1",
        clock=lambda: NOW,
    )

    with pytest.raises(ReconciliationFailureError, match="failed"):
        scheduler.run_once()

    assert repository.get_control("kill_switch") == "active"
    database.dispose()


def test_heartbeat_watchdog_fails_closed_for_missing_and_stale(
    tmp_path: Path,
) -> None:
    database, repository = _repository(tmp_path)
    watchdog = HeartbeatWatchdog(
        repository,
        service_name="tradeagent-worker",
        maximum_age_seconds=60,
    )

    with pytest.raises(HeartbeatStaleError, match="not recorded"):
        watchdog.check(observed_at=NOW)
    repository.heartbeat(
        "tradeagent-worker",
        "worker-1",
        {"state": "running"},
        observed_at=NOW,
    )
    watchdog.check(observed_at=NOW + timedelta(seconds=30))
    with pytest.raises(HeartbeatStaleError, match="stale"):
        watchdog.check(observed_at=NOW + timedelta(seconds=61))

    assert repository.get_control("kill_switch") == "active"
    database.dispose()


def test_scheduler_loop_stops_after_signal(tmp_path: Path) -> None:
    database, repository = _repository(tmp_path)
    reconciler = FakeReconciler(True)
    scheduler = ReconciliationScheduler(
        repository,
        reconciler,
        interval_seconds=60,
        instance_id="reconciler-1",
        clock=lambda: NOW,
    )
    stop_event = asyncio.Event()
    stop_event.set()

    asyncio.run(scheduler.run(stop_event))

    assert reconciler.calls == 0
    database.dispose()
