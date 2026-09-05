from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import inspect

from tradeagent.persistence import (
    Database,
    ProductionRepository,
    normalize_database_url,
)


def test_production_schema_and_repository_contracts(tmp_path: Path) -> None:
    database = Database(f"sqlite:///{tmp_path / 'production.db'}")
    database.initialize()
    repository = ProductionRepository(database)
    now = datetime(2026, 1, 2, 15, tzinfo=UTC)

    event_id = repository.append_event(
        "worker_started",
        {"mode": "paper"},
        occurred_at=now,
        trace_id="startup-1",
    )
    repository.set_control("kill_switch", "active")
    repository.set_control("kill_switch", "inactive")
    repository.heartbeat(
        "worker",
        "instance-1",
        {"healthy": True},
        observed_at=now,
    )
    repository.heartbeat(
        "worker",
        "instance-1",
        {"healthy": True, "reconciled": True},
        observed_at=now,
    )

    assert event_id
    assert repository.event_count() == 1
    assert repository.get_control("kill_switch") == "inactive"
    assert repository.get_control("missing") is None
    tables = set(inspect(database.engine).get_table_names())
    assert {
        "events_v2",
        "controls_v2",
        "orders",
        "fills",
        "position_cycles",
        "experiments_v2",
        "heartbeats",
        "notification_outbox",
        "worker_locks",
    }.issubset(tables)
    database.dispose()


def test_worker_lock_allows_exactly_one_owner(tmp_path: Path) -> None:
    with Database(f"sqlite:///{tmp_path / 'locks.db'}") as database:
        database.initialize()
        repository = ProductionRepository(database)

        assert repository.acquire_worker_lock("paper-worker", "owner-1")
        assert not repository.acquire_worker_lock("paper-worker", "owner-2")
        assert not repository.release_worker_lock("paper-worker", "owner-2")
        assert repository.release_worker_lock("paper-worker", "owner-1")
        assert repository.acquire_worker_lock("paper-worker", "owner-2")


def test_render_postgres_url_uses_psycopg_driver() -> None:
    assert (
        normalize_database_url("postgresql://user:password@example.com/tradeagent")
        == "postgresql+psycopg://user:password@example.com/tradeagent"
    )
    database = Database("postgresql://user:password@example.com/tradeagent")

    assert database.engine.url.drivername == "postgresql+psycopg"
    database.dispose()
