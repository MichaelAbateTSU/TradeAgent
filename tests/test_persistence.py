from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
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


def test_stale_worker_lock_is_recovered(tmp_path: Path) -> None:
    with Database(f"sqlite:///{tmp_path / 'stale-lock.db'}") as database:
        database.initialize()
        repository = ProductionRepository(database)
        first = datetime(2026, 1, 1, tzinfo=UTC)

        assert repository.acquire_worker_lock(
            "paper-worker",
            "owner-1",
            observed_at=first,
        )
        assert repository.acquire_worker_lock(
            "paper-worker",
            "owner-2",
            stale_after_seconds=60,
            observed_at=first + timedelta(seconds=61),
        )
        assert repository.refresh_worker_lock(
            "paper-worker",
            "owner-2",
            observed_at=first + timedelta(seconds=62),
        )


def test_render_postgres_url_uses_psycopg_driver() -> None:
    assert (
        normalize_database_url("postgresql://user:password@example.com/tradeagent")
        == "postgresql+psycopg://user:password@example.com/tradeagent"
    )
    database = Database("postgresql://user:password@example.com/tradeagent")

    assert database.engine.url.drivername == "postgresql+psycopg"
    database.dispose()


def test_normalized_market_data_is_idempotent(tmp_path: Path) -> None:
    with Database(f"sqlite:///{tmp_path / 'market.db'}") as database:
        database.initialize()
        repository = ProductionRepository(database)
        now = datetime(2026, 9, 4, 15, tzinfo=UTC)

        first_bar = repository.store_market_bar(
            symbol="SPY",
            timeframe="1Min",
            event_at=now,
            received_at=now,
            open_price=Decimal("100"),
            high_price=Decimal("101"),
            low_price=Decimal("99"),
            close_price=Decimal("100.5"),
            volume=Decimal("1000"),
        )
        duplicate_bar = repository.store_market_bar(
            symbol="SPY",
            timeframe="1Min",
            event_at=now,
            received_at=now,
            open_price=Decimal("100"),
            high_price=Decimal("101"),
            low_price=Decimal("99"),
            close_price=Decimal("100.5"),
            volume=Decimal("1000"),
        )
        first_quote = repository.store_market_quote(
            symbol="SPY",
            event_at=now,
            received_at=now,
            bid_price=Decimal("100.4"),
            ask_price=Decimal("100.6"),
            bid_size=Decimal("10"),
            ask_size=Decimal("20"),
        )
        duplicate_quote = repository.store_market_quote(
            symbol="SPY",
            event_at=now,
            received_at=now,
            bid_price=Decimal("100.4"),
            ask_price=Decimal("100.6"),
            bid_size=Decimal("10"),
            ask_size=Decimal("20"),
        )

        assert first_bar and not duplicate_bar
        assert first_quote and not duplicate_quote
        assert repository.market_data_counts() == (1, 1)
