from __future__ import annotations

import os
import sqlite3
from typing import Any

import pytest
from prepared_cache_pg_fixture import verify_postgresql_prepared_cache
from sqlalchemy import create_engine, event, select
from sqlalchemy.pool import QueuePool

from tradeagent import persistence
from tradeagent.persistence import Database, _bound_prepared_statements


class TrackedConnection(sqlite3.Connection):
    prepared_max = 100
    prepare_threshold = 5


@pytest.mark.parametrize("pool_size", [None, 2])
def test_pg_hook_configures_new_and_replacement_connections_without_eager_checkout(
    monkeypatch: pytest.MonkeyPatch, pool_size: int | None
) -> None:
    connections = []
    arguments = []

    def connect() -> TrackedConnection:
        value = sqlite3.connect(":memory:", factory=TrackedConnection)
        connections.append(value)
        return value

    engine = create_engine(
        "sqlite://", creator=connect, poolclass=QueuePool, pool_size=1, max_overflow=0
    )
    monkeypatch.setattr(engine.dialect, "name", "postgresql")
    monkeypatch.setattr(engine.dialect, "driver", "psycopg")

    def factory(url: str, **kwargs: Any) -> Any:
        arguments.append((url, kwargs))
        return engine

    monkeypatch.setattr(persistence, "create_engine", factory)
    with Database("postgresql://localhost/not-contacted", pool_size=pool_size) as database:
        assert connections == []
        assert event.contains(database.engine, "connect", _bound_prepared_statements)
        with database.engine.connect() as connection:
            assert connections[0].prepared_max == 7
            assert connections[0].prepare_threshold == 5
            assert connection.connection.driver_connection is connections[0]
            connection.invalidate()
        with database.engine.connect() as connection:
            assert len(connections) == 2
            assert connections[1].prepared_max == 7
            assert connections[1].prepare_threshold == 5
            assert connection.scalar(select(1)) == 1
        expected = {"future": True, "pool_pre_ping": True}
        if pool_size is not None:
            expected.update(
                poolclass=QueuePool, pool_size=pool_size, max_overflow=0, pool_timeout=20
            )
        assert arguments == [("postgresql+psycopg://localhost/not-contacted", expected)]


@pytest.mark.parametrize("pool_size", [None, 1])
def test_sqlite_connections_and_pool_options_remain_unchanged(pool_size: int | None) -> None:
    with Database("sqlite:///:memory:", pool_size=pool_size) as database:
        assert not event.contains(database.engine, "connect", _bound_prepared_statements)
        with database.begin() as connection:
            assert not hasattr(connection.connection.driver_connection, "prepared_max")
            assert connection.scalar(select(1)) == 1


def test_other_postgresql_drivers_are_not_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = create_engine("sqlite://")
    monkeypatch.setattr(engine.dialect, "name", "postgresql")
    monkeypatch.setattr(engine.dialect, "driver", "psycopg2")
    monkeypatch.setattr(persistence, "create_engine", lambda *args, **kwargs: engine)
    with Database("postgresql+psycopg2://localhost/not-contacted") as database:
        assert not event.contains(database.engine, "connect", _bound_prepared_statements)


@pytest.mark.skipif(
    not os.getenv("TRADEAGENT_TEST_POSTGRES_URL"),
    reason="Requires an explicitly supplied PostgreSQL URL for a read-only probe",
)
def test_actual_pg_prepared_cache_bound_and_query_results() -> None:
    with Database(os.environ["TRADEAGENT_TEST_POSTGRES_URL"], pool_size=1) as database:
        result = verify_postgresql_prepared_cache(database.engine)
        assert result["verified"] and result["read_only"] and result["rolled_back"]
        assert result["observed_prepared_counts"][-1] == 8
