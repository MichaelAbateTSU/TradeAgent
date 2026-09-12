from __future__ import annotations

import importlib
import zlib
from datetime import UTC, datetime, timedelta
from hashlib import sha256

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import event, func, inspect, select

from tradeagent.persistence import Database, ProductionRepository, event_reporting_metadata, events
from tradeagent.scalping_config import ScalpingConfig
from tradeagent.scalping_execution import ScalpOrderEngine
from tradeagent.scalping_store import (
    SCALPING_TABLES,
    ScalpStore,
    canonical,
    scalping_market_batches,
    scalping_runs,
)

NOW = datetime(2026, 9, 11, 7, tzinfo=UTC)


def config():
    return ScalpingConfig(cohort_id="store", account_digest="a" * 64, approved_at=NOW)


def test_constructors_never_create_schema(tmp_path):
    with Database(f"sqlite:///{tmp_path / 'empty.db'}") as database:
        statements = []
        event.listen(
            database.engine,
            "before_cursor_execute",
            lambda conn, cursor, statement, parameters, context, executemany: statements.append(
                statement
            ),
        )
        ScalpStore(database)
        ScalpOrderEngine(database, object(), config(), owner_id="fixture", code_sha="b" * 40)
        assert statements == []


def test_market_batches_are_deterministic_append_only_compressed_and_audited(tmp_path):
    with Database(f"sqlite:///{tmp_path / 'batches.db'}") as database:
        database.initialize()
        store = ScalpStore(database)
        batch = [{"type": "quote", "exchange_time_ns": 1789110000000000001, "bid": "100"}]
        store.persist_market_batch(batch, at=NOW)
        store.persist_market_batch(batch, at=NOW + timedelta(seconds=1))
        with database.begin() as connection:
            row = connection.execute(select(scalping_market_batches)).mappings().one()
            assert zlib.decompress(row["raw"]).decode() == canonical(batch)
            assert row["batch_id"] == sha256(canonical(batch).encode()).hexdigest()
            assert row["recorded_at"].replace(tzinfo=UTC) == NOW
            assert connection.scalar(select(func.count()).select_from(events)) == 1
            assert (
                connection.scalar(select(func.count()).select_from(event_reporting_metadata)) == 1
            )


def test_run_freezes_config_plus_code_and_does_not_overwrite_old_runs(tmp_path):
    with Database(f"sqlite:///{tmp_path / 'runs.db'}") as database:
        database.initialize()
        store = ScalpStore(database)
        first = store.freeze_run(config(), "b" * 40, at=NOW)
        assert store.freeze_run(config(), "b" * 40, at=NOW + timedelta(seconds=1)) == first
        assert store.freeze_run(config(), "c" * 40, at=NOW + timedelta(seconds=2)) != first
        with database.begin() as connection:
            assert connection.scalar(select(func.count()).select_from(scalping_runs)) == 2


def test_market_batch_range_uses_canonical_exchange_at_ns_without_rewriting_history(tmp_path):
    with Database(f"sqlite:///{tmp_path / 'canonical-times.db'}") as database:
        database.initialize()
        store = ScalpStore(database)
        batch = [
            {"event_type": "quote", "exchange_at_ns": 1789110000000000001},
            {"event_type": "trade", "exchange_at_ns": 1789110000000000009},
        ]
        store.persist_market_batch(batch, at=NOW)
        with database.begin() as connection:
            row = connection.execute(select(scalping_market_batches)).mappings().one()
            assert row["first_exchange_ns"] == 1789110000000000001
            assert row["last_exchange_ns"] == 1789110000000000009
            assert zlib.decompress(row["raw"]).decode() == canonical(batch)


def test_audit_and_reporting_metadata_rollback_together(tmp_path):
    with Database(f"sqlite:///{tmp_path / 'rollback.db'}") as database:
        database.initialize()
        store = ScalpStore(database)
        with pytest.raises(RuntimeError), database.begin() as connection:
            store.audit("fixture", {"run_id": "test"}, at=NOW, connection=connection)
            raise RuntimeError("rollback")
        with database.begin() as connection:
            assert connection.scalar(select(func.count()).select_from(events)) == 0
            assert (
                connection.scalar(select(func.count()).select_from(event_reporting_metadata)) == 0
            )


def test_migration_0014_only_adds_or_drops_v30_tables(tmp_path):
    migration = importlib.import_module("migrations.versions.0014_scalping_runtime")
    assert migration.down_revision == "0013_trade_event_identity"
    with Database(f"sqlite:///{tmp_path / 'migration.db'}") as database:
        database.initialize()
        repo = ProductionRepository(database)
        repo.set_control("kill_switch", "active")
        with database.begin() as connection:
            for table in reversed(SCALPING_TABLES):
                table.drop(connection)
            old_tables = set(inspect(connection).get_table_names())
            with Operations.context(MigrationContext.configure(connection)):
                migration.upgrade()
                migration.upgrade()
            assert set(inspect(connection).get_table_names()) == old_tables | {
                table.name for table in SCALPING_TABLES
            }
            with Operations.context(MigrationContext.configure(connection)):
                migration.downgrade()
            assert set(inspect(connection).get_table_names()) == old_tables
        assert repo.get_control("kill_switch") == "active"
