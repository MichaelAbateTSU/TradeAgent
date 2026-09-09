from __future__ import annotations

import importlib.util
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import MetaData, UniqueConstraint, event, inspect, select, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql.dml import Insert

from tradeagent.alpaca_stream import MarketTrade, ReceivedStreamEvent
from tradeagent.persistence import (
    Database,
    ProductionRepository,
    event_reporting_metadata,
    events,
    market_bars,
    market_data_totals,
    market_quotes,
    market_trades,
    metadata,
)
from tradeagent.shadow_recorder import persist_shadow_batch


def identity_migration():
    path = Path("migrations") / "versions" / "0013_trade_event_identity.py"
    spec = importlib.util.spec_from_file_location("trade_identity_migration", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def legacy_trade_table():
    table = market_trades.to_metadata(MetaData())
    for constraint in list(table.constraints):
        if isinstance(constraint, UniqueConstraint):
            table.constraints.remove(constraint)
    table.append_constraint(
        UniqueConstraint(
            "symbol", "feed_source", "provider_trade_id", name="uq_market_trade_provider_id"
        )
    )
    return table


def trade(at, *, identity="4118", venue="V", price=Decimal(100)):
    return MarketTrade(
        symbol="QQQ",
        timestamp=at,
        trade_id=identity,
        exchange=venue,
        price=price,
        size=Decimal(5),
        conditions=("@",),
        tape="C",
    )


def write(repository, value, *, received_at=None, method="batch", batch_id=None):
    received_at = received_at or value.timestamp + timedelta(milliseconds=35)
    if method == "repository":
        return repository.store_market_trade(
            provider_trade_id=str(value.trade_id),
            symbol=value.symbol,
            event_at=value.timestamp,
            received_at=received_at,
            price=value.price,
            size=value.size,
            exchange=value.exchange,
            conditions=value.conditions,
            tape=value.tape,
        )
    return (
        persist_shadow_batch(
            repository,
            [ReceivedStreamEvent(value, received_at)],
            [],
            batch_id or str(uuid4()),
        ).inserted
        == 1
    )


@pytest.mark.parametrize("method", ["repository", "batch"])
def test_trade_identity_retains_cross_day_venue_and_event_time_without_rewriting_retry(
    tmp_path, method
):
    with Database(f"sqlite:///{tmp_path / 'identity.db'}") as database:
        database.initialize()
        repository = ProductionRepository(database)
        yesterday = datetime(2026, 9, 8, 16, 9, tzinfo=UTC)
        today = datetime(2026, 9, 9, 16, 49, tzinfo=UTC)
        old = trade(yesterday)
        assert write(repository, old, method=method)
        with database.begin() as connection:
            original = dict(connection.execute(select(market_trades)).mappings().one())
        assert write(repository, trade(today, price=Decimal(101)), method=method)
        assert write(repository, trade(today, venue="N", price=Decimal(102)), method=method)
        assert write(repository, trade(today + timedelta(microseconds=1)), method=method)
        assert not write(
            repository,
            trade(yesterday, price=Decimal(999)),
            received_at=today + timedelta(hours=1),
            method=method,
        )
        assert repository.market_data_counts() == (0, 0, 4)
        with database.begin() as connection:
            retained = dict(
                connection.execute(
                    select(market_trades).where(
                        market_trades.c.market_trade_id == original["market_trade_id"]
                    )
                )
                .mappings()
                .one()
            )
        assert retained == original


def test_microsecond_timestamp_does_not_collapse_distinct_trade_ids(tmp_path):
    with Database(f"sqlite:///{tmp_path / 'precision.db'}") as database:
        database.initialize()
        repository = ProductionRepository(database)
        first = datetime.fromisoformat("2026-09-09T16:49:00.123456100+00:00")
        second = datetime.fromisoformat("2026-09-09T16:49:00.123456999+00:00")
        assert (
            first == second
        )  # Existing storage precision; IDs/venue must remain part of identity.
        assert write(repository, trade(first, identity="4118"))
        assert write(repository, trade(second, identity="4119"))
        assert repository.market_data_counts() == (0, 0, 2)


def test_generic_trade_writer_uses_one_atomic_correct_identity_insert(tmp_path):
    with Database(f"sqlite:///{tmp_path / 'atomic.db'}") as database:
        database.initialize()
        statements = []
        event.listen(
            database.engine,
            "before_execute",
            lambda connection, statement, multiparams, params, options: statements.append(
                statement
            ),
        )
        repository = ProductionRepository(database)
        value = trade(datetime(2026, 9, 9, 16, 49, tzinfo=UTC))
        assert write(repository, value, method="repository")
        assert not write(repository, value, method="repository")
        assert len(statements) == 2 and all(isinstance(value, Insert) for value in statements)
        compiled = str(statements[0].compile(dialect=database.engine.dialect))
        assert (
            "ON CONFLICT (symbol, feed_source, provider_trade_id, exchange, event_at) DO NOTHING"
        ) in compiled
        assert "RETURNING market_trade_id" in compiled
    captured = []
    connection = SimpleNamespace(
        dialect=postgresql.dialect(),
        scalar=lambda statement: captured.append(statement) or "created-id",
    )
    repository = ProductionRepository(SimpleNamespace(begin=lambda: nullcontext(connection)))
    assert write(repository, value, method="repository")
    compiled = str(captured[0].compile(dialect=postgresql.dialect()))
    assert (
        "ON CONFLICT (symbol, feed_source, provider_trade_id, exchange, event_at) DO NOTHING"
    ) in compiled
    assert "RETURNING market_trades.market_trade_id" in compiled


def initialize_legacy(database):
    with database.begin() as connection:
        legacy_trade_table().create(connection)
        metadata.create_all(
            connection,
            tables=[
                market_bars,
                market_quotes,
                events,
                event_reporting_metadata,
                market_data_totals,
            ],
        )


def test_sqlite_upgrade_preserves_lost_batch_evidence_and_existing_indexes_triggers(tmp_path):
    migration = identity_migration()
    with Database(f"sqlite:///{tmp_path / 'legacy.db'}") as database:
        initialize_legacy(database)
        repository = ProductionRepository(database)
        old = trade(datetime(2026, 9, 8, 16, 9, tzinfo=UTC))
        current = trade(datetime(2026, 9, 9, 16, 49, tzinfo=UTC), price=Decimal(101))
        assert write(repository, old)
        lost_batch = str(uuid4())
        assert not write(repository, current, batch_id=lost_batch)
        with database.begin() as connection:
            before = dict(connection.execute(select(market_trades)).mappings().one())
            lost = connection.scalar(
                select(events.c.payload).where(events.c.event_id == lost_batch)
            )
            connection.exec_driver_sql(
                "CREATE INDEX retained_trade_expression ON market_trades(length(provider_trade_id))"
            )
            connection.exec_driver_sql(
                "CREATE TRIGGER retained_trade_trigger AFTER INSERT ON market_trades "
                "BEGIN SELECT 1; END"
            )
            operations = Operations(MigrationContext.configure(connection))
            migration.swap_sqlite_constraint(connection, operations)
            migration.swap_sqlite_constraint(connection, operations)
            names = set(
                connection.scalars(
                    text("SELECT name FROM sqlite_schema WHERE tbl_name='market_trades'")
                )
            )
            assert {
                "retained_trade_expression",
                "retained_trade_trigger",
                "ix_market_trades_symbol_time",
            } <= names
            assert dict(connection.execute(select(market_trades)).mappings().one()) == before
            assert (
                connection.scalar(select(events.c.payload).where(events.c.event_id == lost_batch))
                == lost
            )
        assert write(repository, current)
        assert repository.market_data_counts() == (0, 0, 2)
        with database.begin() as connection:
            after = [dict(row) for row in connection.execute(select(market_trades)).mappings()]
            operations = Operations(MigrationContext.configure(connection))
            with pytest.raises(RuntimeError, match="collapse distinct trades"):
                migration.swap_sqlite_constraint(connection, operations, downgrade=True)
            assert [
                dict(row) for row in connection.execute(select(market_trades)).mappings()
            ] == after
            constraints = inspect(connection).get_unique_constraints("market_trades")
            assert any(value["name"] == migration.NEW_NAME for value in constraints)
            assert not any(value["name"] == migration.OLD_NAME for value in constraints)


def test_sqlite_late_rebuild_failure_rolls_back_all_ddl_and_rows(tmp_path):
    migration = identity_migration()
    with Database(f"sqlite:///{tmp_path / 'ddl-rollback.db'}") as database:
        initialize_legacy(database)
        repository = ProductionRepository(database)
        assert write(repository, trade(datetime(2026, 9, 8, 16, 9, tzinfo=UTC)))

        def fail_rename(_connection, _cursor, statement, _params, _context, _many):
            if statement.startswith("ALTER TABLE _alembic_tmp_market_trades RENAME"):
                raise RuntimeError("injected late DDL failure")

        event.listen(database.engine, "before_cursor_execute", fail_rename)
        with database.begin() as connection:
            before = list(connection.execute(select(market_trades)))
            with pytest.raises(RuntimeError, match="late DDL"):
                migration.swap_sqlite_constraint(
                    connection, Operations(MigrationContext.configure(connection))
                )
            assert list(connection.execute(select(market_trades))) == before
            assert not inspect(connection).has_table("_alembic_tmp_market_trades")
            assert any(
                value["name"] == migration.OLD_NAME
                for value in inspect(connection).get_unique_constraints("market_trades")
            )
        event.remove(database.engine, "before_cursor_execute", fail_rename)


def test_sqlite_alembic_revision_up_down_and_rerun_are_persistent(tmp_path, monkeypatch):
    url = f"sqlite:///{tmp_path / 'alembic-identity.db'}"
    with Database(url) as database:
        initialize_legacy(database)
        assert write(ProductionRepository(database), trade(datetime(2026, 9, 8, 16, 9, tzinfo=UTC)))
    monkeypatch.setenv("TRADEAGENT_DATABASE_URL", url)
    config = Config("alembic.ini")
    command.stamp(config, "0012_market_data_totals")
    command.upgrade(config, "0013_trade_event_identity")
    command.upgrade(config, "0013_trade_event_identity")
    command.downgrade(config, "0012_market_data_totals")
    command.upgrade(config, "0013_trade_event_identity")
    with Database(url) as database, database.begin() as connection:
        assert len(connection.execute(select(market_trades)).all()) == 1
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == (
            "0013_trade_event_identity"
        )


def pg_index(columns, *, valid=True, ready=True, owned=False):
    return {
        "key_columns": list(columns),
        "unique": True,
        "valid": valid,
        "ready": ready,
        "owned": owned,
        "method": "btree",
        "plain": True,
        "full": True,
        "no_include": True,
        "default_order": True,
        "default_collations": True,
        "default_opclasses": True,
    }


class PgMigrationConnection:
    dialect = postgresql.dialect()

    def __init__(self, module):
        self.module = module
        self.connection = SimpleNamespace(driver_connection=SimpleNamespace(autocommit=True))
        self.statements = []
        self.constraints = {
            module.OLD_NAME: {
                "kind": "u",
                "deferred": False,
                "key_columns": list(module.OLD_COLUMNS),
            }
        }
        self.indexes = {module.OLD_NAME: pg_index(module.OLD_COLUMNS, owned=True)}
        self.settings = {"lock_timeout": "0", "statement_timeout": "0"}
        self.fail_create = False
        self.fail_swap = False

    def in_transaction(self):
        return not self.connection.driver_connection.autocommit

    def scalar(self, statement, parameters=None):
        value = str(statement)
        if value.startswith("SHOW "):
            return self.settings[value.removeprefix("SHOW ")]
        return None

    def execute(self, statement, parameters=None):
        assert "set_config" in str(statement)
        self.settings[parameters["name"]] = parameters["value"]

    def exec_driver_sql(self, statement):
        self.statements.append(statement)
        module = self.module
        if statement.startswith("DROP INDEX"):
            self.indexes.pop(statement.split(".")[-1].strip('"'))
        elif statement.startswith("CREATE UNIQUE INDEX"):
            name = module.BUILD_NEW if module.BUILD_NEW in statement else module.BUILD_OLD
            columns = module.NEW_COLUMNS if name == module.BUILD_NEW else module.OLD_COLUMNS
            self.indexes[name] = pg_index(columns, valid=not self.fail_create)
            if self.fail_create:
                raise IntegrityError("CREATE UNIQUE INDEX", {}, RuntimeError("duplicate identity"))
        elif statement.startswith("ALTER TABLE"):
            if self.fail_swap:
                raise RuntimeError("injected atomic ALTER failure")
            adding_new = f"ADD CONSTRAINT {module.NEW_NAME}" in statement
            source, target, columns, build = module._direction(not adding_new)
            if f"ADD CONSTRAINT {target}" in statement:
                value = self.indexes.pop(build)
                value["owned"] = True
                self.indexes[target] = value
                self.constraints[target] = {
                    "kind": "u",
                    "deferred": False,
                    "key_columns": list(columns),
                }
            if f"DROP CONSTRAINT {source}" in statement:
                self.constraints.pop(source)
                self.indexes.pop(source)


def pg_migration_fixture(monkeypatch):
    module = identity_migration()
    connection = PgMigrationConnection(module)
    monkeypatch.setattr(module, "_schema", lambda _connection: "identity_fixture")
    monkeypatch.setattr(module, "_constraints", lambda _connection, _schema: connection.constraints)
    monkeypatch.setattr(module, "_indexes", lambda _connection, _schema: connection.indexes)
    return module, connection


def test_pg_concurrent_build_is_resumable_and_swap_is_short_atomic_metadata_only(monkeypatch):
    module, connection = pg_migration_fixture(monkeypatch)
    with module.concurrent_index_limits(connection):
        assert connection.settings == {"lock_timeout": "2s", "statement_timeout": "300s"}
        assert module.prepare_postgresql_index(connection) == module.BUILD_NEW
    assert connection.settings == {"lock_timeout": "0", "statement_timeout": "0"}
    assert any(
        value.startswith("CREATE UNIQUE INDEX CONCURRENTLY") for value in connection.statements
    )
    assert module.OLD_NAME in connection.constraints
    created = len(connection.statements)
    assert module.prepare_postgresql_index(connection) == module.BUILD_NEW
    assert len(connection.statements) == created
    connection.connection.driver_connection.autocommit = False
    module.swap_postgresql_constraint(connection)
    alterations = [value for value in connection.statements if value.startswith("ALTER TABLE")]
    assert len(alterations) == 1
    assert f"ADD CONSTRAINT {module.NEW_NAME}" in alterations[0]
    assert f"DROP CONSTRAINT {module.OLD_NAME}" in alterations[0]
    assert "SET LOCAL lock_timeout = '2s'" in connection.statements
    assert "SET LOCAL statement_timeout = '10s'" in connection.statements
    assert any("ACCESS EXCLUSIVE MODE" in value for value in connection.statements)
    module.swap_postgresql_constraint(connection)
    assert sum(value.startswith("ALTER TABLE") for value in connection.statements) == 1
    assert not any(
        value.startswith(("UPDATE", "DELETE", "TRUNCATE")) for value in connection.statements
    )


def test_pg_failed_index_cleanup_and_invalid_resume_never_drop_original_constraint(monkeypatch):
    module, connection = pg_migration_fixture(monkeypatch)
    connection.fail_create = True
    with pytest.raises(IntegrityError), module.concurrent_index_limits(connection):
        module.prepare_postgresql_index(connection)
    assert module.BUILD_NEW not in connection.indexes
    assert module.OLD_NAME in connection.constraints
    assert connection.settings == {"lock_timeout": "0", "statement_timeout": "0"}
    connection.fail_create = False
    connection.indexes[module.BUILD_NEW] = pg_index(module.NEW_COLUMNS, valid=False)
    connection.statements.clear()
    assert module.prepare_postgresql_index(connection) == module.BUILD_NEW
    assert connection.statements[0].startswith("DROP INDEX CONCURRENTLY")
    assert connection.statements[1].startswith("CREATE UNIQUE INDEX CONCURRENTLY")


def test_pg_wrong_index_or_wrong_transaction_mode_fails_before_destructive_action(monkeypatch):
    module, connection = pg_migration_fixture(monkeypatch)
    connection.indexes[module.BUILD_NEW] = pg_index(module.OLD_COLUMNS)
    with pytest.raises(RuntimeError, match="reusable"):
        module.prepare_postgresql_index(connection)
    assert connection.statements == []
    connection.connection.driver_connection.autocommit = False
    with pytest.raises(RuntimeError, match="transaction mode"):
        module.prepare_postgresql_index(connection)
    connection.indexes.clear()
    with pytest.raises(RuntimeError, match="replacement index"):
        module.swap_postgresql_constraint(connection)
    assert module.OLD_NAME in connection.constraints
    assert not any(value.startswith("ALTER TABLE") for value in connection.statements)


@pytest.mark.parametrize("option", ["default_order", "default_collations", "default_opclasses"])
def test_pg_never_reuses_an_index_with_different_identity_comparison_semantics(monkeypatch, option):
    module, connection = pg_migration_fixture(monkeypatch)
    connection.indexes[module.BUILD_NEW] = {
        **pg_index(module.NEW_COLUMNS),
        option: False,
    }
    with pytest.raises(RuntimeError, match="reusable"):
        module.prepare_postgresql_index(connection)
    assert connection.statements == []
    assert module.OLD_NAME in connection.constraints


def test_pg_swap_failure_does_not_drop_old_uniqueness(monkeypatch):
    module, connection = pg_migration_fixture(monkeypatch)
    module.prepare_postgresql_index(connection)
    connection.connection.driver_connection.autocommit = False
    connection.fail_swap = True
    with pytest.raises(RuntimeError, match="atomic ALTER"):
        module.swap_postgresql_constraint(connection)
    assert module.OLD_NAME in connection.constraints
    assert module.NEW_NAME not in connection.constraints
    assert module.BUILD_NEW in connection.indexes


def test_pg_unsafe_downgrade_removes_only_failed_build_artifact(monkeypatch):
    module, connection = pg_migration_fixture(monkeypatch)
    connection.constraints = {
        module.NEW_NAME: {"kind": "u", "deferred": False, "key_columns": list(module.NEW_COLUMNS)}
    }
    connection.indexes = {module.NEW_NAME: pg_index(module.NEW_COLUMNS, owned=True)}
    connection.fail_create = True
    with pytest.raises(IntegrityError), module.concurrent_index_limits(connection):
        module.prepare_postgresql_index(connection, downgrade=True)
    assert set(connection.constraints) == {module.NEW_NAME}
    assert set(connection.indexes) == {module.NEW_NAME}
    assert not any(value.startswith("ALTER TABLE") for value in connection.statements)


def test_new_identity_later_page_failure_rolls_back_raw_and_counter_then_retries(tmp_path):
    with Database(f"sqlite:///{tmp_path / 'counter-rollback.db'}") as database:
        database.initialize()
        with database.begin() as connection:
            connection.execute(
                market_data_totals.insert(),
                [
                    {"table_name": name, "row_count": 0, "updated_at": datetime.now(UTC)}
                    for name in ("market_bars", "market_quotes", "market_trades")
                ],
            )
            connection.exec_driver_sql(
                "CREATE TRIGGER fixture_trade_total AFTER INSERT ON market_trades BEGIN "
                "UPDATE market_data_totals SET row_count=row_count+1 "
                "WHERE table_name='market_trades'; END"
            )
        repository = ProductionRepository(database)
        today = datetime(2026, 9, 9, 16, 49, tzinfo=UTC)
        batch = [
            ReceivedStreamEvent(
                trade(today + timedelta(microseconds=index)), today + timedelta(seconds=1)
            )
            for index in range(501)
        ]
        pages = 0

        def fail_page(_connection, _cursor, statement, _params, _context, _many):
            nonlocal pages
            if statement.startswith("INSERT INTO market_trades"):
                pages += 1
                if pages == 2:
                    raise RuntimeError("fixture second-page failure")

        batch_id = str(uuid4())
        event.listen(database.engine, "before_cursor_execute", fail_page)
        try:
            with pytest.raises(RuntimeError, match="second-page"):
                persist_shadow_batch(repository, batch, [], batch_id)
        finally:
            event.remove(database.engine, "before_cursor_execute", fail_page)
        assert pages == 2 and repository.market_data_counts() == (0, 0, 0)
        with database.begin() as connection:
            assert set(connection.scalars(select(market_data_totals.c.row_count))) == {0}
            assert connection.execute(select(events)).all() == []
            assert connection.execute(select(event_reporting_metadata)).all() == []
        assert persist_shadow_batch(repository, batch, [], batch_id).inserted == 501
        with database.begin() as connection:
            assert (
                connection.scalar(
                    select(market_data_totals.c.row_count).where(
                        market_data_totals.c.table_name == "market_trades"
                    )
                )
                == 501
            )


def test_pg_identity_fixture_refuses_sqlite_without_opening_a_connection(monkeypatch):
    from trade_identity_pg_fixture import verify_postgresql_trade_identity

    with Database("sqlite:///:memory:") as database:

        def forbidden():
            pytest.fail("PostgreSQL fixture must not connect automatically")

        monkeypatch.setattr(database.engine, "connect", forbidden)
        with pytest.raises(ValueError, match="PostgreSQL"):
            verify_postgresql_trade_identity(database.engine)


def test_sqlite_unexpected_old_unique_index_fails_closed_even_on_idempotent_upgrade(tmp_path):
    module = identity_migration()
    with Database(f"sqlite:///{tmp_path / 'unexpected-index.db'}") as database:
        database.initialize()
        with database.begin() as connection:
            connection.exec_driver_sql(
                "CREATE UNIQUE INDEX unexpected_legacy ON market_trades "
                "(symbol, feed_source, provider_trade_id)"
            )
            with pytest.raises(RuntimeError, match="legacy SQLite unique index"):
                module.swap_sqlite_constraint(
                    connection, Operations(MigrationContext.configure(connection))
                )
            assert any(
                value["name"] == "unexpected_legacy"
                for value in inspect(connection).get_indexes("market_trades")
            )
