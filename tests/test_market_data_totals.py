from __future__ import annotations

import os
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest
from alembic import command
from alembic.config import Config
from market_data_totals_pg_fixture import migration_module, verify_postgresql_market_totals
from sqlalchemy import create_engine, event, insert, inspect, select
from sqlalchemy.dialects import postgresql

from tradeagent.persistence import (
    MARKET_DATA_TABLE_NAMES,
    Database,
    MarketDataTotalsUnavailableError,
    ProductionRepository,
    market_data_totals,
    market_trades,
)


def test_postgresql_market_counts_read_three_totals_without_scanning_history(monkeypatch):
    with Database("sqlite:///:memory:") as database:
        database.initialize()
        values = (2**34, 0, 54321)
        with database.begin() as connection:
            connection.execute(
                insert(market_data_totals),
                [
                    {"table_name": name, "row_count": count, "updated_at": datetime.now(UTC)}
                    for name, count in zip(MARKET_DATA_TABLE_NAMES, values, strict=True)
                ],
            )
        queries = []
        event.listen(
            database.engine,
            "before_cursor_execute",
            lambda conn, cursor, sql, params, context, many: queries.append(sql),
        )
        monkeypatch.setattr(database.engine.dialect, "name", "postgresql")
        assert ProductionRepository(database).market_data_counts() == values
        assert len(queries) == 1
        assert "FROM market_data_totals" in queries[0]
        assert "count(" not in queries[0].lower()
        assert all(f"FROM {name}" not in queries[0] for name in MARKET_DATA_TABLE_NAMES)


@pytest.mark.parametrize("present", [(), ("market_bars",), ("market_bars", "market_quotes")])
def test_missing_postgresql_counter_rows_fail_closed(monkeypatch, present):
    with Database("sqlite:///:memory:") as database:
        database.initialize()
        with database.begin() as connection:
            for name in present:
                connection.execute(
                    insert(market_data_totals).values(
                        table_name=name, row_count=0, updated_at=datetime.now(UTC)
                    )
                )
        monkeypatch.setattr(database.engine.dialect, "name", "postgresql")
        with pytest.raises(MarketDataTotalsUnavailableError, match="0012_market_data_totals"):
            ProductionRepository(database).market_data_counts()


def migration_connection(isolation="READ COMMITTED", autocommit=False) -> Any:
    statements = []
    return SimpleNamespace(
        dialect=postgresql.dialect(),
        get_isolation_level=lambda: isolation,
        get_execution_options=lambda: {"isolation_level": "AUTOCOMMIT"} if autocommit else {},
        connection=SimpleNamespace(driver_connection=SimpleNamespace(autocommit=autocommit)),
        scalar=lambda query: "counter_fixture",
        exec_driver_sql=lambda query: statements.append(query),
        execute=lambda query: statements.append(str(query)),
        statements=statements,
    )


def test_pg_migration_locks_before_atomic_initialize_and_installs_statement_triggers():
    connection = migration_connection()
    migration_module().install_postgresql_totals(connection)
    sql = connection.statements
    lock = next(index for index, value in enumerate(sql) if value.startswith("LOCK TABLE"))
    initialize = next(index for index, value in enumerate(sql) if value.startswith("INSERT INTO"))
    function = next(
        index for index, value in enumerate(sql) if "CREATE OR REPLACE FUNCTION" in value
    )
    assert lock < initialize < function
    assert all(name in sql[lock] for name in MARKET_DATA_TABLE_NAMES)
    assert sql[lock].endswith("IN SHARE ROW EXCLUSIVE MODE")
    assert sql[initialize].count("count(*)") == 3
    assert "ON CONFLICT (table_name) DO UPDATE" in sql[initialize]
    assert "max_parallel_workers_per_gather = 0" in " ".join(sql)
    assert "lock_timeout = '5s'" in " ".join(sql)
    assert "statement_timeout = '30s'" in " ".join(sql)
    trigger_sql = [value for value in sql if value.startswith("CREATE TRIGGER")]
    assert len(trigger_sql) == 9
    assert all(
        "FOR EACH STATEMENT" in value and "FOR EACH ROW" not in value for value in trigger_sql
    )
    assert sum("REFERENCING NEW TABLE AS inserted_rows" in value for value in trigger_sql) == 3
    assert sum("REFERENCING OLD TABLE AS deleted_rows" in value for value in trigger_sql) == 3
    assert sum("AFTER TRUNCATE" in value for value in trigger_sql) == 3
    assert "SELECT count(*) INTO delta FROM inserted_rows" in sql[function]
    assert "SELECT -count(*) INTO delta FROM deleted_rows" in sql[function]
    assert "row_count = row_count + delta" in sql[function]
    assert "IF NOT FOUND" in sql[function]
    assert not any(value.startswith(("COMMIT", "TRUNCATE", "DELETE FROM market_")) for value in sql)


@pytest.mark.parametrize(
    "isolation,autocommit", [("REPEATABLE READ", False), ("READ COMMITTED", True)]
)
def test_pg_initialization_rejects_racy_or_nontransactional_modes(isolation, autocommit):
    connection = migration_connection(isolation, autocommit)
    with pytest.raises(RuntimeError):
        migration_module().install_postgresql_totals(connection)
    assert connection.statements == []


def test_pg_initialization_rejects_driver_autocommit_even_without_execution_option():
    connection = migration_connection(autocommit=True)
    connection.get_execution_options = lambda: {}
    with pytest.raises(RuntimeError, match="single transaction"):
        migration_module().install_postgresql_totals(connection)
    assert connection.statements == []


def test_sqlite_upgrade_and_downgrade_keep_existing_count_contract(tmp_path, monkeypatch):
    url = f"sqlite:///{tmp_path / 'market-totals.db'}"
    monkeypatch.setenv("TRADEAGENT_DATABASE_URL", url)
    config = Config("alembic.ini")
    command.upgrade(config, "0011_reporting_metadata")
    with Database(url) as database:
        now = datetime.now(UTC)
        ProductionRepository(database).store_market_trade(
            provider_trade_id="retained-market-trade",
            symbol="SPY",
            event_at=now,
            received_at=now,
            price=Decimal("100.25"),
            size=Decimal("0.5"),
        )
        with database.begin() as connection:
            original = dict(connection.execute(select(market_trades)).mappings().one())
    command.upgrade(config, "0012_market_data_totals")
    with Database(url) as database:
        assert inspect(database.engine).has_table("market_data_totals")
        assert ProductionRepository(database).market_data_counts() == (0, 0, 1)
        with database.begin() as connection:
            assert dict(connection.execute(select(market_trades)).mappings().one()) == original
    command.downgrade(config, "0011_reporting_metadata")
    with Database(url) as database:
        assert not inspect(database.engine).has_table("market_data_totals")
        assert ProductionRepository(database).market_data_counts() == (0, 0, 1)
        with database.begin() as connection:
            assert dict(connection.execute(select(market_trades)).mappings().one()) == original


@pytest.mark.skipif(
    not os.getenv("TRADEAGENT_TEST_POSTGRES_URL"),
    reason="Requires an explicitly supplied PostgreSQL URL; fixture always rolls back",
)
def test_actual_postgresql_statement_totals_rollback_only():
    engine = create_engine(os.environ["TRADEAGENT_TEST_POSTGRES_URL"])
    try:
        result = verify_postgresql_market_totals(engine)
        assert result["verified"] and result["rolled_back"]
        assert len(result["stages"]) == 7
    finally:
        engine.dispose()
