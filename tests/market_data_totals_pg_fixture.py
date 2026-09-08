"""Explicit, rollback-only PostgreSQL verification; never touches production market tables."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any
from uuid import uuid4

from sqlalchemy import select, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError

from tradeagent.persistence import market_data_totals


def migration_module() -> Any:
    path = (
        Path(__file__).resolve().parents[1]
        / "migrations"
        / "versions"
        / "0012_market_data_totals.py"
    )
    spec = importlib.util.spec_from_file_location("market_totals_migration", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def verify_postgresql_market_totals(engine: Engine) -> dict[str, Any]:
    if not __debug__:
        raise ValueError("PostgreSQL verification must run with assertions enabled")
    if engine.dialect.name != "postgresql":
        raise ValueError("This verification requires an explicitly supplied PostgreSQL engine")
    schema = f"market_totals_probe_{uuid4().hex}"
    stages = []
    with engine.connect() as connection:
        if getattr(connection.connection.driver_connection, "autocommit", False):
            raise ValueError("Rollback-only verification forbids an autocommit connection")
        transaction = connection.begin()
        try:
            connection.exec_driver_sql(f'CREATE SCHEMA "{schema}"')
            connection.exec_driver_sql(f'SET LOCAL search_path TO "{schema}", pg_catalog')
            for name in ("market_bars", "market_quotes", "market_trades"):
                extra = ", quote_detail text" if name == "market_quotes" else ", detail jsonb"
                connection.exec_driver_sql(
                    f"CREATE TABLE {name} (id integer PRIMARY KEY, value integer{extra})"
                )
            market_data_totals.create(connection)
            connection.exec_driver_sql("INSERT INTO market_bars(id,value) VALUES (1, 10), (2, 20)")
            connection.exec_driver_sql("INSERT INTO market_quotes(id,value) VALUES (1, 10)")
            migration_module().install_postgresql_totals(connection)

            def counts() -> tuple[int, int, int]:
                rows: dict[str, int] = {
                    name: count
                    for name, count in connection.execute(
                        select(market_data_totals.c.table_name, market_data_totals.c.row_count)
                    )
                }
                actual = tuple(
                    connection.scalar(text(f"SELECT count(*) FROM {name}"))
                    for name in ("market_bars", "market_quotes", "market_trades")
                )
                result = rows["market_bars"], rows["market_quotes"], rows["market_trades"]
                assert result == actual
                return result

            assert counts() == (2, 1, 0)
            stages.append("existing_rows_initialized")
            connection.exec_driver_sql(
                "INSERT INTO market_bars(id,value) VALUES (2, 200), (3, 30), (4, 40) "
                "ON CONFLICT (id) DO NOTHING"
            )
            assert counts() == (4, 1, 0)
            assert connection.scalar(text("SELECT value FROM market_bars WHERE id=2")) == 20
            connection.exec_driver_sql(
                "INSERT INTO market_bars(id,value) VALUES (2, 222), (5, 50) "
                "ON CONFLICT (id) DO UPDATE SET value=EXCLUDED.value"
            )
            assert counts() == (5, 1, 0)
            assert connection.scalar(text("SELECT value FROM market_bars WHERE id=2")) == 222
            connection.exec_driver_sql(
                "INSERT INTO market_bars(id,value) "
                "SELECT value, value FROM generate_series(6, 1005) AS value"
            )
            connection.exec_driver_sql(
                "INSERT INTO market_trades(id,value) VALUES (1, 10), (2, 20), (3, 30)"
            )
            assert counts() == (1005, 1, 3)
            stages.append("multirow_insert_conflicts_and_updates_exact")
            savepoint = connection.begin_nested()
            connection.exec_driver_sql(
                "INSERT INTO market_quotes(id,value) VALUES (2, 20), (3, 30)"
            )
            connection.exec_driver_sql("DELETE FROM market_trades WHERE id < 3")
            assert counts() == (1005, 3, 1)
            savepoint.rollback()
            assert counts() == (1005, 1, 3)
            stages.append("insert_delete_rollback_atomic")
            connection.exec_driver_sql("DELETE FROM market_bars WHERE id IN (3, 4)")
            connection.exec_driver_sql("DELETE FROM market_bars WHERE id = -1")
            assert counts() == (1003, 1, 3)
            stages.append("multirow_and_empty_delete_exact")
            savepoint = connection.begin_nested()
            connection.exec_driver_sql("TRUNCATE market_bars, market_quotes")
            assert counts() == (0, 0, 3)
            savepoint.rollback()
            assert counts() == (1003, 1, 3)
            stages.append("truncate_and_rollback_exact")
            savepoint = connection.begin_nested()
            connection.exec_driver_sql(
                "DELETE FROM market_data_totals WHERE table_name='market_trades'"
            )
            try:
                connection.exec_driver_sql("INSERT INTO market_trades(id,value) VALUES (99, 99)")
            except DBAPIError:
                savepoint.rollback()
            else:
                raise AssertionError("missing counter did not reject the unaccounted write")
            assert counts() == (1003, 1, 3)
            savepoint = connection.begin_nested()
            try:
                connection.exec_driver_sql("UPDATE market_data_totals SET row_count=-1")
            except DBAPIError:
                savepoint.rollback()
            else:
                raise AssertionError("negative counter was accepted")
            assert counts() == (1003, 1, 3)
            stages.append("missing_and_negative_counters_fail_closed")
            triggers = connection.execute(
                text(
                    "SELECT t.tgtype, t.tgnewtable, t.tgoldtable FROM pg_trigger t "
                    "JOIN pg_class c ON c.oid=t.tgrelid "
                    "JOIN pg_namespace n ON n.oid=c.relnamespace "
                    "WHERE n.nspname=:schema AND NOT t.tgisinternal"
                ),
                {"schema": schema},
            ).all()
            assert len(triggers) == 9
            assert all(kind & 1 == 0 for kind, _, _ in triggers)
            assert sum(new == "inserted_rows" for _, new, _ in triggers) == 3
            assert sum(old == "deleted_rows" for _, _, old in triggers) == 3
            stages.append("nine_statement_level_transition_triggers")
        finally:
            transaction.rollback()
        assert (
            connection.scalar(text("SELECT to_regnamespace(:schema)"), {"schema": schema}) is None
        )
    return {"verified": True, "rolled_back": True, "stages": stages}
