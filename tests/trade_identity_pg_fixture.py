"""Parent-invoked, private-schema, outer-rollback verification of migration 0013."""

from __future__ import annotations

import importlib.util
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import uuid4

from sqlalchemy import MetaData, UniqueConstraint, event, select, text
from sqlalchemy.exc import DBAPIError

from tradeagent.alpaca_stream import MarketQuote, MarketTrade, ReceivedStreamEvent
from tradeagent.domain import MarketBar
from tradeagent.persistence import (
    ProductionRepository,
    event_reporting_metadata,
    events,
    market_bars,
    market_data_totals,
    market_quotes,
    market_trades,
    metadata,
)
from tradeagent.reporting_reads import REPORTING_PROJECTION_VERSION, event_reporting_projection
from tradeagent.shadow_recorder import persist_shadow_batch


def migration(number: str, name: str) -> Any:
    path = Path("migrations") / "versions" / f"{number}_{name}.py"
    spec = importlib.util.spec_from_file_location(f"private_identity_{number}", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class BorrowedDatabase:
    def __init__(self, connection: Any) -> None:
        self.connection = connection

    @contextmanager
    def begin(self):
        with self.connection.begin_nested():
            yield self.connection


def evidence(connection: Any) -> dict[str, Any]:
    raw = {
        table.name: {
            row[next(iter(table.primary_key.columns)).name]: dict(row)
            for row in connection.execute(select(table)).mappings()
        }
        for table in (market_bars, market_quotes, market_trades)
    }
    totals = {
        row["table_name"]: dict(row)
        for row in connection.execute(select(market_data_totals)).mappings()
    }
    assert all(totals[name]["row_count"] == len(rows) for name, rows in raw.items())
    originals = {
        row["event_id"]: dict(row) for row in connection.execute(select(events)).mappings()
    }
    sidecars = {
        row["event_id"]: dict(row)
        for row in connection.execute(select(event_reporting_metadata)).mappings()
    }
    assert set(originals) == set(sidecars)
    for identity, original in originals.items():
        assert sidecars[identity]["projection_version"] == REPORTING_PROJECTION_VERSION
        assert sidecars[identity]["payload"] == event_reporting_projection(
            original["event_type"], original["payload"]
        )
    return {"raw": raw, "totals": totals, "events": originals, "metadata": sidecars}


def verify_postgresql_trade_identity(engine: Any) -> dict[str, Any]:
    if not __debug__ or engine.dialect.name != "postgresql":
        raise ValueError("Explicit PostgreSQL engine and assertions are required")
    schema = f"trade_identity_probe_{uuid4().hex}"
    identity_migration = migration("0013", "trade_event_identity")
    stages = []
    with engine.connect() as connection:
        if connection.connection.driver_connection.autocommit:
            raise ValueError("The identity fixture forbids autocommit")

        def forbid_commit(_connection):
            raise RuntimeError("The identity fixture may not commit")

        event.listen(connection, "commit", forbid_commit)
        outer = connection.begin()
        try:
            connection.exec_driver_sql("SET LOCAL statement_timeout = '10s'")
            connection.exec_driver_sql("SET LOCAL lock_timeout = '2s'")
            connection.exec_driver_sql(f'CREATE SCHEMA "{schema}"')
            connection.exec_driver_sql(f'SET LOCAL search_path TO "{schema}", pg_catalog')
            assert connection.scalar(text("SELECT current_schema()")) == schema
            connection.execution_options(schema_translate_map={None: schema})
            legacy = market_trades.to_metadata(MetaData())
            for constraint in list(legacy.constraints):
                if isinstance(constraint, UniqueConstraint):
                    legacy.constraints.remove(constraint)
            legacy.append_constraint(
                UniqueConstraint(*identity_migration.OLD_COLUMNS, name=identity_migration.OLD_NAME)
            )
            legacy.create(connection)
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
            migration("0012", "market_data_totals").install_postgresql_totals(connection)
            connection.exec_driver_sql("SET LOCAL statement_timeout = '10s'")
            before_oid = connection.scalar(
                text("SELECT to_regclass(:name)::oid"), {"name": f"{schema}.market_trades"}
            )
            trigger_query = text(
                "SELECT t.oid, t.tgname FROM pg_catalog.pg_trigger t "
                "JOIN pg_catalog.pg_class c ON c.oid=t.tgrelid "
                "JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace "
                "WHERE n.nspname=:schema AND NOT t.tgisinternal ORDER BY t.oid"
            )
            triggers = connection.execute(trigger_query, {"schema": schema}).all()
            assert len(triggers) == 9
            repository = ProductionRepository(BorrowedDatabase(connection))
            yesterday = datetime(2026, 9, 8, 16, 9, tzinfo=UTC)
            today = datetime(2026, 9, 9, 16, 49, tzinfo=UTC)

            def trade(at, *, venue="V", price=Decimal(100), provider_id="4118"):
                return MarketTrade(
                    symbol="QQQ",
                    timestamp=at,
                    trade_id=provider_id,
                    exchange=venue,
                    price=price,
                    size=Decimal(5),
                    conditions=("@",),
                    tape="C",
                )

            def persist(values, batch_id=None):
                return persist_shadow_batch(
                    repository,
                    [
                        ReceivedStreamEvent(value, value.timestamp + timedelta(milliseconds=35))
                        for value in values
                    ],
                    [],
                    batch_id or str(uuid4()),
                    clock=lambda: today + timedelta(minutes=1),
                    instance_id="private-identity-probe",
                )

            bar = MarketBar(
                symbol="QQQ",
                timestamp=yesterday,
                open=Decimal(100),
                high=Decimal(101),
                low=Decimal(99),
                close=Decimal(100),
                volume=Decimal(1000),
            )
            quote = MarketQuote(
                symbol="QQQ",
                timestamp=yesterday,
                bid_price=Decimal(100),
                ask_price=Decimal(101),
                bid_size=Decimal(5),
                ask_size=Decimal(10),
            )
            assert persist([bar, quote, trade(yesterday)]).inserted == 3
            broken_batch = str(uuid4())
            assert persist([trade(today, price=Decimal(101))], broken_batch).duplicates == 1
            original = evidence(connection)
            assert len(original["raw"]["market_trades"]) == 1
            stages.append("old_global_id_constraint_reproduces_cross_day_loss")

            trial = connection.begin_nested()
            identity_migration.prepare_postgresql_index(connection, concurrently=False)
            identity_migration.swap_postgresql_constraint(connection)
            assert identity_migration.NEW_NAME in identity_migration._constraints(
                connection, schema
            )
            trial.rollback()
            assert identity_migration.OLD_NAME in identity_migration._constraints(
                connection, schema
            )
            assert evidence(connection) == original
            stages.append("transactional_index_and_constraint_swap_roll_back_without_data_changes")

            identity_migration.prepare_postgresql_index(connection, concurrently=False)
            identity_migration.swap_postgresql_constraint(connection)
            assert (
                identity_migration.prepare_postgresql_index(connection, concurrently=False) is None
            )
            identity_migration.swap_postgresql_constraint(connection)
            assert evidence(connection) == original
            assert (
                connection.scalar(
                    text("SELECT to_regclass(:name)::oid"), {"name": f"{schema}.market_trades"}
                )
                == before_oid
            )
            assert connection.execute(trigger_query, {"schema": schema}).all() == triggers
            stages.append(
                "idempotent_swap_preserves_table_oid_nine_triggers_totals_audits_sidecars"
            )

            assert persist([trade(today, price=Decimal(101))]).inserted == 1
            assert repository.store_market_trade(
                provider_trade_id="4118",
                symbol="QQQ",
                event_at=today,
                received_at=today,
                price=Decimal(102),
                size=Decimal(5),
                exchange="N",
            )
            assert repository.store_market_trade(
                provider_trade_id="4118",
                symbol="QQQ",
                event_at=today + timedelta(microseconds=1),
                received_at=today + timedelta(seconds=1),
                price=Decimal(103),
                size=Decimal(5),
                exchange="V",
            )
            assert not repository.store_market_trade(
                provider_trade_id="4118",
                symbol="QQQ",
                event_at=yesterday,
                received_at=today,
                price=Decimal(999),
                size=Decimal(99),
                exchange="V",
            )
            current = evidence(connection)
            assert len(current["raw"]["market_trades"]) == 4
            for identity, row in original["raw"]["market_trades"].items():
                assert current["raw"]["market_trades"][identity] == row
            for identity in original["events"]:
                assert current["events"][identity] == original["events"][identity]
                assert current["metadata"][identity] == original["metadata"][identity]
            stages.append("cross_day_venue_time_writes_and_first_original_retries_are_exact")

            failing = [
                trade(
                    today + timedelta(seconds=2, microseconds=index), provider_id=f"later-{index}"
                )
                for index in range(501)
            ]
            failing[-1] = failing[-1].model_copy(update={"price": Decimal("1e30")})
            try:
                persist(failing)
            except DBAPIError as exc:
                assert getattr(exc.orig, "sqlstate", None) == "22003"
            else:
                raise AssertionError("Expected later-page numeric failure")
            assert evidence(connection) == current
            stages.append("late_batch_failure_rolls_back_raw_counter_audit_metadata")

            trial = connection.begin_nested()
            try:
                identity_migration.prepare_postgresql_index(
                    connection, concurrently=False, downgrade=True
                )
            except DBAPIError as exc:
                assert getattr(exc.orig, "sqlstate", None) == "23505"
            else:
                raise AssertionError("Downgrade accepted distinct records sharing the old key")
            finally:
                trial.rollback()
            assert evidence(connection) == current
            assert identity_migration.NEW_NAME in identity_migration._constraints(
                connection, schema
            )
            assert connection.execute(trigger_query, {"schema": schema}).all() == triggers
            stages.append("unsafe_downgrade_refused_without_collapsing_history")
        finally:
            outer.rollback()
        connection.exec_driver_sql("SET TRANSACTION READ ONLY")
        assert (
            connection.scalar(text("SELECT to_regnamespace(:schema)"), {"schema": schema}) is None
        )
        connection.rollback()
    return {
        "verified": True,
        "rolled_back": True,
        "schema": schema,
        "stages": stages,
        "actual_commits": 0,
        "concurrent_index_build_exercised": False,
        "concurrent_index_build_requires_separate_reviewed_migration": True,
        "not_a_live_data_acceptance_or_backfill": True,
    }
