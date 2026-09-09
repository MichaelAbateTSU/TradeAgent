"""Scope trade identity to venue and event time without rewriting historical evidence."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import sqlalchemy as sa
from alembic import op
from alembic.operations import Operations
from sqlalchemy.engine import Connection

revision = "0013_trade_event_identity"
down_revision = "0012_market_data_totals"
branch_labels = None
depends_on = None

OLD_NAME = "uq_market_trade_provider_id"
NEW_NAME = "uq_market_trade_event_identity"
OLD_COLUMNS = ("symbol", "feed_source", "provider_trade_id")
NEW_COLUMNS = (*OLD_COLUMNS, "exchange", "event_at")
BUILD_NEW = "ix_market_trades_event_identity_build"
BUILD_OLD = "ix_market_trades_provider_identity_build"


def _schema(connection: Connection) -> str:
    schema = connection.scalar(sa.select(sa.func.current_schema()))
    if not isinstance(schema, str):
        raise RuntimeError("Trade identity migration requires a current schema")
    return schema


def _qualified(connection: Connection, schema: str, name: str) -> str:
    quote = connection.dialect.identifier_preparer
    return f"{quote.quote_schema(schema)}.{quote.quote(name)}"


def _constraints(connection: Connection, schema: str) -> dict[str, dict[str, Any]]:
    return {
        row["name"]: dict(row)
        for row in connection.execute(
            sa.text("""
            SELECT c.conname AS name, c.contype AS kind, c.condeferrable AS deferred,
                   ARRAY(SELECT a.attname::text
                         FROM unnest(c.conkey) WITH ORDINALITY AS k(num, ordinal)
                         JOIN pg_catalog.pg_attribute a
                           ON a.attrelid=c.conrelid AND a.attnum=k.num
                         ORDER BY k.ordinal) AS key_columns
            FROM pg_catalog.pg_constraint c
            JOIN pg_catalog.pg_class t ON t.oid=c.conrelid
            JOIN pg_catalog.pg_namespace n ON n.oid=t.relnamespace
            WHERE n.nspname=:schema AND t.relname='market_trades'
        """),
            {"schema": schema},
        ).mappings()
    }


def _indexes(connection: Connection, schema: str) -> dict[str, dict[str, Any]]:
    return {
        row["name"]: dict(row)
        for row in connection.execute(
            sa.text("""
            SELECT x.relname AS name, i.indisunique AS "unique", i.indisvalid AS valid,
                   i.indisready AS ready, am.amname AS method,
                   i.indexprs IS NULL AS plain, i.indpred IS NULL AS "full",
                   i.indnatts=i.indnkeyatts AS no_include,
                   NOT EXISTS(SELECT 1 FROM unnest(i.indoption) AS flags(value)
                              WHERE flags.value<>0) AS default_order,
                   NOT EXISTS(
                       SELECT 1 FROM unnest(i.indkey::smallint[], i.indcollation::oid[])
                           AS k(num, coll_oid)
                       JOIN pg_catalog.pg_attribute a
                         ON a.attrelid=i.indrelid AND a.attnum=k.num
                       WHERE k.coll_oid<>a.attcollation
                   ) AS default_collations,
                   NOT EXISTS(
                       SELECT 1 FROM unnest(i.indclass::oid[]) AS classes(identifier)
                       JOIN pg_catalog.pg_opclass c ON c.oid=classes.identifier
                       WHERE NOT c.opcdefault
                   ) AS default_opclasses,
                   EXISTS(SELECT 1 FROM pg_catalog.pg_constraint c
                          WHERE c.conindid=i.indexrelid) AS owned,
                   ARRAY(SELECT a.attname::text
                         FROM unnest(i.indkey) WITH ORDINALITY AS k(num, ordinal)
                         JOIN pg_catalog.pg_attribute a
                           ON a.attrelid=i.indrelid AND a.attnum=k.num
                         ORDER BY k.ordinal) AS key_columns
            FROM pg_catalog.pg_index i
            JOIN pg_catalog.pg_class t ON t.oid=i.indrelid
            JOIN pg_catalog.pg_namespace n ON n.oid=t.relnamespace
            JOIN pg_catalog.pg_class x ON x.oid=i.indexrelid
            JOIN pg_catalog.pg_am am ON am.oid=x.relam
            WHERE n.nspname=:schema AND t.relname='market_trades'
        """),
            {"schema": schema},
        ).mappings()
    }


def _constraint_matches(value: dict[str, Any], columns: tuple[str, ...]) -> bool:
    return value["kind"] == "u" and not value["deferred"] and tuple(value["key_columns"]) == columns


def _index_matches(value: dict[str, Any], columns: tuple[str, ...]) -> bool:
    return bool(
        value["unique"]
        and value["method"] == "btree"
        and value["plain"]
        and value["full"]
        and value["no_include"]
        and value["default_order"]
        and value["default_collations"]
        and value["default_opclasses"]
        and tuple(value["key_columns"]) == columns
    )


def _direction(downgrade: bool) -> tuple[str, str, tuple[str, ...], str]:
    return (
        (NEW_NAME, OLD_NAME, OLD_COLUMNS, BUILD_OLD)
        if downgrade
        else (OLD_NAME, NEW_NAME, NEW_COLUMNS, BUILD_NEW)
    )


@contextmanager
def concurrent_index_limits(connection: Connection) -> Iterator[None]:
    if not getattr(connection.connection.driver_connection, "autocommit", False):
        raise RuntimeError("CONCURRENTLY requires the explicit Alembic autocommit block")
    saved = {
        name: connection.scalar(sa.text(f"SHOW {name}"))
        for name in ("lock_timeout", "statement_timeout")
    }
    try:
        for name, value in (("lock_timeout", "2s"), ("statement_timeout", "300s")):
            connection.execute(
                sa.text("SELECT set_config(:name, :value, false)"), {"name": name, "value": value}
            )
        yield
    finally:
        for name, value in saved.items():
            connection.execute(
                sa.text("SELECT set_config(:name, :value, false)"), {"name": name, "value": value}
            )


def prepare_postgresql_index(
    connection: Connection, *, downgrade: bool = False, concurrently: bool = True
) -> str | None:
    """Build first, leaving current uniqueness intact; nonconcurrent is fixture-only."""
    if connection.dialect.name != "postgresql":
        raise ValueError("PostgreSQL is required")
    autocommit = bool(getattr(connection.connection.driver_connection, "autocommit", False))
    if concurrently != autocommit or (not concurrently and not connection.in_transaction()):
        raise RuntimeError("Index build transaction mode does not match concurrently")
    schema = _schema(connection)
    _, target, columns, build = _direction(downgrade)
    constraints = _constraints(connection, schema)
    if target in constraints:
        if not _constraint_matches(constraints[target], columns):
            raise RuntimeError(f"Existing {target} has an unexpected definition")
        return None
    indexes = _indexes(connection, schema)
    # An interrupted attachment may leave an already named standalone index.
    for name in (target, build):
        existing = indexes.get(name)
        if existing is None:
            if (
                connection.scalar(
                    sa.text("SELECT to_regclass(:name)"),
                    {"name": _qualified(connection, schema, name)},
                )
                is not None
            ):
                raise RuntimeError(f"Existing relation {name} is not the expected trade index")
            continue
        if not _index_matches(existing, columns) or existing["owned"]:
            raise RuntimeError(f"Existing {name} is not a reusable identity index")
        if existing["valid"] and existing["ready"]:
            return name
        connection.exec_driver_sql(
            f"DROP INDEX {'CONCURRENTLY ' if concurrently else ''}"
            f"{_qualified(connection, schema, name)}"
        )
    quote = connection.dialect.identifier_preparer.quote
    try:
        connection.exec_driver_sql(
            f"CREATE UNIQUE INDEX {'CONCURRENTLY ' if concurrently else ''}{quote(build)} "
            f"ON {_qualified(connection, schema, 'market_trades')} "
            f"({', '.join(quote(column) for column in columns)})"
        )
    except Exception:
        # A failed concurrent UNIQUE build can leave an enforcing invalid index.
        # Remove only this migration's unattached, definition-matching artifact.
        if concurrently:
            failed = _indexes(connection, schema).get(build)
            if (
                failed
                and _index_matches(failed, columns)
                and not failed["valid"]
                and not failed["owned"]
            ):
                connection.exec_driver_sql(
                    f"DROP INDEX CONCURRENTLY {_qualified(connection, schema, build)}"
                )
        raise
    built = _indexes(connection, schema).get(build)
    if not built or not _index_matches(built, columns) or not built["valid"] or not built["ready"]:
        raise RuntimeError("Replacement trade identity index did not become valid")
    return build


def swap_postgresql_constraint(connection: Connection, *, downgrade: bool = False) -> None:
    """Short atomic metadata-only swap; the table OID, data and nine triggers stay intact."""
    if (
        connection.dialect.name != "postgresql"
        or not connection.in_transaction()
        or getattr(connection.connection.driver_connection, "autocommit", False)
    ):
        raise RuntimeError("Constraint swap requires a PostgreSQL transaction")
    schema = _schema(connection)
    source, target, columns, build = _direction(downgrade)
    source_columns = NEW_COLUMNS if downgrade else OLD_COLUMNS
    table = _qualified(connection, schema, "market_trades")
    quote = connection.dialect.identifier_preparer.quote
    connection.exec_driver_sql("SET LOCAL lock_timeout = '2s'")
    connection.exec_driver_sql("SET LOCAL statement_timeout = '10s'")
    connection.exec_driver_sql(f"LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE")
    constraints = _constraints(connection, schema)
    for name, expected in ((source, source_columns), (target, columns)):
        if name in constraints and not _constraint_matches(constraints[name], expected):
            raise RuntimeError(f"Unexpected constraint definition for {name}")
    changes = []
    if target not in constraints:
        indexes = _indexes(connection, schema)
        selected = next(
            (
                name
                for name in (target, build)
                if name in indexes
                and _index_matches(indexes[name], columns)
                and indexes[name]["valid"]
                and indexes[name]["ready"]
                and not indexes[name]["owned"]
            ),
            None,
        )
        if selected is None:
            raise RuntimeError("A valid replacement index must exist before the constraint swap")
        changes.append(f"ADD CONSTRAINT {quote(target)} UNIQUE USING INDEX {quote(selected)}")
    if source in constraints:
        changes.append(f"DROP CONSTRAINT {quote(source)}")
    if changes:
        connection.exec_driver_sql(f"ALTER TABLE {table} {', '.join(changes)}")
    after = _constraints(connection, schema)
    if target not in after or not _constraint_matches(after[target], columns) or source in after:
        raise RuntimeError("Trade identity constraint swap did not reach its required state")
    if not downgrade:
        for index in _indexes(connection, schema).values():
            if (
                index["unique"]
                and index["ready"]
                and index["plain"]
                and set(index["key_columns"]) == set(OLD_COLUMNS)
            ):
                raise RuntimeError(
                    "An unexpected legacy unique index still blocks cross-day trades"
                )


def _check_sqlite_legacy_indexes(connection: Connection) -> None:
    inspector = sa.inspect(connection)
    definitions = [
        *inspector.get_unique_constraints("market_trades"),
        *[item for item in inspector.get_indexes("market_trades") if item["unique"]],
    ]
    if any(set(value["column_names"]) == set(OLD_COLUMNS) for value in definitions):
        raise RuntimeError("An unexpected legacy SQLite unique index still blocks new trades")


def swap_sqlite_constraint(
    connection: Connection, operations: Operations, *, downgrade: bool = False
) -> None:
    if connection.dialect.name != "sqlite":
        raise ValueError("SQLite is required")
    # Legacy sqlite3 does not BEGIN for DDL: ensure the rebuild and version update
    # remain inside a real transaction, not an outermost SAVEPOINT that commits.
    if not connection.connection.driver_connection.in_transaction:
        connection.exec_driver_sql("BEGIN IMMEDIATE")
    source, target, columns, _ = _direction(downgrade)
    with connection.begin_nested():
        inspector = sa.inspect(connection)
        constraints = {
            value["name"]: tuple(value["column_names"])
            for value in inspector.get_unique_constraints("market_trades")
        }
        for name, expected in (
            (source, NEW_COLUMNS if downgrade else OLD_COLUMNS),
            (target, columns),
        ):
            if name in constraints and constraints[name] != expected:
                raise RuntimeError(f"Unexpected SQLite constraint definition for {name}")
        if target in constraints and source not in constraints:
            if not downgrade:
                _check_sqlite_legacy_indexes(connection)
            return
        if downgrade:
            fields = ", ".join(OLD_COLUMNS)
            duplicate = connection.execute(
                sa.text(
                    f"SELECT 1 FROM market_trades GROUP BY {fields} HAVING count(*) > 1 LIMIT 1"
                )
            ).first()
            if duplicate:
                raise RuntimeError("Downgrade would collapse distinct trades; no rows were changed")
        for table in inspector.get_table_names():
            if table != "market_trades" and any(
                value["referred_table"] == "market_trades"
                for value in inspector.get_foreign_keys(table)
            ):
                raise RuntimeError("SQLite rebuild refuses incoming foreign keys to market_trades")
        objects = (
            connection.execute(
                sa.text(
                    "SELECT type, name, sql FROM sqlite_schema "
                    "WHERE tbl_name='market_trades' AND type IN ('index','trigger') "
                    "AND sql IS NOT NULL"
                )
            )
            .mappings()
            .all()
        )
        with operations.batch_alter_table("market_trades", recreate="always") as batch:
            if source in constraints:
                batch.drop_constraint(source, type_="unique")
            if target not in constraints:
                batch.create_unique_constraint(target, list(columns))
        remaining = set(
            connection.scalars(
                sa.text("SELECT name FROM sqlite_schema WHERE tbl_name='market_trades'")
            )
        )
        for value in objects:
            if value["name"] not in remaining:
                connection.exec_driver_sql(value["sql"])
        after = {
            value["name"]: tuple(value["column_names"])
            for value in sa.inspect(connection).get_unique_constraints("market_trades")
        }
        if after.get(target) != columns or source in after:
            raise RuntimeError("SQLite trade identity swap did not reach its required state")
        if not downgrade:
            _check_sqlite_legacy_indexes(connection)


def _migrate(*, downgrade: bool) -> None:
    connection = op.get_bind()
    if connection.dialect.name == "postgresql":
        with op.get_context().autocommit_block(), concurrent_index_limits(connection):
            prepare_postgresql_index(connection, downgrade=downgrade, concurrently=True)
        swap_postgresql_constraint(connection, downgrade=downgrade)
    elif connection.dialect.name == "sqlite":
        swap_sqlite_constraint(connection, op, downgrade=downgrade)
    else:
        raise RuntimeError("Trade identity migration supports only PostgreSQL and SQLite")


def upgrade() -> None:
    _migrate(downgrade=False)


def downgrade() -> None:
    _migrate(downgrade=True)
