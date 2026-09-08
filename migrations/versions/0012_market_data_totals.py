"""Maintain exact market-data totals once per statement, without recurring history scans."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.engine import Connection

revision = "0012_market_data_totals"
down_revision = "0011_reporting_metadata"
branch_labels = None
depends_on = None

TABLE_NAMES = ("market_bars", "market_quotes", "market_trades")
FUNCTION_NAME = "maintain_market_data_totals"


def install_postgresql_totals(connection: Connection) -> None:
    if connection.get_execution_options().get("isolation_level") == "AUTOCOMMIT" or getattr(
        connection.connection.driver_connection, "autocommit", False
    ):
        raise RuntimeError("Market counter initialization requires a single transaction")
    if connection.get_isolation_level() != "READ COMMITTED":
        raise RuntimeError("Market counter initialization requires READ COMMITTED isolation")
    schema = connection.scalar(sa.select(sa.func.current_schema()))
    if not isinstance(schema, str):
        raise RuntimeError("Market counter initialization requires a current schema")
    quoted = connection.dialect.identifier_preparer.quote_schema(schema)
    totals = f"{quoted}.market_data_totals"
    function = f"{quoted}.{FUNCTION_NAME}"
    connection.exec_driver_sql("SET LOCAL lock_timeout = '5s'")
    connection.exec_driver_sql("SET LOCAL statement_timeout = '30s'")
    connection.exec_driver_sql("SET LOCAL max_parallel_workers_per_gather = 0")
    connection.exec_driver_sql(
        "LOCK TABLE "
        + ", ".join(f"{quoted}.{name}" for name in TABLE_NAMES)
        + " IN SHARE ROW EXCLUSIVE MODE"
    )
    connection.exec_driver_sql(
        f"INSERT INTO {totals} (table_name, row_count, updated_at) "
        + " UNION ALL ".join(
            f"SELECT '{name}', count(*), clock_timestamp() FROM {quoted}.{name} WHERE true"
            for name in TABLE_NAMES
        )
        + " ON CONFLICT (table_name) DO UPDATE "
        "SET row_count = EXCLUDED.row_count, updated_at = EXCLUDED.updated_at"
    )
    connection.execute(
        sa.text(f"""
CREATE OR REPLACE FUNCTION {function}() RETURNS trigger
LANGUAGE plpgsql AS $function$
DECLARE
    delta bigint;
BEGIN
    IF TG_OP = 'TRUNCATE' THEN
        UPDATE {totals} SET row_count = 0, updated_at = clock_timestamp()
        WHERE table_name = TG_TABLE_NAME;
    ELSE
        IF TG_OP = 'INSERT' THEN
            SELECT count(*) INTO delta FROM inserted_rows;
        ELSIF TG_OP = 'DELETE' THEN
            SELECT -count(*) INTO delta FROM deleted_rows;
        ELSE
            RAISE EXCEPTION 'Unsupported market counter operation: %', TG_OP;
        END IF;
        IF delta = 0 THEN
            IF NOT EXISTS (SELECT 1 FROM {totals} WHERE table_name = TG_TABLE_NAME) THEN
                RAISE EXCEPTION 'Missing exact market counter for %', TG_TABLE_NAME;
            END IF;
            RETURN NULL;
        END IF;
        UPDATE {totals} SET row_count = row_count + delta, updated_at = clock_timestamp()
        WHERE table_name = TG_TABLE_NAME;
    END IF;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Missing exact market counter for %', TG_TABLE_NAME;
    END IF;
    RETURN NULL;
END;
$function$
""")
    )
    for name in TABLE_NAMES:
        for operation, transition in (
            ("INSERT", "REFERENCING NEW TABLE AS inserted_rows"),
            ("DELETE", "REFERENCING OLD TABLE AS deleted_rows"),
            ("TRUNCATE", ""),
        ):
            trigger = f"trg_{name}_totals_{operation.lower()}"
            connection.exec_driver_sql(f"DROP TRIGGER IF EXISTS {trigger} ON {quoted}.{name}")
            connection.exec_driver_sql(
                f"CREATE TRIGGER {trigger} AFTER {operation} ON {quoted}.{name} "
                f"{transition} FOR EACH STATEMENT EXECUTE FUNCTION {function}()"
            )


def upgrade() -> None:
    connection = op.get_bind()
    inspector = sa.inspect(connection)
    if inspector.has_table("market_data_totals"):
        columns = {value["name"] for value in inspector.get_columns("market_data_totals")}
        primary = inspector.get_pk_constraint("market_data_totals")["constrained_columns"]
        if columns != {"table_name", "row_count", "updated_at"} or primary != ["table_name"]:
            raise RuntimeError("Existing market-data totals table has an incompatible schema")
    else:
        op.create_table(
            "market_data_totals",
            sa.Column("table_name", sa.String(32), primary_key=True),
            sa.Column("row_count", sa.BigInteger, nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
            sa.CheckConstraint("row_count >= 0", name="ck_market_data_totals_nonnegative"),
        )
    if connection.dialect.name == "postgresql":
        install_postgresql_totals(connection)


def downgrade() -> None:
    connection = op.get_bind()
    if connection.dialect.name == "postgresql":
        for name in TABLE_NAMES:
            for operation in ("insert", "delete", "truncate"):
                op.execute(f"DROP TRIGGER IF EXISTS trg_{name}_totals_{operation} ON {name}")
        op.execute(f"DROP FUNCTION {FUNCTION_NAME}()")
    op.drop_table("market_data_totals")
