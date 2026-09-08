"""Read-only, small-query verification of a candidate Database engine's cache bound."""

from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from tradeagent.persistence import POSTGRES_PREPARED_MAX

PREPARED_SERVER_BUDGET = 8


def verify_postgresql_prepared_cache(engine: Engine) -> dict[str, Any]:
    if not __debug__:
        raise ValueError("Prepared-cache verification requires assertions enabled")
    if engine.dialect.name != "postgresql" or engine.dialect.driver != "psycopg":
        raise ValueError("Prepared-cache verification requires a candidate psycopg engine")
    counts = []
    with engine.connect() as connection:
        driver = connection.connection.driver_connection
        assert driver is not None
        if getattr(driver, "autocommit", False):
            raise ValueError("Read-only rollback verification forbids autocommit")
        assert driver.prepared_max == POSTGRES_PREPARED_MAX
        assert driver.prepare_threshold == 5
        transaction = connection.begin()
        try:
            connection.exec_driver_sql("SET TRANSACTION READ ONLY")
            assert connection.scalar(text("SHOW transaction_read_only")) == "on"
            for shape in range(16):
                statement = text(
                    "SELECT CAST(:value AS INTEGER) + CAST(:offset AS INTEGER) "
                    f"/* bounded_prepared_probe_{shape} */"
                )
                for execution in range(7):
                    assert (
                        connection.scalar(
                            statement, {"value": execution + 100, "offset": shape + 10}
                        )
                        == execution + shape + 110
                    )
                with driver.cursor() as cursor:
                    cursor.execute("SELECT count(*) FROM pg_prepared_statements", prepare=False)
                    count = cursor.fetchone()[0]
                    assert 0 <= count <= PREPARED_SERVER_BUDGET
                    counts.append(count)
            assert 0 < counts[-1] <= PREPARED_SERVER_BUDGET
            assert driver.prepared_max == POSTGRES_PREPARED_MAX
            assert driver.prepare_threshold == 5
        finally:
            transaction.rollback()
        assert not connection.in_transaction()
    return {
        "verified": True,
        "read_only": True,
        "rolled_back": True,
        "shapes": 16,
        "executions_per_shape": 7,
        "prepared_max": POSTGRES_PREPARED_MAX,
        "server_budget": PREPARED_SERVER_BUDGET,
        "verification_scope": "sequential small SELECT workload; not a universal cache bound",
        "prepare_threshold": 5,
        "observed_prepared_counts": counts,
    }
