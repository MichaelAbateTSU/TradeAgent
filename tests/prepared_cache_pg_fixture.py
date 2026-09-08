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
    cleanup_counts = []
    with engine.connect() as connection:
        driver = connection.connection.driver_connection
        assert driver is not None
        if getattr(driver, "autocommit", False):
            raise ValueError("Read-only rollback verification forbids autocommit")
        assert driver.prepared_max == POSTGRES_PREPARED_MAX
        assert driver.prepare_threshold == 5

        def exercise(label: str, shape: int, execution: int) -> None:
            statement = text(
                "SELECT CAST(:value AS INTEGER) + CAST(:offset AS INTEGER) "
                f"/* bounded_prepared_{label}_{shape} */"
            )
            assert (
                connection.scalar(statement, {"value": execution + 100, "offset": shape + 10})
                == execution + shape + 110
            )

        def count_prepared() -> int:
            with driver.cursor() as cursor:
                cursor.execute("SELECT count(*) FROM pg_prepared_statements", prepare=False)
                count = int(cursor.fetchone()[0])
                assert 0 <= count <= PREPARED_SERVER_BUDGET
                return count

        transaction = connection.begin()
        try:
            connection.exec_driver_sql("SET TRANSACTION READ ONLY")
            assert connection.scalar(text("SHOW transaction_read_only")) == "on"
            for shape in range(16):
                for execution in range(7):
                    exercise("sequential", shape, execution)
                counts.append(count_prepared())
            assert 0 < counts[-1] <= PREPARED_SERVER_BUDGET
        finally:
            transaction.rollback()
        assert not connection.in_transaction()

        transaction = connection.begin()
        try:
            connection.exec_driver_sql("SET TRANSACTION READ ONLY")
            assert connection.scalar(text("SHOW transaction_read_only")) == "on"
            for shape in range(POSTGRES_PREPARED_MAX):
                for execution in range(7):
                    exercise("burst_base", shape, execution)
            baseline = count_prepared()
            assert baseline == POSTGRES_PREPARED_MAX
            for shape in range(POSTGRES_PREPARED_MAX):
                for execution in range(5):
                    exercise("burst_warm", shape, execution)
            # No inspection queries between warmup and promotion: a new key can trigger eviction.
            for shape in range(POSTGRES_PREPARED_MAX):
                exercise("burst_warm", shape, 5)
            burst_first = count_prepared()
            burst_second = count_prepared()
            for shape in range(POSTGRES_PREPARED_MAX):
                exercise("new_key_cleanup", shape, 0)
                cleanup_counts.append(count_prepared())
            assert cleanup_counts[-1] <= POSTGRES_PREPARED_MAX
            assert driver.prepared_max == POSTGRES_PREPARED_MAX
            assert driver.prepare_threshold == 5
        finally:
            transaction.rollback()
        assert not connection.in_transaction()
    peak = max(*counts, baseline, burst_first, burst_second, *cleanup_counts)
    assert 0 < peak <= PREPARED_SERVER_BUDGET
    return {
        "verified": True,
        "read_only": True,
        "rolled_back": True,
        "shapes": 16,
        "executions_per_shape": 7,
        "prepared_max": POSTGRES_PREPARED_MAX,
        "server_budget": PREPARED_SERVER_BUDGET,
        "verification_scope": (
            "sequential and warmed-key promotion workloads; not a universal cache or RSS bound"
        ),
        "prepare_threshold": 5,
        "observed_prepared_counts": counts,
        "burst_prepared_baseline": baseline,
        "burst_warmed_shapes": POSTGRES_PREPARED_MAX,
        "burst_first_count": burst_first,
        "burst_second_count": burst_second,
        "new_key_cleanup_counts": cleanup_counts,
        "observed_peak": peak,
        "inspection_semantics": (
            "A new inspection-query key may schedule eviction after its count snapshot; "
            "first, second and explicit new-key cleanup observations are recorded separately."
        ),
    }
