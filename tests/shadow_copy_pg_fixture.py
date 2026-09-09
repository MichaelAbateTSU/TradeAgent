"""Explicit rollback-only PG comparison. This prototype is never used by a running recorder."""

from __future__ import annotations

import hashlib
import importlib.util
import inspect
import json
import math
import re
import statistics
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import uuid4

import psycopg
from psycopg import sql
from sqlalchemy import event, func, select, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.sql.dml import Insert
from sqlalchemy.sql.selectable import Select

import tradeagent.shadow_recorder as recorder
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

BASELINE_SHA256 = "96679b8a6d8db325b9742de785790543139a409a3e5badc4e7ced767bfd326c0"
PAGE_SIZE = 500
RAW_TABLES = (market_bars, market_quotes, market_trades)
TABLES = (*RAW_TABLES, events, event_reporting_metadata, market_data_totals)
COLUMNS = {
    market_bars.name: (
        "bar_id",
        "symbol",
        "timeframe",
        "feed_source",
        "event_at",
        "received_at",
        "processed_at",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ),
    market_quotes.name: (
        "quote_id",
        "symbol",
        "feed_source",
        "event_at",
        "received_at",
        "processed_at",
        "bid_price",
        "bid_exchange",
        "ask_price",
        "ask_exchange",
        "bid_size",
        "ask_size",
    ),
    market_trades.name: (
        "market_trade_id",
        "provider_trade_id",
        "symbol",
        "feed_source",
        "exchange",
        "event_at",
        "received_at",
        "processed_at",
        "price",
        "size",
        "conditions",
        "tape",
    ),
}


class ProbeInconclusiveError(RuntimeError):
    pass


class ProbeBudget:
    def __init__(
        self,
        guard: Callable[[], bool],
        *,
        max_rows: int,
        pause_seconds: float,
        deadline_seconds: float,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.guard = guard
        self.max_rows = max_rows
        self.pause_seconds = pause_seconds
        self.clock = clock
        self.sleep = sleep
        self.deadline = clock() + deadline_seconds
        self.rows = 0
        self.pages = 0
        self.guard_checks = 0
        self.control_seconds = 0.0
        self.control_cpu_seconds = 0.0
        self.last_page_at: float | None = None
        self.last_guard_at: float | None = None

    def check(self, *, force_guard: bool = False) -> None:
        if self.clock() >= self.deadline:
            raise ProbeInconclusiveError("deadline")
        if force_guard or self.last_guard_at is None or self.clock() - self.last_guard_at >= 1:
            started, cpu = time.perf_counter(), time.thread_time()
            try:
                self.guard_checks += 1
                if self.guard() is not True:
                    raise ProbeInconclusiveError("live_guard_rejected")
            except ProbeInconclusiveError:
                raise
            except Exception as exc:
                raise ProbeInconclusiveError(f"live_guard_error:{type(exc).__name__}") from exc
            finally:
                self.control_seconds += time.perf_counter() - started
                self.control_cpu_seconds += time.thread_time() - cpu
                self.last_guard_at = self.clock()
        if self.clock() >= self.deadline:
            raise ProbeInconclusiveError("deadline")

    def reserve_page(self, rows: int) -> None:
        if not 0 < rows <= PAGE_SIZE or self.rows + rows > self.max_rows:
            raise ProbeInconclusiveError("raw_row_budget")
        if self.last_page_at is not None:
            until = self.last_page_at + self.pause_seconds
            while self.clock() < until:
                self.check()
                delay = min(0.25, until - self.clock(), self.deadline - self.clock())
                if delay <= 0:
                    raise ProbeInconclusiveError("deadline")
                started, cpu = time.perf_counter(), time.thread_time()
                self.sleep(delay)
                self.control_seconds += time.perf_counter() - started
                self.control_cpu_seconds += time.thread_time() - cpu
        self.check(force_guard=True)
        self.rows += rows
        self.pages += 1
        self.last_page_at = self.clock()


def counter_migration() -> Any:
    path = Path("migrations") / "versions" / "0012_market_data_totals.py"
    spec = importlib.util.spec_from_file_location("isolated_copy_counter_migration", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("The exact image's counter migration is required")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def validate_schema(schema: str) -> None:
    if re.fullmatch(r"shadow_copy_probe_[0-9a-f]{32}", schema) is None:
        raise ValueError("A random, private recorder-probe schema is required")


def copy_statements(schema: str, table: Any) -> tuple[sql.Composed, sql.Composed]:
    validate_schema(schema)
    if table not in RAW_TABLES or tuple(table.columns.keys()) != COLUMNS[table.name]:
        raise ValueError("Only the exact frozen raw-table columns are supported")
    columns = COLUMNS[table.name]
    stage = sql.Identifier("pg_temp", f"sc_{schema[-32:]}_{table.name}")
    names = sql.SQL(", ").join(map(sql.Identifier, columns))
    copy = sql.SQL("COPY {} ({}, {}) FROM STDIN").format(stage, names, sql.Identifier("_ordinal"))
    # No DISTINCT: pre-deduplication can lose a later valid row when a prior
    # natural-key candidate itself loses a different primary-key conflict.
    merge = sql.SQL(
        "WITH incoming AS (DELETE FROM {} RETURNING {}, {}) "
        "INSERT INTO {} ({}) SELECT {} FROM incoming ORDER BY {} "
        "ON CONFLICT DO NOTHING RETURNING {}"
    ).format(
        stage,
        names,
        sql.Identifier("_ordinal"),
        sql.Identifier(schema, table.name),
        names,
        names,
        sql.Identifier("_ordinal"),
        sql.Identifier(next(iter(table.primary_key.columns)).name),
    )
    return copy, merge


def copy_row(table: Any, row: dict[str, Any], ordinal: int) -> tuple[Any, ...]:
    columns = COLUMNS[table.name]
    if set(row) != set(columns):
        raise ValueError("COPY rows must contain exactly the frozen column set")
    values = tuple(
        json.dumps(row[name], ensure_ascii=False, allow_nan=False)
        if name == "conditions"
        else row[name]
        for name in columns
    )
    return (*values, ordinal)


class Rows:
    def __init__(self, rows: list[Any]) -> None:
        self.rows = rows

    def fetchall(self) -> list[Any]:
        return self.rows

    def all(self) -> list[Any]:
        return self.rows


class BorrowedConnection:
    """Only the frozen writer's table operations; no commit/close/engine escape."""

    def __init__(
        self,
        connection: Any,
        schema: str,
        *,
        use_copy: bool,
        deadline: float | None = None,
        page_rows: int = PAGE_SIZE,
        budget: ProbeBudget | None = None,
    ) -> None:
        validate_schema(schema)
        if (
            not isinstance(page_rows, int)
            or isinstance(page_rows, bool)
            or not 1 <= page_rows <= PAGE_SIZE
        ):
            raise ValueError("COPY adapter pages must remain within the fixed 500-row cap")
        if not connection.in_transaction() or connection.get_execution_options().get(
            "schema_translate_map"
        ) != {None: schema}:
            raise ValueError(
                "An active outer transaction and exact private schema mapping are required"
            )
        self.connection = connection
        self.schema = schema
        self.use_copy = use_copy
        self.deadline = deadline
        self.page_rows = page_rows
        self.budget = budget
        self.dialect = connection.dialect
        self.metrics: dict[str, float] = {}
        self.raw_ids: list[str] = []
        self.audit_ids: list[str] = []
        self.metadata_calls = 0
        self.fail_metadata_at: int | None = None

    def reset(self) -> None:
        self.metrics = {}
        self.raw_ids = []
        self.audit_ids = []
        self.metadata_calls = 0

    def in_transaction(self) -> bool:
        return bool(self.connection.in_transaction())

    def check_deadline(self) -> None:
        if self.budget is not None:
            self.budget.check()
        if self.deadline is not None and time.monotonic() > self.deadline:
            raise ProbeInconclusiveError("deadline")

    @contextmanager
    def phase(self, name: str) -> Iterator[None]:
        wall, cpu = time.perf_counter(), time.thread_time()
        control = self.budget.control_seconds if self.budget else 0
        control_cpu = self.budget.control_cpu_seconds if self.budget else 0
        try:
            yield
        finally:
            elapsed, elapsed_cpu = time.perf_counter() - wall, time.thread_time() - cpu
            waiting = self.budget.control_seconds - control if self.budget else 0
            waiting_cpu = self.budget.control_cpu_seconds - control_cpu if self.budget else 0
            self.metrics[name] = self.metrics.get(name, 0) + elapsed
            self.metrics[f"{name}_excluding_controls"] = self.metrics.get(
                f"{name}_excluding_controls", 0
            ) + max(0, elapsed - waiting)
            self.metrics[f"{name}_thread_cpu"] = self.metrics.get(f"{name}_thread_cpu", 0) + max(
                0, elapsed_cpu - waiting_cpu
            )

    @staticmethod
    def guard(statement: Any) -> None:
        if isinstance(statement, Insert) and statement.table in TABLES:
            return
        if isinstance(statement, Select):
            tables = statement.get_final_froms()
            if tables and all(table in TABLES for table in tables):
                return
        raise ValueError("The probe adapter only permits known private-table statements")

    def execute(self, statement: Any, parameters: Any = None, **kwargs: Any) -> Any:
        self.check_deadline()
        self.guard(statement)
        if isinstance(statement, Insert) and statement.table in RAW_TABLES:
            if not isinstance(parameters, list) or not 0 < len(parameters) <= 1500:
                raise ValueError("The isolated raw fixture accepts at most 1500 rows per call")
            with self.phase("raw_execute_returning"):
                if self.use_copy:
                    rows = self.copy_insert(statement.table, parameters)
                else:
                    kwargs["execution_options"] = {
                        **kwargs.get("execution_options", {}),
                        "insertmanyvalues_page_size": self.page_rows,
                    }
                    rows = self.connection.execute(statement, parameters, **kwargs).fetchall()
            self.raw_ids.extend(str(row[0]) for row in rows)
            return Rows(rows)
        if not isinstance(statement, Insert) or statement.table is not event_reporting_metadata:
            raise ValueError("Only sidecar inserts are delegated through execute")
        with self.phase("metadata_execute"):
            self.metadata_calls += 1
            if self.metadata_calls == self.fail_metadata_at:
                # An actual private-schema FK error, not a fake successful write.
                self.connection.execute(
                    event_reporting_metadata.insert().values(
                        event_id=str(uuid4()), projection_version=1, payload={}
                    )
                )
                raise AssertionError("The private metadata foreign key was not enforced")
            return self.connection.execute(statement, parameters, **kwargs)

    def scalar(self, statement: Any, *args: Any, **kwargs: Any) -> Any:
        self.check_deadline()
        self.guard(statement)
        with self.phase("batch_lookup"):
            return self.connection.scalar(statement, *args, **kwargs)

    def scalars(self, statement: Any, *args: Any, **kwargs: Any) -> Rows:
        self.check_deadline()
        self.guard(statement)
        if not isinstance(statement, Insert) or statement.table is not events:
            raise ValueError("Only audit INSERT RETURNING is supported through scalars")
        kwargs["execution_options"] = {
            **kwargs.get("execution_options", {}),
            "insertmanyvalues_page_size": self.page_rows,
        }
        with self.phase("audit_execute_returning"):
            ids = self.connection.scalars(statement, *args, **kwargs).all()
        self.audit_ids.extend(str(value) for value in ids)
        return Rows(ids)

    def copy_insert(self, table: Any, values: list[dict[str, Any]]) -> list[Any]:
        copy, merge = copy_statements(self.schema, table)
        driver = self.connection.connection.driver_connection
        rows = []
        try:
            with driver.cursor() as cursor:
                for start in range(0, len(values), self.page_rows):
                    self.check_deadline()
                    page = values[start : start + self.page_rows]
                    if self.budget is not None:
                        self.budget.reserve_page(len(page))
                    with self.phase("copy_staging"), cursor.copy(copy) as writer:
                        for ordinal, row in enumerate(page):
                            writer.write_row(copy_row(table, row, ordinal))
                    if self.budget is not None:
                        self.budget.check(force_guard=True)
                    with self.phase("ordered_merge_and_stage_delete"):
                        cursor.execute(merge)
                        rows.extend(cursor.fetchall())
        except psycopg.Error as exc:
            raise DBAPIError(
                "isolated COPY staging/merge", None, exc, hide_parameters=True
            ) from exc
        return rows


class SavepointDatabase:
    def __init__(self, borrowed: BorrowedConnection) -> None:
        self.borrowed = borrowed
        self.lose_release_ack_once = False

    @contextmanager
    def begin(self) -> Iterator[BorrowedConnection]:
        borrowed = self.borrowed
        borrowed.reset()
        finish_started = None
        failed = False
        begin_started = time.perf_counter()
        try:
            with borrowed.connection.begin_nested():
                borrowed.metrics["savepoint_begin"] = time.perf_counter() - begin_started
                try:
                    yield borrowed
                except BaseException:
                    failed = True
                    raise
                finally:
                    finish_started = time.perf_counter()
        finally:
            if finish_started is not None:
                borrowed.metrics["savepoint_rollback" if failed else "savepoint_release"] = (
                    time.perf_counter() - finish_started
                )
        if self.lose_release_ack_once:
            self.lose_release_ack_once = False
            raise OperationalError(
                "simulated lost RELEASE SAVEPOINT acknowledgement (NOT COMMIT)",
                {},
                RuntimeError("probe acknowledgement loss"),
            )


def receipts(count: int, *, mixed: bool = False) -> list[ReceivedStreamEvent]:
    start = datetime(2026, 9, 9, 13, 30, tzinfo=UTC)
    output = []
    for index in range(count):
        stamp = start + timedelta(microseconds=index)
        symbol = ("SPY", "QQQ", "AAPL", "MSFT", "NVDA")[index % 5]
        if mixed and index % 100 < 5:
            market_event: Any = MarketBar(
                symbol=symbol,
                timestamp=start + timedelta(minutes=index // 100),
                open=Decimal("100.123456789012"),
                high=Decimal("101.123456789012"),
                low=Decimal("99.123456789012"),
                close=Decimal("100.123456789012"),
                volume=Decimal(1000),
            )
        elif mixed and index % 100 < 10:
            market_event = MarketTrade(
                symbol=symbol,
                timestamp=stamp,
                price=Decimal("100.123456789012"),
                size=Decimal("10.5"),
                trade_id=f"probe-{index}",
                exchange="V",
                conditions=("@", "\t", "\\", '"'),
                tape=None,
            )
        else:
            market_event = MarketQuote(
                symbol=symbol,
                timestamp=stamp,
                bid_price=Decimal("100.123456789012"),
                ask_price=Decimal("100.223456789012"),
                bid_size=Decimal(10),
                ask_size=Decimal(20),
                bid_exchange="V",
                ask_exchange="V",
            )
        output.append(
            ReceivedStreamEvent(market_event, market_event.timestamp + timedelta(milliseconds=35))
        )
    return output


def snapshot(connection: Any) -> dict[str, Any]:
    counts = []
    raw = {}
    for table in RAW_TABLES:
        rows = connection.execute(select(table)).mappings().all()
        raw[table.name] = {
            row[next(iter(table.primary_key.columns)).name]: dict(row) for row in rows
        }
        counts.append(len(rows))
    totals = dict(
        connection.execute(
            select(market_data_totals.c.table_name, market_data_totals.c.row_count)
        ).all()
    )
    assert tuple(counts) == tuple(totals[table.name] for table in RAW_TABLES)
    stored_events = connection.execute(
        select(events.c.event_id, events.c.event_type, events.c.payload)
    ).all()
    bodies = {row.event_id: row.payload for row in stored_events}
    types = {row.event_id: row.event_type for row in stored_events}
    sidecars = connection.execute(select(event_reporting_metadata)).mappings().all()
    assert {row["event_id"] for row in sidecars} == set(bodies)
    for row in sidecars:
        assert row["projection_version"] == REPORTING_PROJECTION_VERSION
        assert row["payload"] == event_reporting_projection(
            types[row["event_id"]], bodies[row["event_id"]]
        )
    return {
        "counts": tuple(counts),
        "raw": raw,
        "events": bodies,
        "metadata": {row["event_id"]: dict(row) for row in sidecars},
    }


def assert_empty_staging(connection: Any, schema: str) -> None:
    for table in RAW_TABLES:
        stage = f"sc_{schema[-32:]}_{table.name}"
        assert connection.scalar(text(f'SELECT count(*) FROM pg_temp."{stage}"')) == 0


def verify_packet_rows(
    current: dict[str, Any],
    batch: list[ReceivedStreamEvent],
    *,
    processed_at: datetime | None = None,
) -> None:
    indexed = {
        (name, row["symbol"], row["event_at"]): row
        for name, rows in current["raw"].items()
        for row in rows.values()
    }
    for item in batch:
        table = (
            market_bars
            if isinstance(item.event, MarketBar)
            else (market_quotes if isinstance(item.event, MarketQuote) else market_trades)
        )
        row = indexed[table.name, item.event.symbol, item.event.timestamp]
        assert row["received_at"] == item.received_at and row["feed_source"] == "iex"
        if processed_at is not None:
            assert row["processed_at"] == processed_at
        if isinstance(item.event, MarketBar):
            names = ("open", "high", "low", "close", "volume")
            assert row["timeframe"] == "1Min"
        elif isinstance(item.event, MarketQuote):
            names = (
                "bid_price",
                "ask_price",
                "bid_size",
                "ask_size",
                "bid_exchange",
                "ask_exchange",
            )
        else:
            names = ("price", "size", "exchange")
            assert row["provider_trade_id"] == str(item.event.trade_id)
            assert row["conditions"] == list(item.event.conditions)
            assert row["tape"] == item.event.tape
        for name in names:
            assert row[name] == getattr(item.event, name)


def backend_observation(connection: Any) -> dict[str, Any]:
    values = dict(
        connection.execute(
            text(
                "SELECT pg_backend_pid() AS backend_pid, "
                "(SELECT count(*) FROM pg_catalog.pg_prepared_statements) AS prepared_statements, "
                "(SELECT sum(total_bytes) FROM pg_catalog.pg_backend_memory_contexts) "
                "AS own_backend_context_bytes"
            )
        )
        .mappings()
        .one()
    )
    return {key: int(value) if value is not None else None for key, value in values.items()}


def bounded_statement(statement: str, *, deadline: float, cleanup: bool = False) -> str:
    normalized = statement.strip().upper()
    if normalized.rstrip(";") in {"COMMIT", "END"} or normalized.startswith(("COMMIT ", "END ")):
        raise RuntimeError("The isolated recorder probe may NEVER commit")
    if cleanup or normalized.startswith(("ROLLBACK", "RELEASE SAVEPOINT")):
        return statement
    if time.monotonic() > deadline:
        raise TimeoutError("The isolated recorder probe exceeded its SQL deadline")
    # The imported migration normally sets broader installation timeouts. Only
    # this private probe connection gets the tighter, fixed benchmark limits.
    if normalized.startswith("SET LOCAL STATEMENT_TIMEOUT"):
        return "SET LOCAL statement_timeout = '2s'"
    if normalized.startswith("SET LOCAL LOCK_TIMEOUT"):
        return "SET LOCAL lock_timeout = '500ms'"
    return statement


def raw_page_count(schema: str, statement: str, parameters: Any) -> int | None:
    for table in RAW_TABLES:
        targets = (
            f"{schema}.{table.name}",
            f'"{schema}".{table.name}',
            f'"{schema}"."{table.name}"',
        )
        if any(statement.startswith(f"INSERT INTO {target} ") for target in targets):
            width = len(COLUMNS[table.name])
            if not parameters or len(parameters) % width:
                raise ProbeInconclusiveError("unexpected_raw_parameter_shape")
            return len(parameters) // width
    return None


def validate_options(
    pairs: int,
    page_rows: int,
    pause_seconds: float,
    deadline_seconds: float,
    max_raw_rows: int,
    workload: str,
    exercise_faults: bool,
) -> int:
    for name, value, lower, upper in (
        ("pairs", pairs, 1, 3),
        ("page_rows", page_rows, 25, 500),
        ("max_raw_rows", max_raw_rows, 50, 2000),
    ):
        if not isinstance(value, int) or isinstance(value, bool) or not lower <= value <= upper:
            raise ValueError(f"{name} must be an integer in [{lower}, {upper}]")
    for name, value, lower, upper in (
        ("pause_seconds", pause_seconds, 1, 5),
        ("deadline_seconds", deadline_seconds, 15, 90),
    ):
        if (
            not isinstance(value, (int, float))
            or isinstance(value, bool)
            or not math.isfinite(value)
            or not lower <= value <= upper
        ):
            raise ValueError(f"{name} must be finite and in [{lower}, {upper}]")
    if workload not in {"quotes", "mixed", "all_conflict"} or not isinstance(exercise_faults, bool):
        raise ValueError("A single supported workload and boolean exercise_faults are required")
    if exercise_faults and page_rows > 50:
        raise ValueError("Extended fault checks are limited to 25-50 row test pages")
    estimated = pairs * page_rows * (4 if workload == "all_conflict" else 2)
    if exercise_faults:
        estimated += 20 * page_rows + 14
    if estimated > max_raw_rows:
        raise ValueError(f"Requested work exceeds max_raw_rows ({estimated} > {max_raw_rows})")
    return estimated


def check_semantics(
    connection: Any,
    schema: str,
    *,
    use_copy: bool,
    page_rows: int,
    budget: ProbeBudget,
) -> list[str]:
    borrowed = BorrowedConnection(
        connection, schema, use_copy=use_copy, page_rows=page_rows, budget=budget
    )
    database = SavepointDatabase(borrowed)
    repository = ProductionRepository(database)  # type: ignore[arg-type]
    stages = []

    def persist(batch: Any, identity: str | None = None, notices: Any = ()):
        return recorder.persist_shadow_batch(
            repository,
            batch,
            list(notices),
            identity or str(uuid4()),
            clock=lambda: datetime(2026, 9, 9, 14, 30, tzinfo=UTC),
            instance_id="isolated-copy-probe",
        )

    def scenario() -> Any:
        return connection.begin_nested()

    savepoint = scenario()
    try:
        batch = receipts(page_rows, mixed=True)
        result = persist(batch)
        current = snapshot(connection)
        assert sum(current["counts"]) == result.inserted == page_rows and result.duplicates == 0
        actual_ids = {identity for rows in current["raw"].values() for identity in rows}
        assert set(borrowed.raw_ids) == actual_ids
        verify_packet_rows(current, batch, processed_at=datetime(2026, 9, 9, 14, 30, tzinfo=UTC))
        assert_empty_staging(connection, schema)
        stages.append("mixed_precision_json_provenance_exact_ids_and_counters")
    finally:
        savepoint.rollback()

    savepoint = scenario()
    try:
        persist(receipts(1))
        template = dict(connection.execute(select(market_quotes)).mappings().one())
        changed_at = template["event_at"] + timedelta(seconds=1)
        collision = [
            {**template, "quote_id": "collision-first", "event_at": changed_at},
            {
                **template,
                "quote_id": "collision-first",
                "event_at": changed_at + timedelta(seconds=1),
            },
            {
                **template,
                "quote_id": "collision-third",
                "event_at": changed_at + timedelta(seconds=1),
                "received_at": changed_at + timedelta(seconds=2),
            },
        ]
        statement = (
            recorder.pg_insert(market_quotes)
            .on_conflict_do_nothing()
            .returning(market_quotes.c.quote_id)
        )
        with database.begin() as adapter:
            ids = adapter.execute(
                statement, collision, execution_options={"insertmanyvalues_page_size": PAGE_SIZE}
            ).fetchall()
        assert {row[0] for row in ids} == {"collision-first", "collision-third"}
        assert snapshot(connection)["counts"] == (0, 3, 0)
        assert (
            connection.scalar(
                select(market_quotes.c.received_at).where(
                    market_quotes.c.quote_id == "collision-third"
                )
            )
            == collision[2]["received_at"]
        )
        assert_empty_staging(connection, schema)
        stages.append("interacting_primary_and_natural_key_conflicts_do_not_lose_later_row")
    finally:
        savepoint.rollback()

    savepoint = scenario()
    try:
        originals = receipts(page_rows)
        persist(originals)
        now = datetime(2026, 9, 9, 14, 30, tzinfo=UTC)
        notices = [
            {
                "event_id": str(uuid4()),
                "event_type": "shadow_stream_status",
                "occurred_at": now,
                "recorded_at": now,
                "trace_id": "isolated-original",
                "payload": {"session_date": "2026-09-08"},
            }
            for _ in range(page_rows)
        ]
        connection.execute(events.insert(), notices)
        connection.execute(
            event_reporting_metadata.insert(),
            [
                {
                    "event_id": row["event_id"],
                    "projection_version": REPORTING_PROJECTION_VERSION,
                    "payload": event_reporting_projection(row["event_type"], row["payload"]),
                }
                for row in notices
            ],
        )
        first = snapshot(connection)
        new = receipts(page_rows + 2)[page_rows:]
        repeated = [ReceivedStreamEvent(new[0].event, new[0].received_at + timedelta(seconds=1))]
        batch = [*originals, new[0], *repeated * (page_rows - 1), new[1]]
        identity = str(uuid4())
        changed = [{**row, "payload": {"session_date": "2099-09-09"}} for row in notices]
        new_notice = {**notices[0], "event_id": str(uuid4())}
        result = persist(batch, identity, notices=[*changed, new_notice])
        current = snapshot(connection)
        assert (result.inserted, result.duplicates) == (2, page_rows * 2 - 1)
        assert current["counts"] == (0, page_rows + 2, 0)
        before = set(first["raw"][market_quotes.name])
        assert set(borrowed.raw_ids) == set(current["raw"][market_quotes.name]) - before
        assert set(borrowed.audit_ids) == {identity, new_notice["event_id"]}
        assert borrowed.metadata_calls == 2
        assert all(current["events"][key] == body for key, body in first["events"].items())
        assert all(
            current["metadata"][identity] == row for identity, row in first["metadata"].items()
        )
        assert all(
            current["raw"][market_quotes.name][key] == row
            for key, row in first["raw"][market_quotes.name].items()
        )
        assert persist(batch, identity) == result
        assert not borrowed.raw_ids and not borrowed.audit_ids and borrowed.metadata_calls == 0
        assert snapshot(connection) == current
        assert_empty_staging(connection, schema)
        stages.append("all_conflict_page_input_duplicates_and_original_batch_replay")
    finally:
        savepoint.rollback()

    savepoint = scenario()
    try:
        good = receipts(page_rows * 2 + 1)
        bad = list(good)
        invalid_index = page_rows + page_rows // 2
        bad[invalid_index] = ReceivedStreamEvent(
            good[invalid_index].event.model_copy(
                update={"bid_price": Decimal("1e30"), "ask_price": Decimal("1e30")}
            ),
            good[invalid_index].received_at,
        )
        identity = str(uuid4())
        try:
            persist(bad, identity)
        except DBAPIError as exc:
            if getattr(exc.orig, "sqlstate", None) != "22003":
                raise
        else:
            raise AssertionError("Later-page numeric overflow did not abort the batch")
        assert snapshot(connection)["counts"] == (0, 0, 0)
        assert_empty_staging(connection, schema)
        assert persist(good, identity).inserted == len(good)
        assert snapshot(connection)["counts"] == (0, len(good), 0)
        stages.append("later_page_failure_counters_staging_rollback_and_retry")
    finally:
        savepoint.rollback()

    savepoint = scenario()
    try:
        now = datetime(2026, 9, 9, 14, 30, tzinfo=UTC)
        notice = {
            "event_id": str(uuid4()),
            "event_type": "shadow_stream_status",
            "occurred_at": now,
            "recorded_at": now,
            "trace_id": "isolated-probe",
            "payload": {"state": "subscribed"},
        }
        borrowed.fail_metadata_at = 2
        try:
            persist(receipts(page_rows), notices=[notice])
        except DBAPIError as exc:
            if getattr(exc.orig, "sqlstate", None) != "23503":
                raise
        else:
            raise AssertionError("Metadata FK failure did not roll back the raw batch")
        borrowed.fail_metadata_at = None
        empty = snapshot(connection)
        assert empty["counts"] == (0, 0, 0) and not empty["events"]
        assert_empty_staging(connection, schema)
        database.lose_release_ack_once = True
        identity = str(uuid4())
        try:
            persist(receipts(page_rows), identity)
        except OperationalError as exc:
            if not isinstance(exc.orig, RuntimeError) or "NOT COMMIT" not in str(exc.statement):
                raise
        else:
            raise AssertionError("Savepoint acknowledgement-loss injection did not execute")
        original = snapshot(connection)
        retry = [
            ReceivedStreamEvent(row.event, row.received_at + timedelta(hours=1))
            for row in receipts(page_rows)
        ]
        result = persist(retry, identity)
        assert (result.inserted, result.duplicates) == (page_rows, 0)
        assert snapshot(connection) == original
        assert not borrowed.raw_ids and borrowed.metadata_calls == 0
        stages.append("metadata_atomicity_and_lost_savepoint_ack_not_real_commit")
    finally:
        savepoint.rollback()
    return stages


def compare_shadow_copy(
    engine: Engine,
    *,
    guard: Callable[[], bool] | None = None,
    pairs: int = 1,
    page_rows: int = 25,
    workload: str = "quotes",
    pause_seconds: float = 2,
    deadline_seconds: float = 90,
    max_raw_rows: int = 750,
    exercise_faults: bool = False,
) -> dict[str, Any]:
    """Explicit parent-run smoke by default: 50 raw rows, never a production acceptance.

    guard must be a bounded, read-only parent check of live owner/lease/lag/queue/drops.
    A false result, exception, timeout or budget exhaustion stops all subsequent work.
    The guard callback itself must use finite HTTP timeouts; idle server transactions
    are killed after 10 seconds and active SQL has a two-second statement cap.
    """
    if not __debug__:
        raise ValueError("Assertions must remain enabled")
    if engine.dialect.name != "postgresql" or engine.dialect.driver != "psycopg":
        raise ValueError("An explicitly supplied PostgreSQL/psycopg engine is required")
    if not callable(guard):
        raise ValueError("An explicit bounded live-health guard is required")
    estimated_rows = validate_options(
        pairs, page_rows, pause_seconds, deadline_seconds, max_raw_rows, workload, exercise_faults
    )
    digest = hashlib.sha256(
        Path(inspect.getfile(recorder)).read_text(encoding="utf-8").encode("utf-8")
    ).hexdigest()
    if digest != BASELINE_SHA256:
        raise ValueError("The probe requires the exact frozen 645 writer")
    schema = f"shadow_copy_probe_{uuid4().hex}"
    budget = ProbeBudget(
        guard, max_rows=max_raw_rows, pause_seconds=pause_seconds, deadline_seconds=deadline_seconds
    )
    output: dict[str, Any] = {
        "baseline_sha256": digest,
        "schema": schema,
        "page_rows": page_rows,
        "production_page_cap_unchanged": PAGE_SIZE,
        "pairs": pairs,
        "workload": workload,
        "estimated_raw_rows_upper_bound": estimated_rows,
        "max_raw_rows": max_raw_rows,
        "pause_seconds": pause_seconds,
        "deadline_seconds": deadline_seconds,
        "statement_timeout_seconds": 2,
        "lock_timeout_seconds": 0.5,
        "psycopg_version": psycopg.__version__,
        "actual_commits": 0,
        "write_transactions": 0,
        "savepoint_timings_only": True,
        "production_candidate_accepted": False,
        "not_a_live_throughput_or_commit_acceptance": True,
        "small_private_indexes_not_production_history": True,
        "own_backend_context_bytes_not_rss_or_total_database_memory": True,
        "trial_rollback_and_validation_queries_can_disrupt_prepared_plan_warmth": True,
        "status": "inconclusive",
        "reason": "not_started",
        "rolled_back": False,
        "rollback_verified": False,
        "temporary_tables_rollback_verified": False,
        "samples": [],
        "correctness": {},
        "started_at": datetime.now(UTC).isoformat(),
    }

    def failed(exc: Exception) -> None:
        output["status"] = "inconclusive"
        output["reason"] = (
            str(exc)
            if isinstance(exc, ProbeInconclusiveError)
            else "deadline"
            if isinstance(exc, TimeoutError)
            else "correctness_failed"
            if isinstance(exc, AssertionError)
            else "database_or_setup_error"
        )
        output["error_type"] = type(exc).__name__
        output["sqlstate"] = getattr(getattr(exc, "orig", None), "sqlstate", None)

    try:
        budget.check(force_guard=True)
        with engine.connect() as connection:
            driver = connection.connection.driver_connection
            if driver.autocommit or driver.prepared_max != 4:
                raise ValueError("Autocommit is forbidden; the existing cache4 policy is required")
            budget.check(force_guard=True)
            cleanup = False

            def forbid_commit(_connection: Any) -> None:
                raise RuntimeError("The isolated recorder probe may NEVER commit")

            def limit_statement(_conn, _cursor, statement, parameters, _context, _many):
                checked = bounded_statement(statement, deadline=budget.deadline, cleanup=cleanup)
                if not cleanup:
                    count = raw_page_count(schema, statement, parameters)
                    if count is not None:
                        budget.reserve_page(count)
                return checked, parameters

            event.listen(connection, "commit", forbid_commit)
            event.listen(connection, "before_cursor_execute", limit_statement, retval=True)
            transaction = connection.begin()
            output["write_transactions"] = 1
            try:
                connection.exec_driver_sql("SET LOCAL statement_timeout = '2s'")
                connection.exec_driver_sql("SET LOCAL lock_timeout = '500ms'")
                connection.exec_driver_sql("SET LOCAL idle_in_transaction_session_timeout = '10s'")
                version = connection.dialect.server_version_info
                if version is not None and version >= (17,):
                    connection.exec_driver_sql(
                        f"SET LOCAL transaction_timeout = '{int(deadline_seconds)}s'"
                    )
                    output["server_transaction_deadline_enabled"] = True
                else:
                    output["server_transaction_deadline_enabled"] = False
                connection.exec_driver_sql(f'CREATE SCHEMA "{schema}"')
                connection.exec_driver_sql(f'SET LOCAL search_path TO "{schema}", pg_catalog')
                assert connection.scalar(select(func.current_schema())) == schema
                connection.execution_options(schema_translate_map={None: schema})
                budget.check(force_guard=True)
                metadata.create_all(connection, tables=list(TABLES), checkfirst=False)
                counter_migration().install_postgresql_totals(connection)
                connection.exec_driver_sql("SET LOCAL work_mem = '1MB'")
                for table in RAW_TABLES:
                    budget.check(force_guard=True)
                    stage = sql.SQL(
                        "CREATE TEMP TABLE {} (LIKE {}, {} integer NOT NULL) ON COMMIT DROP"
                    ).format(
                        sql.Identifier(f"sc_{schema[-32:]}_{table.name}"),
                        sql.Identifier(schema, table.name),
                        sql.Identifier("_ordinal"),
                    )
                    with driver.cursor() as cursor:
                        cursor.execute(stage)
                output["prepared_max"] = driver.prepared_max
                output["server_version"] = version
                output["backend_before"] = backend_observation(connection)
                if exercise_faults:
                    for use_copy in (False, True):
                        budget.check(force_guard=True)
                        output["correctness"]["copy" if use_copy else "baseline"] = check_semantics(
                            connection,
                            schema,
                            use_copy=use_copy,
                            page_rows=page_rows,
                            budget=budget,
                        )
                batch = receipts(page_rows, mixed=workload == "mixed")
                for pair in range(pairs):
                    for use_copy in (False, True) if pair % 2 == 0 else (True, False):
                        budget.check(force_guard=True)
                        trial = connection.begin_nested()
                        try:
                            borrowed = BorrowedConnection(
                                connection,
                                schema,
                                use_copy=use_copy,
                                page_rows=page_rows,
                                budget=budget,
                            )
                            repository = ProductionRepository(SavepointDatabase(borrowed))  # type: ignore[arg-type]
                            if workload == "all_conflict":
                                recorder.persist_shadow_batch(repository, batch, [], str(uuid4()))
                            identity = str(uuid4())
                            wall, cpu = time.perf_counter(), time.thread_time()
                            control, control_cpu = (
                                budget.control_seconds,
                                budget.control_cpu_seconds,
                            )
                            result = recorder.persist_shadow_batch(
                                repository, batch, [], identity, instance_id="isolated-probe"
                            )
                            elapsed, elapsed_cpu = (
                                time.perf_counter() - wall,
                                time.thread_time() - cpu,
                            )
                            waited = budget.control_seconds - control
                            waited_cpu = budget.control_cpu_seconds - control_cpu
                            assert (result.inserted, result.duplicates) == (
                                (0, page_rows) if workload == "all_conflict" else (page_rows, 0)
                            )
                            current = snapshot(connection)
                            assert sum(current["counts"]) == page_rows
                            verify_packet_rows(current, batch)
                            actual_ids = {key for rows in current["raw"].values() for key in rows}
                            assert set(borrowed.raw_ids) == (
                                set() if workload == "all_conflict" else actual_ids
                            )
                            assert borrowed.audit_ids == [identity] and borrowed.metadata_calls == 1
                            assert_empty_staging(connection, schema)
                            budget.check(force_guard=True)
                            output["samples"].append(
                                {
                                    "variant": "copy" if use_copy else "baseline",
                                    "pair": pair,
                                    "events": page_rows,
                                    "inserted": result.inserted,
                                    "duplicates": result.duplicates,
                                    "wall_seconds_with_controls": elapsed,
                                    "active_wall_seconds": max(0, elapsed - waited),
                                    "control_seconds": waited,
                                    "thread_cpu_seconds_excluding_controls": max(
                                        0, elapsed_cpu - waited_cpu
                                    ),
                                    "phases_seconds": borrowed.metrics,
                                }
                            )
                        finally:
                            trial.rollback()
                output["median_active_seconds"] = {
                    variant: statistics.median(
                        row["active_wall_seconds"]
                        for row in output["samples"]
                        if row["variant"] == variant
                    )
                    for variant in ("baseline", "copy")
                }
                output["backend_after"] = backend_observation(connection)
                output["status"] = (
                    "inconclusive" if pairs == 1 else "comparison_complete_unaccepted"
                )
                output["reason"] = (
                    "single_pair_smoke_only" if pairs == 1 else "not_production_acceptance"
                )
            except Exception as exc:
                failed(exc)
            finally:
                cleanup = True
                try:
                    transaction.rollback()
                    output["rolled_back"] = True
                except Exception as exc:
                    failed(exc)
                    output["rollback_error_type"] = type(exc).__name__
            # Cleanup bypasses guards/deadlines, but is explicitly read-only and bounded.
            try:
                connection.exec_driver_sql("SET TRANSACTION READ ONLY")
                connection.exec_driver_sql("SET LOCAL statement_timeout = '2s'")
                assert (
                    connection.scalar(text("SELECT to_regnamespace(:schema)"), {"schema": schema})
                    is None
                )
                for table in RAW_TABLES:
                    stage = f"pg_temp.sc_{schema[-32:]}_{table.name}"
                    assert (
                        connection.scalar(text("SELECT to_regclass(:stage)"), {"stage": stage})
                        is None
                    )
                output["rollback_verified"] = output["temporary_tables_rollback_verified"] = True
            except Exception as exc:
                failed(exc)
                output["cleanup_verification_error_type"] = type(exc).__name__
            finally:
                connection.rollback()
    except Exception as exc:
        failed(exc)
    output.update(
        raw_rows_attempted=budget.rows,
        raw_pages_attempted=budget.pages,
        guard_checks=budget.guard_checks,
        control_seconds=budget.control_seconds,
        finished_at=datetime.now(UTC).isoformat(),
    )
    return output
