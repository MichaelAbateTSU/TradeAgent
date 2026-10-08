"""Opt-in PostgreSQL collector validation in one disposable, UUID-owned schema.

No AppConfig, dotenv, production URL default, study claim, new protocol/window,
broker/provider requests, or service operations. Supply an EXISTING scratch DB:

python -m tradeagent.kraken_confirmation_pg_validation \
  --scratch-url-env MY_EXISTING_SCRATCH_URL --expected-database scratch_database \
  --expected-major-version 16 \
  --archived-protocol PATH_TO_EXISTING_FROZEN_E05_JSON --allow-schema-writes

Only loopback connections are accepted. Five available connections, READ COMMITTED
isolation, and CREATE/DROP SCHEMA permission are required. The CLI supervises its
own child for at most 240 seconds, plus at most 20 seconds for recovery cleanup.
Pre-existing table fingerprints are bounded to 32 tables, 1,000 rows per table,
and 4 MiB of content. An empty database is not a populated-table witness.
The archived protocol is only a synthetic event-clock fixture; leases always use
real current UTC after PostgreSQL row acquisition. The 112-second concurrency test
uses real PG pool contention and a labeled 110-second pg_sleep reporting stall.
It does not certify provider/account safety, actual full reports, source quality,
available storage or 72-hour endurance. Cleanup targets only the generated schema.
An unavailable database/process kill can prevent cleanup; that is reported, never
called a pass. The generated schema is printed for explicit operator recovery.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import re
import subprocess
import sys
import threading
import time
import zlib
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from typing import Literal
from uuid import uuid4

from psycopg.errors import LockNotAvailable
from pydantic import BaseModel, ConfigDict, Field, JsonValue
from sqlalchemy import delete, event, func, insert, select, text, update
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError, SQLAlchemyError
from sqlalchemy.exc import TimeoutError as PoolTimeout
from sqlalchemy.schema import CreateSchema, DropSchema

from tradeagent.kraken_confirmation import (
    NS,
    SYMBOLS,
    V3_STUDY_ID,
    CausalGrid,
    ChunkHeader,
    ConfirmationProtocol,
    ConfirmationStore,
    Quote,
    confirmation_evidence,
    record,
)
from tradeagent.kraken_confirmation_book import Book, checksum
from tradeagent.kraken_confirmation_runtime import DurableWriter, RuntimeIO
from tradeagent.persistence import Database, worker_locks
from tradeagent.scalping_market import datetime_ns
from tradeagent.scalping_store import canonical, utc

SCHEMA_PREFIX = "confirmation_pg_validation_"
SCHEMA_PATTERN = re.compile(r"confirmation_pg_validation_[0-9a-f]{32}\Z")
CONCURRENCY_SECONDS = 112
REPORT_STALL_SECONDS = 110
ARCHIVE_HASH = "e05f3c559cfc3841c8492500aeebdecbcb2812e0b2f4bc743c549ee9ed560f3a"
CLI_TIMEOUT_SECONDS = 240
RECOVERY_TIMEOUT_SECONDS = 20


def owned_schema(value: str) -> str:
    if SCHEMA_PATTERN.fullmatch(value) is None:
        raise ValueError("only UUID-owned validation schemas are allowed")
    return value


def scratch_address(address: str, expected_database: str) -> str:
    url = make_url(address)
    if (
        url.get_backend_name() != "postgresql"
        or not expected_database
        or url.database != expected_database
        or url.host not in {"127.0.0.1", "::1", "localhost"}
        or any(key in url.query for key in ("host", "hostaddr", "port", "service"))
    ):
        raise ValueError("an explicitly matching loopback PostgreSQL scratch database is required")
    options = url.query.get("options", "")
    if not isinstance(options, str):
        raise ValueError("ambiguous PostgreSQL scratch options")
    return url.set(
        query={
            **url.query,
            "connect_timeout": "5",
            "options": options
            + " -c statement_timeout=150000 -c idle_in_transaction_session_timeout=150000",
        }
    ).render_as_string(hide_password=False)


def schema_marker(name: str) -> str:
    return "confirmation-pg-validation-v1:" + owned_schema(name)


def verify_server(database: Database, expected_database: str, expected_major_version: int) -> int:
    if not 12 <= expected_major_version <= 18:
        raise ValueError("explicit PostgreSQL major version must be 12..18")
    with database.begin() as connection:
        actual_database = connection.exec_driver_sql("SELECT current_database()").scalar_one()
        if actual_database != expected_database:
            raise RuntimeError("server scratch database identity differs")
        version = connection.exec_driver_sql("SHOW server_version_num").scalar_one()
        if not isinstance(version, str) or not version.isdecimal():
            raise RuntimeError("server PostgreSQL version unavailable")
        number = int(version)
        if number // 10000 != expected_major_version:
            raise RuntimeError("server PostgreSQL major version differs")
        isolation = connection.exec_driver_sql("SHOW transaction_isolation").scalar_one()
        if isolation != "read committed":
            raise RuntimeError("collector validation requires READ COMMITTED scratch isolation")
    return number


def table_fingerprints(database: Database) -> dict[str, JsonValue]:
    """Bounded read-only proof of pre-existing user tables; never outputs row data."""
    tables: list[JsonValue] = []
    total_bytes = total_rows = 0
    with database.begin() as connection:
        connection.exec_driver_sql("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        relations = (
            connection.execute(
                text(
                    "SELECT n.nspname, c.relname, c.oid, c.relkind, "
                    "pg_catalog.row_security_active(c.oid) AS rls_active "
                    "FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n "
                    "ON n.oid=c.relnamespace "
                    "WHERE c.relkind IN ('r','p') AND n.nspname !~ '^pg_' "
                    "AND n.nspname <> 'information_schema' ORDER BY n.nspname,c.relname LIMIT 33"
                )
            )
            .mappings()
            .all()
        )
        if len(relations) > 32:
            raise RuntimeError("pre-existing table proof budget exceeded")
        for relation in relations:
            schema, name, oid = relation["nspname"], relation["relname"], relation["oid"]
            if not isinstance(schema, str) or not isinstance(name, str) or not isinstance(oid, int):
                raise RuntimeError("user relation identity malformed")
            if relation["rls_active"] is not False:
                raise RuntimeError("row-level security prevents complete existing-table proof")
            metadata = connection.scalar(
                text(
                    "SELECT json_build_object("
                    "'relation',(SELECT row(c.relowner,c.relacl,c.reloptions,"
                    "c.relrowsecurity,c.relforcerowsecurity,c.relreplident,c.reltablespace) "
                    "FROM pg_catalog.pg_class c WHERE c.oid=:oid),"
                    "'columns',(SELECT json_agg(row(a.attnum,a.attname,a.atttypid,a.atttypmod,"
                    "a.attnotnull,pg_catalog.pg_get_expr(d.adbin,d.adrelid)) ORDER BY a.attnum) "
                    "FROM pg_catalog.pg_attribute a LEFT JOIN pg_catalog.pg_attrdef d "
                    "ON d.adrelid=a.attrelid AND d.adnum=a.attnum "
                    "WHERE a.attrelid=:oid AND a.attnum>0 AND NOT a.attisdropped),"
                    "'constraints',(SELECT json_agg(pg_catalog.pg_get_constraintdef(c.oid) "
                    "ORDER BY c.conname) FROM pg_catalog.pg_constraint c WHERE c.conrelid=:oid),"
                    "'indexes',(SELECT json_agg(pg_catalog.pg_get_indexdef(i.indexrelid) "
                    "ORDER BY i.indexrelid) FROM pg_catalog.pg_index i WHERE i.indrelid=:oid)"
                    ")::text"
                ),
                {"oid": oid},
            )
            if not isinstance(metadata, str):
                raise RuntimeError("user relation metadata unavailable")
            quote = connection.dialect.identifier_preparer.quote
            target = f"{quote(schema)}.{quote(name)}"
            size = connection.exec_driver_sql(
                "SELECT COALESCE(sum(octet_length(row_to_json(t)::text)),0)::bigint "
                f"FROM (SELECT * FROM {target} LIMIT 1001) t"
            ).scalar_one()
            if (
                not isinstance(size, int)
                or total_bytes + len(metadata.encode()) + size > 4 * 1024**2
            ):
                raise RuntimeError("pre-existing content proof byte budget exceeded")
            rows = (
                connection.exec_driver_sql(
                    f"SELECT row_to_json(t)::text FROM {target} t LIMIT 1001"
                )
                .scalars()
                .all()
            )
            if len(rows) > 1000 or any(not isinstance(row, str) for row in rows):
                raise RuntimeError("pre-existing row proof budget exceeded")
            row_text = [str(row) for row in rows]
            total_bytes += len(metadata.encode()) + sum(len(row.encode()) for row in row_text)
            total_rows += len(row_text)
            if total_bytes > 4 * 1024**2:
                raise RuntimeError("pre-existing content proof byte budget exceeded")
            tables.append(
                {
                    "schema": schema,
                    "table": name,
                    "oid": oid,
                    "kind": str(relation["relkind"]),
                    "metadata_sha256": sha256(metadata.encode()).hexdigest(),
                    "rows_sha256": sha256(canonical(sorted(row_text)).encode()).hexdigest(),
                    "row_count": len(row_text),
                }
            )
    return {
        "sha256": sha256(canonical(tables).encode()).hexdigest(),
        "table_count": len(tables),
        "row_count": total_rows,
        "nonempty_existing_table_witness": total_rows > 0,
    }


@dataclass
class IsolationGuard:
    schema: str
    table_mutations: int = 0
    rejected_statements: int = 0

    def statement(
        self,
        connection: object,
        cursor: object,
        statement: str,
        parameters: object,
        context: object,
        executemany: bool,
    ) -> None:
        normalized = " ".join(statement.upper().split())
        schema = owned_schema(self.schema)
        if ";" in statement or re.search(r"\b(public|alembic_version)\s*\.", statement, re.I):
            self.rejected_statements += 1
            raise RuntimeError("validation SQL outside scratch isolation rejected")
        write = normalized.startswith(
            ("INSERT ", "UPDATE ", "DELETE ", "CREATE ", "DROP ", "ALTER ")
        )
        if (
            not write
            and not normalized.startswith(("SELECT ", "SHOW "))
            and normalized
            not in {
                "SET TRANSACTION READ ONLY",
                "SET LOCAL STATEMENT_TIMEOUT = '15000MS'",
                "SET LOCAL LOCK_TIMEOUT = '5000MS'",
            }
        ):
            self.rejected_statements += 1
            raise RuntimeError("unapproved scratch validation statement rejected")
        target = (
            r"^\s*(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM|CREATE\s+TABLE|DROP\s+TABLE|ALTER\s+TABLE)"
            + rf'\s+"?{re.escape(schema)}"?\."?(?:worker_locks|kraken_confirmation_evidence)"?'
            + r"(?=\s|\(|$)"
        )
        if write and re.match(target, statement, re.I) is None:
            self.rejected_statements += 1
            raise RuntimeError("validation mutation lacks its owned schema")
        if write:
            self.table_mutations += 1


@dataclass
class ScratchSchema:
    admin: Database
    name: str
    databases: list[Database]
    guard: IsolationGuard
    created: bool = False
    cleanup_verified: bool = False
    connection_verified: bool = False
    server_version_num: int | None = None
    before_tables: dict[str, JsonValue] = field(default_factory=dict)
    after_tables: dict[str, JsonValue] = field(default_factory=dict)

    def database(self, address: str) -> Database:
        database = Database(address, pool_size=1)
        database.engine = database.engine.execution_options(
            schema_translate_map={None: owned_schema(self.name)},
        )
        event.listen(database.engine, "before_cursor_execute", self.guard.statement)
        self.databases.append(database)
        return database

    def cleanup(self) -> None:
        for database in self.databases:
            database.dispose()
        with self.admin.begin() as connection:
            connection.exec_driver_sql("SET LOCAL lock_timeout = '5000ms'")
            exists = connection.scalar(
                text("SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname=:name)"),
                {"name": self.name},
            )
            if exists is True:
                marker = connection.scalar(
                    text(
                        "SELECT pg_catalog.obj_description(n.oid,'pg_namespace') "
                        "FROM pg_catalog.pg_namespace n WHERE n.nspname=:name"
                    ),
                    {"name": self.name},
                )
                if marker != schema_marker(self.name):
                    raise RuntimeError(
                        "unconfirmed schema ownership needs explicit operator recovery"
                    )
                name = owned_schema(self.name)
                for table_name in ("worker_locks", "kraken_confirmation_evidence"):
                    connection.exec_driver_sql(f"DROP TABLE IF EXISTS {name}.{table_name} RESTRICT")
                connection.execute(DropSchema(name, cascade=False))
            elif exists is not False:
                raise RuntimeError("scratch schema existence could not be determined")
        self.created = False
        with self.admin.begin() as connection:
            exists = connection.scalar(
                text("SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname=:name)"),
                {"name": self.name},
            )
        self.cleanup_verified = exists is False
        if self.before_tables:
            self.after_tables = table_fingerprints(self.admin)
            if self.before_tables != self.after_tables:
                raise RuntimeError("pre-existing user tables changed during validation")


@contextmanager
def isolated_schema(
    address: str,
    expected_database: str,
    *,
    state: list[ScratchSchema] | None = None,
    expected_major_version: int,
    schema_name: str | None = None,
) -> Iterator[ScratchSchema]:
    checked = scratch_address(address, expected_database)
    admin = Database(checked, pool_size=1)
    scratch = ScratchSchema(
        admin=admin,
        name=owned_schema(schema_name) if schema_name is not None else SCHEMA_PREFIX + uuid4().hex,
        databases=[],
        guard=IsolationGuard(SCHEMA_PREFIX + "0" * 32),
    )
    scratch.guard.schema = scratch.name
    if state is not None:
        state.append(scratch)
    try:
        scratch.server_version_num = verify_server(admin, expected_database, expected_major_version)
        scratch.connection_verified = True
        scratch.before_tables = table_fingerprints(admin)
        with admin.begin() as connection:
            connection.execute(CreateSchema(owned_schema(scratch.name)))
            connection.exec_driver_sql(
                f"COMMENT ON SCHEMA {owned_schema(scratch.name)} IS '{schema_marker(scratch.name)}'"
            )
        scratch.created = True
        writer = scratch.database(checked)
        worker_locks.create(writer.engine)
        confirmation_evidence.create(writer.engine)
        yield scratch
    finally:
        try:
            if scratch.connection_verified:
                scratch.cleanup()
        finally:
            admin.dispose()


def seed(store: ConfirmationStore) -> None:
    """Local diagnostic rows only; no protocol record or claim API."""
    now = datetime.now(UTC)
    body = canonical({"pg_validation_only": True}).encode()
    blob = zlib.compress(body)
    header = ChunkHeader(
        study_id=V3_STUDY_ID,
        sequence=0,
        key="pg-validation-seed",
        kind="pg_validation_seed",
        owner_id=store.owner_id,
        at_ns=datetime_ns(now),
        previous_hash=sha256(store.owner_id.encode()).hexdigest(),
        payload_hash=sha256(blob).hexdigest(),
        chain_hash="",
        decoded_bytes=len(body),
        stored_bytes=len(blob) + 512,
    )
    header = header.model_copy(update={"chain_hash": header.calculated_hash()})
    with store.database.begin() as connection:
        connection.execute(delete(confirmation_evidence))
        connection.execute(delete(worker_locks))
        connection.execute(
            insert(worker_locks).values(
                lock_name=store.lock_name,
                owner_id=store.owner_id,
                acquired_at=now,
            )
        )
        connection.execute(
            insert(confirmation_evidence).values(
                **header.model_dump(),
                payload=blob,
            )
        )


def _row_time(store: ConfirmationStore) -> datetime:
    with store.database.begin() as connection:
        value = connection.scalar(
            select(worker_locks.c.acquired_at).where(
                worker_locks.c.lock_name == store.lock_name,
            )
        )
    if not isinstance(value, datetime):
        raise RuntimeError("scratch lease timestamp missing")
    return utc(value)


def _pid(database: Database) -> int:
    with database.begin() as connection:
        value = connection.exec_driver_sql("SELECT pg_backend_pid()").scalar_one()
    if not isinstance(value, int):
        raise RuntimeError("scratch backend PID missing")
    return value


def wait_for_block(admin: Database, target_pid: int, blocker_pid: int) -> None:
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        with admin.begin() as connection:
            blocked = connection.scalar(
                text("SELECT :blocker = ANY(pg_catalog.pg_blocking_pids(:target))"),
                {"blocker": blocker_pid, "target": target_pid},
            )
        if blocked is True:
            return
        time.sleep(0.01)
    raise RuntimeError("expected real PostgreSQL row wait was not observed")


def row_lock_checks(
    scratch: ScratchSchema,
    worker: Database,
    contender: Database,
) -> dict[str, JsonValue]:
    store = ConfirmationStore(contender, "pg-validator", study_id=V3_STUDY_ID)
    seed(store)
    target_pid = _pid(contender)
    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="pg-row-fence") as executor:
        # Old event timestamps stay unchanged even though row acquisition waits.
        at_ns = time.time_ns()
        payload: dict[str, JsonValue] = {"original_event_ns": at_ns - 1234567}
        with worker.begin() as blocker:
            blocker_pid = blocker.exec_driver_sql("SELECT pg_backend_pid()").scalar_one()
            blocker.execute(select(worker_locks).with_for_update()).all()
            future = executor.submit(
                store.append,
                "pg_event",
                "waited-event",
                payload,
                at_ns=at_ns,
            )
            wait_for_block(scratch.admin, target_pid, int(blocker_pid))
        header = future.result(timeout=10)
        _, retained = store.read_chunk(header.sequence)
        if header.at_ns != at_ns or retained != payload:
            raise RuntimeError("waiting for a lock retimestamped original evidence")
        seed(store)
        with worker.begin() as blocker:
            blocker_pid = blocker.exec_driver_sql("SELECT pg_backend_pid()").scalar_one()
            blocker.execute(select(worker_locks).with_for_update()).all()
            healthy_refresh = executor.submit(store.refresh)
            wait_for_block(scratch.admin, target_pid, int(blocker_pid))
            released_at = datetime.now(UTC)
        healthy_refresh.result(timeout=10)
        if _row_time(store) < released_at:
            raise RuntimeError("renewal timestamp was sampled before row-lock release")
        # Initial age is valid; physical PG wait crosses the unchanged 90s fence.
        seed(store)
        acquired = datetime.now(UTC) - timedelta(seconds=89)
        with worker.begin() as connection:
            connection.execute(update(worker_locks).values(acquired_at=acquired))
        before = datetime.now(UTC)
        if not 0 <= (before - acquired).total_seconds() < 90:
            raise RuntimeError("expiry test did not start inside the actual lease boundary")
        with worker.begin() as blocker:
            blocker_pid = blocker.exec_driver_sql("SELECT pg_backend_pid()").scalar_one()
            blocker.execute(select(worker_locks).with_for_update()).all()
            future_refresh = executor.submit(store.refresh)
            wait_for_block(scratch.admin, target_pid, int(blocker_pid))
            time.sleep(1.25)
        try:
            future_refresh.result(timeout=10)
        except RuntimeError as error:
            if str(error) != "confirmation_lease_expired":
                raise
        else:
            raise RuntimeError("expired after-lock lease was incorrectly renewed")
        if _row_time(store) != acquired:
            raise RuntimeError("expired lease was mutated")
        try:
            store.append("pg_event", "expired-writer", {}, at_ns=time.time_ns())
        except RuntimeError as error:
            if str(error) != "confirmation_lease_expired":
                raise
        else:
            raise RuntimeError("expired writer appended diagnostic evidence")
        if len(list(store.chunks())) != 1:
            raise RuntimeError("expired-owner journal changed")
        # Preserve the real collector's five-second row-lock timeout as well.
        seed(store)
        initial_time = _row_time(store)
        with worker.begin() as blocker:
            blocker_pid = blocker.exec_driver_sql("SELECT pg_backend_pid()").scalar_one()
            blocker.execute(select(worker_locks).with_for_update()).all()
            timeout_attempt = executor.submit(store.refresh)
            wait_for_block(scratch.admin, target_pid, int(blocker_pid))
            try:
                timeout_attempt.result(timeout=8)
            except DBAPIError as error:
                if not isinstance(error.orig, LockNotAvailable):
                    raise
            else:
                raise RuntimeError("blocked row renewal did not fail at collector lock timeout")
        if _row_time(store) != initial_time:
            raise RuntimeError("row-lock timeout mutated its lease")
        # Owner changes committed by the blocker must fence the waiting writer.
        seed(store)
        count_before = len(list(store.chunks()))
        with worker.begin() as blocker:
            blocker_pid = blocker.exec_driver_sql("SELECT pg_backend_pid()").scalar_one()
            blocker.execute(select(worker_locks).with_for_update()).all()
            blocker.execute(update(worker_locks).values(owner_id="different-owner"))
            lost = executor.submit(
                store.append,
                "pg_event",
                "forbidden-after-owner-loss",
                {},
                at_ns=time.time_ns(),
            )
            wait_for_block(scratch.admin, target_pid, int(blocker_pid))
        try:
            lost.result(timeout=10)
        except RuntimeError as error:
            if str(error) != "confirmation_lease_lost":
                raise
        else:
            raise RuntimeError("lost owner appended scratch evidence")
        if len(list(store.chunks())) != count_before:
            raise RuntimeError("lost-owner journal changed")
    return {
        "actual_pg_row_wait_observed": True,
        "healthy_renewal_timestamp_after_real_lock_release": True,
        "after_lock_real_wall_clock_expiry_rejected": True,
        "expired_timestamp_unchanged": True,
        "expired_writer_append_rejected": True,
        "actual_collector_five_second_lock_timeout_rejected": True,
        "blocked_owner_change_fenced": True,
        "original_event_and_header_timestamps_preserved": True,
    }


def _quote(symbol: Literal["BTC/USD", "ETH/USD"], at: int, frame: int) -> Quote:
    size = Decimal(".011")
    book = Book().apply(
        {
            "bids": [{"price": "999", "qty": ".011"}],
            "asks": [{"price": "1000", "qty": ".011"}],
            "checksum": checksum({Decimal(999): size}, {Decimal(1000): size}),
        },
        "snapshot",
        at,
    )
    if book is None:
        raise RuntimeError("synthetic quote construction failed")
    return Quote.model_validate(
        {
            **book.model_dump(),
            "symbol": symbol,
            "frame_id": frame,
            "connection_id": "pg-validation-synthetic",
            "epoch": 0,
            "received_ns": at,
            "accepted_ns": at,
            "received_monotonic_ns": at,
        }
    )


def synthetic_grid(protocol: ConfirmationProtocol) -> CausalGrid:
    grid = CausalGrid(protocol)
    before_start = datetime_ns(protocol.start) - NS // 10
    for frame, symbol in enumerate(SYMBOLS, start=1):
        grid.accept(_quote(symbol, before_start, frame))
    return grid


async def concurrency_checks(
    scratch: ScratchSchema,
    address: str,
    protocol: ConfirmationProtocol,
) -> dict[str, JsonValue]:
    writer_database = scratch.databases[0]
    lease_database = scratch.database(address)
    report_database = scratch.database(address)
    safety_database = scratch.database(address)
    store = ConfirmationStore(writer_database, "pg-concurrency-validator", study_id=V3_STUDY_ID)
    seed(store)
    lease_store = ConfirmationStore(lease_database, store.owner_id, study_id=V3_STUDY_ID)
    io = RuntimeIO(
        writer_database,
        lease_database=lease_database,
        safety_database=safety_database,
        report_database=report_database,
    )
    writer = DurableWriter(store)
    writer.task = asyncio.create_task(writer.work())
    grid = synthetic_grid(protocol)
    stop = asyncio.Event()
    entered = threading.Event()
    report_elapsed = 0.0
    renewals = 0
    max_gap = 0.0
    last_renewal = 0.0
    labels = evaluations_count = missed = ring_misses = 0
    maximum_resolution_delay = 0

    async def heartbeat() -> None:
        nonlocal renewals, max_gap, last_renewal
        while not stop.is_set():
            await io.call("lease", lease_store.refresh)
            at = time.monotonic()
            if last_renewal:
                max_gap = max(max_gap, at - last_renewal)
            last_renewal = at
            renewals += 1
            try:
                await asyncio.wait_for(stop.wait(), 10)
            except TimeoutError:
                continue

    def stalled_report() -> None:
        nonlocal report_elapsed
        started = time.monotonic()
        with report_database.begin() as connection:
            connection.exec_driver_sql("SET TRANSACTION READ ONLY")
            connection.scalar(select(func.count()).select_from(confirmation_evidence))
            entered.set()
            connection.execute(
                text("SELECT pg_catalog.pg_sleep(:seconds)"),
                {
                    "seconds": REPORT_STALL_SECONDS,
                },
            ).all()
        report_elapsed = time.monotonic() - started

    def competing_checkout() -> bool:
        try:
            with report_database.begin():
                pass
        except PoolTimeout:
            return True
        raise RuntimeError("busy single-connection report pool did not apply backpressure")

    report_task = asyncio.create_task(io.call("report", stalled_report))
    pulse = asyncio.create_task(heartbeat())
    pool_task: asyncio.Task[bool] | None = None
    try:
        ready_deadline = time.monotonic() + 10
        while not entered.is_set():
            if report_task.done():
                await report_task
            if time.monotonic() > ready_deadline:
                raise RuntimeError("report lane did not occupy its PG connection")
            await asyncio.sleep(0.01)
        pool_task = asyncio.create_task(asyncio.to_thread(competing_checkout))
        await asyncio.wait_for(
            io.call(
                "safety",
                lambda: _pid(safety_database),
            ),
            5,
        )
        started = time.monotonic()
        synthetic_start = datetime_ns(protocol.start)
        frame = len(SYMBOLS) + 1
        while time.monotonic() - started < CONCURRENCY_SECONDS:
            for task in (pulse, writer.task):
                if task.done():
                    await task
                    raise RuntimeError("essential scratch collector component stopped")
            if report_task.done():
                await report_task
            at_ns = synthetic_start + int((time.monotonic() - started) * NS)
            for symbol in SYMBOLS:
                grid.accept(_quote(symbol, at_ns, frame))
                frame += 1
            evaluations, resolved = grid.advance(at_ns)
            evaluations_count += len(evaluations)
            missed += sum(row.missed_scheduled_slot for row in evaluations)
            for row in resolved:
                labels += 1
                ring_misses += int(row.future is None)
                maximum_resolution_delay = max(
                    maximum_resolution_delay,
                    row.resolved_ns - row.information_available_ns,
                )
                if not row.eligibility.complete:
                    raise RuntimeError("synthetic causal label lost eligibility during reporting")
            for kind, rows in (("pg_evaluation", evaluations), ("pg_label", resolved)):
                if rows:
                    await writer.submit(kind, tuple(record(row) for row in rows))
            await asyncio.sleep(0.1)
        await report_task
        if pool_task is None or await pool_task is not True:
            raise RuntimeError("expected real report-pool exhaustion was not demonstrated")
        await writer.close()

        def verify_journal() -> tuple[int, int, int]:
            journal = ConfirmationStore(
                report_database,
                store.owner_id,
                study_id=V3_STUDY_ID,
            )
            chunks = stored_evaluations = stored_labels = 0
            for header, payload in journal.chunks(verify_payloads=True):
                chunks += 1
                if header.kind not in {"pg_evaluation", "pg_label"}:
                    continue
                records = payload.get("records") if payload is not None else None
                if not isinstance(records, list):
                    raise RuntimeError("scratch journal records missing")
                if header.kind == "pg_evaluation":
                    stored_evaluations += len(records)
                else:
                    stored_labels += len(records)
            return chunks, stored_evaluations, stored_labels

        chunks, stored_evaluations, stored_labels = await io.call("report", verify_journal)
        if stored_evaluations != evaluations_count or stored_labels != labels:
            raise RuntimeError("durable scratch journal lost scheduled evidence")
        await io.call("lease", lease_store.refresh)
        stop.set()
        await pulse
        if (
            missed
            or ring_misses
            or labels == 0
            or report_elapsed < REPORT_STALL_SECONDS
            or renewals < 10
            or max_gap >= 25
            or maximum_resolution_delay >= 2 * NS
        ):
            raise RuntimeError("PG concurrency/cadence/renewal acceptance failed")
        return {
            "report_pg_sleep_seconds": REPORT_STALL_SECONDS,
            "actual_report_lane_elapsed_seconds": report_elapsed,
            "real_report_pool_exhaustion_rejected": True,
            "separate_safety_pool_responsive": True,
            "real_wall_clock_lease_renewals": renewals,
            "maximum_renewal_gap_seconds": max_gap,
            "synthetic_labels": labels,
            "synthetic_evaluations": evaluations_count,
            "verified_journal_chunks": chunks,
            "stored_evaluations": stored_evaluations,
            "stored_labels": stored_labels,
            "missed_scheduled_slots": missed,
            "synthetic_ring_expiry_misses": ring_misses,
            "maximum_synthetic_resolution_delay_ns": maximum_resolution_delay,
            "maximum_writer_queue": writer.maximum_queue,
            "event_clock": "archived fixture plus elapsed monotonic time; not native observations",
        }
    finally:
        stop.set()
        if writer.task is not None and not writer.task.done():
            writer.task.cancel()
        await asyncio.gather(pulse, writer.task, return_exceptions=True)
        # PG statement timeout bounds the real reporting query; wait before DROP
        # rather than abandon a database thread which still owns a connection.
        await asyncio.gather(report_task, return_exceptions=True)
        if pool_task is not None:
            await asyncio.gather(pool_task, return_exceptions=True)
        await io.close()


class ValidationReport(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    schema_version: Literal["confirmation-pg-validation-v1"] = "confirmation-pg-validation-v1"
    status: Literal["passed", "failed", "unavailable", "skipped"]
    pg_lock_pool_concurrency_validated: bool = False
    source_quality_certified: Literal[False] = False
    full_report_endurance_certified: Literal[False] = False
    formal_claim_created: Literal[False] = False
    existing_table_writes: Literal[0] = 0
    scratch_schema: str | None = None
    cleanup_verified: bool | None = None
    checks: dict[str, JsonValue] = Field(default_factory=dict)
    error_type: str | None = None
    error_code: str | None = None
    expected_database: str | None = None
    server_version_num: int | None = None
    existing_tables_unchanged: bool | None = None
    limitations: tuple[str, ...] = (
        "Dedicated scratch PostgreSQL only; not production/provider/account certification.",
        "Synthetic event-clock fixture, 110s deterministic PG reporting stall, not a full report.",
        "No free-capacity, network-outage, crash recovery or 72h endurance certification.",
        "A killed process/unavailable server can prevent cleanup; never treat that as a pass.",
    )


def validate_postgres(
    address: str,
    expected_database: str,
    protocol: ConfirmationProtocol,
    *,
    allow_schema_writes: bool = False,
    expected_major_version: int,
    schema_name: str | None = None,
) -> ValidationReport:
    if not allow_schema_writes:
        return ValidationReport(status="skipped", error_type="ScratchSchemaWriteOptInRequired")
    if protocol.identity != ARCHIVE_HASH:
        return ValidationReport(status="failed", error_type="ArchivedFixtureIdentityMismatch")
    scratch: ScratchSchema | None = None
    state: list[ScratchSchema] = []
    checks: dict[str, JsonValue] = {
        "archived_protocol_identity": protocol.identity,
        "active_phase": "scratch_setup",
    }
    try:
        checked_address = scratch_address(address, expected_database)
        with isolated_schema(
            address,
            expected_database,
            state=state,
            expected_major_version=expected_major_version,
            schema_name=schema_name,
        ) as scratch:
            worker = scratch.databases[0]
            contender = scratch.database(checked_address)
            checks["active_phase"] = "row_locking"
            checks["row_locking"] = row_lock_checks(scratch, worker, contender)
            contender.dispose()
            checks["active_phase"] = "concurrency"
            checks["concurrency"] = asyncio.run(
                concurrency_checks(
                    scratch,
                    checked_address,
                    protocol,
                )
            )
            checks["existing_tables_before"] = scratch.before_tables
            checks["active_phase"] = "cleanup"
        if not scratch.cleanup_verified:
            raise RuntimeError("scratch schema removal could not be verified")
        checks["existing_tables_after"] = scratch.after_tables
        checks["active_phase"] = "closed"
        return ValidationReport(
            status="passed",
            pg_lock_pool_concurrency_validated=True,
            scratch_schema=scratch.name,
            cleanup_verified=True,
            checks=checks,
            expected_database=expected_database,
            server_version_num=scratch.server_version_num,
            existing_tables_unchanged=scratch.before_tables == scratch.after_tables,
        )
    except (ValueError, RuntimeError, OSError, SQLAlchemyError) as error:
        if scratch is None and state:
            scratch = state[0]
        return ValidationReport(
            status="failed"
            if scratch is not None and scratch.connection_verified
            else "unavailable",
            scratch_schema=scratch.name if scratch is not None else None,
            cleanup_verified=scratch.cleanup_verified if scratch is not None else None,
            checks=checks,
            error_type=type(error).__name__,
            error_code=(
                str(error)
                if isinstance(error, RuntimeError)
                and str(error)
                in {
                    "synthetic causal label lost eligibility during reporting",
                    "PG concurrency/cadence/renewal acceptance failed",
                    "durable scratch journal lost scheduled evidence",
                    "confirmation_lease_lost",
                    "confirmation_lease_expired",
                }
                else None
            ),
            expected_database=expected_database,
            server_version_num=scratch.server_version_num if scratch is not None else None,
            existing_tables_unchanged=(
                scratch.before_tables == scratch.after_tables
                if scratch is not None and scratch.before_tables and scratch.after_tables
                else None
            ),
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scratch-url-env", required=True)
    parser.add_argument("--expected-database", required=True)
    parser.add_argument("--archived-protocol", type=Path, required=True)
    parser.add_argument("--allow-schema-writes", action="store_true")
    parser.add_argument("--expected-major-version", type=int, required=True, choices=range(12, 19))
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--owned-schema", help=argparse.SUPPRESS)
    parser.add_argument("--cleanup-only", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if not args.allow_schema_writes:
        result = ValidationReport(status="skipped", error_type="ScratchSchemaWriteOptInRequired")
    elif "SCRATCH" not in args.scratch_url_env.upper():
        result = ValidationReport(
            status="failed", error_type="ExplicitScratchEnvironmentNameRequired"
        )
    else:
        address = os.getenv(args.scratch_url_env)
        if not address:
            result = ValidationReport(
                status="unavailable", error_type="ScratchConnectionNotProvided"
            )
        else:
            try:
                if args.cleanup_only:
                    if not args.worker or args.owned_schema is None:
                        raise ValueError("cleanup requires the supervised owned schema")
                    result = recover_schema(
                        address,
                        args.expected_database,
                        args.expected_major_version,
                        owned_schema(args.owned_schema),
                    )
                    print(result.model_dump_json(), flush=True)
                    raise SystemExit(2)
                with args.archived_protocol.open("rb") as stream:
                    value = stream.read(32769)
                if len(value) > 32768:
                    raise ValueError("archived fixture byte budget exceeded")
                protocol = ConfirmationProtocol.model_validate_json(value)
                if not args.worker:
                    result = supervise(args)
                else:
                    if args.owned_schema is None:
                        raise ValueError("supervised worker requires an owned schema")
                    result = validate_postgres(
                        address,
                        args.expected_database,
                        protocol,
                        allow_schema_writes=True,
                        expected_major_version=args.expected_major_version,
                        schema_name=owned_schema(args.owned_schema),
                    )
            except (ValueError, OSError) as error:
                result = ValidationReport(status="failed", error_type=type(error).__name__)
    print(result.model_dump_json(), flush=True)
    raise SystemExit(0 if result.status == "passed" else 2)


def supervise(args: argparse.Namespace) -> ValidationReport:
    """One owned child, bounded execution; never manages Docker or cloud resources."""
    name = SCHEMA_PREFIX + uuid4().hex
    command = [
        sys.executable,
        "-m",
        "tradeagent.kraken_confirmation_pg_validation",
        "--scratch-url-env",
        args.scratch_url_env,
        "--expected-database",
        args.expected_database,
        "--expected-major-version",
        str(args.expected_major_version),
        "--archived-protocol",
        str(args.archived_protocol),
        "--allow-schema-writes",
        "--worker",
        "--owned-schema",
        name,
    ]
    try:
        child = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
            timeout=CLI_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        return recover_child(command, name, "ValidationHardTimeout")
    except OSError as error:
        return ValidationReport(status="unavailable", error_type=type(error).__name__)
    try:
        report = ValidationReport.model_validate_json(child.stdout)
    except ValueError:
        return recover_child(command, name, "WorkerDidNotReturnValidationJSON")
    if report.scratch_schema not in (None, name):
        return recover_child(command, name, "WorkerReturnedDifferentSchema")
    if report.status == "passed" and (
        not report.pg_lock_pool_concurrency_validated
        or report.cleanup_verified is not True
        or report.scratch_schema != name
        or report.expected_database != args.expected_database
        or report.server_version_num is None
        or report.server_version_num // 10000 != args.expected_major_version
        or report.existing_tables_unchanged is not True
    ):
        return recover_child(command, name, "WorkerReturnedIncompleteCertification")
    if report.cleanup_verified is False:
        recovered = recover_child(command, name, report.error_type or "WorkerCleanupUnverified")
        return report.model_copy(
            update={
                "status": "failed",
                "pg_lock_pool_concurrency_validated": False,
                "scratch_schema": name,
                "cleanup_verified": recovered.cleanup_verified,
                "error_type": recovered.error_type,
            }
        )
    if child.returncode != 0 and report.status == "passed":
        return report.model_copy(
            update={
                "status": "failed",
                "pg_lock_pool_concurrency_validated": False,
                "error_type": "WorkerExitDisagreesWithReport",
            }
        )
    return report


def recover_child(command: list[str], name: str, failure: str) -> ValidationReport:
    """A second bounded child checks DB identity and marker before any DROP."""
    try:
        child = subprocess.run(
            [*command, "--cleanup-only"],
            capture_output=True,
            text=True,
            check=False,
            timeout=RECOVERY_TIMEOUT_SECONDS,
        )
        report = ValidationReport.model_validate_json(child.stdout)
    except (subprocess.TimeoutExpired, OSError, ValueError):
        return ValidationReport(
            status="failed",
            scratch_schema=name,
            cleanup_verified=False,
            error_type=failure + "CleanupUnverified",
        )
    verified = report.scratch_schema == name and report.cleanup_verified is True
    return ValidationReport(
        status="failed",
        scratch_schema=name,
        cleanup_verified=verified,
        error_type=failure if verified else failure + "CleanupUnverified",
    )


def recover_schema(
    address: str,
    expected_database: str,
    expected_major_version: int,
    name: str,
) -> ValidationReport:
    database: Database | None = None
    try:
        checked = scratch_address(address, expected_database)
        database = Database(checked, pool_size=1)
        version = verify_server(database, expected_database, expected_major_version)
        scope = ScratchSchema(database, owned_schema(name), [], IsolationGuard(name))
        scope.cleanup()
        return ValidationReport(
            status="failed",
            scratch_schema=name,
            cleanup_verified=scope.cleanup_verified,
            expected_database=expected_database,
            server_version_num=version,
            error_type="InterruptedValidationSchemaRemoved",
        )
    except (ValueError, RuntimeError, OSError, SQLAlchemyError) as error:
        return ValidationReport(
            status="failed",
            scratch_schema=name,
            cleanup_verified=False,
            error_type=type(error).__name__,
        )
    finally:
        if database is not None:
            database.dispose()


if __name__ == "__main__":
    main()
