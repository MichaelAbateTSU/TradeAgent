"""Offline PG-validator tests. These never inspect credentials or connect to PG."""

from __future__ import annotations

import argparse
import json
import subprocess
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from uuid import UUID

import pytest
from sqlalchemy.engine import make_url
from sqlalchemy.schema import CreateSchema, DropSchema

import tradeagent.kraken_confirmation_pg_validation as validation
from tradeagent.kraken_confirmation import NS, SYMBOLS, CausalGrid, ConfirmationProtocol
from tradeagent.persistence import Database


def archived() -> ConfirmationProtocol:
    path = (
        Path(__file__).parents[1]
        / "docs"
        / "experiments"
        / ("2026-10-book-confirmation-72h-v2")
        / "frozen-protocol.json"
    )
    return ConfirmationProtocol.model_validate_json(path.read_bytes())


@pytest.mark.parametrize(
    "address",
    [
        "sqlite://",
        "sqlite:///scratch.sqlite",
        "mysql://synthetic@localhost/scratch",
        "postgresql+psycopg://synthetic@localhost/not-the-expected-database",
        "postgresql+psycopg://synthetic@remote.example/scratch",
        "postgresql+psycopg://synthetic@127.0.0.1/scratch?host=remote.example",
        "postgresql+psycopg://synthetic@127.0.0.1/scratch?hostaddr=192.0.2.1",
        "postgresql+psycopg://synthetic@127.0.0.1/scratch?service=production",
        "postgresql+psycopg://synthetic@127.0.0.1/scratch?port=5432",
    ],
)
def test_no_backend_fallback_or_database_guess(address: str) -> None:
    with pytest.raises(ValueError, match="explicitly matching"):
        validation.scratch_address(address, "scratch")


@pytest.mark.parametrize(
    "name",
    [
        "public",
        "alembic_version",
        "confirmation_pg_validation_*",
        "confirmation_pg_validation_abc; DROP SCHEMA public",
        "confirmation_pg_validation_" + "a" * 31,
    ],
)
def test_cleanup_identifier_is_only_a_generated_uuid_schema(name: str) -> None:
    with pytest.raises(ValueError, match="UUID-owned"):
        validation.owned_schema(name)


def test_drop_ddl_cannot_target_public_or_unnamed_tables() -> None:
    name = validation.SCHEMA_PREFIX + "a" * 32
    create = str(CreateSchema(validation.owned_schema(name)).compile())
    drop = str(DropSchema(validation.owned_schema(name), cascade=False).compile())
    assert name in create and drop.endswith(name) and "CASCADE" not in drop
    assert "public" not in drop and "*" not in drop


def test_table_mutations_require_exact_owned_target_not_a_schema_in_a_comment() -> None:
    name = validation.SCHEMA_PREFIX + "a" * 32
    guard = validation.IsolationGuard(name)
    statements = [
        "UPDATE public.worker_locks SET owner_id='changed'",
        f"UPDATE worker_locks SET owner_id='changed' /* {name}. */",
        f"DELETE FROM {name}.worker_locks; DELETE FROM public.worker_locks",
        "DELETE FROM alembic_version",
        f"INSERT INTO {name}.not-a-validation-table VALUES(1)",
        "TRUNCATE worker_locks",
        "SET search_path TO public",
    ]
    for statement in statements:
        with pytest.raises(RuntimeError, match=r"rejected|owned schema"):
            guard.statement(
                None,
                object(),
                statement,
                (),
                object(),
                False,
            )
    assert guard.rejected_statements == len(statements)
    assert guard.table_mutations == 0
    for statement in (
        f"INSERT INTO {name}.worker_locks (lock_name) VALUES (%s)",
        f'DELETE FROM "{name}"."kraken_confirmation_evidence"',
        f"CREATE TABLE {name}.worker_locks (lock_name VARCHAR(128))",
    ):
        guard.statement(
            None,
            object(),
            statement,
            (),
            object(),
            False,
        )
    assert guard.table_mutations == 3


def test_no_write_optin_means_skip_before_connection_or_fixture_inspection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def prohibited(address: str, *, pool_size: int) -> Database:
        raise AssertionError("database construction must not happen")

    monkeypatch.setattr(validation, "Database", prohibited)
    result = validation.validate_postgres(
        "not-read",
        "not-read",
        archived(),
        expected_major_version=16,
    )
    assert result.status == "skipped"
    assert result.pg_lock_pool_concurrency_validated is False
    assert result.existing_table_writes == 0
    assert not result.formal_claim_created


def test_invalid_target_is_unavailable_not_sqlite_pass() -> None:
    result = validation.validate_postgres(
        "sqlite://",
        "scratch",
        archived(),
        allow_schema_writes=True,
        expected_major_version=16,
    )
    assert result.status == "unavailable"
    assert result.pg_lock_pool_concurrency_validated is False
    assert result.cleanup_verified is None


def test_injected_pg_pools_have_schema_translation_without_connecting() -> None:
    address = validation.scratch_address(
        "postgresql+psycopg://synthetic:synthetic@127.0.0.1/scratch",
        "scratch",
    )
    name = validation.SCHEMA_PREFIX + "a" * 32
    admin = Database(address, pool_size=1)
    scope = validation.ScratchSchema(admin, name, [], validation.IsolationGuard(name))
    try:
        first = scope.database(address)
        second = scope.database(address)
        assert first.engine.pool is not second.engine.pool
        assert first.engine.get_execution_options()["schema_translate_map"] == {None: name}
        assert second.engine.get_execution_options()["schema_translate_map"] == {None: name}
    finally:
        for database in scope.databases:
            database.dispose()
        admin.dispose()


def test_missing_cli_connection_is_honestly_unavailable_without_reading_fixture(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "pg-validation",
            "--scratch-url-env",
            "MY_EXISTING_SCRATCH_URL",
            "--expected-database",
            "scratch",
            "--expected-major-version",
            "16",
            "--archived-protocol",
            "not-read",
            "--allow-schema-writes",
        ],
    )
    monkeypatch.delenv("MY_EXISTING_SCRATCH_URL", raising=False)
    with pytest.raises(SystemExit) as stopped:
        validation.main()
    assert stopped.value.code == 2
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "unavailable"
    assert result["error_type"] == "ScratchConnectionNotProvided"
    assert result["pg_lock_pool_concurrency_validated"] is False


def test_cli_never_defaults_to_a_production_environment_name(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "pg-validation",
            "--scratch-url-env",
            "TRADEAGENT_DATABASE_URL",
            "--expected-database",
            "not-read",
            "--expected-major-version",
            "16",
            "--archived-protocol",
            "not-read",
            "--allow-schema-writes",
        ],
    )
    with pytest.raises(SystemExit):
        validation.main()
    result = json.loads(capsys.readouterr().out)
    assert result["error_type"] == "ExplicitScratchEnvironmentNameRequired"


def test_real_clock_and_limits_are_not_overridden() -> None:
    protocol = archived()
    first = validation._quote("BTC/USD", protocol.slot_ns(0), 1)
    assert first.provider_book_update_ns == first.received_ns == first.accepted_ns
    assert validation.CONCURRENCY_SECONDS > validation.REPORT_STALL_SECONDS > 90
    assert protocol.freshness_seconds == 2 and protocol.notional_usd.as_tuple().digits == (
        1,
        0,
        2,
        5,
    )


def test_synthetic_fixture_needs_causal_pre_slot_quote_not_a_later_first_capture() -> None:
    protocol = archived()
    at = protocol.slot_ns(0)
    empty = CausalGrid(protocol)
    primed = validation.synthetic_grid(protocol)
    for grid in (empty, primed):
        for frame, symbol in enumerate(SYMBOLS, start=3):
            grid.accept(validation._quote(symbol, at + NS // 1000, frame))
        evaluations, _ = grid.advance(at + NS // 1000)
        assert all((row.entry is None) == (grid is empty) for row in evaluations)
        for frame, symbol in enumerate(SYMBOLS, start=5):
            grid.accept(validation._quote(symbol, at + 60 * NS, frame))
        _, labels = grid.advance(at + 62 * NS)
        assert len(labels) == 2
        assert all(row.eligibility.complete == (grid is primed) for row in labels)
        assert all(row.future is not None for row in labels)
        assert all(row.evaluation.at_ns == at for row in labels)


def test_loopback_port_and_timeout_are_explicit_not_url_query_redirects() -> None:
    url = make_url(
        validation.scratch_address(
            "postgresql+psycopg://synthetic@127.0.0.1:55432/scratch",
            "scratch",
        )
    )
    assert url.host == "127.0.0.1" and url.port == 55432
    assert url.query["connect_timeout"] == "5"
    assert "statement_timeout=150000" in url.query["options"]


class FakeResult:
    def __init__(
        self,
        value: object = None,
        rows: list[dict[str, object]] | None = None,
        values: list[object] | None = None,
    ) -> None:
        self.value = value
        self.rows = rows if rows is not None else []
        self.values = values if values is not None else []

    def scalar_one(self) -> object:
        return self.value

    def mappings(self) -> FakeResult:
        return self

    def scalars(self) -> FakeResult:
        return self

    def all(self) -> list[object]:
        return self.values if self.values else list(self.rows)


class FakeConnection:
    def __init__(self, database: Database) -> None:
        self.dialect = database.engine.dialect
        self.statements: list[str] = []
        self.database_name = "scratch"
        self.version = "160013"
        self.isolation = "read committed"
        self.exists = False
        self.marker: str | None = None
        self.relations: list[dict[str, object]] = []
        self.rows: list[object] = []
        self.size = 0

    def exec_driver_sql(self, statement: str) -> FakeResult:
        self.statements.append(statement)
        if statement == "SELECT current_database()":
            return FakeResult(self.database_name)
        if statement == "SHOW server_version_num":
            return FakeResult(self.version)
        if statement == "SHOW transaction_isolation":
            return FakeResult(self.isolation)
        if statement.startswith("SELECT COALESCE(sum("):
            return FakeResult(self.size)
        return FakeResult(values=self.rows)

    def execute(self, statement: object) -> FakeResult:
        sql = str(statement)
        self.statements.append(sql)
        if isinstance(statement, DropSchema):
            self.exists = False
        return FakeResult(rows=self.relations)

    def scalar(self, statement: object, parameters: object = None) -> object:
        sql = str(statement)
        self.statements.append(sql)
        if "SELECT EXISTS" in sql:
            return self.exists
        if "obj_description" in sql:
            return self.marker
        return '{"columns":[],"constraints":[],"indexes":[]}'


def fake_database(monkeypatch: pytest.MonkeyPatch) -> tuple[Database, FakeConnection]:
    database = Database("postgresql+psycopg://synthetic@127.0.0.1/scratch", pool_size=1)
    connection = FakeConnection(database)

    @contextmanager
    def begin() -> Iterator[FakeConnection]:
        yield connection

    monkeypatch.setattr(database, "begin", begin)
    return database, connection


@pytest.mark.parametrize("failure", ["database", "major", "isolation", "malformed", "range"])
def test_server_identity_version_and_isolation_are_required(
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    database, connection = fake_database(monkeypatch)
    try:
        if failure == "database":
            connection.database_name = "other"
        elif failure == "major":
            connection.version = "170001"
        elif failure == "isolation":
            connection.isolation = "serializable"
        elif failure == "malformed":
            connection.version = "unknown"
        with pytest.raises((RuntimeError, ValueError)):
            validation.verify_server(database, "scratch", 11 if failure == "range" else 16)
        assert not any(sql.startswith(("DROP ", "CREATE ")) for sql in connection.statements)
    finally:
        database.dispose()


def test_parent_supplied_postgres_17_identity_is_supported_without_connecting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database, connection = fake_database(monkeypatch)
    connection.version = "170006"
    try:
        assert validation.verify_server(database, "scratch", 17) == 170006
    finally:
        database.dispose()


def test_schema_cleanup_requires_exact_marker_not_only_uuid_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database, connection = fake_database(monkeypatch)
    name = validation.SCHEMA_PREFIX + "a" * 32
    scope = validation.ScratchSchema(database, name, [], validation.IsolationGuard(name))
    connection.exists = True
    connection.marker = validation.schema_marker(validation.SCHEMA_PREFIX + "b" * 32)
    try:
        with pytest.raises(RuntimeError, match="ownership"):
            scope.cleanup()
        assert not scope.cleanup_verified and connection.exists
        assert not any(sql.startswith("DROP ") for sql in connection.statements)
        connection.marker = validation.schema_marker(name)
        scope.cleanup()
        assert scope.cleanup_verified and not connection.exists
        assert [sql for sql in connection.statements if sql.startswith("DROP ")] == [
            f"DROP TABLE IF EXISTS {name}.worker_locks RESTRICT",
            f"DROP TABLE IF EXISTS {name}.kraken_confirmation_evidence RESTRICT",
            f"DROP SCHEMA {name}",
        ]
    finally:
        database.dispose()


def test_wrong_server_never_attempts_schema_cleanup(monkeypatch: pytest.MonkeyPatch) -> None:
    database, connection = fake_database(monkeypatch)
    connection.database_name = "unexpected"

    def factory(address: str, *, pool_size: int) -> Database:
        return database

    monkeypatch.setattr(validation, "Database", factory)
    with (
        pytest.raises(RuntimeError, match="identity"),
        validation.isolated_schema(
            "postgresql+psycopg://synthetic@localhost/scratch",
            "scratch",
            expected_major_version=16,
        ),
    ):
        pytest.fail("wrong server must not yield a schema")
    assert not any(
        "pg_namespace" in sql or sql.startswith("DROP ") for sql in connection.statements
    )


def test_fingerprints_are_read_only_order_independent_and_keep_duplicate_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database, connection = fake_database(monkeypatch)
    connection.relations = [
        {
            "nspname": "public",
            "relname": "sentinel",
            "oid": 123,
            "relkind": "r",
            "rls_active": False,
        },
    ]
    connection.rows = ['{"value":1}', '{"value":2}', '{"value":1}']
    try:
        first = validation.table_fingerprints(database)
        connection.rows.reverse()
        assert validation.table_fingerprints(database) == first
        assert first["row_count"] == 3 and first["nonempty_existing_table_witness"] is True
        connection.rows.pop()
        assert validation.table_fingerprints(database) != first
        assert connection.statements[0].endswith("REPEATABLE READ READ ONLY")
        assert not any(
            sql.startswith(("INSERT ", "UPDATE ", "DELETE ")) for sql in connection.statements
        )
        connection.relations = []
        empty = validation.table_fingerprints(database)
        assert empty["table_count"] == 0 and empty["nonempty_existing_table_witness"] is False
    finally:
        database.dispose()


@pytest.mark.parametrize("budget", ["tables", "rows", "bytes", "row-security"])
def test_table_proof_fails_closed_at_each_bound(
    monkeypatch: pytest.MonkeyPatch,
    budget: str,
) -> None:
    database, connection = fake_database(monkeypatch)
    connection.relations = [
        {
            "nspname": "public",
            "relname": "sentinel",
            "oid": 123,
            "relkind": "r",
            "rls_active": False,
        },
    ]
    if budget == "tables":
        connection.relations *= 33
    elif budget == "rows":
        connection.rows = ["{}"] * 1001
    elif budget == "row-security":
        connection.relations[0]["rls_active"] = True
    else:
        connection.size = 4 * 1024**2 + 1
    try:
        with pytest.raises(RuntimeError, match=r"budget|row-level security"):
            validation.table_fingerprints(database)
    finally:
        database.dispose()


def test_cleanup_reports_existing_table_changes_not_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database, _connection = fake_database(monkeypatch)
    name = validation.SCHEMA_PREFIX + "a" * 32
    scope = validation.ScratchSchema(
        database,
        name,
        [],
        validation.IsolationGuard(name),
        before_tables={"sha256": "deliberately-different"},
    )
    try:
        with pytest.raises(RuntimeError, match="tables changed"):
            scope.cleanup()
        assert scope.cleanup_verified
        assert scope.before_tables != scope.after_tables
    finally:
        database.dispose()


@pytest.mark.parametrize("failure", ["timeout", "invalid-json", "incomplete-pass"])
def test_supervisor_recovers_only_its_child_schema_with_separate_hard_bound(
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    identifier = UUID(int=1)
    name = validation.SCHEMA_PREFIX + identifier.hex
    monkeypatch.setattr(validation, "uuid4", lambda: identifier)
    calls: list[tuple[list[str], float]] = []

    def run(
        command: list[str],
        *,
        capture_output: bool,
        text: bool,
        check: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        calls.append((command, timeout))
        assert capture_output and text and not check
        if len(calls) == 1:
            if failure == "timeout":
                raise subprocess.TimeoutExpired(command, timeout, stderr="DO-NOT-ECHO")
            output = (
                validation.ValidationReport(status="passed").model_dump_json()
                if failure == "incomplete-pass"
                else "DO-NOT-ECHO"
            )
            return subprocess.CompletedProcess(command, 0, output, "DO-NOT-ECHO")
        assert "--cleanup-only" in command and command[command.index("--owned-schema") + 1] == name
        output = validation.ValidationReport(
            status="failed",
            scratch_schema=name,
            cleanup_verified=True,
        ).model_dump_json()
        return subprocess.CompletedProcess(command, 2, output, "")

    monkeypatch.setattr("tradeagent.kraken_confirmation_pg_validation.subprocess.run", run)
    result = validation.supervise(
        argparse.Namespace(
            scratch_url_env="MY_EXISTING_SCRATCH_URL",
            expected_database="scratch",
            expected_major_version=16,
            archived_protocol=Path("fixture"),
        )
    )
    assert result.status == "failed" and not result.pg_lock_pool_concurrency_validated
    assert result.scratch_schema == name and result.cleanup_verified
    assert [timeout for _, timeout in calls] == [240, 20]
    assert "DO-NOT-ECHO" not in result.model_dump_json()


def test_recovery_timeout_is_not_a_cleanup_or_pg_pass(monkeypatch: pytest.MonkeyPatch) -> None:
    def run(
        command: list[str],
        *,
        capture_output: bool,
        text: bool,
        check: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        raise subprocess.TimeoutExpired(command, timeout, stderr="DO-NOT-ECHO")

    monkeypatch.setattr("tradeagent.kraken_confirmation_pg_validation.subprocess.run", run)
    result = validation.recover_child(
        ["fake-supervised-child"],
        validation.SCHEMA_PREFIX + "a" * 32,
        "ValidationHardTimeout",
    )
    assert result.cleanup_verified is False and result.status == "failed"
    assert result.error_type == "ValidationHardTimeoutCleanupUnverified"
    assert "DO-NOT-ECHO" not in result.model_dump_json()
