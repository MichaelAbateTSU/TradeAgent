from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Event, Lock
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.engine import make_url
from sqlalchemy.exc import OperationalError
from sqlalchemy.pool import NullPool, QueuePool

from tradeagent import event_session_report as reports
from tradeagent import reporting_reads
from tradeagent.reporting_reads import (
    REPORT_ADMISSION_LOCK_KEY,
    ReportBusyError,
    ReportingReadModelIncompleteError,
    ReportPayloadTooLargeError,
    report_admission,
)


@pytest.fixture
def pg_admission(monkeypatch: pytest.MonkeyPatch) -> Any:
    state = SimpleNamespace(
        owner=None, mutex=Lock(), engines=[], statements=[], fail=None, options=[]
    )

    class Connection:
        closed = False

        def __enter__(self) -> Connection:
            return self

        def __exit__(self, *args: Any) -> None:
            with state.mutex:
                if state.owner is self:
                    state.owner = None
                self.closed = True

        def scalar(self, statement: Any) -> bool:
            sql = str(statement)
            state.statements.append((sql, statement.compile().params))
            with state.mutex:
                if "pg_try_advisory_lock" in sql:
                    if state.fail == "acquire":
                        raise OperationalError(sql, {}, RuntimeError("connection lost"))
                    if state.owner is None:
                        state.owner = self
                        return True
                    return False
                assert "pg_advisory_unlock" in sql
                if state.fail == "unlock":
                    raise OperationalError(sql, {}, RuntimeError("connection lost"))
                assert state.owner is self
                state.owner = None
                return True

    class Coordination:
        disposed = False

        def __init__(self) -> None:
            self.connection = Connection()

        def connect(self) -> Connection:
            if state.fail == "connect":
                raise OperationalError("connect", {}, RuntimeError("unavailable"))
            return self.connection

        def dispose(self) -> None:
            self.disposed = True

    def factory(url: Any, **kwargs: Any) -> Coordination:
        state.options.append((url, kwargs))
        engine = Coordination()
        state.engines.append(engine)
        return engine

    monkeypatch.setattr(reporting_reads, "create_engine", factory)
    state.main = SimpleNamespace(
        dialect=SimpleNamespace(name="postgresql"),
        url=make_url("postgresql+psycopg://localhost/report-tests"),
        connect=Mock(side_effect=AssertionError("admission starved the main pool")),
        begin=Mock(side_effect=AssertionError("admission used a main transaction")),
    )
    return state


def test_pg_report_admission_uses_dedicated_autocommit_session(pg_admission: Any) -> None:
    state = pg_admission
    with report_admission(state.main):
        assert state.owner is state.engines[0].connection
        assert not state.engines[0].connection.closed
    assert state.owner is None
    assert state.engines[0].connection.closed and state.engines[0].disposed
    assert state.options == [
        (
            state.main.url,
            {
                "poolclass": NullPool,
                "isolation_level": "AUTOCOMMIT",
                "connect_args": {"connect_timeout": 5},
            },
        )
    ]
    assert len(state.statements) == 2
    assert all(sql.startswith("SELECT ") for sql, _ in state.statements)
    assert all(
        list(params.values()) == [REPORT_ADMISSION_LOCK_KEY] for _, params in state.statements
    )
    state.main.connect.assert_not_called()
    state.main.begin.assert_not_called()


@pytest.mark.parametrize(
    "error",
    [
        RuntimeError("report failed"),
        ReportingReadModelIncompleteError("retained-original"),
        ReportPayloadTooLargeError("oversized report"),
    ],
)
def test_pg_report_exceptions_propagate_and_always_release(
    pg_admission: Any, error: Exception
) -> None:
    with pytest.raises(type(error), match=str(error)), report_admission(pg_admission.main):
        raise error
    assert pg_admission.owner is None
    assert pg_admission.engines[0].connection.closed
    assert pg_admission.engines[0].disposed


@pytest.mark.parametrize("phase", ["connect", "acquire", "unlock"])
def test_pg_coordination_failures_close_or_dispose(pg_admission: Any, phase: str) -> None:
    state = pg_admission
    state.fail = phase
    if phase == "unlock":
        with report_admission(state.main):
            assert state.owner is not None
    else:
        with pytest.raises(OperationalError), report_admission(state.main):
            pytest.fail("report must not run without confirmed admission")
    assert state.owner is None
    assert state.engines[0].disposed
    if phase != "connect":
        assert state.engines[0].connection.closed


def test_producer_wait_is_bounded_without_blocking_sql(
    pg_admission: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = pg_admission
    other_process = object()
    state.owner = other_process
    clock = [0.0]
    sleeps = []

    def sleep(seconds: float) -> None:
        sleeps.append(seconds)
        clock[0] += seconds

    monkeypatch.setattr(reporting_reads, "monotonic", lambda: clock[0])
    monkeypatch.setattr(reporting_reads, "sleep", sleep)
    with pytest.raises(ReportBusyError), report_admission(state.main, wait_seconds=1):
        pytest.fail("busy report was admitted")
    assert clock[0] == 1
    assert sleeps == [0.25] * 4
    assert state.owner is other_process
    assert all("pg_try_advisory_lock" in sql for sql, _ in state.statements)
    assert state.engines[0].connection.closed and state.engines[0].disposed


def test_waiting_producer_acquires_after_other_process_releases(
    pg_admission: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = pg_admission
    state.owner = object()
    clock = [0.0]

    def release_during_wait(seconds: float) -> None:
        clock[0] += seconds
        state.owner = None

    monkeypatch.setattr(reporting_reads, "monotonic", lambda: clock[0])
    monkeypatch.setattr(reporting_reads, "sleep", release_during_wait)
    with report_admission(state.main, wait_seconds=1):
        assert state.owner is state.engines[0].connection
    assert clock[0] == 0.25
    assert len(state.statements) == 3
    assert state.owner is None


def test_pg_admission_leaves_single_data_pool_slot_available(pg_admission: Any) -> None:
    engine = create_engine(
        "sqlite://", poolclass=QueuePool, pool_size=1, max_overflow=0, pool_timeout=0.1
    )
    state = pg_admission
    state.main.connect = engine.connect
    state.main.begin = engine.begin
    try:
        with report_admission(state.main), state.main.begin() as connection:
            assert connection.scalar(select(1)) == 1
            assert engine.pool.checkedout() == 1
            assert state.owner is state.engines[0].connection
        assert engine.pool.checkedout() == 0
    finally:
        engine.dispose()


def test_full_report_global_admission_spans_report_body_and_rejects_other_process(
    pg_admission: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = pg_admission
    entered = Event()
    release = Event()
    database = SimpleNamespace(engine=state.main)
    other_database = SimpleNamespace(engine=SimpleNamespace(**vars(state.main)))
    calls = []

    def full_report(*args: Any, **kwargs: Any) -> dict[str, Any]:
        assert state.owner is not None
        calls.append(kwargs["persist"])
        entered.set()
        if kwargs["persist"]:
            assert release.wait(timeout=3)
        return {"snapshot_persisted": kwargs["persist"]}

    monkeypatch.setattr(reports, "_session_report", full_report)
    with ThreadPoolExecutor(max_workers=1) as executor:
        producer = executor.submit(reports.session_report, database, "producer", persist=True)
        try:
            assert entered.wait(timeout=3)
            with pytest.raises(ReportBusyError):
                reports.session_report(other_database, "historical-api")
            assert calls == [True]
        finally:
            release.set()
        assert producer.result(timeout=3) == {"snapshot_persisted": True}
    assert reports.session_report(other_database, "historical-api") == {"snapshot_persisted": False}
    assert calls == [True, False]
    assert all(engine.connection.closed and engine.disposed for engine in state.engines)
    state.main.connect.assert_not_called()


def test_sqlite_admission_never_creates_a_coordination_connection(
    pg_admission: Any,
) -> None:
    with report_admission(SimpleNamespace(dialect=SimpleNamespace(name="sqlite"))):
        pass
    assert pg_admission.engines == []


@pytest.mark.parametrize("seconds", [-1, 61, float("nan")])
def test_report_admission_rejects_unbounded_wait(pg_admission: Any, seconds: float) -> None:
    with pytest.raises(ValueError), report_admission(pg_admission.main, wait_seconds=seconds):
        pytest.fail("invalid timeout accepted")
    assert pg_admission.engines == []
