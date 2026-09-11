from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import event, insert, select, update
from sqlalchemy.engine import Connection
from sqlalchemy.sql.dml import Insert

from tradeagent.event_store import EventStore, event_cohorts
from tradeagent.notifications import (
    NotificationDispatcher,
    OutboxMessage,
    RoundTripNotificationRepository,
)
from tradeagent.notifier import NotifierService
from tradeagent.operational_status_notifications import OperationalStatusNotifications
from tradeagent.persistence import (
    Database,
    ProductionRepository,
    controls,
    heartbeats,
    notification_outbox,
)

NOW = datetime(2026, 9, 9, 16, tzinfo=UTC)
MISSED_AT = datetime(2026, 9, 9, 14, 30, 33, tzinfo=UTC)
COHORT = "v20-news-paper-20260909-r1"


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Database]:
    database = Database(f"sqlite:///{tmp_path / 'operational.db'}")
    database.initialize()
    _cohort(database)
    yield database
    database.dispose()


def _cohort(database: Database, cohort: str = COHORT, day: str = "2026-09-09") -> None:
    EventStore(database).freeze(
        cohort,
        "config",
        {"code_sha": "code", "settings": {"practice_start_date": day}},
        "experimental-paper",
        NOW,
    )
    ProductionRepository(database).heartbeat(
        "tradeagent-event-worker",
        "worker-1",
        {
            "cohort_id": cohort,
            "practice_start_date": day,
            "mode": "experimental-paper",
            "purpose": "iex-practice",
            "config_hash": "config",
            "code_sha": "code",
        },
        observed_at=NOW,
    )


def _control(
    database: Database,
    suffix: str,
    payload: Any,
    *,
    cohort: str = COHORT,
    stamp: datetime = MISSED_AT,
    raw: bool = False,
) -> None:
    with database.begin() as connection:
        connection.execute(
            insert(controls).values(
                control_key=f"{cohort}:{suffix}",
                control_value=payload if raw else json.dumps(payload),
                updated_at=stamp,
            )
        )


def _missed(database: Database, **changes: Any) -> None:
    payload = {
        "state": "missed_window_no_trade",
        "outcome": "MISSED",
        "entry_attempts": 0,
        "missed_at": MISSED_AT.isoformat(),
        "blocking_evidence": {"reasons": ["NEWS_AUTHORIZATION_REQUIRED"], "raw_payload": "PRIVATE"},
    }
    payload.update(changes)
    _control(database, "calibration_status", payload)


def _terminal(database: Database, stamp: datetime = MISSED_AT) -> None:
    _control(
        database,
        "news-terminal",
        {
            "entries_disabled": True,
            "observed_at": stamp.isoformat(),
            "reason": "SESSION_FINISHED_OR_EQUIPMENT_FAILED",
        },
        stamp=stamp,
    )


def _rows(database: Database) -> list[dict[str, Any]]:
    with database.begin() as connection:
        return [dict(row) for row in connection.execute(select(notification_outbox)).mappings()]


def _enqueue(database: Database, now: datetime = NOW) -> bool:
    return OperationalStatusNotifications(database).enqueue_due(observed_at=now)


def test_missed_is_lightweight_and_does_not_change_controls(database: Database) -> None:
    _missed(database)
    _terminal(database)
    with database.begin() as connection:
        before = connection.execute(select(controls)).all()
    assert _enqueue(database)
    row = _rows(database)[0]
    assert row["cycle_id"] is None and row["notification_type"] == "daily_agent_status"
    assert row["payload"]["operational_status"] == "MISSED"
    assert row["payload"]["entry_attempts"] == 0
    assert row["payload"]["broker_state_verified"] is False
    assert "NEWS_AUTHORIZATION_REQUIRED" in row["payload"]["text"]
    assert MISSED_AT.isoformat() in row["payload"]["text"]
    assert "PRIVATE" not in json.dumps(row["payload"])
    assert len(json.dumps(row["payload"])) < 5000
    with database.begin() as connection:
        assert connection.execute(select(controls)).all() == before


@pytest.mark.parametrize("status", ["pending", "sending", "sent", "failed", "needs_review"])
def test_poll_restart_deduplicates_every_existing_outbox_state(
    database: Database, status: str
) -> None:
    _missed(database)
    assert _enqueue(database)
    with database.begin() as connection:
        connection.execute(update(notification_outbox).values(status=status))
    before = _rows(database)
    assert not _enqueue(database)
    assert not OperationalStatusNotifications(database).enqueue_due(observed_at=NOW)
    assert _rows(database) == before


def test_terminal_then_missed_and_completion_same_failure_is_one_email(database: Database) -> None:
    _terminal(database)
    assert _enqueue(database)
    _missed(database)
    assert not _enqueue(database)
    _control(
        database,
        "session-completion",
        {
            "state": "complete",
            "session_date": "2026-09-09",
            "positions": {},
            "open_orders": 0,
        },
    )
    assert not _enqueue(database)
    assert len(_rows(database)) == 1


def test_terminal_settles_before_worker_missed_projection(database: Database) -> None:
    _terminal(database, NOW - timedelta(seconds=5))
    assert not _enqueue(database)
    _missed(database)
    assert _enqueue(database)
    assert _rows(database)[0]["payload"]["operational_status"] == "MISSED"


@pytest.mark.parametrize(
    "state,outcome",
    [
        ("complete", "COMPLETE"),
        ("incident_unresolved_exposure", "UNRESOLVED_EXPOSURE"),
    ],
)
def test_session_completion_is_reported_without_current_flatness_claim(
    database: Database, state: str, outcome: str
) -> None:
    _control(database, "session-completion", {"state": state, "session_date": "2026-09-09"})
    assert _enqueue(database)
    assert _rows(database)[0]["payload"]["operational_status"] == outcome
    assert not _rows(database)[0]["payload"]["broker_state_verified"]


def test_unresolved_completion_is_not_hidden_by_prior_missed(database: Database) -> None:
    _missed(database)
    assert _enqueue(database)
    _control(
        database,
        "session-completion",
        {
            "state": "incident_unresolved_exposure",
            "session_date": "2026-09-09",
            "mismatches": ["BROKER_POSITION_MISMATCH"],
        },
    )
    assert _enqueue(database)
    assert len(_rows(database)) == 2


def test_only_current_cohort_not_old_cohorts_or_manual_result(database: Database) -> None:
    _missed(database)
    _cohort(database, "new-current")
    _control(database, "manual-workflow-result", {"state": "failed"}, cohort="new-current")
    assert not _enqueue(database)
    assert not _rows(database)


@pytest.mark.parametrize(
    "change",
    [
        {"practice_start_date": "2026-09-08"},
        {"cohort_id": "absent"},
        {"mode": "shadow"},
        {"purpose": "research"},
        {"config_hash": "changed"},
        {"code_sha": "changed"},
    ],
)
def test_invalid_current_worker_scope_is_ignored(
    database: Database, change: dict[str, str]
) -> None:
    _missed(database)
    with database.begin() as connection:
        value = dict(connection.scalar(select(heartbeats.c.details)))
        value.update(change)
        connection.execute(update(heartbeats).values(details=value))
    assert not _enqueue(database)


@pytest.mark.parametrize("age", [-1, 121])
def test_stale_or_future_worker_is_ignored(database: Database, age: int) -> None:
    _missed(database)
    with database.begin() as connection:
        connection.execute(update(heartbeats).values(observed_at=NOW - timedelta(seconds=age)))
    assert not _enqueue(database)


@pytest.mark.parametrize(
    "value",
    [
        "broken json",
        "[]",
        "[" * 1500 + "]" * 1500,
        '{"state":"healthy","old_pg_failures":22292}',
        "x" * 8193,
    ],
)
def test_malformed_or_irrelevant_controls_do_not_notify(database: Database, value: str) -> None:
    _control(database, "calibration_status", value, raw=True)
    assert not _enqueue(database)


def test_oversized_control_is_not_treated_as_absent_for_valid_missed(database: Database) -> None:
    _missed(database)
    _control(database, "news-terminal", "x" * 8193, raw=True)
    assert not _enqueue(database)


@pytest.mark.parametrize("stamp", [NOW + timedelta(seconds=1), NOW - timedelta(days=1)])
def test_old_or_future_control_versions_are_ignored(database: Database, stamp: datetime) -> None:
    _missed(database)
    with database.begin() as connection:
        connection.execute(update(controls).values(updated_at=stamp))
    assert not _enqueue(database)


@pytest.mark.parametrize(
    "value",
    [
        "2026-09-08T14:30:00+00:00",
        "2026-09-09T03:30:00+00:00",
        "2026-09-09T17:30:00+00:00",
        "2026-09-09T14:30:00",
        "invalid",
    ],
)
def test_invalid_missed_timestamp_does_not_fall_back_to_terminal(
    database: Database, value: str
) -> None:
    _missed(database, missed_at=value)
    _terminal(database)
    assert not _enqueue(database)


def test_wrong_completion_day_and_frozen_day_are_rejected(database: Database) -> None:
    _control(database, "session-completion", {"state": "complete", "session_date": "2026-09-08"})
    assert not _enqueue(database)
    with database.begin() as connection:
        connection.execute(
            update(event_cohorts).values(
                manifest={"code_sha": "code", "settings": {"practice_start_date": "2026-09-08"}}
            )
        )
    assert not _enqueue(database)


@pytest.mark.parametrize("change", ["value", "timestamp", "cohort", "missing_control"])
def test_final_insert_rechecks_snapshot_versions(database: Database, change: str) -> None:
    _missed(database)
    triggered = False

    def mutate(connection: Connection, statement: Any, *_: Any) -> None:
        nonlocal triggered
        if (
            triggered
            or not isinstance(statement, Insert)
            or statement.table is not notification_outbox
        ):
            return
        triggered = True
        if change == "cohort":
            connection.execute(update(heartbeats).values(details={"cohort_id": "other"}))
        elif change == "missing_control":
            connection.execute(
                insert(controls).values(
                    control_key=f"{COHORT}:news-terminal",
                    control_value="{}",
                    updated_at=NOW,
                )
            )
        else:
            connection.execute(
                update(controls).values(
                    **({"control_value": "{}"} if change == "value" else {"updated_at": NOW})
                )
            )

    event.listen(database.engine, "before_execute", mutate)
    try:
        assert not _enqueue(database)
    finally:
        event.remove(database.engine, "before_execute", mutate)
    assert triggered and not _rows(database)


def test_outbox_insertion_rolls_back_on_transaction_failure(database: Database) -> None:
    _missed(database)
    with database.begin() as connection:
        before = connection.execute(select(controls)).all()

    def fail(_: Connection, statement: Any, *args: Any) -> None:
        if isinstance(statement, Insert) and statement.table is notification_outbox:
            raise RuntimeError("rollback after insert")

    event.listen(database.engine, "after_execute", fail)
    try:
        with pytest.raises(RuntimeError, match="rollback after insert"):
            _enqueue(database)
    finally:
        event.remove(database.engine, "after_execute", fail)
    assert not _rows(database)
    with database.begin() as connection:
        assert connection.execute(select(controls)).all() == before
    assert _enqueue(database)


def test_notifier_does_not_enqueue_individual_operational_emails(database: Database) -> None:
    _missed(database)
    delivered: list[OutboxMessage] = []

    class Provider:
        def send(self, message: OutboxMessage) -> str:
            delivered.append(message)
            return "provider-acceptance"

    class Daily:
        calls = 0

        def enqueue_due(self, *, observed_at: datetime) -> bool:
            assert observed_at == NOW
            self.calls += 1
            return False

    daily = Daily()
    service = NotifierService(
        NotificationDispatcher(RoundTripNotificationRepository(database), Provider()),
        ProductionRepository(database),
        instance_id="notifier",
        clock=lambda: NOW,
        daily_scheduler=daily,
    )
    assert not service.run_once()
    assert not service.run_once()
    assert delivered == [] and daily.calls == 2
    assert _rows(database) == []


def test_missing_worker_and_naive_observation(database: Database) -> None:
    with database.begin() as connection:
        connection.execute(heartbeats.delete())
    assert not _enqueue(database)
    with pytest.raises(ValueError, match="aware"):
        _enqueue(database, NOW.replace(tzinfo=None))
