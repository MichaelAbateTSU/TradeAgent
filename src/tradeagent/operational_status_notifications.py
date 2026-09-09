from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from typing import Any
from uuid import NAMESPACE_URL, uuid5
from zoneinfo import ZoneInfo

from sqlalchemy import ColumnElement, exists, func, literal, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from tradeagent.event_store import event_cohorts
from tradeagent.persistence import Database, controls, heartbeats, notification_outbox

EASTERN = ZoneInfo("America/New_York")
SUFFIXES = ("calibration_status", "news-terminal", "session-completion")
MAX_CONTROL_CHARACTERS = 8192


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _dated_timestamp(value: Any, now: datetime) -> bool:
    if not isinstance(value, str):
        return False
    try:
        stamp = datetime.fromisoformat(value)
    except ValueError:
        return False
    return (
        stamp.tzinfo is not None
        and stamp <= now
        and stamp.astimezone(EASTERN).date() == now.astimezone(EASTERN).date()
    )


def _result(
    payloads: dict[str, dict[str, Any]], now: datetime
) -> tuple[str, str, int | None, list[str]] | None:
    calibration = payloads.get("calibration_status", {})
    terminal = payloads.get("news-terminal", {})
    completion = payloads.get("session-completion", {})
    attempts = calibration.get("entry_attempts")
    attempts = attempts if type(attempts) is int and attempts >= 0 else None
    missed = (
        calibration.get("outcome") == "MISSED"
        and calibration.get("state") == "missed_window_no_trade"
        and _dated_timestamp(calibration.get("missed_at"), now)
    )
    disabled = terminal.get("entries_disabled") is True and _dated_timestamp(
        terminal.get("observed_at"), now
    )
    if (calibration.get("outcome") == "MISSED" and not missed) or (
        terminal.get("entries_disabled") is True and not disabled
    ):
        return None
    if completion.get("state") == "incident_unresolved_exposure":
        outcome, identity = "UNRESOLVED_EXPOSURE", "unresolved_exposure"
    elif missed:
        outcome, identity = "MISSED", "entries_disabled"
    elif completion.get("state") == "complete" and not (disabled and attempts == 0):
        outcome, identity = "COMPLETE", "complete"
    elif disabled:
        # The worker writes terminal and MISSED separately; prefer the more informative result.
        if now - datetime.fromisoformat(terminal["observed_at"]) < timedelta(seconds=30):
            return None
        outcome, identity = "BLOCKED", "entries_disabled"
    else:
        return None
    evidence = calibration.get("blocking_evidence")
    evidence = evidence if isinstance(evidence, dict) else {}
    reasons: list[str] = []
    for values in (
        calibration.get("reasons"),
        evidence.get("reasons"),
        [calibration.get("reason"), evidence.get("reason")],
        [terminal.get("reason")],
        completion.get("mismatches"),
    ):
        if isinstance(values, list):
            for reason in values[:8]:
                if isinstance(reason, str) and reason and reason[:256] not in reasons:
                    reasons.append(reason[:256])
    return outcome, identity, attempts, reasons[:8]


class OperationalStatusNotifications:
    """Observe the fresh current paper cohort without changing controls or generating reports."""

    def __init__(self, database: Database) -> None:
        self._database = database

    def enqueue_due(self, *, observed_at: datetime) -> bool:
        if observed_at.tzinfo is None:
            raise ValueError("notification observation requires an aware timestamp")
        now = observed_at.astimezone(UTC)
        day = now.astimezone(EASTERN).date().isoformat()
        with self._database.begin() as connection:
            worker = (
                connection.execute(
                    select(
                        heartbeats.c.instance_id,
                        heartbeats.c.observed_at,
                        *(
                            heartbeats.c.details[key].as_string().label(key)
                            for key in (
                                "cohort_id",
                                "practice_start_date",
                                "mode",
                                "purpose",
                                "config_hash",
                                "code_sha",
                            )
                        ),
                    )
                    .where(heartbeats.c.service_name == "tradeagent-event-worker")
                    .with_for_update(read=True)
                )
                .mappings()
                .one_or_none()
            )
            if (
                worker is None
                or not timedelta(0) <= now - _utc(worker["observed_at"]) <= timedelta(seconds=120)
                or worker["practice_start_date"] != day
                or worker["mode"] != "experimental-paper"
                or worker["purpose"] != "iex-practice"
                or not isinstance(worker["cohort_id"], str)
                or not 1 <= len(worker["cohort_id"]) <= 51
            ):
                return False
            cohort = worker["cohort_id"]
            frozen = (
                connection.execute(
                    select(
                        event_cohorts.c.config_hash,
                        event_cohorts.c.manifest["code_sha"].as_string().label("code_sha"),
                        event_cohorts.c.manifest["settings"]["practice_start_date"]
                        .as_string()
                        .label("session_date"),
                    ).where(event_cohorts.c.cohort_id == cohort)
                )
                .mappings()
                .one_or_none()
            )
            if (
                frozen is None
                or frozen["session_date"] != day
                or frozen["config_hash"] != worker["config_hash"]
                or frozen["code_sha"] != worker["code_sha"]
            ):
                return False
            keys = [f"{cohort}:{suffix}" for suffix in SUFFIXES]
            rows = {
                row["control_key"]: dict(row)
                for row in connection.execute(
                    select(controls)
                    .where(
                        controls.c.control_key.in_(keys),
                        func.length(controls.c.control_value) <= MAX_CONTROL_CHARACTERS,
                    )
                    .with_for_update(read=True)
                ).mappings()
            }
            payloads = {}
            for key, row in rows.items():
                stamp = _utc(row["updated_at"])
                if stamp > now or stamp.astimezone(EASTERN).date().isoformat() != day:
                    return False
                try:
                    payload = json.loads(row["control_value"])
                except (TypeError, ValueError, RecursionError):
                    return False
                if not isinstance(payload, dict):
                    return False
                suffix = key[len(cohort) + 1 :]
                if suffix == "session-completion" and payload.get("session_date") != day:
                    return False
                payloads[suffix] = payload
            result = _result(payloads, now)
            if result is None:
                return False
            outcome, category, attempts, reasons = result
            notification_id = str(
                uuid5(NAMESPACE_URL, f"tradeagent:operational-result:{cohort}:{day}:{category}")
            )
            payload = {
                "subject": f"[TradeAgent PAPER {outcome}] {cohort}",
                "text": (
                    f"Persisted paper workflow result: {outcome}\nCohort: {cohort}\n"
                    f"Session date: {day} (America/New_York)\nObserved: {now.isoformat()}\n"
                    f"Worker heartbeat: {_utc(worker['observed_at']).isoformat()}\n"
                    f"Cohort entry attempts: {attempts if attempts is not None else 'unknown'}\n"
                    f"Reasons: {', '.join(reasons) or 'see recorded worker status'}\n"
                    + "\n".join(
                        f"{key}: {_utc(row['updated_at']).isoformat()}" for key, row in rows.items()
                    )
                    + "\n\nThis observer did not check the broker. Current flatness, orders, fills "
                    "and P&L are not established by this notification. Cohort attempts are not "
                    "account-wide budget usage. No entry permission or profitability claim."
                ),
                "cohort_id": cohort,
                "session_date": day,
                "operational_status": outcome,
                "entry_attempts": attempts,
                "reasons": reasons,
                "paper_only": True,
                "qualification_eligible": False,
                "broker_state_verified": False,
                "observed_at": now.isoformat(),
                "control_versions": {
                    key: {
                        "updated_at": _utc(row["updated_at"]).isoformat(),
                        "sha256": sha256(row["control_value"].encode()).hexdigest(),
                    }
                    for key, row in rows.items()
                },
            }
            conditions: list[ColumnElement[bool]] = [
                exists(
                    select(heartbeats.c.service_name).where(
                        heartbeats.c.service_name == "tradeagent-event-worker",
                        heartbeats.c.instance_id == worker["instance_id"],
                        heartbeats.c.observed_at == worker["observed_at"],
                        *(
                            heartbeats.c.details[key].as_string() == worker[key]
                            for key in (
                                "cohort_id",
                                "practice_start_date",
                                "mode",
                                "purpose",
                                "config_hash",
                                "code_sha",
                            )
                        ),
                    )
                )
            ]
            for key in keys:
                query = select(controls.c.control_key).where(controls.c.control_key == key)
                version = rows.get(key)
                conditions.append(
                    exists(
                        query.where(
                            controls.c.control_value == version["control_value"],
                            controls.c.updated_at == version["updated_at"],
                        )
                    )
                    if version
                    else ~exists(query)
                )
            insert = pg_insert if connection.dialect.name == "postgresql" else sqlite_insert
            inserted = connection.execute(
                insert(notification_outbox)
                .from_select(
                    [
                        "notification_id",
                        "cycle_id",
                        "notification_type",
                        "payload",
                        "status",
                        "attempts",
                        "created_at",
                    ],
                    select(
                        literal(notification_id),
                        literal(None),
                        literal("daily_agent_status"),
                        literal(payload, type_=notification_outbox.c.payload.type),
                        literal("pending"),
                        literal(0),
                        literal(now, type_=notification_outbox.c.created_at.type),
                    ).where(*conditions),
                )
                .on_conflict_do_nothing(index_elements=[notification_outbox.c.notification_id])
            )
            return inserted.rowcount == 1
