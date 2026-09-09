from __future__ import annotations

import json
import re
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import insert, select, update
from sqlalchemy.engine import Connection

from tradeagent.event_session import equipment_identity
from tradeagent.event_store import EventStore, event_cohorts
from tradeagent.experimental_policy import ExperimentalSettings, OperationalCertificate
from tradeagent.intraday import NyseSessionCalendar
from tradeagent.persistence import ProductionRepository, controls


def _control_state(connection: Connection, cohort: str) -> dict[str, tuple[Any, Any]]:
    keys = [
        "kill_switch",
        *(f"{cohort}:{suffix}" for suffix in ("pause", "demo-terminal", "demo-authorization")),
    ]
    return {
        row.control_key: (row.control_value, row.updated_at)
        for row in connection.execute(
            select(controls).where(controls.c.control_key.in_(keys)).with_for_update()
        )
    }


def demo_control_state(store: EventStore, cohort: str) -> dict[str, tuple[Any, Any]]:
    with store.database.begin() as connection:
        return _control_state(connection, cohort)


def _write_control(connection: Connection, key: str, value: str, now: datetime) -> None:
    updated = connection.execute(
        update(controls)
        .where(controls.c.control_key == key)
        .values(control_value=value, updated_at=now)
    )
    if not updated.rowcount:
        connection.execute(
            insert(controls).values(control_key=key, control_value=value, updated_at=now)
        )


def activate_demo(
    store: EventStore,
    settings: ExperimentalSettings,
    proof: OperationalCertificate,
    acceptance_sha256: str,
    expected_controls: dict[str, tuple[Any, Any]],
    now: datetime,
) -> None:
    cohort = settings.cohort_id
    authorization = {
        **demo_scope(settings, proof.config_hash, proof.code_sha),
        "issued_at": now.isoformat(),
        "acceptance_sha256": acceptance_sha256,
        "incident_acceptance_passed": True,
        "authority": "separate user-authorized one-paper-demo after live gate",
    }
    with store.database.begin() as connection:
        connection.execute(
            select(event_cohorts).where(event_cohorts.c.cohort_id == cohort).with_for_update()
        ).one()
        current = _control_state(connection, cohort)
        if (
            not proof.permits_paper
            or current != expected_controls
            or current.get("kill_switch", (None,))[0] != "active"
            or current.get(f"{cohort}:pause", (None,))[0] != "OPERATOR_PAUSE"
            or f"{cohort}:demo-terminal" in current
            or f"{cohort}:demo-authorization" in current
        ):
            raise ValueError("demo activation refused: safety controls changed or already consumed")
        for key, value in {
            f"{cohort}:certificate": proof.model_dump_json(),
            f"{cohort}:demo-authorization": json.dumps(authorization, sort_keys=True),
            "v20:mechanics_attestation": proof.code_sha,
            f"{cohort}:pause": "",
            "kill_switch": "inactive",
        }.items():
            _write_control(connection, key, value, now)
        store.audit("demo_authorization", authorization, now, cohort, connection)


def terminate_demo(store: EventStore, cohort: str, reason: str, now: datetime) -> None:
    with store.database.begin() as connection:
        connection.execute(
            select(event_cohorts).where(event_cohorts.c.cohort_id == cohort).with_for_update()
        ).one()
        current = _control_state(connection, cohort)
        key = f"{cohort}:demo-terminal"
        if key not in current:
            value = {"reason": reason, "observed_at": now.isoformat(), "entries_disabled": True}
            _write_control(connection, key, json.dumps(value, sort_keys=True), now)
            store.audit("demo_terminal", value, now, cohort, connection)
        if not current.get(f"{cohort}:pause", (None,))[0]:
            _write_control(connection, f"{cohort}:pause", "DEMO_TERMINAL", now)
        if current.get("kill_switch", (None,))[0] != "active":
            _write_control(connection, "kill_switch", "active", now)


def demo_scope(settings: ExperimentalSettings, config_hash: str, code_sha: str) -> dict[str, Any]:
    if settings.entry_policy != "equipment-only-demo":
        raise ValueError("not an equipment-only demo")
    if settings.demo_account_digest is None or settings.practice_start_date is None:
        raise ValueError("demo account/date missing")
    return {
        "protocol": "manual-paper-demo-v1",
        "cohort_id": settings.cohort_id,
        "code_sha": code_sha,
        "config_hash": config_hash,
        "account_digest": settings.demo_account_digest,
        "session_date": settings.practice_start_date.isoformat(),
        "equipment_test_id": equipment_identity(
            settings.demo_account_digest, settings.practice_start_date
        ),
        "symbol": "AAPL",
        "maximum_entries": 1,
        "maximum_notional": str(settings.max_entry_notional),
        "window_minutes_after_open": list(settings.calibration_window_minutes),
        "qualification_eligible": False,
    }


def demo_authorized(
    repo: ProductionRepository,
    settings: ExperimentalSettings,
    config_hash: str,
    code_sha: str,
    account_digest: str,
    now: datetime,
    calendar: NyseSessionCalendar,
) -> bool:
    if settings.entry_policy != "equipment-only-demo":
        return False
    raw = repo.get_control(f"{settings.cohort_id}:demo-authorization")
    if not raw or repo.get_control(f"{settings.cohort_id}:demo-terminal"):
        return False
    try:
        value = json.loads(raw)
        scope = demo_scope(settings, config_hash, code_sha)
        bounds = (
            calendar.session_bounds(settings.practice_start_date)
            if settings.practice_start_date
            else None
        )
        issued = datetime.fromisoformat(value["issued_at"])
        return bool(
            all(value.get(key) == expected for key, expected in scope.items())
            and account_digest == settings.demo_account_digest
            and re.fullmatch(r"[0-9a-f]{64}", value["acceptance_sha256"])
            and value["incident_acceptance_passed"] is True
            and bounds
            and bounds[0] + timedelta(minutes=35) <= issued <= now
            and now < bounds[0] + timedelta(minutes=settings.calibration_window_minutes[1])
        )
    except (ValueError, TypeError, KeyError):
        return False
