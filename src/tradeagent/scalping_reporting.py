from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import func, select

from tradeagent.persistence import (
    Database,
    ProductionRepository,
    controls,
    events,
    heartbeats,
    worker_locks,
)
from tradeagent.reporting_reads import payload_from_projection, projected_payload

PROFILE = "v30-paper-unrestricted"


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def scalping_status(
    database: Database, *, cohort_id: str | None = None, now: datetime | None = None
) -> dict[str, Any]:
    observed_at = now or datetime.now(UTC)
    with database.begin() as connection:
        fields = ("entry_policy", "cohort_id", "code_sha", "config_hash")
        worker = (
            connection.execute(
                select(
                    heartbeats.c.instance_id,
                    heartbeats.c.observed_at,
                    *projected_payload(heartbeats.c.details, fields),
                ).where(heartbeats.c.service_name == "tradeagent-event-worker")
            )
            .mappings()
            .one_or_none()
        )
        details = payload_from_projection(dict(worker), fields) if worker else {}
        current_cohort = (
            details.get("cohort_id") if details.get("entry_policy") == PROFILE else None
        )
        selected = cohort_id or current_cohort
        raw = (
            connection.execute(
                select(controls).where(controls.c.control_key == f"scalping:{selected}:status")
            )
            .mappings()
            .one_or_none()
            if selected
            else None
        )
        lease = (
            connection.execute(
                select(worker_locks).where(worker_locks.c.lock_name == "tradeagent-event-worker")
            )
            .mappings()
            .one_or_none()
        )
    if raw is None:
        return {
            "state": "not_started",
            "as_of": observed_at.isoformat(),
            "cohort_id": selected,
            "active": False,
            "live_execution_available": False,
            "message": (
                "No persisted v30 status is available; this is not proof of no broker exposure."
            ),
        }
    snapshot = json.loads(raw["control_value"])
    if not isinstance(snapshot, dict) or snapshot.get("cohort_id") != selected:
        raise ValueError("persisted scalping status identity is invalid")
    age = (observed_at - _utc(raw["updated_at"])).total_seconds()
    fresh = bool(
        worker
        and lease
        and selected == current_cohort
        and worker["instance_id"] == lease["owner_id"] == snapshot.get("owner_id")
        and details.get("code_sha") == snapshot.get("code_sha")
        and details.get("config_hash") == snapshot.get("config_hash")
        and 0 <= age <= 30
        and timedelta(0) <= observed_at - _utc(worker["observed_at"]) <= timedelta(seconds=30)
        and timedelta(0) <= observed_at - _utc(lease["acquired_at"]) <= timedelta(seconds=90)
    )
    return {
        **snapshot,
        "reported_state": snapshot.get("state"),
        "state": snapshot.get("state") if fresh else "stale_or_unowned",
        "active": fresh,
        "age_seconds": age,
        "status_at": _utc(raw["updated_at"]).isoformat(),
        "served_at": observed_at.isoformat(),
        "live_execution_available": False,
        "qualification_eligible": False,
        "profitability_validated": False,
        "read_model": "current owner/version-checked runtime snapshot, not a fresh broker request",
    }


def request_scalping_stop(database: Database, cohort_id: str, reason: str) -> dict[str, Any]:
    if not cohort_id.strip() or not reason.strip():
        raise ValueError("an existing cohort and an explicit stop reason are required")
    repo = ProductionRepository(database)
    status = scalping_status(database, cohort_id=cohort_id)
    if status["state"] == "not_started":
        raise ValueError("cannot stop an unknown scalping cohort")
    now = datetime.now(UTC)
    command = {
        "action": "stop_new_entries",
        "reason": reason,
        "requested_at": now.isoformat(),
        "cohort_id": cohort_id,
        "owned_recovery_continues": True,
    }
    repo.set_control(f"scalping:{cohort_id}:stop", json.dumps(command, sort_keys=True))
    repo.append_event("scalping_operator_stop", command, occurred_at=now, trace_id=cohort_id)
    return command


def scalping_diagnostic_journal(
    database: Database, *, cohort_id: str | None = None, limit: int = 20
) -> dict[str, Any]:
    from tradeagent.scalping_store import scalping_cycles, scalping_runs

    if not 1 <= limit <= 100:
        raise ValueError("diagnostic page size must be between 1 and 100")
    observed_at = datetime.now(UTC)
    status = scalping_status(database, cohort_id=cohort_id)
    selected = cohort_id or status.get("cohort_id")
    with database.begin() as connection:
        run_ids = select(scalping_runs.c.run_id).where(scalping_runs.c.cohort_id == selected)
        cycle_ids = select(scalping_cycles.c.cycle_id).where(scalping_cycles.c.run_id.in_(run_ids))
        latest = (
            select(
                events.c.event_id,
                events.c.occurred_at,
                events.c.trace_id,
                events.c.payload,
                func.row_number()
                .over(partition_by=events.c.trace_id, order_by=events.c.occurred_at.desc())
                .label("revision_rank"),
            )
            .where(
                events.c.event_type == "scalp_trade_diagnostics",
                events.c.trace_id.in_(cycle_ids),
                events.c.occurred_at <= observed_at,
            )
            .subquery()
        )
        records = [
            {
                "event_id": row["event_id"],
                "observed_at": _utc(row["occurred_at"]).isoformat(),
                "cycle_id": row["trace_id"],
                "payload": row["payload"],
            }
            for row in connection.execute(
                select(latest)
                .where(latest.c.revision_rank == 1)
                .order_by(latest.c.occurred_at.desc())
                .limit(limit)
            ).mappings()
        ]
    return {
        "cohort_id": selected,
        "as_of": observed_at.isoformat(),
        "state": "recorded_diagnostics" if records else "no_completed_diagnostics_recorded",
        "records": records,
        "limit": limit,
        "scope": "latest diagnostic revision per cycle; a bounded page, not daily P&L",
        "runtime_active": status.get("active", False),
        "missing_telemetry_is_unknown": True,
        "profitability_validated": False,
    }


def build_scalping_daily_status(database: Database, now: datetime, timezone: str) -> dict[str, Any]:
    local = now.astimezone(ZoneInfo(timezone))
    status = scalping_status(database, now=now)
    lines = [
        "TRADEAGENT V30 - AUTONOMOUS PAPER SCALPING",
        f"Reporting date: {local.date()} ({timezone})",
        f"Runtime state: {status['state']}",
        f"Cohort: {status.get('cohort_id', 'not available')}",
        f"Last status: {status.get('status_at', 'not available')}",
        "",
        "The v30 profile has no paper loss, drawdown, exposure, trade-count, "
        "news approval, dated entry-window or qualification gate.",
        "Paper endpoint, ownership, actual order reconciliation and broker constraints remain.",
        "",
        "RECORDED EXECUTION AND ACCOUNTING",
        json.dumps(status.get("trade_summary", {}), indent=2, sort_keys=True, default=str),
        "",
        "CURRENT EXECUTION",
        json.dumps(status.get("execution", {}), indent=2, sort_keys=True, default=str),
        "",
        "MARKET DATA AND LATENCY",
        json.dumps(
            {
                "market": status.get("market"),
                "raw": status.get("raw"),
                "latency": status.get("latency"),
                "errors": status.get("errors"),
            },
            indent=2,
            sort_keys=True,
            default=str,
        ),
        "",
        "Pending or estimated fees are not actual final net P&L. Paper fills do not prove "
        "real queue position, live execution quality or profitability.",
        "Status is a recorded runtime observation, not an additional broker reconciliation.",
        "Dashboard: https://tradeagent-runtime-dashboard.onrender.com",
    ]
    return {
        "subject": f"[TradeAgent PAPER v30] Daily scalping status - {local.date()}",
        "text": "\n".join(lines),
        "cohort_id": status.get("cohort_id"),
        "local_date": local.date().isoformat(),
        "timezone": timezone,
        "profile": PROFILE,
        "snapshot": status,
        "qualification_eligible": False,
        "live_execution_available": False,
    }
