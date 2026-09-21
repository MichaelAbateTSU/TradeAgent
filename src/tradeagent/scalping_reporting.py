from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from decimal import Decimal
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
from tradeagent.scalping_experiments import (
    ACCEPTANCE_CLASSIFICATION,
    EXPERIMENTAL_CLASSIFICATION,
    ExecutionAcceptancePolicy,
    ExperimentalScalpPolicy,
    experiment_policy_report,
)
from tradeagent.scalping_store import scalping_cycles, scalping_order_links

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


def scalping_shadow_status(database: Database, *, cohort_id: str | None = None) -> dict[str, Any]:
    from tradeagent.scalping_store import scalping_runs

    status = scalping_status(database, cohort_id=cohort_id)
    selected = cohort_id or status.get("cohort_id")
    observed_at = datetime.now(UTC)
    since = observed_at - timedelta(days=30)
    with database.begin() as connection:
        run_ids = select(scalping_runs.c.run_id).where(scalping_runs.c.cohort_id == selected)
        candidates = [
            dict(row)
            for row in connection.execute(
                select(
                    events.c.payload["candidate_id"].as_string().label("candidate_id"),
                    events.c.payload["signal"]["symbol"].as_string().label("symbol"),
                    events.c.payload["signal"]["family"].as_string().label("family"),
                    events.c.occurred_at,
                )
                .where(
                    events.c.event_type == "scalp_shadow_action_candidate",
                    events.c.payload["run_id"].as_string().in_(run_ids),
                    events.c.occurred_at >= since,
                )
                .order_by(events.c.occurred_at.desc())
                .limit(100)
            ).mappings()
        ]
        outcomes = [
            dict(row)
            for row in connection.execute(
                select(
                    events.c.payload["action"].as_string().label("action"),
                    events.c.payload["symbol"].as_string().label("symbol"),
                    events.c.payload["family"].as_string().label("family"),
                    events.c.payload["complete"].as_boolean().label("complete"),
                    events.c.payload["filled"].as_boolean().label("filled"),
                    func.count().label("count"),
                )
                .where(
                    events.c.event_type == "scalp_shadow_action_outcome",
                    events.c.payload["run_id"].as_string().in_(run_ids),
                    events.c.occurred_at >= since,
                )
                .group_by("action", "symbol", "family", "complete", "filled")
            ).mappings()
        ]
    return {
        "cohort_id": selected,
        "observed_at": observed_at.isoformat(),
        "window_start": since.isoformat(),
        "runtime_active": status.get("active", False),
        "recent_candidate_count": len(candidates),
        "recent_candidates": candidates,
        "outcome_groups": outcomes,
        "no_order_submission": True,
        "model_status": (status.get("economics") or {}).get("model_status"),
        "profitability_validated": False,
    }


def scalping_probe_status(database: Database, *, cohort_id: str | None = None) -> dict[str, Any]:
    """Return the recorded probe projection without contacting a broker."""
    status = scalping_status(database, cohort_id=cohort_id)
    probe = status.get("execution_validation_probes")
    if not isinstance(probe, dict):
        probe = status.get("execution", {}).get("execution_validation_probes")
    if not isinstance(probe, dict):
        return {
            "state": "not_started",
            "cohort_id": cohort_id,
            "actual_broker_labels_only": True,
            "message": "No recorded execution-validation probe status is available.",
        }
    return {
        **probe,
        "runtime_active": status.get("active", False),
        "read_model": "recorded runtime projection, not a fresh broker request",
    }


def scalping_experiment_status(
    database: Database,
    *,
    account_digest: str | None = None,
) -> dict[str, Any]:
    """Return the paper decision funnel, order evidence, and model-transition gate."""
    acceptance = ExecutionAcceptancePolicy()
    experimental = ExperimentalScalpPolicy()
    classifications = (
        "execution_validation_probe",
        ACCEPTANCE_CLASSIFICATION,
        EXPERIMENTAL_CLASSIFICATION,
    )
    with database.begin() as connection:
        cycle_query = select(scalping_cycles).where(
            scalping_cycles.c.payload["classification"].as_string().in_(classifications)
        )
        if account_digest is not None:
            cycle_query = cycle_query.where(
                scalping_cycles.c.account_digest == account_digest
            )
        cycles = [
            dict(row)
            for row in connection.execute(
                cycle_query.order_by(scalping_cycles.c.created_at)
            )
            .mappings()
            .all()
        ]
        cycle_ids = [row["cycle_id"] for row in cycles]
        orders = (
            [
                dict(row)
                for row in connection.execute(
                    select(scalping_order_links)
                    .where(scalping_order_links.c.cycle_id.in_(cycle_ids))
                    .order_by(scalping_order_links.c.created_at)
                )
                .mappings()
                .all()
            ]
            if cycle_ids
            else []
        )
        candidate_events = [
            dict(row)
            for row in connection.execute(
                select(events.c.occurred_at, events.c.payload)
                .where(events.c.event_type == "scalp_paper_experiment_candidate")
                .order_by(events.c.occurred_at)
            )
            .mappings()
            .all()
        ]
    orders_by_cycle: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in orders:
        broker = dict(row["broker"] or {})
        intent = dict(row["intent"] or {})
        orders_by_cycle[str(row["cycle_id"])].append(
            {
                "client_order_id": row["client_order_id"],
                "broker_order_id": broker.get("id"),
                "side": (intent.get("request") or {}).get("side"),
                "limit_price": intent.get("limit_price"),
                "quote": intent.get("quote"),
                "dispatch_state": row["dispatch_state"],
                "broker_status": broker.get("status"),
                "broker_created_at": broker.get("created_at"),
                "broker_submitted_at": broker.get("submitted_at"),
                "broker_filled_at": broker.get("filled_at"),
                "broker_canceled_at": broker.get("canceled_at"),
                "filled_quantity": broker.get("filled_quantity"),
                "filled_average_price": broker.get("filled_average_price"),
                "submission_started_at": (
                    row["submission_started_at"].isoformat()
                    if row["submission_started_at"]
                    else None
                ),
                "cancel_requested_at": (
                    row["cancel_requested_at"].isoformat()
                    if row["cancel_requested_at"]
                    else None
                ),
                "first_positive_fill_at": (
                    row["first_positive_fill_at"].isoformat()
                    if row["first_positive_fill_at"]
                    else None
                ),
                "last_reconciled_at": (
                    row["last_reconciled_at"].isoformat()
                    if row["last_reconciled_at"]
                    else None
                ),
                "submission_error": row["submission_error"],
            }
        )
    cycle_evidence = []
    exclusions: Counter[str] = Counter()
    for row in cycles:
        classification = str(row["payload"].get("classification"))
        qualifying = (
            classification == EXPERIMENTAL_CLASSIFICATION
            and row["state"] == "closed_owned_flat"
            and Decimal(str(row["entry_quantity"])) > 0
            and Decimal(str(row["exit_quantity"])) > 0
            and Decimal(str(row["owned_quantity"])) == 0
        )
        if not qualifying:
            reason = (
                "NO_ENTRY_FILL"
                if row["state"] == "no_fill"
                else "ACCEPTANCE_TEST_NOT_ECONOMIC_TRAINING"
                if classification == ACCEPTANCE_CLASSIFICATION
                else "LEGACY_PASSIVE_PROBE_NOT_ECONOMIC_TRAINING"
                if classification == "execution_validation_probe"
                else "ROUND_TRIP_NOT_COMPLETE"
            )
            exclusions[reason] += 1
        cycle_evidence.append(
            {
                "cycle_id": row["cycle_id"],
                "classification": classification,
                "symbol": row["symbol"],
                "state": row["state"],
                "decision_at": row["created_at"].isoformat(),
                "opened_at": row["opened_at"].isoformat() if row["opened_at"] else None,
                "closed_at": row["closed_at"].isoformat() if row["closed_at"] else None,
                "entry_quantity": str(row["entry_quantity"]),
                "exit_quantity": str(row["exit_quantity"]),
                "owned_quantity": str(row["owned_quantity"]),
                "gross_cash_flow_usd": (
                    str(row["gross_cash_flow"]) if row["gross_cash_flow"] is not None else None
                ),
                "actual_net_pnl_usd": (
                    str(row["actual_net_pnl"]) if row["actual_net_pnl"] is not None else None
                ),
                "modeled_net_pnl_usd": (
                    str(row["modeled_net_pnl"])
                    if row["modeled_net_pnl"] is not None
                    else None
                ),
                "fees_pending": row["fees_pending"],
                "qualifying_independent_observation": qualifying,
                "orders": orders_by_cycle.get(str(row["cycle_id"]), []),
            }
        )
    qualifying = [
        row for row in cycle_evidence if row["qualifying_independent_observation"]
    ]
    blocked_candidates = sum(
        event["payload"].get("eligible") is False for event in candidate_events
    )
    entry_submissions = sum(
        1 for row in orders if (row["intent"].get("request") or {}).get("side") == "buy"
    )
    broker_acceptances = sum(bool((row["broker"] or {}).get("id")) for row in orders)
    partial_fills = sum(
        (row["broker"] or {}).get("status") == "partially_filled" for row in orders
    )
    required_total = (
        experimental.minimum_training_round_trips
        + experimental.minimum_held_out_round_trips
    )
    return {
        "schema": "paper-scalping-experiment-status-v1",
        "as_of": datetime.now(UTC).isoformat(),
        "paper_only": True,
        "qualified_strategy_gate_preserved": True,
        "funnel": {
            "candidates": len(candidate_events),
            "blocked_candidates": blocked_candidates,
            "submissions": len(orders),
            "entry_submissions": entry_submissions,
            "broker_acceptances": broker_acceptances,
            "partial_fills": partial_fills,
            "entry_fills": sum(
                Decimal(str(row["entry_quantity"])) > 0 for row in cycles
            ),
            "completed_exits": sum(
                row["state"] == "closed_owned_flat" for row in cycles
            ),
            "flat_reconciliations": sum(
                row["state"] == "closed_owned_flat"
                and Decimal(str(row["owned_quantity"])) == 0
                for row in cycles
            ),
        },
        "evidence_accounting": {
            "required_independent_round_trips": required_total,
            "required_training_round_trips": experimental.minimum_training_round_trips,
            "required_held_out_round_trips": experimental.minimum_held_out_round_trips,
            "qualifying_independent_round_trips": len(qualifying),
            "chronological_training_available": min(
                len(qualifying), experimental.minimum_training_round_trips
            ),
            "chronological_held_out_available": max(
                0, len(qualifying) - experimental.minimum_training_round_trips
            ),
            "eligible_for_calibration": len(qualifying) >= required_total,
            "reason": (
                "READY_FOR_CHRONOLOGICAL_CALIBRATION"
                if len(qualifying) >= required_total
                else "INSUFFICIENT_ACTUAL_COMPLETED_ROUND_TRIPS"
            ),
            "exclusion_reasons": dict(sorted(exclusions.items())),
            "one_cycle_is_one_independent_observation": True,
            "status_updates_do_not_inflate_observation_count": True,
        },
        "cycles": cycle_evidence,
        "candidate_checks": candidate_events[-200:],
        "policy": experiment_policy_report(),
        "model_transition": {
            "source": "broker_confirmed_experimental_signal_scalp_cycles",
            "chronological_split_required": True,
            "realistic_costs_required": True,
            "artifact_write_required": True,
            "worker_load_requires_validated_status": True,
            "current_state": (
                "ready_for_calibration"
                if len(qualifying) >= required_total
                else "collecting_independent_round_trips"
            ),
        },
        "acceptance_complete": any(
            row["classification"] == ACCEPTANCE_CLASSIFICATION
            and row["state"] == "closed_owned_flat"
            for row in cycle_evidence
        ),
        "acceptance_policy": acceptance.model_dump(mode="json"),
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
