from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from pydantic import ValidationError
from sqlalchemy import and_, case, func, or_, select

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
    EXPERIMENT_CUTOFF,
    EXPERIMENTAL_CLASSIFICATION,
    ExecutionAcceptancePolicy,
    ExperimentalScalpPolicy,
    experiment_policy_report,
)
from tradeagent.scalping_probes import CLASSIFICATION as PROBE_CLASSIFICATION
from tradeagent.scalping_probes import ExecutionValidationProbePolicy
from tradeagent.scalping_store import scalping_cycles, scalping_order_links

PROFILE = "v30-paper-unrestricted"
EXPERIMENT_REPORT_START = datetime(2026, 9, 21, tzinfo=UTC)
PROBE_REPORT_START = datetime(2026, 9, 16, tzinfo=UTC)


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
    probe = ExecutionValidationProbePolicy()
    policies = (
        (PROBE_CLASSIFICATION, probe),
        (ACCEPTANCE_CLASSIFICATION, acceptance),
        (EXPERIMENTAL_CLASSIFICATION, experimental),
    )
    cycle_scope = or_(
        *(
            and_(
                scalping_cycles.c.payload["classification"].as_string() == classification,
                scalping_cycles.c.payload["probe_policy"]["cohort_id"].as_string()
                == policy.cohort_id,
                scalping_cycles.c.created_at < policy.authorization_cutoff,
            )
            for classification, policy in policies
        )
    )
    cohort_candidate_scope = or_(
        *(
            and_(
                events.c.payload["classification"].as_string() == classification,
                events.c.payload["cohort_id"].as_string() == policy.cohort_id,
                events.c.occurred_at >= (
                    PROBE_REPORT_START
                    if classification == PROBE_CLASSIFICATION
                    else EXPERIMENT_REPORT_START
                ),
                events.c.occurred_at < policy.authorization_cutoff,
            )
            for classification, policy in policies
        )
    )
    candidate_scope = cohort_candidate_scope
    if account_digest is not None:
        candidate_scope = and_(
            candidate_scope,
            events.c.payload["account_digest"].as_string() == account_digest,
        )
    with database.begin() as connection:
        cycle_query = select(scalping_cycles).where(cycle_scope)
        if account_digest is not None:
            cycle_query = cycle_query.where(
                scalping_cycles.c.account_digest == account_digest
            )
        raw_cycles = [
            dict(row)
            for row in connection.execute(
                cycle_query.order_by(scalping_cycles.c.created_at)
            )
            .mappings()
        ]
        policy_by_classification = {
            classification: policy for classification, policy in policies
        }
        cycles = []
        excluded_policies: Counter[str] = Counter()
        for row in raw_cycles:
            payload = row["payload"]
            if not isinstance(payload, dict):
                excluded_policies["INVALID_CYCLE_PAYLOAD"] += 1
                continue
            classification = payload.get("classification")
            if (
                not isinstance(classification, str)
                or classification not in policy_by_classification
            ):
                excluded_policies["INVALID_CLASSIFICATION"] += 1
                continue
            policy = policy_by_classification[classification]
            try:
                stored_policy = type(policy).model_validate(payload.get("probe_policy"))
            except ValidationError:
                excluded_policies["INVALID_STORED_POLICY"] += 1
                continue
            if stored_policy.identity != policy.identity:
                excluded_policies["POLICY_HASH_MISMATCH"] += 1
                continue
            cycles.append(row)
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
        candidate_counts = [
            dict(row)
            for row in connection.execute(
                select(
                    events.c.payload["classification"].as_string().label("classification"),
                    events.c.payload["symbol"].as_string().label("symbol"),
                    func.count().label("candidates"),
                    func.sum(
                        case(
                            (events.c.payload["eligible"].as_boolean().is_(False), 1),
                            else_=0,
                        )
                    ).label("blocked_candidates"),
                )
                .where(
                    events.c.event_type == "scalp_paper_experiment_candidate",
                    candidate_scope,
                )
                .group_by(
                    events.c.payload["classification"].as_string(),
                    events.c.payload["symbol"].as_string(),
                )
            )
            .mappings()
        ]
        candidate_events = [
            dict(row)
            for row in connection.execute(
                select(events.c.event_id, events.c.occurred_at, events.c.payload)
                .where(
                    events.c.event_type == "scalp_paper_experiment_candidate",
                    candidate_scope,
                )
                .order_by(events.c.occurred_at.desc(), events.c.event_id.desc())
                .limit(200)
            )
            .mappings()
        ]
        unscoped_candidates = (
            int(
                connection.scalar(
                    select(func.count()).where(
                        events.c.event_type == "scalp_paper_experiment_candidate",
                        cohort_candidate_scope,
                        events.c.payload["account_digest"].as_string().is_(None),
                    )
                )
                or 0
            )
            if account_digest is not None
            else 0
        )
    candidate_events.reverse()
    orders_by_cycle: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in orders:
        broker = dict(row["broker"] or {})
        intent = dict(row["intent"] or {})
        orders_by_cycle[str(row["cycle_id"])].append(
            {
                "client_order_id": row["client_order_id"],
                "broker_order_id": broker.get("id"),
                "side": (intent.get("request") or {}).get("side"),
                "requested_quantity": (intent.get("request") or {}).get("quantity"),
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
    funnel_by_symbol: dict[tuple[str, str], Counter[str]] = defaultdict(Counter)
    for group in candidate_counts:
        classification = str(group["classification"])
        symbol = str(group["symbol"] or "unknown")
        bucket = funnel_by_symbol[(classification, symbol)]
        bucket["candidates"] = int(group["candidates"])
        bucket["blocked_candidates"] = int(group["blocked_candidates"] or 0)
    for row in cycles:
        classification = str(row["payload"].get("classification"))
        symbol = str(row["symbol"])
        bucket = funnel_by_symbol[(classification, symbol)]
        bucket["cycles"] += 1
        if Decimal(str(row["entry_quantity"])) > 0:
            bucket["entry_fills"] += 1
        if row["state"] == "closed_owned_flat":
            bucket["completed_exits"] += 1
            if Decimal(str(row["owned_quantity"])) == 0:
                bucket["flat_reconciliations"] += 1
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
                "run_id": row["run_id"],
                "decision_id": row["decision_id"],
                "classification": classification,
                "cohort_id": row["payload"].get("probe_policy", {}).get("cohort_id"),
                "policy_hash": policy_by_classification[classification].identity,
                "signal": row["payload"].get("signal"),
                "entry_quote": row["payload"].get("entry_quote"),
                "symbol": row["symbol"],
                "state": row["state"],
                "decision_at": row["created_at"].isoformat(),
                "opened_at": row["opened_at"].isoformat() if row["opened_at"] else None,
                "closed_at": row["closed_at"].isoformat() if row["closed_at"] else None,
                "entry_quantity": str(row["entry_quantity"]),
                "entry_value_usd": (
                    str(row["entry_value"]) if row["entry_value"] is not None else None
                ),
                "exit_quantity": str(row["exit_quantity"]),
                "exit_value_usd": (
                    str(row["exit_value"]) if row["exit_value"] is not None else None
                ),
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
    cycle_context = {
        str(row["cycle_id"]): (str(row["payload"].get("classification")), str(row["symbol"]))
        for row in cycles
    }
    for order in orders:
        context = cycle_context.get(str(order["cycle_id"]))
        if context is None:
            continue
        bucket = funnel_by_symbol[context]
        bucket["submissions"] += 1
        intent = order["intent"] if isinstance(order["intent"], dict) else {}
        raw_request = intent.get("request")
        request = raw_request if isinstance(raw_request, dict) else {}
        side = request.get("side")
        if side == "buy":
            bucket["entry_submissions"] += 1
        broker = order["broker"] if isinstance(order["broker"], dict) else {}
        if broker.get("id"):
            bucket["broker_acceptances"] += 1
        if broker.get("status") == "partially_filled":
            bucket["partial_fills"] += 1
    qualifying = [
        row for row in cycle_evidence if row["qualifying_independent_observation"]
    ]
    total_candidates = sum(int(group["candidates"]) for group in candidate_counts)
    blocked_candidates = sum(int(group["blocked_candidates"] or 0) for group in candidate_counts)
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
    completed_experiments = [
        row for row in cycle_evidence
        if row["classification"] == EXPERIMENTAL_CLASSIFICATION
        and row["state"] == "closed_owned_flat"
    ]
    modeled_rows = [
        row for row in completed_experiments if row["modeled_net_pnl_usd"] is not None
    ]
    actual_rows = [
        row for row in completed_experiments
        if not row["fees_pending"] and row["actual_net_pnl_usd"] is not None
    ]
    modeled_net = sum(
        (Decimal(row["modeled_net_pnl_usd"]) for row in modeled_rows), Decimal(0)
    )
    gross_cash = sum(
        (Decimal(row["gross_cash_flow_usd"]) for row in completed_experiments
         if row["gross_cash_flow_usd"] is not None),
        Decimal(0),
    )
    modeled_complete = len(modeled_rows) == len(completed_experiments)
    actual_complete = len(actual_rows) == len(completed_experiments)
    fill_price_coverage = sum(
        row["entry_value_usd"] is not None
        and row["exit_value_usd"] is not None
        and Decimal(row["entry_quantity"]) > 0
        for row in completed_experiments
    )
    gross_cash_coverage = sum(
        row["gross_cash_flow_usd"] is not None for row in completed_experiments
    )
    deployed_value = sum(
        (
            Decimal(row["entry_value_usd"])
            for row in completed_experiments
            if row["entry_value_usd"] is not None
        ),
        Decimal(0),
    )
    fill_price_pnl = sum(
        (
            (
                Decimal(row["exit_value_usd"])
                - Decimal(row["entry_value_usd"])
                / Decimal(row["entry_quantity"])
                * Decimal(row["exit_quantity"])
            )
            for row in completed_experiments
            if row["entry_value_usd"] is not None
            and row["exit_value_usd"] is not None
            and Decimal(row["entry_quantity"]) > 0
        ),
        Decimal(0),
    )
    as_of = datetime.now(UTC)
    expired = as_of >= EXPERIMENT_CUTOFF
    return {
        "schema": "paper-scalping-experiment-status-v1",
        "as_of": as_of.isoformat(),
        "paper_only": True,
        "qualified_strategy_gate_preserved": True,
        "funnel": {
            "candidates": total_candidates,
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
        "funnel_by_symbol": [
            {
                "classification": classification,
                "symbol": symbol,
                **dict(sorted(counts.items())),
            }
            for (classification, symbol), counts in sorted(funnel_by_symbol.items())
        ],
        "candidate_account_scope": {
            "account_digest_matched": account_digest is not None,
            "legacy_unscoped_candidates_excluded": unscoped_candidates,
            "candidate_totals_complete": (
                account_digest is not None and unscoped_candidates == 0
            ),
        },
        "cohort_economics": {
            "cohort_id": experimental.cohort_id,
            "entry_cutoff": EXPERIMENT_CUTOFF.isoformat(),
            "entry_cutoff_passed": expired,
            "completed_round_trips": len(completed_experiments),
            "deployed_entry_value_usd": str(deployed_value),
            "fill_price_pnl_usd": (
                str(fill_price_pnl)
                if fill_price_coverage == len(completed_experiments) else None
            ),
            "fill_price_coverage": fill_price_coverage,
            "gross_cash_flow_usd": (
                str(gross_cash)
                if gross_cash_coverage == len(completed_experiments) else None
            ),
            "gross_cash_coverage": gross_cash_coverage,
            "fill_price_minus_cash_flow_usd": (
                str(fill_price_pnl - gross_cash)
                if fill_price_coverage == gross_cash_coverage == len(completed_experiments)
                else None
            ),
            "modeled_net_pnl_usd": str(modeled_net) if modeled_complete else None,
            "modeled_net_return_on_deployed_bps": (
                str(modeled_net / deployed_value * Decimal(10_000))
                if modeled_complete and deployed_value > 0 else None
            ),
            "modeled_net_coverage": len(modeled_rows),
            "modeled_minus_gross_cash_usd": (
                str(modeled_net - gross_cash)
                if modeled_complete and gross_cash_coverage == len(completed_experiments)
                else None
            ),
            "cost_basis": (
                "broker cash flow includes effects of base-inventory fees; "
                "additional conservative configured fee reserve is included in modeled net; "
                "observed fill prices already embed spread and execution slippage; "
                "fill-price minus cash-flow also includes any reduced sellable quantity "
                "and is not an independently confirmed fee"
            ),
            "actual_net_pnl_usd": (
                str(sum(
                    (Decimal(row["actual_net_pnl_usd"]) for row in actual_rows),
                    Decimal(0),
                )) if actual_complete else None
            ),
            "pending_fee_cycles": len(completed_experiments) - len(actual_rows),
            "observed_result": (
                "insufficient_or_incomplete_cost_data"
                if not completed_experiments or not modeled_complete
                else "negative_after_modeled_costs"
                if modeled_net < 0
                else "positive_after_modeled_costs_unvalidated"
                if modeled_net > 0
                else "zero_after_modeled_costs"
            ),
            "qualification": (
                "expired_insufficient_round_trips"
                if expired and len(qualifying) < required_total
                else "collecting_round_trips"
                if len(qualifying) < required_total
                else "requires_chronological_validation"
            ),
            "no_live_profitability_claim": True,
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
            "exclusion_reasons": dict(sorted((exclusions + excluded_policies).items())),
            "one_cycle_is_one_independent_observation": True,
            "status_updates_do_not_inflate_observation_count": True,
        },
        "cycles": cycle_evidence,
        "candidate_checks": candidate_events,
        "candidate_checks_page_size": 200,
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
                else "expired_insufficient_round_trips"
                if expired
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
