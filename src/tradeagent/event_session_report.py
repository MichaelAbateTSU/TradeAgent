"""Durable, observational session evidence. Never an order or qualification permission."""

from __future__ import annotations

import json
from collections import Counter
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from typing import Any
from urllib.parse import urlencode
from uuid import NAMESPACE_URL, uuid5
from zoneinfo import ZoneInfo

from sqlalchemy import insert, inspect, or_, select
from sqlalchemy.exc import IntegrityError

from tradeagent.event_performance import allocation_ledgers, cost_assumptions
from tradeagent.event_reporting import (
    evidence_labels,
    reported_calibration,
    reporting_limitations,
    reporting_purpose,
    trade_classification,
)
from tradeagent.persistence import (
    Database,
    ProductionRepository,
    events,
    fills,
    notification_outbox,
    orders,
)

REPORT_VERSION = "v20-session-evidence-v1"
EASTERN = ZoneInfo("America/New_York")
TERMINAL = {"filled", "canceled", "cancelled", "rejected", "expired", "risk_rejected"}


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _json(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_json(item) for item in value]
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return value


def _day(value: Any) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _number(value: Any) -> Decimal | None:
    try:
        number = Decimal(str(value))
        return number if number.is_finite() else None
    except (ArithmeticError, ValueError):
        return None


def _timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return _utc(parsed) if parsed.tzinfo is not None else None
    except ValueError:
        return None


def _belongs(payload: dict[str, Any], at: datetime, session_date: date) -> bool:
    planned = _day(payload.get("planned_session_date", payload.get("session_date")))
    return (
        planned == session_date if planned else _utc(at).astimezone(EASTERN).date() == session_date
    )


def _forward(payload: dict[str, Any]) -> bool:
    return not (
        payload.get("mode") == "offline_replay"
        or payload.get("synthetic") is True
        or payload.get("evidence_kind") in {"synthetic", "replay"}
    )


def _record(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_id": row["event_id"],
        "kind": row["event_type"].removeprefix("event_"),
        "at": _utc(row["occurred_at"]).isoformat(),
        "evidence": row["payload"],
    }


def _references(value: Any, identities: set[str]) -> bool:
    if isinstance(value, dict):
        return any(_references(item, identities) for item in value.values())
    if isinstance(value, list):
        return any(_references(item, identities) for item in value)
    return isinstance(value, str) and value in identities


def _timeline(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    timeline = []
    previous: dict[str, str] = {}
    for row in rows:
        kind = row["event_type"].removeprefix("event_")
        if kind in {"performance", "quote_path", "session_report"}:
            continue
        payload = row["payload"]
        if kind in {"source_poll", "calibration_status", "reconciliation", "session_completion"}:
            counts = payload.get("counts", {})
            new_items = (isinstance(counts, dict) and bool(counts.get("items_received"))) or bool(
                payload.get("new_evidence_versions")
            )
            summarized = (
                {
                    key: payload.get(key)
                    for key in (
                        "healthy",
                        "health",
                        "status",
                        "errors",
                        "last_errors",
                        "capability_gaps",
                    )
                }
                if kind == "source_poll"
                else {
                    key: value
                    for key, value in payload.items()
                    if key not in {"observed_at", "checked_at", "snapshot_at", "counts", "coverage"}
                }
            )
            if kind == "source_poll" and isinstance(payload.get("source_health"), dict):
                summarized["source_health"] = {
                    name: {
                        "status": value.get("status"),
                        "document_errors": value.get("document_errors"),
                    }
                    for name, value in payload["source_health"].items()
                    if isinstance(value, dict)
                }
                summarized["coverage_complete"] = payload.get("coverage_complete")
            signature = json.dumps(
                summarized,
                sort_keys=True,
                default=str,
            )
            if previous.get(kind) == signature and not (kind == "source_poll" and new_items):
                continue
            previous[kind] = signature
        timeline.append(_record(row))
    return timeline


def _poll_count(polls: list[dict[str, Any]], key: str) -> dict[str, Any]:
    known: dict[str, int] = {}
    invalid = duplicates = 0
    for poll in polls:
        payload = poll["payload"]
        if key == "items_received" and payload.get("poll_id"):
            identity = str(payload["poll_id"])
            value = payload.get("raw_items_received")
            semantics = "per_poll"
        else:
            identity = poll["event_id"]
            counts = payload.get("counts", payload)
            value = counts.get(key) if isinstance(counts, dict) else None
            semantics = payload.get("count_semantics", payload.get("counts_semantics"))
        if (
            isinstance(value, int)
            and not isinstance(value, bool)
            and value >= 0
            and semantics in {"delta", "per_poll"}
        ):
            if identity in known:
                duplicates += 1
                invalid += known[identity] != value
            else:
                known[identity] = value
        else:
            invalid += 1
    return {
        "count": sum(known.values()) if polls and not invalid else None,
        "recorded_count": sum(known.values()) if known else None,
        "missing_or_conflicting_poll_counts": invalid,
        "duplicate_poll_records": duplicates,
        "basis": (
            "Raw receipts summed once per poll_id may include provider news items, "
            "feed entries and fetched documents, including re-polls/cache/duplicates. "
            "Per-poll unique/matched counts never establish session unique counts."
        ),
    }


def _health(
    polls: list[dict[str, Any]],
    heartbeat: dict[str, Any],
    now: datetime,
    brief: dict[str, Any] | None = None,
) -> dict[str, Any]:
    last = polls[-1] if polls else None
    payload = last["payload"] if last else {}
    capabilities = (brief or {}).get("source_capabilities") or {}
    cache_ttl = capabilities.get("cache_ttl_seconds") if isinstance(capabilities, dict) else None
    freshness_seconds = (
        max(120, 2 * cache_ttl)
        if isinstance(cache_ttl, int | float) and 0 <= cache_ttl <= 86400
        else 120
    )
    freshness = timedelta(seconds=freshness_seconds)
    observed_at = _timestamp(payload.get("observed_at")) or (
        _utc(last["occurred_at"]) if last else None
    )
    errors = payload.get("errors", payload.get("last_errors"))
    healthy = payload.get("healthy")
    if healthy is None and isinstance(payload.get("health"), dict):
        healthy = payload["health"].get("healthy")
    if healthy is None and payload.get("status") in {"healthy", "ok"}:
        healthy = True
    provider_health = payload.get("source_health")
    confirmations: dict[str, str | None] = {}
    interval_sources = []
    interval_failed = False
    document_errors = {}
    if isinstance(provider_health, dict):
        document_errors = {
            name: provider["document_errors"]
            for name, provider in provider_health.items()
            if isinstance(provider, dict) and provider.get("document_errors")
        }
        for name, provider in provider_health.items():
            if (
                not isinstance(provider, dict)
                or str(provider.get("status", "")).startswith("disabled")
                or provider.get("status") == "unavailable_no_verified_feed"
            ):
                continue
            if name == "news" or name.startswith(("sec:", "issuer_feed:")):
                interval_sources.append(name)
                interval_failed |= provider.get("status") == "incomplete_or_failed"
            last_success = None
            for row in reversed(polls):
                historical = row["payload"].get("source_health", {})
                observed = historical.get(name, {}) if isinstance(historical, dict) else {}
                if not isinstance(observed, dict):
                    continue
                received = _timestamp(observed.get("last_http_received_at"))
                verified_feed = (
                    _timestamp(observed.get("feed_verified_at"))
                    if name.startswith("issuer_feed:")
                    else None
                )
                proof_times = []
                if (
                    received is not None
                    and isinstance(observed.get("http_successes"), int)
                    and observed["http_successes"] > 0
                ):
                    proof_times.append(received)
                if verified_feed is not None:
                    proof_times.append(verified_feed)
                recent = [
                    value for value in proof_times if timedelta(0) <= now - value <= freshness
                ]
                if recent:
                    last_success = max(recent).isoformat()
                    break
            confirmations[name] = last_success
        confirmed_healthy = (
            payload.get("coverage_complete") is True
            and errors == []
            and bool(interval_sources)
            and all(confirmations.get(name) for name in interval_sources)
            and not interval_failed
        )
        healthy = True if confirmed_healthy else None
    fresh = bool(observed_at and timedelta(0) <= now - observed_at <= freshness)
    coverage = (brief or {}).get("coverage", payload.get("coverage"))
    through_observation = (
        coverage.get("complete_through_observation") if isinstance(coverage, dict) else None
    )
    return {
        "state": (
            "error"
            if errors or interval_failed or healthy is False
            else "healthy_bounded_coverage"
            if healthy is True and fresh and heartbeat["fresh"] and through_observation is False
            else "healthy"
            if healthy is True and fresh and heartbeat["fresh"]
            else "unknown_or_stale"
        ),
        "worker": heartbeat,
        "last_provider_check_at": observed_at.isoformat() if observed_at else None,
        "provider_check_fresh": fresh,
        "provider_freshness_window_seconds": freshness_seconds,
        "provider_healthy_last_reported": healthy,
        "errors": errors,
        "document_errors": document_errors,
        "coverage": coverage,
        "coverage_complete": payload.get("coverage_complete"),
        "complete_through_observation": through_observation,
        "capability_gaps": (brief or {}).get("capability_gaps", payload.get("capability_gaps")),
        "source_health_status": (brief or {}).get("source_health_status"),
        "source_health_status_as_of": (brief or {}).get("prepared_at"),
        "provider_http_confirmations": confirmations,
        "scope_limitation": (
            "Health applies to configured INTERVAL sources, not universal company-news coverage."
        ),
        "assessment_basis": (
            "Fresh worker/poll plus HTTP or retained feed verification, coverage_complete and "
            "errors for configured INTERVAL sources. Full required-through coverage is separate; "
            "optional comparison/document failures remain separate."
            if isinstance(provider_health, dict)
            else "Legacy explicit provider-health observation; detailed interval scope unavailable."
        ),
        "coverage_scope": payload.get("coverage_scope"),
        "coverage_basis": payload.get("coverage_basis"),
        "coverage_excludes": payload.get("coverage_excludes"),
        "coverage_watermark": payload.get("coverage_watermark"),
        "requested_start": payload.get("requested_start"),
        "requested_end": payload.get("requested_end"),
        "interval_end": payload.get("interval_end"),
        "health_evidence": payload,
        "poll_count": len(polls),
    }


def _merge_brief_news(
    table: list[dict[str, Any]], brief: dict[str, Any] | None
) -> list[dict[str, Any]]:
    by_evidence = {row["evidence_id"]: row for row in table}
    for item in (brief or {}).get("news", []) or []:
        if not isinstance(item, dict) or not item.get("evidence_id"):
            continue
        identity = item["evidence_id"]
        source_times = {
            "published_at": item.get("publication_time"),
            "provider_received_at": item.get("provider_receipt_time"),
            "first_received_at": item.get("bot_first_receipt_time"),
            "revision_at": item.get("revision_time"),
            "revision_observed_at": item.get("revision_observed_at"),
            "decided_at": item.get("decision_time"),
        }
        if identity not in by_evidence:
            row = {
                "evidence_id": identity,
                "decision_id": None,
                "symbol": ", ".join(item.get("symbols") or []) or None,
                "action": item.get("decision_action") or "not_evaluated",
                "failed_rules": item.get("decision_reasons"),
                "rule_results_and_observed_values": None,
                "source_url": item.get("source_url"),
                "publisher": item.get("publisher", item.get("source")),
                "document_id": item.get("document_id"),
                "facts": item.get("quantitative_facts"),
                "timestamps": source_times,
                "decision_ticket": None,
                "selection_records": [],
                "original_source_evidence": None,
                "original_decision": None,
                "expected_net_edge": None,
                "qualification_eligible": False,
            }
            table.append(row)
            by_evidence[identity] = row
        row = by_evidence[identity]
        row["premarket_evidence"] = item
        row["supporting_excerpt_or_reference"] = item.get("immutable_reference")
        if row.get("facts") is None:
            row["facts"] = item.get("quantitative_facts")
        for key in ("source_url", "publisher", "document_id"):
            if row.get(key) is None:
                row[key] = item.get(key)
        for key, value in source_times.items():
            if row["timestamps"].get(key) is None:
                row["timestamps"][key] = value
    return table


def _brief_funnel(brief: dict[str, Any], funnel: dict[str, dict[str, Any]]) -> None:
    snapshot = brief.get("funnel", {})
    if not isinstance(snapshot, dict):
        return
    mapping = {
        "items_received": "raw_items_received",
        "source_health_checks": "source_health_checks",
        "unique_events": "unique_events",
        "supported_company_matches": "supported_company_matches",
        "valid_quantitative_events": "valid_quantitative_events",
        "strategy_candidates": "strategy_candidates",
    }
    for output, source in mapping.items():
        value = snapshot.get(source)
        if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
            funnel[output] = {
                "count": value,
                "recorded_count": value,
                "basis": (
                    "Latest typed premarket-brief snapshot, never summed across snapshots. "
                    + (
                        "Raw items may include provider news, feed entries and fetched documents "
                        "plus re-polls/cache/duplicates, not distinct news."
                        if source == "raw_items_received"
                        else "Source/extraction evidence only; not execution or risk approval."
                    )
                ),
                "source_metric": source,
                "snapshot_id": brief.get("snapshot_id"),
                "as_of": brief.get("prepared_at"),
            }


def _execution_view(
    decision: dict[str, Any], audit: list[dict[str, Any]], state: dict[str, Any] | None
) -> tuple[dict[str, Any] | None, dict[str, Any] | None, dict[str, Any] | None]:
    identity = decision["decision_id"]
    payload_identity = decision["payload"].get("decision_id")

    def evaluation(payload: dict[str, Any]) -> dict[str, Any]:
        result = dict(payload)
        for key in ("payload", "decision", "evaluation", "decision_snapshot"):
            nested = result.get(key)
            if isinstance(nested, dict):
                result.update(nested)
        return result

    matching = []
    for row in audit:
        if row["event_type"] not in {
            "event_candidate_state",
            "event_candidate_evaluation",
            "event_decision_observation",
        }:
            continue
        value = evaluation(row["payload"])
        if (
            value.get("decision_id") in {identity, payload_identity} - {None}
            or value.get("evidence_id") == decision["evidence_id"]
        ):
            matching.append(row)
    state_view = (
        {
            "status": state["status"],
            "updated_at": _utc(state["updated_at"]).isoformat(),
            "payload": state["payload"],
        }
        if state
        else None
    )
    evaluations = [
        {
            "at": _utc(row["occurred_at"]).isoformat(),
            "kind": row["event_type"].removeprefix("event_"),
            "payload": evaluation(row["payload"]),
        }
        for row in matching
    ]
    for row in matching:
        value = evaluation(row["payload"])
        if row["event_type"] == "event_candidate_state":
            status = value.get("status", value.get("state"))
            at = _utc(row["occurred_at"]).isoformat()
            if isinstance(status, str) and (state_view is None or at > state_view["updated_at"]):
                state_view = {"status": status, "updated_at": at, "payload": row["payload"]}
    if state_view:
        evaluations.append(
            {
                "at": state_view["updated_at"],
                "kind": "candidate_state",
                "payload": evaluation(state_view["payload"]),
            }
        )
    informative = [
        row
        for row in evaluations
        if any(
            key in row["payload"]
            for key in ("action", "reasons", "reason", "rule_results", "rule_observations")
        )
    ]
    latest = max(informative, key=lambda row: row["at"]) if informative else None
    reevaluations = [
        row
        for row in evaluations
        if row["kind"] in {"decision_observation", "candidate_evaluation"}
    ]
    review = max(reevaluations, key=lambda row: row["at"]) if reevaluations else None
    return state_view, latest, review


def _news_table(
    decisions: list[dict[str, Any]],
    evidence: dict[str, dict[str, Any]],
    audit: list[dict[str, Any]],
    order_rows: list[dict[str, Any]],
    candidate_states: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    selections = [row for row in audit if row["event_type"] == "event_candidate_selection"]
    tickets = [
        row["payload"].get("decision_ticket", row["payload"])
        for row in audit
        if row["event_type"] == "event_decision_ticket"
    ]
    tickets.extend(
        row["link"]["decision_ticket"]
        for row in order_rows
        if isinstance(row["link"].get("decision_ticket"), dict)
    )
    table = []
    for row in decisions:
        decision = row["payload"]
        if trade_classification(decision) == "EQUIPMENT_TEST":
            continue
        source = evidence.get(row["evidence_id"], {})
        ticket = next(
            (
                item
                for item in tickets
                if item.get("decision_id") == row["decision_id"]
                or item.get("evidence_id") == row["evidence_id"]
            ),
            decision.get("decision_ticket"),
        )
        ticket_fields = ticket if isinstance(ticket, dict) else {}
        state, latest, review = _execution_view(
            row, audit, candidate_states.get(row["decision_id"])
        )
        execution = latest["payload"] if latest else decision
        rule_evidence = review["payload"] if review else execution
        reasons = execution.get("reasons")
        if reasons is None and execution.get("reason") is not None:
            reasons = [execution["reason"]]
        table.append(
            {
                "decision_id": row["decision_id"],
                "evidence_id": row["evidence_id"],
                "symbol": decision.get("symbol", source.get("symbol")),
                "action": execution.get(
                    "action", state["status"] if state else decision.get("action")
                ),
                "failed_rules": reasons,
                "rule_results_and_observed_values": rule_evidence.get(
                    "rule_observations",
                    rule_evidence.get("rule_results", rule_evidence.get("gates")),
                ),
                "source_url": source.get("source_url", source.get("url")),
                "publisher": source.get("publisher", source.get("source")),
                "document_id": source.get("source_event_id", source.get("document_id")),
                "supporting_excerpt_or_reference": source.get(
                    "supporting_excerpt", source.get("content", source.get("immutable_reference"))
                ),
                "facts": decision.get(
                    "facts",
                    ticket_fields.get(
                        "facts", ticket_fields.get("quantitative_facts", source.get("facts"))
                    ),
                ),
                "timestamps": {
                    "published_at": source.get("published_at"),
                    "provider_received_at": source.get("provider_received_at"),
                    "first_received_at": source.get("first_received_at"),
                    "revision_at": source.get("revision_at", source.get("revised_at")),
                    "revision_observed_at": source.get("revision_observed_at"),
                    "decided_at": _utc(row["decided_at"]).isoformat(),
                },
                "decision_ticket": ticket,
                "selection_records": [
                    _record(selection)
                    for selection in selections
                    if _references(selection["payload"], {row["decision_id"], row["evidence_id"]})
                ],
                "original_source_evidence": source,
                "original_decision": decision,
                "first_eligibility_action": decision.get("action"),
                "candidate_state": state,
                "latest_execution_evaluation": review or latest,
                "latest_execution_status_evidence": latest,
                "expected_net_edge": None,
                "qualification_eligible": False,
            }
        )
    return table


def _exposure(
    audit: list[dict[str, Any]], order_rows: list[dict[str, Any]], now: datetime
) -> dict[str, Any]:
    def positions_of(payload: dict[str, Any]) -> Any:
        return payload.get("positions", payload.get("broker_positions"))

    def open_orders_of(payload: dict[str, Any]) -> Any:
        return payload.get("open_orders", payload.get("broker_open_orders"))

    def flat(positions: Any) -> bool:
        values = positions.values() if isinstance(positions, dict) else positions
        for value in values:
            quantity = value.get("quantity", value.get("qty")) if isinstance(value, dict) else value
            if _number(quantity) != 0:
                return False
        return True

    reconciliation_records = [
        row
        for row in audit
        if row["event_type"] in {"event_reconciliation", "event_session_completion"}
    ]
    latest_reconciliation = reconciliation_records[-1] if reconciliation_records else None
    latest_payload = latest_reconciliation["payload"] if latest_reconciliation else {}
    reconciliation_state = str(latest_payload.get("state", "")).lower()
    unresolved = bool(
        latest_payload.get("unconfirmed_local_orders")
        or latest_payload.get("mismatches")
        or latest_payload.get("broker_confirmed", latest_payload.get("confirmed", True)) is False
        or (
            reconciliation_state
            and reconciliation_state
            not in {
                "complete",
                "completed",
                "resolved",
                "reconciled",
                "confirmed",
                "ok",
                "flat",
                "confirmed_flat",
                "flat_confirmed",
                "flat_and_no_open_orders",
            }
        )
    )
    confirmations = [
        row
        for row in reconciliation_records
        if isinstance(positions_of(row["payload"]), list | dict)
        and isinstance(open_orders_of(row["payload"]), list)
        and row["payload"].get("broker_confirmed", row["payload"].get("confirmed", True))
        is not False
    ]
    last = confirmations[-1] if confirmations else None
    payload = last["payload"] if last else {}
    confirmed_at = _utc(last["occurred_at"]) if last else None
    pending = []
    for row in order_rows:
        broker = row["link"].get("broker") or {}
        status = broker.get("status", row["status"])
        if status not in TERMINAL:
            pending.append(
                {
                    "client_order_id": row["client_order_id"],
                    "broker_order_id": broker.get("order_id", row.get("broker_order_id")),
                    "symbol": row["symbol"],
                    "side": row["side"],
                    "status": status,
                    "requested_quantity": str(row["quantity"]),
                    "filled_quantity": broker.get("filled_quantity"),
                    "last_local_update_at": _utc(row["updated_at"]).isoformat(),
                    "broker_observed_at": broker.get("observed_at", broker.get("updated_at")),
                    "trade_classification": trade_classification(row["link"]),
                }
            )
    no_later_orders = bool(
        confirmed_at and all(_utc(row["updated_at"]) <= confirmed_at for row in order_rows)
    )
    return {
        "positions": positions_of(payload),
        "broker_open_orders": open_orders_of(payload),
        "last_broker_confirmation_at": confirmed_at.isoformat() if confirmed_at else None,
        "confirmation_fresh": bool(
            confirmed_at and timedelta(0) <= now - confirmed_at <= timedelta(seconds=120)
        ),
        "pending_or_uncertain_local_orders": pending,
        "unconfirmed_local_orders": latest_payload.get("unconfirmed_local_orders"),
        "mismatches": latest_payload.get("mismatches"),
        "reconciliation_state": latest_payload.get("state"),
        "has_unresolved_reconciliation": unresolved,
        "latest_reconciliation": _record(latest_reconciliation) if latest_reconciliation else None,
        "flat_and_no_orders_at_last_confirmation": (
            flat(positions_of(payload)) and not open_orders_of(payload) if last else None
        ),
        "completion_confirmed": bool(
            last
            and flat(positions_of(payload))
            and not open_orders_of(payload)
            and no_later_orders
            and not pending
            and not unresolved
        ),
        "reconciliation_evidence": _record(last) if last else None,
        "limitation": (
            "No local fill calculation substitutes for broker-confirmed positions/orders."
        ),
    }


def report_delivery(database: Database, cohort_id: str | None, day: date) -> list[dict[str, Any]]:
    with database.begin() as connection:
        rows = connection.execute(
            select(notification_outbox).where(
                notification_outbox.c.notification_type == "daily_agent_status",
                notification_outbox.c.created_at >= datetime.combine(day, time.min, EASTERN),
                notification_outbox.c.created_at
                < datetime.combine(day + timedelta(days=1), time.min, EASTERN),
            )
        ).mappings()
        return [
            {
                "notification_id": row["notification_id"],
                "status": row["status"],
                "attempts": row["attempts"],
                "sent_at": _json(row["sent_at"]),
                "report_id": row["payload"].get("session_report_id"),
                "delivery_failure_or_uncertainty": (
                    row["status"] in {"failed", "needs_review", "sending"}
                ),
            }
            for row in rows
            if row["payload"].get("cohort_id") == cohort_id
        ]


def _session_budget(
    repository: ProductionRepository, cohort_id: str | None, planned: date, now: datetime
) -> dict[str, Any]:
    from tradeagent.event_session import (
        equipment_identity,
        session_control_key,
        session_identity,
    )

    account = repository.get_control(f"{cohort_id}:broker-account") if cohort_id else None
    result: dict[str, Any] = {
        "state": "unknown_account" if not account else "not_recorded",
        "read_at": now.isoformat(),
        "scope": "account and planned session across cohorts; not an active-cohort trade count",
        "session_id": None,
        "planned_session_date": planned.isoformat(),
        "account_digest": account,
        "total_entries_reserved": None,
        "news_entries_reserved": None,
        "equipment_test_id": None,
        "equipment_client_order_id": None,
        "equipment_cohort_id": None,
        "limitation": "Reservations are not proof of submissions, fills or broker reconciliation.",
    }
    if not account:
        return result
    result.update(
        expected_session_id=session_identity(account, planned),
        expected_equipment_test_id=equipment_identity(account, planned),
    )
    raw = repository.get_control(session_control_key(account, planned))
    if raw is None:
        return result
    try:
        budget = json.loads(raw)
    except (TypeError, ValueError):
        return {**result, "state": "unreadable_budget"}
    if not isinstance(budget, dict):
        return {**result, "state": "unreadable_budget"}
    if (
        budget.get("session_id") != result["expected_session_id"]
        or budget.get("planned_session_date") != planned.isoformat()
        or budget.get("account_digest") != account
    ):
        return {**result, "state": "budget_identity_mismatch"}
    for key in (
        "session_id",
        "total_entries_reserved",
        "news_entries_reserved",
        "equipment_test_id",
        "equipment_client_order_id",
        "equipment_cohort_id",
    ):
        result[key] = budget.get(key)
    result["state"] = "recorded"
    return result


def _submission_activity(
    order_rows: list[dict[str, Any]], audit: list[dict[str, Any]], now: datetime
) -> dict[str, Any]:
    dispatches: dict[str, list[dict[str, Any]]] = {}
    unattributed = []
    for record in audit:
        if record["event_type"] != "event_submission_attempt":
            continue
        payload = record["payload"]
        client_id = payload.get("client_order_id")
        if not client_id:
            request = payload.get("order", payload.get("request"))
            client_id = request.get("client_order_id") if isinstance(request, dict) else None
        if isinstance(client_id, str) and client_id:
            dispatches.setdefault(client_id, []).append(record)
        else:
            unattributed.append(_record(record))
    entries = []
    known_order_ids = {row["client_order_id"] for row in order_rows}
    for row in order_rows:
        if row["side"] != "buy":
            continue
        link = row["link"]
        broker = link.get("broker") or {}
        client_id = row["client_order_id"]
        marker = _timestamp(link.get("submission_attempted_at"))
        records = dispatches.get(client_id, [])
        broker_id = broker.get("order_id", row.get("broker_order_id"))
        filled = _number(broker.get("filled_quantity"))
        basis = []
        if marker and marker <= now:
            basis.append("submission_attempted_at")
        if link.get("submission_attempted") is True:
            basis.append("legacy_explicit_submission_attempted")
        if records:
            basis.append("event_submission_attempt")
        if broker_id or (filled is not None and filled > 0):
            basis.append("persisted_broker_acknowledgement_or_fill")
        status = str(row["status"]).lower()
        pre_dispatch_expired = bool(
            not basis
            and status == "expired"
            and link.get("session_id")
            and link.get("planned_session_date")
            and not link.get("submission_attempted_at")
        )
        entries.append(
            {
                "client_order_id": client_id,
                "trade_classification": trade_classification(link),
                "state": (
                    "dispatch_evidenced"
                    if basis
                    else "pre_dispatch_expired"
                    if pre_dispatch_expired
                    else "reserved_not_dispatched"
                    if status in {"reserved", "pending_submission", "risk_rejected"}
                    else "dispatch_unknown"
                ),
                "attempted": True
                if basis
                else False
                if pre_dispatch_expired
                or status in {"reserved", "pending_submission", "risk_rejected"}
                else None,
                "submission_attempted_at": marker.isoformat() if marker else None,
                "first_dispatch_audit_at": (
                    _utc(records[0]["occurred_at"]).isoformat() if records else None
                ),
                "dispatch_evidence": basis,
                "dispatch_audits": [_record(record) for record in records],
                "broker_order_id": broker_id,
                "status": row["status"],
                "provider_code": link.get("provider_code", broker.get("provider_code")),
            }
        )
    for client_id, records in dispatches.items():
        if client_id in known_order_ids:
            continue
        buys = [record for record in records if record["payload"].get("side") == "buy"]
        if buys:
            entries.append(
                {
                    "client_order_id": client_id,
                    "trade_classification": trade_classification(buys[0]["payload"]),
                    "state": "dispatch_evidenced_order_snapshot_unavailable",
                    "attempted": True,
                    "dispatch_evidence": ["event_submission_attempt"],
                    "dispatch_audits": [_record(record) for record in buys],
                }
            )
        elif not all(record["payload"].get("side") == "sell" for record in records):
            unattributed.extend(_record(record) for record in records)
    recorded = sum(entry["attempted"] is True for entry in entries)
    return {
        "count": (
            recorded
            if not unattributed and all(entry["attempted"] is not None for entry in entries)
            else None
        ),
        "recorded_count": recorded,
        "entries": entries,
        "pre_dispatch_expirations": sum(
            entry["state"] == "pre_dispatch_expired" for entry in entries
        ),
        "unattributed_dispatch_audits": unattributed,
        "recorded_by_classification": {
            classification: sum(
                entry["attempted"] is True and entry["trade_classification"] == classification
                for entry in entries
            )
            for classification in ("EQUIPMENT_TEST", "NEWS_STRATEGY", "UNCLASSIFIED")
        },
        "basis": (
            "Distinct entry client IDs with a dispatch marker/audit or persisted broker "
            "acknowledgement/fill. Reservations and local terminal statuses alone "
            "are not attempts. A pre-call marker does not prove broker receipt; "
            "retries of one ID count once."
        ),
    }


def _prior_session_activity(
    database: Database,
    cohort_id: str | None,
    session_id: str | None,
    account: str | None,
    planned: date,
    now: datetime,
    current_orders: list[dict[str, Any]],
    current_fills: list[dict[str, Any]],
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    from tradeagent.event_store import event_cohorts, event_order_links

    result: dict[str, Any] = {
        "state": "unknown_session_identity" if not session_id else "no_matching_rows",
        "session_id": session_id,
        "account_digest": account,
        "scope": (
            "same session identity, separately preserved owning cohorts; not current validation"
        ),
        "qualification_eligible": False,
        "versions": [],
        "order_states_unavailable_at_snapshot": [],
        "current_exit_orders_attributed_to_prior_entries": [],
    }
    if not session_id:
        return result, [], []
    with database.begin() as connection:
        query = (
            select(
                orders,
                event_order_links.c.payload.label("link"),
                event_order_links.c.cohort_id.label("owning_cohort_id"),
            )
            .join(
                event_order_links, orders.c.client_order_id == event_order_links.c.client_order_id
            )
            .where(
                event_order_links.c.cohort_id != cohort_id,
                event_order_links.c.payload["session_id"].as_string() == session_id,
                orders.c.created_at <= now,
            )
            .order_by(orders.c.created_at, orders.c.order_id)
        )
        if account:
            query = query.where(
                event_order_links.c.payload["account_digest"].as_string() == account
            )
        candidates = [
            dict(row)
            for row in connection.execute(query).mappings()
            if _belongs(row["link"], row["created_at"], planned) and _forward(row["link"])
        ]
        result["order_states_unavailable_at_snapshot"] = [
            row["client_order_id"] for row in candidates if _utc(row["updated_at"]) > now
        ]
        rows = [row for row in candidates if _utc(row["updated_at"]) <= now]
        owners = {row["owning_cohort_id"] for row in candidates}
        versions = {
            row["cohort_id"]: dict(row)
            for row in connection.execute(
                select(event_cohorts).where(event_cohorts.c.cohort_id.in_(owners))
            ).mappings()
        }
        prior_audit = (
            [
                dict(row)
                for row in connection.execute(
                    select(events)
                    .where(
                        or_(
                            events.c.trace_id.in_(owners),
                            *[
                                events.c.trace_id.startswith(f"{owner}:", autoescape=True)
                                for owner in owners
                            ],
                        ),
                        events.c.occurred_at >= datetime.combine(planned, time.min, EASTERN),
                        events.c.occurred_at <= now,
                        events.c.event_type != "event_session_report",
                    )
                    .order_by(events.c.occurred_at, events.c.recorded_at)
                ).mappings()
                if _belongs(row["payload"], row["occurred_at"], planned)
                and row["payload"].get("session_id", session_id) == session_id
                and (not account or row["payload"].get("account_digest", account) == account)
                and _forward(row["payload"])
            ]
            if owners
            else []
        )
        execution_fills = [
            dict(row)
            for row in connection.execute(
                select(fills)
                .where(
                    fills.c.order_id.in_([row["order_id"] for row in rows]),
                    fills.c.filled_at <= now,
                )
                .order_by(fills.c.filled_at, fills.c.fill_id)
            ).mappings()
        ]
    origins = {row["client_order_id"]: row for row in rows if row["side"] == "buy"}
    inherited_exits: dict[str, list[dict[str, Any]]] = {}
    for row in current_orders:
        origin = origins.get(row["link"].get("entry_client_order_id"))
        if (
            row["side"] == "sell"
            and origin
            and row["link"].get("session_id") == session_id
            and (not account or row["link"].get("account_digest") == account)
            and row["symbol"] == origin["symbol"]
            and trade_classification(row["link"]) == trade_classification(origin["link"])
        ):
            inherited_exits.setdefault(origin["owning_cohort_id"], []).append(
                {**row, "owning_cohort_id": cohort_id}
            )
            result["current_exit_orders_attributed_to_prior_entries"].append(row["client_order_id"])
    for owner in sorted(owners):
        owned = [row for row in rows if row["owning_cohort_id"] == owner]
        related_exits = inherited_exits.get(owner, [])
        valued_rows = sorted(
            [*owned, *related_exits], key=lambda row: (_utc(row["created_at"]), row["order_id"])
        )
        owned_ids = {row["order_id"] for row in valued_rows}
        history = [
            row
            for row in prior_audit
            if row["trace_id"] == owner or row["trace_id"].startswith(f"{owner}:")
        ]
        metadata = versions.get(owner)
        manifest = (metadata or {}).get("manifest") or {}
        economics = allocation_ledgers(
            valued_rows,
            {},
            _number(manifest.get("settings", {}).get("virtual_equity")),
            session_date=planned,
            purpose=reporting_purpose(manifest),
        )
        result["versions"].append(
            {
                "cohort_id": owner,
                "cohort_metadata": metadata,
                "qualification_eligible": False,
                "included_in_current_cohort_performance": False,
                "economics_basis": (
                    "Factual prior-version fills valued under this report's labeled cost model; "
                    "original cohort metadata and archived reports remain unchanged."
                ),
                "orders": owned,
                "related_exit_orders_from_current_cohort": related_exits,
                "mixed_execution_versions": bool(related_exits),
                "performance_attribution": (
                    "original entry cohort; related exits retain their actual execution cohort"
                ),
                "execution_fills": [
                    row
                    for row in [*execution_fills, *current_fills]
                    if row["order_id"] in owned_ids
                ],
                "submission_activity": _submission_activity(owned, history, now),
                "news_strategy": economics["news_strategy"],
                "equipment_test": economics["equipment_test"],
                "all_execution_economics": economics,
                "timeline": _timeline(history),
            }
        )
    if candidates:
        result["state"] = "recorded_prior_activity"
    return result, rows, prior_audit


def session_report(
    database: Database,
    cohort_id: str | None,
    session_date: date | None = None,
    *,
    observed_at: datetime | None = None,
    persist: bool = False,
) -> dict[str, Any]:
    # Local imports let EventStore.report call this without a module-import cycle.
    from tradeagent.event_outcomes import outcome_summary
    from tradeagent.event_store import (
        EventStore,
        event_candidate_states,
        event_cohorts,
        event_decisions,
        event_evidence,
        event_order_links,
    )

    now = observed_at or datetime.now(UTC)
    if now.tzinfo is None:
        raise ValueError("session report clock must be timezone-aware")
    now = now.astimezone(UTC)
    repository = ProductionRepository(database)
    worker = repository.latest_heartbeat("tradeagent-event-worker")
    with database.begin() as connection:
        if cohort_id is None:
            cohort_id = (worker[2].get("cohort_id") if worker else None) or connection.scalar(
                select(event_cohorts.c.cohort_id)
                .order_by(event_cohorts.c.created_at.desc())
                .limit(1)
            )
        details = worker[2] if worker and worker[2].get("cohort_id") == cohort_id else {}
        manifest = (
            connection.scalar(
                select(event_cohorts.c.manifest).where(event_cohorts.c.cohort_id == cohort_id)
            )
            if cohort_id
            else None
        ) or {}
        settings = manifest.get("settings", {})
        planned = (
            session_date
            or _day(settings.get("planned_session_date", settings.get("practice_start_date")))
            or _day(details.get("planned_session_date", details.get("practice_start_date")))
        )
        planned = planned or now.astimezone(EASTERN).date()
        identity = str(
            uuid5(NAMESPACE_URL, f"{REPORT_VERSION}:{cohort_id}:{planned}:{now.isoformat()}")
        )
        if persist:
            existing = connection.scalar(
                select(events.c.payload).where(events.c.event_id == identity)
            )
            if existing:
                return dict(existing)
        start = datetime.combine(planned - timedelta(days=7), time.min, EASTERN).astimezone(UTC)
        audit = (
            [
                dict(row)
                for row in connection.execute(
                    select(events)
                    .where(
                        or_(
                            events.c.trace_id == cohort_id,
                            events.c.trace_id.startswith(f"{cohort_id}:", autoescape=True),
                        ),
                        events.c.occurred_at >= start,
                        events.c.occurred_at <= now,
                        events.c.event_type != "event_session_report",
                    )
                    .order_by(events.c.occurred_at, events.c.recorded_at)
                ).mappings()
                if _belongs(row["payload"], row["occurred_at"], planned)
                or row["trace_id"].startswith(f"{cohort_id}:{planned}:premarket_brief")
            ]
            if cohort_id
            else []
        )
        decision_rows = (
            [
                dict(row)
                for row in connection.execute(
                    select(event_decisions)
                    .where(
                        event_decisions.c.cohort_id == cohort_id,
                        event_decisions.c.decided_at <= now,
                        event_decisions.c.decided_at >= start,
                    )
                    .order_by(event_decisions.c.decided_at)
                ).mappings()
                if _belongs(row["payload"], row["decided_at"], planned)
            ]
            if cohort_id
            else []
        )
        evidence_ids = {row["evidence_id"] for row in decision_rows}
        candidate_states: dict[str, dict[str, Any]] = {}
        if cohort_id and inspect(database.engine).has_table("event_candidate_states"):
            candidate_states = {
                row["decision_id"]: dict(row)
                for row in connection.execute(
                    select(event_candidate_states).where(
                        event_candidate_states.c.cohort_id == cohort_id,
                        event_candidate_states.c.decision_id.in_(
                            [decision["decision_id"] for decision in decision_rows]
                        ),
                        event_candidate_states.c.updated_at <= now,
                    )
                ).mappings()
                if _forward(row["payload"])
            }
        source_evidence = {
            row["evidence_id"]: row["payload"]
            for row in connection.execute(
                select(event_evidence).where(event_evidence.c.evidence_id.in_(evidence_ids))
            ).mappings()
        }
        order_rows = (
            [
                dict(row)
                for row in connection.execute(
                    select(orders, event_order_links.c.payload.label("link"))
                    .join(
                        event_order_links,
                        orders.c.client_order_id == event_order_links.c.client_order_id,
                    )
                    .where(
                        event_order_links.c.cohort_id == cohort_id,
                        orders.c.created_at <= now,
                    )
                    .order_by(orders.c.created_at, orders.c.order_id)
                ).mappings()
                if _belongs(row["link"], row["created_at"], planned)
            ]
            if cohort_id
            else []
        )
        execution_fills = [
            dict(row)
            for row in connection.execute(
                select(fills)
                .where(
                    fills.c.order_id.in_([order["order_id"] for order in order_rows]),
                    fills.c.filled_at <= now,
                )
                .order_by(fills.c.filled_at, fills.c.fill_id)
            ).mappings()
        ]
        latest_saved = connection.scalar(
            select(events.c.payload)
            .where(
                events.c.event_type == "event_session_report",
                events.c.trace_id == (cohort_id or "unassigned-session"),
                events.c.occurred_at <= now,
            )
            .order_by(events.c.occurred_at.desc())
            .limit(1)
        )
    excluded = {
        "audit_records": [row["event_id"] for row in audit if not _forward(row["payload"])],
        "decisions": [row["decision_id"] for row in decision_rows if not _forward(row["payload"])],
        "orders": [row["client_order_id"] for row in order_rows if not _forward(row["link"])],
    }
    audit = [row for row in audit if _forward(row["payload"])]
    decision_rows = [row for row in decision_rows if _forward(row["payload"])]
    order_rows = [row for row in order_rows if _forward(row["link"])]
    future_order_states = [
        row["client_order_id"] for row in order_rows if _utc(row["updated_at"]) > now
    ]
    order_rows = [row for row in order_rows if _utc(row["updated_at"]) <= now]
    order_by_id = {row["order_id"]: row for row in order_rows}
    execution_fills = [row for row in execution_fills if row["order_id"] in order_by_id]
    heartbeat = {
        "state": details.get("state"),
        "last_confirmed_at": worker[1].isoformat() if details and worker else None,
        "fresh": bool(
            details and worker and timedelta(0) <= now - worker[1] <= timedelta(seconds=120)
        ),
    }
    purpose = reporting_purpose(manifest, details)
    polls = [row for row in audit if row["event_type"] == "event_source_poll"]
    premarket_record = next(
        (row for row in reversed(audit) if row["event_type"] == "event_premarket_brief"),
        None,
    )
    brief_payload = (
        premarket_record["payload"]
        if premarket_record
        and premarket_record["payload"].get("schema_version") == "premarket-news-brief-v1"
        and premarket_record["payload"].get("cohort_id") == cohort_id
        and _day(premarket_record["payload"].get("session_date")) == planned
        else None
    )
    health = _health(polls, heartbeat, now, brief_payload)
    performance: dict[str, Any] = next(
        (row["payload"] for row in reversed(audit) if row["event_type"] == "event_performance"),
        {},
    )
    capital = _number(settings.get("virtual_equity", performance.get("virtual_equity_anchor")))
    budget = _session_budget(repository, cohort_id, planned, now)
    runtime_budget = details.get("session_budget")
    runtime_budget = runtime_budget if isinstance(runtime_budget, dict) else {}
    linked_sessions = {
        row["link"]["session_id"] for row in order_rows if row["link"].get("session_id")
    }
    linked_accounts = {
        row["link"]["account_digest"] for row in order_rows if row["link"].get("account_digest")
    }
    session_id = (
        budget.get("session_id")
        or budget.get("expected_session_id")
        or details.get("session_id")
        or runtime_budget.get("session_id")
        or (next(iter(linked_sessions)) if len(linked_sessions) == 1 else None)
    )
    account = (
        budget.get("account_digest")
        or runtime_budget.get("account_digest")
        or (next(iter(linked_accounts)) if len(linked_accounts) == 1 else None)
    )
    prior_activity, prior_orders, prior_audit = _prior_session_activity(
        database, cohort_id, session_id, account, planned, now, order_rows, execution_fills
    )
    session_orders = [*order_rows, *prior_orders]
    session_audit = sorted(
        [*audit, *prior_audit], key=lambda row: (row["occurred_at"], row["recorded_at"])
    )
    submissions = _submission_activity(order_rows, audit, now)
    session_submissions = _submission_activity(session_orders, session_audit, now)
    if future_order_states:
        submissions["count"] = None
    if future_order_states or prior_activity["order_states_unavailable_at_snapshot"]:
        session_submissions["count"] = None
    session_plan = details.get("session_plan", details.get("session_schedule"))
    if session_plan is None:
        session_plan = next(
            (
                row["payload"].get("plan", row["payload"])
                for row in reversed(audit)
                if row["event_type"] == "event_session_plan"
            ),
            None,
        )
    if session_plan is None and brief_payload:
        session_plan = {
            key: brief_payload.get(key)
            for key in ("session_date", "session_open", "session_close", "previous_session_close")
        }
        session_plan.update(
            verified_at=None,
            calendar_source=None,
            source_record_id=premarket_record["event_id"] if premarket_record else None,
        )
    attributed_exits = set(prior_activity["current_exit_orders_attributed_to_prior_entries"])
    current_performance_orders = [
        row for row in order_rows if row["client_order_id"] not in attributed_exits
    ]
    economics = allocation_ledgers(
        current_performance_orders, {}, capital, session_date=planned, purpose=purpose
    )
    equipment_orders = [
        _json(row) for row in order_rows if trade_classification(row["link"]) == "EQUIPMENT_TEST"
    ]
    equipment_order_ids = {row["client_order_id"] for row in equipment_orders}
    news_orders = [
        row for row in order_rows if trade_classification(row["link"]) == "NEWS_STRATEGY"
    ]
    news = _merge_brief_news(
        _news_table(decision_rows, source_evidence, audit, news_orders, candidate_states),
        brief_payload,
    )
    exposure = _exposure(session_audit, session_orders, now)
    unavailable_states = [
        *future_order_states,
        *prior_activity["order_states_unavailable_at_snapshot"],
    ]
    exposure["order_states_unavailable_at_snapshot"] = unavailable_states
    exposure["scope"] = "account/session across current and separately preserved prior cohorts"
    exposure["dispatches_without_order_snapshot"] = [
        entry
        for entry in session_submissions["entries"]
        if entry["state"] == "dispatch_evidenced_order_snapshot_unavailable"
    ]
    exposure["unattributed_dispatch_audits"] = session_submissions["unattributed_dispatch_audits"]
    if (
        unavailable_states
        or exposure["dispatches_without_order_snapshot"]
        or exposure["unattributed_dispatch_audits"]
    ):
        exposure["completion_confirmed"] = False
    news_filled = bool(economics["news_strategy"]["execution_costs"])
    prior_news_filled = any(
        version["news_strategy"]["execution_costs"] for version in prior_activity["versions"]
    )
    recorded_stages = {
        "strategy_candidates": sum(
            row["payload"].get("action") == "eligible" for row in decision_rows
        ),
        "attempted_entries": submissions["recorded_count"],
        "filled_orders": sum(
            bool(_number((row["link"].get("broker") or {}).get("filled_quantity")))
            for row in order_rows
        ),
        "completed_round_trips": economics["closed_round_trips"],
    }
    funnel = {"items_received": _poll_count(polls, "items_received")}
    for key in ("unique_events", "supported_company_matches", "valid_quantitative_events"):
        funnel[key] = {
            "count": None,
            "recorded_count": None,
            "basis": (
                "Requires latest session evidence/extraction snapshot. Per-poll unique/matched "
                "counts can repeat; quantitative validity is not a source-poll measurement."
            ),
        }
    funnel["source_health_checks"] = {
        "count": None,
        "recorded_count": None,
        "recorded_poll_audits": len(polls),
        "basis": "Requires latest brief counter; a poll audit is not an individual source check.",
    }
    for key, value in recorded_stages.items():
        funnel[key] = {
            "count": value if polls or value else None,
            "recorded_count": value,
            "basis": (
                "recorded decisions/orders only; absence of upstream evidence is not healthy zero"
            ),
        }
    for key in ("attempted_entries", "filled_orders", "completed_round_trips"):
        funnel[key]["classification_note"] = (
            "All equipment/news executions; equipment is never a news-signal conversion."
        )
    funnel["attempted_entries"].update(
        count=submissions["count"] if order_rows or polls or submissions["entries"] else None,
        basis=submissions["basis"],
        scope="current cohort only; account/session dispatches are reported separately",
    )
    fill_quantities: dict[str, Decimal] = {}
    for row in execution_fills:
        fill_quantities[row["order_id"]] = fill_quantities.get(
            row["order_id"], Decimal(0)
        ) + Decimal(str(row["quantity"]))
    fill_history_complete = bool(order_rows) and all(
        _number((row["link"].get("broker") or {}).get("filled_quantity"))
        == fill_quantities.get(row["order_id"], Decimal(0))
        for row in order_rows
    )
    funnel["fills"] = {
        "count": len(execution_fills) if fill_history_complete else None,
        "recorded_count": len(execution_fills),
        "basis": (
            "Individual execution records only. Cumulative order VWAP is not a fill-event count; "
            "unknown unless execution quantities reconcile to every scoped broker order."
        ),
    }
    risk_records = [row for row in audit if isinstance(row["payload"].get("risk_approved"), bool)]
    risk_count = sum(row["payload"]["risk_approved"] for row in risk_records)
    funnel["risk_approved_decisions"] = {
        "count": risk_count if risk_records else None,
        "recorded_count": risk_count if risk_records else None,
        "basis": "explicit risk_approved audit flags; candidate eligibility is not risk approval",
    }
    if brief_payload:
        _brief_funnel(brief_payload, funnel)
    calibration_audit = next(
        (row for row in reversed(audit) if row["event_type"] == "event_calibration_status"), None
    )
    calibration = (
        calibration_audit["payload"] if calibration_audit else reported_calibration(details)
    )
    runtime_calibration = reported_calibration(details)
    if (
        isinstance(runtime_calibration, dict)
        and runtime_calibration.get("state") == "already_consumed_by_prior_cohort"
    ):
        calibration = runtime_calibration
    calibration_fields = calibration if isinstance(calibration, dict) else {}
    prior_owner = budget.get("equipment_cohort_id") or calibration_fields.get("owning_cohort")
    if prior_owner and prior_owner != cohort_id:
        prior_activity["equipment_reference"] = {
            "cohort_id": prior_owner,
            "client_order_id": (
                budget.get("equipment_client_order_id") or calibration_fields.get("client_order_id")
            ),
            "matched_version": any(
                version["cohort_id"] == prior_owner for version in prior_activity["versions"]
            ),
        }
        if not prior_activity["equipment_reference"]["matched_version"]:
            prior_activity["state"] = "referenced_prior_activity_not_found"
    if not session_id or prior_activity["state"] == "referenced_prior_activity_not_found":
        session_submissions["count"] = None
    reasons = Counter(str(reason) for row in news for reason in (row.get("failed_rules") or []))
    state = (
        "NOT_STARTED"
        if now.astimezone(EASTERN).date() < planned
        else "MISSED"
        if now.astimezone(EASTERN).date() > planned and not audit and not session_orders
        else "COMPLETE_AT_LAST_BROKER_CONFIRMATION"
        if exposure["completion_confirmed"]
        else "UNRESOLVED_OR_IN_PROGRESS"
    )
    next_actions = []
    if economics["unvalued_fills"]:
        next_actions.append(
            "Reconcile missing broker fill prices for "
            + ", ".join(str(row["client_order_id"]) for row in economics["unvalued_fills"])
            + "; known quantities are retained, but no substitute price or P&L is assumed."
        )
    if not exposure["completion_confirmed"]:
        next_actions.append(
            "Confirm both broker positions and outstanding orders; reconcile uncertain IDs "
            "and continue permitted recovery. No claim of completed flattening is supported."
        )
    if health["state"] == "healthy_bounded_coverage":
        next_actions.append(
            "Verify the uncovered required-through interval "
            "beyond the recorded coverage watermark; "
            "healthy bounded acquisition does not establish complete current interval coverage."
        )
    elif health["state"] != "healthy":
        next_actions.append(
            "Restore/verify fresh worker and independent provider/parser health evidence; "
            "missing news counts do not establish healthy silence."
        )
    if not news_filled and not prior_news_filled:
        next_actions.append(
            "Review recorded failed rules and coverage gaps; do not relax rules to force a trade."
        )
    missing_stages = [key for key, value in funnel.items() if value["count"] is None]
    if missing_stages:
        next_actions.append(
            "Complete or reconcile missing funnel evidence: "
            + ", ".join(missing_stages)
            + ". Cumulative order VWAP does not establish individual fill counts."
        )
    if budget["state"] != "recorded":
        next_actions.append(
            "Verify the account/session reservation record before interpreting remaining capacity; "
            f"reservation evidence is {budget['state']}, not a confirmed zero."
        )
    if prior_activity["state"] == "referenced_prior_activity_not_found":
        next_actions.append(
            "Reconcile the prior-cohort equipment reference: its matching session order history "
            "was not found. Current-cohort zero fills do not mean the session budget was unused."
        )
    next_actions.append(
        "Preserve all news outcomes; obtain comparable declared benchmark observations "
        "and timestamped thesis evidence before making economic or causal claims."
    )
    report = _json(
        {
            **evidence_labels(purpose),
            "report_version": REPORT_VERSION,
            "report_id": identity,
            "snapshot_persisted": persist,
            "snapshot_at": now,
            "cohort_id": cohort_id,
            "planned_session_date": planned,
            "session_id": session_id,
            "session_budget": budget,
            "submission_activity": submissions,
            "exit_orders_attributed_to_prior_entry_cohorts": sorted(attributed_exits),
            "prior_cohort_activity": prior_activity,
            "account_session_activity": {
                "session_id": session_id,
                "account_digest": account,
                "scope": (
                    "current and preserved prior cohorts; no cross-version performance pooling"
                ),
                "qualification_eligible": False,
                "current_cohort_id": cohort_id,
                "prior_cohort_ids": [
                    version["cohort_id"] for version in prior_activity["versions"]
                ],
                "submission_activity": session_submissions,
                "recorded_filled_orders": sum(
                    bool(_number((row["link"].get("broker") or {}).get("filled_quantity")))
                    for row in session_orders
                ),
            },
            "runtime_session_budget_summary": details.get("session_budget"),
            "runtime_premarket_brief_summary": details.get("premarket_brief"),
            "broker_stream_health": details.get("broker_stream"),
            "session_state": state,
            "qualification_eligible": False,
            "qualified": False,
            "qualifying_sessions": 0,
            "qualifying_round_trips": 0,
            "protocol": {
                "manifest": manifest,
                "code_sha": details.get("code_sha", manifest.get("code_sha")),
                "config_hash": details.get("config_hash", manifest.get("config_hash")),
                "execution_feed": details.get("execution_feed"),
                "planned_entry_budget": settings.get("max_entries_per_session"),
                "news_entry_budget": settings.get("max_news_entries_per_session"),
                "equipment_test_id": (
                    budget.get("equipment_test_id") or details.get("equipment_test_id")
                ),
                "entry_and_exit_times": session_plan,
                "cost_assumptions": cost_assumptions(),
            },
            "health": health,
            "limitations": reporting_limitations(details, purpose),
            "funnel": funnel,
            "excluded_synthetic_or_replay": excluded,
            "order_states_unavailable_at_snapshot": future_order_states,
            "no_news_trade_status": (
                "news_trade_recorded"
                if news_filled
                else "prior_cohort_news_trade_recorded_not_current_cohort_performance"
                if prior_news_filled
                else "healthy_candidates_not_executed"
                if health["state"] == "healthy" and recorded_stages["strategy_candidates"]
                else "healthy_no_qualifying_event"
                if health["state"] == "healthy"
                else "healthy_bounded_coverage_no_news_fill"
                if health["state"] == "healthy_bounded_coverage"
                else "unknown_or_failed_processing_not_healthy_silence"
            ),
            "failed_rules": dict(reasons),
            "premarket_brief": next(
                (
                    _record(row)
                    for row in reversed(audit)
                    if row["event_type"] == "event_premarket_brief"
                ),
                None,
            ),
            "source_funnel_snapshot": (brief_payload or {}).get("funnel"),
            "equipment_test": {
                "trade_classification": "EQUIPMENT_TEST",
                "qualification_eligible": False,
                "status": calibration,
                "stable_test_id": (
                    budget.get("equipment_test_id")
                    or calibration_fields.get("equipment_test_id", calibration_fields.get("id"))
                ),
                "reserved_client_order_id": (
                    budget.get("equipment_client_order_id")
                    or calibration_fields.get("client_order_id")
                ),
                "recorded_equipment_cohort_id": (
                    budget.get("equipment_cohort_id") or calibration_fields.get("owning_cohort")
                ),
                "orders": equipment_orders,
                "execution_fills": [
                    row
                    for row in execution_fills
                    if trade_classification(order_by_id[row["order_id"]]["link"])
                    == "EQUIPMENT_TEST"
                ],
                "economics": economics["equipment_test"],
                "operational_records": [
                    _record(row)
                    for row in audit
                    if row["event_type"]
                    in {
                        "event_calibration_status",
                        "event_reconciliation",
                        "event_session_completion",
                    }
                    or trade_classification(row["payload"]) == "EQUIPMENT_TEST"
                    or _references(row["payload"], equipment_order_ids)
                ],
                "not_source_event": True,
            },
            "news_decisions": news,
            "candidate_evaluations": [
                _record(row)
                for row in audit
                if row["event_type"] in {"event_candidate_evaluation", "event_decision_observation"}
            ],
            "candidate_states": [
                {"decision_id": row["decision_id"], **row["candidate_state"]}
                for row in news
                if row.get("candidate_state")
            ],
            "candidate_state_table_snapshot": list(candidate_states.values()),
            "broker_stream_updates": [
                _record(row) for row in audit if row["event_type"] == "event_broker_stream_update"
            ],
            "broker_stream_accounting_basis": (
                "Original stream observations and REST confirmations are retained as evidence. "
                "Economics use confirmed persisted order links, never stream-only fill claims."
            ),
            "candidate_selections": [
                _record(row) for row in audit if row["event_type"] == "event_candidate_selection"
            ],
            "decision_tickets": [
                _record(row) for row in audit if row["event_type"] == "event_decision_ticket"
            ],
            "prospective_diagnostics": outcome_summary(
                EventStore(database),
                cohort_id,
                decision_ids={row["decision_id"] for row in decision_rows},
            )
            if cohort_id and inspect(database.engine).has_table("event_outcomes")
            else {
                "state": "unknown: quote-path schema or cohort unavailable",
                "qualification_eligible": False,
                "available_quote_paths": None,
            },
            "news_strategy": economics["news_strategy"],
            "news_execution_fills": [
                row
                for row in execution_fills
                if trade_classification(order_by_id[row["order_id"]]["link"]) == "NEWS_STRATEGY"
            ],
            "all_execution_economics": economics,
            "ending_exposure": exposure,
            "timeline": _timeline(audit),
            "benchmark": {
                "declared": manifest.get("benchmarks", manifest.get("baselines")),
                "cash": economics["cash_benchmark"],
                "start_at": manifest.get("evaluation_start", details.get("evaluation_start")),
                "end_at": now,
                "passive": None,
                "passive_status": "unknown: comparable capital/timing observations not recorded",
            },
            "what_was_established": {
                "software": "Only timestamped order/reconciliation outcomes establish mechanics.",
                "extraction": (
                    "Rule results and source evidence are inspectable; accuracy is not inferred."
                ),
                "trading_hypothesis": (
                    "Unproven. All actual news outcomes, including losses, are retained."
                ),
                "thesis_invalidated": None,
                "sharpe": None,
                "formal_forward_record": (
                    "IEX practice is excluded. A separately frozen research protocol must declare "
                    "strategy, eligibility, accounting, benchmarks and evaluation start; meet "
                    "feed/evidence requirements and later duration/reconciled-trade floors. "
                    "Counts alone never establish an edge."
                ),
            },
            "next_actions": next_actions,
            "delivery": report_delivery(database, cohort_id, now.astimezone(EASTERN).date()),
            "previous_snapshot": (
                {"report_id": latest_saved["report_id"], "snapshot_at": latest_saved["snapshot_at"]}
                if latest_saved
                else None
            ),
        }
    )
    if persist:
        try:
            with database.begin() as connection:
                connection.execute(
                    insert(events).values(
                        event_id=identity,
                        occurred_at=now,
                        recorded_at=datetime.now(UTC),
                        event_type="event_session_report",
                        trace_id=cohort_id or "unassigned-session",
                        payload=report,
                    )
                )
        except IntegrityError:
            with database.begin() as connection:
                existing = connection.scalar(
                    select(events.c.payload).where(events.c.event_id == identity)
                )
                if existing is None:
                    raise
                return dict(existing)
    return dict(report)


def render_session_report(report: dict[str, Any]) -> str:
    def text(value: Any) -> str:
        return "unknown / not recorded" if value is None else json.dumps(value, default=str)

    def economic_lines(ledger: dict[str, Any]) -> list[str]:
        values = [
            f"Broker-paper P&L: {text(ledger['broker_paper_pnl'])} USD; "
            f"base net after modeled reserves: {text(ledger['economic_paper_pnl'])} USD",
            f"Actual filled entry notional: {text(ledger['actual_deployed_notional'])} USD; "
            f"allocated capital: {text(ledger['strategy_capital_denominator'])} USD",
            "Base return / deployed notional: "
            f"{text(ledger['economic_return_on_deployed_notional'])}; "
            "base return / allocated capital: "
            f"{text(ledger['economic_return_on_strategy_capital'])}",
            "Stress net P&L (USD): "
            + "; ".join(
                f"{name}: {text(value['net_pnl'])}"
                for name, value in ledger["stress_scenarios"].items()
            ),
            f"Cost method: {ledger['cost_assumptions']['version']}; "
            f"additional residual: {text(ledger['omitted_slippage_assumption'])} USD; "
            f"sell fee reserve: {text(ledger['regulatory_fee_reserve'])} USD",
        ]
        if ledger.get("unvalued_fills"):
            values.append(
                f"Unvalued broker fills (known quantities): {text(ledger['unvalued_fills'])}"
            )
        for cost in ledger["execution_costs"]:
            values.append(
                f"Fill {cost['client_order_id']} {cost['side']}: "
                f"{cost['filled_quantity']} shares @ {text(cost['filled_average_price'])}; "
                f"partial={text(cost['partially_filled'])}; "
                f"additional cost={text(cost['total_additional_cost'])} USD; "
                f"broker time={text(cost['broker_confirmed_at'])}"
            )
        for trade in ledger.get("trades", []):
            ticket = trade.get("entry_ticket") or {}
            values.extend(
                [
                    f"Round {trade['trade_id']} {trade['symbol']} {trade['state']}: "
                    f"broker {text(trade['broker_paper_pnl'])} / "
                    f"base {text(trade['economic_paper_pnl'])} USD",
                    f"Entry {text(trade['entry_at'])}; exit {text(trade['exit_at'])}; "
                    f"original thesis {text(ticket.get('thesis'))}",
                    f"Thesis invalidated: {text(trade['thesis_invalidated'])}; "
                    f"evidence: {text(trade['thesis_outcome_evidence'])}",
                ]
            )
        return values

    def incident_text(record: dict[str, Any]) -> str:
        evidence = record["evidence"]
        summary = {
            key: value
            for key, value in evidence.items()
            if key
            in {
                "state",
                "status",
                "symbol",
                "client_order_id",
                "order_id",
                "broker_order_id",
                "filled_quantity",
                "filled_average_price",
                "side",
                "quantity",
                "reasons",
                "errors",
                "error",
                "mismatches",
                "positions",
                "open_orders",
                "outcome",
                "entry_attempts",
                "selected",
                "blocked",
                "rule",
                "alternatives",
                "counts",
                "healthy",
            }
        }
        return (
            f"{record['at']} {record['kind']} [{record['event_id']}]: {text(summary or evidence)}"
        )

    lines = [
        f"SESSION EVIDENCE REPORT — {report['planned_session_date']} (America/New_York)",
        f"Snapshot {report['snapshot_at']}; report {report['report_id']}",
        f"State: {report['session_state']}; news processing: {report['health']['state']}",
        f"No-news-trade assessment: {report['no_news_trade_status']}",
        f"Broker stream health (last reported): {text(report.get('broker_stream_health'))}",
        f"Account/session reservation budget: {text(report.get('session_budget'))}",
        "Session calendar plan (verification/source as recorded): "
        f"{text(report['protocol']['entry_and_exit_times'])}",
        "Equipment and NEWS_STRATEGY results are permanently separate. "
        "Paper practice is unqualified.",
        "",
        "DECISION FUNNEL — CURRENT COHORT",
        *[
            f"- {name}: {text(value['count'])}; recorded subset: {text(value['recorded_count'])}"
            for name, value in report["funnel"].items()
        ],
        f"Failed rules: {text(report['failed_rules'])}",
        f"Entry dispatch evidence: {text(report.get('submission_activity'))}",
        "Exit orders attributed to preserved entry cohorts, not current performance: "
        + text(report.get("exit_orders_attributed_to_prior_entry_cohorts")),
        f"Provider health and coverage: {text(report['health'])}",
        "Premarket brief: "
        + text(
            {
                key: report["premarket_brief"]["evidence"].get(key)
                for key in (
                    "snapshot_id",
                    "preparation_status",
                    "prepared_at",
                    "initial_prepared_at",
                    "prepared_before_open",
                    "coverage",
                    "funnel",
                    "capability_gaps",
                    "blocking_reason_counts",
                )
            }
            if report["premarket_brief"]
            else None
        ),
        "",
        "CURRENT-COHORT EQUIPMENT_TEST — OPERATIONAL ONLY",
        f"Outcome: {text(report['equipment_test']['status'])}",
        *[
            f"Order {row['client_order_id']} {row['side']} {row['status']}; "
            f"requested {row['quantity']}; broker {text(row['link'].get('broker'))}; "
            f"last local update {row['updated_at']}"
            for row in report["equipment_test"]["orders"]
        ],
        *economic_lines(report["equipment_test"]["economics"]),
        "",
        "NEWS DECISION TABLE — ALL RECORDED CANDIDATES",
    ]
    if not report["news_decisions"]:
        lines.append(
            "No news decisions recorded; see coverage and funnel before inferring silence."
        )
    for row in report["news_decisions"]:
        lines.extend(
            [
                f"{row['symbol']} | current execution action: {row['action']}",
                f"First eligibility/decision time: {text(row['timestamps']['decided_at'])}; "
                f"first action: {text(row.get('first_eligibility_action'))}",
                f"Latest candidate state: {text(row.get('candidate_state'))}",
                f"Latest execution re-evaluation: {text(row.get('latest_execution_evaluation'))}",
                f"Source: {text(row['source_url'])}; publisher: {text(row['publisher'])}",
                f"Immutable source reference: {text(row.get('supporting_excerpt_or_reference'))}",
                f"Facts: {text(row['facts'])}; timestamps: {text(row['timestamps'])}",
                f"Failed rules: {text(row['failed_rules'])}; values: "
                f"{text(row['rule_results_and_observed_values'])}",
                f"Pre-entry ticket: {text(row['decision_ticket'])}",
            ]
        )
    lines.extend(
        [
            f"Selection alternatives/ranking: {text(report['candidate_selections'])}",
            "",
            "NEWS_STRATEGY ECONOMICS — INCLUDING EVERY LOSS (CURRENT COHORT)",
            *economic_lines(report["news_strategy"]),
            f"Benchmarks: {text(report['benchmark'])}",
            "Returns are ratios with actual filled-entry notional "
            "and allocated-capital denominators.",
            "Base fills already contain execution prices; no second spread subtraction.",
            "1.5x/2x/3x residual-cost stresses are separate assumptions, not a replacement base.",
            "Thesis invalidation: unknown unless evidenced. "
            "No meaningful Sharpe or qualified edge.",
            "",
            "ACCOUNT/SESSION ACTIVITY — INCLUDING PRESERVED PRIOR COHORTS",
            text(report.get("account_session_activity")),
            "PRIOR-COHORT ACTIVITY — SEPARATELY VERSIONED, NEVER CURRENT VALIDATION",
            "Lookup status: " + text((report.get("prior_cohort_activity") or {}).get("state")),
            "Prior equipment reference: "
            + text((report.get("prior_cohort_activity") or {}).get("equipment_reference")),
        ]
    )
    for version in (report.get("prior_cohort_activity") or {}).get("versions", []):
        metadata = version.get("cohort_metadata") or {}
        lines.extend(
            [
                f"Preserved cohort: {version['cohort_id']}; metadata: "
                + text({key: value for key, value in metadata.items() if key != "manifest"}),
                f"Valuation basis: {version['economics_basis']}",
                f"Entry dispatches: {text(version['submission_activity'])}",
                f"Original orders, frozen tickets and exit decisions: {text(version['orders'])}",
                "Related exits executed under the current cohort: "
                + text(version.get("related_exit_orders_from_current_cohort")),
                "Prior EQUIPMENT_TEST — operational only",
                *economic_lines(version["equipment_test"]),
                "Prior NEWS_STRATEGY — all outcomes retained in their original version",
                *economic_lines(version["news_strategy"]),
            ]
        )
    lines.extend(
        [
            "",
            "ENDING POSITIONS AND OUTSTANDING ORDERS — ACCOUNT/SESSION",
            text(report["ending_exposure"]),
            "",
            "MEANINGFUL ACTIONS AND INCIDENTS",
            f"{len(report['timeline'])} timeline records retained in the structured report; "
            "latest 100 shown below.",
            *[incident_text(row) for row in report["timeline"][-100:]],
            "",
            "NEXT CORRECTIONS / OBSERVATIONS",
            *[f"- {action}" for action in report["next_actions"]],
            (
                "This structured report is persisted independently of email delivery."
                if report["snapshot_persisted"]
                else "Live read-only snapshot, not persisted. "
                "Saved email snapshots remain available."
            ),
            "Read-only report: https://tradeagent-runtime-dashboard.onrender.com/"
            "api/event-session-report?"
            + urlencode(
                {"report_id": report["report_id"]}
                if report["snapshot_persisted"]
                else {
                    "cohort_id": report["cohort_id"] or "",
                    "session_date": report["planned_session_date"],
                }
            ),
        ]
    )
    return "\n".join(lines)
