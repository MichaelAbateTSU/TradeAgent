from __future__ import annotations

import logging
from collections import Counter
from datetime import UTC, date, datetime, time, timedelta
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import func, select

from tradeagent.event_reporting import (
    evidence_labels,
    reported_calibration,
    reporting_limitations,
    reporting_purpose,
)
from tradeagent.event_session_report import render_session_report, session_report
from tradeagent.event_store import event_cohorts, event_decisions, event_order_links
from tradeagent.notifications import RoundTripNotificationRepository
from tradeagent.persistence import Database, ProductionRepository, events, orders
from tradeagent.reporting_reads import payload_from_projection, projected_payload, stream_rows

LOGGER = logging.getLogger(__name__)


class DailyStatusSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="EMAIL_DAILY_",
        env_file=".env",
        extra="ignore",
        frozen=True,
    )

    enabled: bool = True
    timezone: str = "America/New_York"
    hour: int = Field(default=18, ge=0, le=23)
    minute: int = Field(default=0, ge=0, le=59)

    @field_validator("timezone")
    @classmethod
    def validate_timezone(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except ZoneInfoNotFoundError as error:
            raise ValueError("daily email timezone must be a valid IANA timezone") from error
        return value


def daily_notification_id(day: date, timezone: str) -> UUID:
    return uuid5(NAMESPACE_URL, f"tradeagent:daily-agent-status:{timezone}:{day.isoformat()}")


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


class DailyStatusScheduler:
    """Enqueue once per local day, without generating a historical backlog."""

    def __init__(self, database: Database, settings: DailyStatusSettings):
        self.database = database
        self.settings = settings
        self.outbox = RoundTripNotificationRepository(database)
        self.repository = ProductionRepository(database)
        self._last_enqueued_day: date | None = None

    def enqueue_due(self, *, observed_at: datetime) -> bool:
        if observed_at.tzinfo is None or observed_at.utcoffset() is None:
            raise ValueError("daily email clock must be timezone-aware")
        if not self.settings.enabled:
            return False
        local = observed_at.astimezone(ZoneInfo(self.settings.timezone))
        day = local.date()
        if local.time() < time(self.settings.hour, self.settings.minute):
            return False
        if self._last_enqueued_day == day:
            return False
        notification_id = daily_notification_id(day, self.settings.timezone)
        if self.outbox.contains(notification_id):
            self._last_enqueued_day = day
            return False
        payload = build_daily_status(self.database, observed_at, self.settings.timezone)
        created = self.outbox.enqueue_status(notification_id, payload, created_at=observed_at)
        self._last_enqueued_day = day
        if created:
            self.repository.append_event(
                "daily_agent_status_enqueued",
                {
                    "notification_id": str(notification_id),
                    "local_date": day.isoformat(),
                    "timezone": self.settings.timezone,
                    "cohort_id": payload["cohort_id"],
                },
                occurred_at=observed_at,
                trace_id=f"daily-status:{day.isoformat()}",
            )
            LOGGER.info(
                "Daily agent status queued: date=%s notification_id=%s", day, notification_id
            )
        return created


def build_daily_status(database: Database, now: datetime, timezone: str) -> dict[str, Any]:
    zone = ZoneInfo(timezone)
    local = now.astimezone(zone)
    start = datetime.combine(local.date(), time.min, zone).astimezone(UTC)
    end = datetime.combine(local.date() + timedelta(days=1), time.min, zone).astimezone(UTC)
    repository = ProductionRepository(database)
    worker = repository.latest_heartbeat("tradeagent-event-worker")
    details = worker[2] if worker else {}
    cohort_id = str(details["cohort_id"]) if details.get("cohort_id") else None
    if cohort_id is None:
        with database.begin() as connection:
            cohort_id = connection.scalar(
                select(event_cohorts.c.cohort_id)
                .order_by(event_cohorts.c.created_at.desc())
                .limit(1)
            )
    fresh = worker is not None and timedelta(0) <= now - worker[1] <= timedelta(seconds=120)
    status = str(details.get("state", "not_started")) if fresh else "stale_or_missing"
    reasons: Counter[str] = Counter()
    decisions_today = candidates = 0
    position_count = pending_count = 0
    perf: dict[str, Any] | None = None
    perf_at: datetime | None = None
    cohort: dict[str, Any] = {}
    with database.begin() as connection:
        if cohort_id:
            manifest = connection.scalar(
                select(event_cohorts.c.manifest).where(event_cohorts.c.cohort_id == cohort_id)
            )
            cohort = dict(manifest) if manifest else {}
            decisions = stream_rows(
                connection,
                select(*projected_payload(event_decisions.c.payload, ("action", "reasons"))).where(
                    event_decisions.c.cohort_id == cohort_id,
                    event_decisions.c.decided_at >= start,
                    event_decisions.c.decided_at < end,
                    event_decisions.c.decided_at <= now,
                ),
            )
            for decision in decisions:
                decisions_today += 1
                candidates += decision.get("action") == "eligible"
                reasons.update(str(reason) for reason in (decision.get("reasons") or []))
            pending_count = int(
                connection.scalar(
                    select(func.count())
                    .select_from(
                        orders.join(
                            event_order_links,
                            orders.c.client_order_id == event_order_links.c.client_order_id,
                        )
                    )
                    .where(
                        event_order_links.c.cohort_id == cohort_id,
                        orders.c.status.not_in(
                            ("filled", "canceled", "rejected", "expired", "risk_rejected")
                        ),
                    )
                )
                or 0
            )
            performance_fields = (
                "purpose",
                "positions",
                "broker_paper_pnl",
                "economic_paper_pnl",
                "closed_round_trips",
                "calibration_round_trips",
                "fixed_service_cost_usd",
            )
            performance = (
                connection.execute(
                    select(
                        *projected_payload(events.c.payload, performance_fields),
                        events.c.occurred_at,
                    )
                    .where(
                        events.c.event_type == "event_performance",
                        events.c.trace_id == cohort_id,
                        events.c.occurred_at <= now,
                    )
                    .order_by(events.c.occurred_at.desc())
                    .limit(1)
                )
                .mappings()
                .one_or_none()
            )
            if performance:
                perf = payload_from_projection(dict(performance), performance_fields)
                perf_at = _utc(performance["occurred_at"])
                position_count = len(perf.get("positions", {}))
    perf_current = perf_at is not None and timedelta(0) <= now - perf_at <= timedelta(seconds=120)
    purpose = reporting_purpose(cohort, details, perf)
    practice = purpose == "iex-practice"
    limitations = reporting_limitations(details, purpose)
    blockers = limitations["blockers"]
    capabilities = limitations["capability_limitations"]
    pause = repository.get_control(f"{cohort_id}:pause") if cohort_id else None
    if pause:
        blockers.append(f"Operational pause: {pause}")
    next_steps = []
    if not fresh:
        next_steps.append(
            "Restore the event worker and confirm fresh heartbeats; keep entries paused."
        )
    if pause or pending_count:
        next_steps.append(
            "Reconcile pending orders and owned positions before permitting any new risk."
        )
    if any("IEX" in value or "SIP" in value for value in blockers):
        next_steps.append(
            "Restore fresh real-time Alpaca IEX data and rerun the practice paper-preflight. "
            "SIP is not required for this isolated practice cohort; all risk gates remain."
            if practice
            else "Resolve latest-SIP access and select a feed allowed by the frozen protocol, "
            "then rerun paper-preflight. No subscription purchase or gate relaxation is automatic."
        )
    if any("INFERENCE" in value for value in [*capabilities, *blockers]):
        next_steps.append(
            "Continue the deterministic extractor; broader document understanding requires an "
            "explicitly configured, budgeted inference provider. Unknown facts remain abstentions."
        )
    if any(value.startswith(("news:", "sec:", "primary:")) for value in blockers):
        next_steps.append(
            "Resolve the reported source errors and verify primary-source arrival before trading."
        )
    if fresh and not candidates:
        next_steps.append(
            "Keep recording primary-source events and justified abstentions; do not force a trade."
        )
    if not next_steps:
        next_steps.append(
            "Continue the frozen cohort, monitor risk/reconciliation, and review forward evidence."
        )
    next_steps.append(
        "Operational IEX paper practice only: sessions and round trips (including calibration) "
        "are excluded from research qualification and its 60-session/60-round-trip floor. "
        "No profitability or alpha claim is established."
        if practice
        else "No profitability or alpha claim is established. Forward qualification requires "
        "genuine observations, including the declared 60-session/60-round-trip floor."
    )

    def metric(key: str) -> str:
        value = perf.get(key) if perf else None
        return str(value) if value is not None else "unknown / not recorded"

    config = cohort.get("settings", {})
    practice_start_date = details.get(
        "practice_start_date", config.get("practice_start_date", "unknown")
    )
    calibration = reported_calibration(details)
    calibration_state = (
        calibration.get("state", "not reported") if isinstance(calibration, dict) else calibration
    )
    lines = [
        "TRADEAGENT DAILY STATUS - PAPER ONLY",
        f"Reporting date: {local.date()} ({timezone})",
        f"Snapshot: {local.isoformat()}",
        "",
        "CURRENT AGENT STATUS",
        f"Mode (last reported): {details.get('mode', 'unknown')}",
        f"Purpose: {purpose}",
        f"Execution feed (last reported): {details.get('execution_feed', 'unknown')}",
        *([f"Practice start date: {practice_start_date}"] if practice else []),
        f"Event worker: {status}",
        f"Last heartbeat: {worker[1].astimezone(zone).isoformat() if worker else 'none'}",
        f"Market phase (last reported): {details.get('market_phase', 'unknown')}",
        f"Next open (last reported): {details.get('next_open', 'unknown')}",
        f"Cohort: {cohort_id or 'not started'}",
        f"Agent code: {details.get('code_sha', 'unknown')}",
        f"Configuration: {details.get('config_hash', 'unknown')}",
        "",
        "TODAY'S EVENT ACTIVITY",
        "First persisted decisions; "
        "current candidate execution states appear in the session report.",
        f"Decisions: {decisions_today}; eligible: {candidates}; "
        f"abstained: {decisions_today - candidates}",
        f"Calibration status (last reported): {calibration_state or 'not reported'}",
        *(
            [
                "Calibration entry attempts (last reported): "
                f"{calibration.get('entry_attempts', 'unknown')}",
                "Calibration reasons (last reported): "
                f"{', '.join(str(reason) for reason in calibration.get('reasons', [])) or 'none'}",
            ]
            if isinstance(calibration, dict)
            else []
        ),
        *(
            [
                "Calibration records are operational checks, "
                "not source events or qualification evidence."
            ]
            if practice
            else []
        ),
        f"Pending/unknown orders: {pending_count}",
        f"Positions (last valuation): {position_count if perf else 'unknown'}",
        "",
        "LAST RECORDED ALLOCATION RESULTS (NOT NECESSARILY TODAY'S P&L)",
        "All executions combined; not NEWS_STRATEGY performance. Separate session ledgers follow.",
        f"Valuation: {perf_at.astimezone(zone).isoformat() if perf_at else 'unavailable'}"
        f" ({'current' if perf_current else 'stale or missing; current results unknown'})",
        f"Broker-paper cumulative P&L: {metric('broker_paper_pnl')} USD",
        (
            "Practice cumulative P&L after modeled reserves (operational estimate only): "
            f"{metric('economic_paper_pnl')} USD"
            if practice
            else f"Economic-paper cumulative P&L: {metric('economic_paper_pnl')} USD"
        ),
        f"Reconciled round trips: {metric('closed_round_trips')}",
        *(
            [
                f"Calibration round trips (operational only): {metric('calibration_round_trips')}",
                "Research-qualifying practice sessions: 0; research-qualifying round trips: 0",
                "Practice P&L is not validated strategy economics.",
            ]
            if practice
            else []
        ),
        f"Fixed service costs: {metric('fixed_service_cost_usd')} USD",
        "Economic reserves are estimates; paper results do not establish executable live profits.",
        f"Entry ceiling: {config.get('max_entry_notional', 'unknown')} USD; "
        "live trading remains unavailable.",
        "",
        "BLOCKERS / WHY IT IS NOT TRADING",
        *(
            [f"- {value}" for value in blockers]
            or ["- No explicit blocker reported; this is not an authorization to trade."]
        ),
        *[f"- Abstention {reason}: {count}" for reason, count in reasons.most_common(5)],
        "",
        "SOURCE / CAPABILITY LIMITATIONS (NOT TRADING-PERMISSION FAILURES)",
        *(
            [f"- {value}" for value in [*limitations["source_limitations"], *capabilities]]
            or ["- None reported."]
        ),
        "",
        "NEXT STEPS",
        *[f"{index}. {step}" for index, step in enumerate(next_steps, start=1)],
        "",
        "Dashboard: https://tradeagent-runtime-dashboard.onrender.com",
        "This email does not change any strategy, risk limit, order permission, or subscription.",
    ]
    structured = session_report(database, cohort_id, observed_at=now, persist=True)
    return {
        **evidence_labels(purpose),
        **limitations,
        "calibration": calibration,
        "subject": f"[TradeAgent PAPER] Daily agent status - {local.date()}",
        "text": "\n".join(lines) + "\n\n" + render_session_report(structured),
        "session_report_reference": {
            "report_id": structured["report_id"],
            "snapshot_at": structured["snapshot_at"],
            "immutable_table": "events_v2",
            "url": f"/api/event-session-report?report_id={structured['report_id']}",
        },
        "session_report_id": structured["report_id"],
        "local_date": local.date().isoformat(),
        "timezone": timezone,
        "cohort_id": cohort_id,
        "snapshot_at": now.isoformat(),
    }
