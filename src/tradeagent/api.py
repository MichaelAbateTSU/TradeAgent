from __future__ import annotations

import os
from collections import Counter
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager

# ruff: noqa: E501
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from threading import Lock
from time import monotonic
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from sqlalchemy import func, inspect, select
from sqlalchemy.exc import SQLAlchemyError

from tradeagent import __version__
from tradeagent.broker import PaperBroker
from tradeagent.config import BrokerConfig
from tradeagent.domain import AccountSnapshot, PaperBrokerState
from tradeagent.event_reporting import (
    evidence_labels,
    performance_labels,
    reported_calibration,
    reporting_limitations,
    reporting_purpose,
)
from tradeagent.event_session_report import (
    EASTERN,
    render_session_report,
    report_delivery,
    session_report,
)
from tradeagent.event_store import event_cohorts, event_decisions
from tradeagent.experimental_policy import ExperimentalSettings
from tradeagent.ledger import SQLiteLedger
from tradeagent.persistence import (
    Database,
    MarketDataTotalsUnavailableError,
    ProductionRepository,
    controls,
    heartbeats,
    market_news,
    worker_locks,
)
from tradeagent.persistence import events as stored_events
from tradeagent.reporting_reads import (
    ReportBusyError,
    ReportingReadModelIncomplete,
    ReportPayloadTooLargeError,
    payload_from_projection,
    projected_payload,
    stream_rows,
)
from tradeagent.research import ExperimentRegistry

PERFORMANCE_FIELDS = (
    "purpose",
    "state",
    "broker_paper_pnl",
    "economic_paper_pnl",
    "broker_paper_equity",
    "economic_paper_equity",
    "closed_round_trips",
    "calibration_round_trips",
    "qualifying_closed_round_trips",
    "virtual_equity_anchor",
    "fixed_service_cost_usd",
)
EVENT_HEARTBEAT_FIELDS = (
    "state",
    "mode",
    "purpose",
    "execution_feed",
    "practice_start_date",
    "planned_session_date",
    "cohort_id",
    "code_sha",
    "config_hash",
    "market_phase",
    "next_open",
    "last_successful_source_at",
    "events_received",
    "market_errors",
    "tick_latency_seconds",
    "limitations",
    "blockers",
    "source_limitations",
    "capability_limitations",
    "calibration",
    "calibration_status",
    "session_budget",
    "broker_stream",
)
ROLE_HEARTBEAT_FIELDS = (
    "state",
    "healthy",
    "mode",
    "cohort_id",
    "code_sha",
    "last_successful_source_at",
    "events_received",
    "tick_latency_seconds",
    "received",
    "committed",
    "inserted",
    "duplicates",
    "queue_depth",
    "queue_capacity",
    "in_flight",
    "gaps",
    "dropped_events",
    "notice_overflow",
    "decision_errors",
    "derived",
    "persistence_error",
    "last_received_at",
    "last_received_event_at",
    "last_event_at",
    "last_committed_event_at",
    "last_market_commit_at",
    "last_commit_at",
    "last_committed_received_at",
    "receive_lag_seconds",
    "commit_lag_seconds",
    "exchange_to_commit_lag_seconds",
    "committed_event_age_seconds",
    "market_commit_age_seconds",
    "freshness_basis",
    "freshness_reason",
    "durable_batch",
    "recorder_heartbeat_at",
    "recorder_lease_age_seconds",
    "last_market_event_at",
    "event_age_seconds",
    "oldest_uncommitted_age_seconds",
    "batch_write_seconds",
    "dispatched",
)
ROLE_LOCKS = {
    "tradeagent-event-worker": "tradeagent-event-worker",
    "tradeagent-shadow-recorder": "tradeagent-shadow-recorder",
    "tradeagent-notifier": "tradeagent-notifier",
    "tradeagent-news-worker": None,
    "tradeagent-shadow-market-feed": None,
}


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _operational_status(database: Database, now: datetime | None = None) -> dict[str, Any]:
    """Fixed-size role/control projections and one committed batch, never history scans."""
    query_at = now or datetime.now(UTC)
    with database.begin() as connection:
        observations = {
            row["service_name"]: row
            for row in stream_rows(
                connection,
                select(
                    heartbeats.c.service_name,
                    heartbeats.c.instance_id,
                    heartbeats.c.observed_at,
                    *projected_payload(heartbeats.c.details, ROLE_HEARTBEAT_FIELDS),
                    heartbeats.c.details["source_capabilities"]["http_cache"].label(
                        "source_http_cache"
                    ),
                ).where(heartbeats.c.service_name.in_(ROLE_LOCKS)),
            )
        }
        leases = {
            row["lock_name"]: row
            for row in stream_rows(
                connection,
                select(worker_locks).where(
                    worker_locks.c.lock_name.in_([name for name in ROLE_LOCKS.values() if name]),
                ),
            )
        }
        cohort = observations.get("tradeagent-event-worker", {}).get("cohort_id")
        control_keys = ["kill_switch", *([f"{cohort}:pause"] if cohort else [])]
        current_controls = {
            row["control_key"]: {
                "value": row["control_value"],
                "updated_at": _aware(row["updated_at"]).isoformat(),
            }
            for row in stream_rows(
                connection,
                select(controls).where(
                    controls.c.control_key.in_(control_keys),
                ),
            )
        }
        batch_fields = (
            "instance_id",
            "received",
            "inserted",
            "duplicates",
            "first_event_at",
            "last_event_at",
            "first_received_at",
            "last_received_at",
            "processing_started_at",
        )
        batch = (
            connection.execute(
                select(
                    stored_events.c.event_id,
                    stored_events.c.occurred_at,
                    *projected_payload(stored_events.c.payload, batch_fields),
                )
                .where(
                    stored_events.c.event_type == "shadow_recorder_batch",
                    stored_events.c.occurred_at <= query_at,
                )
                .order_by(stored_events.c.occurred_at.desc(), stored_events.c.event_id)
                .limit(1)
            )
            .mappings()
            .one_or_none()
        )
    # A heartbeat may advance while the queries run. Assess against the completed
    # observation, not an earlier request-start clock that makes it look future-dated.
    now = now or datetime.now(UTC)
    roles: dict[str, Any] = {}
    for service, lock_name in ROLE_LOCKS.items():
        row = observations.get(service)
        observed_at = _aware(row["observed_at"]) if row else None
        age = (now - observed_at).total_seconds() if observed_at else None
        lease = leases.get(lock_name) if lock_name else None
        renewed = _aware(lease["acquired_at"]) if lease else None
        lease_age = (now - renewed).total_seconds() if renewed else None
        roles[service] = {
            "heartbeat_at": observed_at.isoformat() if observed_at else None,
            "age_seconds": age,
            "fresh": age is not None and 0 <= age <= 120,
            "instance_id": row["instance_id"] if row else None,
            "reported": {
                **payload_from_projection(row, ROLE_HEARTBEAT_FIELDS),
                "source_http_cache": row.get("source_http_cache"),
            }
            if row
            else {},
            "lease": {
                "lock_name": lock_name,
                "present": lease is not None,
                "owner_id": lease["owner_id"] if lease else None,
                "renewed_at": renewed.isoformat() if renewed else None,
                "age_seconds": lease_age,
                "owner_matches_heartbeat": (
                    row["instance_id"] == lease["owner_id"] if row and lease else None
                ),
                "recent_within_120_seconds": lease_age is not None and 0 <= lease_age <= 120,
                "basis": "observation only; actual takeover thresholds are role-configured",
            }
            if lock_name
            else None,
        }
    return {
        "as_of": now.isoformat(),
        "state_source": "production_database",
        "database": "reachable",
        "roles": roles,
        "controls": {
            "cohort_id": cohort,
            "kill_switch": current_controls.get("kill_switch"),
            "cohort_pause": current_controls.get(f"{cohort}:pause") if cohort else None,
            "missing_means": "not_recorded_not_inferred_inactive",
        },
        "latest_committed_recorder_batch": {
            "event_id": batch["event_id"],
            "at": _aware(batch["occurred_at"]).isoformat(),
            **payload_from_projection(dict(batch), batch_fields),
        }
        if batch
        else None,
        "progress_basis": (
            "Compare batch IDs and timestamps between samples for durable progress. "
            "Recorder counters are instance-local; event-worker events_received is last-tick only. "
            "Fresh heartbeats do not prove source coverage, market progress, or trading permission."
        ),
    }


def _event_overview(database: Database, cohort_id: str, now: datetime) -> dict[str, Any]:
    """Small read model: never invoke EventStore.report or a full EOD report here."""
    decision_fields = ("action", "state", "reasons", "symbol", "trade_classification", "entry_kind")
    with database.begin() as connection:
        manifest = (
            connection.scalar(
                select(event_cohorts.c.manifest).where(
                    event_cohorts.c.cohort_id == cohort_id,
                )
            )
            or {}
        )
        decision_scope = (
            event_decisions.c.cohort_id == cohort_id,
            event_decisions.c.decided_at <= now,
        )
        count = (
            connection.scalar(
                select(func.count()).select_from(event_decisions).where(*decision_scope)
            )
            or 0
        )
        reasons: Counter[str] = Counter()
        for row in stream_rows(
            connection,
            select(event_decisions.c.payload["reasons"].label("reasons")).where(*decision_scope),
        ):
            reasons.update(str(reason) for reason in (row["reasons"] or []))
        decisions = [
            {
                "decision_id": row["decision_id"],
                "evidence_id": row["evidence_id"],
                "decided_at": row["decided_at"],
                "payload": payload_from_projection(row, decision_fields),
                "immutable_reference": f"event_decisions:{row['decision_id']}",
            }
            for row in stream_rows(
                connection,
                select(
                    event_decisions.c.decision_id,
                    event_decisions.c.evidence_id,
                    event_decisions.c.decided_at,
                    *projected_payload(event_decisions.c.payload, decision_fields),
                )
                .where(*decision_scope)
                .order_by(event_decisions.c.decided_at.desc(), event_decisions.c.decision_id)
                .limit(20),
            )
        ]
        latest = (
            connection.execute(
                select(
                    stored_events.c.occurred_at,
                    *projected_payload(stored_events.c.payload, PERFORMANCE_FIELDS),
                )
                .where(
                    stored_events.c.event_type == "event_performance",
                    stored_events.c.trace_id == cohort_id,
                    stored_events.c.occurred_at <= now,
                )
                .order_by(stored_events.c.occurred_at.desc(), stored_events.c.recorded_at.desc())
                .limit(1)
            )
            .mappings()
            .one_or_none()
        )
        saved = (
            connection.execute(
                select(
                    stored_events.c.event_id.label("report_id"),
                    stored_events.c.occurred_at.label("snapshot_at"),
                    stored_events.c.payload["planned_session_date"]
                    .as_string()
                    .label("planned_session_date"),
                )
                .where(
                    stored_events.c.event_type == "event_session_report",
                    stored_events.c.trace_id == cohort_id,
                    stored_events.c.occurred_at <= now,
                )
                .order_by(stored_events.c.occurred_at.desc())
                .limit(1)
            )
            .mappings()
            .one_or_none()
        )
    return {
        "manifest": manifest,
        "decisions": decisions,
        "decision_count": int(count),
        "leading_no_trade_reasons": dict(reasons),
        "decisions_display": {
            "shown": len(decisions),
            "total": int(count),
            "truncated": count > len(decisions),
            "projection": "decision summary; full evidence retained in event_decisions by decision_id",
        },
        "performance": payload_from_projection(dict(latest), PERFORMANCE_FIELDS)
        if latest
        else None,
        "performance_as_of": latest["occurred_at"].isoformat() if latest else None,
        "latest_persisted_report": dict(saved) if saved else None,
    }


DASHBOARD = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>TradeAgent Paper Console</title>
  <style>
    :root { color-scheme: dark; font-family: Inter, system-ui, sans-serif; }
    body { max-width: 1100px; margin: 0 auto; padding: 2rem; background: #09111f; color: #dce7f7; }
    header { display: flex; align-items: center; justify-content: space-between; gap: 1rem; }
    h1 { margin-bottom: .25rem; }
    .badge { background: #123d2b; color: #71e2a7; padding: .4rem .7rem; border-radius: 999px; }
    .warning { background: #392b11; border: 1px solid #765f24; padding: 1rem; border-radius: .6rem; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(210px, 1fr)); gap: 1rem; margin: 1rem 0; }
    .card { background: #101c2f; border: 1px solid #233653; border-radius: .7rem; padding: 1rem; }
    .value { font-size: 1.8rem; font-weight: 700; margin-top: .4rem; }
    table { width: 100%; border-collapse: collapse; font-size: .9rem; }
    th, td { padding: .65rem; border-bottom: 1px solid #233653; text-align: left; }
    code { color: #9bc4ff; }
  </style>
</head>
<body>
  <header><div><h1>TradeAgent</h1><div>Read-only paper-trading console</div></div><span class="badge">PAPER ONLY</span></header>
  <p class="warning">No live broker is connected. Qualification means a research gate passed, not that profit is guaranteed.</p>
  <section class="grid">
    <div class="card"><div>NAV</div><div id="nav" class="value">-</div></div>
    <div class="card"><div>Gross exposure</div><div id="exposure" class="value">-</div></div>
    <div class="card"><div>Kill switch</div><div id="kill-switch" class="value">-</div></div>
    <div class="card"><div>Audit events</div><div id="events" class="value">-</div></div>
    <div class="card"><div>Experiments</div><div id="experiments" class="value">-</div></div>
    <div class="card"><div>Qualified trials</div><div id="qualified" class="value">-</div></div>
    <div class="card"><div>Hosted bars</div><div id="hosted-bars" class="value">-</div></div>
    <div class="card"><div>Hosted quotes</div><div id="hosted-quotes" class="value">-</div></div>
    <div class="card"><div>Shadow NAV</div><div id="shadow-nav" class="value">-</div></div>
    <div class="card"><div>Recent news</div><div id="news-count" class="value">-</div></div>
  </section>
  <p id="statistics-status">Hosted counts are sampled once per minute; readiness is checked separately.</p>
  <section class="card"><h2>Recent experiments</h2>
    <table><thead><tr><th>ID</th><th>Strategy</th><th>Seed</th><th>Qualified</th><th>Git SHA</th></tr></thead>
    <tbody id="experiment-rows"></tbody></table>
  </section>
  <section class="card">
    <h2>Event paper cohorts</h2>
    <p id="event-state">Connecting to event worker...</p>
    <p>Operational permission is not statistical qualification. No live broker is connected.</p>
    <h3>Service observations</h3>
    <pre id="service-observations" style="white-space:pre-wrap;overflow-wrap:anywhere" role="status">Waiting for production service observations...</pre>
    <p>Heartbeat freshness is not proof of market-data coverage or entry permission. <a href="/ready" target="_blank" rel="noopener">Open current dependency and recorder progress details</a>.</p>
    <p id="event-evidence"></p>
    <pre id="event-ledgers" style="white-space:pre-wrap"></pre>
    <h3>Trading blockers and abstentions</h3><pre id="event-reasons" style="white-space:pre-wrap"></pre>
    <h3>Source and capability limitations</h3><pre id="event-limitations" style="white-space:pre-wrap"></pre>
    <h3>Calibration (operational only)</h3><pre id="event-calibration" style="white-space:pre-wrap"></pre>
    <h3>Planned session report</h3>
    <p id="event-session-report" role="status">Loading report reference...</p>
    <a href="/api/event-session-report" target="_blank" rel="noopener">Open full session evidence report on demand</a>
    <h3>Recent evidence and decisions</h3>
    <div id="event-decisions"></div>
  </section>
  <script>
    function escapeHtml(value) {
      const element = document.createElement('span');
      element.textContent = String(value);
      return element.innerHTML;
    }
    async function refreshSection(name, url, targets, render) {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 20000);
      try {
        const response = await fetch(url, {signal: controller.signal});
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        render(await response.json());
      } catch (error) {
        targets.forEach(target => {
          const element = document.querySelector(target);
          element.textContent = `${name} unavailable (${error.name === 'AbortError' ? 'request timed out' : error.message}). Retrying automatically; previous values are not current.`;
        });
      } finally {
        clearTimeout(timeout);
      }
    }
    function renderStatus(status) {
      document.querySelector('#events').textContent = status.event_count;
      document.querySelector('#nav').textContent =
        status.account ? `$${Number(status.account.equity).toLocaleString()}`
          : status.state_source === 'production_database' ? 'Broker snapshot unavailable' : 'No run';
      document.querySelector('#exposure').textContent =
        status.account ? `${(Number(status.account.gross_exposure_ratio) * 100).toFixed(2)}%` : '-';
      document.querySelector('#kill-switch').textContent = status.kill_switch;
    }
    function renderExperiments(experiments) {
      document.querySelector('#experiments').textContent = experiments.total;
      document.querySelector('#qualified').textContent = experiments.qualified_total;
      document.querySelector('#experiment-rows').innerHTML = experiments.items.map(item =>
        `<tr><td>${escapeHtml(item.experiment_id)}</td><td>${escapeHtml(item.strategy_id)}</td>` +
        `<td>${escapeHtml(item.random_seed)}</td><td>${item.qualified ? 'yes' : 'no'}</td>` +
        `<td><code>${escapeHtml(item.git_sha.slice(0, 8))}</code></td></tr>`
      ).join('');
    }
    function renderRuntime(runtime) {
      document.querySelector('#hosted-bars').textContent = runtime.market_bars ?? '-';
      document.querySelector('#hosted-quotes').textContent = runtime.market_quotes ?? '-';
      document.querySelector('#statistics-status').textContent = runtime.statistics_as_of
        ? `Hosted counts observed ${new Date(runtime.statistics_as_of).toLocaleTimeString()}; cached for up to ${runtime.statistics_cache_seconds}s. Readiness is checked separately.`
        : 'Hosted counts unavailable; readiness is checked separately.';
      document.querySelector('#shadow-nav').textContent =
        runtime.shadow_nav ? `$${Number(runtime.shadow_nav).toLocaleString()}` : '-';
    }
    function renderProduct(product) {
      const operational = product.operational_status;
      document.querySelector('#service-observations').textContent = operational
        ? `Dashboard: responding; PostgreSQL: ${operational.database}; observed ${operational.as_of}\\n` +
          Object.entries(operational.roles || {}).map(([name, role]) => {
            const observed = role.reported || {};
            const state = role.fresh ? (observed.state || 'heartbeat only') : 'stale or missing';
            const age = role.age_seconds == null ? 'unknown' : `${role.age_seconds.toFixed(1)}s`;
            const owner = role.lease ? `; lease owner match: ${role.lease.owner_matches_heartbeat ?? 'unknown'}` : '';
            const progress = observed.last_committed_event_at ? `; committed exchange time: ${observed.last_committed_event_at}; queue: ${observed.queue_depth ?? 'unknown'}; dropped: ${observed.dropped_events ?? 'unknown'}; derived frames: ${observed.derived?.state ?? 'unknown'}` : '';
            return `${name}: ${state}; heartbeat age: ${age}${owner}${progress}`;
          }).join('\\n')
        : 'Production service observations unavailable; not evidence of healthy workers.';
      document.querySelector('#event-state').textContent = JSON.stringify({
        version: product.version, mode: product.mode, purpose: product.purpose, state: product.state,
        execution_feed: product.execution_feed, practice_start_date: product.practice_start_date,
        market_phase: product.market_phase, next_session: product.next_open,
        performance: product.performance_label, code: product.code_sha,
        overview_as_of: product.as_of, heartbeat_at: product.heartbeat_at,
        cache_ttl_seconds: product.cache_ttl_seconds
      });
      document.querySelector('#event-evidence').textContent = product.purpose === 'iex-practice'
        ? 'IEX paper practice only. Sessions and round trips, including calibration, do not count toward the 60-session/60-round-trip research qualification floors. Dollar results are broker-paper facts and modeled operational estimates, not validated strategy economics.'
        : 'Research evidence remains unproven; qualification gates and minimum evidence floors still apply.';
      document.querySelector('#event-ledgers').textContent =
        JSON.stringify(product.ledgers || {performance:'No measured forward outcomes'}, null, 2);
      document.querySelector('#event-reasons').textContent =
        JSON.stringify({blockers: product.blockers || [], abstentions: product.leading_no_trade_reasons || {}}, null, 2);
      document.querySelector('#event-limitations').textContent =
        JSON.stringify({source: product.source_limitations || [], capability: product.capability_limitations || []}, null, 2);
      document.querySelector('#event-calibration').textContent =
        JSON.stringify(product.calibration || product.calibration_status || 'Not reported / not applicable', null, 2);
      document.querySelector('#event-session-report').textContent =
        product.latest_persisted_report
          ? `Latest persisted report: ${product.latest_persisted_report.snapshot_at}. Full immutable evidence is available through the on-demand report link.`
          : 'No persisted report yet. The overview is not a full report or proof of no activity. Open the full report on demand.';
      document.querySelector('#event-decisions').replaceChildren(...(product.decisions || []).map(row => {
        const detail = document.createElement('details');
        const title = document.createElement('summary');
        title.textContent = `${row.decided_at} ${row.payload.action || row.payload.state || 'decision'} ${row.evidence_id.slice(0,12)}`;
        const text = document.createElement('pre'); text.style.whiteSpace = 'pre-wrap';
        text.textContent = JSON.stringify(row.payload, null, 2);
        detail.append(title, text); return detail;
      }));
    }
    let refreshing = false;
    async function refresh() {
      if (refreshing) return;
      refreshing = true;
      try {
        await Promise.allSettled([
          refreshSection('Production controls', '/api/status', ['#kill-switch', '#events', '#nav', '#exposure'], renderStatus),
          refreshSection('Experiments', '/api/experiments?limit=10', ['#experiments', '#qualified'], renderExperiments),
          refreshSection('Recorder', '/api/runtime', ['#hosted-bars', '#hosted-quotes', '#shadow-nav'], renderRuntime),
          refreshSection('News', '/api/news?limit=20', ['#news-count'], news => {
            document.querySelector('#news-count').textContent = news.items.length;
          }),
          refreshSection('Event overview', '/api/event-product', ['#event-state', '#event-session-report', '#service-observations'], renderProduct)
        ]);
      } finally {
        refreshing = false;
        setTimeout(refresh, 10000);
      }
    }
    refresh();
  </script>
</body>
</html>
"""


def _latest_account(ledger: SQLiteLedger) -> AccountSnapshot | None:
    checkpoint = ledger.latest_event("broker_checkpoint")
    if checkpoint is None:
        return None
    state = PaperBrokerState.model_validate(checkpoint["payload"])
    broker = PaperBroker.from_state(BrokerConfig(), state)
    return broker.account(datetime.fromisoformat(str(checkpoint["occurred_at"])))


def create_app(
    *,
    ledger_path: Path = Path("data/tradeagent.db"),
    experiments_path: Path = Path("data/experiments.db"),
    production_database_url: str | None = None,
    overview_cache_seconds: float = 15.0,
    statistics_cache_seconds: float = 60.0,
) -> FastAPI:
    if not 0 <= statistics_cache_seconds <= 300:
        raise ValueError("statistics cache duration must be between zero and 300 seconds")
    shared_database = (
        Database(production_database_url, pool_size=2) if production_database_url else None
    )

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        try:
            yield
        finally:
            if shared_database is not None:
                shared_database.dispose()

    @contextmanager
    def production_database() -> Iterator[Database]:
        if shared_database is None:
            raise RuntimeError("production database is not configured")
        # One bounded pool per process, not a fresh pool for every concurrent section.
        yield shared_database

    app = FastAPI(
        title="TradeAgent Paper Console",
        version=__version__,
        description="Read-only process and paper-trading evidence; no trading authority.",
        lifespan=lifespan,
    )
    app.state.production_database = shared_database
    overview_lock = Lock()
    report_lock = Lock()
    cached_overview: dict[str, object] | None = None
    overview_expires = 0.0
    overview_failed = False
    statistics_lock = Lock()
    cached_statistics: dict[str, Any] | None = None
    statistics_expires = 0.0
    statistics_failed = False

    def reporting_statistics(database: Database) -> dict[str, Any]:
        nonlocal cached_statistics, statistics_expires, statistics_failed
        with statistics_lock:
            if monotonic() < statistics_expires:
                if statistics_failed:
                    raise HTTPException(status_code=503, detail="Production statistics unavailable")
                assert cached_statistics is not None
                return cached_statistics
            try:
                bars, quotes, trades = ProductionRepository(database).market_data_counts()
                with database.begin() as connection:
                    counts = {
                        str(kind): int(count)
                        for kind, count in connection.execute(
                            select(stored_events.c.event_type, func.count()).group_by(
                                stored_events.c.event_type
                            )
                        )
                    }
                cached_statistics = {
                    "market_bars": bars,
                    "market_quotes": quotes,
                    "market_trades": trades,
                    "event_count": sum(counts.values()),
                    "event_counts": counts,
                    "statistics_as_of": datetime.now(UTC).isoformat(),
                    "statistics_cache_seconds": statistics_cache_seconds,
                    "statistics_basis": "exact counts at the recorded observation, not live health",
                }
            except (SQLAlchemyError, MarketDataTotalsUnavailableError) as error:
                cached_statistics = None
                statistics_failed = True
                statistics_expires = monotonic() + 5.0
                raise HTTPException(
                    status_code=503, detail="Production statistics unavailable"
                ) from error
            statistics_failed = False
            statistics_expires = monotonic() + statistics_cache_seconds
            return cached_statistics

    @app.get("/", response_class=HTMLResponse, include_in_schema=False)
    def dashboard() -> str:
        return DASHBOARD

    @app.get("/health")
    def health() -> dict[str, object]:
        return {
            "status": "ok",
            "mode": "paper",
            "live_trading_available": False,
            "check": "process_liveness_only",
            "dependencies": "/ready",
        }

    @app.get("/ready")
    def ready() -> JSONResponse:
        if production_database_url is None:
            return JSONResponse(
                {
                    "ready": True,
                    "database": "not_configured_local_mode",
                    "live_trading_available": False,
                }
            )
        try:
            with production_database() as database:
                with database.begin() as connection:
                    connection.execute(select(1)).scalar_one()
                operational = _operational_status(database)
                event_worker = operational["roles"]["tradeagent-event-worker"]
            return JSONResponse(
                {
                    "ready": True,
                    "database": "reachable",
                    "check": "dashboard_read_dependency",
                    "event_worker_fresh": event_worker["fresh"],
                    "event_worker_age_seconds": event_worker["age_seconds"],
                    "operational_status": operational,
                    "live_trading_available": False,
                }
            )
        except (SQLAlchemyError, ReportingReadModelIncomplete):
            return JSONResponse(
                {
                    "ready": False,
                    "database": "unavailable",
                    "error": "production_database_unavailable",
                },
                status_code=503,
            )

    @app.get("/api/event-product")
    def event_product() -> dict[str, object]:
        nonlocal cached_overview, overview_expires, overview_failed
        with overview_lock:
            if cached_overview is None or monotonic() >= overview_expires:
                try:
                    cached_overview = build_event_product()
                    overview_failed = False
                except SQLAlchemyError:
                    overview_failed = True
                    cached_overview = {
                        "state": "reporting_unavailable",
                        "error": "production_read_failed",
                        "as_of": datetime.now(UTC).isoformat(),
                        "message": "Dashboard reporting failed; not evidence of no activity. Retry after the cache interval.",
                    }
                overview_expires = monotonic() + (
                    5.0 if overview_failed else overview_cache_seconds
                )
            if overview_failed:
                raise HTTPException(
                    status_code=503, detail=cached_overview, headers={"Retry-After": "5"}
                )
            return {
                **cached_overview,
                "served_at": datetime.now(UTC).isoformat(),
                "cache_ttl_seconds": overview_cache_seconds,
                "overview_only": True,
                "full_report_url": "/api/event-session-report",
            }

    def build_event_product() -> dict[str, object]:
        now = datetime.now(UTC)
        settings = ExperimentalSettings()
        base: dict[str, object] = {
            **evidence_labels(reporting_purpose(settings.model_dump())),
            "version": __version__,
            "code_sha": os.getenv("RENDER_GIT_COMMIT", "local"),
            "mode": settings.mode,
            "state": "not_running",
            "qualified": False,
            "live_execution_available": False,
            "blockers": ["EVENT_WORKER_NOT_STARTED"],
            "as_of": now.isoformat(),
        }
        if production_database_url is None:
            return base
        with production_database() as database:
            with database.begin() as connection:
                heartbeat_row = (
                    connection.execute(
                        select(
                            heartbeats.c.observed_at,
                            *projected_payload(heartbeats.c.details, EVENT_HEARTBEAT_FIELDS),
                            heartbeats.c.details["source_capabilities"]["last_errors"].label(
                                "source_errors"
                            ),
                            heartbeats.c.details["source_capabilities"]["http_cache"].label(
                                "source_http_cache"
                            ),
                            heartbeats.c.details["premarket_brief"]["funnel"].label(
                                "premarket_funnel"
                            ),
                            heartbeats.c.details["premarket_brief"]["coverage"].label(
                                "premarket_coverage"
                            ),
                            heartbeats.c.details["premarket_brief"]["preparation_status"].label(
                                "preparation_status"
                            ),
                        ).where(heartbeats.c.service_name == "tradeagent-event-worker")
                    )
                    .mappings()
                    .one_or_none()
                )
            heartbeat_at = _aware(heartbeat_row["observed_at"]) if heartbeat_row else None
            base["operational_status"] = _operational_status(database)
            if not inspect(database.engine).has_table("event_cohorts"):
                return base
            details = (
                payload_from_projection(dict(heartbeat_row), EVENT_HEARTBEAT_FIELDS)
                if heartbeat_row
                else {}
            )
            if heartbeat_row:
                details["source_capabilities"] = {
                    "last_errors": heartbeat_row["source_errors"] or [],
                    "http_cache": heartbeat_row["source_http_cache"],
                    "projection": "errors and cache metrics only; not full source capability evidence",
                }
                details["premarket_brief"] = {
                    "funnel": heartbeat_row["premarket_funnel"],
                    "coverage": heartbeat_row["premarket_coverage"],
                    "preparation_status": heartbeat_row["preparation_status"],
                    "projection": "overview only; full brief remains in immutable audit evidence",
                }
            with database.begin() as connection:
                cohort_id = str(
                    details.get("cohort_id")
                    or connection.scalar(
                        select(event_cohorts.c.cohort_id)
                        .order_by(event_cohorts.c.created_at.desc())
                        .limit(1)
                    )
                    or settings.cohort_id
                )
            overview = _event_overview(database, cohort_id, now)
            purpose = reporting_purpose(
                details,
                overview.pop("manifest"),
                settings.model_dump() if settings.cohort_id == cohort_id else None,
            )
            latest = overview.pop("performance")
            age = (now - heartbeat_at).total_seconds() if heartbeat_at else None
            fresh = age is not None and 0 <= age <= 120
            return {
                **base,
                **details,
                **overview,
                **evidence_labels(purpose),
                "cohort_id": cohort_id,
                "ledgers": performance_labels(latest, purpose) if latest is not None else None,
                "usable_forward_trading_sessions": 0 if purpose == "iex-practice" else None,
                "calibration": reported_calibration(details),
                "calibration_status": reported_calibration(details),
                "session_report": {
                    "overview_only": True,
                    "cohort_id": cohort_id,
                    "health": {"worker": {"fresh": fresh}},
                    "latest_persisted_report": overview["latest_persisted_report"],
                    "full_report_url": "/api/event-session-report",
                },
                "heartbeat_at": heartbeat_at.isoformat() if heartbeat_at else None,
                "state": details.get("state")
                if fresh
                else "worker_stale"
                if heartbeat_at
                else "not_running",
                **reporting_limitations(details, purpose),
                "code_sha": details.get("code_sha", base["code_sha"]),
            }

    @app.get("/api/event-session-report")
    def event_session_report(
        cohort_id: str | None = Query(default=None, max_length=64),
        session_date: date | None = None,
        report_id: str | None = Query(default=None, max_length=36),
    ) -> dict[str, object]:
        if not report_lock.acquire(blocking=False):
            raise HTTPException(
                status_code=429,
                detail="A full report is already being read; retry shortly.",
                headers={"Retry-After": "10"},
            )
        try:
            return read_event_session_report(cohort_id, session_date, report_id)
        except ReportBusyError as error:
            raise HTTPException(
                status_code=429, detail=str(error), headers={"Retry-After": "5"}
            ) from error
        except (ReportingReadModelIncomplete, ReportPayloadTooLargeError) as error:
            raise HTTPException(
                status_code=503,
                detail=str(error),
                headers={"Retry-After": "60"},
            ) from error
        finally:
            report_lock.release()

    def read_event_session_report(
        cohort_id: str | None,
        session_date: date | None,
        report_id: str | None,
    ) -> dict[str, object]:
        if production_database_url is None:
            return {"state": "database_not_configured", "session_report": None}
        with production_database() as database:
            if report_id is not None:
                with database.begin() as connection:
                    saved = connection.scalar(
                        select(stored_events.c.payload).where(
                            stored_events.c.event_type == "event_session_report",
                            stored_events.c.event_id == report_id,
                        )
                    )
                if saved is None:
                    raise HTTPException(status_code=404, detail="Session report not found")
                saved_day = datetime.fromisoformat(saved["snapshot_at"]).astimezone(EASTERN).date()
                return {
                    "session_report": saved,
                    "text": render_session_report(saved),
                    "daily_email_delivery": report_delivery(
                        database, saved["cohort_id"], saved_day
                    ),
                }
            selected_cohort = cohort_id
            if selected_cohort is None:
                heartbeat = ProductionRepository(database).latest_heartbeat(
                    "tradeagent-event-worker"
                )
                selected_cohort = heartbeat[2].get("cohort_id") if heartbeat else None
            report = session_report(
                database, selected_cohort, session_date, observed_at=datetime.now(UTC)
            )
            return {
                "session_report": report,
                "text": render_session_report(report),
                "daily_email_delivery": report_delivery(
                    database, report["cohort_id"], datetime.now(EASTERN).date()
                ),
            }

    @app.get("/api/status")
    def status() -> dict[str, object]:
        if production_database_url is not None:
            try:
                with production_database() as database:
                    repository = ProductionRepository(database)
                    statistics = reporting_statistics(database)
                    with database.begin() as connection:
                        cohort = connection.scalar(
                            select(heartbeats.c.details["cohort_id"]).where(
                                heartbeats.c.service_name == "tradeagent-event-worker"
                            )
                        )
                    kill = repository.get_control("kill_switch") or "not_recorded"
                    pause = repository.get_control(f"{cohort}:pause") if cohort else None
                    return {
                        "mode": "paper",
                        "trading_enabled": False,
                        "live_trading_available": False,
                        "state_source": "production_database",
                        "controls_scope": "global kill switch and current heartbeat cohort pause",
                        "as_of": datetime.now(UTC).isoformat(),
                        "kill_switch": kill,
                        "cohort_pause": pause,
                        "cohort_id": cohort,
                        "event_count": statistics["event_count"],
                        "event_counts": statistics["event_counts"],
                        "statistics_as_of": statistics["statistics_as_of"],
                        "statistics_cache_seconds": statistics["statistics_cache_seconds"],
                        "statistics_basis": statistics["statistics_basis"],
                        "account": None,
                        "account_status": "not_a_broker_account_snapshot",
                    }
            except SQLAlchemyError as error:
                raise HTTPException(
                    status_code=503, detail="Production controls unavailable; no local fallback"
                ) from error
        with SQLiteLedger(ledger_path) as ledger:
            account = _latest_account(ledger)
            return {
                "mode": "paper",
                "trading_enabled": False,
                "live_trading_available": False,
                "kill_switch": ledger.get_control("kill_switch", default="inactive"),
                "event_count": ledger.event_count(),
                "event_counts": ledger.event_counts(),
                "account": (
                    {
                        **account.model_dump(mode="json"),
                        "gross_exposure_ratio": str(account.gross_exposure / account.equity),
                    }
                    if account is not None
                    else None
                ),
            }

    @app.get("/api/events")
    def events(limit: int = Query(default=50, ge=1, le=500)) -> dict[str, object]:
        with SQLiteLedger(ledger_path) as ledger:
            return {"items": list(ledger.events(limit=limit))}

    @app.get("/api/experiments")
    def experiments(limit: int = Query(default=20, ge=1, le=100)) -> dict[str, object]:
        with ExperimentRegistry(experiments_path) as registry:
            return {
                "total": registry.count(),
                "qualified_total": registry.qualified_count(),
                "items": registry.recent(limit=limit),
            }

    @app.get("/api/runtime")
    def runtime() -> dict[str, object]:
        if production_database_url is None:
            return {
                "connected": False,
                "market_bars": None,
                "market_quotes": None,
                "market_trades": None,
                "shadow_nav": None,
                "worker_heartbeat": None,
                "market_feed_status": None,
                "notifier_heartbeat": None,
                "news_heartbeat": None,
            }
        with production_database() as database:
            repository = ProductionRepository(database)
            statistics = reporting_statistics(database)
            outcome = repository.latest_event_payload("shadow_outcome")
            operational = _operational_status(database)
            roles = operational["roles"]
            worker = roles["tradeagent-shadow-recorder"]
            notifier = roles["tradeagent-notifier"]
            news = roles["tradeagent-news-worker"]
            market_feed = roles["tradeagent-shadow-market-feed"]
            return {
                "connected": True,
                "market_bars": statistics["market_bars"],
                "market_quotes": statistics["market_quotes"],
                "market_trades": statistics["market_trades"],
                "statistics_as_of": statistics["statistics_as_of"],
                "statistics_cache_seconds": statistics["statistics_cache_seconds"],
                "statistics_basis": statistics["statistics_basis"],
                "shadow_nav": outcome.get("shadow_nav") if outcome else None,
                "worker_heartbeat": worker["heartbeat_at"],
                "worker_status": worker,
                "market_feed_status": market_feed,
                "notifier_heartbeat": notifier["heartbeat_at"],
                "news_heartbeat": news["heartbeat_at"],
            }

    @app.get("/api/news")
    def news(limit: int = Query(default=20, ge=1, le=100)) -> dict[str, object]:
        if production_database_url is None:
            return {"items": [], "feed_heartbeat": None}
        with production_database() as database:
            repository = ProductionRepository(database)
            heartbeat = repository.latest_heartbeat("tradeagent-news-worker")
            with database.begin() as connection:
                items = list(
                    connection.execute(
                        select(
                            market_news.c.headline,
                            market_news.c.source,
                            market_news.c.source_url,
                            market_news.c.symbols,
                            market_news.c.category,
                            market_news.c.published_at,
                            market_news.c.received_at,
                        )
                        .where(
                            market_news.c.received_at >= datetime.now(UTC) - timedelta(hours=24),
                            market_news.c.received_at <= datetime.now(UTC),
                        )
                        .order_by(market_news.c.received_at.desc())
                        .limit(limit)
                    ).mappings()
                )
            return {
                "feed_heartbeat": heartbeat[1].isoformat() if heartbeat else None,
                "items": [dict(item) for item in items],
            }

    @app.get("/metrics", response_class=PlainTextResponse)
    def metrics() -> str:
        with SQLiteLedger(ledger_path) as ledger:
            counts = ledger.event_counts()
            lines = [
                "# HELP tradeagent_events_total Audit events recorded by type.",
                "# TYPE tradeagent_events_total counter",
                *[
                    f'tradeagent_events_total{{event_type="{event_type}"}} {count}'
                    for event_type, count in sorted(counts.items())
                ],
            ]
            account = _latest_account(ledger)
            if account is not None:
                lines.extend(
                    [
                        "# HELP tradeagent_nav Paper account net asset value.",
                        "# TYPE tradeagent_nav gauge",
                        f"tradeagent_nav {account.equity}",
                        "# HELP tradeagent_gross_exposure_dollars Paper gross exposure.",
                        "# TYPE tradeagent_gross_exposure_dollars gauge",
                        f"tradeagent_gross_exposure_dollars {account.gross_exposure}",
                    ]
                )
        with ExperimentRegistry(experiments_path) as registry:
            lines.extend(
                [
                    "# HELP tradeagent_experiments_total Research experiments recorded.",
                    "# TYPE tradeagent_experiments_total counter",
                    f"tradeagent_experiments_total {registry.count()}",
                ]
            )
        return "\n".join(lines) + "\n"

    return app
