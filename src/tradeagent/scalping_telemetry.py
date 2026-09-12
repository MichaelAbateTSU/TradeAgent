from __future__ import annotations

import logging
import math
from collections import Counter, OrderedDict, deque
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from threading import RLock
from typing import Any

from sqlalchemy import and_, or_, select

from tradeagent.persistence import Database, events, orders
from tradeagent.scalping_config import ScalpQuote, ScalpSignal
from tradeagent.scalping_market import NS, MarketEvent, datetime_ns
from tradeagent.scalping_store import (
    ScalpStore,
    json_value,
    scalping_activities,
    scalping_cycles,
    scalping_order_links,
    utc,
)

LOGGER = logging.getLogger(__name__)
STAGES = {
    "feed_processing": ("t0_received_ns", "t1_decoded_ns"),
    "state_update": ("t1_decoded_ns", "t2_state_updated_ns"),
    "feature": ("t2_state_updated_ns", "t3_features_ns"),
    "feature_compute": ("feature_started_ns", "t3_features_ns"),
    "model": ("t3_features_ns", "t4_model_ns"),
    "risk": ("t4_model_ns", "t5_risk_approved_ns"),
    "decision_send": ("t4_model_ns", "t6_submitted_ns"),
    "send_ack": ("t6_submitted_ns", "t7_acknowledged_ns"),
    "ack_fill": ("t7_acknowledged_ns", "t8_filled_ns"),
    "decision_fill": ("t4_model_ns", "t8_filled_ns"),
    "total_end_to_end": ("t0_received_ns", "t8_filled_ns"),
}


def percentiles(values: list[float]) -> dict[str, float | int | None]:
    ordered = sorted(values)
    return {
        "samples": len(ordered),
        **{
            name: ordered[max(0, math.ceil(len(ordered) * quantile) - 1)] if ordered else None
            for name, quantile in (("p50", 0.5), ("p95", 0.95), ("p99", 0.99))
        },
    }


class LatencyWindow:
    def __init__(self, capacity: int = 2000) -> None:
        self._lock = RLock()
        self.samples: dict[str, deque[float]] = {name: deque(maxlen=capacity) for name in STAGES}
        self.missing: Counter[str] = Counter()
        self.invalid: Counter[str] = Counter()

    def observe(self, timing: Mapping[str, Any], names: tuple[str, ...]) -> None:
        with self._lock:
            self._observe(timing, names)

    def _observe(self, timing: Mapping[str, Any], names: tuple[str, ...]) -> None:
        for name in names:
            first, last = (timing.get(key) for key in STAGES[name])
            if first is None or last is None:
                self.missing[name] += 1
            elif (
                isinstance(first, bool)
                or isinstance(last, bool)
                or not isinstance(first, int)
                or not isinstance(last, int)
                or last < first
            ):
                self.invalid[name] += 1
                LOGGER.warning(
                    "Unusable %s latency sample; clocks/broker event ordering differ", name
                )
            else:
                self.samples[name].append((last - first) / 1_000_000)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return self._snapshot()

    def _snapshot(self) -> dict[str, Any]:
        return {
            "units": "milliseconds",
            "basis": "measured stage timestamps; missing or reversed clocks are not zero latency",
            "window": "latest 2000 samples per stage in this process",
            "stages": {
                name: {
                    **percentiles(list(values)),
                    "missing_samples": self.missing[name],
                    "invalid_or_fill_before_ack_samples": self.invalid[name],
                }
                for name, values in self.samples.items()
            },
        }


class ScalpTelemetry:
    """Independent diagnostic writes never alter an order, position, or execution lease."""

    def __init__(self, database: Database, *, account_digest: str, started_at: datetime) -> None:
        self.database = database
        self.store = ScalpStore(database)
        self.account_digest = account_digest
        self.started_at = started_at
        self._lock = RLock()
        self._quotes: deque[dict[str, Any]] = deque()
        self._stages: OrderedDict[str, dict[str, int | None]] = OrderedDict()
        self._retention_ns = 900 * NS
        self._capacity = 50_000
        self._evictions = 0
        self._cursor_at = datetime(1970, 1, 1, tzinfo=UTC)
        self._cursor_id = ""
        self._neutral: Counter[str] = Counter()
        self._neutral_since = started_at
        self._diagnostic_reports = 0
        self.latencies = LatencyWindow()

    def on_market(
        self, event: MarketEvent, quote: ScalpQuote | None, *, state_updated_at: datetime
    ) -> None:
        stages = {
            "t0_received_ns": event.received_at_ns,
            "t1_decoded_ns": event.decoded_at_ns,
            "t2_state_updated_ns": datetime_ns(state_updated_at),
        }
        with self._lock:
            self._stages[event.event_id] = stages
            if len(self._stages) > 4096:
                self._stages.popitem(last=False)
            if (
                quote is None
                or event.event_type not in {"book", "quote"}
                or quote.exchange_time_ns != event.exchange_at_ns
            ):
                return
            self._quotes.append(
                {
                    "event_id": event.event_id,
                    "symbol": quote.symbol,
                    "bid": str(quote.bid),
                    "ask": str(quote.ask),
                    "bid_size": str(quote.bid_size),
                    "ask_size": str(quote.ask_size),
                    "exchange_at_ns": quote.exchange_time_ns,
                    "received_at_ns": event.received_at_ns,
                    "source": "native_quote" if event.event_type == "quote" else "reconstructed_l2",
                }
            )
            cutoff = event.received_at_ns - self._retention_ns
            while self._quotes and (
                len(self._quotes) > self._capacity or self._quotes[0]["received_at_ns"] < cutoff
            ):
                self._quotes.popleft()
                self._evictions += 1

    def decision_timing(
        self,
        source_id: str,
        *,
        features_started_at: datetime,
        features_at: datetime,
        model_at: datetime,
    ) -> dict[str, int | None]:
        with self._lock:
            timing = {
                **self._stages.get(
                    source_id,
                    {"t0_received_ns": None, "t1_decoded_ns": None, "t2_state_updated_ns": None},
                ),
                "t3_features_ns": datetime_ns(features_at),
                "feature_started_ns": datetime_ns(features_started_at),
                "t4_model_ns": datetime_ns(model_at),
            }
            self.latencies.observe(
                timing, ("feed_processing", "state_update", "feature", "feature_compute", "model")
            )
            return timing

    def record_decision(self, signal: ScalpSignal, *, run_id: str, now: datetime) -> None:
        if signal.family == "none" and signal.action == "hold":
            with self._lock:
                self._neutral.update(signal.reasons)
        else:
            self.store.audit(
                "candidate_decision",
                {
                    "run_id": run_id,
                    "account_digest": self.account_digest,
                    "signal": signal.model_dump(mode="json"),
                    "candidate_only_not_a_fill": True,
                },
                at=now,
                identity=f"scalp-candidate:{self.account_digest}:{signal.decision_id}",
            )
        with self._lock:
            if now - self._neutral_since >= timedelta(minutes=1):
                self._flush_neutral(run_id, now)

    def _flush_neutral(self, run_id: str, now: datetime) -> None:
        if self._neutral:
            self.store.audit(
                "no_trade_observations",
                {
                    "run_id": run_id,
                    "period_start": self._neutral_since.isoformat(),
                    "period_end": now.isoformat(),
                    "reason_counts": dict(self._neutral),
                    "basis": "neutral observations aggregated; every candidate retained",
                },
                at=now,
            )
        self._neutral.clear()
        self._neutral_since = now

    def flush(self, *, run_id: str, now: datetime) -> None:
        with self._lock:
            self._flush_neutral(run_id, now)

    def diagnose_closed(self, *, run_id: str, now: datetime) -> int:
        from tradeagent.scalping_diagnostics import reconstruct_period

        with self.database.begin() as connection:
            candidates = [
                dict(row)
                for row in connection.execute(
                    select(scalping_cycles)
                    .where(
                        scalping_cycles.c.account_digest == self.account_digest,
                        scalping_cycles.c.run_id == run_id,
                        scalping_cycles.c.closed_at <= now - timedelta(seconds=10),
                        scalping_cycles.c.updated_at <= now - timedelta(seconds=10),
                        or_(
                            scalping_cycles.c.updated_at > self._cursor_at,
                            and_(
                                scalping_cycles.c.updated_at == self._cursor_at,
                                scalping_cycles.c.cycle_id > self._cursor_id,
                            ),
                        ),
                    )
                    .order_by(scalping_cycles.c.updated_at, scalping_cycles.c.cycle_id)
                    .limit(100)
                ).mappings()
            ]
        completed = 0
        for cycle in candidates:
            identity = str(cycle["cycle_id"])
            with self.database.begin() as connection:
                existing = connection.scalar(
                    select(events.c.event_id).where(
                        events.c.event_type == "scalp_trade_diagnostics",
                        events.c.trace_id == identity,
                        events.c.payload["cycle_updated_at"].as_string()
                        == utc(cycle["updated_at"]).isoformat(),
                    )
                )
                if existing is not None:
                    self._cursor_at, self._cursor_id = utc(cycle["updated_at"]), identity
                    continue
                order_rows = [
                    dict(row)
                    for row in connection.execute(
                        select(
                            orders,
                            scalping_order_links.c.intent,
                            scalping_order_links.c.broker,
                            scalping_order_links.c.cycle_id,
                            scalping_order_links.c.submission_started_at,
                            scalping_order_links.c.first_positive_fill_at,
                            scalping_order_links.c.cancel_requested_at,
                            scalping_order_links.c.last_reconciled_at,
                            scalping_order_links.c.expires_at,
                            scalping_order_links.c.dispatch_state,
                        )
                        .join(
                            scalping_order_links,
                            orders.c.client_order_id == scalping_order_links.c.client_order_id,
                        )
                        .where(scalping_order_links.c.cycle_id == identity)
                    ).mappings()
                ]
                activities = [
                    dict(row)
                    for row in connection.execute(
                        select(scalping_activities).where(
                            scalping_activities.c.cycle_id == identity
                        )
                    ).mappings()
                ]
                audit = [
                    dict(row)
                    for row in connection.execute(
                        select(events).where(
                            events.c.trace_id == identity,
                            events.c.event_type != "scalp_trade_diagnostics",
                        )
                    ).mappings()
                ]
            with self._lock:
                start_ns = datetime_ns(utc(cycle["created_at"])) - 5 * NS
                end_ns = datetime_ns(utc(cycle["closed_at"])) + 10 * NS
                quotes = [
                    row
                    for row in self._quotes
                    if row["symbol"] == cycle["symbol"]
                    and start_ns <= row["received_at_ns"] <= end_ns
                ]
                evictions = self._evictions
            dataset = json_value(
                {
                    "cycles": [cycle],
                    "orders": order_rows,
                    "activities": activities,
                    "audits": audit,
                    "quotes": quotes,
                    "account_digest": self.account_digest,
                    "period_start": utc(cycle["created_at"]).isoformat(),
                    "period_end": now.isoformat(),
                    "tape_coverage": {
                        "basis": "bounded live cache; immutable raw tape enables backfill",
                        "cache_evictions": evictions,
                        "unobserved_prices_are_unknown": True,
                    },
                }
            )
            report = reconstruct_period(dataset)
            self.store.audit(
                "trade_diagnostics",
                {
                    "cycle_id": identity,
                    "cycle_updated_at": utc(cycle["updated_at"]).isoformat(),
                    "run_id": run_id,
                    "report": report,
                },
                at=now,
                identity=f"scalp-diagnostic:{identity}:{utc(cycle['updated_at']).isoformat()}",
            )
            self._cursor_at, self._cursor_id = utc(cycle["updated_at"]), identity
            completed += 1
        self._diagnostic_reports += completed
        return completed

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "diagnostic_reports": self._diagnostic_reports,
                "quote_cache_size": len(self._quotes),
                "quote_cache_evictions": self._evictions,
                "raw_tape_backfill_available": True,
                "pipeline_latency": self.latencies.snapshot(),
                "neutral_observations_pending_flush": sum(self._neutral.values()),
            }
