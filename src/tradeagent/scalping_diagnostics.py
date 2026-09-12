"""Bounded, read-only execution research; no broker, database, clock, or filesystem I/O.

``reconstruct_period(snapshot)`` accepts an already selected evidence snapshot, not a job
envelope. It returns JSON-compatible ``scalp-diagnostics-v1`` data. Decimal measurements
are strings; unavailable measurements have ``value=None`` and an explicit ``reason``.
No missing prediction, fee, fill, queue position, or latency is replaced with a guess.
Raw ``calibration_samples`` instead use the empirical model's finite JSON-number transport,
with exact decimal evidence and separate missing/eligibility reasons; no model is fitted here.

Quotes must have exchange AND receipt times no later than the observation target.
Maximum age and capture delay are 250 ms; the 100 ms horizon has a 100 ms age limit.
There is no interpolation, nearest/future quote matching, or spread deduction from fills.
Markouts are quantity-weighted *per execution*, each at its own fill time plus horizon.
Cumulative-only orders use their completion VWAP/time, explicitly not a first-fill claim.
Excursions use the first execution price and only quotes inside the actual holding period.
Observed extrema are not complete-curve extrema when any freshness coverage is missing.

``QuoteTimeline`` exposes the same kernels to live telemetry and replay. Pass an explicit
``observed_through_ns`` watermark in those callers: future horizons remain pending. Without
one, availability ends at the latest valid quote receipt, not at an inferred wall clock.

Future signals may record prediction fields directly or in ``signal.prediction``:
``predicted_move_bps``, ``predicted_horizon_seconds``, ``expected_net_edge_bps``,
``predicted_fill_probability``, ``confidence``, ``uncertainty``, and ``validated``.
Heuristic scores and feature lookbacks are never promoted to those fields.
Stage timing accepts the t0_received_ns ... t8_filled_ns telemetry contract, including
scalp_dispatch_timing and scalp_fill_timing audits keyed by original client order ID.
"""

from __future__ import annotations

import json
import re
from bisect import bisect_left, bisect_right
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation, localcontext
from hashlib import sha256
from math import isfinite
from typing import Any

NS = 1_000_000_000
MS = 1_000_000
D = Decimal
ZERO = D(0)
QUANTITY_TOLERANCE = D("0.000000000001")
MONEY_TOLERANCE = D("0.00000001")
MAX_TIMESTAMP_NS = 253_402_300_799_999_999_999
HORIZONS_MS = (100, 250, 500, 1000, 2000, 5000, 10000)
HORIZON_NAMES = ("100ms", "250ms", "500ms", "1s", "2s", "5s", "10s")
CALIBRATION_HORIZONS_SECONDS = (1, 5)
CALIBRATION_FEATURES = (
    "normalized_ofi_1s",
    "normalized_ofi_5s",
    "ofi_1s",
    "ofi_5s",
    "l1_imbalance",
    "l5_imbalance",
    "taker_delta_1s",
    "taker_delta_5s",
    "microprice_displacement_bps",
    "spread_bps",
    "bid_depth_l5",
    "ask_depth_l5",
    "bid_slope_bps_per_unit",
    "ask_slope_bps_per_unit",
    "bid_depletion_fraction_5s",
    "ask_depletion_fraction_5s",
    "bid_additions_1s",
    "bid_additions_5s",
    "ask_additions_1s",
    "ask_additions_5s",
    "bid_removals_proxy_5s",
    "ask_removals_proxy_5s",
    "event_intensity_5s",
    "trade_intensity_5s",
    "trade_volume_5s",
    "trade_count_5s",
    "known_taker_fraction_5s",
    "volatility_1s_bps",
    "volatility_5s_bps",
    "volatility_15s_bps",
    "return_1s_bps",
    "return_5s_bps",
    "return_15s_bps",
    "momentum_score",
    "reversion_score",
)
FAILURE_LABELS = (
    "ALPHA_FAILURE",
    "EXECUTION_FAILURE",
    "ADVERSE_SELECTION",
    "LATENCY_FAILURE",
    "COST_FAILURE",
    "EXIT_FAILURE",
    "REGIME_FAILURE",
    "STALE_DATA_FAILURE",
    "ACCOUNTING_FAILURE",
    "UNKNOWN",
)
FINAL_ORDER_STATES = frozenset(
    {"filled", "canceled", "cancelled", "expired", "rejected", "done_for_day", "replaced"}
)
CLOSED_CYCLE_STATES = frozenset({"closed_owned_flat", "closed"})
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


@dataclass(frozen=True)
class DiagnosticPolicy:
    """Research bounds, not trading limits. Oversized inputs fail, never silently truncate."""

    max_cycles: int = 2000
    max_orders: int = 10000
    max_activities: int = 30000
    max_audits: int = 100000
    max_quotes: int = 250000
    max_quote_age_ms: int = 250
    max_capture_delay_ms: int = 250
    max_curve_seconds: int = 900
    quantity_tolerance: Decimal = QUANTITY_TOLERANCE
    money_tolerance: Decimal = MONEY_TOLERANCE
    material_latency_ms: int = 250

    def __post_init__(self) -> None:
        for name, value in asdict(self).items():
            if isinstance(value, Decimal) and not value.is_finite():
                raise ValueError(f"diagnostic policy {name} must be finite")
            if isinstance(value, bool) or not isinstance(value, (int, Decimal)) or value <= 0:
                raise ValueError(f"diagnostic policy {name} must be positive")
        if self.max_quote_age_ms > 250 or self.max_capture_delay_ms > 250:
            raise ValueError("quote age/capture bounds may be tightened, not relaxed beyond 250 ms")


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _decimal(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = D(str(value))
    except (InvalidOperation, ValueError):
        return None
    return result if result.is_finite() else None


def _ns(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if 0 <= value <= MAX_TIMESTAMP_NS else None
    if isinstance(value, str) and value.isdecimal():
        parsed = int(value)
        return parsed if parsed <= MAX_TIMESTAMP_NS else None
    if not isinstance(value, (str, datetime)):
        return None
    remainder = 0
    if isinstance(value, str):
        fraction = re.search(r"\.(\d+)(?:Z|[+-]\d\d:\d\d)$", value)
        if fraction:
            digits = fraction.group(1)
            if len(digits) > 9:
                return None
            remainder = int(digits.ljust(9, "0")) % 1000
        try:
            instant = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        instant = value
    if instant.tzinfo is None:
        return None
    delta = instant.astimezone(UTC) - datetime(1970, 1, 1, tzinfo=UTC)
    result = (delta.days * 86400 + delta.seconds) * NS + delta.microseconds * 1000 + remainder
    return result if result >= 0 else None


def _iso(value: int | None) -> str | None:
    if value is None or not 0 <= value <= MAX_TIMESTAMP_NS:
        return None
    seconds, remainder = divmod(value, NS)
    instant = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(seconds=seconds)
    return instant.strftime("%Y-%m-%dT%H:%M:%S") + f".{remainder:09d}Z"


def _json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if isinstance(value, float) and _decimal(value) is None:
        return None
    return value


def _metric(value: Any, basis: str, reason: str = "not_recorded_or_invalid") -> dict[str, Any]:
    return {
        "value": _json_value(value),
        "basis": basis,
        "reason": reason if value is None else None,
    }


def _time(value: int | None, basis: str, reason: str = "timestamp_not_recorded") -> dict[str, Any]:
    return {**_metric(_iso(value), basis, reason), "unix_ns": value}


def _value(metric: Mapping[str, Any]) -> Decimal | None:
    return _decimal(metric.get("value"))


def _symbol(value: Any) -> str:
    symbol = str(value or "").upper().strip()
    if "/" not in symbol and symbol.endswith("USD"):
        symbol = symbol[:-3] + "/USD"
    return symbol


def _rows(snapshot: Mapping[str, Any], name: str, limit: int) -> list[Mapping[str, Any]]:
    values = snapshot.get(name, [])
    if not isinstance(values, Sequence) or isinstance(values, (str, bytes)):
        raise ValueError(f"{name} must be a sequence of mappings")
    if len(values) > limit:
        raise ValueError(f"{name} has {len(values)} rows; diagnostic bound is {limit}")
    if any(not isinstance(item, Mapping) for item in values):
        raise ValueError(f"{name} contains a non-mapping row")
    return [_mapping(item) for item in values]


def _distribution(values: Sequence[Decimal | None], basis: str) -> dict[str, Any]:
    present = sorted(value for value in values if value is not None)
    count = len(present)
    return {
        "samples": count,
        "missing": len(values) - count,
        "total": len(values),
        "basis": basis,
        "reason": None if present else "no_usable_samples",
        "mean": str(sum(present, ZERO) / count) if present else None,
        "min": str(present[0]) if present else None,
        "max": str(present[-1]) if present else None,
        "positive": sum(value > 0 for value in present),
        "negative": sum(value < 0 for value in present),
        "zero": sum(value == 0 for value in present),
        **{
            name: str(present[max(0, (count * percent + 99) // 100 - 1)]) if count else None
            for name, percent in (("p50", 50), ("p95", 95), ("p99", 99))
        },
    }


@dataclass(frozen=True)
class _Quote:
    symbol: str
    bid: Decimal
    ask: Decimal
    bid_size: Decimal | None
    ask_size: Decimal | None
    exchange: int
    received: int
    event_id: str | None
    batch_id: str | None
    source: str

    @property
    def mid(self) -> Decimal:
        return (self.bid + self.ask) / 2

    def report(self, target: int) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "batch_id": self.batch_id,
            "batch_id_reason": None if self.batch_id else "not_recorded_in_snapshot",
            "source": self.source,
            "exchange_at_ns": self.exchange,
            "received_at_ns": self.received,
            "exchange_at": _iso(self.exchange),
            "received_at": _iso(self.received),
            "capture_delay_ms": str(D(self.received - self.exchange) / MS),
            "age_at_target_ms": str(D(target - self.exchange) / MS),
            "bid": str(self.bid),
            "ask": str(self.ask),
            "mid": str(self.mid),
            "bid_size": _metric(self.bid_size, "recorded BBO size"),
            "ask_size": _metric(self.ask_size, "recorded BBO size"),
        }


class _Quotes:
    def __init__(self, rows: Sequence[Mapping[str, Any]], policy: DiagnosticPolicy) -> None:
        self.policy = policy
        self.rejected: Counter[str] = Counter()
        grouped: dict[str, list[_Quote]] = defaultdict(list)
        seen: dict[str, _Quote] = {}
        conflicting: set[str] = set()
        for row in rows:
            quote, reason = self.parse(row)
            if quote is None:
                self.rejected[reason] += 1
                continue
            if quote.event_id and quote.event_id in seen:
                previous = seen[quote.event_id]
                if (
                    previous.symbol,
                    previous.bid,
                    previous.ask,
                    previous.exchange,
                    previous.received,
                ) == (quote.symbol, quote.bid, quote.ask, quote.exchange, quote.received):
                    self.rejected["duplicate_quote"] += 1
                else:
                    conflicting.add(quote.event_id)
                    self.rejected["conflicting_quote_identity"] += 1
                continue
            if quote.event_id:
                seen[quote.event_id] = quote
            grouped[quote.symbol].append(quote)
        self.by_symbol = {
            symbol: sorted(
                (q for q in quotes if q.event_id not in conflicting),
                key=lambda q: (q.exchange, q.received, q.event_id or ""),
            )
            for symbol, quotes in grouped.items()
        }
        self.times = {
            symbol: [quote.exchange for quote in quotes]
            for symbol, quotes in self.by_symbol.items()
        }
        self.input_count = len(rows)

    def parse(self, row: Mapping[str, Any]) -> tuple[_Quote | None, str]:
        bid, ask = _decimal(row.get("bid")), _decimal(row.get("ask"))
        exchange = _ns(row.get("exchange_at_ns", row.get("exchange_time_ns")))
        if exchange is None:
            exchange = _ns(row.get("exchange_at"))
        received = _ns(row.get("received_at_ns", row.get("received_at")))
        symbol = _symbol(row.get("symbol"))
        if not symbol or bid is None or ask is None or bid <= 0 or ask <= 0:
            return None, "missing_or_invalid_quote_price_symbol"
        if bid > ask:
            return None, "crossed_quote"
        sizes = (_decimal(row.get("bid_size")), _decimal(row.get("ask_size")))
        for name, size in zip(("bid_size", "ask_size"), sizes, strict=True):
            if row.get(name) is not None and (size is None or size < 0):
                return None, "invalid_quote_size"
        if exchange is None or received is None:
            return None, "missing_or_invalid_quote_timestamp"
        if received < exchange:
            return None, "negative_capture_delay"
        if received - exchange > self.policy.max_capture_delay_ms * MS:
            return None, "lagged_at_capture"
        source = str(row.get("source", "recorded_decision_quote"))
        if source not in {
            "native_quote",
            "reconstructed_l2",
            "recorded_decision_quote",
            "provided_quote",
        }:
            return None, "unsupported_quote_source"
        return (
            _Quote(
                symbol,
                bid,
                ask,
                _decimal(row.get("bid_size")),
                _decimal(row.get("ask_size")),
                exchange,
                received,
                row.get("event_id"),
                row.get("batch_id"),
                source,
            ),
            "",
        )

    def at(self, symbol: str, target: int | None, max_age_ms: int) -> _Quote | None:
        if target is None:
            return None
        quotes = self.by_symbol.get(symbol, [])
        times = self.times.get(symbol, [])
        low = bisect_left(times, target - max_age_ms * MS)
        high = bisect_right(times, target)
        for index in range(high - 1, low - 1, -1):
            quote = quotes[index]
            if quote.received <= target:
                return quote
        return None

    def within(self, symbol: str, start: int, end: int) -> list[_Quote]:
        times = self.times.get(symbol, [])
        candidates = self.by_symbol.get(symbol, [])[
            bisect_left(times, start) : bisect_right(times, end)
        ]
        result: list[_Quote] = []
        newest_exchange = start
        for quote in sorted(candidates, key=lambda item: (item.received, item.exchange)):
            if (
                quote.received <= end
                and quote.received - quote.exchange <= self.policy.max_quote_age_ms * MS
                and quote.exchange >= newest_exchange
            ):
                result.append(quote)
                newest_exchange = quote.exchange
        return result


@dataclass(frozen=True)
class _BrokerOrder:
    quantity: Decimal | None
    price: Decimal | None
    broker_id: str | None
    client_id: str | None
    symbol: str
    side: str
    status: str
    filled_at: int | None
    submitted_at: int | None
    issues: tuple[str, ...] = ()

    @property
    def value(self) -> Decimal | None:
        if self.quantity == 0:
            return ZERO
        if self.quantity is None or self.price is None:
            return None
        return self.quantity * self.price


def _broker_order(raw: Mapping[str, Any]) -> _BrokerOrder:
    issues: list[str] = []

    def number_alias(typed: str, public: str) -> Decimal | None:
        value = _decimal(raw.get(typed, raw.get(public)))
        if typed in raw and public in raw and _decimal(raw[typed]) != _decimal(raw[public]):
            issues.append(f"conflicting_aliases:{typed}:{public}")
            return None
        return value

    quantity = number_alias("filled_quantity", "filled_qty")
    price = number_alias("filled_average_price", "filled_avg_price")
    if quantity is not None and quantity < 0:
        issues.append("negative_filled_quantity")
        quantity = None
    if price is not None and price <= 0:
        issues.append("nonpositive_fill_price")
        price = None
    return _BrokerOrder(
        quantity=quantity,
        price=price,
        broker_id=raw.get("id", raw.get("broker_order_id")),
        client_id=raw.get("client_order_id"),
        symbol=_symbol(raw.get("symbol")),
        side=str(raw.get("side", "")),
        status=str(raw.get("status", "")),
        filled_at=_ns(raw.get("filled_at")),
        submitted_at=_ns(raw.get("submitted_at")),
        issues=tuple(issues),
    )


@dataclass(frozen=True)
class _Activity:
    identity: str
    kind: str
    broker_id: str | None
    attributed_order_id: str | None
    cycle_id: str | None
    symbol: str
    side: str
    quantity: Decimal | None
    price: Decimal | None
    cash: Decimal | None
    at: int | None
    attribution: str
    currency: str | None
    issues: tuple[str, ...]

    @property
    def value(self) -> Decimal | None:
        return self.quantity * self.price if self.quantity and self.price is not None else None

    def report(self) -> dict[str, Any]:
        return {
            "activity_id": self.identity,
            "kind": self.kind,
            "broker_order_id": self.broker_id,
            "attribution_basis": self.attribution,
            "quantity": _metric(self.quantity, "incremental activity qty, never cum_qty"),
            "price": _metric(self.price, "FILL activity execution price"),
            "net_amount": _metric(self.cash, "signed activity cash amount"),
            "occurred_at": _time(self.at, "broker activity transaction timestamp"),
        }


def _activities(rows: Sequence[Mapping[str, Any]], issues: list[dict[str, Any]]) -> list[_Activity]:
    unique: dict[str, _Activity] = {}
    conflicts: set[str] = set()
    for index, row in enumerate(rows):
        raw = _mapping(row.get("payload")) or row
        identity = row.get("activity_id") or raw.get("id") or row.get("activity_key")
        if not identity:
            issues.append(
                {"scope": "activity", "row": index, "reason": "missing_activity_identity"}
            )
            continue
        kind = str(row.get("kind", raw.get("activity_type", ""))).upper()
        notes = []
        for outer, inner in (
            ("broker_order_id", "order_id"),
            ("side", "side"),
            ("symbol", "symbol"),
        ):
            if row.get(outer) and raw.get(inner):
                left, right = row[outer], raw[inner]
                if outer == "symbol":
                    left, right = _symbol(left), _symbol(right)
                if left != right:
                    notes.append(f"conflicting_activity_{outer}")
        transaction_at = _ns(
            raw.get("transaction_time", raw.get("created_at", row.get("occurred_at")))
        )
        if kind == "FILL" and row.get("time_precision") in {"day", "date"}:
            transaction_at = None
            issues.append(
                {
                    "scope": "activity",
                    "identity": identity,
                    "reason": "fill_time_precision_insufficient_for_markouts",
                }
            )
        item = _Activity(
            str(identity),
            kind,
            raw.get("order_id") or row.get("broker_order_id"),
            row.get("attributed_order_id"),
            row.get("cycle_id"),
            _symbol(raw.get("symbol") or row.get("symbol")),
            str(raw.get("side") or row.get("side") or ""),
            _decimal(raw.get("qty", row.get("quantity"))),
            _decimal(raw.get("price")),
            _decimal(raw.get("net_amount", row.get("net_amount"))),
            transaction_at,
            str(row.get("attribution_basis") or ("broker_order_id" if raw.get("order_id") else "")),
            raw.get("currency"),
            tuple(notes),
        )
        key = kind + ":" + str(identity)
        if key in unique:
            reason = (
                "duplicate_activity" if unique[key] == item else "conflicting_duplicate_activity"
            )
            if unique[key] != item:
                conflicts.add(key)
            issues.append({"scope": "activity", "identity": identity, "reason": reason})
        else:
            unique[key] = item
    return [item for key, item in unique.items() if key not in conflicts]


def _delta(left: Decimal | None, right: Decimal | None, tolerance: Decimal) -> dict[str, Any]:
    difference = left - right if left is not None and right is not None else None
    return {
        "difference": str(difference) if difference is not None else None,
        "within_tolerance": abs(difference) <= tolerance if difference is not None else None,
        "tolerance": str(tolerance),
        "reason": None if difference is not None else "one_or_both_sources_missing",
    }


@dataclass
class _Order:
    row: Mapping[str, Any]
    cycle_id: str
    client_id: str
    broker_id: str | None
    symbol: str
    side: str
    local: _BrokerOrder
    historical: _BrokerOrder | None
    fills: list[_Activity]
    fees: list[_Activity]
    issues: list[str]

    @property
    def primary(self) -> _BrokerOrder:
        return self.historical if self.historical is not None else self.local

    @property
    def quantity(self) -> Decimal | None:
        return self.primary.quantity if not self.issues else None

    @property
    def value(self) -> Decimal | None:
        return self.primary.value if not self.issues else None

    @property
    def price(self) -> Decimal | None:
        return self.primary.price if not self.issues else None

    @property
    def activity_quantity(self) -> Decimal | None:
        if not self.fills or any(item.quantity is None for item in self.fills):
            return None
        return sum((item.quantity for item in self.fills if item.quantity is not None), ZERO)

    @property
    def activity_value(self) -> Decimal | None:
        if not self.fills or any(item.value is None for item in self.fills):
            return None
        return sum((item.value for item in self.fills if item.value is not None), ZERO)

    def activity_matches(self, policy: DiagnosticPolicy) -> bool:
        return (
            self.quantity is not None
            and self.activity_quantity is not None
            and self.value is not None
            and self.activity_value is not None
            and abs(self.quantity - self.activity_quantity) <= policy.quantity_tolerance
            and abs(self.value - self.activity_value) <= policy.money_tolerance
        )

    def report(self, policy: DiagnosticPolicy) -> dict[str, Any]:
        primary = self.primary
        historical = self.historical
        history_agrees_with_activities = historical is not None and self.activity_matches(policy)
        checks = {
            "historical_minus_stored_quantity": _delta(
                historical.quantity if historical else None,
                self.local.quantity,
                policy.quantity_tolerance,
            ),
            "historical_minus_stored_value": _delta(
                historical.value if historical else None,
                self.local.value,
                policy.money_tolerance,
            ),
            "activities_minus_primary_quantity": _delta(
                self.activity_quantity,
                self.quantity,
                policy.quantity_tolerance,
            ),
            "activities_minus_primary_value": _delta(
                self.activity_value,
                self.value,
                policy.money_tolerance,
            ),
            "local_row_minus_primary_quantity": _delta(
                _decimal(self.row.get("filled_quantity")),
                self.quantity,
                policy.quantity_tolerance,
            ),
        }
        proof = history_agrees_with_activities and any(
            checks[key]["within_tolerance"] is False
            for key in (
                "historical_minus_stored_quantity",
                "historical_minus_stored_value",
                "local_row_minus_primary_quantity",
            )
        )
        intent = _mapping(self.row.get("intent"))
        request = _mapping(intent.get("request"))
        return {
            "client_order_id": self.client_id,
            "broker_order_id": self.broker_id,
            "cycle_id": self.cycle_id,
            "run_id": self.row.get("run_id"),
            "symbol": self.symbol,
            "side": self.side,
            "status": primary.status,
            "dispatch_state": self.row.get("dispatch_state"),
            "identity_valid": not self.issues,
            "issues": self.issues,
            "source": "broker_historical_orders" if historical else "stored_broker_order",
            "order_type": _metric(request.get("order_type"), "original intent"),
            "limit_price": _metric(_decimal(intent.get("limit_price")), "original intent"),
            "requested_quantity": _metric(_decimal(request.get("quantity")), "original intent"),
            "filled_quantity": _metric(self.quantity, "positive cumulative execution quantity"),
            "fill_value": _metric(self.value, "cumulative quantity times execution VWAP"),
            "weighted_price": _metric(
                self.price, "broker cumulative execution VWAP", "no_priced_fill"
            ),
            "broker_filled_at": _time(primary.filled_at, "broker cumulative completion timestamp"),
            "broker_submitted_at": _time(primary.submitted_at, "broker acceptance, not local ack"),
            "dispatch_started_at": _time(
                _ns(self.row.get("submission_started_at")),
                "durable dispatch marker; includes pre-POST account/risk work",
            ),
            "first_positive_fill_observed_at": _time(
                _ns(self.row.get("first_positive_fill_at")),
                "stored first-positive-fill timestamp; may be broker time or receipt fallback",
            ),
            "expires_at": _time(_ns(self.row.get("expires_at")), "original local order TTL"),
            "cancel_requested_at": _time(
                _ns(self.row.get("cancel_requested_at")), "local cancellation request"
            ),
            "submission_error": self.row.get("submission_error"),
            "fill_activities": [item.report() for item in self.fills],
            "fee_activities": [item.report() for item in self.fees],
            "activity_quantity": _metric(
                self.activity_quantity, "sum of unique incremental FILL qty"
            ),
            "activity_value": _metric(self.activity_value, "sum of unique FILL qty times price"),
            "historical_coverage": historical is not None and historical.quantity is not None,
            "fill_activity_coverage": self.activity_matches(policy),
            "independent_fill_crosscheck": history_agrees_with_activities,
            "cross_checks": checks,
            "proven_stored_fill_mismatch": proof,
        }


class _Evidence:
    def __init__(self, snapshot: Mapping[str, Any], policy: DiagnosticPolicy) -> None:
        self.policy = policy
        self.issues: list[dict[str, Any]] = []
        self.cycles = _rows(snapshot, "cycles", policy.max_cycles)
        self.audits = _rows(snapshot, "audits", policy.max_audits)
        self.quotes = _Quotes(_rows(snapshot, "quotes", policy.max_quotes), policy)
        rows = _rows(snapshot, "orders", policy.max_orders)
        history = _rows(snapshot, "broker_historical_orders", policy.max_orders)
        activity_rows = _rows(snapshot, "activities", policy.max_activities)
        activities = _activities(activity_rows, self.issues)
        self.activities = activities
        self.invalid_cycles: dict[str, list[str]] = defaultdict(list)
        cycle_map: dict[str, Mapping[str, Any]] = {}
        account = snapshot.get("account_digest")
        accounts = {
            str(row["account_digest"])
            for row in [*self.cycles, *activity_rows]
            if row.get("account_digest")
        }
        if len(accounts) > 1 or (account and accounts and accounts != {account}):
            raise ValueError("cycle/activity account_digest does not match snapshot")
        for cycle_row in self.cycles:
            identity = str(cycle_row.get("cycle_id", ""))
            if not identity or identity in cycle_map:
                raise ValueError("cycles require unique, nonempty cycle_id values")
            cycle_map[identity] = cycle_row
        hist_by_client: dict[str, list[_BrokerOrder]] = defaultdict(list)
        for raw in history:
            normalized = _broker_order(raw)
            if normalized.client_id and normalized not in hist_by_client[normalized.client_id]:
                hist_by_client[normalized.client_id].append(normalized)
        activities_by_broker: dict[str, list[_Activity]] = defaultdict(list)
        for item in activities:
            if item.broker_id:
                activities_by_broker[item.broker_id].append(item)
        clients: dict[str, Mapping[str, Any]] = {}
        conflicts: set[str] = set()
        broker_owners: dict[str, set[str]] = defaultdict(set)
        for index, row in enumerate(rows):
            client = str(row.get("client_order_id") or "")
            if not client:
                self.issues.append(
                    {"scope": "order", "row": index, "reason": "missing_client_order_id"}
                )
                if row.get("cycle_id") in cycle_map:
                    self.invalid_cycles[str(row["cycle_id"])].append("missing_client_order_id")
                continue
            if client in clients:
                reason = (
                    "duplicate_order" if row == clients[client] else "conflicting_duplicate_order"
                )
                self.issues.append({"scope": "order", "identity": client, "reason": reason})
                if row != clients[client]:
                    conflicts.add(client)
            else:
                clients[client] = row
            broker_id = row.get("broker_order_id") or _mapping(row.get("broker")).get("id")
            if broker_id:
                broker_owners[str(broker_id)].add(client)
        self.orders: dict[str, list[_Order]] = defaultdict(list)
        self.by_client: dict[str, _Order] = {}
        for client, row in clients.items():
            cycle_id = str(row.get("cycle_id") or "")
            cycle = cycle_map.get(cycle_id)
            if cycle is None:
                self.issues.append(
                    {"scope": "order", "identity": client, "reason": "unknown_cycle_id"}
                )
                continue
            local = _broker_order(_mapping(row.get("broker")) or row)
            broker_id = row.get("broker_order_id") or local.broker_id
            symbol, side = _symbol(cycle.get("symbol")), str(row.get("side") or local.side)
            notes = list(local.issues)
            request = _mapping(_mapping(row.get("intent")).get("request"))
            if client in conflicts:
                notes.append("conflicting_duplicate_order")
            if broker_id and len(broker_owners[broker_id]) > 1:
                notes.append("broker_order_reused_by_multiple_local_orders")
            for name, observed, expected in (
                ("symbol", _symbol(row.get("symbol")), symbol),
                ("broker_symbol", local.symbol, symbol),
                ("broker_side", local.side, side),
                ("broker_client_id", local.client_id, client),
                ("broker_order_id", local.broker_id, broker_id),
                ("intent_client_id", request.get("client_order_id"), client),
                ("intent_symbol", _symbol(request.get("symbol")), symbol),
                ("intent_side", request.get("side"), side),
                ("run_id", row.get("run_id"), cycle.get("run_id")),
            ):
                if observed and expected and observed != expected:
                    notes.append(f"mismatched_{name}")
            if side not in {"buy", "sell"}:
                notes.append("missing_or_invalid_side")
            candidates = hist_by_client.get(client, [])
            historical = candidates[0] if len(candidates) == 1 else None
            if len(candidates) > 1:
                notes.append("conflicting_historical_orders")
            if historical:
                notes.extend(historical.issues)
                for name, observed, expected in (
                    ("broker_id", historical.broker_id, broker_id),
                    ("symbol", historical.symbol, symbol),
                    ("side", historical.side, side),
                ):
                    if observed and expected and observed != expected:
                        notes.append(f"historical_mismatched_{name}")
            matched: list[_Activity] = []
            for item in activities_by_broker.get(str(broker_id), []):
                mismatch = (
                    bool(item.issues)
                    or (item.cycle_id is not None and item.cycle_id != cycle_id)
                    or (item.symbol not in {"", symbol})
                    or (bool(item.side) and item.side != side)
                )
                if mismatch:
                    self.issues.append(
                        {
                            "scope": "activity",
                            "identity": item.identity,
                            "cycle_id": cycle_id,
                            "reason": "mispaired_activity",
                        }
                    )
                else:
                    matched.append(item)
            fills = [
                item
                for item in matched
                if item.kind == "FILL"
                and item.quantity is not None
                and item.quantity > 0
                and item.price is not None
                and item.price > 0
            ]
            if fills and (historical if historical is not None else local).quantity == 0:
                notes.append("positive_activity_conflicts_with_zero_cumulative_fill")
            fees = [
                item
                for item in matched
                if item.kind in {"CFEE", "FEE"}
                and item.attribution == "broker_order_id"
                and item.quantity is not None
                and item.cash is not None
                and item.currency in {None, "", "USD"}
            ]
            for item in matched:
                if item.kind in {"FILL", "CFEE", "FEE"} and item not in fills and item not in fees:
                    self.issues.append(
                        {
                            "scope": "activity",
                            "identity": item.identity,
                            "cycle_id": cycle_id,
                            "reason": "invalid_or_unconfirmed_activity",
                        }
                    )
            order = _Order(
                row,
                cycle_id,
                client,
                broker_id,
                symbol,
                side,
                local,
                historical,
                sorted(fills, key=lambda item: (item.at or 0, item.identity)),
                fees,
                notes,
            )
            self.orders[cycle_id].append(order)
            self.by_client[client] = order
            self.issues.extend(
                {"scope": "order", "identity": client, "cycle_id": cycle_id, "reason": note}
                for note in notes
            )
        self.audits_by_cycle: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
        self.audits_by_client: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
        for audit in self.audits:
            payload = _mapping(audit.get("payload"))
            audit_cycle = payload.get("cycle_id") or audit.get("trace_id")
            if audit_cycle in cycle_map:
                self.audits_by_cycle[str(audit_cycle)].append(audit)
            audit_client = payload.get("client_order_id")
            if audit_client in self.by_client:
                self.audits_by_client[str(audit_client)].append(audit)
        for item in activities:
            if item.broker_id and item.broker_id not in broker_owners:
                self.issues.append(
                    {
                        "scope": "activity",
                        "identity": item.identity,
                        "reason": "unmatched_activity_broker_order_id",
                    }
                )
            elif item.kind in {"CFEE", "FEE"} and not item.broker_id:
                self.issues.append(
                    {
                        "scope": "activity",
                        "identity": item.identity,
                        "reason": "unkeyed_fee_is_unconfirmed",
                    }
                )


@dataclass(frozen=True)
class _FillAnchor:
    at: int | None
    price: Decimal
    quantity: Decimal
    identity: str
    basis: str


def _anchors(orders: Sequence[_Order], policy: DiagnosticPolicy) -> list[_FillAnchor]:
    result: list[_FillAnchor] = []
    for order in orders:
        if order.quantity is None or order.quantity <= 0 or order.price is None:
            continue
        if order.activity_matches(policy) and all(item.at is not None for item in order.fills):
            result.extend(
                _FillAnchor(item.at, item.price, item.quantity, item.identity, "FILL activity")
                for item in order.fills
                if item.price is not None and item.quantity is not None
            )
        else:
            result.append(
                _FillAnchor(
                    order.primary.filled_at,
                    order.price,
                    order.quantity,
                    order.client_id,
                    "cumulative order completion VWAP/time; not a first-fill claim",
                )
            )
    return sorted(result, key=lambda item: (item.at or 0, item.identity))


def _markout(
    anchors: Sequence[_FillAnchor],
    symbol: str,
    sign: Decimal,
    horizon_ms: int,
    quotes: _Quotes,
    exit_at: int | None,
    *,
    observed_through_ns: int | None = None,
) -> dict[str, Any]:
    max_age = min(quotes.policy.max_quote_age_ms, horizon_ms or quotes.policy.max_quote_age_ms)
    observations = []
    priced = ZERO
    weighted = ZERO
    mid_weighted = ZERO
    reference = ZERO
    for anchor in anchors:
        target = anchor.at + horizon_ms * MS if anchor.at is not None else None
        pending = (
            target is not None and observed_through_ns is not None and target > observed_through_ns
        )
        quote = None if pending else quotes.at(symbol, target, max_age)
        change = sign * (quote.mid - anchor.price) if quote is not None else None
        observations.append(
            {
                "fill_id": anchor.identity,
                "fill_basis": anchor.basis,
                "fill_time": _iso(anchor.at),
                "fill_price": str(anchor.price),
                "fill_quantity": str(anchor.quantity),
                "target_at_ns": target,
                "target_at": _iso(target),
                "after_position_exit": target > exit_at if target and exit_at else None,
                "sample": quote.report(target) if quote and target is not None else None,
                "signed_price_change": str(change) if change is not None else None,
                "reason": (
                    None
                    if quote
                    else "fill_timestamp_missing"
                    if target is None
                    else "target_not_observed_yet"
                    if pending
                    else "no_causal_fresh_quote"
                ),
            }
        )
        if quote is not None and change is not None:
            priced += anchor.quantity
            weighted += anchor.quantity * change
            mid_weighted += anchor.quantity * quote.mid
            reference += anchor.quantity * anchor.price
    complete = bool(anchors) and all(row["sample"] is not None for row in observations)
    return {
        "horizon_ms": horizon_ms,
        "max_age_ms": max_age,
        "basis": (
            "quantity-weighted per-fill midpoint markout; post-exit quotes are diagnostic only"
        ),
        "samples": sum(row["sample"] is not None for row in observations),
        "missing": sum(row["sample"] is None for row in observations),
        "total_fills": len(anchors),
        "quantity_coverage": str(priced),
        "signed_price": _metric(
            weighted / priced if complete and priced else None,
            "signed(mid at each fill+h - that fill price), quantity weighted",
            "no_priced_fills" if not anchors else "one_or_more_fill_markouts_missing",
        ),
        "signed_bps": _metric(
            weighted / reference * 10000 if complete and reference else None,
            "signed markout value / execution value * 10000",
            "no_priced_fills" if not anchors else "one_or_more_fill_markouts_missing",
        ),
        "mid": _metric(
            mid_weighted / priced if complete and priced else None,
            "quantity-weighted midpoints at each execution-relative target",
        ),
        "observations": observations,
    }


def _curve(
    quotes: _Quotes,
    symbol: str,
    start: int | None,
    end: int | None,
    reference: Decimal | None,
    sign: Decimal,
) -> tuple[dict[str, Any], list[_Quote]]:
    reason = None
    if start is None or end is None or reference is None:
        reason = "exact_fill_lifetime_or_reference_missing"
    elif end <= start:
        reason = "nonpositive_or_reversed_position_lifetime"
    elif end - start > quotes.policy.max_curve_seconds * NS:
        reason = "holding_window_exceeds_bound_no_truncated_curve_claim"
    values: list[_Quote] = []
    if reason is None and start is not None and end is not None:
        values = quotes.within(symbol, start, end)
    if not values:
        reason = reason or "no_fresh_quotes_inside_position_lifetime"
    changes = [(sign * (quote.mid - reference), quote) for quote in values] if reference else []
    high = max(changes, key=lambda item: item[0]) if changes else None
    low = min(changes, key=lambda item: item[0]) if changes else None
    gaps: list[int] = []
    if start is not None and end is not None and end > start and reason is None:
        cursor = start
        for quote in sorted(values, key=lambda item: item.received):
            available = max(start, quote.received)
            expiry = min(end, quote.exchange + quotes.policy.max_quote_age_ms * MS)
            if available > cursor:
                gaps.append(available - cursor)
            cursor = max(cursor, expiry)
        if cursor < end:
            gaps.append(end - cursor)
    complete = bool(values) and reason is None and not gaps
    report = {
        "start_at": _time(start, "first independently timestamped execution"),
        "end_at": _time(end, "last independently timestamped exit execution, not closure polling"),
        "reference_price": _metric(
            reference, "first entry execution price; not final cumulative VWAP"
        ),
        "sample_count": len(values),
        "complete_freshness_coverage": complete,
        "complete_curve_reason": None if complete else reason or "uncovered_freshness_intervals",
        "coverage_basis": (
            "union of [receipt, exchange+max_age] intervals inside actual lifetime; "
            "complete means bounded-age coverage, not continuous exchange/MBO truth"
        ),
        "uncovered_intervals": len(gaps) if reason is None else None,
        "uncovered_ms": str(D(sum(gaps)) / MS) if reason is None else None,
        "max_uncovered_ms": str(D(max(gaps, default=0)) / MS) if reason is None else None,
        "mfe_signed_price": _metric(
            high[0] if high else None, "observed signed maximum, not clamped", reason or ""
        ),
        "mae_signed_price": _metric(
            low[0] if low else None, "observed signed minimum, not clamped", reason or ""
        ),
        "mfe_bps": _metric(
            high[0] / reference * 10000 if high and reference else None,
            "observed MFE bps",
            reason or "",
        ),
        "mae_bps": _metric(
            low[0] / reference * 10000 if low and reference else None,
            "observed MAE bps",
            reason or "",
        ),
        "complete_mfe_bps": _metric(
            high[0] / reference * 10000 if complete and high and reference else None,
            "bounded-age complete holding curve",
            reason or "uncovered_freshness_intervals",
        ),
        "complete_mae_bps": _metric(
            low[0] / reference * 10000 if complete and low and reference else None,
            "bounded-age complete holding curve",
            reason or "uncovered_freshness_intervals",
        ),
        "mfe_quote": high[1].report(high[1].received) if high else None,
        "mae_quote": low[1].report(low[1].received) if low else None,
    }
    return report, values


class QuoteTimeline:
    """Immutable bounded quote index shared by offline, replay, and live diagnostics.

    Quote primitives contain symbol, bid, ask, exchange_at_ns and received_at_ns. Sizes,
    event_id, batch_id and source are optional. Missing source is ``provided_quote``, never
    asserted native data. Quote parsing, causal selection, markout arithmetic and signed
    excursions reuse the period-reconstruction kernels.

    ``markouts`` describes ONE incremental execution/revision, not an entire eventual order.
    Supply its actual execution timestamp/price/quantity and a stable fill_id, for example
    original client ID plus fill revision. Receipt proxies must not masquerade as fill times.
    ``excursions`` requires the actual first-fill/last-exit window and first execution price.
    Missing numeric/timestamp evidence returns None with reasons; malformed side, identity,
    policy, table or explicit watermark arguments raise ValueError.

    No method reads a clock, mutates input, executes SQL, or performs I/O. A new index is
    needed when its quote snapshot or observation watermark changes.
    """

    def __init__(
        self,
        quotes: Sequence[Mapping[str, Any]],
        *,
        observed_through_ns: int | None = None,
        policy: DiagnosticPolicy | None = None,
    ) -> None:
        self.policy = policy or DiagnosticPolicy()
        rows = _rows({"quotes": quotes}, "quotes", self.policy.max_quotes)
        self._quotes = _Quotes(
            [{**row, "source": row.get("source") or "provided_quote"} for row in rows],
            self.policy,
        )
        watermark = _ns(observed_through_ns)
        if observed_through_ns is not None and watermark is None:
            raise ValueError("observed_through_ns must be a valid nonnegative timestamp")
        self.observed_through_ns = (
            watermark
            if observed_through_ns is not None
            else max(
                (quote.received for values in self._quotes.by_symbol.values() for quote in values),
                default=None,
            )
        )
        self.watermark_basis = (
            "explicit observation watermark"
            if observed_through_ns is not None
            else "latest valid quote receipt; no later observation is assumed"
        )

    @staticmethod
    def _sign(side: str) -> Decimal:
        if side in {"buy", "long"}:
            return D(1)
        if side in {"sell", "short"}:
            return D(-1)
        raise ValueError("side must be buy, sell, long, or short")

    def _observation_reason(self, target: int | None) -> str | None:
        if target is None or not 0 <= target <= MAX_TIMESTAMP_NS:
            return "target_timestamp_missing_or_invalid"
        if self.observed_through_ns is None:
            return "observation_watermark_unavailable"
        if target > self.observed_through_ns:
            return "target_not_observed_yet"
        return None

    def sample(
        self, symbol: str, target_at_ns: int | None, *, horizon_ms: int | None = None
    ) -> dict[str, Any]:
        """Return an as-of quote and provenance, never a nearest/future observation."""
        if horizon_ms is not None and (
            isinstance(horizon_ms, bool) or not isinstance(horizon_ms, int) or horizon_ms < 0
        ):
            raise ValueError("horizon_ms must be a nonnegative integer or None")
        max_age = min(self.policy.max_quote_age_ms, horizon_ms or self.policy.max_quote_age_ms)
        target = _ns(target_at_ns)
        reason = self._observation_reason(target)
        quote = self._quotes.at(_symbol(symbol), target, max_age) if reason is None else None
        with localcontext() as context:
            context.prec = 50
            return {
                "schema": "scalp-quote-sample-v1",
                "symbol": _symbol(symbol),
                "target_at_ns": target,
                "target_at": _iso(target),
                "observed_through_ns": self.observed_through_ns,
                "watermark_basis": self.watermark_basis,
                "max_age_ms": max_age,
                "max_capture_delay_ms": self.policy.max_capture_delay_ms,
                "sample": quote.report(target) if quote and target is not None else None,
                "status": (
                    "observed"
                    if quote
                    else "pending"
                    if reason == "target_not_observed_yet"
                    else "missing"
                ),
                "reason": None if quote else reason or "no_causal_fresh_quote",
            }

    def markouts(
        self,
        symbol: str,
        *,
        fill_at_ns: int | None,
        fill_price: Decimal | str | None,
        quantity: Decimal | str | None,
        side: str,
        fill_id: str,
        exit_at_ns: int | None = None,
    ) -> dict[str, Any]:
        """Return all seven execution-relative horizons, including pending/missing facts."""
        sign = self._sign(side)
        if not isinstance(fill_id, str) or not fill_id:
            raise ValueError("fill_id must be a nonempty original execution/revision identity")
        at, price, qty = _ns(fill_at_ns), _decimal(fill_price), _decimal(quantity)
        input_reason = (
            "fill_price_missing_or_invalid"
            if price is None or price <= 0
            else "fill_quantity_missing_or_invalid"
            if qty is None or qty <= 0
            else None
        )
        anchors = (
            [_FillAnchor(at, price, qty, fill_id, "caller-provided incremental fill revision")]
            if price is not None and price > 0 and qty is not None and qty > 0
            else []
        )
        result = {}
        with localcontext() as context:
            context.prec = 50
            for name, horizon in zip(HORIZON_NAMES, HORIZONS_MS, strict=True):
                target = at + horizon * MS if at is not None else None
                observation_reason = self._observation_reason(target)
                fact = _markout(
                    anchors,
                    _symbol(symbol),
                    sign,
                    horizon,
                    self._quotes,
                    _ns(exit_at_ns),
                    observed_through_ns=self.observed_through_ns,
                )
                reason = input_reason or observation_reason or fact["signed_bps"]["reason"]
                if reason is not None:
                    for field in ("signed_price", "signed_bps", "mid"):
                        if fact[field]["value"] is None:
                            fact[field]["reason"] = reason
                fact.update(
                    {
                        "schema": "scalp-fill-markout-v1",
                        "fill_id": fill_id,
                        "target_at_ns": target,
                        "observed_through_ns": self.observed_through_ns,
                        "watermark_basis": self.watermark_basis,
                        "status": (
                            "observed"
                            if fact["signed_bps"]["value"] is not None
                            else "pending"
                            if observation_reason == "target_not_observed_yet"
                            else "missing"
                        ),
                        "reason": reason,
                        "request": {
                            "fill_at_ns": at,
                            "fill_price": _metric(price, "provided execution"),
                            "quantity": _metric(qty, "provided incremental execution quantity"),
                            "side": side,
                        },
                    }
                )
                result[name] = fact
        return result

    def excursions(
        self,
        symbol: str,
        *,
        opened_at_ns: int | None,
        closed_at_ns: int | None,
        entry_price: Decimal | str | None,
        side: str,
    ) -> dict[str, Any]:
        """Return signed observed extrema; never crop an unfinished or over-bound lifetime."""
        sign = self._sign(side)
        start, end, price = _ns(opened_at_ns), _ns(closed_at_ns), _decimal(entry_price)
        if price is not None and price <= 0:
            price = None
        observation_reason = self._observation_reason(end)
        with localcontext() as context:
            context.prec = 50
            report, _ = _curve(
                self._quotes,
                _symbol(symbol),
                start,
                end if observation_reason is None else None,
                price,
                sign,
            )
            if observation_reason is not None:
                report["end_at"] = _time(end, "requested actual last-exit execution timestamp")
                report["complete_curve_reason"] = observation_reason
                for field in (
                    "mfe_signed_price",
                    "mae_signed_price",
                    "mfe_bps",
                    "mae_bps",
                    "complete_mfe_bps",
                    "complete_mae_bps",
                ):
                    report[field]["reason"] = observation_reason
            report.update(
                {
                    "schema": "scalp-excursions-v1",
                    "observed_through_ns": self.observed_through_ns,
                    "watermark_basis": self.watermark_basis,
                    "status": (
                        "pending"
                        if observation_reason == "target_not_observed_yet"
                        else "complete"
                        if report["complete_freshness_coverage"]
                        else "partial"
                        if report["sample_count"]
                        else "missing"
                    ),
                    "reason": report["complete_curve_reason"],
                }
            )
            return report


def _total(orders: Sequence[_Order], field: str) -> Decimal | None:
    values = [order.quantity if field == "quantity" else order.value for order in orders]
    if any(value is None for value in values):
        return None
    return sum((value for value in values if value is not None), ZERO)


def _leg(orders: Sequence[_Order], policy: DiagnosticPolicy) -> dict[str, Any]:
    quantity, value = _total(orders, "quantity"), _total(orders, "value")
    return {
        "quantity": _metric(quantity, "sum of original owned order cumulative fills"),
        "value": _metric(value, "sum of owned order filled_quantity * filled_VWAP"),
        "weighted_price": _metric(
            value / quantity if quantity and value is not None else None,
            "independent fill value / filled quantity",
            "no_complete_positive_priced_fills",
        ),
        "orders": [order.report(policy) for order in orders],
    }


def _fee_total(orders: Sequence[_Order], field: str) -> Decimal:
    values = (
        item.quantity if field == "base" else item.cash
        for order in orders
        if not order.issues
        for item in order.fees
    )
    return -sum((value for value in values if value is not None), ZERO)


def _pnl(
    cycle: Mapping[str, Any],
    entries: Sequence[_Order],
    exits: Sequence[_Order],
    sign: Decimal,
    closed: bool,
    policy: DiagnosticPolicy,
) -> tuple[dict[str, Any], dict[str, Any]]:
    payload = _mapping(cycle.get("payload"))
    config = _mapping(payload.get("config"))
    eq, ev = _total(entries, "quantity"), _total(entries, "value")
    xq, xv = _total(exits, "quantity"), _total(exits, "value")
    entry_price = ev / eq if eq and ev is not None else None
    exit_price = xv / xq if xq and xv is not None else None
    common = min(eq, xq) if eq is not None and xq is not None else None
    raw = (
        sign * (exit_price - entry_price) * common
        if entry_price is not None and exit_price is not None and common is not None
        else None
    )
    cashflow = sign * (xv - ev) if ev is not None and xv is not None else None
    buys, sells = (entries, exits) if sign > 0 else (exits, entries)
    buy_qty, sell_qty = _total(buys, "quantity"), _total(sells, "quantity")
    buy_value = _total(buys, "value")
    buy_price = buy_value / buy_qty if buy_qty and buy_value is not None else None
    filled_buys = [order for order in buys if order.quantity is not None and order.quantity > 0]
    filled_sells = [order for order in sells if order.quantity is not None and order.quantity > 0]
    all_filled = filled_buys + filled_sells
    missing_buys = [order for order in filled_buys if not order.fees]
    missing_sells = [order for order in filled_sells if not order.fees]
    fee_count = sum(len(order.fees) for order in all_filled)
    cash_fees, base_fees = _fee_total(all_filled, "cash"), _fee_total(all_filled, "base")
    complete = bool(all_filled) and all(order.fees and not order.issues for order in all_filled)
    inventory_difference = (
        buy_qty - sell_qty if buy_qty is not None and sell_qty is not None else None
    )
    owned = _decimal(cycle.get("owned_quantity"))
    unattributed = (
        inventory_difference - base_fees - owned
        if inventory_difference is not None and owned is not None
        else None
    )
    quantity_accounted = (
        inventory_difference is not None
        and abs(inventory_difference - base_fees) <= policy.quantity_tolerance
    )
    maker, taker = _decimal(config.get("maker_fee_bps")), _decimal(config.get("taker_fee_bps"))
    rates_valid = maker is not None and taker is not None and maker >= 0 and taker >= 0
    missing_buy_qty = _total(missing_buys, "quantity")
    missing_sell_value = _total(missing_sells, "value")
    base_reserve: Decimal | None = ZERO if not missing_buys else None
    cash_reserve: Decimal | None = ZERO if not missing_sells else None
    if missing_buys and rates_valid and missing_buy_qty is not None and unattributed is not None:
        assert maker is not None and taker is not None
        base_reserve = max(
            ZERO, missing_buy_qty * max(maker, taker) / 10000 - max(ZERO, unattributed)
        )
    if missing_sells and taker is not None and taker >= 0 and missing_sell_value is not None:
        cash_reserve = missing_sell_value * taker / 10000
        inferred = _decimal(payload.get("inferred_account_cash_fee_allocation"))
        if inferred is not None:
            cash_reserve = max(cash_reserve, inferred)
    base_reserve_value = (
        base_reserve * buy_price
        if base_reserve is not None and buy_price is not None
        else ZERO
        if base_reserve == 0
        else None
    )
    priced = all(
        order.quantity is not None and order.value is not None for order in [*entries, *exits]
    )
    model = (
        cashflow - cash_fees - cash_reserve - base_reserve_value
        if closed
        and priced
        and cashflow is not None
        and cash_reserve is not None
        and base_reserve_value is not None
        else None
    )
    actual = (
        cashflow - cash_fees
        if closed and priced and complete and quantity_accounted and cashflow is not None
        else None
    )
    actual_reason = (
        "position_not_closed"
        if not closed
        else "broker_keyed_fee_coverage_incomplete"
        if not complete
        else "unexplained_inventory_difference_or_missing_fills"
    )
    fees = {
        "basis": (
            "CFEE/FEE activities must match broker ID, cycle, side, symbol and USD cash currency; "
            "a zero observed subtotal without records does not establish zero actual fees"
        ),
        "confirmed_fee_records": fee_count,
        "filled_orders": len(all_filled),
        "orders_with_confirmed_fee_records": sum(bool(order.fees) for order in all_filled),
        "complete_order_fee_coverage": complete,
        "confirmation_limit": (
            "coverage establishes recorded order-keyed fees, not unseen later postings"
        ),
        "observed_confirmed_cash_subtotal": str(cash_fees),
        "observed_confirmed_base_quantity_subtotal": str(base_fees),
        "actual_cash_fees": _metric(
            cash_fees if complete else None, "order-keyed fee activities", actual_reason
        ),
        "actual_base_fees_quantity": _metric(
            base_fees if complete else None, "order-keyed base-currency fees", actual_reason
        ),
        "entry_fee": _metric(
            _fee_total(entries, "cash")
            if entries and all(order.fees for order in entries)
            else None,
            "confirmed entry cash fees, excluding base units",
            "entry_fee_coverage_incomplete",
        ),
        "exit_fee": _metric(
            _fee_total(exits, "cash") if exits and all(order.fees for order in exits) else None,
            "confirmed exit cash fees",
            "exit_fee_coverage_incomplete",
        ),
        "entry_base_fee_quantity": _metric(
            _fee_total(entries, "base")
            if entries and all(order.fees for order in entries)
            else None,
            "confirmed entry base-currency fees",
            "entry_fee_coverage_incomplete",
        ),
        "exit_base_fee_quantity": _metric(
            _fee_total(exits, "base") if exits and all(order.fees for order in exits) else None,
            "confirmed exit base-currency fees",
            "exit_fee_coverage_incomplete",
        ),
        "inventory_difference_quantity": _metric(
            inventory_difference, "buy quantity minus sell quantity; not automatically a fee"
        ),
        "unattributed_inventory_reduction_quantity": _metric(
            unattributed, "buy qty - sell qty - observed confirmed base fees - owned qty"
        ),
        "quantity_difference_value_already_in_cashflow": _metric(
            raw - cashflow if raw is not None and cashflow is not None else None,
            "raw common-quantity price P&L minus cashflow; not an additional charge or proven fee",
        ),
        "confirmed_base_fee_reference_value_not_deducted": _metric(
            base_fees * buy_price if buy_price is not None and fee_count else None,
            "base fee quantity at buy VWAP; explanatory only, already absent from sell proceeds",
            "no_confirmed_fee_records_or_buy_price",
        ),
        "estimated_cash_reserve": _metric(
            cash_reserve,
            "unconfirmed sell value * taker bps, floored by recorded inferred cash allocation",
            "missing_fee_rate_or_priced_fills",
        ),
        "configured_maker_fee_bps": _metric(maker, "frozen per-cycle fee assumption"),
        "configured_taker_fee_bps": _metric(taker, "frozen per-cycle fee assumption"),
        "estimated_base_reserve_quantity": _metric(
            base_reserve,
            "unconfirmed buy qty * max(maker,taker) bps less already-unattributed coin reduction",
            "missing_fee_rate_inventory_or_priced_fills",
        ),
        "estimated_base_reserve_value": _metric(
            base_reserve_value,
            "additional base reserve quantity * buy VWAP",
            "missing_base_reserve_or_buy_price",
        ),
        "inferred_cash_allocation": _metric(
            _decimal(payload.get("inferred_account_cash_fee_allocation")),
            "stored account-level allocation, not a confirmed order-keyed fee",
        ),
        "inferred_base_allocation": _metric(
            _decimal(payload.get("inferred_account_base_fee_allocation")),
            "stored account-level allocation, not a confirmed order-keyed fee",
        ),
        "stored_cash_reserve": _metric(
            _decimal(payload.get("pending_cash_fee_reserve")), "stored reserve, comparison only"
        ),
        "stored_base_reserve_quantity": _metric(
            _decimal(payload.get("pending_base_fee_reserve_quantity")),
            "stored reserve, comparison only",
        ),
        "cash_reserve_difference": _delta(
            cash_reserve, _decimal(payload.get("pending_cash_fee_reserve")), policy.money_tolerance
        ),
        "base_reserve_quantity_difference": _delta(
            base_reserve,
            _decimal(payload.get("pending_base_fee_reserve_quantity")),
            policy.quantity_tolerance,
        ),
        "spread_deducted_again": False,
        "confirmed_base_fee_deducted_again": False,
        "estimated_fee_applied_to_confirmed_orders": False,
    }
    pnl = {
        "realized": closed,
        "common_quantity": _metric(common, "min(entry quantity, exit quantity)"),
        "raw_price_pnl": _metric(
            raw if closed else None,
            "signed(exit VWAP - entry VWAP) * common quantity",
            "position_not_closed" if not closed else "incomplete_priced_fills",
        ),
        "gross_cash_flow": _metric(
            cashflow if closed else None,
            "sell execution value - buy execution value",
            "position_not_closed" if not closed else "incomplete_priced_fills",
        ),
        "cash_flow_to_date_not_realized": _metric(
            cashflow, "execution cash flows including open inventory; not necessarily realized P&L"
        ),
        "known_cash_adjusted_subtotal": _metric(
            cashflow - cash_fees if closed and cashflow is not None else None,
            "cashflow less observed order-keyed cash fees; missing fees still unknown",
            "position_not_closed_or_incomplete_fills",
        ),
        "actual_net_pnl": _metric(
            actual,
            "cashflow less confirmed cash fees, with all filled orders fee-covered",
            actual_reason,
        ),
        "modeled_net_pnl": _metric(
            model,
            "cashflow - confirmed cash fees - unmatched cash/base reserves",
            "position_not_closed" if not closed else "missing_model_inputs_or_priced_fills",
        ),
        "stored_actual_net_pnl": _metric(_decimal(cycle.get("actual_net_pnl")), "stored cycle"),
        "stored_modeled_net_pnl": _metric(_decimal(cycle.get("modeled_net_pnl")), "stored cycle"),
        "spread_cost": _metric(
            None,
            "separate realized spread component",
            "execution prices already include spread; no independently identified incremental cost",
        ),
        "estimated_slippage": _metric(
            None, "incremental slippage cost", "no_recorded_independent_slippage_estimate"
        ),
        "estimated_market_impact": _metric(
            None, "incremental market impact", "no_recorded_independent_impact_estimate"
        ),
        "modeled_adverse_selection_cost": _metric(
            None, "a cost estimate is not a markout", "not_recorded"
        ),
        "modeled_opportunity_cost": _metric(
            None, "unfilled returns are not opportunity-cost estimates", "not_recorded"
        ),
    }
    return pnl, fees


def _reconciliation(
    cycle: Mapping[str, Any],
    entry: Mapping[str, Any],
    exit_leg: Mapping[str, Any],
    pnl: Mapping[str, Any],
    fees: Mapping[str, Any],
    policy: DiagnosticPolicy,
) -> dict[str, Any]:
    order_reports = list(entry["orders"]) + list(exit_leg["orders"])
    filled = [
        row
        for row in order_reports
        if (quantity := _value(row["filled_quantity"])) is not None and quantity > 0
    ]
    independent = bool(filled) and all(row["independent_fill_crosscheck"] for row in filled)
    comparisons = {
        "entry_quantity": _delta(
            _value(entry["quantity"]),
            _decimal(cycle.get("entry_quantity")),
            policy.quantity_tolerance,
        ),
        "entry_value": _delta(
            _value(entry["value"]), _decimal(cycle.get("entry_value")), policy.money_tolerance
        ),
        "exit_quantity": _delta(
            _value(exit_leg["quantity"]),
            _decimal(cycle.get("exit_quantity")),
            policy.quantity_tolerance,
        ),
        "exit_value": _delta(
            _value(exit_leg["value"]), _decimal(cycle.get("exit_value")), policy.money_tolerance
        ),
        "gross_cash_flow": _delta(
            _value(pnl["gross_cash_flow"]),
            _decimal(cycle.get("gross_cash_flow")),
            policy.money_tolerance,
        ),
        "actual_net_pnl": _delta(
            _value(pnl["actual_net_pnl"]),
            _decimal(cycle.get("actual_net_pnl")),
            policy.money_tolerance,
        ),
        "modeled_net_pnl": _delta(
            _value(pnl["modeled_net_pnl"]),
            _decimal(cycle.get("modeled_net_pnl")),
            policy.money_tolerance,
        ),
    }
    proven = [
        key
        for key, comparison in comparisons.items()
        if key != "modeled_net_pnl" and independent and comparison["within_tolerance"] is False
    ]
    proven.extend(
        "stored_order:" + row["client_order_id"]
        for row in filled
        if row["proven_stored_fill_mismatch"]
    )
    return {
        "filled_orders": len(filled),
        "historical_orders_checked": sum(row["historical_coverage"] for row in filled),
        "fill_activity_orders_checked": sum(row["fill_activity_coverage"] for row in filled),
        "independent_orders_checked": sum(row["independent_fill_crosscheck"] for row in filled),
        "all_filled_orders_independently_crosschecked": independent,
        "differences_reconstructed_minus_stored": comparisons,
        "proven_inconsistencies": proven,
        "accounting_failure_proven": bool(proven),
        "fee_comparison_complete": fees["complete_order_fee_coverage"],
        "reason": None
        if independent
        else "historical_and_matching_incremental_fill_coverage_incomplete",
        "interpretation": (
            "model/reserve differences alone are not accounting failures; "
            "unknown fees are not bugs; "
            "a canceled order with positive cumulative executions is still a partial fill"
        ),
    }


def _prediction(signal: Mapping[str, Any]) -> dict[str, Any]:
    recorded = _mapping(signal.get("prediction"))
    result: dict[str, Any] = {}
    fields = {
        "predicted_move_bps": (
            "predicted_move_bps",
            "expected_move_bps",
            "expected_gross_move_bps",
        ),
        "predicted_horizon_seconds": ("predicted_horizon_seconds", "prediction_horizon_seconds"),
        "expected_net_edge_bps": ("expected_net_edge_bps", "predicted_net_edge_bps"),
        "predicted_fill_probability": ("predicted_fill_probability", "fill_probability"),
        "confidence": ("confidence",),
        "uncertainty": ("uncertainty",),
    }
    for output, aliases in fields.items():
        value = None
        source = "explicit prediction telemetry"
        for container, name in ((recorded, "signal.prediction"), (signal, "signal")):
            for alias in aliases:
                if container.get(alias) is not None:
                    value = _decimal(container[alias])
                    source = f"{name}.{alias}"
                    break
            if value is not None:
                break
        if output in {"predicted_fill_probability", "confidence"} and value is not None:
            value = value if ZERO <= value <= 1 else None
        if output == "predicted_horizon_seconds" and value is not None and value <= 0:
            value = None
        result[output] = _metric(
            value, source, "not_recorded_as_a_prediction; heuristic_score_is_not_a_probability"
        )
    result["predicted_direction"] = _metric(
        recorded.get("predicted_direction", signal.get("predicted_direction")),
        "explicit prediction telemetry",
        "directional_action_is_not_a_recorded_prediction",
    )
    result["validated"] = (
        recorded.get("validated") is True or signal.get("prediction_validated") is True
    )
    result["heuristic_score"] = _metric(
        _decimal(signal.get("score")), "recorded heuristic only; not calibrated confidence"
    )
    result["calibration_reason"] = (
        None if result["validated"] else "prediction_validation_not_recorded"
    )
    return result


def _features(signal: Mapping[str, Any]) -> dict[str, Any]:
    recorded = _mapping(signal.get("features"))
    aliases = {
        "OFI": "normalized_ofi_5s",
        "queue_imbalance": "l1_imbalance",
        "aggressor_delta": "taker_delta_5s",
        "microprice_displacement": "microprice_displacement_bps",
        "spread": "spread_bps",
        "bid_depth": "bid_depth_l5",
        "ask_depth": "ask_depth_l5",
        "trade_intensity": "trade_intensity_5s",
        "cancel_intensity": "cancel_intensity",
        "volatility": "volatility_5s_bps",
        "short_horizon_return_1s": "return_1s_bps",
        "short_horizon_return_5s": "return_5s_bps",
        "queue_position": "queue_position",
        "queue_ahead": "queue_ahead",
    }
    return {
        "recorded": _json_value(recorded),
        "requested": {
            name: _metric(
                _decimal(recorded.get(key)),
                f"recorded features.{key}",
                "not_recorded; aggregate_removals_do_not_establish_cancellations_or_MBO_queue",
            )
            for name, key in aliases.items()
        },
        "depth_basis": "aggregate recorded depth, not individual orders or an exact queue",
    }


def _eastern(instant: int | None) -> dict[str, Any]:
    if instant is None:
        return {"local_time": None, "hour_bucket": "UNKNOWN", "reason": "decision_time_missing"}
    value = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(seconds=instant // NS)
    if value.year < 2007:
        return {
            "local_time": None,
            "hour_bucket": "UNKNOWN",
            "reason": "portable_US_Eastern_rules_only_supported_from_2007",
        }
    march = datetime(value.year, 3, 8, 7, tzinfo=UTC)
    november = datetime(value.year, 11, 1, 6, tzinfo=UTC)
    start = march + timedelta(days=(6 - march.weekday()) % 7)
    end = november + timedelta(days=(6 - november.weekday()) % 7)
    offset = 4 if start <= value < end else 5
    local = value - timedelta(hours=offset)
    return {
        "local_time": local.strftime("%Y-%m-%dT%H:%M:%S") + f"-0{offset}:00",
        "hour_bucket": f"{local.hour:02d}:00-{(local.hour + 1) % 24:02d}:00 ET",
        "zone": "EDT" if offset == 4 else "EST",
        "basis": "US Eastern DST rules from 2007; no system tzdata dependency",
        "reason": None,
    }


def _style(
    entries: Sequence[_Order], signal: Mapping[str, Any], config: Mapping[str, Any]
) -> dict[str, Any]:
    styles = []
    for order in entries:
        intent = _mapping(order.row.get("intent"))
        request = _mapping(intent.get("request"))
        quote = _mapping(intent.get("quote")) or _mapping(signal.get("quote"))
        limit = _decimal(intent.get("limit_price"))
        bid, ask = _decimal(quote.get("bid")), _decimal(quote.get("ask"))
        kind = request.get("order_type")
        if kind == "market":
            styles.append("aggressive")
        elif (
            kind == "limit"
            and limit is not None
            and bid is not None
            and ask is not None
            and bid <= ask
        ):
            crossing = limit >= ask if order.side == "buy" else limit <= bid
            styles.append("aggressive" if crossing else "passive")
        else:
            styles.append("UNKNOWN")
    basis = "original order type and limit vs original BBO; intent, not proven maker/taker fill"
    if not styles and config.get("entry_style") in {"passive", "marketable"}:
        styles = ["passive" if config["entry_style"] == "passive" else "aggressive"]
        basis = "configured intent only; no owned order recorded"
    style = styles[0] if styles and len(set(styles)) == 1 else "mixed" if styles else "UNKNOWN"
    return {
        "entry_style": style,
        "basis": basis,
        "actual_maker_taker": _metric(
            None,
            "broker execution liquidity flag",
            "not_recorded; passive_intent_is_not_maker_proof",
        ),
    }


def _stage_difference(
    start: int | None, end: int | None, basis: str, *, proxy: bool = False
) -> dict[str, Any]:
    value = D(end - start) / MS if start is not None and end is not None and end >= start else None
    reason = (
        "end_precedes_start_cross_clock_or_fill_before_ack"
        if start is not None and end is not None
        else "stage_timestamp_missing"
    )
    return {
        **_metric(value, basis, reason),
        "measurement_kind": "proxy" if proxy else "recorded_stage",
        "units": "milliseconds",
    }


def _timing(
    cycle: Mapping[str, Any],
    signal: Mapping[str, Any],
    entries: Sequence[_Order],
    exits: Sequence[_Order],
    anchors: Sequence[_FillAnchor],
    exit_anchors: Sequence[_FillAnchor],
    evidence: _Evidence,
) -> dict[str, Any]:
    decision = _ns(signal.get("observed_at"))
    stages: dict[str, int] = {}
    conflicts: set[str] = set()
    sources = [_mapping(signal.get("timing"))]
    first_order = min(
        entries, key=lambda order: _ns(order.row.get("created_at")) or 0, default=None
    )
    audits = evidence.audits_by_client.get(first_order.client_id, []) if first_order else []
    for audit in audits:
        if audit.get("event_type") in {"scalp_dispatch_timing", "scalp_fill_timing"}:
            sources.append(_mapping(_mapping(audit.get("payload")).get("timing")))
    for source in sources:
        for key, value in source.items():
            parsed = _ns(value)
            if parsed is not None:
                if key in stages and stages[key] != parsed:
                    conflicts.add(key)
                stages[key] = parsed
    for key in conflicts:
        stages.pop(key, None)
    has_model_time = "t4_model_ns" in stages
    decision = stages.get("t4_model_ns", decision)
    exact_first = (
        min(anchor.at for anchor in anchors if anchor.at is not None)
        if anchors
        and all(anchor.at is not None and anchor.basis == "FILL activity" for anchor in anchors)
        else None
    )
    completion = (
        max(anchor.at for anchor in anchors if anchor.at is not None)
        if anchors and all(anchor.at is not None for anchor in anchors)
        else None
    )
    exit_fill = (
        max(anchor.at for anchor in exit_anchors if anchor.at is not None)
        if exit_anchors and all(anchor.at is not None for anchor in exit_anchors)
        else None
    )
    dispatch = _ns(first_order.row.get("submission_started_at")) if first_order else None
    observations = [
        _ns(_mapping(audit.get("payload")).get("source_received_at", audit.get("occurred_at")))
        for audit in audits
        if audit.get("event_type") == "scalp_broker_order_observation"
    ]
    observed_ack = min((value for value in observations if value is not None), default=None)
    exit_audits = evidence.audits_by_cycle.get(str(cycle["cycle_id"]), [])
    exit_decisions = [
        _ns(audit.get("occurred_at"))
        for audit in exit_audits
        if audit.get("event_type") == "scalp_exit_requested"
    ]
    exit_decision = min((value for value in exit_decisions if value is not None), default=None)
    exit_dispatch = min(
        (
            value
            for order in exits
            if (value := _ns(order.row.get("submission_started_at"))) is not None
        ),
        default=None,
    )
    exit_send_values = [
        _ns(_mapping(_mapping(audit.get("payload")).get("timing")).get("t6_submitted_ns"))
        for order in exits
        for audit in evidence.audits_by_client.get(order.client_id, [])
        if audit.get("event_type") == "scalp_dispatch_timing"
    ]
    exit_send = min((value for value in exit_send_values if value is not None), default=None)
    timestamps = {
        "signal_time": _time(_ns(signal.get("observed_at")), "original signal.observed_at"),
        "decision_time": _time(
            decision,
            "t4_model_ns" if has_model_time else "signal.observed_at; model-completion proxy",
        ),
        "send_time": _time(
            stages.get("t6_submitted_ns"), "measured t6 immediately before broker POST"
        ),
        "broker_ack_time": _time(
            stages.get("t7_acknowledged_ns"), "measured local POST response receipt"
        ),
        "fill_time": _time(
            exact_first, "first FILL activity", "first_execution_time_not_independently_known"
        ),
        "entry_completion_time": _time(completion, "last entry execution/completion timestamp"),
        "exit_decision_time": _time(exit_decision, "scalp_exit_requested audit"),
        "exit_send_time": _time(exit_send, "measured t6 before exit POST"),
        "exit_fill_time": _time(exit_fill, "last exit execution/completion timestamp"),
        "dispatch_started_at": _time(dispatch, "durable entry dispatch marker, not wire-send time"),
        "first_broker_observation_at": _time(
            observed_ack, "first locally observed broker order; ack upper bound"
        ),
        "exit_dispatch_started_at": _time(exit_dispatch, "durable exit dispatch marker"),
        "stored_opened_at": _time(
            _ns(cycle.get("opened_at")), "stored opening timestamp; may use receipt fallback"
        ),
        "stored_closed_at": _time(
            _ns(cycle.get("closed_at")), "closure verification/polling, not execution"
        ),
        "intended_exit_at": _time(
            _ns(cycle.get("exit_due_at")), "persisted strategy timer, not a predicted horizon"
        ),
    }
    latency = {
        name: _stage_difference(stages.get(first), stages.get(last), f"{first} -> {last}")
        for name, (first, last) in STAGES.items()
    }
    if latency["decision_fill"]["value"] is None and "t4_model_ns" not in conflicts:
        latency["decision_fill"] = _stage_difference(
            decision,
            exact_first,
            "signal observation -> first broker FILL; not model compute time",
            proxy=not has_model_time,
        )
    latency.update(
        {
            "decision_dispatch_start": _stage_difference(
                decision, dispatch, "decision anchor -> durable pre-POST dispatch start", proxy=True
            ),
            "dispatch_start_broker_observation": _stage_difference(
                dispatch,
                observed_ack,
                "durable dispatch -> first local order observation",
                proxy=True,
            ),
            "broker_observation_first_fill": _stage_difference(
                observed_ack,
                exact_first,
                "local broker observation -> broker FILL; mixed clocks",
                proxy=True,
            ),
            "exit_decision_dispatch_start": _stage_difference(
                exit_decision,
                exit_dispatch,
                "exit requested -> durable exit dispatch marker",
                proxy=True,
            ),
            "exit_dispatch_start_fill": _stage_difference(
                exit_dispatch,
                exit_fill,
                "durable exit dispatch -> last broker exit FILL",
                proxy=True,
            ),
        }
    )
    holding = (
        D(exit_fill - exact_first) / NS
        if exact_first is not None and exit_fill is not None and exit_fill >= exact_first
        else None
    )
    return {
        "timestamps": timestamps,
        "stages_ms": latency,
        "recorded_stages_ns": stages,
        "conflicting_stage_timestamps": sorted(conflicts),
        "holding_seconds": _metric(
            holding,
            "first actual entry execution -> last actual exit execution",
            "exact_fill_lifetime_missing_or_reversed",
        ),
        "feature_lookback_seconds_not_prediction": _metric(
            _decimal(
                _mapping(_mapping(cycle.get("payload")).get("config")).get(
                    "feature_horizon_seconds"
                )
            ),
            "frozen feature lookback only",
        ),
    }


def _union_length(intervals: Sequence[tuple[int, int]]) -> int:
    total = 0
    previous_end: int | None = None
    for start, end in sorted(intervals):
        start = max(start, previous_end) if previous_end is not None else start
        total += max(0, end - start)
        previous_end = max(end, previous_end) if previous_end is not None else end
    return total


def _backoff_analysis(
    cycle: Mapping[str, Any], timing: Mapping[str, Any], evidence: _Evidence
) -> dict[str, Any]:
    timestamps = timing["timestamps"]
    start = timestamps["fill_time"]["unix_ns"]
    end = timestamps["exit_fill_time"]["unix_ns"]
    due = timestamps["intended_exit_at"]["unix_ns"]
    requested = timestamps["exit_decision_time"]["unix_ns"]
    overlaps: list[tuple[int, int]] = []
    after_due: list[tuple[int, int]] = []
    events = []
    recoveries: Counter[str] = Counter()
    for audit in evidence.audits:
        payload = _mapping(audit.get("payload"))
        if not cycle.get("run_id") or payload.get("run_id") != cycle["run_id"]:
            continue
        at = _ns(audit.get("occurred_at"))
        if at is None or start is None or end is None:
            continue
        if audit.get("event_type") == "scalp_technical_recovery" and start <= at <= end:
            recoveries[str(payload.get("error", "unknown"))] += 1
        if audit.get("event_type") != "scalp_broker_backoff":
            continue
        retry = _ns(payload.get("retry_after"))
        if retry is None or retry <= at or retry <= start or at >= end:
            continue
        left, right = max(at, start), min(retry, end)
        overlaps.append((left, right))
        if due is not None and right > due:
            after_due.append((max(left, due), right))
        events.append(
            {
                "event_id": audit.get("event_id"),
                "run_id": payload.get("run_id"),
                "operation": payload.get("operation"),
                "status_code": payload.get("status_code"),
                "occurred_at": _iso(at),
                "retry_after": _iso(retry),
                "directive_duration_seconds": str(D(retry - at) / NS),
                "overlap_holding_seconds": str(D(right - left) / NS),
            }
        )
    delayed = max(0, end - due) if end is not None and due is not None else None
    union = _union_length(overlaps)
    union_after = _union_length(after_due)
    return {
        "intended_exit_at": timestamps["intended_exit_at"],
        "actual_exit_request_at": timestamps["exit_decision_time"],
        "actual_exit_fill_at": timestamps["exit_fill_time"],
        "deadline_to_exit_request_ms": _metric(
            D(requested - due) / MS if requested is not None and due is not None else None,
            "signed exit request minus persisted timer due; negative means early signal exit",
        ),
        "deadline_to_exit_fill_ms": _metric(
            D(end - due) / MS if end is not None and due is not None else None,
            "signed last execution minus persisted timer due",
        ),
        "backoff_directive_union_seconds": _metric(
            D(union) / NS if start is not None and end is not None else None,
            "union of same-run audited cooldown intervals intersecting actual holding lifetime",
        ),
        "backoff_after_deadline_seconds": _metric(
            D(union_after) / NS if delayed is not None else None,
            "cooldown union clipped to [intended exit, actual exit]",
        ),
        "delay_outside_documented_backoffs_seconds": _metric(
            D(max(0, delayed - union_after)) / NS if delayed is not None else None,
            "residual deadline overrun; request/loop/recovery contribution not separately measured",
        ),
        "backoff_events": events,
        "technical_recovery_counts_during_holding": dict(recoveries),
        "causality_limit": (
            "audits prove cooldown overlap and timer overrun, not lost alpha or the duration of "
            "unlogged HTTP requests; overlapping directives are counted once"
        ),
    }


def _decision_evidence(
    signal: Mapping[str, Any],
    symbol: str,
    decision: int | None,
    sign: Decimal | None,
    quotes: _Quotes,
) -> dict[str, Any]:
    recorded = _mapping(signal.get("quote"))
    original, invalid = quotes.parse(recorded)
    baseline = quotes.at(symbol, decision, quotes.policy.max_quote_age_ms)
    if (
        original is not None
        and original.symbol == symbol
        and decision is not None
        and original.received <= decision
        and original.exchange <= decision
        and decision - original.exchange <= quotes.policy.max_quote_age_ms * MS
        and (baseline is None or original.exchange > baseline.exchange)
    ):
        baseline = original
    bid, ask = _decimal(recorded.get("bid")), _decimal(recorded.get("ask"))
    exchange = _ns(recorded.get("exchange_time_ns", recorded.get("exchange_at")))
    received = _ns(recorded.get("received_at_ns", recorded.get("received_at")))
    outcomes = {}
    for name, horizon in zip(HORIZON_NAMES, HORIZONS_MS, strict=True):
        target = decision + horizon * MS if decision is not None else None
        quote = quotes.at(symbol, target, min(horizon, quotes.policy.max_quote_age_ms))
        value = (
            sign * (quote.mid - baseline.mid) / baseline.mid * 10000
            if baseline is not None and quote is not None and sign is not None
            else None
        )
        outcomes[name] = {
            "horizon_ms": horizon,
            "target_at_ns": target,
            "signed_return_bps": _metric(
                value,
                "signed future as-of midpoint return from the same decision anchor",
                "position_direction_not_recorded"
                if sign is None
                else "decision_baseline_missing_or_stale"
                if baseline is None
                else "no_causal_fresh_quote",
            ),
            "sample": quote.report(target) if quote and target is not None else None,
            "realizable_profit": False,
        }
    return {
        "decision_bid": _metric(bid, "original signal quote; not silently replaced with a fill"),
        "decision_ask": _metric(ask, "original signal quote"),
        "decision_mid": _metric(
            (bid + ask) / 2 if bid is not None and ask is not None and 0 < bid <= ask else None,
            "original signal midpoint",
        ),
        "original_quote_exchange_at": _time(exchange, "original signal quote"),
        "original_quote_received_at": _time(received, "original signal quote"),
        "original_quote_age_ms": _metric(
            D(decision - exchange) / MS if decision is not None and exchange is not None else None,
            "decision anchor minus original quote exchange time",
        ),
        "original_quote_capture_delay_ms": _metric(
            D(received - exchange) / MS if received is not None and exchange is not None else None,
            "original quote receipt minus exchange time",
        ),
        "original_quote_sampling_rejection": invalid or None,
        "comparison_baseline": baseline.report(decision)
        if baseline and decision is not None
        else None,
        "comparison_baseline_reason": None if baseline else "no_causal_fresh_decision_quote",
        "future_returns": outcomes,
        "comparison_limit": (
            "filled and unfilled groups share decision-relative horizons and age rules; "
            "unfilled midpoint returns are not attainable trading profit"
        ),
    }


def _net_excursion(
    entries: Sequence[_Order],
    exits: Sequence[_Order],
    anchors: Sequence[_FillAnchor],
    exit_anchors: Sequence[_FillAnchor],
    curve_quotes: Sequence[_Quote],
    fees: Mapping[str, Any],
    sign: Decimal,
    config: Mapping[str, Any],
) -> dict[str, Any]:
    result: Decimal | None = None
    best: _Quote | None = None
    best_exit_fee: Decimal | None = None
    reason = "complete_order_keyed_fees_and_single_entry_execution_required"
    eq, ev = _total(entries, "quantity"), _total(entries, "value")
    entry_cash_fees = _fee_total(entries, "cash")
    exit_cash_fees = _fee_total(exits, "cash")
    base_fees = _fee_total([*entries, *exits], "base")
    exit_value = _total(exits, "value")
    taker_bps = _decimal(config.get("taker_fee_bps"))
    first_exit = min((anchor.at for anchor in exit_anchors if anchor.at is not None), default=None)
    if (
        fees["complete_order_fee_coverage"]
        and len(anchors) == 1
        and eq is not None
        and ev is not None
        and exit_value is not None
        and exit_value > 0
        and first_exit is not None
        and (sign > 0 or base_fees == 0)
    ):
        quantity = eq - base_fees if sign > 0 else eq
        reason = "no_sufficient_size_fresh_quote_before_any_exit"
        for quote in curve_quotes:
            size = quote.bid_size if sign > 0 else quote.ask_size
            if quote.received > first_exit or size is None or size < quantity or quantity <= 0:
                continue
            price = quote.bid if sign > 0 else quote.ask
            quote_value = price * quantity
            exit_fee = max(ZERO, exit_cash_fees, exit_cash_fees * quote_value / exit_value)
            if taker_bps is not None and taker_bps >= 0:
                exit_fee = max(exit_fee, quote_value * taker_bps / 10000)
            candidate = sign * (quote_value - ev) - entry_cash_fees - exit_fee
            if result is None or candidate > result:
                result, best, best_exit_fee = candidate, quote, exit_fee
    return {
        "net_profitable_mfe": _metric(
            result,
            "displayed-size liquidation less actual entry cash fees "
            "and conservative exit fee floor",
            reason,
        ),
        "counterfactual_exit_cash_fee": _metric(
            best_exit_fee,
            "max(recorded exit fee, notional-scaled recorded exit fee, configured taker fee, zero)",
            "no_costed_liquidation_quote",
        ),
        "sample": best.report(best.received) if best else None,
        "basis": (
            "requires single known entry execution and complete explicit fee coverage; uses bid "
            "for long/ask for short, not midpoint. Counterfactual liquidity/fees are not guaranteed"
        ),
        "not_realized_profit": True,
    }


def _classify(
    report: Mapping[str, Any],
    signal: Mapping[str, Any],
    config: Mapping[str, Any],
    forecast_curve: Mapping[str, Any],
    policy: DiagnosticPolicy,
) -> dict[str, Any]:
    assessments: dict[str, dict[str, Any]] = {}

    def assess(
        label: str,
        condition: bool | None,
        reason: str,
        threshold: str,
        evidence: Mapping[str, Any],
    ) -> None:
        assessments[label] = {
            "status": "unknown"
            if condition is None
            else "supported"
            if condition
            else "not_observed",
            "reason": reason,
            "threshold": threshold,
            "evidence": dict(evidence),
        }

    prediction = report["prediction"]
    predicted_move = _value(prediction["predicted_move_bps"])
    predicted_horizon = _value(prediction["predicted_horizon_seconds"])
    expected_net = _value(prediction["expected_net_edge_bps"])
    meaningful = (
        prediction["validated"]
        and predicted_move is not None
        and predicted_move > 0
        and predicted_horizon is not None
        and 0 < predicted_horizon <= 10
    )
    forecast_mfe = _value(forecast_curve["mfe_bps"])
    alpha: bool | None = None
    if meaningful and forecast_curve["complete_freshness_coverage"] and forecast_mfe is not None:
        assert predicted_move is not None
        alpha = forecast_mfe < predicted_move
    assess(
        "ALPHA_FAILURE",
        alpha,
        "requires a validated magnitude/horizon prediction "
        "and complete bounded-age horizon coverage",
        "maximum signed decision-relative move < recorded positive predicted move",
        {
            "predicted_move_bps": prediction["predicted_move_bps"],
            "horizon_observed_mfe_bps": forecast_curve["mfe_bps"],
        },
    )
    pnl = report["pnl"]
    actual, modeled = _value(pnl["actual_net_pnl"]), _value(pnl["modeled_net_pnl"])
    net = actual if actual is not None else modeled
    net_basis = "actual_net_pnl" if actual is not None else "modeled_net_pnl"
    raw = _value(pnl["raw_price_pnl"])
    assess(
        "COST_FAILURE",
        raw > policy.money_tolerance and net < -policy.money_tolerance
        if raw is not None and net is not None
        else None,
        "positive common-quantity price P&L became negative "
        "after inventory/cash fee effects and disclosed reserves",
        f"raw_price_pnl > {policy.money_tolerance} and {net_basis} < -{policy.money_tolerance}",
        {"raw_price_pnl": pnl["raw_price_pnl"], "net_basis": net_basis, "net": pnl[net_basis]},
    )
    markouts = {
        horizon: _value(report["markouts"][name]["signed_bps"])
        for name, horizon in zip(HORIZON_NAMES, HORIZONS_MS, strict=True)
    }
    negative = [horizon for horizon, value in markouts.items() if value is not None and value < 0]
    adverse: bool | None = None
    if report["execution"]["entry_style"] != "passive":
        adverse = False if report["execution"]["entry_style"] == "aggressive" else None
    elif any(value is not None for horizon, value in markouts.items() if horizon <= 1000) and any(
        value is not None for horizon, value in markouts.items() if horizon >= 2000
    ):
        adverse = any(h <= 1000 for h in negative) and any(h >= 2000 for h in negative)
    assess(
        "ADVERSE_SELECTION",
        adverse,
        "unfavorable post-fill marks on passive-intent executions; "
        "evidence condition, not causal or maker/queue proof",
        "strictly negative markout at >=2 horizons, including <=1s and >=2s",
        {"entry_style": report["execution"], "negative_horizons_ms": negative},
    )
    backoff = report["backoff_analysis"]
    deadline_delay = _value(backoff["deadline_to_exit_fill_ms"])
    cooldown = _value(backoff["backoff_after_deadline_seconds"])
    decision_latency = _value(report["timing"]["stages_ms"]["decision_fill"])
    decision_mid = _value(report["decision"]["decision_mid"])
    entry_price = _value(report["entry"]["weighted_price"])
    sign = D(1) if report["side"] == "long" else D(-1)
    deterioration = (
        sign * (entry_price - decision_mid) / decision_mid * 10000
        if entry_price is not None and decision_mid is not None and decision_mid > 0
        else None
    )
    late_profitable_signal: bool | None = None
    if (
        meaningful
        and expected_net is not None
        and expected_net > 0
        and decision_latency is not None
        and deterioration is not None
        and predicted_horizon is not None
    ):
        late_profitable_signal = (
            decision_latency > predicted_horizon * 1000 and deterioration >= expected_net
        )
    operational_delay = (
        deadline_delay is not None
        and deadline_delay > policy.material_latency_ms
        and cooldown is not None
        and cooldown * 1000 > policy.material_latency_ms
    )
    assess(
        "LATENCY_FAILURE",
        True if operational_delay else late_profitable_signal,
        "documented exit-timer overrun overlapping cooldown, "
        "or validated positive-edge signal consumed before a late fill",
        f"timer overrun and cooldown overlap each > {policy.material_latency_ms}ms; "
        "no lost-alpha claim from duration alone",
        {
            "operational_deadline_failure": operational_delay,
            "entry_economic_latency_condition": late_profitable_signal,
            "deadline_overrun_ms": backoff["deadline_to_exit_fill_ms"],
            "cooldown_after_deadline_seconds": backoff["backoff_after_deadline_seconds"],
            "decision_to_fill_ms": report["timing"]["stages_ms"]["decision_fill"],
        },
    )
    execution: bool | None = None
    if (
        meaningful
        and forecast_mfe is not None
        and predicted_move is not None
        and net is not None
        and decision_latency is not None
        and predicted_horizon is not None
        and deterioration is not None
    ):
        execution = (
            forecast_mfe >= predicted_move
            and net < -policy.money_tolerance
            and (decision_latency > predicted_horizon * 1000 or deterioration >= predicted_move)
        )
    assess(
        "EXECUTION_FAILURE",
        execution,
        "requires a validated move observed within its horizon "
        "plus a losing late/edge-consuming execution",
        "observed horizon move >= prediction, negative net, "
        "and fill after horizon or entry shortfall >= predicted move",
        {
            "horizon_mfe_bps": forecast_curve["mfe_bps"],
            "entry_shortfall_bps": _metric(deterioration, "entry VWAP vs original decision mid"),
        },
    )
    net_mfe = _value(report["executable_excursion"]["net_profitable_mfe"])
    assess(
        "EXIT_FAILURE",
        net_mfe > policy.money_tolerance and net < -policy.money_tolerance
        if net_mfe is not None and net is not None
        else None,
        "requires a sufficient-size favorable liquidation quote "
        "after explicit fees before any exit; "
        "raw midpoint MFE is insufficient",
        f"net-costed observed MFE > {policy.money_tolerance} "
        f"and final net < -{policy.money_tolerance}",
        {"net_mfe": report["executable_excursion"], "net_basis": net_basis},
    )
    observed_regime = signal.get("observed_regime")
    regime: bool | None = None
    if signal.get("observed_regime_validated") is True and observed_regime in {
        "momentum",
        "reversion",
    }:
        regime = (
            report["family"] in {"momentum", "reversion"} and report["family"] != observed_regime
        )
    assess(
        "REGIME_FAILURE",
        regime,
        "family is not a trained regime label; "
        "an independently validated observed regime is required",
        "recorded validated observed regime contradicts entry family",
        {
            "entry_family": report["family"],
            "observed_regime": observed_regime,
            "observed_regime_validated": signal.get("observed_regime_validated"),
        },
    )
    quote = _mapping(signal.get("quote"))
    decision = report["timing"]["timestamps"]["decision_time"]["unix_ns"]
    exchange = _ns(quote.get("exchange_time_ns", quote.get("exchange_at")))
    received = _ns(quote.get("received_at_ns", quote.get("received_at")))
    threshold_seconds = _decimal(config.get("maximum_quote_age_seconds"))
    threshold_basis = "explicit frozen maximum_quote_age_seconds"
    if threshold_seconds is None:
        feature = _decimal(config.get("feature_horizon_seconds"))
        interval = _decimal(config.get("decision_interval_seconds"))
        if config.get("decision_policy", "legacy-v30") == "legacy-v30":
            threshold_seconds = feature
            threshold_basis = "legacy-v30 frozen feature-horizon freshness contract"
        elif feature is not None and interval is not None:
            threshold_seconds = min(feature, interval)
            threshold_basis = "action-value-v1 min(feature horizon, decision interval)"
    stale: bool | None = None
    if decision is not None and exchange is not None and received is not None:
        if exchange > received or received > decision:
            stale = True
        elif threshold_seconds is not None and threshold_seconds > 0:
            stale = D(decision - exchange) > threshold_seconds * NS
            send = report["timing"]["timestamps"]["send_time"]["unix_ns"]
            if send is not None:
                stale |= D(send - exchange) > threshold_seconds * NS
    assess(
        "STALE_DATA_FAILURE",
        stale,
        "tests the actually used original quote against frozen execution freshness, "
        "not the stricter research sampling bound",
        threshold_basis,
        {
            "threshold_seconds": str(threshold_seconds) if threshold_seconds is not None else None,
            "original_quote_age_ms": report["decision"]["original_quote_age_ms"],
            "research_quote_max_age_ms": policy.max_quote_age_ms,
        },
    )
    reconciliation = report["reconciliation"]
    accounting: bool | None = (
        True
        if reconciliation["proven_inconsistencies"]
        else False
        if reconciliation["all_filled_orders_independently_crosschecked"]
        else None
    )
    assess(
        "ACCOUNTING_FAILURE",
        accounting,
        "requires independently corroborated fill/value "
        "or fully confirmed fee-adjusted P&L inconsistency",
        f"quantity tolerance {policy.quantity_tolerance}; money tolerance {policy.money_tolerance}",
        {"proven_inconsistencies": reconciliation["proven_inconsistencies"]},
    )
    unknown = [name for name, item in assessments.items() if item["status"] == "unknown"]
    assess(
        "UNKNOWN",
        bool(unknown),
        "unavailable evidence stays unknown even when another failure condition is supported",
        "one or more requested attribution categories cannot be evaluated",
        {"categories": unknown},
    )
    return {
        "labels": [name for name in FAILURE_LABELS if assessments[name]["status"] == "supported"],
        "assessments": {name: assessments[name] for name in FAILURE_LABELS},
        "unknown_categories": unknown,
        "causal_claim": False,
    }


def _reconstruct_cycle(cycle: Mapping[str, Any], evidence: _Evidence) -> dict[str, Any]:
    policy = evidence.policy
    payload = _mapping(cycle.get("payload"))
    signal = _mapping(payload.get("signal"))
    config = _mapping(payload.get("config"))
    cycle_id, symbol = str(cycle["cycle_id"]), _symbol(cycle.get("symbol"))
    orders = evidence.orders.get(cycle_id, [])
    action = signal.get("action")
    action_side = "long" if action == "buy" else "short" if action == "sell" else None
    side = str(cycle["side"]) if cycle.get("side") in {"long", "short"} else action_side
    if side is None:
        opening_sides = {
            _mapping(order.row.get("broker")).get("position_intent") for order in orders
        } & {"buy_to_open", "sell_to_open"}
        if len(opening_sides) == 1:
            side = "long" if "buy_to_open" in opening_sides else "short"
        else:
            evidence.invalid_cycles[cycle_id].append("position_direction_not_recorded")
    entry_side, exit_side = (
        ("buy", "sell") if side == "long" else ("sell", "buy") if side == "short" else ("", "")
    )
    sign = D(1) if side == "long" else D(-1)
    entries = [order for order in orders if order.side == entry_side]
    exits = [order for order in orders if order.side == exit_side]
    eq, xq = _total(entries, "quantity"), _total(exits, "quantity")
    filled = eq is not None and eq > 0
    closed = (
        filled
        and xq is not None
        and xq > 0
        and cycle.get("state") in CLOSED_CYCLE_STATES
        and _decimal(cycle.get("owned_quantity")) == 0
        and _ns(cycle.get("closed_at")) is not None
        and all(order.primary.status in FINAL_ORDER_STATES and not order.issues for order in orders)
        and not evidence.invalid_cycles.get(cycle_id)
    )
    no_fill = (
        eq == 0
        and xq == 0
        and cycle.get("state") == "no_fill"
        and all(order.primary.status in FINAL_ORDER_STATES for order in orders)
        and not evidence.invalid_cycles.get(cycle_id)
    )
    status = "completed" if closed else "open_or_unresolved" if filled or not no_fill else "no_fill"
    entry, exit_leg = _leg(entries, policy), _leg(exits, policy)
    anchors, exit_anchors = _anchors(entries, policy), _anchors(exits, policy)
    timing = _timing(cycle, signal, entries, exits, anchors, exit_anchors, evidence)
    timestamps = timing["timestamps"]
    decision_at = timestamps["decision_time"]["unix_ns"]
    start, end = timestamps["fill_time"]["unix_ns"], timestamps["exit_fill_time"]["unix_ns"]
    if not closed:
        end = None
    decision = _decision_evidence(
        signal, symbol, decision_at, sign if side is not None else None, evidence.quotes
    )
    prediction = _prediction(signal)
    pnl, fees = _pnl(cycle, entries, exits, sign, closed, policy)
    if side is None:
        for leg in (entry, exit_leg):
            for field in ("quantity", "value", "weighted_price"):
                leg[field] = _metric(None, "unpaired orders", "position_direction_not_recorded")
        for field in ("common_quantity", "cash_flow_to_date_not_realized"):
            pnl[field] = _metric(None, "unpaired orders", "position_direction_not_recorded")
        for field in (
            "estimated_cash_reserve",
            "estimated_base_reserve_quantity",
            "estimated_base_reserve_value",
            "inventory_difference_quantity",
            "unattributed_inventory_reduction_quantity",
        ):
            fees[field] = _metric(None, "unpaired orders", "position_direction_not_recorded")
    actual, modeled = _value(pnl["actual_net_pnl"]), _value(pnl["modeled_net_pnl"])
    outcome_value = actual if actual is not None else modeled
    outcome = (
        "UNKNOWN"
        if outcome_value is None
        else "winner"
        if outcome_value > policy.money_tolerance
        else "loser"
        if outcome_value < -policy.money_tolerance
        else "flat"
    )
    reference = anchors[0].price if anchors and start is not None else None
    curve, curve_quotes = _curve(evidence.quotes, symbol, start, end, reference, sign)
    horizon = _value(prediction["predicted_horizon_seconds"])
    forecast_end = (
        decision_at + int(horizon * NS)
        if decision_at is not None and horizon is not None and 0 < horizon <= 10
        else None
    )
    baseline = decision["comparison_baseline"]
    forecast_curve, _ = _curve(
        evidence.quotes,
        symbol,
        decision_at,
        forecast_end,
        _decimal(baseline.get("mid")) if baseline else None,
        sign,
    )
    observed_regime = signal.get("regime")
    regime = (
        str(observed_regime)
        if observed_regime and signal.get("regime_validated") is True
        else "UNKNOWN"
    )
    report = {
        "trade_id": cycle_id,
        "cycle_id": cycle_id,
        "run_id": cycle.get("run_id"),
        "decision_id": cycle.get("decision_id"),
        "symbol": symbol,
        "strategy": _metric(
            entries[0].row.get("strategy_version") if entries else None, "original owned order"
        ),
        "family": str(signal.get("family") or "UNKNOWN"),
        "regime": regime,
        "regime_reason": None if regime != "UNKNOWN" else "validated_regime_not_recorded",
        "recorded_regime_unvalidated": observed_regime,
        "side": side,
        "side_reason": None if side else "position_direction_not_recorded",
        "side_basis": (
            "original directional signal/explicit cycle side and checked owned order sides"
        ),
        "status": status,
        "stored_state": cycle.get("state"),
        "source_integrity_issues": evidence.invalid_cycles.get(cycle_id, []),
        "unpaired_orders": [order.report(policy) for order in orders] if side is None else [],
        "signal_fill_group": "filled" if filled else "unfilled" if no_fill else "unknown",
        "outcome": outcome,
        "outcome_basis": "actual_net_pnl"
        if actual is not None
        else "modeled_net_pnl"
        if modeled is not None
        else "unknown",
        "time_of_day_et": _eastern(decision_at),
        "entry": entry,
        "exit": exit_leg,
        "entry_reason": _metric(signal.get("reasons"), "original signal reasons"),
        "exit_reason": _metric(payload.get("exit_reason"), "original exit request"),
        "execution": _style(entries, signal, config),
        "decision": decision,
        "prediction": prediction,
        "features": _features(signal),
        "timing": timing,
        "pnl": pnl,
        "fees": fees,
        "reconciliation": _reconciliation(cycle, entry, exit_leg, pnl, fees, policy),
        "mid_at_entry_fill": _markout(anchors, symbol, sign, 0, evidence.quotes, end),
        "markouts": {
            name: _markout(anchors, symbol, sign, horizon_ms, evidence.quotes, end)
            for name, horizon_ms in zip(HORIZON_NAMES, HORIZONS_MS, strict=True)
        },
        "excursions": curve,
        "forecast_horizon_excursions": forecast_curve,
        "executable_excursion": _net_excursion(
            entries, exits, anchors, exit_anchors, curve_quotes, fees, sign, config
        ),
        "backoff_analysis": _backoff_analysis(cycle, timing, evidence),
    }
    report["failure_attribution"] = _classify(report, signal, config, forecast_curve, policy)
    return report


def _trade_distributions(reports: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "trades": len(reports),
        "outcome_basis_counts": dict(Counter(str(report["outcome_basis"]) for report in reports)),
        "markout_bps": {
            name: _distribution(
                [_value(report["markouts"][name]["signed_bps"]) for report in reports],
                "per-trade complete quantity-weighted markouts; missing fills are not dropped",
            )
            for name in HORIZON_NAMES
        },
        "excursions": {
            name: _distribution(
                [_value(report["excursions"][name]) for report in reports],
                "observed first-execution-relative signed extrema"
                if not name.startswith("complete")
                else "only bounded-age complete holding curves",
            )
            for name in ("mfe_bps", "mae_bps", "complete_mfe_bps", "complete_mae_bps")
        },
        "holding_seconds": _distribution(
            [_value(report["timing"]["holding_seconds"]) for report in reports],
            "actual fill lifetime",
        ),
        "pnl": {
            name: _distribution([_value(report["pnl"][name]) for report in reports], name)
            for name in ("raw_price_pnl", "gross_cash_flow", "actual_net_pnl", "modeled_net_pnl")
        },
    }


def _signal_group(reports: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "signals": len(reports),
        "predicted_move_bps": _distribution(
            [_value(report["prediction"]["predicted_move_bps"]) for report in reports],
            "recorded prediction only",
        ),
        "expected_net_edge_bps": _distribution(
            [_value(report["prediction"]["expected_net_edge_bps"]) for report in reports],
            "recorded prospective net edge only",
        ),
        "returns_bps": {
            name: _distribution(
                [
                    _value(report["decision"]["future_returns"][name]["signed_return_bps"])
                    for report in reports
                ],
                "decision-relative future midpoint return, not realizable profit",
            )
            for name in HORIZON_NAMES
        },
    }


def _signal_comparison(reports: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    filled = [report for report in reports if report["signal_fill_group"] == "filled"]
    unfilled = [report for report in reports if report["signal_fill_group"] == "unfilled"]
    unknown = [report for report in reports if report["signal_fill_group"] == "unknown"]
    filled_group, unfilled_group = _signal_group(filled), _signal_group(unfilled)
    contrasts = {}
    for name in HORIZON_NAMES:
        first = _decimal(filled_group["returns_bps"][name]["mean"])
        second = _decimal(unfilled_group["returns_bps"][name]["mean"])
        contrasts[name] = {
            "filled_minus_unfilled_mean_bps": _metric(
                first - second if first is not None and second is not None else None,
                "unpaired descriptive means of decision-relative returns; no causal identification",
                "one_or_both_groups_have_no_fresh_samples",
            ),
            "negative_filled_positive_unfilled": (
                first < 0 < second if first is not None and second is not None else None
            ),
            "filled_samples": filled_group["returns_bps"][name]["samples"],
            "unfilled_samples": unfilled_group["returns_bps"][name]["samples"],
        }
    return {
        "filled": filled_group,
        "unfilled": unfilled_group,
        "unknown_fill_state": _signal_group(unknown),
        "contrasts": contrasts,
        "basis": (
            "one original recorded cycle/decision each; "
            "canceled positive partial fills belong to filled"
        ),
        "population_limit": (
            "recorded cycles only, not unpersisted neutral signals or a randomized sample"
        ),
        "unfilled_returns_are_realizable_profit": False,
        "causal_claim": False,
    }


def _dimension(report: Mapping[str, Any], name: str) -> str:
    if name == "entry_style":
        return str(report["execution"]["entry_style"])
    if name == "time_of_day_et":
        return str(report["time_of_day_et"]["hour_bucket"])
    return str(report[name])


def _grouped(
    reports: Sequence[Mapping[str, Any]], dimension: str
) -> dict[str, list[Mapping[str, Any]]]:
    groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for report in reports:
        groups[_dimension(report, dimension)].append(report)
    return dict(sorted(groups.items()))


def _totals(
    reports: Sequence[Mapping[str, Any]], name: str, *, section: str = "pnl"
) -> dict[str, Any]:
    values = [_value(report[section][name]) for report in reports]
    present = [value for value in values if value is not None]
    return {
        "value": str(sum(present, ZERO)) if present else None,
        "samples": len(present),
        "missing": len(values) - len(present),
        "reason": None if present else "no_known_values",
        "basis": name,
    }


def _broker_snapshot(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    current = _mapping(snapshot.get("current_broker"))
    counts = {}
    for key in ("positions", "open_orders"):
        items = current.get(key)
        counts[key] = len(items) if isinstance(items, list) else None
    known = all(value is not None for value in counts.values())
    return {
        "observed_at": _time(
            _ns(current.get("observed_at")), "captured read-only current-broker observation"
        ),
        "position_count": _metric(counts["positions"], "supplied broker positions response"),
        "open_order_count": _metric(counts["open_orders"], "supplied broker open-orders response"),
        "flat_at_observation": _metric(
            all(value == 0 for value in counts.values()) if known else None,
            "no positions and no open orders at this later observation",
            "current_broker_snapshot_missing_or_malformed",
        ),
        "limit": "later flatness is not a substitute for each cycle's fills or fee reconciliation",
    }


def _tape_summary(snapshot: Mapping[str, Any], policy: DiagnosticPolicy) -> dict[str, Any]:
    coverage = _mapping(snapshot.get("tape_coverage"))
    batches = coverage.get("batches")
    windows = coverage.get("windows")
    invalid = coverage.get("invalid_batches")
    for name, rows in (("batches", batches), ("windows", windows), ("invalid_batches", invalid)):
        if rows is not None and (not isinstance(rows, list) or len(rows) > policy.max_quotes):
            raise ValueError(f"tape_coverage.{name} must be a list within the metadata row bound")
    return {
        "basis": _metric(coverage.get("basis"), "source snapshot tape selection"),
        "quote_count": _metric(coverage.get("quote_count"), "source snapshot"),
        "window_total_seconds": _metric(
            _decimal(coverage.get("window_total_seconds")), "source snapshot selected windows"
        ),
        "window_count": _metric(len(windows) if windows is not None else None, "source snapshot"),
        "windows": _metric(_json_value(windows), "full selected coverage-window list"),
        "batch_count": _metric(len(batches) if batches is not None else None, "source snapshot"),
        "batch_metadata_sha256": _metric(
            sha256(
                json.dumps(_json_value(batches), sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest()
            if batches is not None
            else None,
            "canonical complete batch-metadata list, retained in the source snapshot",
        ),
        "invalid_batches": _metric(_json_value(invalid), "source snapshot invalid-batch evidence"),
        "cache_evictions": _metric(coverage.get("cache_evictions"), "bounded live-cache telemetry"),
        "retention": (
            "full batch metadata stays in the hashed source; "
            "each sampled quote retains its batch_id; "
            "the report does not duplicate tens of thousands of unused batch rows"
        ),
    }


def _model_number(value: Decimal | None) -> float | None:
    """Only the strict empirical-model transport uses floats; accounting stays Decimal."""
    if value is None:
        return None
    converted = float(value)
    return converted if isfinite(converted) else None


def _raw_quote(report: Any) -> dict[str, Any] | None:
    if not isinstance(report, Mapping):
        return None
    return {
        key: report.get(key)
        for key in (
            "event_id",
            "batch_id",
            "source",
            "exchange_at_ns",
            "received_at_ns",
            "capture_delay_ms",
            "age_at_target_ms",
            "bid",
            "ask",
            "mid",
        )
    }


def _snapshot_watermark(snapshot: Mapping[str, Any]) -> tuple[int | None, str]:
    for name in ("observed_through_ns", "finished_at", "observed_at"):
        if snapshot.get(name) is not None:
            return _ns(snapshot[name]), name
    return None, "explicit_observation_watermark_not_recorded; period_end_is_only_a_selection"


def _calibration_costs(
    config: Mapping[str, Any],
    basis: str,
    has_return: bool,
    entry_value: Decimal | None,
    end_value: Decimal | None,
) -> dict[str, Any]:
    maker, taker = _decimal(config.get("maker_fee_bps")), _decimal(config.get("taker_fee_bps"))
    entry_bps = (
        max(maker, taker)
        if maker is not None and taker is not None and min(maker, taker) >= 0
        else None
    )
    exit_bps = taker if taker is not None and taker >= 0 else None
    if (
        exit_bps is not None
        and entry_value is not None
        and entry_value > 0
        and end_value is not None
    ):
        exit_bps *= max(D(1), end_value / entry_value)

    def estimate(bps: Decimal | None, provenance: str) -> dict[str, Any]:
        value = _model_number(bps)
        return {
            "bps": value,
            "included_in_return": False,
            "kind": "estimated" if value is not None else "unknown",
            "provenance": provenance
            if value is not None
            else "frozen_fee_budget_missing_or_invalid",
            "validation_sha256": None,
        }

    def component(embedded: bool, provenance: str) -> dict[str, Any]:
        return {
            "bps": None,
            "included_in_return": embedded,
            "kind": "embedded" if embedded else "unknown",
            "provenance": provenance,
            "validation_sha256": None,
        }

    execution_embedded = has_return and basis == "fill_to_fill"
    return {
        "entry_fee": estimate(entry_bps, "entry_liquidity_unconfirmed_use_worst_configured_fee"),
        "exit_fee": estimate(
            exit_bps,
            "configured_taker_exit_budget; at_least_taker_bps_and_scaled_up_for_notional_growth",
        ),
        **{
            name: component(
                execution_embedded,
                "actual_fill_prices_already_embed_execution_cost; no_second_charge"
                if execution_embedded
                else f"unembedded_exit_{name}_not_independently_estimated",
            )
            for name in ("spread", "slippage", "impact")
        },
        "adverse_selection": component(
            has_return,
            "observed_fill_conditioned_return; no_second_adverse_selection_charge"
            if has_return
            else "fill_conditioned_return_not_available",
        ),
        "residual": None,
        "residual_covers": [],
    }


def _terminal_observed_at(order: _Order, evidence: _Evidence) -> tuple[int | None, str]:
    candidates: list[tuple[int, str]] = []
    for audit in evidence.audits_by_client.get(order.client_id, []):
        payload = _mapping(audit.get("payload"))
        broker = _broker_order(_mapping(payload.get("broker")))
        at = _ns(
            payload.get("source_received_at", audit.get("recorded_at", audit.get("occurred_at")))
        )
        if (
            at is not None
            and broker.status in FINAL_ORDER_STATES
            and broker.quantity is not None
            and order.quantity is not None
            and abs(broker.quantity - order.quantity) <= evidence.policy.quantity_tolerance
            and broker.broker_id == order.broker_id
            and broker.client_id == order.client_id
            and broker.symbol == order.symbol
            and broker.side == order.side
            and not broker.issues
            and broker.value is not None
            and order.value is not None
            and abs(broker.value - order.value) <= evidence.policy.money_tolerance
        ):
            candidates.append((at, str(audit.get("event_id") or "terminal_order_audit")))
    if candidates:
        return min(candidates)
    return None, "terminal_order_receipt_not_recorded"


def _calibration_maturity(
    cycle: Mapping[str, Any],
    required_orders: Sequence[_Order],
    deadline: int | None,
    received: Mapping[str, int | None],
    evidence: _Evidence,
    observed: int | None,
) -> tuple[int | None, list[dict[str, Any]]]:
    if deadline is None:
        return None, []
    times = [deadline]
    references: list[dict[str, Any]] = []
    fallback = False
    for order in required_orders:
        at, identity = _terminal_observed_at(order, evidence)
        if at is None and order.row.get("dispatch_state") == "expired_unsent":
            at, identity = _ns(cycle.get("closed_at")), "local_unsent_cycle_closed_at"
        if at is None:
            fallback = True
        else:
            times.append(at)
            references.append({"kind": "terminal_order_receipt", "identity": identity, "at_ns": at})
        for fill in order.fills:
            receipt = received.get(fill.identity)
            if receipt is None:
                fallback = True
            else:
                times.append(receipt)
                references.append(
                    {"kind": "FILL_activity_receipt", "identity": fill.identity, "at_ns": receipt}
                )
    if not required_orders:
        closed = _ns(cycle.get("closed_at"))
        if closed is not None and cycle.get("state") == "no_fill":
            times.append(closed)
            references.append(
                {"kind": "local_no_order_outcome", "identity": cycle["cycle_id"], "at_ns": closed}
            )
        else:
            fallback = True
    if fallback:
        if observed is None:
            return None, references
        times.append(observed)
        references.append(
            {
                "kind": "snapshot_availability_upper_bound",
                "identity": "individual_receipt_evidence_incomplete",
                "at_ns": observed,
            }
        )
    return max(times), references


def _requested_notional(
    orders: Sequence[_Order], config: Mapping[str, Any]
) -> tuple[Decimal | None, str]:
    if not orders:
        budget = _decimal(config.get("order_notional_usd"))
        return (
            budget if budget is not None and budget > 0 else None,
            "configured_candidate_budget; no_submitted_order_is_invented",
        )
    values: list[Decimal] = []
    for order in orders:
        intent = _mapping(order.row.get("intent"))
        request = _mapping(intent.get("request"))
        qty, limit = _decimal(request.get("quantity")), _decimal(intent.get("limit_price"))
        notional = _decimal(request.get("notional"))
        if notional is not None and notional > 0:
            values.append(notional)
        elif qty is not None and qty > 0 and limit is not None and limit > 0:
            values.append(qty * limit)
        else:
            return None, "requested_entry_notional_not_recorded"
    return sum(values, ZERO), "sum_original_requested_qty_times_limit_or_explicit_notional"


def _calibration_rows(
    snapshot: Mapping[str, Any],
    evidence: _Evidence,
    reports: Sequence[Mapping[str, Any]],
    source_sha256: str,
) -> list[dict[str, Any]]:
    observed, watermark_basis = _snapshot_watermark(snapshot)
    cycles = {str(cycle["cycle_id"]): cycle for cycle in evidence.cycles}
    received = {
        str(
            row.get("activity_id")
            or _mapping(row.get("payload")).get("id")
            or row.get("activity_key")
        ): _ns(row.get("received_at"))
        for row in _rows(snapshot, "activities", evidence.policy.max_activities)
    }
    result: list[dict[str, Any]] = []
    for report in reports:
        cycle = cycles[str(report["cycle_id"])]
        payload = _mapping(cycle.get("payload"))
        signal, config = _mapping(payload.get("signal")), _mapping(payload.get("config"))
        raw_features = _mapping(signal.get("features"))
        features = {
            name: _model_number(_decimal(raw_features.get(name))) for name in CALIBRATION_FEATURES
        }
        timing = report["timing"]
        timestamps = timing["timestamps"]
        stages = timing["recorded_stages_ns"]
        decision = timestamps["decision_time"]["unix_ns"]
        feature_time = _ns(stages.get("t3_features_ns", raw_features.get("as_of_ns")))
        first_fill = timestamps["fill_time"]["unix_ns"]
        last_entry = timestamps["entry_completion_time"]["unix_ns"]
        last_exit = timestamps["exit_fill_time"]["unix_ns"]
        entries = [
            evidence.by_client[order["client_order_id"]] for order in report["entry"]["orders"]
        ]
        exits = [evidence.by_client[order["client_order_id"]] for order in report["exit"]["orders"]]
        entry_qty, entry_value = (
            _value(report["entry"]["quantity"]),
            _value(report["entry"]["value"]),
        )
        entry_price = _value(report["entry"]["weighted_price"])
        notional, notional_basis = _requested_notional(entries, config)
        ttl = _decimal(config.get("entry_order_ttl_seconds"))
        if ttl is not None and ttl <= 0:
            ttl = None
        group = report["signal_fill_group"]
        filled = True if group == "filled" else False if group == "unfilled" else None
        style = report["execution"]["entry_style"]
        action = (
            ("PASSIVE_" if style == "passive" else "AGGRESSIVE_")
            + ("BUY" if report["side"] == "long" else "SELL")
            if style in {"passive", "aggressive"} and report["side"] in {"long", "short"}
            else None
        )
        account = snapshot.get("account_digest") or cycle.get("account_digest")
        quote_age = _value(report["decision"]["original_quote_age_ms"])
        quote = _mapping(signal.get("quote"))
        bid, ask = _decimal(quote.get("bid")), _decimal(quote.get("ask"))
        quote_exchange = _ns(quote.get("exchange_time_ns", quote.get("exchange_at")))
        quote_received = _ns(quote.get("received_at_ns", quote.get("received_at")))
        valid_decision_quote = (
            decision is not None
            and quote_exchange is not None
            and quote_received is not None
            and quote_exchange <= quote_received <= decision
            and _symbol(quote.get("symbol")) == report["symbol"]
            and bid is not None
            and ask is not None
            and 0 < bid <= ask
        )
        spread = (
            (ask - bid) / ((ask + bid) / 2) * 10000
            if valid_decision_quote and bid is not None and ask is not None
            else None
        )
        latency = _value(timing["stages_ms"]["decision_send"])
        if "t4_model_ns" not in stages or "t6_submitted_ns" not in stages:
            latency = None
        fraction = (
            ZERO
            if filled is False
            else entry_value / notional
            if filled and entry_value is not None and notional is not None and notional > 0
            else None
        )
        fill_delay = (
            D(first_fill - decision) / NS
            if filled and first_fill is not None and decision is not None and first_fill >= decision
            else None
        )
        entry_complete = bool(entries) and all(
            order.activity_matches(evidence.policy) for order in entries
        )
        entry_terminal = all(order.primary.status in FINAL_ORDER_STATES for order in entries)
        proxy_latency = _value(timing["stages_ms"]["decision_dispatch_start"])
        features_causal = (
            feature_time is not None and decision is not None and feature_time <= decision
        )
        if not features_causal:
            features = {name: None for name in CALIBRATION_FEATURES}
        exposure = (
            "venue_order"
            if entries and all(order.broker_id for order in entries)
            else "unsent_order"
            if entries
            and all(order.row.get("dispatch_state") == "expired_unsent" for order in entries)
            else "no_order"
            if not entries
            else "unknown"
        )
        for horizon in CALIBRATION_HORIZONS_SECONDS:
            missing: dict[str, str] = {}
            invalid: list[str] = []
            target = decision + horizon * NS if decision is not None else None
            deadline = (
                decision + int(max(D(horizon), ttl) * NS)
                if decision is not None and ttl is not None
                else None
            )
            expiries = [_ns(order.row.get("expires_at")) for order in entries]
            if deadline is not None:
                deadline = max([deadline, *(value for value in expiries if value is not None)])
            endpoint = report["decision"]["future_returns"][f"{horizon}s"]
            horizon_observed = target is not None and observed is not None and target <= observed
            horizon_quote = _raw_quote(endpoint["sample"]) if horizon_observed else None
            baseline = _raw_quote(report["decision"]["comparison_baseline"])
            directional = _value(endpoint["signed_return_bps"]) if horizon_observed else None
            if horizon_quote is None:
                missing["horizon_quote"] = (
                    "no_causal_fresh_quote"
                    if horizon_observed
                    else "horizon_not_observed_at_snapshot"
                )
            if baseline is None:
                missing["directional_return_bps"] = "no_causal_fresh_decision_midpoint"
            elif directional is None:
                missing["directional_return_bps"] = (
                    "no_causal_fresh_horizon_quote"
                    if horizon_observed
                    else "horizon_not_observed_at_snapshot"
                )
            if not features_causal:
                missing["features"] = "feature_snapshot_missing_or_timestamp_after_decision"
            if not horizon_observed:
                invalid.append("horizon_not_observed_at_snapshot")
            gross: Decimal | None = None
            end_value: Decimal | None = None
            return_end: int | None = None
            basis = "fill_to_mid"
            required_orders = entries
            quantity_within: Decimal | None = ZERO if filled is False else None
            value_within: Decimal | None = ZERO if filled is False else None
            if filled and entry_complete and target is not None:
                past_fills = [
                    fill
                    for order in entries
                    for fill in order.fills
                    if fill.at is not None and fill.at <= target
                ]
                quantity_within = sum(
                    (fill.quantity for fill in past_fills if fill.quantity is not None), ZERO
                )
                value_within = sum(
                    (fill.value for fill in past_fills if fill.value is not None), ZERO
                )
            if filled:
                payoff_reason = None
                if not entry_complete or first_fill is None or last_entry is None:
                    payoff_reason = "incremental_entry_fill_evidence_or_exact_timestamps_incomplete"
                elif target is None or last_entry > target:
                    payoff_reason = (
                        "first_fill_after_label_horizon"
                        if target is not None and first_fill > target
                        else "entry_fill_tail_after_label_horizon"
                    )
                elif not entry_terminal:
                    payoff_reason = "entry_order_not_terminal; later_execution_tail_unresolved"
                elif not horizon_observed:
                    payoff_reason = "horizon_not_observed_at_snapshot"
                elif entry_value is None or entry_value <= 0 or entry_price is None:
                    payoff_reason = "priced_entry_value_missing"
                elif (
                    report["status"] == "completed"
                    and last_exit is not None
                    and last_exit <= target
                    and all(order.activity_matches(evidence.policy) for order in exits)
                ):
                    raw = _value(report["pnl"]["raw_price_pnl"])
                    gross = raw / entry_value * 10000 if raw is not None else None
                    end_value = _value(report["exit"]["value"])
                    return_end, basis, required_orders = (
                        last_exit,
                        "fill_to_fill",
                        [*entries, *exits],
                    )
                elif report["status"] == "completed" and (last_exit is None or last_exit <= target):
                    payoff_reason = "exact_exit_fill_evidence_incomplete"
                elif any(
                    fill.at is not None and fill.at <= target
                    for order in exits
                    for fill in order.fills
                ):
                    payoff_reason = (
                        "partial_exit_spans_label_horizon; no_full_quantity_midpoint_payoff"
                    )
                elif horizon_quote is not None:
                    mid = _decimal(horizon_quote["mid"])
                    if mid is not None and entry_qty is not None:
                        sign = D(1) if report["side"] == "long" else D(-1)
                        gross = sign * (mid - entry_price) * entry_qty / entry_value * 10000
                        end_value = mid * entry_qty
                        return_end = target
                else:
                    payoff_reason = "no_causal_fresh_horizon_quote"
                if gross is None:
                    missing["gross_return_bps"] = payoff_reason or "horizon_payoff_unavailable"
                    missing["return_end_at"] = payoff_reason or "horizon_payoff_unavailable"
            mature, maturity_references = _calibration_maturity(
                cycle, required_orders, deadline, received, evidence, observed
            )
            if observed is None or mature is None or mature > observed:
                invalid.append("label_not_mature_at_snapshot_cutoff")
                gross, return_end, end_value = None, None, None
                if filled:
                    missing["gross_return_bps"] = "label_not_mature_at_snapshot_cutoff"
                    missing["return_end_at"] = "label_not_mature_at_snapshot_cutoff"
            costs = _calibration_costs(config, basis, gross is not None, entry_value, end_value)
            sample: dict[str, Any] = {
                "sample_id": f"{cycle['cycle_id']}:h={horizon}s",
                "account_digest": account,
                "source_sha256": source_sha256,
                "symbol": report["symbol"],
                "family": report["family"]
                if report["family"] in {"momentum", "reversion"}
                else None,
                "action": action,
                "decision_at": _iso(decision),
                "feature_observed_at": _iso(feature_time),
                "label_matured_at": _iso(mature),
                "return_end_at": _iso(return_end),
                "horizon_seconds": float(horizon),
                "cancellation_horizon_seconds": _model_number(ttl),
                "time_unit": "seconds",
                "return_unit": "bps",
                "features": features,
                "quote_age_seconds": _model_number(quote_age / 1000)
                if quote_age is not None and valid_decision_quote
                else None,
                "decision_latency_seconds": _model_number(latency / 1000)
                if latency is not None
                else None,
                "spread_bps": _model_number(spread),
                "notional_usd": _model_number(notional),
                "filled": filled,
                "filled_fraction": _model_number(fraction),
                "fill_delay_seconds": _model_number(fill_delay),
                "gross_return_bps": _model_number(gross),
                "directional_return_bps": _model_number(directional),
                "return_basis": basis,
                "costs": costs,
                "execution_evidence": "observed"
                if filled is not None
                else "unsupported_counterfactual",
                "execution_validation_sha256": None,
                "position_effect": "open_long" if report["side"] == "long" else None,
            }
            required = [
                "account_digest",
                "symbol",
                "family",
                "action",
                "decision_at",
                "feature_observed_at",
                "label_matured_at",
                "cancellation_horizon_seconds",
                "quote_age_seconds",
                "decision_latency_seconds",
                "spread_bps",
                "notional_usd",
                "filled",
                "filled_fraction",
                "position_effect",
            ]
            if filled:
                required.extend(("fill_delay_seconds", "gross_return_bps", "return_end_at"))
            for name in required:
                if sample[name] is None:
                    missing.setdefault(name, "required_model_input_not_recorded_or_not_supported")
                    invalid.append(f"missing:{name}")
            if latency is None:
                missing["decision_latency_seconds"] = (
                    "exact_t4_model_to_t6_send_not_recorded; "
                    "dispatch_marker_proxy_is_not_substituted"
                )
            if not isinstance(account, str) or re.fullmatch(r"[0-9a-f]{64}", account) is None:
                invalid.append("invalid_account_digest")
            if re.fullmatch(r"[A-Z0-9]+/USD", str(sample["symbol"])) is None:
                invalid.append("symbol_not_supported_by_economic_sample")
            if feature_time is not None and decision is not None and feature_time > decision:
                invalid.append("feature_timestamp_after_decision")
            if not any(value is not None for value in features.values()):
                invalid.append("no_recorded_numeric_microstructure_features")
            if fraction is not None and (fraction < 0 or fraction > 1):
                invalid.append("filled_fraction_outside_observed_notional_bounds")
            if filled and fraction is not None and fraction <= 0:
                invalid.append("filled_sample_requires_positive_executed_fraction")
            if filled and fill_delay is not None and ttl is not None and fill_delay > ttl:
                invalid.append("fill_after_declared_cancellation_horizon")
            if first_fill is not None and return_end is not None and return_end < first_fill:
                invalid.append("payoff_precedes_first_execution")
            if filled and not entry_complete:
                invalid.append("incremental_fill_crosscheck_incomplete")
            if filled is False and any(order.fills for order in entries):
                invalid.append("positive_activity_conflicts_with_zero_cumulative_fill")
            if (
                any(order.issues for order in [*entries, *exits])
                or report["source_integrity_issues"]
            ):
                invalid.append("source_identity_or_pairing_inconsistent")
            if decision is not None and observed is not None and decision > observed:
                invalid.append("decision_after_snapshot_cutoff")
            invalid = sorted(set(invalid))
            unpriced = [
                name
                for name, component in costs.items()
                if isinstance(component, dict)
                and not component["included_in_return"]
                and component["bps"] is None
            ]
            result.append(
                {
                    "schema": "scalp-calibration-row-v1",
                    "cycle_id": cycle["cycle_id"],
                    "run_id": cycle.get("run_id"),
                    "decision_id": cycle.get("decision_id"),
                    "horizon_seconds": horizon,
                    "sample": sample,
                    "training_eligible": not invalid,
                    "eligibility_reasons": invalid,
                    "missing_reasons": missing,
                    "unpriced_cost_components": unpriced,
                    "evidence": {
                        "decision_time_basis": timestamps["decision_time"]["basis"],
                        "horizon_origin": (
                            "research_label_window; not_a_recorded_historical_prediction"
                        ),
                        "cutoff_at_ns": observed,
                        "cutoff_basis": watermark_basis,
                        "feature_observed_at_ns": feature_time,
                        "feature_snapshot_causal": features_causal,
                        "original_signal_at_ns": _ns(signal.get("observed_at")),
                        "decision_at_ns": decision,
                        "horizon_target_at_ns": target,
                        "return_end_at_ns": return_end,
                        "first_fill_at_ns": first_fill,
                        "last_entry_fill_at_ns": last_entry,
                        "last_exit_fill_at_ns": last_exit,
                        "label_matured_at_ns": mature,
                        "maturity_evidence": maturity_references,
                        "client_order_ids": [order.client_id for order in entries],
                        "broker_order_ids": [order.broker_id for order in entries],
                        "venue_exposure": exposure,
                        "execution_scope": (
                            "observed_candidate_policy_outcomes; "
                            "not_venue_conditional_fill_probability"
                        ),
                        "entry_style_basis": report["execution"]["basis"],
                        "entry_quantity": str(entry_qty) if entry_qty is not None else None,
                        "entry_value": str(entry_value) if entry_value is not None else None,
                        "entry_quantity_within_horizon": str(quantity_within)
                        if quantity_within is not None
                        else None,
                        "entry_value_within_horizon": str(value_within)
                        if value_within is not None
                        else None,
                        "entry_fill_tail_after_horizon": last_entry > target
                        if last_entry is not None and target is not None
                        else None,
                        "requested_notional_exact": str(notional) if notional is not None else None,
                        "requested_notional_basis": notional_basis,
                        "cancellation_origin": (
                            "recorded_local_order_TTL; actual_expiry_may_follow_decision"
                        ),
                        "original_order_expires_at_ns": expiries,
                        "decision_to_dispatch_start_proxy_seconds": _model_number(
                            proxy_latency / 1000
                        )
                        if proxy_latency is not None
                        else None,
                        "original_decision_quote": {
                            "bid": str(bid) if bid is not None else None,
                            "ask": str(ask) if ask is not None else None,
                            "exchange_at_ns": quote_exchange,
                            "received_at_ns": quote_received,
                        },
                        "decision_baseline_quote": baseline,
                        "horizon_quote": horizon_quote,
                        "horizon_quote_max_age_ms": evidence.policy.max_quote_age_ms,
                        "gross_return_exact_bps": str(gross) if gross is not None else None,
                        "directional_return_exact_bps": str(directional)
                        if directional is not None
                        else None,
                        "return_is_realized_raw_price_pnl": gross is not None
                        and basis == "fill_to_fill",
                        "directional_return_is_realizable_profit": False,
                        "fee_basis": (
                            "raw_price_return; no_inventory_fee_principal_in_payoff; "
                            "never_recharge_spread"
                        ),
                        "recorded_diagnostic_round_trip_cost_bps_not_used": signal.get(
                            "estimated_round_trip_cost_bps"
                        ),
                        "actual_maker_liquidity_proven": False,
                    },
                }
            )
    keys = [
        (
            row["horizon_seconds"],
            row["sample"]["decision_at"],
            row["sample"]["symbol"],
            row["sample"]["family"],
            row["sample"]["action"],
        )
        for row in result
    ]
    counts = Counter(keys)
    for row, key in zip(result, keys, strict=True):
        if counts[key] > 1:
            row["training_eligible"] = False
            row["eligibility_reasons"].append("duplicate_decision_action_observation")
    return result


def _calibration_coverage(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "raw_rows": len(rows),
        "unique_candidates": len({row["cycle_id"] for row in rows}),
        "by_horizon_seconds": {
            str(horizon): {
                "rows": len(group),
                "filled": sum(row["sample"]["filled"] is True for row in group),
                "unfilled": sum(row["sample"]["filled"] is False for row in group),
                "unknown_fill_state": sum(row["sample"]["filled"] is None for row in group),
                "model_input_ready": sum(row["training_eligible"] for row in group),
                "gross_return_samples": sum(
                    row["sample"]["gross_return_bps"] is not None for row in group
                ),
                "directional_return_samples": sum(
                    row["sample"]["directional_return_bps"] is not None for row in group
                ),
                "return_bases": dict(Counter(str(row["sample"]["return_basis"]) for row in group)),
                "exposure_counts": dict(
                    Counter(str(row["evidence"]["venue_exposure"]) for row in group)
                ),
                "eligibility_reason_counts": dict(
                    Counter(reason for row in group for reason in row["eligibility_reasons"])
                ),
                "missing_reason_counts": dict(
                    Counter(
                        reason for row in group for reason in set(row["missing_reasons"].values())
                    )
                ),
            }
            for horizon in CALIBRATION_HORIZONS_SECONDS
            for group in [[row for row in rows if row["horizon_seconds"] == horizon]]
        },
    }


def reconstruct_period(
    snapshot: Mapping[str, Any], *, policy: DiagnosticPolicy | None = None
) -> dict[str, Any]:
    """Reconstruct every supplied cycle without I/O, mutation, training, or order actions.

    The caller selects the reporting period and retains the raw evidence. Snapshot tables
    are optional (missing evidence is reported); ``cycles`` requires stable unique IDs.
    Wrapper/job envelopes, mixed-account cycles, malformed tables, and exceeded resource
    bounds raise ValueError. Output ``trades`` contains only closed, positively filled cycles;
    ``open_or_unresolved`` never realizes their P&L. ``signals`` includes every supplied cycle.
    Monetary/statistical diagnostics use Decimal strings, counts use ints, and clocks use UTC/ns.
    ``calibration_samples`` retains every candidate at 1s and 5s in raw nullable envelopes.
    Its model-transport scalars use finite JSON numbers; exact source decimals stay in evidence.
    """
    if not isinstance(snapshot, Mapping):
        raise ValueError("snapshot must be a mapping")
    if "results" in snapshot:
        raise ValueError("pass the raw snapshot, not the job results envelope")
    selected = policy or DiagnosticPolicy()
    with localcontext() as context:
        context.prec = 50
        evidence = _Evidence(snapshot, selected)
        tape_summary = _tape_summary(snapshot, selected)
        reconstructed = [
            _reconstruct_cycle(cycle, evidence)
            for cycle in sorted(
                evidence.cycles,
                key=lambda row: (_ns(row.get("created_at")) or 0, str(row["cycle_id"])),
            )
        ]
        source_sha256 = sha256(
            json.dumps(
                _json_value(snapshot), sort_keys=True, separators=(",", ":"), allow_nan=False
            ).encode("utf-8")
        ).hexdigest()
        calibration_rows = _calibration_rows(snapshot, evidence, reconstructed, source_sha256)
        trades = [report for report in reconstructed if report["status"] == "completed"]
        open_reports = [
            report for report in reconstructed if report["status"] == "open_or_unresolved"
        ]
        dimensions = ("outcome", "family", "entry_style", "symbol", "regime", "time_of_day_et")
        pnl_fields = ("raw_price_pnl", "gross_cash_flow", "actual_net_pnl", "modeled_net_pnl")
        label_counts = Counter(
            label for report in trades for label in report["failure_attribution"]["labels"]
        )
        latency = {}
        stage_names = (
            *STAGES,
            "decision_dispatch_start",
            "dispatch_start_broker_observation",
            "broker_observation_first_fill",
            "exit_decision_dispatch_start",
            "exit_dispatch_start_fill",
        )
        for name in stage_names:
            measurements = [report["timing"]["stages_ms"][name] for report in trades]
            latency[name] = {
                **_distribution(
                    [_value(item) for item in measurements],
                    "milliseconds; nearest-rank percentiles",
                ),
                "recorded_stage_samples": sum(
                    item["value"] is not None and item["measurement_kind"] == "recorded_stage"
                    for item in measurements
                ),
                "proxy_samples": sum(
                    item["value"] is not None and item["measurement_kind"] == "proxy"
                    for item in measurements
                ),
                "missing_reasons": dict(
                    Counter(str(item["reason"]) for item in measurements if item["value"] is None)
                ),
            }
        longest = max(
            [row for row in trades if _value(row["timing"]["holding_seconds"]) is not None],
            key=lambda report: _value(report["timing"]["holding_seconds"]) or ZERO,
            default=None,
        )
        report = {
            "schema": "scalp-diagnostics-v1",
            "source_schema": snapshot.get("schema"),
            "account_digest": snapshot.get("account_digest"),
            "period_start": snapshot.get("period_start"),
            "period_end": snapshot.get("period_end"),
            "provenance": {
                "source_code_sha": snapshot.get("code_sha"),
                "declared_source_payload_sha256": snapshot.get("source_payload_sha256"),
                "canonical_snapshot_sha256": source_sha256,
                "selection_basis": (
                    "all supplied cycles; caller selects period, "
                    "library does not silently drop boundary trades"
                ),
                "broker_history_complete_for_request": snapshot.get(
                    "broker_history_complete_for_request"
                ),
                "read_only": True,
                "core_io": False,
            },
            "policy": {
                **_json_value(asdict(selected)),
                "horizons_ms": list(HORIZONS_MS),
                "quote_sampling": (
                    "exchange<=target AND receipt<=target AND age<=min(250ms,horizon); "
                    "no future/nearest/interpolated quotes"
                ),
                "markout_anchor": (
                    "each execution at its own timestamp; "
                    "weighted only with all fill samples available"
                ),
                "mfe_mae_basis": (
                    "signed observed extrema vs first actual execution, "
                    "strictly inside actual fill lifetime"
                ),
                "numeric_encoding": (
                    "Decimal strings; None with reason for unavailable measurements"
                ),
                "percentiles": "nearest rank; no float conversion",
                "failure_labels_are_evidence_conditions_not_causal_proof": True,
            },
            "summary": {
                "cycles": len(reconstructed),
                "completed_trades": len(trades),
                "no_fill_cycles": sum(report["status"] == "no_fill" for report in reconstructed),
                "open_or_unresolved_cycles": len(open_reports),
                "completed_by_symbol": dict(Counter(str(report["symbol"]) for report in trades)),
                "completed_by_family": dict(Counter(str(report["family"]) for report in trades)),
                "outcomes": dict(Counter(str(report["outcome"]) for report in trades)),
                "outcome_basis": dict(Counter(str(report["outcome_basis"]) for report in trades)),
                "pnl_totals": {name: _totals(trades, name) for name in pnl_fields},
                "stored_modeled_pnl_total": _totals(trades, "stored_modeled_net_pnl"),
                "failure_labels": {name: label_counts[name] for name in FAILURE_LABELS},
                "accounting_failures_proven": sum(
                    report["reconciliation"]["accounting_failure_proven"] for report in trades
                ),
                "fee_records": sum(report["fees"]["confirmed_fee_records"] for report in trades),
                "fee_complete_trades": sum(
                    report["fees"]["complete_order_fee_coverage"] for report in trades
                ),
                "fee_and_inventory_effects": {
                    name: _totals(trades, name, section="fees")
                    for name in (
                        "quantity_difference_value_already_in_cashflow",
                        "estimated_cash_reserve",
                        "estimated_base_reserve_value",
                    )
                },
            },
            "coverage": {
                "closed_trades_with_historical_order_crosscheck": sum(
                    report["reconciliation"]["historical_orders_checked"]
                    == report["reconciliation"]["filled_orders"]
                    for report in trades
                ),
                "closed_trades_with_fill_activity_crosscheck": sum(
                    report["reconciliation"]["fill_activity_orders_checked"]
                    == report["reconciliation"]["filled_orders"]
                    for report in trades
                ),
                "closed_trades_with_both_independent_crosschecks": sum(
                    report["reconciliation"]["all_filled_orders_independently_crosschecked"]
                    for report in trades
                ),
                "filled_orders": sum(
                    report["reconciliation"]["filled_orders"] for report in trades
                ),
                "historical_orders_checked": sum(
                    report["reconciliation"]["historical_orders_checked"] for report in trades
                ),
                "fill_activity_orders_checked": sum(
                    report["reconciliation"]["fill_activity_orders_checked"] for report in trades
                ),
                "quote_rows": evidence.quotes.input_count,
                "valid_unique_quotes": sum(
                    len(rows) for rows in evidence.quotes.by_symbol.values()
                ),
                "quote_rejections": dict(evidence.quotes.rejected),
                "complete_holding_curves": sum(
                    report["excursions"]["complete_freshness_coverage"] for report in trades
                ),
                "markouts": {
                    name: {
                        "samples": sum(
                            report["markouts"][name]["signed_bps"]["value"] is not None
                            for report in trades
                        ),
                        "missing": sum(
                            report["markouts"][name]["signed_bps"]["value"] is None
                            for report in trades
                        ),
                    }
                    for name in HORIZON_NAMES
                },
                "prediction": {
                    name: {
                        "samples": sum(
                            report["prediction"][name]["value"] is not None for report in trades
                        ),
                        "missing": sum(
                            report["prediction"][name]["value"] is None for report in trades
                        ),
                    }
                    for name in (
                        "predicted_move_bps",
                        "predicted_horizon_seconds",
                        "expected_net_edge_bps",
                        "predicted_fill_probability",
                        "confidence",
                        "uncertainty",
                    )
                },
                "tape_source_coverage": tape_summary,
                "data_capability": snapshot.get("data_capability"),
                "coverage_limit": (
                    "missing quote samples are not zero returns; "
                    "sparse 100ms coverage is not 100ms precision"
                ),
            },
            "trades": trades,
            "open_or_unresolved": open_reports,
            "signals": [
                {
                    "cycle_id": report["cycle_id"],
                    "decision_id": report["decision_id"],
                    "symbol": report["symbol"],
                    "family": report["family"],
                    "regime": report["regime"],
                    "entry_style": report["execution"],
                    "fill_group": report["signal_fill_group"],
                    "stored_state": report["stored_state"],
                    "client_order_ids": [
                        order["client_order_id"] for order in report["entry"]["orders"]
                    ],
                    "decision_time": report["timing"]["timestamps"]["decision_time"],
                    "time_of_day_et": report["time_of_day_et"],
                    "prediction": report["prediction"],
                    "decision_returns": report["decision"],
                    "entry_filled_quantity": report["entry"]["quantity"],
                }
                for report in reconstructed
            ],
            "calibration_samples": calibration_rows,
            "calibration_coverage": _calibration_coverage(calibration_rows),
            "calibration_contract": {
                "schema": "scalp-calibration-row-v1",
                "horizons_seconds": list(CALIBRATION_HORIZONS_SECONDS),
                "sample_shape": (
                    "EconomicSample-shaped raw JSON; unsupported required fields stay null"
                ),
                "consumer": (
                    "validate row.sample only after preflight; retain all rows and coverage"
                ),
                "training_eligible_meaning": (
                    "structurally complete observed input; NOT validated net economics"
                ),
                "unknown_costs_must_not_become_zero": True,
                "unknown_costs_allowed_in_raw_input": True,
                "unknown_costs_block_economic_validation": True,
                "decision_latency_seconds_basis": (
                    "exact recorded t4_model_ns to t6_submitted_ns; never a dispatch proxy"
                ),
                "numeric_encoding": (
                    "finite model-transport JSON numbers; exact decimal evidence retained"
                ),
                "features": list(CALIBRATION_FEATURES),
                "feature_scope": (
                    "recorded predecision microstructure only; "
                    "no IDs, future labels, or cost estimates"
                ),
                "source_sha256": source_sha256,
                "no_new_probabilities_or_predictions": True,
                "no_training_or_holdout_creation": True,
                "split_calls_by_horizon": True,
                "do_not_silently_drop_unsupported_candidates_or_select_only_profitable_rows": True,
            },
            "distributions": {
                "all": _trade_distributions(trades),
                **{
                    dimension: {
                        key: _trade_distributions(rows)
                        for key, rows in _grouped(trades, dimension).items()
                    }
                    for dimension in dimensions
                },
            },
            "filled_vs_unfilled": _signal_comparison(reconstructed),
            "passive_filled_vs_unfilled": _signal_comparison(
                [
                    report
                    for report in reconstructed
                    if report["execution"]["entry_style"] == "passive"
                ]
            ),
            "signal_comparison_strata": {
                dimension: {
                    key: _signal_comparison(rows)
                    for key, rows in _grouped(reconstructed, dimension).items()
                }
                for dimension in ("family", "symbol", "regime", "entry_style", "time_of_day_et")
            },
            "latency_stages_ms": latency,
            "run_cohorts": {
                run_id: _trade_distributions(rows)
                for run_id, rows in _grouped(trades, "run_id").items()
            },
            "current_broker_read_only": _broker_snapshot(snapshot),
            "longest_holding_trade": {
                "cycle_id": longest["cycle_id"],
                "holding_seconds": longest["timing"]["holding_seconds"],
                "analysis": longest["backoff_analysis"],
            }
            if longest
            else None,
            "longest_holding_trade_reason": (
                None if longest else "no_completed_trade_has_an_exact_fill_lifetime"
            ),
            "data_quality": {
                "issues": evidence.issues,
                "issue_counts": dict(Counter(str(item["reason"]) for item in evidence.issues)),
            },
            "limitations": [
                "No recorded prediction/confidence/uncertainty is manufactured "
                "from deterministic heuristic scores.",
                "Passive intent does not prove actual maker liquidity, "
                "exact queue position, MBO, or fill causality.",
                "Unknown fee postings prevent confirmed actual net P&L; "
                "reserve-modeled loss is explicitly separate.",
                "Broker cumulative VWAP, incremental FILL activities, "
                "and cycle projections are separate reconciliation sources.",
                "Freshness-incomplete observed excursions cannot prove "
                "the absence of a favorable move.",
                "No holdout, trained regime, model calibration, counterfactual executable "
                "unfilled profit, or production mutation is implied.",
            ],
        }
        return report
