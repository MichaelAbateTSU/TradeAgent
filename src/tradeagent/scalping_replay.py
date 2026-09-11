from __future__ import annotations

import heapq
import math
from collections import Counter
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from decimal import ROUND_CEILING, Decimal
from itertools import groupby
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from tradeagent.scalping_config import ScalpingConfig, ScalpInventory, ScalpQuote, ScalpSignal
from tradeagent.scalping_market import (
    NS,
    BookFeatureEngine,
    MarketEvent,
    ns_datetime,
)
from tradeagent.scalping_strategy import ScalpStrategy

ZERO = Decimal(0)
BPS = Decimal(10000)
_TERMINAL = {"filled", "canceled", "rejected", "withdrawn"}


class ReplayLatency(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    decision_to_send_ms: float = Field(default=0, ge=0)
    send_to_arrival_ms: float = Field(default=20, ge=0)
    arrival_to_ack_ms: float = Field(default=20, ge=0)
    cancel_latency_ms: float = Field(default=20, ge=0)


class _ExecutionAssumptions(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    slippage_bps: Decimal = Field(default=ZERO, ge=0, lt=10000)
    queue_ahead_multiplier: Decimal = Field(default=Decimal(1), ge=1)
    participation_rate: Decimal = Field(default=Decimal(1), gt=0, le=1)


def _delay_ns(milliseconds: float) -> int:
    return int((Decimal(str(milliseconds)) * 1_000_000).to_integral_value(rounding=ROUND_CEILING))


def _percentiles(values: Iterable[float]) -> dict[str, float | int | None]:
    ordered = sorted(values)
    result: dict[str, float | int | None] = {"samples": len(ordered)}
    for name, fraction in (("p50", 0.5), ("p95", 0.95), ("p99", 0.99)):
        result[name] = ordered[math.ceil(len(ordered) * fraction) - 1] if ordered else None
    return result


def _json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def _records(events: Iterable[MarketEvent | Mapping[str, Any] | str]) -> Iterator[MarketEvent]:
    last_received = 0
    previous_connection: dict[str, tuple[str, int, int]] = {}
    for value in events:
        event = (
            value
            if isinstance(value, MarketEvent)
            else MarketEvent.model_validate_json(value)
            if isinstance(value, str)
            else MarketEvent.model_validate(value)
        )
        if event.received_at_ns < last_received:
            raise ValueError("replay tape must be in nondecreasing local receive-time order")
        identity = str(event.connection_id)
        previous = previous_connection.get(identity)
        if previous is not None and (
            event.receive_sequence < previous[1]
            or event.received_monotonic_ns < previous[2]
            or (event.receive_sequence == previous[1] and event.event_id != previous[0])
        ):
            raise ValueError("replay tape violates local connection ordering")
        previous_connection[identity] = (
            event.event_id,
            event.receive_sequence,
            event.received_monotonic_ns,
        )
        last_received = event.received_at_ns
        yield event


@dataclass
class _Order:
    order_id: str
    symbol: str
    side: Literal["buy", "sell"]
    quantity: Decimal
    limit_price: Decimal | None
    decision: ScalpSignal
    decision_at_ns: int
    arrival_due_ns: int
    status: str = "pending_send"
    filled_quantity: Decimal = ZERO
    sent_at_ns: int | None = None
    arrived_at_ns: int | None = None
    acknowledged_at_ns: int | None = None
    terminal_at_ns: int | None = None
    terminal_reported: bool = False
    cancel_requested_at_ns: int | None = None
    cancel_arrived_at_ns: int | None = None
    cancel_acknowledged_at_ns: int | None = None
    cancel_reason: str | None = None
    rejection: str | None = None
    queue_ahead_initial: Decimal | None = None
    queue_ahead_remaining: Decimal = ZERO
    queue_valid: bool = False
    queue_rebases: int = 0
    arrival_mid: Decimal | None = None
    fills: list[int] = field(default_factory=list)

    @property
    def remaining(self) -> Decimal:
        return self.quantity - self.filled_quantity


@dataclass
class _Fill:
    fill_id: str
    order_id: str
    symbol: str
    side: Literal["buy", "sell"]
    quantity: Decimal
    price: Decimal
    fee: Decimal
    fee_bps: Decimal
    liquidity: Literal["maker", "taker"]
    filled_at_ns: int
    midpoint_at_arrival: Decimal | None
    trigger_exchange_at_ns: int | None = None
    trigger_received_at_ns: int | None = None
    reported_at_ns: int | None = None
    mae_bps: float | None = None
    mfe_bps: float | None = None
    markouts: dict[str, dict[str, Any] | None] = field(
        default_factory=lambda: {"100ms": None, "1s": None, "5s": None}
    )


@dataclass
class _Position:
    symbol: str
    opened_at_ns: int
    quantity: Decimal = ZERO
    cost: Decimal = ZERO
    entry_fee_remaining: Decimal = ZERO
    entered_quantity: Decimal = ZERO
    exited_quantity: Decimal = ZERO
    gross_realized: Decimal = ZERO
    realized_fees: Decimal = ZERO
    mae_bps: float | None = None
    mfe_bps: float | None = None


class _Replay:
    def __init__(
        self, config: ScalpingConfig, latency: ReplayLatency, assumptions: _ExecutionAssumptions
    ) -> None:
        self.config, self.latency, self.assumptions = config, latency, assumptions
        self.market = BookFeatureEngine(config)
        self.strategy = ScalpStrategy(config)
        self.orders: dict[str, _Order] = {}
        self.fills: list[_Fill] = []
        self.positions: dict[str, _Position] = {}
        self.reported_positions: dict[str, ScalpInventory] = {}
        self.closed_positions: list[dict[str, Any]] = []
        self.signals: list[ScalpSignal] = []
        self.timers: list[tuple[int, int, str, str]] = []
        self.timer_sequence = 0
        self.now_ns = 0
        self.start_ns: int | None = None
        self.event_count = 0
        self.accepted_events = 0
        self.cash = ZERO
        self.gross_realized = ZERO
        self.realized_fees = ZERO
        self.fees_paid = ZERO
        self.data_latencies: list[float] = []
        self.unavailable_decisions: Counter[str] = Counter()
        self.consumed_depth: dict[tuple[str, str, Decimal], Decimal] = {}
        self._send_delay = _delay_ns(latency.decision_to_send_ms)
        self._arrival_delay = _delay_ns(latency.send_to_arrival_ms)
        self._ack_delay = _delay_ns(latency.arrival_to_ack_ms)
        self._cancel_delay = _delay_ns(latency.cancel_latency_ms)
        self._decision_interval = max(1, _delay_ns(config.decision_interval_seconds * 1000))

    def _schedule(self, at_ns: int, kind: str, reference: str = "") -> None:
        if at_ns < self.now_ns:
            raise ValueError("replay cannot schedule work in the past")
        self.timer_sequence += 1
        heapq.heappush(self.timers, (at_ns, self.timer_sequence, kind, reference))

    def _drain(self, until_ns: int, *, inclusive: bool) -> None:
        while self.timers and (
            self.timers[0][0] <= until_ns if inclusive else self.timers[0][0] < until_ns
        ):
            at_ns, _, kind, reference = heapq.heappop(self.timers)
            self.now_ns = at_ns
            self._timer(kind, reference)

    def _timer(self, kind: str, reference: str) -> None:
        if kind == "decision":
            self._decide()
            self._schedule(self.now_ns + self._decision_interval, "decision")
            return
        if kind == "fill_report":
            self._report_fill(int(reference))
            return
        order = self.orders[reference]
        if kind == "send":
            if order.status != "pending_send":
                return
            order.sent_at_ns, order.status = self.now_ns, "in_flight"
            self._schedule(order.arrival_due_ns, "arrival", reference)
        elif kind == "arrival":
            if order.status != "in_flight":
                return
            self._arrive(order)
        elif kind == "ack":
            order.acknowledged_at_ns = self.now_ns
            self._release(order)
        elif kind == "expiry":
            self._cancel(order, "entry_ttl")
        elif kind == "cancel_arrival":
            order.cancel_arrived_at_ns = self.now_ns
            if order.status not in _TERMINAL:
                order.status, order.terminal_at_ns = "canceled", self.now_ns
            self._schedule(self.now_ns + self._ack_delay, "cancel_ack", reference)
        elif kind == "cancel_ack":
            order.cancel_acknowledged_at_ns = self.now_ns
            self._release(order)
        else:
            raise ValueError("unknown replay timer")

    def _active(self, symbol: str) -> _Order | None:
        return next(
            (
                order
                for order in reversed(tuple(self.orders.values()))
                if order.symbol == symbol and not order.terminal_reported
            ),
            None,
        )

    def _decide(self) -> None:
        now = ns_datetime(self.now_ns)
        for symbol in self.config.symbols:
            active = self._active(symbol)
            features = self.market.features(symbol, now)
            if features is None:
                self.unavailable_decisions[symbol] += 1
                if active is not None and active.side == "buy":
                    self._cancel(active, "market_data_unavailable")
                continue
            signal = self.strategy.decide(
                features, inventory=self.reported_positions.get(symbol), now=now
            )
            self.signals.append(signal)
            if active is not None:
                if active.side == "buy" and (
                    signal.action == "sell"
                    or (
                        signal.action == "hold"
                        and "positive_alpha_maintained" not in signal.reasons
                    )
                ):
                    self._cancel(active, "entry_signal_invalidated")
                continue
            if signal.action == "hold":
                continue
            if signal.action == "buy":
                limit = signal.quote.bid if self.config.entry_style == "passive" else None
                price = limit if limit is not None else signal.quote.ask
                quantity = self.config.order_notional_usd / price
            else:
                owned = self.reported_positions.get(symbol)
                if owned is None or owned.quantity <= 0:
                    raise ValueError("strategy attempted an unsupported crypto short")
                quantity, limit = owned.quantity, None
            order_id = f"replay-{len(self.orders) + 1:08d}"
            order = _Order(
                order_id,
                symbol,
                signal.action,
                quantity,
                limit,
                signal,
                self.now_ns,
                self.now_ns + self._send_delay + self._arrival_delay,
            )
            self.orders[order_id] = order
            self._schedule(self.now_ns + self._send_delay, "send", order_id)
            if signal.action == "buy":
                self._schedule(
                    self.now_ns + _delay_ns(self.config.entry_order_ttl_seconds * 1000),
                    "expiry",
                    order_id,
                )

    def _current_quote(self, symbol: str) -> ScalpQuote | None:
        return self.market.quote_at_ns(symbol, self.now_ns)

    def _arrive(self, order: _Order) -> None:
        order.arrived_at_ns = self.now_ns
        self._schedule(self.now_ns + self._ack_delay, "ack", order.order_id)
        quote = self._current_quote(order.symbol)
        if quote is None:
            self._reject(order, "market_data_unavailable_at_arrival")
            return
        order.arrival_mid = (quote.bid + quote.ask) / 2
        if order.side == "sell":
            position = self.positions.get(order.symbol)
            if position is None or order.quantity > position.quantity:
                self._reject(order, "sell_exceeds_observed_owned_inventory")
                return
        if order.limit_price is not None and order.limit_price < quote.ask:
            order.status = "resting"
            self._rebase_queue(order)
        else:
            self._take_liquidity(order)

    def _reject(self, order: _Order, reason: str) -> None:
        order.status, order.rejection, order.terminal_at_ns = "rejected", reason, self.now_ns

    def _rebase_queue(self, order: _Order) -> None:
        visible = next(
            (
                level.quantity
                for level in self.market.levels(order.symbol, side="bid", depth=2000)
                if level.price == order.limit_price
            ),
            ZERO,
        )
        quote = self._current_quote(order.symbol)
        if quote is not None and quote.bid == order.limit_price:
            visible = max(visible, quote.bid_size)
        visible *= self.assumptions.queue_ahead_multiplier
        if order.queue_ahead_initial is None:
            order.queue_ahead_initial = visible
            order.queue_ahead_remaining = visible
        else:
            order.queue_ahead_remaining = max(order.queue_ahead_remaining, visible)
            order.queue_rebases += 1
        order.queue_valid = True

    def _take_liquidity(self, order: _Order) -> None:
        side: Literal["bid", "ask"] = "ask" if order.side == "buy" else "bid"
        quote = self._current_quote(order.symbol)
        if quote is None:
            self._reject(order, "market_data_unavailable_at_arrival")
            return
        for level in self.market.levels(order.symbol, side=side, depth=2000):
            if (side == "ask" and level.price < quote.ask) or (
                side == "bid" and level.price > quote.bid
            ):
                continue
            displayed = level.quantity
            if side == "ask" and level.price == quote.ask:
                displayed = min(displayed, quote.ask_size)
            elif side == "bid" and level.price == quote.bid:
                displayed = min(displayed, quote.bid_size)
            key = (order.symbol, side, level.price)
            available = max(
                ZERO,
                displayed * self.assumptions.participation_rate
                - self.consumed_depth.get(key, ZERO),
            )
            direction = Decimal(1 if order.side == "buy" else -1)
            price = level.price * (1 + direction * self.assumptions.slippage_bps / BPS)
            if order.limit_price is not None and price > order.limit_price:
                break
            quantity = min(order.remaining, available)
            if quantity > 0:
                self.consumed_depth[key] = self.consumed_depth.get(key, ZERO) + quantity
                self._fill(order, quantity, price, "taker")
            if order.remaining == 0:
                break
        if order.remaining:
            if order.limit_price is not None:
                order.status = "partially_filled" if order.filled_quantity else "resting"
                self._rebase_queue(order)
            else:
                # No synthetic liquidity past observed depth: market orders are modeled as IOC.
                order.status, order.terminal_at_ns = "canceled", self.now_ns
                order.cancel_reason = "modeled_ioc_unfilled_remainder"

    def _sync_depth(self, symbol: str) -> None:
        current = {
            (symbol, side, level.price): level.quantity * self.assumptions.participation_rate
            for side in ("bid", "ask")
            for level in self.market.levels(
                symbol, side="bid" if side == "bid" else "ask", depth=2000
            )
        }
        for key in [key for key in self.consumed_depth if key[0] == symbol]:
            if key not in current:
                del self.consumed_depth[key]
            else:
                self.consumed_depth[key] = min(self.consumed_depth[key], current[key])

    def _passive_trade(self, event: MarketEvent) -> None:
        if event.taker_side != "sell" or self._current_quote(event.symbol) is None:
            return
        price, volume = event.trade_price, event.trade_quantity
        if price is None or volume is None:
            raise ValueError("trade has no executable price or size")
        for order in self.orders.values():
            if (
                order.symbol != event.symbol
                or order.side != "buy"
                or order.status not in ("resting", "partially_filled")
                or order.limit_price is None
                or not order.queue_valid
                or price > order.limit_price
                or order.arrived_at_ns is None
                or event.exchange_at_ns < order.arrived_at_ns
            ):
                continue
            ahead = min(volume, order.queue_ahead_remaining)
            order.queue_ahead_remaining -= ahead
            volume -= ahead
            quantity = min(order.remaining, volume * self.assumptions.participation_rate)
            if quantity > 0:
                self._fill(order, quantity, order.limit_price, "maker", trigger=event)
                volume -= quantity
            if not volume:
                break

    def _fill(
        self,
        order: _Order,
        quantity: Decimal,
        price: Decimal,
        liquidity: Literal["maker", "taker"],
        *,
        trigger: MarketEvent | None = None,
    ) -> None:
        if quantity <= 0 or quantity > order.remaining:
            raise ValueError("invalid simulated fill quantity")
        fee_bps = self.config.maker_fee_bps if liquidity == "maker" else self.config.taker_fee_bps
        fee = quantity * price * fee_bps / BPS
        fill = _Fill(
            f"fill-{len(self.fills) + 1:08d}",
            order.order_id,
            order.symbol,
            order.side,
            quantity,
            price,
            fee,
            fee_bps,
            liquidity,
            self.now_ns,
            order.arrival_mid,
            trigger.exchange_at_ns if trigger is not None else None,
            trigger.received_at_ns if trigger is not None else None,
        )
        index = len(self.fills)
        self.fills.append(fill)
        order.fills.append(index)
        order.filled_quantity += quantity
        order.status = "filled" if order.remaining == 0 else "partially_filled"
        if order.status == "filled":
            order.terminal_at_ns = self.now_ns
        self.fees_paid += fee
        self._book_fill(fill)
        self._schedule(self.now_ns + self._ack_delay, "fill_report", str(index))

    def _book_fill(self, fill: _Fill) -> None:
        if fill.side == "buy":
            position = self.positions.setdefault(
                fill.symbol, _Position(fill.symbol, fill.filled_at_ns)
            )
            position.quantity += fill.quantity
            position.cost += fill.price * fill.quantity
            position.entered_quantity += fill.quantity
            position.entry_fee_remaining += fill.fee
            self.cash -= fill.price * fill.quantity + fill.fee
            return
        existing = self.positions.get(fill.symbol)
        if existing is None or existing.quantity < fill.quantity:
            raise ValueError("simulated fill would create a crypto short")
        position = existing
        fraction = fill.quantity / position.quantity
        basis = position.cost * fraction
        entry_fee = position.entry_fee_remaining * fraction
        gross = fill.price * fill.quantity - basis
        fees = entry_fee + fill.fee
        position.quantity -= fill.quantity
        position.cost -= basis
        position.entry_fee_remaining -= entry_fee
        position.exited_quantity += fill.quantity
        position.gross_realized += gross
        position.realized_fees += fees
        self.gross_realized += gross
        self.realized_fees += fees
        self.cash += fill.price * fill.quantity - fill.fee
        if position.quantity == 0:
            self.closed_positions.append(
                {
                    "symbol": fill.symbol,
                    "opened_at_ns": position.opened_at_ns,
                    "closed_at_ns": fill.filled_at_ns,
                    "hold_seconds": (fill.filled_at_ns - position.opened_at_ns) / NS,
                    "entered_quantity": position.entered_quantity,
                    "exited_quantity": position.exited_quantity,
                    "gross_pnl_usd": position.gross_realized,
                    "fees_usd": position.realized_fees,
                    "net_pnl_usd": position.gross_realized - position.realized_fees,
                    "mae_bps": position.mae_bps,
                    "mfe_bps": position.mfe_bps,
                }
            )
            del self.positions[fill.symbol]

    def _report_fill(self, index: int) -> None:
        fill = self.fills[index]
        fill.reported_at_ns = self.now_ns
        owned = self.reported_positions.get(fill.symbol)
        if fill.side == "buy":
            quantity = (owned.quantity if owned else ZERO) + fill.quantity
            cost = owned.quantity * owned.opening_vwap if owned else ZERO
            self.reported_positions[fill.symbol] = ScalpInventory(
                symbol=fill.symbol,
                quantity=quantity,
                opened_at=owned.opened_at if owned else ns_datetime(fill.filled_at_ns),
                opening_vwap=(cost + fill.price * fill.quantity) / quantity,
            )
        else:
            if owned is None or owned.quantity < fill.quantity:
                raise ValueError("fill feedback is inconsistent with reported inventory")
            remaining = owned.quantity - fill.quantity
            if remaining == 0:
                del self.reported_positions[fill.symbol]
            else:
                self.reported_positions[fill.symbol] = owned.model_copy(
                    update={"quantity": remaining}
                )
        self._release(self.orders[fill.order_id])

    def _release(self, order: _Order) -> None:
        if order.status not in _TERMINAL or order.acknowledged_at_ns is None:
            return
        if order.cancel_requested_at_ns is not None and order.cancel_acknowledged_at_ns is None:
            return
        if any(self.fills[index].reported_at_ns is None for index in order.fills):
            return
        order.terminal_reported = True

    def _cancel(self, order: _Order, reason: str) -> None:
        if order.status in _TERMINAL or order.cancel_requested_at_ns is not None:
            return
        order.cancel_requested_at_ns, order.cancel_reason = self.now_ns, reason
        if order.sent_at_ns is None:
            order.status, order.terminal_at_ns, order.terminal_reported = (
                "withdrawn",
                self.now_ns,
                True,
            )
            return
        self._schedule(
            max(self.now_ns + self._cancel_delay, order.arrival_due_ns),
            "cancel_arrival",
            order.order_id,
        )

    def _observe_markouts(self, symbol: str) -> None:
        quote = self._current_quote(symbol)
        if quote is None:
            return
        mid = (quote.bid + quote.ask) / 2
        for fill in self.fills:
            if fill.symbol != symbol or quote.exchange_time_ns < fill.filled_at_ns:
                continue
            direction = 1 if fill.side == "buy" else -1
            markout = float((mid / fill.price - 1) * BPS) * direction
            fill.mae_bps = min(fill.mae_bps or 0.0, markout)
            fill.mfe_bps = max(fill.mfe_bps or 0.0, markout)
            for name, horizon in (("100ms", NS // 10), ("1s", NS), ("5s", 5 * NS)):
                target = fill.filled_at_ns + horizon
                if fill.markouts[name] is None and quote.exchange_time_ns >= target:
                    fill.markouts[name] = {
                        "target_at_ns": target,
                        "observed_at_ns": self.now_ns,
                        "exchange_at_ns": quote.exchange_time_ns,
                        "observation_lag_ms": (quote.exchange_time_ns - target) / 1_000_000,
                        "midpoint": mid,
                        "gross_bps": markout,
                    }
        position = self.positions.get(symbol)
        if position is not None and quote.exchange_time_ns >= position.opened_at_ns:
            excursion = float((quote.bid / (position.cost / position.quantity) - 1) * BPS)
            position.mae_bps = min(position.mae_bps or 0.0, excursion)
            position.mfe_bps = max(position.mfe_bps or 0.0, excursion)

    def _event(self, event: MarketEvent) -> None:
        self.event_count += 1
        accepted = self.market.on_event(event)
        self.accepted_events += int(accepted)
        if event.event_type != "reset" and event.received_at_ns >= event.exchange_at_ns:
            self.data_latencies.append((event.received_at_ns - event.exchange_at_ns) / 1_000_000)
        if event.event_type == "reset" or self.market.quote(event.symbol) is None:
            for order in self.orders.values():
                if order.symbol == event.symbol:
                    order.queue_valid = False
        if not accepted:
            return
        if event.event_type == "book":
            self._sync_depth(event.symbol)
            for order in self.orders.values():
                if (
                    order.symbol == event.symbol
                    and order.limit_price is not None
                    and order.status in ("resting", "partially_filled")
                    and (event.reset or not order.queue_valid)
                    and self._current_quote(event.symbol) is not None
                ):
                    self._rebase_queue(order)
        elif event.event_type == "trade":
            self._passive_trade(event)
        self._observe_markouts(event.symbol)

    def run(self, events: Iterable[MarketEvent | Mapping[str, Any] | str]) -> dict[str, Any]:
        for received_ns, batch in groupby(_records(events), key=lambda event: event.received_at_ns):
            if self.start_ns is None:
                self.start_ns = received_ns
                self.now_ns = received_ns
                self._schedule((received_ns + 999) // 1000 * 1000, "decision")
            self._drain(received_ns, inclusive=False)
            self.now_ns = received_ns
            # Same-receipt market records precede actions arriving at that instant.
            for event in batch:
                self._event(event)
            self._drain(received_ns, inclusive=True)
            self.now_ns = received_ns
        return self._report()

    def _latencies(self) -> dict[str, Any]:
        pairs = {
            "decision_to_send": ("decision_at_ns", "sent_at_ns"),
            "send_to_arrival": ("sent_at_ns", "arrived_at_ns"),
            "arrival_to_ack": ("arrived_at_ns", "acknowledged_at_ns"),
            "cancel_round_trip": ("cancel_requested_at_ns", "cancel_acknowledged_at_ns"),
        }
        metrics: dict[str, Any] = {"market_receive": _percentiles(self.data_latencies)}
        for name, (start, end) in pairs.items():
            values = []
            for order in self.orders.values():
                first, last = getattr(order, start), getattr(order, end)
                if first is not None and last is not None:
                    values.append((last - first) / 1_000_000)
            metrics[name] = _percentiles(values)
        metrics["send_to_first_fill"] = _percentiles(
            (self.fills[order.fills[0]].filled_at_ns - order.sent_at_ns) / 1_000_000
            for order in self.orders.values()
            if order.sent_at_ns is not None and order.fills
        )
        metrics["fill_feedback"] = _percentiles(
            (fill.reported_at_ns - fill.filled_at_ns) / 1_000_000
            for fill in self.fills
            if fill.reported_at_ns is not None
        )
        return {
            "units": "milliseconds",
            "basis": (
                "recorded tape receipts and realized simulated lifecycle samples, not live SLAs"
            ),
            **metrics,
        }

    def _order_report(self, order: _Order) -> dict[str, Any]:
        times = {
            name: getattr(order, name)
            for name in (
                "decision_at_ns",
                "sent_at_ns",
                "arrived_at_ns",
                "acknowledged_at_ns",
                "terminal_at_ns",
                "cancel_requested_at_ns",
                "cancel_arrived_at_ns",
                "cancel_acknowledged_at_ns",
            )
        }
        return {
            "order_id": order.order_id,
            "symbol": order.symbol,
            "side": order.side,
            "quantity": order.quantity,
            "limit_price": order.limit_price,
            "filled_quantity": order.filled_quantity,
            "unfilled_quantity": order.remaining,
            "status": order.status,
            "terminal_reported": order.terminal_reported,
            "decision_id": order.decision.decision_id,
            "family": order.decision.family,
            "reasons": order.decision.reasons,
            "rejection": order.rejection,
            "cancel_reason": order.cancel_reason,
            "queue_ahead_initial": order.queue_ahead_initial,
            "queue_ahead_remaining": order.queue_ahead_remaining,
            "queue_valid": order.queue_valid,
            "queue_rebases": order.queue_rebases,
            "first_fill_at_ns": self.fills[order.fills[0]].filled_at_ns if order.fills else None,
            "last_fill_at_ns": self.fills[order.fills[-1]].filled_at_ns if order.fills else None,
            **times,
        }

    def _report(self) -> dict[str, Any]:
        orders = list(self.orders.values())
        total_quantity = sum((order.quantity for order in orders), ZERO)
        filled_quantity = sum((order.filled_quantity for order in orders), ZERO)
        open_positions = []
        market_value = ZERO
        all_marks_available = True
        for symbol, position in sorted(self.positions.items()):
            quote = self._current_quote(symbol)
            value = quote.bid * position.quantity if quote is not None else None
            if value is None:
                all_marks_available = False
            else:
                market_value += value
            open_positions.append(
                {
                    "symbol": symbol,
                    "quantity": position.quantity,
                    "opening_vwap": position.cost / position.quantity,
                    "opened_at_ns": position.opened_at_ns,
                    "observed_hold_seconds": (self.now_ns - position.opened_at_ns) / NS,
                    "remaining_entry_fees_usd": position.entry_fee_remaining,
                    "mark_bid": quote.bid if quote is not None else None,
                    "unrealized_gross_usd": value - position.cost if value is not None else None,
                    "unrealized_net_after_entry_fees_usd": (
                        value - position.cost - position.entry_fee_remaining
                        if value is not None
                        else None
                    ),
                    "mae_bps": position.mae_bps,
                    "mfe_bps": position.mfe_bps,
                }
            )
        report = {
            "schema_version": 1,
            "profile": self.config.profile,
            "config_hash": self.config.identity,
            "profitability_validated": False,
            "expected_net_edge_bps": None,
            "started_at_ns": self.start_ns,
            "ended_at_ns": self.now_ns if self.start_ns else None,
            "events": self.event_count,
            "accepted_events": self.accepted_events,
            "decision_count": len(self.signals),
            "decisions": [signal.model_dump(mode="json") for signal in self.signals],
            "unavailable_decision_counts": dict(self.unavailable_decisions),
            "assumptions": {
                "latency": self.latency.model_dump(mode="json"),
                **self.assumptions.model_dump(mode="json"),
                "ordering": "canonical receive order, never sorted by exchange timestamps",
                "tie_order": "market records, then scheduled actions at the same receipt instant",
                "passive_queue": (
                    "visible depth ahead; only native sell trade volume advances buy queues; "
                    "cancellations never assumed ahead; snapshots rebase conservatively"
                ),
                "exact_queue_position_available": False,
                "exchange_gap_coverage": "unprovable without an exchange sequence",
                "unknown_taker_side": "does not fill a passive order",
                "aggressive_fill": (
                    "BBO-consistent observed L2 depth, adverse slippage, "
                    "IOC unfilled market remainder"
                ),
                "market_impact": "not modeled; historical tape is exogenous",
                "fees": "actual simulated partial-fill notional times configured maker/taker bps",
                "markouts": "first subsequently observed valid midpoint at/after each horizon",
                "fill_clock": (
                    "passive fills are modeled when the eligible print is received, not backdated; "
                    "late prints received after cancellation do not reconstruct lost queue priority"
                ),
                "position_excursions": (
                    "observed executable bid versus remaining entry VWAP; signed MAE <= 0, MFE >= 0"
                ),
                "end_of_tape": "no forced fills, acknowledgements, cancellations, marks or closes",
            },
            "orders": [self._order_report(order) for order in orders],
            "fills": [
                {
                    "fill_id": fill.fill_id,
                    "order_id": fill.order_id,
                    "symbol": fill.symbol,
                    "side": fill.side,
                    "quantity": fill.quantity,
                    "price": fill.price,
                    "fee_usd": fill.fee,
                    "fee_bps": fill.fee_bps,
                    "liquidity": fill.liquidity,
                    "filled_at_ns": fill.filled_at_ns,
                    "reported_at_ns": fill.reported_at_ns,
                    "trigger_exchange_at_ns": fill.trigger_exchange_at_ns,
                    "trigger_received_at_ns": fill.trigger_received_at_ns,
                    "midpoint_at_arrival": fill.midpoint_at_arrival,
                    "execution_cost_from_arrival_mid_bps": (
                        float((fill.price / fill.midpoint_at_arrival - 1) * BPS)
                        * (1 if fill.side == "buy" else -1)
                        if fill.midpoint_at_arrival
                        else None
                    ),
                    "mae_bps": fill.mae_bps,
                    "mfe_bps": fill.mfe_bps,
                    "markouts": fill.markouts,
                }
                for fill in self.fills
            ],
            "closed_positions": self.closed_positions,
            "open_positions": open_positions,
            "reported_inventory": [
                inventory.model_dump(mode="json")
                for _, inventory in sorted(self.reported_positions.items())
            ],
            "open_order_count": sum(order.status not in _TERMINAL for order in orders),
            "unreported_terminal_count": sum(
                order.status in _TERMINAL and not order.terminal_reported for order in orders
            ),
            "unfilled_order_count": sum(order.remaining > 0 for order in orders),
            "rates": {
                "orders_with_fill": sum(bool(order.fills) for order in orders) / len(orders)
                if orders
                else None,
                "fully_filled_orders": sum(order.status == "filled" for order in orders)
                / len(orders)
                if orders
                else None,
                "canceled_orders": sum(
                    order.status in ("canceled", "withdrawn") for order in orders
                )
                / len(orders)
                if orders
                else None,
                "rejected_orders": sum(order.status == "rejected" for order in orders) / len(orders)
                if orders
                else None,
                "quantity_fill_rate": float(filled_quantity / total_quantity)
                if total_quantity
                else None,
            },
            "pnl": {
                "realized_gross_usd": self.gross_realized,
                "realized_allocated_fees_usd": self.realized_fees,
                "realized_net_usd": self.gross_realized - self.realized_fees,
                "fees_paid_usd": self.fees_paid,
                "cash_delta_usd": self.cash,
                "net_mark_to_bid_usd": self.cash + market_value if all_marks_available else None,
                "open_marks_include_future_exit_fees": False,
            },
            "hold_seconds": _percentiles(
                position["hold_seconds"] for position in self.closed_positions
            ),
            "latency_ms": self._latencies(),
            "markout_coverage": {
                name: {
                    "observed": sum(fill.markouts[name] is not None for fill in self.fills),
                    "censored": sum(fill.markouts[name] is None for fill in self.fills),
                }
                for name in ("100ms", "1s", "5s")
            },
            "market_health": self.market.health_snapshot(),
        }
        return {key: _json_value(value) for key, value in report.items()}


def replay_events(
    events: Iterable[MarketEvent | Mapping[str, Any] | str],
    config: ScalpingConfig,
    latency_ms: float = 20.0,
    *,
    latency: ReplayLatency | None = None,
    slippage_bps: Decimal = ZERO,
    queue_ahead_multiplier: Decimal = Decimal(1),
    participation_rate: Decimal = Decimal(1),
) -> dict[str, Any]:
    """Replay canonical JSONL records in receipt order without broker access or future closures."""
    if not math.isfinite(latency_ms) or latency_ms < 0:
        raise ValueError("latency_ms must be finite and nonnegative")
    timing = latency or ReplayLatency(
        send_to_arrival_ms=latency_ms, arrival_to_ack_ms=latency_ms, cancel_latency_ms=latency_ms
    )
    assumptions = _ExecutionAssumptions(
        slippage_bps=slippage_bps,
        queue_ahead_multiplier=queue_ahead_multiplier,
        participation_rate=participation_rate,
    )
    return _Replay(config, timing, assumptions).run(events)
