from __future__ import annotations

import json
import zlib
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import select

from tradeagent.persistence import Database, events
from tradeagent.scalping_config import ScalpingConfig, ScalpQuote, ScalpSignal
from tradeagent.scalping_economics import (
    CostEstimate,
    EconomicCosts,
    EconomicSample,
    project_economic_features,
)
from tradeagent.scalping_market import (
    NS,
    BookFeatureEngine,
    BookLevel,
    MarketEvent,
    datetime_ns,
    ns_datetime,
)
from tradeagent.scalping_store import (
    ScalpStore,
    scalping_cycles,
    scalping_market_batches,
    scalping_order_links,
)

ZERO = Decimal(0)
BPS = Decimal(10000)
ShadowAction = Literal["PASSIVE_BUY", "AGGRESSIVE_BUY"]


def _digest(value: object) -> str:
    return sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def _levels_value(
    levels: Sequence[BookLevel], *, side: Literal["bid", "ask"], quantity: Decimal
) -> tuple[Decimal, Decimal]:
    remaining = quantity
    value = ZERO
    filled = ZERO
    for level in levels:
        available = max(ZERO, level.quantity)
        take = min(remaining, available)
        value += take * level.price
        filled += take
        remaining -= take
        if remaining <= 0:
            break
    return filled, value


def _with_bbo(
    quote: ScalpQuote | None,
    levels: Sequence[BookLevel],
    *,
    side: Literal["bid", "ask"],
) -> tuple[BookLevel, ...]:
    if quote is None:
        return tuple(levels)
    price = quote.bid if side == "bid" else quote.ask
    quantity = quote.bid_size if side == "bid" else quote.ask_size
    deeper = [level for level in levels if level.price != price]
    return (BookLevel(price=price, quantity=quantity), *deeper)


class ShadowPolicy(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    schema_version: Literal["shadow-action-policy-v1"] = "shadow-action-policy-v1"
    arrival_latency_ms: int = Field(default=350, ge=0)
    horizon_quote_max_age_ms: int = Field(default=250, gt=0)
    arrival_market_max_age_ms: int = Field(default=250, gt=0)
    queue_ahead_multiplier: Decimal = Field(default=Decimal(1), ge=1)
    participation_rate: Decimal = Field(default=Decimal(1), gt=0, le=1)
    minimum_validation_candidates: int = Field(default=100, ge=20)
    minimum_precision: float = Field(default=0.80, ge=0, le=1)
    maximum_false_positive_rate: float = Field(default=0.10, ge=0, le=1)
    provenance: str = (
        "350ms exceeds the historical p95 dispatch-start to first broker observation "
        "(331.554ms); cancellations never reduce passive queue ahead"
    )

    @property
    def identity(self) -> str:
        return _digest(self.model_dump(mode="json"))


class ShadowCandidate(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    candidate_id: str
    run_id: str
    account_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    signal: ScalpSignal
    shadow_send_at: datetime
    shadow_send_at_ns: int
    quantity: Decimal = Field(gt=0)
    requested_notional_usd: Decimal = Field(gt=0)
    passive_limit: Decimal = Field(gt=0)
    aggressive_limit: Decimal = Field(gt=0)
    passive_queue_ahead: Decimal = Field(ge=0)
    horizon_seconds: float = Field(gt=0)
    cancellation_horizon_seconds: float = Field(gt=0)
    decision_bid_levels: tuple[BookLevel, ...]
    decision_ask_levels: tuple[BookLevel, ...]
    actual_filled: bool | None = None
    actual_first_fill_at: datetime | None = None
    actual_passive_comparable: bool = False

    @model_validator(mode="after")
    def causal(self) -> ShadowCandidate:
        if (
            self.shadow_send_at < self.signal.observed_at
            or datetime_ns(self.shadow_send_at) != self.shadow_send_at_ns // 1000 * 1000
        ):
            raise ValueError("shadow send boundary must follow the recorded decision")
        if self.signal.family == "none":
            raise ValueError("neutral observations are not shadow action candidates")
        return self


@dataclass
class _ActionState:
    action: ShadowAction
    arrival_at_ns: int
    expires_at_ns: int
    quantity: Decimal
    limit_price: Decimal
    queue_ahead: Decimal = ZERO
    sent: bool = False
    rejected_reason: str | None = None
    entry_quantity: Decimal = ZERO
    entry_value: Decimal = ZERO
    first_fill_at_ns: int | None = None
    last_fill_at_ns: int | None = None


@dataclass
class _Pending:
    candidate: ShadowCandidate
    policy: ShadowPolicy
    actions: dict[ShadowAction, _ActionState]
    horizon_at_ns: int
    horizon_quote: ScalpQuote | None = None
    horizon_bid_levels: tuple[BookLevel, ...] = ()
    completed: bool = False


class ShadowActionOutcome(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    schema_version: Literal["shadow-action-outcome-v1"] = "shadow-action-outcome-v1"
    candidate_id: str
    run_id: str
    account_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    decision_id: str
    symbol: str
    family: Literal["momentum", "reversion"]
    features: dict[str, float | int | str | None]
    action: ShadowAction
    decision_at: datetime
    shadow_send_at: datetime
    decision_latency_seconds: float = Field(ge=0)
    arrival_at: datetime
    horizon_at: datetime
    quantity: Decimal
    entry_quantity: Decimal
    entry_value: Decimal
    filled: bool
    filled_fraction: float = Field(ge=0, le=1)
    fill_delay_seconds: float | None = Field(default=None, ge=0)
    exit_quantity: Decimal
    exit_value: Decimal
    gross_return_bps: float | None
    directional_return_bps: float | None
    decision_mid: Decimal
    horizon_mid: Decimal | None
    quote_age_seconds: float = Field(ge=0)
    spread_bps: float = Field(ge=0)
    notional_usd: Decimal = Field(gt=0)
    complete: bool
    missing_reasons: tuple[str, ...]
    simulation_policy: ShadowPolicy
    source_event_first: str
    source_event_last: str
    source_event_count: int = Field(ge=1)
    source_event_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    actual_filled: bool | None
    actual_first_fill_at: datetime | None
    actual_passive_comparable: bool

    @property
    def identity(self) -> str:
        return _digest(self.model_dump(mode="json"))


class ShadowActionEvaluator:
    """No-order passive/aggressive outcomes from already-received market evidence."""

    def __init__(self, policy: ShadowPolicy | None = None) -> None:
        self.policy = policy or ShadowPolicy()
        self._pending: dict[str, _Pending] = {}
        self._last_quote: dict[str, ScalpQuote] = {}
        self._last_bids: dict[str, tuple[BookLevel, ...]] = {}
        self._last_asks: dict[str, tuple[BookLevel, ...]] = {}
        self._source_ids: dict[str, list[str]] = {}
        self._ready: dict[str, ShadowActionOutcome] = {}

    def add(self, candidate: ShadowCandidate) -> None:
        if candidate.candidate_id in self._pending:
            return
        decision_ns = datetime_ns(candidate.signal.observed_at)
        horizon = decision_ns + int(candidate.horizon_seconds * NS)
        arrival = candidate.shadow_send_at_ns + self.policy.arrival_latency_ms * 1_000_000
        ttl = decision_ns + int(candidate.cancellation_horizon_seconds * NS)
        expiry = min(ttl, horizon)
        self._pending[candidate.candidate_id] = _Pending(
            candidate=candidate,
            policy=self.policy,
            actions={
                "PASSIVE_BUY": _ActionState(
                    "PASSIVE_BUY",
                    arrival,
                    expiry,
                    candidate.quantity,
                    candidate.passive_limit,
                    candidate.passive_queue_ahead * self.policy.queue_ahead_multiplier,
                ),
                "AGGRESSIVE_BUY": _ActionState(
                    "AGGRESSIVE_BUY",
                    arrival,
                    expiry,
                    candidate.quantity,
                    candidate.aggressive_limit,
                ),
            },
            horizon_at_ns=horizon,
        )
        self._source_ids[candidate.candidate_id] = [
            str(candidate.signal.features["source_event_id"])
        ]

    def on_market(
        self,
        event: MarketEvent,
        *,
        quote: ScalpQuote | None,
        bid_levels: Sequence[BookLevel],
        ask_levels: Sequence[BookLevel],
    ) -> tuple[ShadowActionOutcome, ...]:
        if quote is not None:
            self._last_quote[event.symbol] = quote
            self._last_bids[event.symbol] = _with_bbo(quote, bid_levels, side="bid")
            self._last_asks[event.symbol] = _with_bbo(quote, ask_levels, side="ask")
        completed: list[ShadowActionOutcome] = list(self._ready.values())
        for identity, pending in tuple(self._pending.items()):
            if pending.candidate.signal.symbol != event.symbol:
                continue
            self._source_ids[identity].append(event.event_id)
            for state in pending.actions.values():
                self._advance_action(state, pending, event, quote, bid_levels, ask_levels)
            if event.received_at_ns <= pending.horizon_at_ns and quote is not None:
                pending.horizon_quote = quote
                pending.horizon_bid_levels = _with_bbo(quote, bid_levels, side="bid")
            if event.received_at_ns >= pending.horizon_at_ns:
                for outcome in self._finish(pending):
                    self._ready[outcome.identity] = outcome
                    completed.append(outcome)
                del self._pending[identity]
        return tuple({row.identity: row for row in completed}.values())

    def flush(self, observed_at: datetime) -> tuple[ShadowActionOutcome, ...]:
        now_ns = datetime_ns(observed_at)
        completed: list[ShadowActionOutcome] = list(self._ready.values())
        for identity, pending in tuple(self._pending.items()):
            if now_ns < pending.horizon_at_ns:
                continue
            for outcome in self._finish(pending):
                self._ready[outcome.identity] = outcome
                completed.append(outcome)
            del self._pending[identity]
        return tuple({row.identity: row for row in completed}.values())

    def acknowledge(self, outcomes: Iterable[ShadowActionOutcome]) -> None:
        for outcome in outcomes:
            self._ready.pop(outcome.identity, None)

    def _advance_action(
        self,
        state: _ActionState,
        pending: _Pending,
        event: MarketEvent,
        quote: ScalpQuote | None,
        bid_levels: Sequence[BookLevel],
        ask_levels: Sequence[BookLevel],
    ) -> None:
        if state.rejected_reason or event.received_at_ns < state.arrival_at_ns:
            return
        if not state.sent:
            if event.received_at_ns > state.expires_at_ns:
                state.rejected_reason = "simulated_arrival_after_action_expiry"
                return
            state.sent = True
            quote_age_ms = (
                (event.received_at_ns - quote.exchange_time_ns) / 1_000_000
                if quote is not None
                else float("inf")
            )
            if quote is None or not 0 <= quote_age_ms <= pending.policy.arrival_market_max_age_ms:
                state.rejected_reason = "no_fresh_market_at_simulated_arrival"
                return
            if state.action == "PASSIVE_BUY":
                if quote.bid != state.limit_price:
                    state.rejected_reason = "passive_touch_changed_before_arrival"
                    return
                state.queue_ahead = max(
                    state.queue_ahead,
                    next(
                        (
                            level.quantity
                            for level in bid_levels
                            if level.price == state.limit_price
                        ),
                        quote.bid_size,
                    )
                    * pending.policy.queue_ahead_multiplier,
                )
            else:
                if quote.ask > state.limit_price:
                    state.rejected_reason = "aggressive_limit_missed_before_arrival"
                    return
                eligible = tuple(
                    level
                    for level in _with_bbo(quote, ask_levels, side="ask")
                    if level.price <= state.limit_price
                )
                quantity, value = _levels_value(
                    eligible,
                    side="ask",
                    quantity=state.quantity * pending.policy.participation_rate,
                )
                if quantity <= 0:
                    state.rejected_reason = "aggressive_observed_depth_unavailable"
                    return
                state.entry_quantity = quantity
                state.entry_value = value
                state.first_fill_at_ns = state.last_fill_at_ns = event.received_at_ns
        if (
            state.action != "PASSIVE_BUY"
            or state.entry_quantity >= state.quantity
            or event.received_at_ns > state.expires_at_ns
            or event.event_type != "trade"
            or event.taker_side != "sell"
            or event.trade_price is None
            or event.trade_quantity is None
            or event.trade_price > state.limit_price
        ):
            return
        available = event.trade_quantity * pending.policy.participation_rate
        ahead = min(available, state.queue_ahead)
        state.queue_ahead -= ahead
        available -= ahead
        fill = min(state.quantity - state.entry_quantity, available)
        if fill > 0:
            state.entry_quantity += fill
            state.entry_value += fill * state.limit_price
            state.first_fill_at_ns = state.first_fill_at_ns or event.received_at_ns
            state.last_fill_at_ns = event.received_at_ns

    def _finish(self, pending: _Pending) -> list[ShadowActionOutcome]:
        result = []
        quote = pending.horizon_quote
        decision = pending.candidate.signal.quote
        decision_mid = (decision.bid + decision.ask) / 2
        source_ids = tuple(dict.fromkeys(self._source_ids[pending.candidate.candidate_id]))
        source_hash = _digest(source_ids)
        for state in pending.actions.values():
            missing = []
            age_seconds = 0.0
            horizon_mid = None
            directional = None
            exit_quantity = exit_value = ZERO
            gross = None
            if quote is None:
                missing.append("no_horizon_quote")
            else:
                age_seconds = max(
                    0.0,
                    (pending.horizon_at_ns - quote.exchange_time_ns) / NS,
                )
                if age_seconds * 1000 > pending.policy.horizon_quote_max_age_ms:
                    missing.append("horizon_quote_too_old")
                else:
                    horizon_mid = (quote.bid + quote.ask) / 2
                    directional = float((horizon_mid / decision_mid - 1) * BPS)
            if state.entry_quantity > 0 and not missing:
                exit_quantity, exit_value = _levels_value(
                    pending.horizon_bid_levels,
                    side="bid",
                    quantity=state.entry_quantity * pending.policy.participation_rate,
                )
                if exit_quantity < state.entry_quantity:
                    missing.append("insufficient_observed_exit_depth")
                elif state.entry_value > 0:
                    gross = float(
                        (
                            (exit_value / exit_quantity)
                            / (state.entry_value / state.entry_quantity)
                            - 1
                        )
                        * BPS
                    )
            if state.rejected_reason:
                missing.append(state.rejected_reason)
            completed = not missing and (
                state.entry_quantity == 0 or exit_quantity == state.entry_quantity
            )
            result.append(
                ShadowActionOutcome(
                    candidate_id=pending.candidate.candidate_id,
                    run_id=pending.candidate.run_id,
                    account_digest=pending.candidate.account_digest,
                    decision_id=pending.candidate.signal.decision_id,
                    symbol=pending.candidate.signal.symbol,
                    family=pending.candidate.signal.family,  # type: ignore[arg-type]
                    features=dict(project_economic_features(pending.candidate.signal.features)),
                    action=state.action,
                    decision_at=pending.candidate.signal.observed_at,
                    shadow_send_at=pending.candidate.shadow_send_at,
                    decision_latency_seconds=max(
                        0.0,
                        (
                            pending.candidate.shadow_send_at_ns
                            - datetime_ns(pending.candidate.signal.observed_at)
                        )
                        / NS,
                    ),
                    arrival_at=ns_datetime(state.arrival_at_ns),
                    horizon_at=ns_datetime(pending.horizon_at_ns),
                    quantity=state.quantity,
                    entry_quantity=state.entry_quantity,
                    entry_value=state.entry_value,
                    filled=state.entry_quantity > 0,
                    filled_fraction=float(state.entry_quantity / state.quantity),
                    fill_delay_seconds=(
                        (state.first_fill_at_ns - datetime_ns(pending.candidate.signal.observed_at))
                        / NS
                        if state.first_fill_at_ns is not None
                        else None
                    ),
                    exit_quantity=exit_quantity,
                    exit_value=exit_value,
                    gross_return_bps=gross,
                    directional_return_bps=directional,
                    decision_mid=decision_mid,
                    horizon_mid=horizon_mid,
                    quote_age_seconds=max(
                        0.0,
                        (
                            datetime_ns(pending.candidate.signal.observed_at)
                            - decision.exchange_time_ns
                        )
                        / NS,
                    ),
                    spread_bps=float((decision.ask - decision.bid) / decision_mid * BPS),
                    notional_usd=pending.candidate.requested_notional_usd,
                    complete=completed,
                    missing_reasons=tuple(sorted(set(missing))),
                    simulation_policy=pending.policy,
                    source_event_first=source_ids[0],
                    source_event_last=source_ids[-1],
                    source_event_count=len(source_ids),
                    source_event_sha256=source_hash,
                    actual_filled=pending.candidate.actual_filled,
                    actual_first_fill_at=pending.candidate.actual_first_fill_at,
                    actual_passive_comparable=pending.candidate.actual_passive_comparable,
                )
            )
        return result

    @property
    def pending_count(self) -> int:
        return len(self._pending)

    @property
    def unacknowledged_count(self) -> int:
        return len(self._ready)


def outcome_sample(
    outcome: ShadowActionOutcome,
    *,
    source_sha256: str,
    execution_validation_sha256: str | None,
    maker_fee_bps: float,
    taker_fee_bps: float,
) -> EconomicSample:
    embedded = CostEstimate(
        included_in_return=True,
        kind="embedded",
        provenance="simulated entry and exit prices already include this component",
    )
    fees = CostEstimate(
        bps=max(maker_fee_bps, taker_fee_bps) + taker_fee_bps,
        kind="estimated",
        provenance=(
            "entry_liquidity_unconfirmed_use_worst_configured_fee; "
            "cash_exit_uses_configured_taker_fee"
        ),
    )
    return EconomicSample(
        sample_id=f"{outcome.candidate_id}:{outcome.action}",
        account_digest=outcome.account_digest,
        source_sha256=source_sha256,
        symbol=outcome.symbol,
        family=outcome.family,
        action=outcome.action,
        decision_at=outcome.decision_at,
        feature_observed_at=outcome.decision_at,
        label_matured_at=outcome.horizon_at,
        return_end_at=outcome.horizon_at if outcome.filled else None,
        horizon_seconds=(outcome.horizon_at - outcome.decision_at).total_seconds(),
        cancellation_horizon_seconds=min(
            3.0, (outcome.horizon_at - outcome.decision_at).total_seconds()
        ),
        features=outcome.features,
        quote_age_seconds=outcome.quote_age_seconds,
        decision_latency_seconds=outcome.decision_latency_seconds,
        spread_bps=outcome.spread_bps,
        notional_usd=float(outcome.notional_usd),
        filled=outcome.filled,
        filled_fraction=outcome.filled_fraction,
        fill_delay_seconds=outcome.fill_delay_seconds,
        gross_return_bps=outcome.gross_return_bps,
        directional_return_bps=outcome.directional_return_bps,
        return_basis="fill_to_fill",
        costs=EconomicCosts(
            fees=fees,
            spread=embedded,
            slippage=embedded,
            impact=embedded,
            adverse_selection=embedded,
        ),
        execution_evidence=(
            "validated_simulation"
            if execution_validation_sha256 is not None
            else "unsupported_counterfactual"
        ),
        execution_validation_sha256=execution_validation_sha256,
    )


def validate_passive_simulation(
    outcomes: Sequence[ShadowActionOutcome], policy: ShadowPolicy
) -> dict[str, Any]:
    rows = [
        row
        for row in outcomes
        if row.action == "PASSIVE_BUY"
        and row.actual_passive_comparable
        and row.actual_filled is not None
    ]
    counts = Counter(
        (
            "true_positive"
            if row.filled and row.actual_filled
            else "false_positive"
            if row.filled and not row.actual_filled
            else "false_negative"
            if not row.filled and row.actual_filled
            else "true_negative"
        )
        for row in rows
    )
    predicted_positive = counts["true_positive"] + counts["false_positive"]
    actual_negative = counts["false_positive"] + counts["true_negative"]
    precision = counts["true_positive"] / predicted_positive if predicted_positive else 0.0
    false_positive_rate = counts["false_positive"] / actual_negative if actual_negative else 0.0
    passed = (
        len(rows) >= policy.minimum_validation_candidates
        and precision >= policy.minimum_precision
        and false_positive_rate <= policy.maximum_false_positive_rate
    )
    population = sorted(row.candidate_id for row in rows)
    policy_ids = sorted({row.simulation_policy.identity for row in rows})
    symbols = sorted({row.symbol for row in rows})
    payload = {
        "schema": "shadow-execution-validation-v1",
        "policy": policy.model_dump(mode="json"),
        "policy_ids": policy_ids,
        "symbols": symbols,
        "candidate_ids_sha256": _digest(population),
        "candidate_count": len(rows),
        "confusion": dict(counts),
        "precision": precision,
        "false_positive_rate": false_positive_rate,
        "minimum_precision": policy.minimum_precision,
        "maximum_false_positive_rate": policy.maximum_false_positive_rate,
        "passed": passed,
        "aggressive_execution_validated": False,
        "limitations": [
            "Aggregate L2 cannot prove exact queue position or cancellation priority.",
            "Only passive intent has historical broker outcomes; aggressive remains unsupported.",
        ],
    }
    payload["validation_sha256"] = _digest(payload)
    return payload


def persist_shadow_outcomes(
    store: ScalpStore, outcomes: Iterable[ShadowActionOutcome], *, at: datetime
) -> int:
    rows = list(outcomes)
    count = 0
    for start in range(0, len(rows), 100):
        with store.database.begin() as connection:
            for outcome in rows[start : start + 100]:
                store.audit(
                    "shadow_action_outcome",
                    outcome.model_dump(mode="json"),
                    at=at,
                    connection=connection,
                    identity=f"shadow-action:{outcome.identity}",
                )
                count += 1
    return count


def recover_abandoned_candidates(
    database: Database,
    *,
    run_id: str,
    account_digest: str,
    config: ScalpingConfig,
    now: datetime,
    limit: int = 1000,
) -> int:
    if limit < 1:
        raise ValueError("recovery limit must be positive")
    cutoff = now - timedelta(seconds=config.feature_horizon_seconds + 1)
    with database.begin() as connection:
        candidate_payloads = [
            row
            for row in connection.scalars(
                select(events.c.payload)
                .where(
                    events.c.event_type == "scalp_shadow_action_candidate",
                    events.c.payload["run_id"].as_string() == run_id,
                    events.c.occurred_at <= cutoff,
                )
                .order_by(events.c.occurred_at)
                .limit(limit)
            )
        ]
        outcome_ids = set(
            connection.scalars(
                select(events.c.payload["candidate_id"].as_string()).where(
                    events.c.event_type == "scalp_shadow_action_outcome",
                    events.c.payload["run_id"].as_string() == run_id,
                )
            )
        )
    store = ScalpStore(database)
    recovered = 0
    for payload in candidate_payloads:
        candidate = ShadowCandidate.model_validate(payload)
        if candidate.candidate_id in outcome_ids:
            continue
        decision = candidate.signal.quote
        mid = (decision.bid + decision.ask) / 2
        source = str(candidate.signal.features["source_event_id"])
        for action in ("PASSIVE_BUY", "AGGRESSIVE_BUY"):
            outcome = ShadowActionOutcome(
                candidate_id=candidate.candidate_id,
                run_id=run_id,
                account_digest=account_digest,
                decision_id=candidate.signal.decision_id,
                symbol=candidate.signal.symbol,
                family=candidate.signal.family,  # type: ignore[arg-type]
                features=dict(project_economic_features(candidate.signal.features)),
                action=action,
                decision_at=candidate.signal.observed_at,
                shadow_send_at=candidate.shadow_send_at,
                decision_latency_seconds=max(
                    0.0,
                    (candidate.shadow_send_at_ns - datetime_ns(candidate.signal.observed_at)) / NS,
                ),
                arrival_at=ns_datetime(
                    candidate.shadow_send_at_ns + ShadowPolicy().arrival_latency_ms * 1_000_000
                ),
                horizon_at=candidate.signal.observed_at
                + timedelta(seconds=candidate.horizon_seconds),
                quantity=candidate.quantity,
                entry_quantity=ZERO,
                entry_value=ZERO,
                filled=False,
                filled_fraction=0,
                fill_delay_seconds=None,
                exit_quantity=ZERO,
                exit_value=ZERO,
                gross_return_bps=None,
                directional_return_bps=None,
                decision_mid=mid,
                horizon_mid=None,
                quote_age_seconds=max(
                    0.0,
                    (datetime_ns(candidate.signal.observed_at) - decision.exchange_time_ns) / NS,
                ),
                spread_bps=float((decision.ask - decision.bid) / mid * BPS),
                notional_usd=candidate.requested_notional_usd,
                complete=False,
                missing_reasons=("process_restarted_before_shadow_outcome",),
                simulation_policy=ShadowPolicy(),
                source_event_first=source,
                source_event_last=source,
                source_event_count=1,
                source_event_sha256=_digest((source,)),
                actual_filled=candidate.actual_filled,
                actual_first_fill_at=candidate.actual_first_fill_at,
                actual_passive_comparable=candidate.actual_passive_comparable,
            )
            store.audit(
                "shadow_action_outcome",
                outcome.model_dump(mode="json"),
                at=now,
                identity=f"shadow-action:{outcome.identity}",
            )
            recovered += 1
    return recovered


def candidate_from_signal(
    signal: ScalpSignal,
    *,
    run_id: str,
    account_digest: str,
    config: ScalpingConfig,
    shadow_send_at: datetime,
    bid_levels: Sequence[BookLevel] = (),
    ask_levels: Sequence[BookLevel] = (),
    actual_filled: bool | None = None,
    actual_first_fill_at: datetime | None = None,
    actual_passive_comparable: bool = False,
) -> ShadowCandidate:
    if signal.family == "none":
        raise ValueError("a shadow action candidate requires a momentum or reversion state")
    quantity = config.order_notional_usd / signal.quote.ask
    passive_queue = max(
        signal.quote.bid_size,
        next(
            (level.quantity for level in bid_levels if level.price == signal.quote.bid),
            ZERO,
        ),
    )
    return ShadowCandidate(
        candidate_id=_digest(
            {
                "run_id": run_id,
                "decision_id": signal.decision_id,
                "policy": "passive_and_aggressive_shadow_v1",
            }
        ),
        run_id=run_id,
        account_digest=account_digest,
        signal=signal,
        shadow_send_at=shadow_send_at,
        shadow_send_at_ns=datetime_ns(shadow_send_at),
        quantity=quantity,
        requested_notional_usd=config.order_notional_usd,
        passive_limit=signal.quote.bid,
        aggressive_limit=signal.quote.ask,
        passive_queue_ahead=passive_queue,
        horizon_seconds=config.feature_horizon_seconds,
        cancellation_horizon_seconds=config.entry_order_ttl_seconds,
        decision_bid_levels=tuple(bid_levels),
        decision_ask_levels=tuple(ask_levels),
        actual_filled=actual_filled,
        actual_first_fill_at=actual_first_fill_at,
        actual_passive_comparable=actual_passive_comparable,
    )


def simulate_candidates(
    candidates: Sequence[ShadowCandidate],
    market_events: Iterable[MarketEvent],
    config: ScalpingConfig,
    policy: ShadowPolicy | None = None,
) -> tuple[ShadowActionOutcome, ...]:
    evaluator = ShadowActionEvaluator(policy)
    engine = BookFeatureEngine(config, stale_after_seconds=config.feature_horizon_seconds)
    pending = iter(sorted(candidates, key=lambda row: row.shadow_send_at_ns))
    current = next(pending, None)
    results: list[ShadowActionOutcome] = []
    latest_at = None
    for market_event in market_events:
        while current is not None and current.shadow_send_at_ns < market_event.received_at_ns:
            evaluator.add(current)
            current = next(pending, None)
        engine.on_event(market_event)
        results.extend(
            evaluator.on_market(
                market_event,
                quote=engine.quote(market_event.symbol),
                bid_levels=engine.levels(market_event.symbol, side="bid", depth=2000),
                ask_levels=engine.levels(market_event.symbol, side="ask", depth=2000),
            )
        )
        latest_at = market_event.received_at
        while current is not None and current.shadow_send_at_ns == market_event.received_at_ns:
            evaluator.add(current)
            current = next(pending, None)
    while current is not None:
        evaluator.add(current)
        latest_at = max(latest_at or current.shadow_send_at, current.shadow_send_at)
        current = next(pending, None)
    if latest_at is not None:
        results.extend(evaluator.flush(latest_at + timedelta(seconds=10)))
    return tuple(results)


def shadow_report(
    outcomes: Sequence[ShadowActionOutcome],
    *,
    passive_validation: Mapping[str, Any] | None = None,
    maximum_incomplete_fraction: float = 0.05,
) -> dict[str, Any]:
    if not 0 <= maximum_incomplete_fraction < 1:
        raise ValueError("maximum incomplete fraction must be between zero and one")
    groups: dict[tuple[str, str, str], list[ShadowActionOutcome]] = {}
    for outcome in outcomes:
        groups.setdefault((outcome.symbol, outcome.family, outcome.action), []).append(outcome)
    group_reports = []
    for key, rows in sorted(groups.items()):
        incomplete = sum(not row.complete for row in rows)
        fraction = incomplete / len(rows)
        group_reports.append(
            {
                "symbol": key[0],
                "family": key[1],
                "action": key[2],
                "candidates": len(rows),
                "complete": sum(row.complete for row in rows),
                "filled": sum(row.filled for row in rows),
                "incomplete": incomplete,
                "incomplete_fraction": fraction,
                "calibration_population_supported": (
                    key[2] == "PASSIVE_BUY"
                    and bool((passive_validation or {}).get("passed"))
                    and fraction <= maximum_incomplete_fraction
                ),
                "missing_reasons": dict(
                    Counter(reason for row in rows for reason in row.missing_reasons)
                ),
            }
        )
    return {
        "schema": "shadow-action-report-v1",
        "outcome_count": len(outcomes),
        "candidate_count": len({row.candidate_id for row in outcomes}),
        "policy_ids": sorted({row.simulation_policy.identity for row in outcomes}),
        "passive_validation": dict(passive_validation or {}),
        "groups": group_reports,
        "outcomes": [row.model_dump(mode="json") for row in outcomes],
        "source_sha256": _digest([row.model_dump(mode="json") for row in outcomes]),
        "no_order_submission": True,
        "aggressive_execution_supported": False,
    }


def estimate_collection_days(
    outcomes: Sequence[ShadowActionOutcome],
    *,
    target_candidates: int = 200,
    target_fills: int = 30,
    minimum_observation_days: float = 7,
) -> dict[str, Any]:
    if target_candidates < 1 or target_fills < 1 or minimum_observation_days <= 0:
        raise ValueError("collection targets must be positive")
    groups: dict[tuple[str, str, str], list[ShadowActionOutcome]] = {}
    for outcome in outcomes:
        groups.setdefault((outcome.symbol, outcome.family, outcome.action), []).append(outcome)
    estimates = []
    passive_support_days: list[float] = []
    for key, rows in sorted(groups.items()):
        first = min(row.decision_at for row in rows)
        last = max(row.decision_at for row in rows)
        observed_days = max((last - first).total_seconds() / 86400, 1 / 24)
        complete = [row for row in rows if row.complete]
        fills = sum(row.filled for row in complete)
        candidate_rate = len(rows) / observed_days
        fill_rate = fills / observed_days
        candidate_days = max(0, target_candidates - len(rows)) / candidate_rate
        fill_days = max(0, target_fills - fills) / fill_rate if fill_rate > 0 else None
        calendar_days = max(0.0, minimum_observation_days - observed_days)
        support_days = None if fill_days is None else max(candidate_days, fill_days, calendar_days)
        if key[2] == "PASSIVE_BUY" and support_days is not None:
            passive_support_days.append(support_days)
        estimates.append(
            {
                "symbol": key[0],
                "family": key[1],
                "action": key[2],
                "observed_days": observed_days,
                "candidates": len(rows),
                "complete": len(complete),
                "fills": fills,
                "candidate_rate_per_day": candidate_rate,
                "fill_rate_per_day": fill_rate,
                "estimated_additional_days_for_support": support_days,
                "estimated_range_days": (
                    [max(1, int(support_days * 0.75)), max(1, int(support_days * 1.5 + 1))]
                    if support_days is not None
                    else None
                ),
            }
        )
    earliest = min(passive_support_days) if passive_support_days else None
    return {
        "targets": {
            "candidates_per_cell": target_candidates,
            "fills_per_cell": target_fills,
            "minimum_observation_days": minimum_observation_days,
        },
        "groups": estimates,
        "earliest_additional_days_for_statistical_support": earliest,
        "valid_model_guaranteed": False,
        "interpretation": (
            "This estimates data support only. A model validates only if its chronological "
            "held-out conservative net-edge lower bound is positive after costs."
        ),
    }


def read_market_events(
    database: Database, *, start: datetime, end: datetime
) -> Iterable[MarketEvent]:
    with database.begin() as connection:
        statement = (
            select(scalping_market_batches.c.raw)
            .where(
                scalping_market_batches.c.recorded_at >= start,
                scalping_market_batches.c.recorded_at <= end,
            )
            .order_by(scalping_market_batches.c.recorded_at)
            .execution_options(stream_results=True, yield_per=100)
        )
        for raw in connection.scalars(statement):
            for item in json.loads(zlib.decompress(raw)):
                yield MarketEvent.model_validate(item)


def runtime_candidates(
    database: Database, *, run_id: str, account_digest: str, config: ScalpingConfig
) -> tuple[ShadowCandidate, ...]:
    with database.begin() as connection:
        rows = list(
            connection.execute(
                select(events.c.occurred_at, events.c.payload)
                .where(
                    events.c.event_type == "scalp_candidate_decision",
                    events.c.payload["run_id"].as_string() == run_id,
                )
                .order_by(events.c.occurred_at, events.c.event_id)
            ).mappings()
        )
    candidates = []
    for row in rows:
        signal = ScalpSignal.model_validate_json(json.dumps(row["payload"]["signal"], default=str))
        candidates.append(
            candidate_from_signal(
                signal,
                run_id=run_id,
                account_digest=account_digest,
                config=config,
                shadow_send_at=row["occurred_at"],
                bid_levels=(BookLevel(price=signal.quote.bid, quantity=signal.quote.bid_size),),
                ask_levels=(BookLevel(price=signal.quote.ask, quantity=signal.quote.ask_size),),
            )
        )
    return tuple(candidates)


def historical_candidates(
    database: Database,
    *,
    account_digest: str,
    start: datetime,
    end: datetime,
) -> tuple[ShadowCandidate, ...]:
    with database.begin() as connection:
        rows = list(
            connection.execute(
                select(
                    scalping_cycles,
                    scalping_order_links.c.intent,
                    scalping_order_links.c.first_positive_fill_at,
                )
                .outerjoin(
                    scalping_order_links,
                    (scalping_order_links.c.cycle_id == scalping_cycles.c.cycle_id)
                    & (scalping_order_links.c.intent["request"]["side"].as_string() == "buy"),
                )
                .where(
                    scalping_cycles.c.account_digest == account_digest,
                    scalping_cycles.c.created_at >= start,
                    scalping_cycles.c.created_at < end,
                )
                .order_by(
                    scalping_cycles.c.created_at,
                    scalping_cycles.c.cycle_id,
                    scalping_order_links.c.first_positive_fill_at,
                )
            ).mappings()
        )
    unique: dict[str, Mapping[str, Any]] = {}
    first_fills: dict[str, datetime | None] = {}
    for row in rows:
        identity = str(row["cycle_id"])
        unique.setdefault(identity, dict(row))
        fill = row["first_positive_fill_at"]
        if fill is not None:
            first_fills[identity] = min(first_fills.get(identity, fill) or fill, fill)
    candidates = []
    for identity, stored in unique.items():
        signal = ScalpSignal.model_validate_json(
            json.dumps(stored["payload"]["signal"], default=str)
        )
        frozen = ScalpingConfig.model_validate(stored["payload"]["config"])
        intent = stored.get("intent") or {}
        original_quote = intent.get("quote") or {}
        passive_comparable = bool(
            (intent.get("request") or {}).get("order_type") == "limit"
            and intent.get("limit_price") is not None
            and str(intent.get("limit_price")) == str(original_quote.get("bid"))
        )
        first_fill = first_fills.get(identity)
        actual_filled_within_ttl = bool(
            passive_comparable
            and first_fill is not None
            and first_fill <= signal.observed_at + timedelta(seconds=frozen.entry_order_ttl_seconds)
        )
        candidates.append(
            candidate_from_signal(
                signal,
                run_id=str(stored["run_id"]),
                account_digest=account_digest,
                config=frozen,
                shadow_send_at=signal.observed_at,
                bid_levels=(BookLevel(price=signal.quote.bid, quantity=signal.quote.bid_size),),
                ask_levels=(BookLevel(price=signal.quote.ask, quantity=signal.quote.ask_size),),
                actual_filled=actual_filled_within_ttl if passive_comparable else None,
                actual_first_fill_at=first_fill,
                actual_passive_comparable=passive_comparable,
            )
        )
    return tuple(candidates)


def simulate_database_candidates(
    database: Database,
    candidates: Sequence[ShadowCandidate],
    config: ScalpingConfig,
    policy: ShadowPolicy | None = None,
) -> tuple[ShadowActionOutcome, ...]:
    if not candidates:
        return ()
    selected_policy = policy or ShadowPolicy()
    windows = sorted(
        (
            row.shadow_send_at - timedelta(seconds=1),
            row.signal.observed_at + timedelta(seconds=row.horizon_seconds + 1),
            row,
        )
        for row in candidates
    )
    clusters: list[dict[str, Any]] = []
    for start, end, candidate in windows:
        if clusters and start <= clusters[-1]["end"] + timedelta(seconds=1):
            clusters[-1]["end"] = max(clusters[-1]["end"], end)
            clusters[-1]["candidates"].append(candidate)
        else:
            clusters.append({"start": start, "end": end, "candidates": [candidate]})
    results: list[ShadowActionOutcome] = []
    for cluster in clusters:
        evaluator = ShadowActionEvaluator(selected_policy)
        pending = iter(sorted(cluster["candidates"], key=lambda row: row.shadow_send_at_ns))
        current = next(pending, None)
        last_quote: dict[str, ScalpQuote] = {}
        latest = cluster["end"]
        for market_event in read_market_events(
            database,
            start=cluster["start"],
            end=cluster["end"] + timedelta(seconds=1),
        ):
            if market_event.event_type not in {"quote", "trade"}:
                continue
            while current is not None and current.shadow_send_at_ns < market_event.received_at_ns:
                evaluator.add(current)
                last_quote.setdefault(current.signal.symbol, current.signal.quote)
                current = next(pending, None)
            if market_event.event_type == "quote":
                bid, ask = market_event.bids[0], market_event.asks[0]
                last_quote[market_event.symbol] = ScalpQuote(
                    symbol=market_event.symbol,
                    exchange_at=market_event.exchange_at,
                    exchange_time_ns=market_event.exchange_at_ns,
                    received_at=market_event.received_at,
                    bid=bid.price,
                    ask=ask.price,
                    bid_size=bid.quantity,
                    ask_size=ask.quantity,
                )
            quote = last_quote.get(market_event.symbol)
            results.extend(
                evaluator.on_market(
                    market_event,
                    quote=quote,
                    bid_levels=_with_bbo(quote, (), side="bid"),
                    ask_levels=_with_bbo(quote, (), side="ask"),
                )
            )
            latest = max(latest, market_event.received_at)
            while current is not None and current.shadow_send_at_ns == market_event.received_at_ns:
                evaluator.add(current)
                last_quote.setdefault(current.signal.symbol, current.signal.quote)
                current = next(pending, None)
        while current is not None:
            evaluator.add(current)
            current = next(pending, None)
        results.extend(evaluator.flush(latest + timedelta(seconds=1)))
    return tuple({row.identity: row for row in results}.values())
