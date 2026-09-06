"""Independent, bounded arithmetic audit; never a strategy backtest or broker adapter."""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta
from decimal import ROUND_CEILING, Decimal
from hashlib import sha256
from pathlib import Path, PureWindowsPath
from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, JsonValue, model_validator

ZERO = Decimal(0)
SEC_SOURCE = "https://www.sec.gov/rules-regulations/fee-rate-advisories/2026-2"
FINRA_SEC_SOURCE = "https://www.finra.org/rules-guidance/notices/information-notice-20260317"
TAF_SOURCE = (
    "https://www.finra.org/rules-guidance/rule-filings/sr-finra-2024-019/fee-adjustment-schedule"
)
SIZE_SOURCE = (
    "https://docs.alpaca.markets/us/v1.1/changelog/marketdata-bid-and-ask-size-display-change"
)
VERIFIED_ON = date(2026, 9, 6)
BASELINE_COMMIT = "2a1db33e7cf4f596e8fce9aac47c4e1e8c854630"
SUPERSEDED_REASONS = (
    "v0.10 close-anchored regular-session fills use quotes at/after the session close; "
    "a completed close decision cannot execute at that close or an unsupported after-hours quote.",
    "Missing arrival quotes fall back to pre-arrival observations, which are valuation marks, "
    "not causal executable fills.",
    "Adjusted historical bar marks/quantities are combined with raw SIP execution prices "
    "without a raw share/corporate-action ledger.",
    "One SEC/TAF/CAT schedule and unverified fee-type/day rounding are applied over all years, "
    "without effective charge dates or account-specific billing provenance.",
    "Historical SIP size normalization is undocumented; treating payload sizes as shares "
    "does not establish displayed capacity, especially before 2025-11-03.",
    "Adverse pre-arrival-to-arrival quote movement is already in the arrival price and is "
    "charged again as slippage; liquidity is not depleted across same-account orders.",
)


class FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True)


class ReferenceSession(FrozenModel):
    opened_at: AwareDatetime
    closed_at: AwareDatetime
    source: str = Field(min_length=1)

    @model_validator(mode="after")
    def ordered(self) -> ReferenceSession:
        if self.closed_at <= self.opened_at:
            raise ValueError("session close must follow open")
        return self


class ReferenceFact(FrozenModel):
    fact_id: str
    category: Literal["market", "news", "corporate"]
    event_at: AwareDatetime
    available_at: AwareDatetime
    value: Decimal


def reference_decision(
    facts: Sequence[ReferenceFact], *, decision_at: datetime
) -> tuple[tuple[str, Decimal], ...]:
    """A deliberately simple frozen signal: retain only facts already available."""
    _aware(decision_at)
    return tuple(
        sorted(
            (fact.fact_id, fact.value)
            for fact in facts
            if fact.available_at <= decision_at and fact.event_at <= decision_at
        )
    )


class ReferenceQuote(FrozenModel):
    event_id: str
    symbol: str
    event_at: AwareDatetime
    provider_at: AwareDatetime
    received_at: AwareDatetime
    bid: Decimal = Field(gt=0)
    ask: Decimal = Field(gt=0)
    bid_size: Decimal = Field(ge=0)
    ask_size: Decimal = Field(ge=0)
    price_basis: Literal["raw", "adjusted", "unknown"] = "unknown"
    size_units: Literal["shares", "round_lots", "unknown"] = "unknown"
    round_lot_size: Decimal | None = Field(default=None, gt=0)
    size_provenance: str | None = None
    source: str
    evidence_kind: Literal["synthetic", "normalized_observation", "raw_provider_response"]

    @model_validator(mode="after")
    def coherent(self) -> ReferenceQuote:
        if self.ask < self.bid:
            raise ValueError("crossed quote")
        if not self.event_at <= self.provider_at <= self.received_at:
            raise ValueError("incoherent event/provider/receipt clocks")
        return self

    def displayed_shares(self, side: Literal["buy", "sell"]) -> Decimal | None:
        if not self.size_provenance or self.size_units == "unknown":
            return None
        size = self.ask_size if side == "buy" else self.bid_size
        if self.size_units == "shares":
            return size
        return size * self.round_lot_size if self.round_lot_size is not None else None


class ReferenceOrder(FrozenModel):
    account_id: str
    order_id: str
    symbol: str
    side: Literal["buy", "sell"]
    quantity: Decimal = Field(gt=0)
    decision_at: AwareDatetime
    arrival_at: AwareDatetime
    fractionable: bool
    limit_price: Decimal | None = Field(default=None, gt=0)
    maximum_wait_seconds: int = Field(default=60, ge=0)

    @model_validator(mode="after")
    def coherent(self) -> ReferenceOrder:
        if self.arrival_at < self.decision_at:
            raise ValueError("arrival precedes decision")
        exponent = self.quantity.as_tuple().exponent
        if not isinstance(exponent, int) or exponent < -9:
            raise ValueError("quantity exceeds nine-decimal precision")
        if not self.fractionable and self.quantity != self.quantity.to_integral_value():
            raise ValueError("fractional asset eligibility unavailable")
        return self


class ReferenceFill(FrozenModel):
    quote_id: str
    timestamp: AwareDatetime
    quantity: Decimal
    price: Decimal


class ReferenceExecution(FrozenModel):
    status: str
    eligible_at: AwareDatetime | None
    fills: tuple[ReferenceFill, ...] = ()
    remaining_quantity: Decimal
    reason: str | None = None


def first_eligible_arrival(
    arrival_at: datetime, sessions: Sequence[ReferenceSession]
) -> datetime | None:
    _aware(arrival_at)
    for session in sorted(sessions, key=lambda item: item.opened_at):
        if arrival_at < session.closed_at:
            return max(arrival_at, session.opened_at)
    return None


def reference_execute(
    order: ReferenceOrder,
    quotes: Sequence[ReferenceQuote],
    sessions: Sequence[ReferenceSession],
    *,
    depleted: dict[tuple[str, str, str], Decimal] | None = None,
    cancel_at: datetime | None = None,
) -> ReferenceExecution:
    """Hypothetical marketable fills, not broker fills; one capacity per account/quote/side."""
    if cancel_at is not None:
        _aware(cancel_at)
    eligible = first_eligible_arrival(order.arrival_at, sessions)
    if eligible is None:
        return ReferenceExecution(
            status="unavailable",
            eligible_at=None,
            remaining_quantity=order.quantity,
            reason="next_session_not_supplied",
        )
    deadline = eligible + timedelta(seconds=order.maximum_wait_seconds)
    used = depleted if depleted is not None else {}
    remaining = order.quantity
    fills: list[ReferenceFill] = []
    reason = "missing_or_stale_quote"
    seen: dict[str, ReferenceQuote] = {}
    for quote in quotes:
        if quote.event_id in seen and quote != seen[quote.event_id]:
            raise ValueError("conflicting quote identity")
        seen[quote.event_id] = quote
    for quote in sorted(seen.values(), key=lambda item: (item.event_at, item.event_id)):
        if quote.symbol != order.symbol or quote.event_at < eligible:
            continue
        if quote.received_at > deadline or quote.event_at > deadline:
            continue
        if cancel_at is not None and quote.received_at >= cancel_at:
            continue
        if not any(
            s.opened_at <= quote.event_at <= quote.received_at < s.closed_at for s in sessions
        ):
            continue
        if quote.price_basis != "raw":
            reason = "raw_price_basis_unavailable"
            continue
        size = quote.displayed_shares(order.side)
        if size is None:
            reason = "quote_size_units_unavailable"
            continue
        price = quote.ask if order.side == "buy" else quote.bid
        if order.limit_price is not None and (
            (order.side == "buy" and price > order.limit_price)
            or (order.side == "sell" and price < order.limit_price)
        ):
            reason = "limit_not_marketable"
            continue
        key = (order.account_id, quote.event_id, order.side)
        quantity = min(remaining, max(ZERO, size - used.get(key, ZERO)))
        if quantity == 0:
            continue
        fills.append(
            ReferenceFill(
                quote_id=quote.event_id,
                timestamp=quote.received_at,
                quantity=quantity,
                price=price,
            )
        )
        used[key] = used.get(key, ZERO) + quantity
        remaining -= quantity
        if remaining == 0:
            break
    cancelled = cancel_at is not None and cancel_at <= deadline and remaining > 0
    return ReferenceExecution(
        status="filled"
        if remaining == 0
        else "cancelled"
        if cancelled
        else "partial"
        if fills
        else "unavailable",
        eligible_at=eligible,
        fills=tuple(fills),
        remaining_quantity=remaining,
        reason=reason if not fills else None,
    )


class DividendReceivable(FrozenModel):
    event_id: str
    symbol: str
    entitled_quantity: Decimal
    amount: Decimal
    payable_on: date


class ReferenceLedger(FrozenModel):
    account_id: str
    cash: Decimal
    positions: dict[str, Decimal] = Field(default_factory=dict)
    cost_basis: dict[str, Decimal] = Field(default_factory=dict)
    receivables: tuple[DividendReceivable, ...] = ()
    applied_events: tuple[str, ...] = ()

    def trade(
        self,
        event_id: str,
        symbol: str,
        quantity: Decimal,
        raw_price: Decimal,
        *,
        fee: Decimal = ZERO,
        price_basis: str = "raw",
    ) -> ReferenceLedger:
        if event_id in self.applied_events:
            raise ValueError("duplicate ledger event")
        if price_basis != "raw" or raw_price <= 0 or fee < 0 or quantity == 0:
            raise ValueError("raw executable price and nonzero quantity required")
        prior_quantity = self.positions.get(symbol, ZERO)
        remaining = prior_quantity + quantity
        cash = self.cash - quantity * raw_price - fee
        if remaining < 0 or cash < 0:
            raise ValueError("long-only fully funded ledger")
        basis = dict(self.cost_basis)
        prior_basis = basis.get(symbol, ZERO)
        basis[symbol] = (
            prior_basis + quantity * raw_price + fee
            if quantity > 0
            else prior_basis * remaining / prior_quantity
        )
        return self.model_copy(
            update={
                "cash": cash,
                "positions": {**self.positions, symbol: remaining},
                "cost_basis": basis,
                "applied_events": (*self.applied_events, event_id),
            }
        )

    def split(self, event_id: str, symbol: str, ratio: Decimal) -> ReferenceLedger:
        if ratio <= 0 or event_id in self.applied_events:
            raise ValueError("invalid or duplicate split")
        return self.model_copy(
            update={
                "positions": {**self.positions, symbol: self.positions.get(symbol, ZERO) * ratio},
                "applied_events": (*self.applied_events, event_id),
            }
        )

    def dividend_entitlement(
        self,
        event_id: str,
        symbol: str,
        per_share: Decimal,
        payable_on: date,
    ) -> ReferenceLedger:
        if event_id in self.applied_events or per_share < 0:
            raise ValueError("invalid or duplicate entitlement")
        quantity = self.positions.get(symbol, ZERO)
        receivable = DividendReceivable(
            event_id=event_id,
            symbol=symbol,
            entitled_quantity=quantity,
            amount=quantity * per_share,
            payable_on=payable_on,
        )
        return self.model_copy(
            update={
                "receivables": (*self.receivables, receivable),
                "applied_events": (*self.applied_events, event_id),
            }
        )

    def pay_dividend(self, event_id: str, *, paid_on: date) -> ReferenceLedger:
        match = next((item for item in self.receivables if item.event_id == event_id), None)
        if match is None or paid_on < match.payable_on:
            raise ValueError("missing receivable or premature payment")
        return self.model_copy(
            update={
                "cash": self.cash + match.amount,
                "receivables": tuple(item for item in self.receivables if item != match),
            }
        )

    def equity(self, marks: dict[str, Decimal], *, price_basis: str = "raw") -> Decimal:
        if price_basis != "raw":
            raise ValueError("adjusted marks would mix bases/double count distributions")
        if any(q and (s not in marks or marks[s] <= 0) for s, q in self.positions.items()):
            raise ValueError("raw mark unavailable for open position")
        return (
            self.cash
            + sum((q * marks[s] for s, q in self.positions.items() if q), ZERO)
            + sum((item.amount for item in self.receivables), ZERO)
        )


class ReferenceFeeRule(FrozenModel):
    fee_type: Literal["sec", "taf"]
    account_product: str = "covered_us_equity_regulatory_obligation"
    side: str = "sell"
    asset_class: str = "us_equity"
    effective_start: date
    effective_end: date
    date_semantics: str
    rate_basis: Literal["sale_notional", "shares"]
    rate: Decimal
    cap_per_trade: Decimal | None = None
    rounding_scope: str = "account_pass_through_unresolved"
    source: str


FEE_RULES = (
    ReferenceFeeRule(
        fee_type="sec",
        effective_start=date(2026, 2, 27),
        effective_end=date(2026, 4, 3),
        date_semantics="charge_date; FINRA OTC charge date is trade date, not signal/settlement",
        rate_basis="sale_notional",
        rate=ZERO,
        source=FINRA_SEC_SOURCE,
    ),
    ReferenceFeeRule(
        fee_type="sec",
        effective_start=date(2026, 4, 4),
        effective_end=VERIFIED_ON,
        date_semantics="charge_date; FINRA OTC charge date is trade date, not signal/settlement",
        rate_basis="sale_notional",
        rate=Decimal("0.00002060"),
        source=FINRA_SEC_SOURCE,
    ),
    ReferenceFeeRule(
        fee_type="taf",
        effective_start=date(2024, 1, 1),
        effective_end=date(2025, 12, 31),
        date_semantics="trade_date",
        rate_basis="shares",
        rate=Decimal("0.000166"),
        cap_per_trade=Decimal("8.30"),
        source=TAF_SOURCE,
    ),
    ReferenceFeeRule(
        fee_type="taf",
        effective_start=date(2026, 1, 1),
        effective_end=VERIFIED_ON,
        date_semantics="trade_date",
        rate_basis="shares",
        rate=Decimal("0.000195"),
        cap_per_trade=Decimal("9.79"),
        source=TAF_SOURCE,
    ),
)


class ReferenceFee(FrozenModel):
    status: str
    fee_type: str
    raw_amount: Decimal | None
    charged_amount: Decimal | None = None
    charge_date: date | None
    schedule_date: date | None
    scenario: Literal["historical", "current_business_cost"]
    rule: ReferenceFeeRule | None = None
    reason: str | None = None


def reference_fee(
    fee_type: str,
    *,
    side: Literal["buy", "sell"],
    quantity: Decimal,
    notional: Decimal,
    charge_date: date | None,
    trade_date: date | None = None,
    scenario: Literal["historical", "current_business_cost"] = "historical",
    current_cost_asof: date = VERIFIED_ON,
) -> ReferenceFee:
    if quantity < 0 or notional < 0:
        raise ValueError("fee inputs cannot be negative")
    effective = charge_date if fee_type == "sec" else trade_date
    selected_date = current_cost_asof if scenario == "current_business_cost" else effective
    rule = next(
        (
            item
            for item in FEE_RULES
            if item.fee_type == fee_type
            and selected_date is not None
            and item.effective_start <= selected_date <= item.effective_end
        ),
        None,
    )
    if rule is None:
        return ReferenceFee(
            status="unavailable",
            fee_type=fee_type,
            raw_amount=None,
            charge_date=charge_date,
            schedule_date=selected_date,
            scenario=scenario,
            reason="historical_schedule_or_date_unknown; CAT pass-through period unresolved",
        )
    amount = (
        ZERO
        if side == "buy"
        else rule.rate * (notional if rule.rate_basis == "sale_notional" else quantity)
    )
    if rule.cap_per_trade is not None:
        amount = min(amount, rule.cap_per_trade)
    return ReferenceFee(
        status="statutory_arithmetic_only",
        fee_type=fee_type,
        raw_amount=amount,
        charge_date=charge_date,
        schedule_date=selected_date,
        scenario=scenario,
        rule=rule,
        reason="account product and billing rounding unverified; not an observed broker charge",
    )


def round_fee_up(amount: Decimal) -> Decimal:
    if amount < 0:
        raise ValueError("negative fee")
    return amount.quantize(Decimal("0.01"), rounding=ROUND_CEILING)


def reference_fee_totals(
    charges: Sequence[tuple[str, str, date, Decimal]],
    *,
    rounding_scope: Literal["per_trade", "account_rule_day", "account_day"],
) -> dict[str, Decimal]:
    totals: dict[tuple[str, str, date], Decimal] = {}
    for account, rule, day, amount in charges:
        if amount < 0:
            raise ValueError("negative fee")
        key = (account, "" if rounding_scope == "account_day" else rule, day)
        totals[key] = totals.get(key, ZERO) + (
            round_fee_up(amount) if rounding_scope == "per_trade" else amount
        )
    result: dict[str, Decimal] = {}
    for (account, _, _), amount in totals.items():
        result[account] = result.get(account, ZERO) + round_fee_up(amount)
    return result


def reference_submission_recovery(
    persisted_client_order_id: str,
    matching_broker_ids: Sequence[str],
    *,
    lookup_complete: bool,
) -> dict[str, JsonValue]:
    """Never resubmit on ambiguity, even after a single negative lookup."""
    matches = set(matching_broker_ids)
    if matches == {persisted_client_order_id}:
        return {"state": "reconciled", "additional_submissions": 0}
    return {
        "state": "blocked",
        "additional_submissions": 0,
        "reason": "conflicting_broker_identity"
        if matches
        else "not_found_is_not_proof_of_rejection"
        if lookup_complete
        else "lookup_unavailable",
    }


def _aware(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timezone-aware timestamp required")


class ExecutionAuditCase(FrozenModel):
    case_id: str
    evidence_kind: str = "synthetic"
    inputs: dict[str, JsonValue]
    expected: dict[str, JsonValue]
    reference: dict[str, JsonValue]
    production: dict[str, JsonValue]
    discrepancy: dict[str, JsonValue] = Field(default_factory=dict)
    status: str
    raw_provider_response: None = None
    provenance: str = (
        "Synthetic inputs retained inline; legacy observations reproduced at baseline_commit; "
        "not a captured market-provider or broker response."
    )


class ExecutionAccountingAuditReport(FrozenModel):
    schema_version: str = "v20-execution-accounting-audit-1"
    generated_at: AwareDatetime
    status: str
    historical_results_valid: bool = False
    untouched_holdouts_opened: bool = False
    historical_reruns: int = 0
    production_legacy_simulator_available: bool = False
    baseline_commit: str = BASELINE_COMMIT
    cases: tuple[ExecutionAuditCase, ...]
    findings: tuple[dict[str, JsonValue], ...]
    sources: tuple[dict[str, JsonValue], ...]
    fee_rules: tuple[ReferenceFeeRule, ...] = FEE_RULES
    artifacts: tuple[dict[str, JsonValue], ...]
    real_data_boundary: dict[str, JsonValue]
    superseded_reasons: tuple[str, ...] = SUPERSEDED_REASONS
    limitations: tuple[str, ...]


def synthetic_gold_cases() -> tuple[ExecutionAuditCase, ...]:
    """Fixtures are explicitly synthetic, never reconstructed provider responses."""
    at = datetime(2026, 9, 4, 20, tzinfo=UTC)
    open_next = datetime(2026, 9, 8, 13, 30, tzinfo=UTC)
    sessions = (
        ReferenceSession(
            opened_at=at - timedelta(hours=6, minutes=30),
            closed_at=at,
            source="synthetic XNYS Friday; Monday Labor Day",
        ),
        ReferenceSession(
            opened_at=open_next,
            closed_at=open_next + timedelta(hours=6, minutes=30),
            source="synthetic XNYS Tuesday",
        ),
    )

    def quote(
        event_id: str, when: datetime, *, size: str = "1", bid: str = "99", ask: str = "101"
    ) -> ReferenceQuote:
        return ReferenceQuote(
            event_id=event_id,
            symbol="SYNTH",
            event_at=when,
            provider_at=when,
            received_at=when,
            bid=Decimal(bid),
            ask=Decimal(ask),
            bid_size=Decimal(size),
            ask_size=Decimal(size),
            price_basis="raw",
            size_units="shares",
            size_provenance="synthetic shares fixture",
            source="synthetic_gold_cases",
            evidence_kind="synthetic",
        )

    def order(**changes: object) -> ReferenceOrder:
        values: dict[str, object] = {
            "account_id": "synthetic-A",
            "order_id": "synthetic-order",
            "symbol": "SYNTH",
            "side": "buy",
            "quantity": Decimal("0.25"),
            "decision_at": at,
            "arrival_at": at + timedelta(seconds=1),
            "fractionable": True,
        }
        values.update(changes)
        return ReferenceOrder.model_validate(values)

    prior = quote("synthetic-prior", at - timedelta(seconds=1))
    after = quote("synthetic-after-close", at + timedelta(seconds=1))
    next_quote = quote("synthetic-next-open", open_next)
    request = order()
    cases: list[ExecutionAuditCase] = []

    def record(
        case_id: str,
        inputs: dict[str, JsonValue],
        expected: dict[str, JsonValue],
        actual: dict[str, JsonValue],
        *,
        production: dict[str, JsonValue] | None = None,
        discrepancy: dict[str, JsonValue] | None = None,
    ) -> None:
        cases.append(
            ExecutionAuditCase(
                case_id=case_id,
                inputs=inputs,
                expected=expected,
                reference=actual,
                production=production
                or {"status": "unavailable", "reason": "legacy_ledger_blocked"},
                discrepancy=discrepancy or {},
                status="reference_pass" if expected == actual else "failed",
            )
        )

    for name, observations in (
        ("daily_completed_close_next_eligible_session", (prior, after, next_quote)),
        ("regular_order_after_close", (after, next_quote)),
    ):
        result = reference_execute(request, observations, sessions)
        result_values: dict[str, JsonValue] = {
            "eligible_at": result.eligible_at.isoformat() if result.eligible_at else None,
            "fill_quantity": str(sum((fill.quantity for fill in result.fills), ZERO)),
            "fill_notional": str(sum((fill.quantity * fill.price for fill in result.fills), ZERO)),
        }
        record(
            name,
            {
                "bar_interval_start": sessions[0].opened_at.isoformat(),
                "bar_interval_end": at.isoformat(),
                "feature_available_at": at.isoformat(),
                "order": request.model_dump(mode="json"),
                "quotes": [q.model_dump(mode="json") for q in observations],
            },
            {
                "eligible_at": open_next.isoformat(),
                "fill_quantity": "0.25",
                "fill_notional": "25.25",
            },
            result_values,
            discrepancy={"legacy": "filled 0.25 at 101 after close; required next eligible quote"},
        )

    # Only comparison adapters import production code; no reference arithmetic uses it.
    from tradeagent.alpaca import HistoricalQuote
    from tradeagent.domain import Side
    from tradeagent.execution_calibration import (
        _decision_quote,
        _market_execution,
        _submission_quote,
    )

    def production_quote(when: datetime) -> HistoricalQuote:
        return HistoricalQuote(
            symbol="SYNTH",
            timestamp=when,
            bid_exchange="P",
            ask_exchange="Q",
            bid_price=Decimal("99"),
            ask_price=Decimal("101"),
            bid_size=Decimal(1),
            ask_size=Decimal(1),
            feed_source="sip",
        )

    regular = at - timedelta(hours=1)
    old_quote = production_quote(regular - timedelta(seconds=1))
    new_quote = production_quote(regular + timedelta(seconds=1))
    no_prior_fill = _submission_quote((old_quote,), regular) is None
    no_future_decision = _decision_quote((new_quote,), regular) is None
    record(
        "arrival_cannot_backfill_and_decision_cannot_look_ahead",
        {
            "arrival": regular.isoformat(),
            "prior_quote": old_quote.model_dump(mode="json"),
            "future_quote": new_quote.model_dump(mode="json"),
        },
        {"no_prior_fill": True, "no_future_decision": True},
        {"no_prior_fill": no_prior_fill, "no_future_decision": no_future_decision},
        production={"no_prior_fill": no_prior_fill, "no_future_decision": no_future_decision},
        discrepancy={"legacy_no_prior_fill": False, "legacy_no_future_decision": False},
    )
    current = _market_execution(
        Side.BUY,
        Decimal("0.25"),
        at + timedelta(seconds=1),
        production_quote(at + timedelta(seconds=1)),
    )
    record(
        "production_after_close_rejected",
        {"submitted_at": (at + timedelta(seconds=1)).isoformat()},
        {"quantity": "0"},
        {"quantity": str(current.filled_quantity)},
        production=current.model_dump(mode="json"),
        discrepancy={"legacy_quantity": "0.25", "legacy_notional": "25.25"},
    )

    fractional = ReferenceLedger(account_id="synthetic-A", cash=Decimal(100)).trade(
        "synthetic-fraction",
        "SYNTH",
        Decimal("0.25"),
        Decimal(100),
        fee=Decimal("0.01"),
    )
    record(
        "fractional_quantity",
        {"quantity": "0.25", "raw_price": "100", "fee": "0.01"},
        {"cash": "74.99", "equity": "99.99"},
        {"cash": str(fractional.cash), "equity": str(fractional.equity({"SYNTH": Decimal(100)}))},
    )

    for day, expected_fee in ((date(2026, 4, 3), "0"), (date(2026, 4, 4), "0.02060000")):
        fee = reference_fee(
            "sec", side="sell", quantity=Decimal(10), notional=Decimal(1000), charge_date=day
        )
        record(
            f"fee_charge_date_{day}",
            {"signal_date": "2026-04-02", "charge_date": str(day), "sold_notional": "1000"},
            {"raw_sec": expected_fee},
            {"raw_sec": str(fee.raw_amount)},
            production={"legacy_undated_raw_sec": "0.02060", "current": "unavailable"},
            discrepancy={"legacy_overcharge": "0.02060" if day.day == 3 else "0"},
        )
    unknown = reference_fee(
        "cat", side="buy", quantity=Decimal(1), notional=Decimal(25), charge_date=date(2016, 1, 4)
    )
    record(
        "unknown_historical_fee",
        {"fee_type": "cat", "date": "2016-01-04"},
        {"status": "unavailable", "amount": None},
        {
            "status": unknown.status,
            "amount": str(unknown.raw_amount) if unknown.raw_amount is not None else None,
        },
    )
    sizes = [
        next_quote.model_copy(
            update={"size_units": unit, "size_provenance": prov, "round_lot_size": lot}
        ).displayed_shares("buy")
        for unit, prov, lot in (
            ("unknown", None, None),
            ("shares", "synthetic shares", None),
            ("round_lots", "synthetic contemporaneous lot of 100", Decimal(100)),
        )
    ]
    record(
        "quote_size_schema_transition",
        {
            "transition": "2025-11-03",
            "payload_size": "1",
            "historical_endpoint_normalization": "unknown",
            "source": SIZE_SOURCE,
        },
        {"unknown": None, "shares": "1", "verified_100_share_lot": "100"},
        {
            "unknown": str(sizes[0]) if sizes[0] is not None else None,
            "shares": str(sizes[1]),
            "verified_100_share_lot": str(sizes[2]),
        },
    )

    ledger = (
        ReferenceLedger(account_id="synthetic-A", cash=Decimal(1000))
        .trade(
            "synthetic-buy",
            "SYNTH",
            Decimal(2),
            Decimal(100),
        )
        .split("synthetic-split", "SYNTH", Decimal(2))
    )
    ledger = ledger.dividend_entitlement(
        "synthetic-dividend",
        "SYNTH",
        Decimal("0.50"),
        date(2026, 9, 10),
    )
    ex_equity = ledger.equity({"SYNTH": Decimal("49.50")})
    exited = ledger.trade("synthetic-exit", "SYNTH", Decimal(-4), Decimal("49.50"))
    paid = exited.pay_dividend("synthetic-dividend", paid_on=date(2026, 9, 10))
    record(
        "split_dividend_receivable_then_payment",
        {
            "start_cash": "1000",
            "buy": "2 @ 100",
            "split_ratio": "2",
            "cash_dividend_per_post_split_share": "0.50",
            "ex_raw_mark": "49.50",
            "synthetic_split_date": "2026-09-08",
            "synthetic_ex_date": "2026-09-09",
            "synthetic_pay_date": "2026-09-10",
        },
        {
            "quantity_after_split": "4",
            "cash_on_ex": "800",
            "ex_equity": "1000.00",
            "cash_after_exit_before_payment": "998.00",
            "cash_after_payment": "1000.00",
        },
        {
            "quantity_after_split": str(ledger.positions["SYNTH"]),
            "cash_on_ex": str(ledger.cash),
            "ex_equity": str(ex_equity),
            "cash_after_exit_before_payment": str(exited.cash),
            "cash_after_payment": str(paid.cash),
        },
    )
    rejected = False
    try:
        ledger.equity({"SYNTH": Decimal("49.50")}, price_basis="adjusted")
    except ValueError:
        rejected = True
    record(
        "raw_adjusted_basis",
        {"raw_purchase": "2 * 100", "adjusted_mark": "50"},
        {"mixed_basis_rejected": True},
        {"mixed_basis_rejected": rejected},
        discrepancy={"legacy_mixed_equity": "900", "correct_raw_equity": "1000"},
    )

    for name, quotes in (("missing_quote", ()), ("stale_quote", (prior,))):
        result = reference_execute(request, quotes, sessions)
        record(
            name,
            {"order": request.model_dump(mode="json")},
            {"status": "unavailable", "quantity": "0"},
            {
                "status": result.status,
                "quantity": str(sum((f.quantity for f in result.fills), ZERO)),
            },
        )

    larger = order(quantity=Decimal(3))
    later = quote("synthetic-later", open_next + timedelta(seconds=2), size="2")
    for name, cancellation, expected_quantity in (
        ("partial_then_cancel", open_next + timedelta(seconds=1), "1"),
        ("partial_then_later_fill", None, "3"),
    ):
        result = reference_execute(larger, (next_quote, later), sessions, cancel_at=cancellation)
        record(
            name,
            {
                "requested": "3",
                "displayed_first": "1",
                "displayed_later": "2",
                "cancel_at": cancellation.isoformat() if cancellation else None,
            },
            {
                "quantity": expected_quantity,
                "remaining": str(Decimal(3) - Decimal(expected_quantity)),
            },
            {
                "quantity": str(sum((f.quantity for f in result.fills), ZERO)),
                "remaining": str(result.remaining_quantity),
            },
        )
    recovery = reference_submission_recovery(
        "synthetic-id", ("synthetic-id",), lookup_complete=True
    )
    record(
        "ambiguous_submit_recovery",
        {
            "persisted_client_order_id": "synthetic-id",
            "broker_lookup_matches": ["synthetic-id"],
            "production_test_references": [
                "tests\\test_order_state.py::test_reconciliation_recovers_lost_acknowledgement",
                "tests\\test_alpaca_paper.py::"
                "test_paper_client_submits_idempotent_market_order_and_tracks_state",
            ],
        },
        {"state": "reconciled", "additional_submissions": 0},
        recovery,
        production={"status": "reference_demonstration_only", "live_broker_response": None},
    )

    capacity: dict[tuple[str, str, str], Decimal] = {}
    sale = order(side="sell", quantity=Decimal(1))
    strategy = reference_execute(sale, (next_quote,), sessions, depleted=capacity)
    benchmark = reference_execute(
        sale.model_copy(update={"account_id": "synthetic-benchmark"}),
        (next_quote,),
        sessions,
        depleted=capacity,
    )
    repeated = reference_execute(sale, (next_quote,), sessions, depleted=capacity)
    strategy_ledger = ReferenceLedger(
        account_id="synthetic-A",
        cash=ZERO,
        positions={"SYNTH": Decimal(1)},
        cost_basis={"SYNTH": Decimal(100)},
    ).trade("synthetic-exit", "SYNTH", Decimal(-1), Decimal(99))
    benchmark_ledger = ReferenceLedger(
        account_id="synthetic-benchmark",
        cash=Decimal(1),
        positions={"SYNTH": Decimal(2)},
        cost_basis={"SYNTH": Decimal(200)},
    ).trade("synthetic-adjustment", "SYNTH", Decimal(-1), Decimal(99))
    record(
        "simultaneous_benchmark_adjustment_strategy_exit",
        {"strategy_account": "synthetic-A", "benchmark_account": "synthetic-benchmark"},
        {
            "strategy": "1",
            "benchmark": "1",
            "same_account_reuse": "0",
            "strategy_cash": "99",
            "benchmark_cash": "100",
            "strategy_equity": "99",
            "benchmark_equity": "200",
        },
        {
            "strategy": str(sum((f.quantity for f in strategy.fills), ZERO)),
            "benchmark": str(sum((f.quantity for f in benchmark.fills), ZERO)),
            "same_account_reuse": str(sum((f.quantity for f in repeated.fills), ZERO)),
            "strategy_cash": str(strategy_ledger.cash),
            "benchmark_cash": str(benchmark_ledger.cash),
            "strategy_equity": str(strategy_ledger.equity({})),
            "benchmark_equity": str(benchmark_ledger.equity({"SYNTH": Decimal(100)})),
        },
    )
    record(
        "spread_latency_bridge",
        {
            "quantity": "0.25",
            "decision_mid": "100",
            "arrival_mid": "101",
            "arrival_ask": "101.02",
            "distinct_residual_slippage": "0",
            "exit_mid": "102",
            "exit_bid": "101.98",
        },
        {"entry_delay": "0.25", "entry_spread": "0.0050", "pre_fee_roundtrip": "0.2400"},
        {
            "entry_delay": str(Decimal("0.25") * (Decimal(101) - Decimal(100))),
            "entry_spread": str(Decimal("0.25") * (Decimal("101.02") - Decimal(101))),
            "pre_fee_roundtrip": str(Decimal("0.25") * (Decimal("101.98") - Decimal("101.02"))),
        },
        discrepancy={"legacy_extra_adverse_slippage_per_share": "1.00"},
    )
    record(
        "favorable_latency_not_suppressed",
        {"decision_mid": "100", "arrival_mid": "99", "quantity": "0.25"},
        {"signed_delay_cost": "-0.25"},
        {"signed_delay_cost": str(Decimal("0.25") * (Decimal(99) - Decimal(100)))},
    )
    charges = (
        ("synthetic-A", "sec", date(2026, 4, 6), Decimal("0.001")),
        ("synthetic-A", "taf", date(2026, 4, 6), Decimal("0.001")),
        ("synthetic-B", "sec", date(2026, 4, 6), Decimal("0.001")),
    )
    rule_day = reference_fee_totals(charges, rounding_scope="account_rule_day")
    account_day = reference_fee_totals(charges, rounding_scope="account_day")
    record(
        "fee_granularity_account_and_rounding_scope",
        {
            "charges": [
                ["synthetic-A", "sec", "0.001"],
                ["synthetic-A", "taf", "0.001"],
                ["synthetic-B", "sec", "0.001"],
            ],
            "billing_scope": "unresolved; alternatives are sensitivity examples only",
        },
        {
            "one_cent_bps_on_10": "10.000",
            "one_cent_bps_on_25": "4.0000",
            "account_A_rule_day": "0.02",
            "account_A_account_day": "0.01",
            "independent_account_B": "0.01",
        },
        {
            "one_cent_bps_on_10": str(Decimal("0.01") / Decimal(10) * 10000),
            "one_cent_bps_on_25": str(Decimal("0.01") / Decimal(25) * 10000),
            "account_A_rule_day": str(rule_day["synthetic-A"]),
            "account_A_account_day": str(account_day["synthetic-A"]),
            "independent_account_B": str(account_day["synthetic-B"]),
        },
    )
    return tuple(cases)


def _file_boundary(root: Path, relative: str) -> dict[str, JsonValue]:
    path = _artifact_path(root, relative)
    if not path.is_file():
        return {"path": relative, "status": "unavailable"}
    digest = sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return {
        "path": relative,
        "sha256": digest.hexdigest(),
        "bytes": path.stat().st_size,
        "status": "preserved_read_only",
    }


def _artifact_path(root: Path, relative: str) -> Path:
    path = PureWindowsPath(relative)
    if path.is_absolute() or path.drive or ".." in path.parts:
        raise ValueError("artifact path must be repository relative")
    return root.joinpath(*path.parts)


def run_execution_accounting_audit(
    repository_root: Path,
    *,
    generated_at: datetime | None = None,
) -> ExecutionAccountingAuditReport:
    """Read an explicit v0.10 allowlist, not sealed datasets or an unbounded market replay."""
    root = repository_root.resolve()
    names = (
        "docs\\V0_10_REPORT.md",
        "research\\datasets\\v0.10.0-lower-execution-evidence.json",
        "research\\datasets\\v0.10.0-pre-2020-daily.json",
        "research\\datasets\\v0.10.0-2025-latest-daily.json",
        "research\\datasets\\v0.10.0-pre-2020-execution-evidence.json",
        "research\\datasets\\v0.10.0-2025-latest-execution-evidence.json",
        "research\\results\\v0.10.0-lower-corrected-provisional.json",
        "research\\results\\v0.10.0-lower-execution-calibration.json",
        "research\\results\\v0.10.0-pre-2020-validation.json",
        "research\\results\\v0.10.0-2025-latest-validation.json",
    )
    artifacts = tuple(_file_boundary(root, name) for name in names)
    metadata: dict[str, JsonValue] = {}
    samples: list[JsonValue] = []
    for name in names[1:6]:
        path = _artifact_path(root, name)
        if not path.is_file():
            continue
        payload = json.loads(path.read_text(encoding="utf-8"))
        metadata[name] = {
            key: value for key, value in payload.items() if key not in ("shards", "files")
        }
        shards = payload.get("shards", [])
        if not shards:
            continue
        # One persisted normalized snapshot per manifest, never all historical observations.
        shard = shards[0]
        relative = str(shard["path"]).replace("/", "\\")
        child = _artifact_path(root, relative).resolve()
        allowed = {
            names[1]: root / "data" / "v010" / "lower-execution-evidence",
            names[4]: root / "data" / "v010" / "external-evidence" / "pre-2020",
            names[5]: root / "data" / "v010" / "external-evidence" / "2025-latest",
        }
        if child.parent != allowed[name].resolve() or not child.is_file():
            samples.append({"manifest": name, "status": "sample_path_unavailable"})
            continue
        boundary = _file_boundary(root, relative)
        if boundary.get("sha256") != shard["sha256"]:
            samples.append({"manifest": name, "status": "sample_hash_mismatch"})
            continue
        data = json.loads(child.read_text(encoding="utf-8"))
        samples.append(
            {
                "manifest": name,
                "artifact": boundary,
                "request_parameters_preserved": {
                    "timestamp": data.get("timestamp"),
                    "window_seconds": data.get("window_seconds"),
                    "requested_symbols": data.get("requested_symbols"),
                },
                "evidence_kind": "persisted_normalized_observation_not_raw_http",
                "snapshot": data["snapshots"][0] if data.get("snapshots") else None,
                "missing": [
                    "raw_http_response",
                    "local_receipt_time",
                    "provider_time",
                    "endpoint_schema_version",
                    "historical_size_normalization",
                ],
            }
        )
    partials: dict[str, JsonValue] = {"status": "unavailable"}
    calibration = _artifact_path(root, names[7])
    if calibration.is_file():
        payload = json.loads(calibration.read_text(encoding="utf-8"))
        market = [
            scenario
            for family in payload.get("families", [])
            for config in family.get("configurations", [])
            for scenario in config.get("scenarios", [])
            if scenario["execution_style"] == "market" and Decimal(scenario["cost_multiplier"]) == 1
        ]
        partials = {
            "count": sum(item["partial_fills"] for item in market),
            "configurations": len(market),
            "definition": "0 < simulated fill quantity < requested difference; one count per order",
            "not_broker_fills": True,
            "not_one_account": True,
            "requested_quantity_reconstruction": "unavailable in aggregate execution report",
            "sizing_rule": "floor(pretrade equity * target weight / current adjusted close)",
            "fee_schedule_recorded": payload.get("fee_schedule"),
            "no_historical_rerun": True,
        }
    cases = synthetic_gold_cases()
    return ExecutionAccountingAuditReport(
        generated_at=generated_at or datetime.now(UTC),
        status="passed_bounded_checks_historical_validation_unavailable"
        if all(case.status == "reference_pass" for case in cases)
        else "failed",
        cases=cases,
        findings=(
            {
                "id": "EXEC-001",
                "status": "confirmed_defect",
                "case": "arrival_cannot_backfill_and_decision_cannot_look_ahead",
                "reason": "Pre-arrival fills and future decision quotes reproduced at baseline.",
                "resolution": "No fallback; stale/missing observations unavailable.",
            },
            {
                "id": "EXEC-002",
                "status": "confirmed_defect",
                "case": "production_after_close_rejected",
                "reason": "0.25 shares filled after regular close in baseline synthetic case.",
                "resolution": "Rejects close/after-close; reference schedules next session.",
            },
            {
                "id": "EXEC-003",
                "status": "confirmed_defect",
                "case": "fee_charge_date_2026-04-03",
                "reason": "$1000 covered sale charged raw SEC .0206 instead of zero at boundary.",
                "resolution": "Legacy simulator blocked; dated statutory reference supplied.",
            },
            {
                "id": "EXEC-004",
                "status": "confirmed_defect",
                "case": "spread_latency_bridge",
                "reason": "Arrival quote movement added again as slippage.",
                "resolution": "Distinct 0.5bps hypothetical residual only; not observed impact.",
            },
            {
                "id": "EXEC-005",
                "status": "basis_reconstruction_unavailable",
                "case": "raw_adjusted_basis",
                "reason": "Adjusted bars mixed with raw quotes and share quantities.",
                "resolution": "Legacy portfolio simulation blocked; raw reference ledger provided.",
            },
            {
                "id": "EXEC-006",
                "status": "size_and_billing_provenance_unavailable",
                "case": "quote_size_schema_transition",
                "reason": "Normalization, CAT periods, product and rounding not established.",
                "resolution": "No blanket size conversion or assumed historical fee schedule.",
            },
        ),
        artifacts=artifacts,
        real_data_boundary={
            "manifest_metadata": metadata,
            "bounded_normalized_samples": samples,
            "partial_fill_count": partials,
            "immutable_raw_provider_responses_available": False,
            "supersession_action": "append audit; preserve old artifacts; no corrected run yet",
            "corrected_frozen_rerun": "blocked pending raw basis, session, unit and fee provenance",
            "deployed_commit_verification": "not queried by this read-only local audit",
            "source_code_boundaries": [
                _file_boundary(root, name)
                for name in (
                    "src\\tradeagent\\alpaca.py",
                    "src\\tradeagent\\observed_execution.py",
                    "src\\tradeagent\\execution_calibration.py",
                    "src\\tradeagent\\execution_reference.py",
                )
            ],
            "historical_bar_basis": {
                "source": "src\\tradeagent\\alpaca.py::AlpacaDataClient.bars",
                "request_parameter_at_baseline_commit": {"adjustment": "all"},
                "data_manifest_declared_basis": "not retained",
                "raw_quote_execution_basis": "unadjusted SIP prices",
                "corporate_action_reconstruction": "unavailable",
            },
        },
        sources=(
            {
                "url": SIZE_SOURCE,
                "verified_on": str(VERIFIED_ON),
                "finding": "CTA/UTP shares from 2025-11-03; historical normalization unknown",
            },
            {
                "url": SEC_SOURCE,
                "verified_on": str(VERIFIED_ON),
                "fetch_status": "HTTP_403",
                "corroborated_by": FINRA_SEC_SOURCE,
            },
            {
                "url": FINRA_SEC_SOURCE,
                "verified_on": str(VERIFIED_ON),
                "finding": "0 through April 3; $20.60/million April 4; OTC charge = trade date",
            },
            {
                "url": TAF_SOURCE,
                "verified_on": str(VERIFIED_ON),
                "finding": "2024/25: .000166 cap 8.30; 2026: .000195 cap 9.79",
            },
            {
                "url": "https://alpaca.markets/support/regulatory-fees",
                "finding": "SEC rounded up; TAF per-trade rounded/capped; CAT pass-through",
            },
            {
                "url": "https://docs.alpaca.markets/us/docs/regulatory-fees",
                "finding": "EOD per-account total rounding; scope conflicts unresolved",
            },
            {
                "url": "https://docs.alpaca.markets/us/reference/stockbars",
                "finding": "raw/split/dividend/all explicitly distinct price/volume bases",
            },
            {
                "url": "https://docs.alpaca.markets/us/docs/fractional-trading",
                "finding": "asset eligibility required; qty/notional exclusive; nine decimals; "
                "extended-hours wording internally inconsistent; use frozen regular-session policy",
            },
            {
                "url": "https://docs.alpaca.markets/us/docs/paper-trading",
                "finding": "paper omits fees/dividends/impact/latency/queue; no NBBO size cap",
            },
        ),
        limitations=(
            "Synthetic gold cases check arithmetic, not actual trades, fills, or alpha.",
            "Account product, historical CAT pass-through periods and billing rounding unresolved.",
            "SEC rules outside verified intervals unavailable, not assumed zero; current costs "
            "are a separately labeled counterfactual, not observed historical fees.",
            "Raw HTTP responses/receipt timestamps were not retained in the inspected shards; "
            "normalized observations are preserved with hashes, never relabeled raw.",
            "Daily all-adjusted bars lack causal corporate-action reconstruction; legacy "
            "simulator is unavailable rather than returning a misleading zero-P&L validation.",
            "7,229 aggregates alternative simulations, not independent orders in one "
            "account; requested order sizes cannot be recovered from aggregate counters alone.",
            "Two protected holdouts remain unopened. Known 2016-2026 data is not fresh evidence.",
            "No corrected frozen historical run was performed; existing failures are not reversed, "
            "statistical qualification is not established and no returns were optimized.",
            "Recovery demonstration is synthetic; production lifecycle tests are separate "
            "and no broker response is fabricated.",
        ),
    )
