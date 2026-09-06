from __future__ import annotations

import inspect
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from tradeagent.execution_reference import (
    ReferenceFact,
    ReferenceLedger,
    ReferenceOrder,
    ReferenceQuote,
    ReferenceSession,
    first_eligible_arrival,
    reference_decision,
    reference_execute,
    reference_fee,
    reference_fee_totals,
    reference_submission_recovery,
    run_execution_accounting_audit,
    synthetic_gold_cases,
)

D = Decimal
AT = datetime(2026, 9, 4, 15, tzinfo=UTC)


@pytest.fixture
def synthetic_sessions() -> tuple[ReferenceSession, ...]:
    return (
        ReferenceSession(
            opened_at=AT - timedelta(hours=1),
            closed_at=AT + timedelta(hours=5),
            source="synthetic regular session fixture",
        ),
    )


@pytest.fixture
def synthetic_quote() -> ReferenceQuote:
    return ReferenceQuote(
        event_id="synthetic-q1",
        symbol="SYNTH",
        event_at=AT,
        provider_at=AT,
        received_at=AT,
        bid=D("99.99"),
        ask=D("100.01"),
        bid_size=D(1),
        ask_size=D(1),
        price_basis="raw",
        size_units="shares",
        size_provenance="synthetic schema",
        source="synthetic",
        evidence_kind="synthetic",
    )


@pytest.fixture
def synthetic_order() -> ReferenceOrder:
    return ReferenceOrder(
        account_id="synthetic-A",
        order_id="synthetic-order",
        symbol="SYNTH",
        side="buy",
        quantity=D("0.25"),
        decision_at=AT,
        arrival_at=AT,
        fractionable=True,
    )


def test_all_synthetic_gold_arithmetic_and_production_regressions() -> None:
    cases = synthetic_gold_cases()
    assert len(cases) >= 17
    for case in cases:
        assert case.evidence_kind == "synthetic"
        assert case.raw_provider_response is None
        assert case.status == "reference_pass", (case.case_id, case.expected, case.reference)
        assert case.expected == case.reference


def test_reference_arithmetic_never_calls_production_fill_or_pnl() -> None:
    for function in (reference_execute, reference_decision, reference_fee):
        source = inspect.getsource(function)
        assert "observed_execution" not in source
        assert "execution_calibration" not in source
        assert "performance_metrics" not in source
    assert "tradeagent." not in inspect.getsource(ReferenceLedger)


@pytest.mark.parametrize("category", ["market", "news", "corporate"])
@pytest.mark.parametrize("future_value", ["-999999", "0", "999999"])
def test_synthetic_future_facts_cannot_change_earlier_signal(
    category: str,
    future_value: str,
) -> None:
    before = ReferenceFact.model_validate(
        {
            "fact_id": "synthetic-known",
            "category": category,
            "event_at": AT,
            "available_at": AT,
            "value": D(1),
        }
    )
    # An old event only learned later is also forbidden at the decision boundary.
    future = before.model_copy(
        update={
            "fact_id": "synthetic-future",
            "event_at": AT - timedelta(days=10),
            "available_at": AT + timedelta(microseconds=1),
            "value": D(future_value),
        }
    )
    assert reference_decision((before, future), decision_at=AT) == reference_decision(
        (before,),
        decision_at=AT,
    )


def test_synthetic_future_outcome_changes_pnl_not_prior_signal() -> None:
    fact = ReferenceFact(
        fact_id="synthetic-known",
        category="market",
        event_at=AT,
        available_at=AT,
        value=D(1),
    )
    signal = reference_decision((fact,), decision_at=AT)
    ledger = ReferenceLedger(account_id="synthetic-A", cash=D(1000)).trade(
        "synthetic-buy",
        "SYNTH",
        D(1),
        D(100),
    )
    assert ledger.equity({"SYNTH": D(150)}) == 1050
    assert ledger.equity({"SYNTH": D(50)}) == 950
    assert reference_decision((fact,), decision_at=AT) == signal


def test_synthetic_close_completion_and_holiday_next_session() -> None:
    close = datetime(2026, 9, 4, 20, tzinfo=UTC)
    next_open = datetime(2026, 9, 8, 13, 30, tzinfo=UTC)
    sessions = (
        ReferenceSession(opened_at=close - timedelta(hours=6), closed_at=close, source="synthetic"),
        ReferenceSession(
            opened_at=next_open, closed_at=next_open + timedelta(hours=6), source="synthetic"
        ),
    )
    assert first_eligible_arrival(close, sessions) == next_open
    assert first_eligible_arrival(close + timedelta(seconds=1), sessions) == next_open
    assert first_eligible_arrival(close, sessions[:1]) is None


def test_synthetic_prior_quote_is_not_arrival_even_if_received_later(
    synthetic_quote: ReferenceQuote,
    synthetic_order: ReferenceOrder,
    synthetic_sessions: tuple[ReferenceSession, ...],
) -> None:
    quote = synthetic_quote.model_copy(
        update={
            "event_at": AT - timedelta(seconds=1),
            "provider_at": AT - timedelta(seconds=1),
        }
    )
    result = reference_execute(synthetic_order, (quote,), synthetic_sessions)
    assert result.status == "unavailable"
    assert not result.fills


@pytest.mark.parametrize(
    "bad_field,value,reason",
    [
        ("size_units", "unknown", "quote_size_units_unavailable"),
        ("size_provenance", None, "quote_size_units_unavailable"),
        ("price_basis", "adjusted", "raw_price_basis_unavailable"),
    ],
)
def test_synthetic_unknown_basis_or_size_fails_closed(
    synthetic_quote: ReferenceQuote,
    synthetic_order: ReferenceOrder,
    synthetic_sessions: tuple[ReferenceSession, ...],
    bad_field: str,
    value: object,
    reason: str,
) -> None:
    quote = synthetic_quote.model_copy(update={bad_field: value})
    result = reference_execute(synthetic_order, (quote,), synthetic_sessions)
    assert result.fills == ()
    assert result.reason == reason


def test_synthetic_no_blanket_100_size_conversion(synthetic_quote: ReferenceQuote) -> None:
    assert synthetic_quote.displayed_shares("buy") == 1
    lots = synthetic_quote.model_copy(update={"size_units": "round_lots"})
    assert lots.displayed_shares("buy") is None
    assert lots.model_copy(update={"round_lot_size": D(40)}).displayed_shares("buy") == 40


def test_synthetic_liquidity_depleted_per_account_and_not_reused_for_duplicate_quote(
    synthetic_quote: ReferenceQuote,
    synthetic_order: ReferenceOrder,
    synthetic_sessions: tuple[ReferenceSession, ...],
) -> None:
    order = synthetic_order.model_copy(update={"quantity": D(2)})
    capacity: dict[tuple[str, str, str], Decimal] = {}
    first = reference_execute(
        order,
        (synthetic_quote, synthetic_quote),
        synthetic_sessions,
        depleted=capacity,
    )
    second = reference_execute(order, (synthetic_quote,), synthetic_sessions, depleted=capacity)
    other = reference_execute(
        order.model_copy(update={"account_id": "synthetic-B"}),
        (synthetic_quote,),
        synthetic_sessions,
        depleted=capacity,
    )
    assert first.remaining_quantity == 1
    assert second.remaining_quantity == 2
    assert other.remaining_quantity == 1


def test_synthetic_fractional_market_limit_and_later_fill(
    synthetic_quote: ReferenceQuote,
    synthetic_order: ReferenceOrder,
    synthetic_sessions: tuple[ReferenceSession, ...],
) -> None:
    missed = reference_execute(
        synthetic_order.model_copy(update={"limit_price": D(100)}),
        (synthetic_quote,),
        synthetic_sessions,
    )
    assert missed.fills == ()
    fill = reference_execute(synthetic_order, (synthetic_quote,), synthetic_sessions)
    assert fill.fills[0].quantity == D("0.25")
    assert fill.fills[0].price == D("100.01")


def test_synthetic_delayed_receipt_and_cancel_prevent_later_execution(
    synthetic_quote: ReferenceQuote,
    synthetic_order: ReferenceOrder,
    synthetic_sessions: tuple[ReferenceSession, ...],
) -> None:
    late = synthetic_quote.model_copy(update={"received_at": AT + timedelta(seconds=61)})
    assert not reference_execute(synthetic_order, (late,), synthetic_sessions).fills
    cancelled = reference_execute(
        synthetic_order,
        (synthetic_quote,),
        synthetic_sessions,
        cancel_at=AT,
    )
    assert cancelled.status == "cancelled"
    assert not cancelled.fills


def test_synthetic_receipt_after_close_is_not_regular_session_fill(
    synthetic_quote: ReferenceQuote,
    synthetic_order: ReferenceOrder,
) -> None:
    session = ReferenceSession(
        opened_at=AT - timedelta(hours=1),
        closed_at=AT + timedelta(seconds=1),
        source="synthetic early close",
    )
    quote = synthetic_quote.model_copy(update={"received_at": AT + timedelta(seconds=2)})
    assert not reference_execute(synthetic_order, (quote,), (session,)).fills


def test_synthetic_future_corporate_action_does_not_rewrite_entitlement() -> None:
    original = ReferenceLedger(account_id="synthetic", cash=D(100)).trade(
        "synthetic-buy",
        "SYNTH",
        D(1),
        D(100),
    )
    entitled = original.dividend_entitlement(
        "synthetic-dividend",
        "SYNTH",
        D(1),
        date(2026, 9, 10),
    )
    later_split = entitled.split("synthetic-split", "SYNTH", D(100))
    assert later_split.positions["SYNTH"] == 100
    assert later_split.receivables[0].entitled_quantity == 1
    assert later_split.receivables[0].amount == 1
    assert entitled.positions["SYNTH"] == 1
    assert entitled.receivables == later_split.receivables


def test_synthetic_fee_date_provenance_and_unknown_history() -> None:
    arguments = {"side": "sell", "quantity": D(10), "notional": D(1000)}
    before = reference_fee("sec", **arguments, charge_date=date(2026, 4, 3))
    after = reference_fee("sec", **arguments, charge_date=date(2026, 4, 4))
    assert before.raw_amount == 0
    assert after.raw_amount == D("0.0206")
    assert after.charged_amount is None
    assert after.rule is not None and "charge_date" in after.rule.date_semantics
    assert reference_fee("sec", **arguments, charge_date=None).status == "unavailable"
    assert reference_fee("sec", **arguments, charge_date=date(2016, 1, 4)).raw_amount is None
    counterfactual = reference_fee(
        "sec",
        **arguments,
        charge_date=date(2016, 1, 4),
        scenario="current_business_cost",
    )
    assert counterfactual.raw_amount == D("0.0206")
    assert counterfactual.scenario == "current_business_cost"
    assert counterfactual.charge_date == date(2016, 1, 4)
    assert counterfactual.schedule_date == date(2026, 9, 6)


@pytest.mark.parametrize(
    "day,rate,cap",
    [
        (date(2025, 12, 31), ".000166", "8.30"),
        (date(2026, 1, 1), ".000195", "9.79"),
    ],
)
def test_synthetic_taf_effective_date_and_per_trade_cap(day: date, rate: str, cap: str) -> None:
    small = reference_fee(
        "taf", side="sell", quantity=D(1), notional=D(100), charge_date=day, trade_date=day
    )
    large = reference_fee(
        "taf", side="sell", quantity=D(100000), notional=D(1000000), charge_date=day, trade_date=day
    )
    assert small.raw_amount == D(rate)
    assert large.raw_amount == D(cap)


def test_synthetic_fee_rounding_never_pools_alternative_accounts() -> None:
    charges = [
        ("synthetic-A", "sec", date(2026, 4, 6), D(".001")),
        ("synthetic-A", "sec", date(2026, 4, 6), D(".001")),
        ("synthetic-B", "sec", date(2026, 4, 6), D(".001")),
    ]
    assert reference_fee_totals(charges, rounding_scope="per_trade") == {
        "synthetic-A": D(".02"),
        "synthetic-B": D(".01"),
    }
    assert reference_fee_totals(charges, rounding_scope="account_rule_day") == {
        "synthetic-A": D(".01"),
        "synthetic-B": D(".01"),
    }
    assert D(".01") / D(10) * 10000 == 10
    assert D(".01") / D(25) * 10000 == 4


def test_synthetic_split_dividend_and_fractional_cash_ledger() -> None:
    ledger = ReferenceLedger(account_id="synthetic", cash=D(100)).trade(
        "synthetic-buy",
        "SYNTH",
        D(".5"),
        D(100),
    )
    split = ledger.split("synthetic-split", "SYNTH", D(2))
    assert split.positions["SYNTH"] == 1
    assert split.cost_basis["SYNTH"] == 50
    assert split.equity({"SYNTH": D(50)}) == 100
    ex = split.dividend_entitlement("synthetic-dividend", "SYNTH", D(1), date(2026, 9, 10))
    assert ex.cash == 50
    assert ex.equity({"SYNTH": D(49)}) == 100
    exited = ex.trade("synthetic-sell", "SYNTH", D(-1), D(49))
    assert exited.equity({}) == 100
    paid = exited.pay_dividend("synthetic-dividend", paid_on=date(2026, 9, 10))
    assert paid.cash == 100 and paid.equity({}) == 100
    with pytest.raises(ValueError, match="missing receivable"):
        paid.pay_dividend("synthetic-dividend", paid_on=date(2026, 9, 10))
    with pytest.raises(ValueError, match="adjusted"):
        ex.equity({"SYNTH": D(49)}, price_basis="adjusted")


def test_synthetic_ambiguous_submission_never_blindly_retries() -> None:
    for complete in (False, True):
        result = reference_submission_recovery("synthetic-id", (), lookup_complete=complete)
        assert result["state"] == "blocked"
        assert result["additional_submissions"] == 0
    recovered = reference_submission_recovery(
        "synthetic-id",
        ("synthetic-id",),
        lookup_complete=True,
    )
    assert recovered == {"state": "reconciled", "additional_submissions": 0}


def test_real_boundary_report_read_only_and_serializable() -> None:
    root = Path(__file__).resolve().parents[1]
    report = run_execution_accounting_audit(root, generated_at=AT)
    assert report.status == "passed_bounded_checks_historical_validation_unavailable"
    assert report.historical_results_valid is False
    assert report.untouched_holdouts_opened is False
    assert report.historical_reruns == 0
    assert report.model_validate_json(report.model_dump_json()) == report
    # Repository artifacts may be absent in installed-source test environments.
    for artifact in report.artifacts:
        if artifact["status"] == "preserved_read_only":
            assert len(str(artifact["sha256"])) == 64
    assert report.superseded_reasons
    assert report.real_data_boundary["immutable_raw_provider_responses_available"] is False
