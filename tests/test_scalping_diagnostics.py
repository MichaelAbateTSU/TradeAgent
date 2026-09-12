from __future__ import annotations

import copy
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal, getcontext
from typing import Any

import pytest

from tradeagent.scalping_diagnostics import (
    FAILURE_LABELS,
    HORIZON_NAMES,
    HORIZONS_MS,
    DiagnosticPolicy,
    QuoteTimeline,
    reconstruct_period,
)

D = Decimal
NOW = datetime(2026, 9, 11, 12, tzinfo=UTC)
BASE = 1_789_128_000_000_000_000


def stamp(milliseconds: int) -> str:
    return (NOW + timedelta(milliseconds=milliseconds)).isoformat()


def quote(
    milliseconds: int,
    mid: str = "100",
    *,
    delay: int = 0,
    symbol: str = "BTC/USD",
    size: str = "1000",
) -> dict[str, Any]:
    return {
        "event_id": f"{symbol}:{milliseconds}:{mid}:{delay}",
        "batch_id": f"batch-{milliseconds // 1000}",
        "symbol": symbol,
        "bid": str(D(mid) - D(".01")),
        "ask": str(D(mid) + D(".01")),
        "bid_size": size,
        "ask_size": size,
        "exchange_at_ns": BASE + milliseconds * 1_000_000,
        "received_at_ns": BASE + (milliseconds + delay) * 1_000_000,
        "received_monotonic_ns": milliseconds * 1_000_000,
        "source": "native_quote",
    }


def snapshot(
    *,
    identity: str = "c",
    entry_fills: list[tuple[int, str, str]] | None = None,
    exit_fills: list[tuple[int, str, str]] | None = None,
    state: str = "closed_owned_flat",
    entry_status: str = "filled",
    short: bool = False,
) -> dict[str, Any]:
    entry_fills = entry_fills if entry_fills is not None else [(1000, "1", "100")]
    exit_fills = exit_fills if exit_fills is not None else [(5000, "1", "101")]
    eq = sum((D(q) for _, q, _ in entry_fills), D(0))
    ev = sum((D(q) * D(p) for _, q, p in entry_fills), D(0))
    xq = sum((D(q) for _, q, _ in exit_fills), D(0))
    xv = sum((D(q) * D(p) for _, q, p in exit_fills), D(0))
    owned = eq - xq if state == "open" else D(0)
    decision_quote = {
        "symbol": "BTC/USD",
        "bid": "100",
        "ask": "102",
        "bid_size": "1000",
        "ask_size": "1000",
        "exchange_at": stamp(0),
        "exchange_time_ns": BASE,
        "received_at": stamp(0),
    }
    signal = {
        "decision_id": "decision-" + identity,
        "symbol": "BTC/USD",
        "action": "sell" if short else "buy",
        "family": "momentum",
        "observed_at": stamp(0),
        "quote": decision_quote,
        "score": 0.8,
        "features": {
            "score_kind": "heuristic_not_probability",
            "normalized_ofi_5s": 0.2,
            "l1_imbalance": 0.3,
            "return_1s_bps": 1.5,
            "return_5s_bps": 2.5,
            "exact_queue_position_available": 0,
        },
        "expected_net_edge_bps": None,
        "reasons": ["positive_momentum"],
    }
    cycle = {
        "cycle_id": identity,
        "run_id": "run",
        "account_digest": "account",
        "decision_id": signal["decision_id"],
        "symbol": "BTC/USD",
        "state": state,
        "created_at": stamp(0),
        "opened_at": stamp(entry_fills[0][0]) if entry_fills else None,
        "closed_at": stamp(exit_fills[-1][0] + 100)
        if exit_fills
        else stamp(6000)
        if state == "no_fill"
        else None,
        "exit_due_at": stamp(entry_fills[0][0] + 15000) if entry_fills else None,
        "owned_quantity": str(owned),
        "entry_quantity": str(eq),
        "entry_value": str(ev),
        "exit_quantity": str(xq),
        "exit_value": str(xv),
        "gross_cash_flow": str((xv - ev) * (-1 if short else 1)),
        "actual_cash_fees": "0",
        "actual_base_fees": "0",
        "actual_net_pnl": None,
        "modeled_net_pnl": None,
        "fees_pending": True,
        "payload": {
            "signal": signal,
            "config": {
                "maker_fee_bps": "15",
                "taker_fee_bps": "25",
                "feature_horizon_seconds": 5,
                "decision_interval_seconds": 1,
                "entry_order_ttl_seconds": 3,
                "entry_style": "passive",
                "exit_after_seconds": 15,
            },
            "exit_reason": "signal",
            "inferred_account_base_fee_allocation": "0",
            "inferred_account_cash_fee_allocation": "0",
        },
    }
    data: dict[str, Any] = {
        "schema": "scalp-execution-evidence-v1",
        "account_digest": "account",
        "period_start": stamp(-10000),
        "period_end": stamp(900000),
        "cycles": [cycle],
        "orders": [],
        "activities": [],
        "audits": [],
        "quotes": [],
        "broker_historical_orders": [],
    }
    for leg, fills, side, status in (
        ("entry", entry_fills, "sell" if short else "buy", entry_status),
        ("exit", exit_fills, "buy" if short else "sell", "filled"),
    ):
        if leg == "exit" and not fills:
            continue
        client, broker_id = f"{identity}-{leg}-client", f"{identity}-{leg}-broker"
        quantity = sum((D(q) for _, q, _ in fills), D(0))
        value = sum((D(q) * D(p) for _, q, p in fills), D(0))
        broker = {
            "id": broker_id,
            "client_order_id": client,
            "symbol": "BTC/USD",
            "side": side,
            "status": status,
            "quantity": "2" if status == "canceled" and fills else str(quantity),
            "filled_quantity": str(quantity),
            "filled_average_price": str(value / quantity) if quantity else None,
            "filled_at": stamp(fills[-1][0]) if fills else None,
            "submitted_at": stamp(200 if leg == "entry" else fills[0][0] - 200),
        }
        order = {
            "cycle_id": identity,
            "run_id": "run",
            "client_order_id": client,
            "broker_order_id": broker_id,
            "symbol": "BTC/USD",
            "side": side,
            "status": status,
            "quantity": broker["quantity"],
            "filled_quantity": str(quantity),
            "strategy_version": "v30:run",
            "created_at": stamp(0 if leg == "entry" else fills[0][0] - 400),
            "submission_started_at": stamp(100 if leg == "entry" else fills[0][0] - 300),
            "first_positive_fill_at": stamp(fills[0][0]) if fills else None,
            "expires_at": stamp(3000) if leg == "entry" else None,
            "dispatch_state": "acknowledged",
            "broker": broker,
            "intent": {
                "request": {
                    "client_order_id": client,
                    "side": side,
                    "symbol": "BTC/USD",
                    "quantity": broker["quantity"],
                    "order_type": "limit" if leg == "entry" else "market",
                },
                "limit_price": (
                    str(
                        (max if side == "buy" else min)(
                            (D(price) for _, _, price in fills), default=D("100")
                        )
                    )
                    if leg == "entry"
                    else None
                ),
                "quote": copy.deepcopy(decision_quote),
            },
        }
        data["orders"].append(order)
        public = copy.deepcopy(broker)
        public["qty"] = public.pop("quantity")
        public["filled_qty"] = public.pop("filled_quantity")
        public["filled_avg_price"] = public.pop("filled_average_price")
        public["symbol"] = "BTCUSD"
        data["broker_historical_orders"].append(public)
        cumulative = D(0)
        for index, (at, qty, price) in enumerate(fills):
            cumulative += D(qty)
            activity_id = f"{identity}-{leg}-fill-{index}"
            data["activities"].append(
                {
                    "activity_id": activity_id,
                    "activity_key": "key-" + activity_id,
                    "kind": "FILL",
                    "cycle_id": identity,
                    "account_digest": "account",
                    "broker_order_id": broker_id,
                    "symbol": "BTC/USD",
                    "side": side,
                    "quantity": qty,
                    "net_amount": "0",
                    "occurred_at": stamp(at),
                    "time_precision": "timestamp",
                    "attribution_basis": "broker_order_id",
                    "payload": {
                        "id": activity_id,
                        "activity_type": "FILL",
                        "order_id": broker_id,
                        "symbol": "BTC/USD",
                        "side": side,
                        "qty": qty,
                        "price": price,
                        "cum_qty": str(cumulative),
                        "leaves_qty": str(quantity - cumulative),
                        "transaction_time": stamp(at),
                    },
                }
            )
        data["audits"].append(
            {
                "event_id": "ack-" + client,
                "event_type": "scalp_broker_order_observation",
                "occurred_at": stamp(300 if leg == "entry" else fills[0][0] - 100),
                "payload": {
                    "client_order_id": client,
                    "source_received_at": stamp(300 if leg == "entry" else fills[0][0] - 100),
                },
            }
        )
    if exit_fills:
        data["audits"].append(
            {
                "event_id": "exit-request-" + identity,
                "event_type": "scalp_exit_requested",
                "occurred_at": stamp(exit_fills[0][0] - 400),
                "payload": {"cycle_id": identity, "reason": "signal"},
            }
        )
    return data


def add_fee(data: dict[str, Any], leg: str, *, base: str = "0", cash: str = "0") -> None:
    cycle_id = data["cycles"][0]["cycle_id"]
    order = next(
        row for row in data["orders"] if row["client_order_id"] == f"{cycle_id}-{leg}-client"
    )
    identity = f"{cycle_id}-{leg}-fee"
    data["activities"].append(
        {
            "activity_id": identity,
            "kind": "CFEE",
            "cycle_id": cycle_id,
            "broker_order_id": order["broker_order_id"],
            "attribution_basis": "broker_order_id",
            "side": order["side"],
            "symbol": "BTC/USD",
            "occurred_at": stamp(10000),
            "quantity": str(-D(base)),
            "net_amount": str(-D(cash)),
            "payload": {
                "id": identity,
                "order_id": order["broker_order_id"],
                "activity_type": "CFEE",
                "qty": str(-D(base)),
                "net_amount": str(-D(cash)),
                "currency": "USD",
                "symbol": "BTC/USD",
                "side": order["side"],
            },
        }
    )


def trade(data: dict[str, Any]) -> dict[str, Any]:
    result = reconstruct_period(data)
    assert len(result["trades"]) == 1
    return result["trades"][0]


def value(measurement: dict[str, Any]) -> Decimal | None:
    return D(measurement["value"]) if measurement["value"] is not None else None


def test_normalizes_public_and_typed_cumulative_fills_independently() -> None:
    data = snapshot(
        entry_fills=[(1000, ".4", "100"), (1200, ".6", "102")],
        exit_fills=[(5000, "1", "103")],
    )
    result = trade(data)
    assert value(result["entry"]["quantity"]) == 1
    assert value(result["entry"]["weighted_price"]) == D("101.2")
    assert value(result["entry"]["value"]) == D("101.2")
    assert value(result["pnl"]["raw_price_pnl"]) == D("1.8")
    assert value(result["pnl"]["gross_cash_flow"]) == D("1.8")
    assert result["reconciliation"]["independent_orders_checked"] == 2
    assert result["reconciliation"]["all_filled_orders_independently_crosschecked"]


def test_markouts_use_each_partial_execution_time_price_and_quantity() -> None:
    data = snapshot(
        entry_fills=[(1000, ".4", "100"), (1200, ".6", "102")],
        exit_fills=[(5000, "1", "103")],
    )
    data["quotes"] = [quote(1100, "101"), quote(1300, "103")]
    mark = trade(data)["markouts"]["100ms"]
    assert value(mark["signed_price"]) == 1
    assert mark["samples"] == 2
    assert [row["target_at_ns"] for row in mark["observations"]] == [
        BASE + 1_100_000_000,
        BASE + 1_300_000_000,
    ]
    data["quotes"].pop()
    missing = trade(data)["markouts"]["100ms"]
    assert missing["signed_price"]["value"] is None
    assert missing["samples"] == 1 and missing["missing"] == 1
    assert D(missing["quantity_coverage"]) == D(".4")


@pytest.mark.parametrize("status", ["canceled", "expired", "rejected"])
def test_terminal_zero_fill_orders_never_become_trades(status: str) -> None:
    data = snapshot(entry_fills=[], exit_fills=[], state="no_fill", entry_status=status)
    data["cycles"][0]["actual_net_pnl"] = "-100"
    data["cycles"][0]["modeled_net_pnl"] = "-100"
    result = reconstruct_period(data)
    assert result["trades"] == []
    assert result["summary"]["completed_trades"] == 0
    assert result["summary"]["no_fill_cycles"] == 1
    assert result["filled_vs_unfilled"]["unfilled"]["signals"] == 1
    assert result["summary"]["pnl_totals"]["actual_net_pnl"]["value"] is None


def test_canceled_positive_partial_fill_counts_only_executed_quantity() -> None:
    data = snapshot(
        entry_fills=[(1000, ".4", "100")],
        exit_fills=[(5000, ".4", "101")],
        entry_status="canceled",
    )
    result = reconstruct_period(data)
    assert result["summary"]["completed_trades"] == 1
    assert result["filled_vs_unfilled"]["filled"]["signals"] == 1
    row = result["trades"][0]
    assert value(row["entry"]["quantity"]) == D(".4")
    assert value(row["pnl"]["gross_cash_flow"]) == D(".4")
    assert row["entry"]["orders"][0]["status"] == "canceled"


def test_open_inventory_is_not_realized_even_if_stored_pnl_is_present() -> None:
    data = snapshot(exit_fills=[], state="open")
    data["cycles"][0]["actual_net_pnl"] = "-100"
    result = reconstruct_period(data)
    assert not result["trades"]
    row = result["open_or_unresolved"][0]
    assert value(row["pnl"]["cash_flow_to_date_not_realized"]) == -100
    for field in ("raw_price_pnl", "gross_cash_flow", "actual_net_pnl", "modeled_net_pnl"):
        assert row["pnl"][field]["value"] is None
        assert row["pnl"][field]["reason"] == "position_not_closed"
    assert result["filled_vs_unfilled"]["filled"]["signals"] == 1


def test_base_fee_cash_fee_and_missing_exit_reserve_are_not_double_charged() -> None:
    data = snapshot(exit_fills=[(5000, ".9985", "101")])
    add_fee(data, "entry", base=".0015", cash="1")
    row = trade(data)
    assert value(row["pnl"]["raw_price_pnl"]) == D(".9985")
    assert value(row["pnl"]["gross_cash_flow"]) == D(".8485")
    assert value(row["fees"]["quantity_difference_value_already_in_cashflow"]) == D(".15")
    assert value(row["fees"]["estimated_base_reserve_quantity"]) == 0
    assert value(row["fees"]["estimated_cash_reserve"]) == D(".25212125")
    assert value(row["pnl"]["modeled_net_pnl"]) == D("-.40362125")
    assert row["pnl"]["actual_net_pnl"]["value"] is None
    assert row["fees"]["entry_fee"]["value"] == "1"
    assert row["fees"]["exit_fee"]["value"] is None
    assert not row["fees"]["spread_deducted_again"]
    assert not row["fees"]["confirmed_base_fee_deducted_again"]


def test_actual_fees_replace_estimates_even_when_actual_fee_is_lower() -> None:
    data = snapshot(exit_fills=[(5000, ".9985", "101")])
    add_fee(data, "entry", base=".0015", cash="1")
    add_fee(data, "exit", cash=".15")
    data["cycles"][0]["payload"]["inferred_account_cash_fee_allocation"] = "99"
    row = trade(data)
    assert value(row["pnl"]["actual_net_pnl"]) == D("-.3015")
    assert value(row["pnl"]["modeled_net_pnl"]) == D("-.3015")
    assert value(row["fees"]["estimated_cash_reserve"]) == 0
    assert value(row["fees"]["estimated_base_reserve_quantity"]) == 0
    assert row["fees"]["complete_order_fee_coverage"]
    assert not row["fees"]["estimated_fee_applied_to_confirmed_orders"]


def test_no_fee_postings_mean_unknown_actual_cost_not_zero_actual_fees() -> None:
    row = trade(snapshot())
    assert row["fees"]["observed_confirmed_cash_subtotal"] == "0"
    assert row["fees"]["actual_cash_fees"]["value"] is None
    assert row["fees"]["actual_base_fees_quantity"]["value"] is None
    assert row["pnl"]["actual_net_pnl"]["value"] is None
    assert value(row["pnl"]["modeled_net_pnl"]) is not None
    assert "ACCOUNTING_FAILURE" not in row["failure_attribution"]["labels"]


def test_unkeyed_inferred_fee_remains_unconfirmed_and_reserve_is_disclosed() -> None:
    data = snapshot(exit_fills=[(5000, ".9985", "101")])
    add_fee(data, "entry", base=".0015", cash=".5")
    activity = data["activities"][-1]
    activity["broker_order_id"] = None
    activity["payload"].pop("order_id")
    activity["attribution_basis"] = "unique_symbol_day"
    activity["attributed_order_id"] = "c-entry-broker"
    data["cycles"][0]["payload"]["inferred_account_cash_fee_allocation"] = ".5"
    data["cycles"][0]["payload"]["inferred_account_base_fee_allocation"] = ".0015"
    row = trade(data)
    assert row["fees"]["confirmed_fee_records"] == 0
    assert value(row["fees"]["estimated_base_reserve_quantity"]) == D(".001")
    assert value(row["fees"]["estimated_cash_reserve"]) == D(".5")
    assert row["pnl"]["actual_net_pnl"]["value"] is None


def test_unexplained_inventory_difference_prevents_confirmed_actual_pnl() -> None:
    data = snapshot(exit_fills=[(5000, ".9", "101")])
    add_fee(data, "entry")
    add_fee(data, "exit")
    row = trade(data)
    assert row["fees"]["complete_order_fee_coverage"]
    assert row["pnl"]["actual_net_pnl"]["value"] is None
    assert "unexplained_inventory" in row["pnl"]["actual_net_pnl"]["reason"]


@pytest.mark.parametrize(
    "field,wrong", [("symbol", "ETHUSD"), ("side", "sell"), ("id", "other-broker")]
)
def test_mispaired_historical_orders_are_quarantined(field: str, wrong: str) -> None:
    data = snapshot()
    data["broker_historical_orders"][0][field] = wrong
    result = reconstruct_period(data)
    assert result["summary"]["completed_trades"] == 0
    assert result["open_or_unresolved"][0]["pnl"]["actual_net_pnl"]["value"] is None
    assert result["data_quality"]["issues"]


def test_mispaired_activity_does_not_supply_fill_coverage_or_accounting_proof() -> None:
    data = snapshot()
    data["activities"][0]["cycle_id"] = "another-cycle"
    row = trade(data)
    assert row["reconciliation"]["fill_activity_orders_checked"] == 1
    assert not row["reconciliation"]["all_filled_orders_independently_crosschecked"]
    assert row["timing"]["timestamps"]["fill_time"]["value"] is None
    assert row["failure_attribution"]["assessments"]["ACCOUNTING_FAILURE"]["status"] == "unknown"


def test_duplicate_orders_and_activities_do_not_double_count_cash_flows() -> None:
    data = snapshot()
    data["orders"].append(copy.deepcopy(data["orders"][0]))
    data["activities"].append(copy.deepcopy(data["activities"][0]))
    result = reconstruct_period(data)
    row = result["trades"][0]
    assert value(row["entry"]["quantity"]) == 1
    assert value(row["pnl"]["gross_cash_flow"]) == 1
    assert result["data_quality"]["issue_counts"]["duplicate_order"] == 1
    assert result["data_quality"]["issue_counts"]["duplicate_activity"] == 1
    assert row["reconciliation"]["all_filled_orders_independently_crosschecked"]


def test_conflicting_duplicate_activity_is_not_arbitrarily_selected() -> None:
    data = snapshot()
    other = copy.deepcopy(data["activities"][0])
    other["payload"]["price"] = "500"
    data["activities"].append(other)
    result = reconstruct_period(data)
    row = result["trades"][0]
    assert result["data_quality"]["issue_counts"]["conflicting_duplicate_activity"] == 1
    assert row["reconciliation"]["fill_activity_orders_checked"] == 1
    assert not row["reconciliation"]["accounting_failure_proven"]


def test_independently_proven_fill_projection_mismatch_is_accounting_failure() -> None:
    data = snapshot()
    data["cycles"][0]["entry_value"] = "99"
    row = trade(data)
    assert row["reconciliation"]["accounting_failure_proven"]
    assert row["reconciliation"]["proven_inconsistencies"] == ["entry_value"]
    assert "ACCOUNTING_FAILURE" in row["failure_attribution"]["labels"]


def test_independent_history_and_activities_can_prove_local_broker_projection_wrong() -> None:
    data = snapshot()
    data["orders"][0]["broker"]["filled_average_price"] = "99"
    row = trade(data)
    assert value(row["entry"]["weighted_price"]) == 100
    assert row["reconciliation"]["accounting_failure_proven"]
    assert "stored_order:c-entry-client" in row["reconciliation"]["proven_inconsistencies"]


def test_small_decimal_rounding_and_model_assumptions_are_not_accounting_failures() -> None:
    data = snapshot()
    data["cycles"][0]["entry_value"] = "100.0000000001"
    data["cycles"][0]["modeled_net_pnl"] = "-999"
    row = trade(data)
    differences = row["reconciliation"]["differences_reconstructed_minus_stored"]
    assert differences["entry_value"]["within_tolerance"]
    assert not differences["modeled_net_pnl"]["within_tolerance"]
    assert not row["reconciliation"]["accounting_failure_proven"]


def test_incomplete_fill_activity_page_is_unknown_not_an_accounting_bug() -> None:
    data = snapshot(entry_fills=[(1000, ".4", "100"), (1200, ".6", "100")])
    data["activities"].pop(1)
    row = trade(data)
    assert value(row["entry"]["quantity"]) == 1
    assert not row["reconciliation"]["all_filled_orders_independently_crosschecked"]
    assert "ACCOUNTING_FAILURE" not in row["failure_attribution"]["labels"]
    assert row["markouts"]["100ms"]["observations"][0]["fill_basis"].startswith("cumulative")
    assert row["excursions"]["mfe_bps"]["value"] is None


def test_all_seven_horizons_have_exact_target_and_quote_provenance() -> None:
    data = snapshot()
    data["quotes"] = [
        quote(1000 + horizon, str(D(100) + D(horizon) / 10000)) for horizon in HORIZONS_MS
    ]
    result = reconstruct_period(data)
    row = result["trades"][0]
    assert tuple(row["markouts"]) == HORIZON_NAMES
    for name, horizon in zip(HORIZON_NAMES, HORIZONS_MS, strict=True):
        mark = row["markouts"][name]
        assert value(mark["signed_price"]) == D(horizon) / 10000
        assert mark["max_age_ms"] == min(250, horizon)
        observation = mark["observations"][0]
        sample = observation["sample"]
        assert observation["target_at_ns"] == BASE + (1000 + horizon) * 1_000_000
        assert sample["exchange_at_ns"] <= observation["target_at_ns"]
        assert sample["received_at_ns"] <= observation["target_at_ns"]
        assert sample["batch_id"] is not None
        assert D(sample["capture_delay_ms"]) == 0
        assert result["coverage"]["markouts"][name] == {"samples": 1, "missing": 0}


def test_future_quote_is_never_used_even_when_it_is_nearest() -> None:
    data = snapshot()
    data["quotes"] = [quote(1050, "99"), quote(1101, "10000")]
    mark = trade(data)["markouts"]["100ms"]
    assert value(mark["signed_price"]) == -1
    assert mark["observations"][0]["sample"]["exchange_at_ns"] == BASE + 1_050_000_000
    data["quotes"].pop(0)
    assert trade(data)["markouts"]["100ms"]["signed_price"]["value"] is None


def test_exchange_time_before_target_does_not_allow_late_receipt() -> None:
    data = snapshot()
    data["quotes"] = [quote(1090, "999", delay=30)]
    mark = trade(data)["markouts"]["100ms"]
    assert mark["signed_price"]["value"] is None
    assert mark["observations"][0]["reason"] == "no_causal_fresh_quote"


def test_100ms_horizon_does_not_accept_a_200ms_old_quote() -> None:
    data = snapshot()
    data["quotes"] = [quote(900), quote(1050)]
    row = trade(data)
    assert (
        row["markouts"]["100ms"]["observations"][0]["sample"]["exchange_at_ns"]
        == BASE + 1_050_000_000
    )
    data["quotes"] = [quote(900)]
    assert trade(data)["markouts"]["100ms"]["signed_price"]["value"] is None


@pytest.mark.parametrize(
    "mutations,reason",
    [
        ({"bid": "102", "ask": "100"}, "crossed_quote"),
        ({"bid": "NaN"}, "missing_or_invalid_quote_price_symbol"),
        ({"ask": "0"}, "missing_or_invalid_quote_price_symbol"),
        ({"received_at_ns": BASE + 1_000_000_000}, "negative_capture_delay"),
        ({"received_at_ns": BASE + 1_500_000_000}, "lagged_at_capture"),
        ({"source": "interpolated"}, "unsupported_quote_source"),
        ({"received_at_ns": None}, "missing_or_invalid_quote_timestamp"),
    ],
)
def test_invalid_crossed_lagged_and_untrusted_quotes_are_rejected(
    mutations: dict[str, Any], reason: str
) -> None:
    data = snapshot()
    item = quote(1090, "999")
    item.update(mutations)
    data["quotes"] = [item]
    result = reconstruct_period(data)
    assert result["coverage"]["quote_rejections"][reason] == 1
    assert result["trades"][0]["markouts"]["100ms"]["signed_price"]["value"] is None


def test_mfe_mae_exclude_prices_before_entry_after_exit_and_received_after_exit() -> None:
    data = snapshot()
    data["quotes"] = [
        quote(999, "10000"),
        quote(1000, "99"),
        quote(2000, "98"),
        quote(4999, "101"),
        quote(4999, "8000", delay=2),
        quote(5001, "10000"),
        quote(6000, "20000"),
    ]
    row = trade(data)
    curve = row["excursions"]
    assert value(curve["mfe_signed_price"]) == 1
    assert value(curve["mae_signed_price"]) == -2
    assert curve["sample_count"] == 3
    assert (
        curve["mfe_quote"]["exchange_at_ns"]
        < row["timing"]["timestamps"]["exit_fill_time"]["unix_ns"]
    )
    assert value(row["markouts"]["5s"]["signed_price"]) == 19900
    assert row["markouts"]["5s"]["observations"][0]["after_position_exit"]


def test_mfe_mae_are_signed_not_clamped_and_incomplete_curves_stay_partial() -> None:
    data = snapshot()
    data["quotes"] = [quote(1000, "99"), quote(2000, "98")]
    curve = trade(data)["excursions"]
    assert value(curve["mfe_signed_price"]) == -1
    assert value(curve["mae_signed_price"]) == -2
    assert not curve["complete_freshness_coverage"]
    assert curve["complete_mfe_bps"]["value"] is None
    assert D(curve["uncovered_ms"]) > 0


def test_dense_holding_quotes_can_establish_bounded_age_coverage() -> None:
    data = snapshot()
    data["quotes"] = [quote(at, "101") for at in range(1000, 5001, 250)]
    row = trade(data)
    assert row["excursions"]["complete_freshness_coverage"]
    assert value(row["excursions"]["complete_mfe_bps"]) == 100
    assert D(row["excursions"]["uncovered_ms"]) == 0


def test_partial_entry_lifetime_starts_at_first_fill_not_last_vwap_timestamp() -> None:
    data = snapshot(entry_fills=[(1000, ".4", "100"), (3000, ".6", "102")])
    data["quotes"] = [quote(2000, "105"), quote(4000, "102")]
    row = trade(data)
    assert value(row["timing"]["holding_seconds"]) == 4
    assert value(row["entry"]["weighted_price"]) == D("101.2")
    assert value(row["excursions"]["reference_price"]) == 100
    assert value(row["excursions"]["mfe_signed_price"]) == 5


def test_short_side_cashflow_and_excursion_signs() -> None:
    data = snapshot(short=True, exit_fills=[(5000, "1", "99")])
    data["quotes"] = [quote(1000, "101"), quote(2000, "102"), quote(4000, "99")]
    row = trade(data)
    assert row["side"] == "short"
    assert value(row["pnl"]["raw_price_pnl"]) == 1
    assert value(row["pnl"]["gross_cash_flow"]) == 1
    assert value(row["excursions"]["mfe_signed_price"]) == 1
    assert value(row["excursions"]["mae_signed_price"]) == -2


def test_unknown_predictions_confidence_queue_and_stage_telemetry_stay_unknown() -> None:
    result = reconstruct_period(snapshot())
    row = result["trades"][0]
    for name in (
        "predicted_direction",
        "predicted_move_bps",
        "predicted_horizon_seconds",
        "expected_net_edge_bps",
        "predicted_fill_probability",
        "confidence",
        "uncertainty",
    ):
        assert row["prediction"][name]["value"] is None
        assert row["prediction"][name]["reason"]
    assert value(row["prediction"]["heuristic_score"]) == D(".8")
    assert row["features"]["requested"]["queue_position"]["value"] is None
    assert row["features"]["requested"]["cancel_intensity"]["value"] is None
    assert result["latency_stages_ms"]["feed_processing"]["samples"] == 0
    assert result["latency_stages_ms"]["feed_processing"]["missing"] == 1
    assert result["latency_stages_ms"]["decision_fill"]["proxy_samples"] == 1
    assert result["latency_stages_ms"]["decision_fill"]["recorded_stage_samples"] == 0
    assert value(row["timing"]["stages_ms"]["decision_fill"]) == 1000


def test_future_stage_telemetry_joins_dispatch_and_fill_audits_by_client_id() -> None:
    data = snapshot()
    data["cycles"][0]["payload"]["signal"]["timing"] = {
        "t0_received_ns": BASE - 20_000_000,
        "t1_decoded_ns": BASE - 10_000_000,
        "t2_state_updated_ns": BASE,
        "t3_features_ns": BASE + 5_000_000,
        "t4_model_ns": BASE + 10_000_000,
    }
    data["audits"].extend(
        [
            {
                "event_type": "scalp_dispatch_timing",
                "payload": {
                    "client_order_id": "c-entry-client",
                    "timing": {
                        "t5_risk_approved_ns": BASE + 150_000_000,
                        "t6_submitted_ns": BASE + 200_000_000,
                        "t7_acknowledged_ns": BASE + 300_000_000,
                    },
                },
            },
            {
                "event_type": "scalp_fill_timing",
                "payload": {
                    "client_order_id": "c-entry-client",
                    "timing": {"t8_filled_ns": BASE + 1_000_000_000},
                },
            },
            {
                "event_type": "scalp_dispatch_timing",
                "payload": {
                    "client_order_id": "another-entry-client",
                    "timing": {"t6_submitted_ns": BASE + 900_000_000},
                },
            },
        ]
    )
    result = reconstruct_period(data)
    stages = result["trades"][0]["timing"]["stages_ms"]
    assert value(stages["feed_processing"]) == 10
    assert value(stages["model"]) == 5
    assert value(stages["decision_send"]) == 190
    assert value(stages["send_ack"]) == 100
    assert value(stages["ack_fill"]) == 700
    assert result["latency_stages_ms"]["send_ack"]["recorded_stage_samples"] == 1


def test_reversed_or_conflicting_latency_clocks_are_missing_not_zero() -> None:
    data = snapshot()
    data["cycles"][0]["payload"]["signal"]["timing"] = {
        "t7_acknowledged_ns": BASE + 2_000_000_000,
        "t8_filled_ns": BASE + 1_000_000_000,
    }
    row = trade(data)
    mark = row["timing"]["stages_ms"]["ack_fill"]
    assert mark["value"] is None
    assert mark["reason"] == "end_precedes_start_cross_clock_or_fill_before_ack"


def test_filled_unfilled_comparison_uses_same_decision_horizons_not_fill_times() -> None:
    filled = snapshot()
    unfilled = snapshot(
        identity="u", entry_fills=[], exit_fills=[], state="no_fill", entry_status="canceled"
    )
    for name in ("cycles", "orders", "activities", "audits", "broker_historical_orders"):
        filled[name].extend(unfilled[name])
    for cycle in filled["cycles"]:
        if cycle["cycle_id"] == "u":
            cycle["payload"]["signal"]["observed_at"] = stamp(20000)
            original = cycle["payload"]["signal"]["quote"]
            original["exchange_at"] = stamp(20000)
            original["exchange_time_ns"] = BASE + 20_000_000_000
            original["received_at"] = stamp(20000)
    filled["quotes"] = [
        quote(0, "101"),
        quote(20000, "101"),
        *[quote(h, "100") for h in HORIZONS_MS],
        *[quote(20000 + h, "102") for h in HORIZONS_MS],
    ]
    result = reconstruct_period(filled)
    groups = result["filled_vs_unfilled"]
    assert groups["filled"]["signals"] == 1
    assert groups["unfilled"]["signals"] == 1
    for name in HORIZON_NAMES:
        assert D(groups["filled"]["returns_bps"][name]["mean"]) < 0
        assert D(groups["unfilled"]["returns_bps"][name]["mean"]) > 0
        assert groups["contrasts"][name]["negative_filled_positive_unfilled"]
    assert not groups["unfilled_returns_are_realizable_profit"]
    assert not groups["causal_claim"]


def test_documented_seven_minute_outlier_separates_cooldown_union_from_residual_delay() -> None:
    data = snapshot(exit_fills=[(421000, "1", "100.1")])
    data["cycles"][0]["exit_due_at"] = stamp(16000)
    for audit in data["audits"]:
        if audit["event_type"] == "scalp_exit_requested":
            audit["occurred_at"] = stamp(16896)
    for index, start in enumerate((17000, 97000, 177000, 257000, 337000)):
        data["audits"].append(
            {
                "event_id": f"backoff-{index}",
                "event_type": "scalp_broker_backoff",
                "occurred_at": stamp(start),
                "payload": {
                    "run_id": "run",
                    "operation": "reconcile",
                    "status_code": 429,
                    "retry_after": stamp(start + 60000),
                },
            }
        )
    overlapping = copy.deepcopy(data["audits"][-1])
    overlapping["event_id"] = "overlapping-directive"
    data["audits"].append(overlapping)
    other_run = copy.deepcopy(overlapping)
    other_run["payload"]["run_id"] = "other-run"
    data["audits"].append(other_run)
    result = reconstruct_period(data)
    row = result["trades"][0]
    assert value(row["timing"]["holding_seconds"]) == 420
    analysis = row["backoff_analysis"]
    assert value(analysis["backoff_directive_union_seconds"]) == 300
    assert value(analysis["backoff_after_deadline_seconds"]) == 300
    assert value(analysis["deadline_to_exit_fill_ms"]) == 405000
    assert value(analysis["deadline_to_exit_request_ms"]) == 896
    assert value(row["timing"]["stages_ms"]["exit_decision_dispatch_start"]) == 403804
    assert value(analysis["delay_outside_documented_backoffs_seconds"]) == 105
    assert len(analysis["backoff_events"]) == 6
    assert "LATENCY_FAILURE" in row["failure_attribution"]["labels"]
    condition = row["failure_attribution"]["assessments"]["LATENCY_FAILURE"]["evidence"]
    assert condition["operational_deadline_failure"]
    assert condition["entry_economic_latency_condition"] is None
    assert result["longest_holding_trade"]["cycle_id"] == "c"


def test_cost_failure_requires_positive_raw_price_pnl_and_negative_explicit_or_model_net() -> None:
    data = snapshot(exit_fills=[(5000, "1", "100.1")])
    row = trade(data)
    assert value(row["pnl"]["raw_price_pnl"]) == D(".1")
    assert value(row["pnl"]["modeled_net_pnl"]) < 0
    assert "COST_FAILURE" in row["failure_attribution"]["labels"]
    evidence = row["failure_attribution"]["assessments"]["COST_FAILURE"]["evidence"]
    assert evidence["net_basis"] == "modeled_net_pnl"
    losing_price = trade(snapshot(exit_fills=[(5000, "1", "99")]))
    assert "COST_FAILURE" not in losing_price["failure_attribution"]["labels"]


def test_adverse_selection_is_a_markout_evidence_condition_not_a_zero_win_inference() -> None:
    data = snapshot(exit_fills=[(5000, "1", "99")])
    assert "ADVERSE_SELECTION" not in trade(data)["failure_attribution"]["labels"]
    data["quotes"] = [quote(2000, "99"), quote(3000, "98")]
    row = trade(data)
    assert "ADVERSE_SELECTION" in row["failure_attribution"]["labels"]
    assert not row["failure_attribution"]["causal_claim"]
    assert row["failure_attribution"]["assessments"]["ALPHA_FAILURE"]["status"] == "unknown"


def test_alpha_requires_recorded_validated_prediction_and_complete_horizon_curve() -> None:
    data = snapshot()
    data["cycles"][0]["payload"]["signal"]["prediction"] = {
        "predicted_move_bps": "10",
        "predicted_horizon_seconds": "1",
        "validated": True,
    }
    data["quotes"] = [quote(at, "101") for at in range(0, 1001, 250)]
    row = trade(data)
    assert "ALPHA_FAILURE" in row["failure_attribution"]["labels"]
    data["quotes"] = [quote(0, "101"), quote(1000, "101")]
    assert trade(data)["failure_attribution"]["assessments"]["ALPHA_FAILURE"]["status"] == "unknown"
    data["quotes"] = [quote(at, "101") for at in range(0, 1001, 250)]
    data["cycles"][0]["payload"]["signal"]["prediction"]["validated"] = False
    assert "ALPHA_FAILURE" not in trade(data)["failure_attribution"]["labels"]


def test_exit_failure_requires_net_costed_and_sufficient_size_favorable_excursion() -> None:
    data = snapshot(exit_fills=[(5000, "1", "99.9")])
    data["quotes"] = [quote(2000, "101")]
    assert trade(data)["failure_attribution"]["assessments"]["EXIT_FAILURE"]["status"] == "unknown"
    add_fee(data, "entry")
    add_fee(data, "exit")
    row = trade(data)
    assert value(row["executable_excursion"]["net_profitable_mfe"]) == D(".737525")
    assert "EXIT_FAILURE" in row["failure_attribution"]["labels"]
    data["quotes"] = [quote(2000, "101", size=".1")]
    assert "EXIT_FAILURE" not in trade(data)["failure_attribution"]["labels"]


def test_strategy_family_is_not_a_regime_and_recorded_invalidated_regime_is_required() -> None:
    data = snapshot()
    row = trade(data)
    assert row["regime"] == "UNKNOWN"
    assert row["failure_attribution"]["assessments"]["REGIME_FAILURE"]["status"] == "unknown"
    data["cycles"][0]["payload"]["signal"]["observed_regime"] = "reversion"
    data["cycles"][0]["payload"]["signal"]["observed_regime_validated"] = True
    assert "REGIME_FAILURE" in trade(data)["failure_attribution"]["labels"]


def test_stale_decision_evidence_uses_frozen_policy_not_the_stricter_markout_age() -> None:
    data = snapshot()
    original = data["cycles"][0]["payload"]["signal"]["quote"]
    original["exchange_at"] = stamp(-6000)
    original["exchange_time_ns"] = BASE - 6_000_000_000
    original["received_at"] = stamp(-5900)
    assert "STALE_DATA_FAILURE" in trade(data)["failure_attribution"]["labels"]
    original["exchange_at"] = stamp(-300)
    original["exchange_time_ns"] = BASE - 300_000_000
    original["received_at"] = stamp(-200)
    row = trade(data)
    assert "STALE_DATA_FAILURE" not in row["failure_attribution"]["labels"]
    assert row["decision"]["comparison_baseline"] is None
    assert row["decision"]["future_returns"]["100ms"]["signed_return_bps"]["value"] is None


def test_every_label_and_required_unavailable_field_has_an_explanation() -> None:
    row = trade(snapshot())
    attribution = row["failure_attribution"]
    assert tuple(attribution["assessments"]) == FAILURE_LABELS
    assert "UNKNOWN" in attribution["labels"]
    for assessment in attribution["assessments"].values():
        assert assessment["reason"] and assessment["threshold"]
        assert "evidence" in assessment
    for field in ("spread_cost", "estimated_slippage", "estimated_market_impact"):
        assert row["pnl"][field]["value"] is None
        assert row["pnl"][field]["reason"]


def test_output_groups_cover_all_requested_dimensions_and_track_missing_counts() -> None:
    result = reconstruct_period(snapshot())
    for dimension in ("outcome", "family", "entry_style", "symbol", "regime", "time_of_day_et"):
        assert result["distributions"][dimension]
    assert result["distributions"]["regime"]["UNKNOWN"]["trades"] == 1
    for horizon in HORIZON_NAMES:
        counts = result["distributions"]["all"]["markout_bps"][horizon]
        assert counts["samples"] == 0
        assert counts["missing"] == 1
    assert result["trades"][0]["time_of_day_et"]["local_time"] == "2026-09-11T08:00:00-04:00"


@pytest.mark.parametrize(
    "observed,zone", [("2026-11-01T05:30:00+00:00", "EDT"), ("2026-11-01T06:30:00+00:00", "EST")]
)
def test_eastern_time_buckets_are_portable_across_dst_fallback(observed: str, zone: str) -> None:
    data = snapshot(entry_fills=[], exit_fills=[], state="no_fill", entry_status="canceled")
    data["cycles"][0]["payload"]["signal"]["observed_at"] = observed
    et = reconstruct_period(data)["signals"][0]["time_of_day_et"]
    assert et["zone"] == zone
    assert et["hour_bucket"] == "01:00-02:00 ET"


def test_resource_bounds_fail_explicitly_and_long_curves_are_not_silently_cropped() -> None:
    data = snapshot()
    with pytest.raises(ValueError, match="orders has 2 rows"):
        reconstruct_period(data, policy=DiagnosticPolicy(max_orders=1))
    data["quotes"] = [quote(1000), quote(5000)]
    result = reconstruct_period(data, policy=DiagnosticPolicy(max_curve_seconds=3))
    row = result["trades"][0]
    assert value(row["timing"]["holding_seconds"]) == 4
    assert row["excursions"]["mfe_bps"]["value"] is None
    assert (
        row["excursions"]["complete_curve_reason"]
        == "holding_window_exceeds_bound_no_truncated_curve_claim"
    )


def test_malformed_tables_job_envelope_and_mixed_accounts_fail_explicitly() -> None:
    with pytest.raises(ValueError, match="raw snapshot"):
        reconstruct_period({"results": [snapshot()]})
    with pytest.raises(ValueError, match="non-mapping"):
        reconstruct_period({"cycles": [1]})
    with pytest.raises(ValueError, match="sequence"):
        reconstruct_period({"quotes": "not-a-table"})
    data = snapshot()
    data["cycles"][0]["account_digest"] = "other-account"
    with pytest.raises(ValueError, match="account_digest"):
        reconstruct_period(data)


def test_library_does_not_mutate_inputs_or_global_decimal_context_and_is_deterministic() -> None:
    data = snapshot()
    original = copy.deepcopy(data)
    precision = getcontext().prec
    first = reconstruct_period(data)
    second = reconstruct_period(data)
    assert data == original
    assert getcontext().prec == precision
    assert first == second
    encoded = json.dumps(first, allow_nan=False, sort_keys=True)
    assert json.loads(encoded)["schema"] == "scalp-diagnostics-v1"


def test_superseded_out_of_order_quotes_cannot_create_a_favorable_excursion() -> None:
    data = snapshot()
    data["quotes"] = [
        quote(1100, "101", delay=0),
        quote(1090, "10000", delay=30),
        quote(1200, "102", delay=0),
    ]
    row = trade(data)
    assert value(row["excursions"]["mfe_signed_price"]) == 2
    assert row["excursions"]["sample_count"] == 2


def test_curve_rejects_quotes_not_fresh_at_receipt_under_a_tighter_policy() -> None:
    data = snapshot()
    data["quotes"] = [quote(1100, "10000", delay=150)]
    result = reconstruct_period(data, policy=DiagnosticPolicy(max_quote_age_ms=100))
    assert result["trades"][0]["excursions"]["mfe_bps"]["value"] is None


@pytest.mark.parametrize("size", ["-1", "NaN"])
def test_invalid_displayed_sizes_are_explicitly_rejected(size: str) -> None:
    data = snapshot()
    data["quotes"] = [quote(1100, size=size)]
    result = reconstruct_period(data)
    assert result["coverage"]["quote_rejections"]["invalid_quote_size"] == 1
    assert result["trades"][0]["markouts"]["100ms"]["signed_price"]["value"] is None


def test_date_precision_fill_still_crosschecks_amounts_but_cannot_claim_first_fill_time() -> None:
    data = snapshot()
    data["activities"][0]["time_precision"] = "date"
    row = trade(data)
    assert row["reconciliation"]["all_filled_orders_independently_crosschecked"]
    assert row["timing"]["timestamps"]["fill_time"]["value"] is None
    assert row["excursions"]["mfe_bps"]["value"] is None


def test_proven_order_mismatch_is_not_hidden_by_another_missing_activity() -> None:
    data = snapshot()
    data["orders"][0]["broker"]["filled_average_price"] = "99"
    data["activities"].pop()
    row = trade(data)
    assert not row["reconciliation"]["all_filled_orders_independently_crosschecked"]
    assert row["reconciliation"]["accounting_failure_proven"]
    assert "ACCOUNTING_FAILURE" in row["failure_attribution"]["labels"]


def test_missing_client_identity_prevents_partial_records_being_called_realized() -> None:
    data = snapshot()
    missing = copy.deepcopy(data["orders"][0])
    missing.pop("client_order_id")
    data["orders"].append(missing)
    result = reconstruct_period(data)
    assert result["summary"]["completed_trades"] == 0
    assert result["open_or_unresolved"][0]["pnl"]["modeled_net_pnl"]["value"] is None
    assert "missing_client_order_id" in result["open_or_unresolved"][0]["source_integrity_issues"]


def test_missing_position_direction_is_not_defaulted_to_long() -> None:
    data = snapshot()
    data["cycles"][0]["payload"]["signal"].pop("action")
    result = reconstruct_period(data)
    assert result["summary"]["completed_trades"] == 0
    row = result["open_or_unresolved"][0]
    assert row["side"] is None and row["side_reason"] == "position_direction_not_recorded"
    assert row["pnl"]["cash_flow_to_date_not_realized"]["value"] is None
    assert len(row["unpaired_orders"]) == 2
    assert row["decision"]["future_returns"]["100ms"]["signed_return_bps"]["value"] is None


def test_explicit_broker_opening_intent_can_establish_direction_without_signal_action() -> None:
    data = snapshot()
    data["cycles"][0]["payload"]["signal"].pop("action")
    data["orders"][0]["broker"]["position_intent"] = "buy_to_open"
    assert trade(data)["side"] == "long"


def test_cross_account_activity_cannot_validate_owned_fills() -> None:
    data = snapshot()
    data["activities"][0]["account_digest"] = "other-account"
    with pytest.raises(ValueError, match="account_digest"):
        reconstruct_period(data)


@pytest.mark.parametrize("tolerance", [D("NaN"), D("Infinity"), D("-1")])
def test_invalid_diagnostic_tolerances_are_rejected(tolerance: Decimal) -> None:
    with pytest.raises(ValueError, match="diagnostic policy"):
        DiagnosticPolicy(money_tolerance=tolerance)


def test_oversized_clock_value_is_missing_telemetry_not_datetime_overflow() -> None:
    data = snapshot()
    data["cycles"][0]["payload"]["signal"]["timing"] = {"t4_model_ns": 10**50}
    data["quotes"] = [quote(1100)]
    data["quotes"][0]["exchange_at_ns"] = 10**50
    result = reconstruct_period(data)
    assert result["coverage"]["quote_rejections"]["missing_or_invalid_quote_timestamp"] == 1
    assert result["trades"][0]["timing"]["timestamps"]["send_time"]["value"] is None


def test_exit_opportunity_uses_counterfactual_fee_floor_not_cheaper_actual_exit_fee() -> None:
    data = snapshot(exit_fills=[(5000, "1", "99.9")])
    add_fee(data, "entry")
    add_fee(data, "exit", cash=".24975")
    data["quotes"] = [quote(2000, "100.26")]
    row = trade(data)
    assert value(row["executable_excursion"]["net_profitable_mfe"]) == D("-.000625")
    assert "EXIT_FAILURE" not in row["failure_attribution"]["labels"]
    assert value(row["pnl"]["actual_net_pnl"]) == D("-.34975")


def test_current_broker_flatness_is_a_later_observation_not_fee_proof() -> None:
    data = snapshot()
    data["current_broker"] = {"positions": [], "open_orders": [], "observed_at": stamp(20000)}
    result = reconstruct_period(data)
    assert result["current_broker_read_only"]["flat_at_observation"]["value"] is True
    assert result["summary"]["fee_complete_trades"] == 0
    assert result["run_cohorts"]["run"]["trades"] == 1
    assert result["summary"]["fee_and_inventory_effects"]["estimated_cash_reserve"]["samples"] == 1


def test_reconstruction_performs_no_filesystem_io(monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("diagnostic core attempted filesystem I/O")

    data = snapshot()
    monkeypatch.setattr("builtins.open", forbidden)
    result = reconstruct_period(data)
    assert result["summary"]["completed_trades"] == 1


def test_report_keeps_window_and_batch_hash_provenance_without_copying_whole_tape_metadata() -> (
    None
):
    data = snapshot()
    data["tape_coverage"] = {
        "basis": "native quote events",
        "quote_count": 0,
        "windows": [[stamp(0), stamp(6000)]],
        "window_total_seconds": 6,
        "batches": [{"batch_id": "abc", "event_count": 1}],
        "invalid_batches": [],
    }
    tape = reconstruct_period(data)["coverage"]["tape_source_coverage"]
    assert tape["batch_count"]["value"] == 1
    assert len(tape["batch_metadata_sha256"]["value"]) == 64
    assert tape["windows"]["value"] == [[stamp(0), stamp(6000)]]
    assert "batches" not in tape


def calibration_snapshot(**kwargs: Any) -> dict[str, Any]:
    data = snapshot(**kwargs)
    data["account_digest"] = "a" * 64
    data["observed_at"] = stamp(20000)
    cycle = data["cycles"][0]
    cycle["account_digest"] = data["account_digest"]
    cycle["payload"]["config"]["order_notional_usd"] = "100"
    signal = cycle["payload"]["signal"]
    signal["features"]["as_of_ns"] = BASE
    signal["features"]["source_event_id"] = "do-not-learn-an-event-id"
    signal["features"]["estimated_round_trip_cost_bps"] = 40
    signal["timing"] = {"t4_model_ns": BASE}
    for activity in data["activities"]:
        activity["account_digest"] = data["account_digest"]
        activity["received_at"] = (
            datetime.fromisoformat(activity["occurred_at"]) + timedelta(milliseconds=20)
        ).isoformat()
    for order in data["orders"]:
        if order["side"] == ("sell" if kwargs.get("short") else "buy"):
            if not D(order["filled_quantity"]):
                order["quantity"] = order["broker"]["quantity"] = "1"
                order["intent"]["request"]["quantity"] = "1"
                for history in data["broker_historical_orders"]:
                    if history["id"] == order["broker_order_id"]:
                        history["qty"] = "1"
            data["audits"].append(
                {
                    "event_type": "scalp_dispatch_timing",
                    "payload": {
                        "client_order_id": order["client_order_id"],
                        "timing": {"t6_submitted_ns": BASE + 200_000_000},
                    },
                }
            )
        completed_at = order["broker"]["filled_at"] or stamp(3000)
        received_at = (
            datetime.fromisoformat(completed_at) + timedelta(milliseconds=10)
        ).isoformat()
        data["audits"].append(
            {
                "event_id": "terminal-" + order["client_order_id"],
                "event_type": "scalp_broker_order_observation",
                "occurred_at": received_at,
                "payload": {
                    "client_order_id": order["client_order_id"],
                    "source_received_at": received_at,
                    "broker": copy.deepcopy(order["broker"]),
                },
            }
        )
    data["quotes"] = [quote(0, "101"), quote(1000, "100.1"), quote(5000, "100.2")]
    return data


def calibration_row(report: dict[str, Any], horizon: int, cycle_id: str = "c") -> dict[str, Any]:
    return next(
        row
        for row in report["calibration_samples"]
        if row["horizon_seconds"] == horizon and row["cycle_id"] == cycle_id
    )


def test_calibration_preserves_each_filled_and_unfilled_candidate_at_both_horizons() -> None:
    data = calibration_snapshot()
    unfilled = calibration_snapshot(
        identity="u", entry_fills=[], exit_fills=[], state="no_fill", entry_status="canceled"
    )
    unfilled["cycles"][0]["payload"]["signal"]["family"] = "reversion"
    for name in ("cycles", "orders", "activities", "audits", "broker_historical_orders"):
        data[name].extend(unfilled[name])
    report = reconstruct_period(data)
    assert len(report["calibration_samples"]) == 4
    for horizon in (1, 5):
        coverage = report["calibration_coverage"]["by_horizon_seconds"][str(horizon)]
        assert coverage["rows"] == 2
        assert coverage["filled"] == coverage["unfilled"] == 1
        row = calibration_row(report, horizon, "u")
        assert row["sample"]["filled"] is False
        assert row["sample"]["filled_fraction"] == 0
        assert row["sample"]["gross_return_bps"] is None
        assert row["sample"]["fill_delay_seconds"] is None
        assert row["sample"]["return_end_at"] is None
        assert row["sample"]["directional_return_bps"] is not None
        assert not row["evidence"]["directional_return_is_realizable_profit"]
    assert report["calibration_coverage"]["unique_candidates"] == 2
    assert {row["sample"]["action"] for row in report["calibration_samples"]} == {"PASSIVE_BUY"}
    assert {row["sample"]["source_sha256"] for row in report["calibration_samples"]} == {
        report["provenance"]["canonical_snapshot_sha256"]
    }


def test_calibration_legacy_dispatch_proxy_is_not_fabricated_as_exact_decision_latency() -> None:
    data = calibration_snapshot()
    data["cycles"][0]["payload"]["signal"].pop("timing")
    data["audits"] = [
        audit for audit in data["audits"] if audit["event_type"] != "scalp_dispatch_timing"
    ]
    report = reconstruct_period(data)
    for row in report["calibration_samples"]:
        assert row["sample"]["decision_latency_seconds"] is None
        assert row["evidence"]["decision_to_dispatch_start_proxy_seconds"] == 0.1
        assert "proxy_is_not_substituted" in row["missing_reasons"]["decision_latency_seconds"]
        assert not row["training_eligible"]


def test_calibration_cost_floor_is_fifty_bps_not_legacy_forty_or_maker_assumption() -> None:
    data = calibration_snapshot(exit_fills=[(3000, "1", "100")])
    data["cycles"][0]["payload"]["signal"]["estimated_round_trip_cost_bps"] = 40
    row = calibration_row(reconstruct_period(data), 5)
    costs = row["sample"]["costs"]
    assert costs["entry_fee"]["bps"] == 25
    assert costs["exit_fee"]["bps"] == 25
    assert (
        costs["entry_fee"]["provenance"] == "entry_liquidity_unconfirmed_use_worst_configured_fee"
    )
    assert costs["entry_fee"]["kind"] == costs["exit_fee"]["kind"] == "estimated"
    assert not costs["entry_fee"]["included_in_return"]
    assert not costs["exit_fee"]["included_in_return"]
    assert row["sample"]["return_basis"] == "fill_to_fill"
    for name in ("spread", "slippage", "impact", "adverse_selection"):
        assert costs[name]["included_in_return"]
        assert costs[name]["kind"] == "embedded"
        assert costs[name]["bps"] is None
    assert row["evidence"]["recorded_diagnostic_round_trip_cost_bps_not_used"] == 40
    assert "estimated_round_trip_cost_bps" not in row["sample"]["features"]
    assert not row["evidence"]["actual_maker_liquidity_proven"]


def test_calibration_raw_price_return_does_not_charge_withheld_base_principal_twice() -> None:
    data = calibration_snapshot(exit_fills=[(3000, ".9985", "101")])
    report = reconstruct_period(data)
    row = calibration_row(report, 5)
    sample = row["sample"]
    assert D(row["evidence"]["gross_return_exact_bps"]) == D("99.85")
    assert sample["filled_fraction"] == 1
    assert sample["costs"]["entry_fee"]["bps"] == 25
    assert D(str(sample["costs"]["exit_fee"]["bps"])) == D("25.212125")
    model_net_bps = (
        D(str(sample["gross_return_bps"]))
        - D(str(sample["costs"]["entry_fee"]["bps"]))
        - D(str(sample["costs"]["exit_fee"]["bps"]))
    )
    assert model_net_bps == D("49.637875")
    assert value(report["trades"][0]["pnl"]["gross_cash_flow"]) == D(".8485")
    assert sample["costs"]["residual"] is None
    assert sample["costs"]["residual_covers"] == []


def test_calibration_late_fill_remains_filled_and_cannot_become_a_one_second_nonfill() -> None:
    data = calibration_snapshot(entry_fills=[(2000, "1", "100")], exit_fills=[(4100, "1", "101")])
    report = reconstruct_period(data)
    early = calibration_row(report, 1)
    assert early["sample"]["filled"] is True
    assert early["sample"]["filled_fraction"] == 1
    assert early["sample"]["fill_delay_seconds"] == 2
    assert early["sample"]["gross_return_bps"] is None
    assert early["missing_reasons"]["gross_return_bps"] == "first_fill_after_label_horizon"
    assert D(early["evidence"]["entry_quantity_within_horizon"]) == 0
    assert not early["training_eligible"]
    later = calibration_row(report, 5)
    assert later["sample"]["gross_return_bps"] == 100
    assert later["evidence"]["return_end_at_ns"] == BASE + 4_100_000_000


def test_calibration_partial_fill_tail_cannot_leak_final_vwap_into_earlier_horizon() -> None:
    data = calibration_snapshot(
        entry_fills=[(500, ".4", "100"), (1500, ".6", "200")],
        exit_fills=[(3000, "1", "160")],
    )
    row = calibration_row(reconstruct_period(data), 1)
    assert row["sample"]["filled"] is True
    assert row["sample"]["gross_return_bps"] is None
    assert row["missing_reasons"]["gross_return_bps"] == "entry_fill_tail_after_label_horizon"
    assert D(row["evidence"]["entry_quantity_within_horizon"]) == D(".4")
    assert D(row["evidence"]["entry_value_within_horizon"]) == 40
    assert D(row["evidence"]["entry_value"]) == 160
    assert row["evidence"]["entry_fill_tail_after_horizon"]


def test_calibration_early_exit_uses_observed_exit_not_later_post_exit_midpoint() -> None:
    data = calibration_snapshot(entry_fills=[(500, "1", "100")], exit_fills=[(2000, "1", "101")])
    data["quotes"] = [quote(0, "101"), quote(5000, "10000")]
    row = calibration_row(reconstruct_period(data), 5)
    assert row["sample"]["return_basis"] == "fill_to_fill"
    assert row["sample"]["gross_return_bps"] == 100
    assert row["evidence"]["return_end_at_ns"] == BASE + 2_000_000_000
    assert row["evidence"]["horizon_quote"]["mid"] == "10000.00"


def test_calibration_horizon_inside_position_uses_endpoint_not_later_roundtrip_or_mfe() -> None:
    data = calibration_snapshot(entry_fills=[(500, "1", "100")], exit_fills=[(8000, "1", "110")])
    data["quotes"] = [quote(0, "101"), quote(2000, "1000"), quote(5000, "98")]
    row = calibration_row(reconstruct_period(data), 5)
    assert row["sample"]["return_basis"] == "fill_to_mid"
    assert row["sample"]["gross_return_bps"] == -200
    assert row["evidence"]["return_end_at_ns"] == BASE + 5_000_000_000
    assert not row["evidence"]["return_is_realized_raw_price_pnl"]
    for name in ("spread", "slippage", "impact"):
        assert row["sample"]["costs"][name]["bps"] is None
        assert row["sample"]["costs"][name]["kind"] == "unknown"
        assert not row["sample"]["costs"][name]["included_in_return"]
    assert set(row["unpriced_cost_components"]) == {"spread", "slippage", "impact"}
    assert row["sample"]["costs"]["adverse_selection"]["included_in_return"]


def test_calibration_does_not_use_a_future_or_late_received_horizon_quote() -> None:
    data = calibration_snapshot(entry_fills=[(500, "1", "100")], exit_fills=[(8000, "1", "110")])
    data["quotes"] = [quote(0, "101"), quote(4990, "999", delay=30), quote(5001, "10000")]
    row = calibration_row(reconstruct_period(data), 5)
    assert row["evidence"]["horizon_quote"] is None
    assert row["sample"]["gross_return_bps"] is None
    assert row["missing_reasons"]["gross_return_bps"] == "no_causal_fresh_horizon_quote"


def test_calibration_maturity_covers_full_ttl_and_original_data_receipts() -> None:
    data = calibration_snapshot()
    report = reconstruct_period(data)
    one = calibration_row(report, 1)
    five = calibration_row(report, 5)
    assert one["evidence"]["label_matured_at_ns"] == BASE + 3_000_000_000
    assert five["evidence"]["label_matured_at_ns"] == BASE + 5_020_000_000
    assert one["sample"]["horizon_seconds"] == 1
    assert one["sample"]["cancellation_horizon_seconds"] == 3
    assert any(
        reference["kind"] == "FILL_activity_receipt"
        for reference in five["evidence"]["maturity_evidence"]
    )


def test_calibration_does_not_export_payoff_before_required_fill_evidence_is_received() -> None:
    data = calibration_snapshot(entry_fills=[(500, "1", "100")], exit_fills=[(2000, "1", "101")])
    data["observed_at"] = stamp(6000)
    data["activities"][0]["received_at"] = stamp(9000)
    row = calibration_row(reconstruct_period(data), 5)
    assert row["evidence"]["label_matured_at_ns"] == BASE + 9_000_000_000
    assert row["sample"]["gross_return_bps"] is None
    assert "label_not_mature_at_snapshot_cutoff" in row["eligibility_reasons"]
    assert not row["training_eligible"]


def test_calibration_drops_no_candidates_when_send_or_quote_telemetry_is_missing() -> None:
    data = calibration_snapshot(
        entry_fills=[], exit_fills=[], state="no_fill", entry_status="canceled"
    )
    data["orders"] = []
    data["broker_historical_orders"] = []
    data["audits"] = []
    data["quotes"] = []
    report = reconstruct_period(data)
    assert len(report["calibration_samples"]) == 2
    for row in report["calibration_samples"]:
        assert row["sample"]["filled"] is False
        assert row["sample"]["filled_fraction"] == 0
        assert row["evidence"]["venue_exposure"] == "no_order"
        assert row["sample"]["notional_usd"] == 100
        assert row["sample"]["decision_latency_seconds"] is None
        assert row["sample"]["directional_return_bps"] is None
        assert row["missing_reasons"]
        assert not row["training_eligible"]


def test_calibration_features_exclude_ids_timing_outcomes_and_future_values() -> None:
    data = calibration_snapshot()
    report = reconstruct_period(data)
    for row in report["calibration_samples"]:
        features = row["sample"]["features"]
        assert "source_event_id" not in features and "as_of_ns" not in features
        assert "estimated_round_trip_cost_bps" not in features and "filled" not in features
        assert "decision_latency_seconds" not in features
        assert features["normalized_ofi_5s"] == 0.2
    data["cycles"][0]["payload"]["signal"]["features"]["as_of_ns"] = BASE + 1
    row = calibration_row(reconstruct_period(data), 5)
    assert not row["evidence"]["feature_snapshot_causal"]
    assert all(value is None for value in row["sample"]["features"].values())
    assert "feature_timestamp_after_decision" in row["eligibility_reasons"]


def test_calibration_complete_raw_rows_match_agreed_strict_economic_sample_schema() -> None:
    from tradeagent.scalping_economics import EconomicSample

    report = reconstruct_period(calibration_snapshot())
    for row in report["calibration_samples"]:
        assert row["training_eligible"], row["eligibility_reasons"]
        sample = EconomicSample.model_validate_json(json.dumps(row["sample"], allow_nan=False))
        assert sample.sample_id == row["sample"]["sample_id"]
        assert sample.filled_fraction == 1
        assert sample.decision_latency_seconds == 0.2
        assert sample.source_sha256 == report["provenance"]["canonical_snapshot_sha256"]


def test_calibration_partial_exit_horizon_is_not_treated_as_full_position_still_open() -> None:
    data = calibration_snapshot(
        entry_fills=[(500, "1", "100")],
        exit_fills=[(4000, ".4", "101"), (6000, ".6", "102")],
    )
    row = calibration_row(reconstruct_period(data), 5)
    assert row["sample"]["gross_return_bps"] is None
    assert row["missing_reasons"]["gross_return_bps"].startswith("partial_exit_spans_label_horizon")
    assert not row["training_eligible"]


def test_calibration_late_cancellation_window_fill_is_retained_but_not_supporting() -> None:
    data = calibration_snapshot(entry_fills=[(3500, "1", "100")], exit_fills=[(4500, "1", "101")])
    row = calibration_row(reconstruct_period(data), 5)
    assert row["sample"]["filled"] is True
    assert row["sample"]["gross_return_bps"] == 100
    assert "fill_after_declared_cancellation_horizon" in row["eligibility_reasons"]
    assert not row["training_eligible"]


def test_calibration_duplicate_decisions_are_retained_but_not_independent_support() -> None:
    data = calibration_snapshot()
    duplicate = calibration_snapshot(identity="duplicate")
    for name in ("cycles", "orders", "activities", "audits", "broker_historical_orders"):
        data[name].extend(duplicate[name])
    report = reconstruct_period(data)
    assert len(report["calibration_samples"]) == 4
    for row in report["calibration_samples"]:
        assert not row["training_eligible"]
        assert "duplicate_decision_action_observation" in row["eligibility_reasons"]


def test_calibration_future_horizon_and_receipt_frontier_never_become_payoff_labels() -> None:
    data = calibration_snapshot(entry_fills=[(500, "1", "100")], exit_fills=[(8000, "1", "110")])
    data["observed_at"] = stamp(4000)
    data["quotes"] = [quote(0, "101"), quote(5000, "10000")]
    row = calibration_row(reconstruct_period(data), 5)
    assert row["sample"]["gross_return_bps"] is None
    assert row["sample"]["directional_return_bps"] is None
    assert row["evidence"]["horizon_quote"] is None
    assert "horizon_not_observed_at_snapshot" in row["eligibility_reasons"]


def test_positive_fill_activity_cannot_be_exported_as_a_zero_fill_training_outcome() -> None:
    data = calibration_snapshot()
    data["cycles"][0]["state"] = "no_fill"
    data["orders"][0]["broker"]["filled_quantity"] = "0"
    data["orders"][0]["filled_quantity"] = "0"
    data["broker_historical_orders"][0]["filled_qty"] = "0"
    report = reconstruct_period(data)
    assert report["summary"]["no_fill_cycles"] == 0
    for row in report["calibration_samples"]:
        assert row["sample"]["filled"] is None
        assert row["sample"]["filled_fraction"] is None
        assert not row["training_eligible"]
    assert (
        report["data_quality"]["issue_counts"][
            "positive_activity_conflicts_with_zero_cumulative_fill"
        ]
        == 1
    )


def test_tape_metadata_is_also_bounded() -> None:
    data = snapshot()
    data["tape_coverage"] = {"batches": [{}, {}]}
    with pytest.raises(ValueError, match="metadata row bound"):
        reconstruct_period(data, policy=DiagnosticPolicy(max_quotes=1))


def test_public_timeline_uses_same_seven_horizon_math_and_quote_provenance() -> None:
    data = snapshot()
    data["quotes"] = [
        quote(1000 + horizon, str(D(100) + D(horizon) / 10000)) for horizon in HORIZONS_MS
    ]
    expected = trade(data)
    timeline = QuoteTimeline(data["quotes"], observed_through_ns=BASE + 20_000_000_000)
    actual = timeline.markouts(
        "BTCUSD",
        fill_at_ns=BASE + 1_000_000_000,
        fill_price="100",
        quantity="1",
        side="buy",
        fill_id="c-entry-fill-0",
        exit_at_ns=BASE + 5_000_000_000,
    )
    assert tuple(actual) == HORIZON_NAMES
    for name in HORIZON_NAMES:
        fact = actual[name]
        assert fact["schema"] == "scalp-fill-markout-v1"
        assert fact["status"] == "observed" and fact["reason"] is None
        assert fact["signed_price"] == expected["markouts"][name]["signed_price"]
        assert fact["signed_bps"] == expected["markouts"][name]["signed_bps"]
        assert (
            fact["observations"][0]["sample"]
            == expected["markouts"][name]["observations"][0]["sample"]
        )
        assert fact["fill_id"] == "c-entry-fill-0"
    json.dumps(actual, allow_nan=False)


def test_public_sampler_excludes_future_and_late_received_quotes() -> None:
    timeline = QuoteTimeline(
        [quote(1050, "99"), quote(1090, "999", delay=30), quote(1101, "10000")],
        observed_through_ns=BASE + 2_000_000_000,
    )
    fact = timeline.sample("BTC/USD", BASE + 1_100_000_000, horizon_ms=100)
    assert fact["schema"] == "scalp-quote-sample-v1"
    assert fact["status"] == "observed"
    assert fact["sample"]["mid"] == "99.00"
    assert fact["sample"]["exchange_at_ns"] == BASE + 1_050_000_000
    assert fact["max_age_ms"] == 100


def test_public_live_watermark_prevents_premature_labels_even_with_cached_future_quotes() -> None:
    timeline = QuoteTimeline(
        [quote(1000, "100"), quote(1100, "999"), quote(20000, "500")],
        observed_through_ns=BASE + 1_000_000_000,
    )
    sample = timeline.sample("BTC/USD", BASE + 1_100_000_000, horizon_ms=100)
    assert sample["status"] == "pending" and sample["sample"] is None
    facts = timeline.markouts(
        "BTC/USD",
        fill_at_ns=BASE + 1_000_000_000,
        fill_price="100",
        quantity="1",
        side="buy",
        fill_id="original-client:fill-revision-1",
    )
    for fact in facts.values():
        assert fact["status"] == "pending"
        assert fact["reason"] == "target_not_observed_yet"
        assert fact["signed_bps"]["value"] is None
        assert fact["observations"][0]["sample"] is None
        assert fact["observations"][0]["reason"] == "target_not_observed_yet"


def test_implicit_watermark_does_not_extend_past_last_quote_receipt() -> None:
    quotes = [quote(1000, "100")]
    implicit = QuoteTimeline(quotes)
    assert implicit.sample("BTC/USD", BASE + 1_100_000_000, horizon_ms=100)["status"] == "pending"
    explicit = QuoteTimeline(quotes, observed_through_ns=BASE + 1_100_000_000)
    fact = explicit.sample("BTC/USD", BASE + 1_100_000_000, horizon_ms=100)
    assert fact["status"] == "observed"
    assert D(fact["sample"]["age_at_target_ms"]) == 100


def test_public_excursions_reuse_signed_lifetime_bounds_and_gap_reporting() -> None:
    data = snapshot()
    data["quotes"] = [
        quote(999, "10000"),
        quote(1000, "99"),
        quote(2000, "98"),
        quote(4999, "101"),
        quote(5001, "10000"),
    ]
    expected = trade(data)["excursions"]
    timeline = QuoteTimeline(data["quotes"], observed_through_ns=BASE + 6_000_000_000)
    actual = timeline.excursions(
        "BTC/USD",
        opened_at_ns=BASE + 1_000_000_000,
        closed_at_ns=BASE + 5_000_000_000,
        entry_price="100",
        side="long",
    )
    assert actual["status"] == "partial"
    for name in (
        "mfe_signed_price",
        "mae_signed_price",
        "mfe_bps",
        "mae_bps",
        "mfe_quote",
        "mae_quote",
    ):
        assert actual[name] == expected[name]
    assert actual["complete_mfe_bps"]["value"] is None
    assert actual["uncovered_ms"] == expected["uncovered_ms"]


def test_public_excursions_never_clip_future_exit_to_observation_watermark() -> None:
    timeline = QuoteTimeline(
        [quote(1000, "100"), quote(2000, "105"), quote(5000, "200")],
        observed_through_ns=BASE + 3_000_000_000,
    )
    actual = timeline.excursions(
        "BTC/USD",
        opened_at_ns=BASE + 1_000_000_000,
        closed_at_ns=BASE + 5_000_000_000,
        entry_price="100",
        side="buy",
    )
    assert actual["status"] == "pending"
    assert actual["reason"] == "target_not_observed_yet"
    assert actual["end_at"]["unix_ns"] == BASE + 5_000_000_000
    assert actual["mfe_bps"]["value"] is None
    assert actual["sample_count"] == 0


@pytest.mark.parametrize("missing", ["fill_at_ns", "fill_price", "quantity"])
def test_public_markouts_keep_missing_execution_telemetry_unknown(missing: str) -> None:
    timeline = QuoteTimeline([quote(1100)], observed_through_ns=BASE + 20_000_000_000)
    inputs: dict[str, Any] = {
        "fill_at_ns": BASE + 1_000_000_000,
        "fill_price": "100",
        "quantity": "1",
        "side": "buy",
        "fill_id": "original-client:revision",
    }
    inputs[missing] = None
    facts = timeline.markouts("BTC/USD", **inputs)
    for fact in facts.values():
        assert fact["fill_id"] == "original-client:revision"
        assert fact["status"] == "missing"
        assert fact["signed_price"]["value"] is None
        assert fact["signed_price"]["reason"]
        assert fact["signed_bps"]["value"] is None


def test_public_quote_source_is_not_invented_as_native() -> None:
    item = quote(1000)
    item.pop("source")
    timeline = QuoteTimeline([item], observed_through_ns=BASE + 1_000_000_000)
    assert timeline.sample("BTC/USD", BASE + 1_000_000_000)["sample"]["source"] == "provided_quote"


def test_public_helpers_preserve_decimal_context_inputs_and_json_serializability() -> None:
    from decimal import localcontext

    data = [quote(1100, "100.123456789"), quote(5000, "99")]
    original = copy.deepcopy(data)
    timeline = QuoteTimeline(data, observed_through_ns=BASE + 20_000_000_000)
    inputs: dict[str, Any] = {
        "fill_at_ns": BASE + 1_000_000_000,
        "fill_price": "100.000000000001",
        "quantity": ".123456789123456789",
        "side": "sell",
        "fill_id": "revision-2",
    }
    expected = timeline.markouts("BTC/USD", **inputs)
    with localcontext() as context:
        context.prec = 8
        actual = timeline.markouts("BTC/USD", **inputs)
        assert context.prec == 8
    assert actual == expected
    assert data == original
    assert value(actual["100ms"]["signed_price"]) < 0
    json.dumps(actual, allow_nan=False)


def test_public_helpers_reject_malformed_arguments_and_enforce_resource_bounds() -> None:
    with pytest.raises(ValueError, match="quotes has 2 rows"):
        QuoteTimeline([quote(1000), quote(2000)], policy=DiagnosticPolicy(max_quotes=1))
    with pytest.raises(ValueError, match="observed_through_ns"):
        QuoteTimeline([], observed_through_ns=-1)
    timeline = QuoteTimeline([], observed_through_ns=BASE + 20_000_000_000)
    with pytest.raises(ValueError, match="horizon_ms"):
        timeline.sample("BTC/USD", BASE, horizon_ms=-1)
    with pytest.raises(ValueError, match="side"):
        timeline.markouts(
            "BTC/USD",
            fill_at_ns=BASE,
            fill_price="100",
            quantity="1",
            side="UNKNOWN",
            fill_id="original",
        )
    with pytest.raises(ValueError, match="fill_id"):
        timeline.markouts(
            "BTC/USD",
            fill_at_ns=BASE,
            fill_price="100",
            quantity="1",
            side="buy",
            fill_id="",
        )
