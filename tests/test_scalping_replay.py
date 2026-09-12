import json
from decimal import Decimal
from typing import Any
from uuid import UUID

import pytest
from pydantic import ValidationError
from test_scalping_market import BASE_NS, Tape, captured_row, config, handshake, rising

from tradeagent.scalping_market import NS, MarketEvent, capture_events
from tradeagent.scalping_replay import ReplayLatency, replay_events


def filled_tape(*, end: int = 23) -> list[MarketEvent]:
    tape = Tape()
    events = rising(tape)
    events.extend(
        [
            tape.trade(5.10, price="100.05", quantity="5.5", side="S", trade_id="first"),
            tape.trade(5.20, price="100.05", quantity="1", side="S", trade_id="second"),
        ]
    )
    events.extend(rising(tape, 6, end))
    return events


def marketable_depth_tape() -> tuple[Tape, list[MarketEvent]]:
    tape = Tape()
    events = rising(tape)
    events.append(tape.event("o", 5.005, a=[{"p": "200", "s": "5"}]))
    return tape, events


def test_marketable_entry_cannot_sweep_depth_above_the_decision_ask_limit() -> None:
    tape, events = marketable_depth_tape()
    events.append(tape.event("q", 5.1, bp="100.05", bs="5", ap="100.06", **{"as": "0.1"}))
    report = replay_events(events, config(entry_style="marketable"))
    assert [(Decimal(fill["price"]), Decimal(fill["quantity"])) for fill in report["fills"]] == [
        (Decimal("100.06"), Decimal("0.1"))
    ]
    order = report["orders"][0]
    assert Decimal(order["limit_price"]) == Decimal("100.06")
    assert order["status"] == "partially_filled"
    assert order["cancel_requested_at_ns"] is None
    assert Decimal(order["unfilled_quantity"]) == Decimal(order["quantity"]) - Decimal("0.1")


def test_marketable_entry_can_cross_multiple_levels_but_only_within_its_frozen_limit() -> None:
    tape, events = marketable_depth_tape()
    events.append(
        tape.event(
            "o",
            5.015,
            b=[{"p": "100.05", "s": "0"}, {"p": "100.03", "s": "5"}],
            a=[{"p": "100.04", "s": "0.1"}, {"p": "100.05", "s": "0.2"}],
        )
    )
    events.append(tape.event("q", 5.1, bp="100.03", bs="5", ap="100.04", **{"as": "0.1"}))
    report = replay_events(events, config(entry_style="marketable"))
    order = report["orders"][0]
    assert Decimal(order["limit_price"]) == Decimal("100.06")
    assert [(Decimal(fill["price"]), Decimal(fill["quantity"])) for fill in report["fills"]] == [
        (Decimal("100.04"), Decimal("0.1")),
        (Decimal("100.05"), Decimal("0.2")),
        (Decimal("100.06"), Decimal("0.1")),
    ]
    assert all(fill["liquidity"] == "taker" for fill in report["fills"])
    assert order["status"] == "partially_filled" and order["cancel_requested_at_ns"] is None
    assert sum(
        Decimal(fill["price"]) * Decimal(fill["quantity"]) for fill in report["fills"]
    ) <= Decimal(order["quantity"]) * Decimal(order["limit_price"])


@pytest.mark.parametrize(
    ("race_quantity", "expected_status"),
    [("0.2", "canceled"), ("2", "filled")],
)
def test_marketable_remainder_waits_for_ttl_and_counts_maker_fills_during_cancel_race(
    race_quantity: str, expected_status: str
) -> None:
    tape, events = marketable_depth_tape()
    events.append(tape.trade(5.1, price="100.07", quantity="100", side="S", trade_id="above-limit"))
    events.extend(rising(tape, 6, 8))
    events.append(
        tape.trade(8.1, price="100.06", quantity=race_quantity, side="S", trade_id="cancel-race")
    )
    events.append(
        tape.trade(8.3, price="100.06", quantity="100", side="S", trade_id="after-cancel")
    )
    report = replay_events(
        events,
        config(entry_style="marketable"),
        latency=ReplayLatency(send_to_arrival_ms=20, arrival_to_ack_ms=20, cancel_latency_ms=200),
    )
    order = report["orders"][0]
    assert len(report["orders"]) == 1
    assert Decimal(order["limit_price"]) == Decimal("100.06")
    assert order["cancel_reason"] == "entry_ttl"
    assert order["cancel_requested_at_ns"] == order["decision_at_ns"] + 3 * NS
    assert order["cancel_arrived_at_ns"] == order["cancel_requested_at_ns"] + 200_000_000
    assert order["status"] == expected_status and order["terminal_reported"]
    assert len(report["fills"]) == 2
    initial, resting = report["fills"]
    assert initial["liquidity"] == "taker" and Decimal(initial["quantity"]) == Decimal("0.1")
    assert Decimal(initial["fee_bps"]) == 25
    assert resting["liquidity"] == "maker" and Decimal(resting["fee_bps"]) == 15
    assert order["cancel_requested_at_ns"] < resting["filled_at_ns"] < order["cancel_arrived_at_ns"]
    for fill in report["fills"]:
        assert Decimal(fill["price"]) == Decimal(order["limit_price"])
        assert Decimal(fill["fee_usd"]) == (
            Decimal(fill["quantity"]) * Decimal(fill["price"]) * Decimal(fill["fee_bps"]) / 10000
        )
    expected_quantity = (
        Decimal("0.3") if expected_status == "canceled" else Decimal(order["quantity"])
    )
    assert Decimal(order["filled_quantity"]) == expected_quantity
    assert Decimal(order["unfilled_quantity"]) == Decimal(order["quantity"]) - expected_quantity
    assert Decimal(report["open_positions"][0]["quantity"]) == expected_quantity
    assert Decimal(report["pnl"]["fees_paid_usd"]) == sum(
        Decimal(fill["fee_usd"]) for fill in report["fills"]
    )
    assert not report["closed_positions"]


def test_marketable_entry_that_misses_the_ask_rests_and_its_first_fill_is_maker() -> None:
    tape, events = marketable_depth_tape()
    events.append(tape.event("q", 5.02, bp="100.05", bs="5", ap="100.10", **{"as": "0.1"}))
    events.append(tape.trade(5.2, price="100.06", quantity="0.2", side="S"))
    report = replay_events(events, config(entry_style="marketable"), latency_ms=50)
    order = report["orders"][0]
    assert Decimal(order["limit_price"]) == Decimal("100.06")
    assert order["status"] == "partially_filled" and order["cancel_requested_at_ns"] is None
    assert len(report["fills"]) == 1
    fill = report["fills"][0]
    assert fill["liquidity"] == "maker" and Decimal(fill["fee_bps"]) == 15
    assert Decimal(fill["price"]) == Decimal("100.06")
    assert Decimal(fill["quantity"]) == Decimal("0.2")


def test_slippage_cannot_turn_marketable_entry_into_a_fill_above_its_ask_limit() -> None:
    tape, events = marketable_depth_tape()
    events.append(tape.event("q", 5.1, bp="100.05", bs="5", ap="100.06", **{"as": "0.1"}))
    report = replay_events(events, config(entry_style="marketable"), slippage_bps=Decimal("1"))
    order = report["orders"][0]
    assert Decimal(order["limit_price"]) == Decimal("100.06")
    assert not report["fills"]
    assert order["status"] == "resting" and order["cancel_requested_at_ns"] is None
    assert report["open_order_count"] == 1


def test_replay_is_repeatable_for_models_dicts_and_canonical_jsonl() -> None:
    events = filled_tape()
    first = replay_events(events, config())
    second = replay_events([event.model_dump(mode="json") for event in events], config())
    third = replay_events((event.model_dump_json() for event in events), config())
    assert first == second == third
    json.dumps(first, allow_nan=False)
    assert first["profitability_validated"] is False and first["expected_net_edge_bps"] is None
    assert not first["assumptions"]["exact_queue_position_available"]
    assert first["decision_count"] > 0 and first["fills"]


def test_visible_queue_must_advance_and_unknown_or_buy_side_volume_cannot_fill() -> None:
    tape = Tape()
    events = rising(tape)
    events.extend(
        [
            tape.trade(5.10, price="100.05", quantity="100", side=None, trade_id="unknown"),
            tape.trade(5.20, price="100.05", quantity="100", side="B", trade_id="buyer"),
            tape.trade(5.30, price="100.05", quantity="2", side="S", trade_id="seller"),
        ]
    )
    events.extend(rising(tape, 6, 8))
    report = replay_events(events, config())
    first = report["orders"][0]
    assert Decimal(first["queue_ahead_initial"]) == 5
    assert Decimal(first["queue_ahead_remaining"]) == 3
    assert Decimal(first["filled_quantity"]) == 0
    assert report["fills"] == []
    assert report["rates"]["orders_with_fill"] == 0


def test_quote_and_l2_cancellations_are_never_a_passive_fill_or_front_of_queue_credit() -> None:
    tape = Tape()
    events = rising(tape)
    events.append(tape.book(5.1, bid="100.05", ask="100.06", bid_size="1", ask_size="0.1"))
    events.append(tape.event("q", 5.2, bp="100.05", bs="0.1", ap="100.06", **{"as": "0.1"}))
    events.append(tape.trade(5.3, price="100.05", quantity="2", side="S", trade_id="seller"))
    report = replay_events(events, config())
    assert report["fills"] == []
    assert Decimal(report["orders"][0]["queue_ahead_remaining"]) == 3


def test_partial_fills_charge_exact_actual_notional_fees_and_exit_only_owned_quantity() -> None:
    report = replay_events(filled_tape(), config())
    entries = [fill for fill in report["fills"] if fill["side"] == "buy"]
    exits = [fill for fill in report["fills"] if fill["side"] == "sell"]
    assert len(entries) == 2 and len(exits) == 1
    assert Decimal(entries[0]["quantity"]) == Decimal("0.5")
    assert sum(Decimal(fill["quantity"]) for fill in entries) == Decimal(exits[0]["quantity"])
    for fill in report["fills"]:
        rate = Decimal(15 if fill["liquidity"] == "maker" else 25)
        assert Decimal(fill["fee_usd"]) == (
            Decimal(fill["price"]) * Decimal(fill["quantity"]) * rate / 10000
        )
    assert report["closed_positions"]
    closed = report["closed_positions"][0]
    assert closed["hold_seconds"] >= 15
    assert Decimal(closed["net_pnl_usd"]) == (
        Decimal(closed["gross_pnl_usd"]) - Decimal(closed["fees_usd"])
    )
    assert Decimal(closed["net_pnl_usd"]) < 0
    assert Decimal(report["pnl"]["fees_paid_usd"]) == sum(
        Decimal(fill["fee_usd"]) for fill in report["fills"]
    )
    assert Decimal(report["pnl"]["cash_delta_usd"]) == pytest.approx(
        Decimal(report["pnl"]["realized_net_usd"])
    )


def test_fees_and_slippage_change_pnl_not_alpha_qualification() -> None:
    events = filled_tape()
    normal = replay_events(events, config())
    stressed = replay_events(events, config(maker_fee_bps=150, taker_fee_bps=250), slippage_bps=10)
    assert [item["action"] for item in normal["decisions"]] == [
        item["action"] for item in stressed["decisions"]
    ]
    assert Decimal(stressed["pnl"]["realized_net_usd"]) < Decimal(normal["pnl"]["realized_net_usd"])


def test_latency_changes_fill_outcomes_and_statistics_come_from_lifecycle_samples() -> None:
    events = filled_tape(end=7)
    fast = replay_events(events, config(), latency_ms=20)
    slow = replay_events(events, config(), latency_ms=500)
    assert len(fast["fills"]) == 2 and slow["fills"] == []
    order = fast["orders"][0]
    assert order["decision_at_ns"] < order["arrived_at_ns"] < order["first_fill_at_ns"]
    assert order["sent_at_ns"] <= order["acknowledged_at_ns"]
    assert fast["latency_ms"]["send_to_arrival"] == {"samples": 1, "p50": 20, "p95": 20, "p99": 20}
    measured = (order["first_fill_at_ns"] - order["sent_at_ns"]) / 1_000_000
    assert fast["latency_ms"]["send_to_first_fill"]["p50"] == measured
    assert fast["latency_ms"]["send_to_first_fill"]["p50"] != 20
    assert fast["latency_ms"]["fill_feedback"]["samples"] == 2


def test_deduplicated_native_trade_cannot_advance_the_replay_queue_twice() -> None:
    tape = Tape()
    events = rising(tape)
    events.extend(
        [
            tape.trade(5.1, price="100.05", quantity="3", trade_id="same", side="S"),
            tape.trade(5.1, price="100.05", quantity="3", trade_id="same", side="S"),
            tape.trade(5.2, price="100.05", quantity="1", trade_id="other", side="S"),
        ]
    )
    report = replay_events(events, config())
    assert not report["fills"]
    assert Decimal(report["orders"][0]["queue_ahead_remaining"]) == 1


def test_trade_received_after_arrival_but_executed_before_arrival_is_not_a_fill() -> None:
    tape = Tape()
    events = rising(tape)
    events.append(tape.trade(5.02, received_seconds=5.2, price="100.05", quantity="100", side="S"))
    report = replay_events(events, config(), latency_ms=50)
    assert report["orders"][0]["arrived_at_ns"] == BASE_NS + 5_060_000_000
    assert not report["fills"]


def test_markouts_require_actual_future_observations_and_unfinished_positions_stay_open() -> None:
    short = replay_events(filled_tape(end=6), config())
    assert short["open_positions"]
    assert short["closed_positions"] == []
    assert short["fills"][0]["markouts"]["100ms"] is None
    assert short["fills"][0]["markouts"]["1s"] is None
    assert short["fills"][0]["markouts"]["5s"] is None
    assert short["markout_coverage"]["5s"]["censored"] == 2
    extended = replay_events(filled_tape(end=12), config())
    first = extended["fills"][0]
    assert first["markouts"]["5s"]["exchange_at_ns"] == BASE_NS + 10 * NS
    assert first["markouts"]["5s"]["observed_at_ns"] >= first["markouts"]["5s"]["target_at_ns"]
    assert first["mae_bps"] is not None and first["mfe_bps"] >= first["mae_bps"]


def test_replay_exposes_all_seven_bounded_markouts_and_stops_excursions_after_close():
    short = replay_events(filled_tape(end=23), config())
    longer = replay_events(filled_tape(end=60), config())
    first = short["fills"][0]
    assert set(first["markouts"]) == {"100ms", "250ms", "500ms", "1s", "2s", "5s", "10s"}
    assert first["markouts"]["100ms"] is None
    assert first["markouts"]["1s"]["observation_age_ms"] <= 250
    assert first["exposure_closed_at_ns"] is not None
    assert first["mfe_bps"] == longer["fills"][0]["mfe_bps"]
    assert first["mae_bps"] == longer["fills"][0]["mae_bps"]


def test_economic_replay_base_coin_fee_matches_cash_conservation_without_double_charge():
    from tradeagent.scalping_replay import _ExecutionAssumptions, _Fill, _Replay

    replay = _Replay(
        config(decision_policy="action-value-v1", catastrophic_stop_bps="100"),
        ReplayLatency(),
        _ExecutionAssumptions(),
    )
    entry = _Fill(
        fill_id="entry",
        order_id="buy",
        symbol="BTC/USD",
        side="buy",
        quantity=Decimal(1),
        price=Decimal(100),
        fee=Decimal(".25"),
        fee_bps=Decimal(25),
        liquidity="maker",
        filled_at_ns=BASE_NS,
        midpoint_at_arrival=Decimal(100),
        base_fee_quantity=Decimal(".0025"),
    )
    replay.fills.append(entry)
    replay._book_fill(entry)
    owned = replay.positions["BTC/USD"].quantity
    assert owned == Decimal(".9975") and replay.cash == Decimal("-100")
    exit_fee = owned * Decimal(101) * Decimal(".0025")
    exit_fill = _Fill(
        fill_id="exit",
        order_id="sell",
        symbol="BTC/USD",
        side="sell",
        quantity=owned,
        price=Decimal(101),
        fee=exit_fee,
        fee_bps=Decimal(25),
        liquidity="taker",
        filled_at_ns=BASE_NS + 5 * NS,
        midpoint_at_arrival=Decimal(101),
    )
    replay._book_fill(exit_fill)
    assert replay.positions == {}
    assert replay.cash == owned * Decimal(101) - Decimal(100) - exit_fee
    assert replay.gross_realized - replay.realized_fees == replay.cash
    assert replay.realized_fees == Decimal(".25") + exit_fee


def test_end_of_tape_does_not_acknowledge_or_arrive_an_inflight_order() -> None:
    report = replay_events(rising(Tape()), config(), latency_ms=500)
    order = report["orders"][0]
    assert order["status"] == "in_flight"
    assert order["sent_at_ns"] is not None and order["arrived_at_ns"] is None
    assert order["acknowledged_at_ns"] is None
    assert report["open_order_count"] == 1
    assert report["latency_ms"]["send_to_arrival"]["samples"] == 0
    assert report["fills"] == []


def test_partial_fill_can_race_a_cancel_and_is_not_lost_or_oversold() -> None:
    tape = Tape()
    events = rising(tape)
    events.append(tape.trade(5.10, price="100.05", quantity="5.2", side="S", trade_id="one"))
    events.extend(rising(tape, 6, 8))
    events.append(tape.trade(8.10, price="100.05", quantity="0.2", side="S", trade_id="race"))
    events.append(tape.book(8.5, bid="100.08", ask="100.09", bid_size="5", ask_size="0.1"))
    report = replay_events(
        events,
        config(),
        latency=ReplayLatency(send_to_arrival_ms=20, arrival_to_ack_ms=20, cancel_latency_ms=200),
    )
    first = report["orders"][0]
    assert first["cancel_requested_at_ns"] < report["fills"][-1]["filled_at_ns"]
    assert report["fills"][-1]["filled_at_ns"] < first["cancel_arrived_at_ns"]
    assert first["status"] == "canceled" and first["terminal_reported"]
    assert Decimal(first["filled_quantity"]) == Decimal("0.4")
    assert Decimal(report["open_positions"][0]["quantity"]) == Decimal("0.4")


def test_marketable_limit_uses_observed_depth_and_keeps_its_partial_remainder() -> None:
    tape = Tape()
    events = rising(tape)
    events.extend(rising(tape, 6, 6))
    report = replay_events(
        events, config(entry_style="marketable"), participation_rate=Decimal("0.5")
    )
    first = report["orders"][0]
    assert first["status"] == "partially_filled"
    assert first["cancel_reason"] is None and first["cancel_requested_at_ns"] is None
    assert Decimal(first["limit_price"]) == Decimal("100.06")
    assert Decimal(first["filled_quantity"]) == Decimal("0.05")
    assert report["fills"][0]["liquidity"] == "taker"
    assert Decimal(report["fills"][0]["fee_bps"]) == 25
    assert report["open_positions"]


def test_invalid_book_at_arrival_rejects_instead_of_using_previous_depth() -> None:
    tape = Tape()
    events = rising(tape)
    events.append(tape.event("o", 5.1, b=[{"p": 101, "s": 5}]))
    events.append(tape.trade(5.6, trade_id="later"))
    report = replay_events(events, config(), latency_ms=500)
    assert report["fills"] == []
    assert report["orders"][0]["status"] == "rejected"
    assert report["orders"][0]["rejection"] == "market_data_unavailable_at_arrival"


def test_snapshot_reset_rebases_queue_without_cancellation_priority_credit() -> None:
    tape = Tape()
    events = rising(tape)
    events.append(tape.trade(5.1, price="100.05", quantity="3", side="S", trade_id="one"))
    events.append(
        tape.book(5.2, bid="100.05", ask="100.06", bid_size="4", ask_size="0.1", snapshot=True)
    )
    events.append(tape.trade(5.3, price="100.05", quantity="3", side="S", trade_id="two"))
    report = replay_events(events, config())
    first = report["orders"][0]
    assert first["queue_rebases"] == 1
    assert Decimal(first["queue_ahead_remaining"]) == 1
    assert not report["fills"]


def test_queue_multiplier_and_participation_are_conservative_not_exact_queue_claims() -> None:
    normal = replay_events(filled_tape(end=7), config())
    deeper = replay_events(filled_tape(end=7), config(), queue_ahead_multiplier=Decimal(2))
    partial = replay_events(filled_tape(end=7), config(), participation_rate=Decimal("0.1"))
    assert normal["fills"] and not deeper["fills"]
    assert Decimal(partial["orders"][0]["filled_quantity"]) < Decimal(
        normal["orders"][0]["filled_quantity"]
    )


def test_empty_replay_and_invalid_input_are_not_success_shaped_profits() -> None:
    empty = replay_events([], config())
    assert empty["events"] == empty["decision_count"] == 0
    assert empty["rates"]["orders_with_fill"] is None
    assert empty["latency_ms"]["send_to_first_fill"]["p99"] is None
    assert empty["open_positions"] == [] and empty["expected_net_edge_bps"] is None
    events = rising(Tape())
    with pytest.raises(ValueError, match="receive-time order"):
        replay_events(list(reversed(events)), config())
    with pytest.raises(ValueError):
        replay_events(["not canonical JSON"], config())
    for latency in (-1, float("nan"), float("inf")):
        with pytest.raises(ValueError, match="latency"):
            replay_events([], config(), latency_ms=latency)


@pytest.mark.parametrize(
    "overrides",
    [
        {"slippage_bps": Decimal("NaN")},
        {"queue_ahead_multiplier": Decimal("0.5")},
        {"participation_rate": Decimal(0)},
        {"participation_rate": Decimal(2)},
    ],
)
def test_invalid_execution_assumptions_fail_explicitly(overrides: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        replay_events([], config(), **overrides)


def test_aggressive_fill_cannot_use_l2_prices_contradicted_by_a_newer_bbo() -> None:
    tape = Tape()
    events = rising(tape)
    events.append(tape.event("q", 5.02, bp="100.09", bs="5", ap="100.10", **{"as": "0.1"}))
    events.append(tape.trade(5.2, trade_id="later", side=None))
    report = replay_events(events, config(entry_style="marketable"), latency_ms=50)
    assert not report["fills"]
    assert report["orders"][0]["status"] == "resting"
    assert Decimal(report["orders"][0]["limit_price"]) == Decimal("100.06")


def test_passive_arrival_accounts_for_more_recent_visible_bbo_depth() -> None:
    tape = Tape()
    events = rising(tape)
    events.append(tape.event("q", 5.02, bp="100.05", bs="20", ap="100.06", **{"as": "0.1"}))
    events.append(tape.trade(5.1, price="100.05", quantity="10", trade_id="seller", side="S"))
    report = replay_events(events, config(), latency_ms=50)
    assert not report["fills"]
    assert Decimal(report["orders"][0]["queue_ahead_initial"]) == 20
    assert Decimal(report["orders"][0]["queue_ahead_remaining"]) == 10


def test_nanosecond_receipts_do_not_make_all_replay_decisions_future_dated() -> None:
    events = [
        MarketEvent.model_validate(
            {
                **event.model_dump(),
                "received_at_ns": event.received_at_ns + 99,
                "received_monotonic_ns": event.received_monotonic_ns + 99,
            }
        )
        for event in filled_tape(end=7)
    ]
    report = replay_events(events, config())
    assert len(report["fills"]) == 2
    assert report["orders"][0]["decision_at_ns"] >= events[5].received_at_ns
    assert report["fills"][0]["trigger_received_at_ns"] == events[6].received_at_ns
    assert report["fills"][0]["filled_at_ns"] == events[6].received_at_ns


def test_market_partial_exit_cannot_reconsume_unchanged_displayed_depth() -> None:
    tape = Tape()
    events = rising(tape)
    events.append(tape.book(6, bid="100.06", ask="100.07", bid_size="0.04", ask_size="0.1"))
    events.append(tape.event("q", 6.4, bp="100.06", bs="0.04", ap="100.07", **{"as": "0.1"}))
    events.append(tape.trade(7.2, trade_id="later", side=None))
    events.append(tape.trade(8.2, trade_id="retry", side=None))
    report = replay_events(events, config(entry_style="marketable", exit_after_seconds=0.5))
    entries = [order for order in report["orders"] if order["side"] == "buy"]
    exits = [order for order in report["orders"] if order["side"] == "sell"]
    assert len(entries) == 1 and entries[0]["status"] == "canceled"
    assert entries[0]["cancel_reason"] == "entry_signal_invalidated"
    assert len(exits) == 2 and all(order["limit_price"] is None for order in exits)
    assert all(order["sent_at_ns"] >= entries[0]["cancel_acknowledged_at_ns"] for order in exits)
    sells = [fill for fill in report["fills"] if fill["side"] == "sell"]
    assert all(fill["liquidity"] == "taker" for fill in sells)
    assert sum(Decimal(fill["quantity"]) for fill in sells) == Decimal("0.04")
    assert Decimal(report["open_positions"][0]["quantity"]) == Decimal("0.06")
    assert not report["closed_positions"]


def test_all_lifecycle_and_markout_evidence_is_causal() -> None:
    report = replay_events(filled_tape(), config())
    for decision in report["decisions"]:
        assert decision["quote"]["exchange_time_ns"] <= decision["features"]["as_of_ns"]
    for order in report["orders"]:
        if order["arrived_at_ns"] is not None:
            assert order["decision_at_ns"] <= order["sent_at_ns"] <= order["arrived_at_ns"]
        if order["first_fill_at_ns"] is not None:
            assert order["arrived_at_ns"] <= order["first_fill_at_ns"] <= report["ended_at_ns"]
    for fill in report["fills"]:
        if fill["mae_bps"] is not None:
            assert fill["mae_bps"] <= 0 <= fill["mfe_bps"]
        for mark in fill["markouts"].values():
            if mark is not None:
                assert fill["filled_at_ns"] < mark["target_at_ns"]
                assert mark["exchange_at_ns"] <= mark["target_at_ns"] == mark["observed_at_ns"]
                assert mark["observed_at_ns"] <= report["ended_at_ns"]
                assert mark["observation_age_ms"] <= 250


def test_replay_old_reset_and_only_l2_updates_allow_alpha_but_never_fake_passive_fills() -> None:
    tape = Tape()
    events = [
        tape.book(
            -67,
            received_seconds=0.01,
            bid="99",
            ask="99.01",
            bid_size="5",
            ask_size="0.1",
            snapshot=True,
        )
    ]
    events.extend(rising(tape, 1, 7))
    report = replay_events(events, config())
    assert report["orders"] and not report["fills"]
    assert all(order["decision_at_ns"] >= BASE_NS + 6 * NS for order in report["orders"])
    buys = [signal for signal in report["decisions"] if signal["action"] == "buy"]
    assert buys and all(signal["features"]["trade_count_5s"] == 0 for signal in buys)
    assert all(signal["features"]["taker_delta_5s"] is None for signal in buys)
    assert all(signal["features"]["session_vwap"] is None for signal in buys)
    assert not report["profitability_validated"] and report["expected_net_edge_bps"] is None


def test_reconnect_old_snapshot_cannot_reactivate_an_existing_passive_queue() -> None:
    tape = Tape()
    events = rising(tape)
    tape.connection, tape.sequence = UUID(int=2), 0
    events.append(tape.book(-34, received_seconds=5.2, bid="100.05", ask="100.06", snapshot=True))
    events.append(tape.trade(5.3, price="100.05", quantity="100", side="S", trade_id="after-reset"))
    report = replay_events(events, config())
    assert not report["fills"]
    assert not report["orders"][0]["queue_valid"]
    assert report["market_health"]["symbols"]["BTC/USD"]["awaiting_current_book_update"]


def test_capture_converter_and_jsonl_use_identical_replay_without_control_or_price_invention() -> (
    None
):
    events = filled_tape()
    records = [captured_row(handshake()[-1][0], 1)]
    for sequence, event in enumerate(events, start=2):
        if event.event_type == "book":
            payload = {
                "T": "o",
                "S": event.symbol,
                "t": event.exchange_timestamp,
                "r": event.reset,
                "b": [{"p": level.price, "s": level.quantity} for level in event.bids],
                "a": [{"p": level.price, "s": level.quantity} for level in event.asks],
            }
        else:
            payload = {
                "T": "t",
                "S": event.symbol,
                "t": event.exchange_timestamp,
                "p": event.trade_price,
                "s": event.trade_quantity,
                "i": event.trade_id,
                "tks": "S",
            }
        records.append(
            {
                "connection_id": str(event.connection_id),
                "receive_sequence": sequence,
                "received_at": event.received_at.isoformat(),
                "received_monotonic_ns": event.received_monotonic_ns,
                "payload": payload,
            }
        )
    canonical = list(capture_events(records))
    report = replay_events(canonical, config())
    assert report == replay_events([event.model_dump_json() for event in canonical], config())
    assert len(canonical) == len(events)
    assert all(event.event_type != "reset" for event in canonical)
    assert canonical[0].receive_sequence == 2
    assert report["market_health"]["local_receive_gaps"] == 0
    assert report["fills"]
