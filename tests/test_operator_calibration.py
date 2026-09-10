from __future__ import annotations

import copy
import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
from pydantic import ValidationError
from sqlalchemy import select, update
from test_alpaca_paper import _settings
from test_event_orders import Broker
from test_iex_practice import Clock, tick
from test_iex_practice import make_runtime as make_runtime

from tradeagent import operator_calibration
from tradeagent.alpaca_paper import AlpacaOrderStatus, AlpacaPaperClient
from tradeagent.event_account_risk import account_risk
from tradeagent.event_session import session_control_key
from tradeagent.operator_calibration import OperatorPaperRequest, configuration, control_versions
from tradeagent.paper_account_history import history_identity, save_baseline, value_history
from tradeagent.persistence import notification_outbox, worker_locks

AT = datetime(2026, 9, 10, 16, 20, tzinfo=UTC)
ACCOUNT = sha256(b"paper-fixture").hexdigest()


def history():
    source = json.loads(
        Path("research", "results", "news-paper-20260909-afternoon-broker-budget.json").read_text()
    )["result"]
    return {
        "account_digest": ACCOUNT,
        "broker_host": "https://paper-api.alpaca.markets",
        "complete": True,
        "observed_at": AT.isoformat(),
        "orders": source["orders"],
        "activities": source["activities"],
    }


def complete_history():
    snapshot = history()
    spy, buy, sell = copy.deepcopy(snapshot["orders"])
    spy.update(
        id="early-spy",
        client_order_id="early-spy",
        symbol="SPY",
        qty="1",
        created_at="2026-09-04T22:24:25Z",
        submitted_at="2026-09-04T22:24:25Z",
        canceled_at="2026-09-05T00:29:19Z",
    )
    buy.update(
        id="early-buy",
        client_order_id="early-buy",
        qty="0.001",
        notional=None,
        filled_qty="0.001",
        filled_avg_price="79793.284",
        created_at="2026-09-04T22:30:59Z",
        submitted_at="2026-09-04T22:30:59Z",
        filled_at="2026-09-04T22:30:59Z",
    )
    sell.update(
        id="early-sell",
        client_order_id="early-sell",
        qty="0.0009975",
        filled_qty="0.0009975",
        filled_avg_price="79591.4",
        created_at="2026-09-05T00:18:00Z",
        submitted_at="2026-09-05T00:18:00Z",
        filled_at="2026-09-05T00:18:00Z",
    )
    asset_fee = copy.deepcopy(snapshot["activities"][1])
    asset_fee.update(
        id="early-coin-fee",
        date="2026-09-04",
        created_at="2026-09-04T22:31:02Z",
        qty="-0.0000025",
        price="79793.28",
    )
    buy_fill, sell_fill = copy.deepcopy(snapshot["activities"][-2:])
    buy_fill.update(
        id="early-buy-fill",
        order_id="early-buy",
        transaction_time=buy["filled_at"],
        qty=buy["filled_qty"],
        cum_qty=buy["filled_qty"],
        price=buy["filled_avg_price"],
    )
    sell_fill.update(
        id="early-sell-fill",
        order_id="early-sell",
        transaction_time=sell["filled_at"],
        qty=sell["filled_qty"],
        cum_qty=sell["filled_qty"],
        price=sell["filled_avg_price"],
    )
    snapshot["orders"] = [spy, buy, sell, *snapshot["orders"]]
    snapshot["activities"] = [
        {
            "id": "initial-funding",
            "activity_type": "JNLC",
            "date": "2026-09-04",
            "created_at": "2026-09-04T22:20:26Z",
            "net_amount": "100000",
            "currency": "USD",
            "status": "executed",
        },
        asset_fee,
        buy_fill,
        sell_fill,
        *snapshot["activities"],
    ]
    snapshot["account_cash"] = "99999.27"
    return snapshot


def prepare(make, monkeypatch, *, snapshot=None, **changes):
    runtime = make(confirmed=False, mode="shadow", practice_start_date=date(2026, 9, 9))
    monkeypatch.setattr(operator_calibration, "datetime", Clock)
    monkeypatch.setattr("tradeagent.event_orders.datetime", Clock)
    Clock.current = runtime.broker.now = AT
    runtime.code_sha = "a" * 40
    runtime.repo.set_control("kill_switch", "active")
    runtime.repo.set_control(f"{runtime.settings.cohort_id}:pause", "R1_REQUIRES_REVIEW")
    runtime.context = runtime.context_client.poll(symbols=["AAPL"])
    runtime.last_source_success = AT
    runtime.market_states["AAPL"] = runtime.market.state("AAPL", AT - timedelta(seconds=3))
    snapshot = snapshot or history()
    runtime.broker.account_history = lambda: copy.deepcopy(snapshot)
    request_id = uuid4()
    request = OperatorPaperRequest(
        **{
            "request_id": request_id,
            "action": "one_paper_AAPL_round_trip",
            "worker_cohort_id": runtime.settings.cohort_id,
            "worker_config_hash": runtime.config_hash,
            "code_sha": runtime.code_sha,
            "account_digest": ACCOUNT,
            "session_date": AT.date(),
            "approved_at": AT - timedelta(seconds=1),
            "entry_deadline": AT + timedelta(minutes=10),
            "acceptance_sha256": "c" * 64,
            "history_sha256": history_identity(snapshot),
            "owner_authority": "Offline fixture: explicit one bounded paper test",
            "checks": dict.fromkeys(
                (
                    "reviewed_release",
                    "market_acceptance",
                    "account_history_complete",
                    "operator_confirmation",
                    "normal_entries_paused",
                ),
                True,
            ),
            "control_versions": control_versions(
                runtime.repo, runtime.settings.cohort_id, "operator-paper-" + request_id.hex
            ),
            **changes,
        }
    )
    runtime.repo.set_control(
        f"operator-paper-request:{runtime.settings.cohort_id}", request.model_dump_json()
    )
    return runtime, request


def test_history_values_crypto_quantity_fee_once_and_imports_two_prior_buys(make_runtime):
    runtime = make_runtime()
    snapshot = history()
    valued = value_history(snapshot)
    assert Decimal(valued["cash_pnl"]) == Decimal("-0.12723217556")
    assert Decimal(valued["economic_pnl"]) < Decimal(valued["cash_pnl"])
    save_baseline(runtime.store, snapshot, AT)
    save_baseline(runtime.store, snapshot, AT)
    budget = json.loads(runtime.repo.get_control(session_control_key(ACCOUNT, date(2026, 9, 9))))
    assert budget["total_entries_reserved"] == 2
    assert len(budget["external_buy_client_ids"]) == 2
    assert runtime.repo.get_control(session_control_key(ACCOUNT, AT.date())) is None


@pytest.mark.parametrize(
    "corruption", ["fee", "fill", "unknown", "duplicate", "open", "incomplete"]
)
def test_unvalued_history_never_imports_risk_baseline(make_runtime, corruption):
    runtime = make_runtime()
    snapshot = history()
    if corruption == "fee":
        snapshot["activities"] = [a for a in snapshot["activities"] if a.get("symbol") != "BTCUSD"]
    elif corruption == "fill":
        snapshot["activities"][-1]["qty"] = "1"
    elif corruption == "unknown":
        snapshot["activities"][0]["activity_type"] = "UNKNOWN"
    elif corruption == "duplicate":
        snapshot["activities"].append(copy.deepcopy(snapshot["activities"][0]))
    elif corruption == "open":
        snapshot["orders"][0]["status"] = "new"
    else:
        snapshot["complete"] = False
    with pytest.raises(ValueError):
        save_baseline(runtime.store, snapshot, AT)
    assert runtime.repo.get_control("paper-baseline:" + ACCOUNT) is None
    assert runtime.repo.get_control(session_control_key(ACCOUNT, date(2026, 9, 9))) is None


def test_operator_request_uses_oms_while_old_shadow_and_global_pause_are_unchanged(
    make_runtime, monkeypatch
):
    runtime, request = prepare(make_runtime, monkeypatch)
    before = control_versions(runtime.repo, runtime.settings.cohort_id)
    first = tick(runtime, AT)
    assert first["operator_paper"]["state"] == "filled", first
    assert runtime.broker.submissions == 1
    rows = runtime.store.linked_orders(request.cohort_id)
    assert rows[0]["quantity"] * Decimal(rows[0]["link"]["limit_price"]) <= 25
    assert rows[0]["link"]["qualification_eligible"] is False
    assert rows[0]["link"]["decision_ticket"]["probability_of_profit"] is None
    assert rows[0]["link"]["protection"]["basis"] == "actual_broker_cumulative_filled_vwap"
    assert not runtime.store.linked_orders(runtime.settings.cohort_id)
    assert control_versions(runtime.repo, runtime.settings.cohort_id) == before
    assert tick(runtime, AT + timedelta(seconds=61))["operator_paper"]["state"] == "completed_flat"
    assert runtime.broker.submissions == 2
    assert not runtime.broker.positions() and not runtime.broker.open_orders()
    assert runtime.repo.get_control("operator-paper-active") is None
    assert runtime.repo.get_control(f"operator-paper-request:{runtime.settings.cohort_id}") is None
    assert runtime.repo.get_control(f"{request.cohort_id}:operator-terminal") is not None
    assert runtime.repo.get_control(f"{request.cohort_id}:operator-scope") is not None
    assert control_versions(runtime.repo, runtime.settings.cohort_id) == before
    with runtime.store.database.begin() as connection:
        messages = list(connection.execute(select(notification_outbox)).mappings())
    assert (
        len([row for row in messages if row["notification_type"] == "paper_order_lifecycle"]) == 2
    )
    settings, _, _ = configuration(request, runtime.app)
    risk = account_risk(runtime.store, settings, {}, AT + timedelta(seconds=62))
    assert risk["state"] == "valued" and not risk["blocked"]
    assert Decimal(risk["week_start_equity"]) == 10000
    assert Decimal(risk["day_start_equity"]) < 10000


@pytest.mark.parametrize("kind", ["partial", "unknown", "stop", "profit"])
def test_owned_partial_and_unknown_recovery_local_protection(make_runtime, monkeypatch, kind):
    runtime, request = prepare(make_runtime, monkeypatch)
    runtime.broker.partial = kind in {"partial", "unknown"}
    runtime.broker.timeout = kind == "unknown"
    tick(runtime, AT)
    bought = runtime.broker.positions()[0].quantity
    runtime.market.changes.update(
        bid=Decimal("101") if kind == "profit" else Decimal("99.4"),
        ask=Decimal("101.01") if kind == "profit" else Decimal("99.41"),
    )
    tick(runtime, AT + timedelta(seconds=10))
    rows = runtime.store.linked_orders(request.cohort_id)
    assert len([row for row in rows if row["side"] == "buy"]) == 1
    assert rows[-1]["side"] == "sell"
    assert rows[-1]["quantity"] == bought
    assert not runtime.broker.positions() and not runtime.broker.open_orders()


@pytest.mark.parametrize(
    "kind", ["expiry", "spread", "stale", "halt", "control", "history", "budget"]
)
def test_no_entry_after_guard_failure(make_runtime, monkeypatch, kind):
    snapshot = history()
    if kind == "budget":
        for order in snapshot["orders"]:
            for field in ("created_at", "submitted_at", "filled_at", "canceled_at"):
                if order.get(field):
                    order[field] = order[field].replace("2026-09-09", "2026-09-10")
        for activity in snapshot["activities"]:
            for field in ("created_at", "transaction_time", "date"):
                if activity.get(field):
                    activity[field] = activity[field].replace("2026-09-09", "2026-09-10")
    runtime, request = prepare(make_runtime, monkeypatch, snapshot=snapshot)
    at = AT
    if kind == "expiry":
        at = request.entry_deadline
    elif kind == "spread":
        runtime.market.changes.update(bid=Decimal("99"), ask=Decimal("100"))
    elif kind == "stale":
        runtime.market.changes["quote_at"] = AT - timedelta(seconds=6)
    elif kind == "halt":
        runtime.context_client.halted = True
        runtime.context = runtime.context_client.poll(symbols=["AAPL"])
    elif kind == "control":
        runtime.repo.set_control(f"{runtime.settings.cohort_id}:pause", "NEW_R1_EVENT")
    elif kind == "history":
        snapshot["orders"][0]["client_order_id"] += "-new"
    result = tick(runtime, at)["operator_paper"]
    assert result["state"] in {"blocked_requires_review", "blocked_or_unfilled"}, result
    assert runtime.broker.submissions == 0
    assert runtime.repo.get_control("kill_switch") == "active"


def test_explicit_window_cannot_expand_or_cross_session(make_runtime, monkeypatch):
    with pytest.raises(ValidationError):
        prepare(make_runtime, monkeypatch, entry_deadline=AT + timedelta(minutes=31))


def test_final_dispatch_rechecks_expiry_without_another_buy(make_runtime, monkeypatch):
    runtime, request = prepare(make_runtime, monkeypatch)
    original = runtime.broker.clock
    calls = 0

    def clock():
        nonlocal calls
        calls += 1
        if calls >= 2:
            runtime.broker.now = request.entry_deadline
        return original()

    runtime.broker.clock = clock
    result = tick(runtime, AT)
    assert runtime.broker.submissions == 0, result


def test_restart_new_worker_pin_recovers_old_owned_request_only(make_runtime, monkeypatch):
    runtime, request = prepare(make_runtime, monkeypatch)
    tick(runtime, AT)
    runtime.code_sha = "b" * 40
    runtime.context = None
    result = tick(runtime, AT + timedelta(seconds=61))
    assert result["operator_paper"]["state"] == "completed_flat"
    assert runtime.broker.submissions == 2 and not runtime.broker.positions()
    rows = runtime.store.linked_orders(request.cohort_id)
    assert sum(row["side"] == "buy" for row in rows) == 1


def test_history_cannot_drop_old_losses_or_counted_attempts(make_runtime):
    runtime = make_runtime()
    first = history()
    save_baseline(runtime.store, first, AT)
    before = runtime.repo.get_control("paper-baseline:" + ACCOUNT)
    second = {
        **first,
        "orders": [],
        "activities": [],
        "observed_at": (AT + timedelta(seconds=1)).isoformat(),
    }
    with pytest.raises(ValueError, match="discard"):
        save_baseline(runtime.store, second, AT + timedelta(seconds=1))
    assert runtime.repo.get_control("paper-baseline:" + ACCOUNT) == before
    assert (
        json.loads(runtime.repo.get_control(session_control_key(ACCOUNT, date(2026, 9, 9))))[
            "total_entries_reserved"
        ]
        == 2
    )


@pytest.mark.parametrize("fee_day", ["2026-09-09", "2026-09-10"])
def test_external_daily_and_weekly_loss_blocks_new_operator_entry(
    make_runtime, monkeypatch, fee_day
):
    snapshot = history()
    snapshot["activities"][0].update(net_amount="-51", created_at=f"{fee_day}T06:21:32Z")
    runtime, _ = prepare(make_runtime, monkeypatch, snapshot=snapshot)
    result = tick(runtime, AT)["operator_paper"]
    assert result["state"] == "blocked_or_unfilled", result
    assert "ECONOMIC_RISK_OR_PAUSE" in result["result"]["reasons"]
    assert runtime.broker.submissions == 0


def test_unfilled_order_cancels_at_30_seconds_and_never_retries(make_runtime, monkeypatch):
    runtime, request = prepare(make_runtime, monkeypatch)
    original = runtime.broker.submit_limit_order

    def unfilled(order, limit):
        response = original(order, limit).model_copy(
            update={
                "status": AlpacaOrderStatus.NEW,
                "filled_quantity": Decimal(0),
                "filled_average_price": None,
                "filled_at": None,
            }
        )
        runtime.broker.values[order.client_order_id] = response
        return response

    runtime.broker.submit_limit_order = unfilled
    tick(runtime, AT)
    tick(runtime, AT + timedelta(seconds=29))
    assert runtime.broker.open_orders()
    assert tick(runtime, AT + timedelta(seconds=30))["operator_paper"]["state"] == "no_fill_flat"
    assert not runtime.broker.open_orders()
    operator_calibration.step(runtime)
    assert runtime.broker.submissions == 1
    assert len(runtime.store.linked_orders(request.cohort_id)) == 1


def test_unknown_original_id_remains_unresolved_without_second_buy(make_runtime, monkeypatch):
    runtime, request = prepare(make_runtime, monkeypatch)

    def unknown(order, limit):
        runtime.broker.submissions += 1
        raise httpx.ReadTimeout("fixture unknown delivery")

    runtime.broker.submit_limit_order = unknown
    tick(runtime, AT)
    tick(runtime, AT + timedelta(seconds=31))
    tick(runtime, AT + timedelta(seconds=61))
    assert runtime.broker.submissions == 1
    rows = runtime.store.linked_orders(request.cohort_id)
    assert len(rows) == 1 and rows[0]["status"] == "reconciliation_required"
    with runtime.store.database.begin() as connection:
        payloads = list(connection.scalars(select(notification_outbox.c.payload)))
    assert any("submission_outcome_unknown" in payload.get("subject", "") for payload in payloads)


def test_cancel_requires_confirmation_before_partial_exit(make_runtime, monkeypatch):
    runtime, _ = prepare(make_runtime, monkeypatch)
    runtime.broker.partial = True
    cancel = runtime.broker.cancel_order
    runtime.broker.cancel_order = lambda _: None
    tick(runtime, AT)
    tick(runtime, AT + timedelta(seconds=31))
    assert runtime.broker.submissions == 1
    for order in tuple(runtime.broker.values.values()):
        cancel(order.id)
    tick(runtime, AT + timedelta(seconds=61))
    assert runtime.broker.submissions == 2 and not runtime.broker.positions()


def test_invalid_request_does_not_rewrite_old_pause(make_runtime, monkeypatch):
    runtime, _ = prepare(make_runtime, monkeypatch)
    before = control_versions(runtime.repo, runtime.settings.cohort_id)
    runtime.repo.set_control(f"operator-paper-request:{runtime.settings.cohort_id}", "{}")
    result = tick(runtime, AT)
    assert result["operator_paper"]["state"] == "operator_command_invalid_requires_review"
    assert control_versions(runtime.repo, runtime.settings.cohort_id) == before
    assert runtime.broker.submissions == 0


@pytest.mark.parametrize("scenario", ["complete", "too_many_orders", "repeated_page"])
def test_paper_history_adapter_is_read_only_and_fails_closed_on_pagination(scenario):
    paths = []

    def handler(request):
        assert request.method == "GET" and request.url.host == "paper-api.alpaca.markets"
        paths.append(request.url.path)
        if request.url.path == "/v2/account":
            account = Broker().account().model_copy(update={"cash": Decimal("99999.27")})
            return httpx.Response(200, json=account.model_dump(mode="json"))
        if request.url.path == "/v2/orders":
            return httpx.Response(
                200,
                json=[{}] * 500 if scenario == "too_many_orders" else complete_history()["orders"],
            )
        return httpx.Response(
            200,
            json=[{"id": "same"}] * 100
            if scenario == "repeated_page"
            else complete_history()["activities"],
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as transport:
        client = AlpacaPaperClient(_settings(), client=transport)
        if scenario == "complete":
            snapshot = client.account_history()
            assert snapshot["complete"] and len(snapshot["orders"]) == 6
            assert value_history(snapshot)["cash_pnl"] == "-0.52809467556"
        else:
            with pytest.raises(ValueError):
                client.account_history()
    assert paths


def test_complete_history_includes_funding_early_cycle_and_unposted_fee_reserve(make_runtime):
    runtime = make_runtime()
    valued = save_baseline(runtime.store, complete_history(), AT)
    assert Decimal(valued["cash_pnl"]) == Decimal("-0.52809467556")
    assert valued["external_funding"] == "100000"
    assert Decimal(valued["unexplained_cash_deficit"]) == Decimal("0.20190532444")
    assert not any(row["id"].endswith(":buy") for row in valued["fee_reserves"])
    assert any(
        row["id"] == "estimated-crypto-fee:2026-09-04:sell" and row["reserve"] == "0.20"
        for row in valued["fee_reserves"]
    )
    assert Decimal(valued["economic_pnl"]) < Decimal("-0.73")
    assert Decimal(valued["peak_pnl"]) == 0
    for day in (date(2026, 9, 4), date(2026, 9, 9)):
        budget = json.loads(runtime.repo.get_control(session_control_key(ACCOUNT, day)))
        assert budget["total_entries_reserved"] == 2
    assert runtime.repo.get_control(session_control_key(ACCOUNT, AT.date())) is None


def test_complete_historical_risk_keeps_todays_entry_budget_available(make_runtime, monkeypatch):
    runtime, request = prepare(make_runtime, monkeypatch, snapshot=complete_history())
    result = tick(runtime, AT)
    assert result["operator_paper"]["state"] == "filled", result
    settings, _, _ = configuration(request, runtime.app)
    report = account_risk(runtime.store, settings, {"AAPL": Decimal(100)}, AT)
    assert report["capital"] == "10000" and not report["blocked"]
    assert Decimal(report["week_start_equity"]) < 10000
    assert (
        json.loads(runtime.repo.get_control(session_control_key(ACCOUNT, AT.date())))[
            "total_entries_reserved"
        ]
        == 1
    )


@pytest.mark.parametrize("change", ["later_funding", "unexplained_credit", "unexplained_loss"])
def test_account_cash_changes_cannot_reset_or_hide_risk(make_runtime, monkeypatch, change):
    snapshot = complete_history()
    if change == "later_funding":
        snapshot["activities"][0]["created_at"] = "2026-09-10T10:00:00Z"
        with pytest.raises(ValueError, match="initial funding"):
            value_history(snapshot)
    elif change == "unexplained_credit":
        snapshot["account_cash"] = "100010"
        with pytest.raises(ValueError, match="positive account cash"):
            value_history(snapshot)
    else:
        snapshot["account_cash"] = "99900"
        runtime, _ = prepare(make_runtime, monkeypatch, snapshot=snapshot)
        result = tick(runtime, AT)["operator_paper"]
        assert result["state"] == "blocked_or_unfilled"
        assert runtime.broker.submissions == 0


def test_late_posted_fee_replaces_reserve_without_double_charge(make_runtime):
    runtime = make_runtime()
    snapshot = complete_history()
    previous = save_baseline(runtime.store, snapshot, AT)
    snapshot["activities"].append(
        {
            "id": "late-earlier-sell-fee",
            "activity_type": "CFEE",
            "date": "2026-09-04",
            "created_at": AT.isoformat(),
            "net_amount": "-0.20",
            "currency": "USD",
            "status": "executed",
        }
    )
    snapshot["observed_at"] = (AT + timedelta(seconds=1)).isoformat()
    updated = save_baseline(runtime.store, snapshot, AT + timedelta(seconds=1))
    assert not any("estimated-crypto-fee" in row["id"] for row in updated["fee_reserves"])
    assert Decimal(updated["cash_pnl"]) == Decimal(previous["cash_pnl"]) - Decimal("0.20")
    assert Decimal(updated["economic_pnl"]) == Decimal(previous["economic_pnl"])


def test_terminal_entry_response_with_fill_stays_under_owned_recovery(make_runtime, monkeypatch):
    runtime, request = prepare(make_runtime, monkeypatch)
    runtime.broker.partial = True
    original = runtime.broker.submit_limit_order

    def rejected_remainder(order, price):
        result = original(order, price)
        if order.side.value == "buy":
            result = result.model_copy(update={"status": AlpacaOrderStatus.REJECTED})
            runtime.broker.values[order.client_order_id] = result
        return result

    runtime.broker.submit_limit_order = rejected_remainder
    assert tick(runtime, AT)["operator_paper"]["_operator_active"]
    assert runtime.repo.get_control(f"{request.cohort_id}:operator-terminal") is None
    assert tick(runtime, AT + timedelta(seconds=2))["operator_paper"]["state"] == "completed_flat"
    assert runtime.broker.submissions == 2 and not runtime.broker.positions()


def test_terminal_cleanup_preserves_a_newer_command_and_all_old_records(make_runtime, monkeypatch):
    runtime, request = prepare(make_runtime, monkeypatch)
    tick(runtime, AT)
    nonce = uuid4()
    newer = request.model_copy(
        update={
            "request_id": nonce,
            "control_versions": control_versions(
                runtime.repo, runtime.settings.cohort_id, "operator-paper-" + nonce.hex
            ),
        }
    )
    pointer = f"operator-paper-request:{runtime.settings.cohort_id}"
    runtime.repo.set_control(pointer, newer.model_dump_json())
    tick(runtime, AT + timedelta(seconds=61))
    assert runtime.repo.get_control(pointer) == newer.model_dump_json()
    assert runtime.repo.get_control("operator-paper-active") is None
    assert runtime.repo.get_control(f"{request.cohort_id}:operator-scope") is not None
    assert len(runtime.store.linked_orders(request.cohort_id)) == 2


@pytest.mark.parametrize("changed", ["kill", "host_pause", "operator_pause", "lease"])
def test_final_database_fence_sees_changes_during_last_broker_call(
    make_runtime, monkeypatch, changed
):
    runtime, request = prepare(make_runtime, monkeypatch)
    original = runtime.broker.clock
    calls = 0

    def last_read():
        nonlocal calls
        calls += 1
        if calls == 2:
            if changed == "lease":
                with runtime.store.database.begin() as connection:
                    connection.execute(update(worker_locks).values(owner_id="new-owner"))
            else:
                key = {
                    "kill": "kill_switch",
                    "host_pause": f"{runtime.settings.cohort_id}:pause",
                    "operator_pause": f"{request.cohort_id}:pause",
                }[changed]
                runtime.repo.set_control(key, runtime.repo.get_control(key) or "")
        return original()

    runtime.broker.clock = last_read
    result = tick(runtime, AT)["operator_paper"]
    assert calls == 2 and runtime.broker.submissions == 0
    assert result["result"]["reasons"] == ["OPERATOR_FINAL_FENCE_FAILED"]


def test_final_control_io_cannot_age_quote_past_its_limit(make_runtime, monkeypatch):
    runtime, _ = prepare(make_runtime, monkeypatch)
    fence = operator_calibration.final_entry_fence

    def slow_fence(*args, **kwargs):
        permitted = fence(*args, **kwargs)
        Clock.current = AT + timedelta(seconds=6)
        return permitted

    monkeypatch.setattr(operator_calibration, "final_entry_fence", slow_fence)
    result = tick(runtime, AT)["operator_paper"]
    assert result["result"]["reasons"] == ["OPERATOR_FINAL_FENCE_FAILED"]
    assert runtime.broker.submissions == 0


def test_new_emergency_kill_does_not_disable_owned_risk_exit(make_runtime, monkeypatch):
    runtime, request = prepare(make_runtime, monkeypatch)
    tick(runtime, AT)
    runtime.repo.set_control("kill_switch", "active")
    expected = control_versions(runtime.repo, runtime.settings.cohort_id)
    result = tick(runtime, AT + timedelta(seconds=2))["operator_paper"]
    assert result["state"] == "completed_flat"
    assert runtime.broker.submissions == 2 and not runtime.broker.positions()
    assert control_versions(runtime.repo, runtime.settings.cohort_id) == expected
    assert result["timing"]["flat_verified_at"] == (AT + timedelta(seconds=2)).isoformat()
    assert not result["timing"]["timing_guaranteed"]
    assert runtime.repo.get_control(f"{request.cohort_id}:operator-scope")


def test_account_switch_after_history_cannot_mint_authority_for_another_account(
    make_runtime, monkeypatch
):
    runtime, request = prepare(make_runtime, monkeypatch)
    account = runtime.broker.account
    snapshot = runtime.broker.account_history
    switched = False

    def after_history():
        nonlocal switched
        result = snapshot()
        switched = True
        return result

    runtime.broker.account_history = after_history
    runtime.broker.account = lambda: (
        account().model_copy(update={"id": "other-paper-account"}) if switched else account()
    )
    result = tick(runtime, AT)["operator_paper"]
    assert result["state"] == "blocked_requires_review"
    assert result["reason"] == "operator account changed after verified history"
    assert runtime.broker.submissions == 0
    assert not runtime.store.linked_orders(request.cohort_id)
    assert runtime.repo.get_control(f"{request.cohort_id}:broker-account") is None


def test_final_dispatch_account_pin_does_not_trust_a_changed_cohort_account_record(
    make_runtime, monkeypatch
):
    runtime, request = prepare(make_runtime, monkeypatch)
    account = runtime.broker.account
    find = runtime.broker.find_order_by_client_id
    switched = False

    def before_dispatch(client_id):
        nonlocal switched
        switched = True
        runtime.repo.set_control(
            f"{request.cohort_id}:broker-account", sha256(b"other-paper-account").hexdigest()
        )
        return find(client_id)

    runtime.broker.find_order_by_client_id = before_dispatch
    runtime.broker.account = lambda: (
        account().model_copy(update={"id": "other-paper-account"}) if switched else account()
    )
    result = tick(runtime, AT)["operator_paper"]
    assert result["state"] == "blocked_requires_review"
    assert result["reason"] == "account changed before dispatch"
    assert runtime.broker.submissions == 0
    rows = runtime.store.linked_orders(request.cohort_id)
    assert len(rows) == 1 and rows[0]["filled_quantity"] == 0
    assert not rows[0]["link"].get("submission_attempted_at")


@pytest.mark.parametrize("malformed", ["{}", "not-json", ""])
def test_malformed_active_command_does_not_disable_owned_recovery_or_lease(
    make_runtime, monkeypatch, malformed
):
    runtime, request = prepare(make_runtime, monkeypatch)
    tick(runtime, AT)
    original_scope = runtime.repo.get_control(f"{request.cohort_id}:operator-scope")
    original_pauses = control_versions(runtime.repo, runtime.settings.cohort_id)
    owned = runtime.broker.positions()[0].quantity
    runtime.repo.set_control("operator-paper-active", malformed)
    result = tick(runtime, AT + timedelta(seconds=61))["operator_paper"]
    assert result["state"] == "operator_command_invalid_requires_review"
    assert runtime.broker.submissions == 2
    assert not runtime.broker.positions() and not runtime.broker.open_orders()
    rows = runtime.store.linked_orders(request.cohort_id)
    assert rows[-1]["side"] == "sell" and rows[-1]["quantity"] == owned
    assert runtime.repo.get_control("operator-paper-active") == malformed
    assert runtime.repo.get_control(f"{request.cohort_id}:operator-scope") == original_scope
    assert control_versions(runtime.repo, runtime.settings.cohort_id) == original_pauses
    with runtime.store.database.begin() as connection:
        renewed = connection.scalar(select(worker_locks.c.acquired_at))
        messages = list(connection.execute(select(notification_outbox)).mappings())
    assert renewed.replace(tzinfo=UTC) >= AT + timedelta(seconds=61)
    tick(runtime, AT + timedelta(seconds=62))
    assert runtime.broker.submissions == 2
    with runtime.store.database.begin() as connection:
        assert len(list(connection.execute(select(notification_outbox)))) == len(messages)
    assert "broker-flat" in next(
        row["payload"]["text"]
        for row in messages
        if row["payload"].get("subject", "").endswith("Invalid command; review required")
    )


def test_valid_active_request_is_supervised_once_without_premature_exit(make_runtime, monkeypatch):
    runtime, request = prepare(make_runtime, monkeypatch)
    tick(runtime, AT)
    assert tick(runtime, AT + timedelta(seconds=10))["operator_paper"]["state"] == "supervising"
    assert runtime.broker.submissions == 1 and runtime.broker.positions()
    assert tick(runtime, AT + timedelta(seconds=61))["operator_paper"]["state"] == "completed_flat"
    assert runtime.broker.submissions == 2
    assert len(runtime.store.linked_orders(request.cohort_id)) == 2


def test_worker_manifest_pins_operator_policy_and_history_modules(make_runtime):
    from tradeagent import event_runtime

    runtime = make_runtime()
    _, manifest = event_runtime.cohort_manifest(runtime.settings, runtime.code_sha)
    for name in ("operator_calibration.py", "paper_account_history.py"):
        expected = sha256(
            Path(event_runtime.__file__).with_name(name).read_text(encoding="utf-8").encode()
        ).hexdigest()
        assert manifest["runtime_module_hashes"][name] == expected


def test_invalid_pointer_recovery_uses_frozen_settings_not_new_environment(
    make_runtime, monkeypatch
):
    runtime, request = prepare(make_runtime, monkeypatch)
    tick(runtime, AT)
    runtime.repo.set_control("operator-paper-active", "{}")
    monkeypatch.setenv("EVENT_VIRTUAL_EQUITY", "5000")
    result = tick(runtime, AT + timedelta(seconds=61))["operator_paper"]
    assert result["state"] == "operator_command_invalid_requires_review"
    assert runtime.broker.submissions == 2 and not runtime.broker.positions()
    assert len(runtime.store.linked_orders(request.cohort_id)) == 2
