from __future__ import annotations

import argparse
import json
from contextlib import nullcontext
from datetime import timedelta
from decimal import Decimal
from hashlib import sha256

import pytest
from pydantic import ValidationError
from sqlalchemy import select, update
from test_equipment_demo import entry_args
from test_iex_practice import NOW, Clock, Context, tick
from test_iex_practice import make_runtime as make_runtime
from test_tuesday_runtime import add_guidance, recorded

from tradeagent import event_cli, event_news_policy
from tradeagent.alpaca_paper import AlpacaOrderStatus
from tradeagent.event_account_risk import account_risk
from tradeagent.event_order_notifications import enqueue_lifecycle
from tradeagent.event_orders import ExperimentalOrderManager
from tradeagent.event_runtime import EventRuntime, cohort_manifest
from tradeagent.event_store import event_order_links
from tradeagent.experimental_policy import ExperimentalSettings, certificate
from tradeagent.persistence import notification_outbox

AT = NOW + timedelta(minutes=35)
ACCOUNT = sha256(b"paper-fixture").hexdigest()
POLICY = {"entry_policy": "news-paper", "news_account_digest": ACCOUNT}
D = Decimal


def prepare(make, *, authorized=True, **changes):
    runtime = make(confirmed=False, **POLICY, **changes)
    runtime.repo.set_control("kill_switch", "active")
    runtime.repo.set_control(f"{runtime.settings.cohort_id}:pause", "OPERATOR_PAUSE")
    if authorized:
        authorize(runtime)
    runtime.first_bar_receipts[("AAPL", AT.replace(second=0, microsecond=0))] = AT - timedelta(
        seconds=2
    )
    return runtime


def authorize(runtime, at=AT - timedelta(minutes=5)):
    proof = certificate(
        runtime.settings,
        config_hash=runtime.config_hash,
        code_sha=runtime.code_sha,
        account_id="paper-fixture",
        checks={"operator_confirmation": True},
        now=at,
    )
    event_news_policy.activate(
        runtime.store,
        runtime.settings,
        proof,
        "a" * 64,
        event_news_policy.control_state(runtime.store, runtime.settings.cohort_id),
        at,
    )


def messages(runtime):
    with runtime.store.database.begin() as connection:
        return list(connection.execute(select(notification_outbox)).mappings())


@pytest.mark.parametrize(
    "changes",
    [
        {"purpose": "research", "practice_start_date": None},
        {"news_account_digest": None},
        {"max_entries_per_session": 1},
        {"news_account_digest": "no"},
        {"entry_policy": "event-strategy"},
        {"symbols": "SPY"},
        {"weekly_loss_fraction": "0.006"},
    ],
)
def test_news_policy_is_explicit_and_cannot_expand_limits(changes):
    with pytest.raises(ValidationError):
        ExperimentalSettings(
            **{
                "purpose": "iex-practice",
                "practice_start_date": NOW.date(),
                **POLICY,
                **changes,
            },
            _env_file=None,
        )


def test_no_approval_no_entries_and_distinct_frozen_exit_policy(make_runtime):
    runtime = prepare(make_runtime, authorized=False)
    assert tick(runtime, AT)["calibration"]["reasons"] == ["NEWS_AUTHORIZATION_REQUIRED"]
    assert runtime.broker.submissions == 0
    assert runtime.repo.get_control("kill_switch") == "active"
    _, manifest = cohort_manifest(runtime.settings, runtime.code_sha)
    assert manifest["session_protocol_version"] == event_news_policy.PROTOCOL
    assert manifest["protective_exits"]["broker_native"] is False
    assert manifest["maximum_news_entries_per_session"] == 1
    assert manifest["calibration"]["start_minutes_after_open"] == 40


def test_real_rule_fixture_equipment_then_one_news_then_local_profit_email(make_runtime):
    runtime = prepare(make_runtime)
    tick(runtime, AT)
    assert runtime.broker.submissions == 1
    tick(runtime, AT + timedelta(seconds=61))
    event = add_guidance(runtime, received=AT - timedelta(minutes=10))
    tick(runtime, AT + timedelta(seconds=62))
    rows = runtime.store.linked_orders(runtime.settings.cohort_id)
    assert runtime.broker.submissions == 3
    news = rows[-1]
    ticket = news["link"]["decision_ticket"]
    assert event.evidence_id in ticket["evidence_ids"]
    assert ticket["confidence_category"] == "validated_deterministic_rule"
    assert ticket["probability_of_profit"] is None
    assert ticket["expected_net_return_bps"] is None
    assert ticket["facts"] and event_news_policy.valid_ticket(
        runtime.store, runtime.settings.cohort_id, ticket
    )
    assert D(news["link"]["protection"]["stop_price"]) == D("99.5")
    assert D(news["link"]["protection"]["take_profit_price"]) == D(101)
    assert runtime.oms.session_budget()["total_entries_reserved"] == 2
    runtime.market.changes.update(bid=D(101), ask=D("101.01"))
    tick(runtime, AT + timedelta(seconds=90))
    rows = runtime.store.linked_orders(runtime.settings.cohort_id)
    assert rows[-1]["link"]["reason"] == "local_take_profit"
    assert runtime.broker.submissions == 4
    assert runtime.broker.positions() == ()
    assert (
        len([m for m in messages(runtime) if m["notification_type"] == "paper_order_lifecycle"])
        == 4
    )
    tick(runtime, AT + timedelta(seconds=120))
    assert len([m for m in messages(runtime) if m["notification_type"] == "round_trip_closed"]) == 2
    assert runtime.repo.get_control(f"{runtime.settings.cohort_id}:news-terminal")
    assert runtime.repo.get_control("kill_switch") == "active"
    assert runtime.broker.submissions == 4
    assert len(messages(runtime)) == 6


@pytest.mark.parametrize("kind", ["stop", "stale", "missing", "partial", "unknown"])
def test_local_protection_is_durable_and_only_exits_owned_quantity(make_runtime, kind):
    runtime = prepare(make_runtime)
    runtime.broker.partial = kind in {"partial", "unknown"}
    runtime.broker.timeout = kind == "unknown"
    tick(runtime, AT)
    if kind == "unknown":
        runtime.oms.reconcile(AT)
    buy = runtime.store.linked_orders(runtime.settings.cohort_id)[0]
    assert buy["link"]["protection"]["basis"] == "actual_broker_cumulative_filled_vwap"
    runtime.market.changes.update(bid=D("99.4"), ask=D("99.41"))
    if kind == "stale":
        runtime.market.changes["quote_at"] = AT - timedelta(minutes=1)
    elif kind == "missing":
        runtime.oms.quote_provider = None
    Clock.current = runtime.broker.now = AT + timedelta(seconds=10)
    runtime.oms.supervise(Clock.current, feed_healthy=True)
    rows = runtime.store.linked_orders(runtime.settings.cohort_id)
    assert rows[-1]["side"] == "sell"
    assert rows[-1]["quantity"] == rows[0]["filled_quantity"]
    assert rows[-1]["link"]["reason"] == (
        "local_protection_unavailable_risk_exit"
        if kind in {"stale", "missing"}
        else "local_stop_loss"
    )
    assert runtime.broker.positions() == ()
    assert runtime.broker.open_orders() == ()
    assert recorded(runtime, "protection_trigger")


def test_restart_preserves_protective_latch_and_original_unknown_id(make_runtime):
    runtime = prepare(make_runtime)
    runtime.broker.timeout = True
    runtime.broker.partial = True
    tick(runtime, AT)
    restarted = EventRuntime(
        runtime.store,
        runtime.settings,
        runtime.source,
        runtime.market,
        runtime.broker,
        instance_id="fixture",
        code_sha=runtime.code_sha,
    )
    restarted.context_client.close()
    restarted.context_client = Context()
    tick(restarted, AT + timedelta(seconds=31))
    assert runtime.broker.submissions == 2
    assert runtime.broker.positions() == ()
    assert runtime.oms.session_budget()["total_entries_reserved"] == 1
    assert (
        len(
            [
                row
                for row in runtime.store.linked_orders(runtime.settings.cohort_id)
                if row["side"] == "buy"
            ]
        )
        == 1
    )


@pytest.mark.parametrize(
    "field,value",
    [
        ("code_sha", "different"),
        ("config_hash", "different"),
        ("account_digest", "f" * 64),
        ("cohort_id", "other"),
        ("maximum_entries", 5),
        ("maximum_notional", "200"),
        ("incident_acceptance_passed", False),
        ("issued_at", NOW.isoformat()),
        ("acceptance_sha256", "fake"),
        ("issued_at", None),
    ],
)
def test_scoped_news_authority_cannot_be_repurposed(make_runtime, field, value):
    runtime = prepare(make_runtime)
    key = f"{runtime.settings.cohort_id}:news-authorization"
    authority = json.loads(runtime.repo.get_control(key))
    authority[field] = value
    runtime.repo.set_control(key, json.dumps(authority))
    assert tick(runtime, AT)["calibration"]["reasons"] == ["NEWS_AUTHORIZATION_REQUIRED"]
    assert runtime.broker.submissions == 0


@pytest.mark.parametrize("change", ["new_pause", "global_kill", "expired", "early", "late"])
def test_activation_rechecks_compare_and_set_and_time(make_runtime, change):
    runtime = prepare(make_runtime, authorized=False)
    expected = event_news_policy.control_state(runtime.store, runtime.settings.cohort_id)
    at = AT - timedelta(minutes=5)
    proof = entry_args(runtime, at)["certificate"]
    if change == "new_pause":
        runtime.repo.set_control(f"{runtime.settings.cohort_id}:pause", "R1_NEW_RISK")
    elif change == "global_kill":
        runtime.repo.set_control("kill_switch", "active")
    elif change == "expired":
        proof = proof.model_copy(update={"expires_at": at})
    elif change == "early":
        at = NOW
    else:
        at = AT.replace(minute=30)
    with pytest.raises(ValueError):
        event_news_policy.activate(runtime.store, runtime.settings, proof, "a" * 64, expected, at)
    assert runtime.repo.get_control(f"{runtime.settings.cohort_id}:news-authorization") is None
    assert runtime.repo.get_control("kill_switch") == "active"


@pytest.mark.parametrize("boundary", ["quote", "end"])
def test_slow_news_authorization_cannot_skip_final_timing_gate(make_runtime, monkeypatch, boundary):
    runtime = prepare(make_runtime)
    at = AT if boundary == "quote" else AT.replace(minute=29, second=59)
    runtime.broker.now = at
    args = entry_args(runtime, at)
    if boundary == "quote":
        old = at - timedelta(seconds=4)
        args.update(
            eligible_at=old - timedelta(seconds=1),
            quote_at=old,
            execution_quote=args["execution_quote"].model_copy(
                update={"timestamp": old, "received_at": old}
            ),
        )
    original = event_news_policy.authorized
    calls = 0

    def delayed(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            runtime.broker.now += timedelta(seconds=2)
        return original(*args, **kwargs)

    monkeypatch.setattr(event_news_policy, "authorized", delayed)
    assert runtime.oms.submit_entry(**args)["state"] == "expired"
    assert runtime.broker.submissions == 0
    assert runtime.oms.session_budget()["total_entries_reserved"] == 0


def test_oms_rejects_job_entries_and_unbacked_news_tickets(make_runtime):
    runtime = prepare(make_runtime)
    args = entry_args(runtime, AT)
    runtime.broker.now = AT
    assert runtime.oms.submit_entry(**{**args, "entry_kind": "strategy"})["reasons"] == [
        "EQUIPMENT_AND_VALID_NEWS_REQUIRED"
    ]
    manager = ExperimentalOrderManager(
        runtime.store,
        runtime.broker,
        runtime.settings,
        runtime.app,
        runtime.config_hash,
        runtime.code_sha,
    )
    assert manager.submit_entry(**args)["reasons"] == ["NEWS_AUTHORIZATION_REQUIRED"]
    assert not event_news_policy.valid_ticket(runtime.store, runtime.settings.cohort_id, {})


def test_terminal_no_equipment_after_cutoff_preserves_stricter_pause(make_runtime):
    runtime = prepare(make_runtime, authorized=False)
    runtime.repo.set_control(f"{runtime.settings.cohort_id}:pause", "R1_UNREVIEWED")
    runtime.broker.now = AT.replace(minute=30)
    runtime.oms.supervise(runtime.broker.now, feed_healthy=True)
    runtime.oms.supervise(runtime.broker.now, feed_healthy=True)
    assert len(recorded(runtime, "news_terminal")) == 1
    assert runtime.repo.get_control(f"{runtime.settings.cohort_id}:pause") == "R1_UNREVIEWED"
    assert runtime.broker.submissions == 0


def test_stop_waits_for_confirmed_cancel_and_survives_rebound_restart(make_runtime):
    runtime = prepare(make_runtime)
    runtime.broker.partial = True
    tick(runtime, AT)
    original_cancel = runtime.broker.cancel_order
    runtime.broker.cancel_order = lambda order_id: None
    runtime.market.changes.update(bid=D("99.4"), ask=D("99.41"))
    Clock.current = runtime.broker.now = AT + timedelta(seconds=10)
    runtime.oms.supervise(Clock.current, feed_healthy=True)
    assert runtime.broker.submissions == 1
    buy = runtime.store.linked_orders(runtime.settings.cohort_id)[0]
    assert buy["link"]["protection_trigger"]["reason"] == "local_stop_loss"
    original_cancel(buy["broker_order_id"])
    runtime.market.changes.update(bid=D("99.99"), ask=D(100))
    restarted = EventRuntime(
        runtime.store,
        runtime.settings,
        runtime.source,
        runtime.market,
        runtime.broker,
        instance_id="fixture",
        code_sha=runtime.code_sha,
    )
    restarted.context_client.close()
    Clock.current = runtime.broker.now = AT + timedelta(seconds=20)
    restarted.oms.supervise(Clock.current, feed_healthy=True)
    rows = runtime.store.linked_orders(runtime.settings.cohort_id)
    assert runtime.broker.submissions == 2
    assert rows[-1]["quantity"] == rows[0]["filled_quantity"]
    assert rows[-1]["link"]["reason"] == "local_stop_loss"
    assert runtime.broker.positions() == ()


def test_slow_exit_preparation_cannot_queue_an_afterhours_sell(make_runtime):
    runtime = prepare(make_runtime)
    tick(runtime, AT)
    late = AT.replace(hour=19, minute=59, second=59)
    Clock.current = runtime.broker.now = late
    original_clock = runtime.broker.clock
    calls = 0

    def delayed_clock():
        nonlocal calls
        calls += 1
        if calls == 3:
            runtime.broker.now += timedelta(seconds=2)
        return original_clock()

    runtime.broker.clock = delayed_clock
    runtime.oms.supervise(late, feed_healthy=True)
    assert calls == 3
    rows = runtime.store.linked_orders(runtime.settings.cohort_id)
    assert rows[-1]["side"] == "sell" and rows[-1]["status"] == "expired"
    assert runtime.broker.submissions == 1
    assert recorded(runtime, "exit_expired_unsent")


def test_lifecycle_duplicate_partial_stale_fill_and_http_rejection(make_runtime):
    runtime = prepare(make_runtime)
    runtime.broker.partial = True
    tick(runtime, AT)
    row = runtime.store.linked_orders(runtime.settings.cohort_id)[0]
    order = runtime.broker.values[row["client_order_id"]]
    runtime.oms._observe(row["client_order_id"], order, AT)
    assert len(messages(runtime)) == 1
    full = order.model_copy(
        update={"status": AlpacaOrderStatus.FILLED, "filled_quantity": order.quantity}
    )
    runtime.broker.values[row["client_order_id"]] = full
    runtime.oms._observe(row["client_order_id"], full, AT)
    runtime.oms._observe(row["client_order_id"], order, AT)
    assert len(messages(runtime)) == 2
    with runtime.store.database.begin() as connection:
        for _ in range(2):
            for stage, link in (
                ("acknowledged", {"broker": order.model_dump(mode="json")}),
                ("rejected", {"rejection": {"http_status": 422}}),
                ("created", {}),
            ):
                enqueue_lifecycle(
                    connection,
                    cohort=runtime.settings.cohort_id,
                    client_id="different",
                    symbol="AAPL",
                    side="buy",
                    status=stage,
                    link=link,
                    now=AT,
                )
    assert len(messages(runtime)) == 4
    assert all(m["cycle_id"] is None for m in messages(runtime))


def test_account_weekly_loss_survives_cohort_and_day_change(make_runtime):
    runtime = prepare(
        make_runtime,
        virtual_equity=D(2000),
        daily_loss_fraction=D("0.00025"),
        weekly_loss_fraction=D("0.00025"),
    )
    tick(runtime, AT)
    tick(runtime, AT + timedelta(seconds=61))
    rows = runtime.store.linked_orders(runtime.settings.cohort_id)
    sell = rows[-1]
    link = dict(sell["link"])
    link["broker"] = {**link["broker"], "filled_average_price": "90"}
    runtime.store.update_link(sell["client_order_id"], link)
    tomorrow = AT + timedelta(days=1)
    settings = runtime.settings.model_copy(
        update={"cohort_id": "new-policy-cohort", "practice_start_date": tomorrow.date()}
    )
    report = account_risk(runtime.store, settings, {}, tomorrow)
    assert report["blocked"]
    assert D(report["day_start_equity"]) == D(report["economic_equity"])
    assert D(report["week_start_equity"]) > D(report["economic_equity"])
    assert D(report["weekly_limit"]) == D("0.5")
    larger = settings.model_copy(update={"virtual_equity": D(10000)})
    report = account_risk(runtime.store, larger, {}, tomorrow)
    assert report["capital"] == "2000"
    assert D(report["weekly_limit"]) == D("0.5")
    assert report["blocked"]
    with runtime.store.database.begin() as connection:
        link["broker"]["filled_average_price"] = None
        connection.execute(
            update(event_order_links)
            .where(event_order_links.c.client_order_id == sell["client_order_id"])
            .values(payload=link)
        )
    assert account_risk(runtime.store, settings, {}, tomorrow)["state"] == "unvalued"


@pytest.mark.parametrize("confirmed", [True, False])
def test_cli_requires_explicit_news_confirmation_after_incident(
    make_runtime, monkeypatch, capsys, confirmed
):
    runtime = prepare(make_runtime, authorized=False)
    tick(runtime, AT - timedelta(minutes=5))
    monkeypatch.setattr(event_cli, "datetime", Clock)
    monkeypatch.setenv("TRADEAGENT_DATABASE_URL", str(runtime.store.database.engine.url))
    monkeypatch.setenv("EVENT_SYMBOLS", "AAPL")
    diagnostics = {
        "broker_healthy": True,
        "live_credential_environment_present": False,
        "broker_positions": 0,
        "broker_open_orders": 0,
        "market_data": {"iex_latest_quote": {"accessible": True}},
        "active_execution_feed": "iex",
        "assets": [{"fractionable": True, "tradable": True}],
    }
    monkeypatch.setattr(event_cli, "source_capabilities", lambda: diagnostics)
    monkeypatch.setattr(event_cli, "code_identity", lambda: runtime.code_sha)
    monkeypatch.setattr(event_cli, "replay_event_pipeline", lambda: {"reconciled": True})
    monkeypatch.setattr(event_cli, "OfficialContextClient", lambda: nullcontext(Context()))
    monkeypatch.setattr(
        "tradeagent.alpaca_paper.AlpacaPaperClient", lambda settings: nullcontext(runtime.broker)
    )
    monkeypatch.setenv("ALPACA_KEY_ID", "fixture")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "fixture")
    args = argparse.Namespace(
        command="paper-preflight",
        output=None,
        cohort_id=runtime.settings.cohort_id,
        purpose="iex-practice",
        practice_start_date=NOW.date(),
        **POLICY,
        confirm_experimental_paper=True,
        confirm_news_paper=confirmed,
        news_acceptance_sha256="a" * 64,
        news_reviewed_code_sha=runtime.code_sha,
    )
    assert event_cli.handle_event_command(args)
    result = json.loads(capsys.readouterr().out)
    assert result["operational_certificate_issued"] is confirmed, result["blockers"]
    assert runtime.repo.get_control("kill_switch") == ("inactive" if confirmed else "active")
    assert (
        bool(runtime.repo.get_control(f"{runtime.settings.cohort_id}:news-authorization"))
        is confirmed
    )
    assert runtime.broker.submissions == 0
