from __future__ import annotations

import argparse
import json
from contextlib import nullcontext
from datetime import timedelta
from hashlib import sha256

import pytest
from pydantic import ValidationError
from test_iex_practice import NOW, Clock, Context, tick
from test_iex_practice import make_runtime as make_runtime

from tradeagent import event_cli
from tradeagent.event_demo import activate_demo, demo_control_state, demo_scope
from tradeagent.event_orders import ExperimentalOrderManager
from tradeagent.event_runtime import EventRuntime, cohort_manifest
from tradeagent.experimental_policy import ExperimentalSettings, certificate

DEMO_AT = NOW + timedelta(minutes=35)
ACCOUNT = sha256(b"paper-fixture").hexdigest()
POLICY = {
    "entry_policy": "equipment-only-demo",
    "demo_account_digest": ACCOUNT,
    "max_entries_per_session": 1,
}


def prepare(make, *, authorized=True):
    runtime = make(**POLICY)
    runtime.repo.set_control("kill_switch", "active")
    runtime.repo.set_control(f"{runtime.settings.cohort_id}:pause", "OPERATOR_PAUSE")
    if authorized:
        authorize(runtime)
    bar_at = DEMO_AT.replace(second=0, microsecond=0)
    runtime.first_bar_receipts[("AAPL", bar_at)] = DEMO_AT - timedelta(seconds=2)
    return runtime


def authorize(runtime):
    at = DEMO_AT - timedelta(minutes=5)
    proof = certificate(
        runtime.settings,
        config_hash=runtime.config_hash,
        code_sha=runtime.code_sha,
        account_id=runtime.broker.account().id,
        checks={"operator_confirmation": True},
        now=at,
    )
    activate_demo(
        runtime.store,
        runtime.settings,
        proof,
        "a" * 64,
        demo_control_state(runtime.store, runtime.settings.cohort_id),
        at,
    )


def entry_args(runtime, at=DEMO_AT):
    market = runtime.market.state("AAPL", at)
    from tradeagent.event_runtime import _execution_quote

    return {
        "symbol": "AAPL",
        "cluster_key": f"opening-calibration:{at.date()}:AAPL",
        "decision_id": "demo-decision",
        "eligible_at": at - timedelta(seconds=1),
        "expires_at": at + timedelta(seconds=30),
        "bid": market.bid,
        "ask": market.ask,
        "quote_at": market.quote_at,
        "median_dollar_volume": market.median_daily_dollar_volume,
        "source_valid": True,
        "certificate": certificate(
            runtime.settings,
            config_hash=runtime.config_hash,
            code_sha=runtime.code_sha,
            account_id="paper-fixture",
            checks={"operator_confirmation": True},
            now=at,
        ),
        "now": at,
        "entry_kind": "calibration",
        "execution_quote": _execution_quote(market),
    }


@pytest.mark.parametrize(
    "change",
    [
        {"purpose": "research", "practice_start_date": None},
        {"symbols": "AAPL,MSFT"},
        {"max_entries_per_session": 2},
        {"demo_account_digest": None},
        {"demo_account_digest": "wrong"},
        {"entry_policy": "event-strategy"},
    ],
)
def test_demo_configuration_is_explicit_and_narrow(change):
    with pytest.raises(ValidationError):
        ExperimentalSettings(
            **{
                "purpose": "iex-practice",
                "practice_start_date": NOW.date(),
                "symbols": "AAPL",
                **POLICY,
                **change,
            },
            _env_file=None,
        )


def test_demo_preserves_pauses_and_delays_until_declared_window(make_runtime):
    runtime = prepare(make_runtime, authorized=False)
    assert tick(runtime)["calibration"]["state"] == "waiting_for_declared_demo_window"
    assert tick(runtime, DEMO_AT)["calibration"]["reasons"] == ["DEMO_AUTHORIZATION_REQUIRED"]
    assert runtime.broker.submissions == 0
    assert runtime.repo.get_control("kill_switch") == "active"
    assert runtime.repo.get_control(f"{runtime.settings.cohort_id}:pause") == "OPERATOR_PAUSE"
    _, manifest = cohort_manifest(runtime.settings, runtime.code_sha)
    assert manifest["session_protocol_version"] == "manual-paper-demo-v1"
    assert manifest["hypotheses"] == []
    assert manifest["maximum_news_entries_per_session"] == 0
    assert manifest["calibration"]["start_minutes_after_open"] == 40


@pytest.mark.parametrize("partial,unknown", [(False, False), (True, False), (True, True)])
def test_demo_one_entry_recovers_then_flattens_and_cannot_repeat(make_runtime, partial, unknown):
    runtime = prepare(make_runtime)
    runtime.broker.partial = partial
    runtime.broker.timeout = unknown
    first = tick(runtime, DEMO_AT)
    assert first["entry_policy"] == "equipment-only-demo"
    assert runtime.broker.submissions == 1
    buy = runtime.store.linked_orders(runtime.settings.cohort_id)[0]
    assert buy["quantity"] * 100 <= 25
    assert (
        buy["link"]["equipment_test_id"]
        == demo_scope(runtime.settings, runtime.config_hash, runtime.code_sha)["equipment_test_id"]
    )
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
    tick(restarted, DEMO_AT + timedelta(seconds=31))
    tick(restarted, DEMO_AT + timedelta(seconds=61))
    tick(restarted, DEMO_AT + timedelta(seconds=90))
    assert runtime.broker.positions() == ()
    assert runtime.broker.open_orders() == ()
    assert runtime.broker.submissions == 2
    rows = runtime.store.linked_orders(runtime.settings.cohort_id)
    assert rows[1]["quantity"] == rows[0]["filled_quantity"]
    assert runtime.repo.get_control(f"{runtime.settings.cohort_id}:demo-terminal")
    assert runtime.repo.get_control("kill_switch") == "active"
    assert restarted.oms.valuation({}, DEMO_AT)["qualifying_closed_round_trips"] == 0
    tick(restarted, DEMO_AT + timedelta(minutes=3))
    assert runtime.broker.submissions == 2


@pytest.mark.parametrize(
    "field,value",
    [
        ("code_sha", "other"),
        ("config_hash", "other"),
        ("account_digest", "f" * 64),
        ("cohort_id", "other"),
        ("session_date", "2026-09-09"),
        ("equipment_test_id", "other"),
        ("incident_acceptance_passed", False),
        ("acceptance_sha256", "not-evidence"),
        ("issued_at", NOW.isoformat()),
        ("issued_at", None),
    ],
)
def test_demo_authority_cannot_be_reused_outside_exact_scope(make_runtime, field, value):
    runtime = prepare(make_runtime)
    key = f"{runtime.settings.cohort_id}:demo-authorization"
    authority = json.loads(runtime.repo.get_control(key))
    authority[field] = value
    runtime.repo.set_control(key, json.dumps(authority))
    result = tick(runtime, DEMO_AT)
    assert result["calibration"]["reasons"] == ["DEMO_AUTHORIZATION_REQUIRED"]
    assert runtime.broker.submissions == 0


def test_demo_oms_blocks_news_and_lease_free_jobs(make_runtime):
    runtime = prepare(make_runtime)
    args = entry_args(runtime)
    assert runtime.oms.submit_entry(**{**args, "entry_kind": "strategy"})["reasons"] == [
        "DEMO_NO_STRATEGY_ENTRIES"
    ]
    assert runtime._entry(None, None, DEMO_AT)["reasons"] == ["DEMO_NO_STRATEGY_ENTRIES"]
    local = ExperimentalOrderManager(
        runtime.store,
        runtime.broker,
        runtime.settings,
        runtime.app,
        runtime.config_hash,
        runtime.code_sha,
    )
    assert local.submit_entry(**args)["reasons"] == ["DEMO_AUTHORIZATION_REQUIRED"]
    assert runtime.broker.submissions == 0


@pytest.mark.parametrize("failure", ["stale", "spread", "paused", "reserved", "afterhours"])
def test_demo_retains_order_risk_gates(make_runtime, failure):
    runtime = prepare(make_runtime)
    runtime.broker.now = DEMO_AT
    args = entry_args(runtime)
    if failure == "stale":
        args["quote_at"] -= timedelta(seconds=6)
    elif failure == "spread":
        args["ask"] += 1
    elif failure == "paused":
        runtime.repo.set_control("kill_switch", "active")
    elif failure == "reserved":
        original = runtime.broker.open_orders
        runtime.broker.open_orders = lambda: ("unowned-pending",)
        # Exercise the atomic shared-account slot check after healthy reconciliation.
        runtime.oms.reconcile = lambda now: {"healthy": True}
        runtime.broker.open_orders_original = original
    else:
        args = entry_args(runtime, NOW.replace(hour=23))
        runtime.broker.now = args["now"]
    result = runtime.oms.submit_entry(**args)
    assert result["state"] == "risk_rejected"
    assert runtime.broker.submissions == 0


def test_demo_terminal_cutoff_does_not_depend_on_news_ingestion(make_runtime):
    runtime = prepare(make_runtime, authorized=False)
    at = NOW.replace(hour=14, minute=30)
    runtime.broker.now = at
    runtime.oms.supervise(at, feed_healthy=False)
    terminal = runtime.repo.get_control(f"{runtime.settings.cohort_id}:demo-terminal")
    assert terminal
    runtime.oms.supervise(at + timedelta(days=1), feed_healthy=False)
    assert runtime.repo.get_control(f"{runtime.settings.cohort_id}:demo-terminal") == terminal
    assert runtime.repo.get_control("kill_switch") == "active"
    with pytest.raises(ValueError, match="activation refused"):
        authorize(runtime)


def test_demo_activation_does_not_clear_concurrently_changed_pause(make_runtime):
    runtime = prepare(make_runtime, authorized=False)
    expected = demo_control_state(runtime.store, runtime.settings.cohort_id)
    runtime.repo.set_control(f"{runtime.settings.cohort_id}:pause", "RECONCILIATION_REQUIRED")
    proof = entry_args(runtime)["certificate"]
    with pytest.raises(ValueError, match="activation refused"):
        activate_demo(runtime.store, runtime.settings, proof, "a" * 64, expected, DEMO_AT)
    assert runtime.repo.get_control("kill_switch") == "active"
    assert runtime.repo.get_control(f"{runtime.settings.cohort_id}:demo-authorization") is None


@pytest.mark.parametrize("boundary", ["window_end", "quote_age"])
def test_slow_demo_authority_is_rechecked_by_final_broker_clock(
    make_runtime, monkeypatch, boundary
):
    from tradeagent import event_orders

    runtime = prepare(make_runtime)
    at = DEMO_AT if boundary == "quote_age" else DEMO_AT.replace(minute=29, second=59)
    runtime.broker.now = at
    args = entry_args(runtime, at)
    if boundary == "quote_age":
        quote_at = at - timedelta(seconds=4)
        args.update(
            eligible_at=quote_at - timedelta(seconds=1),
            quote_at=quote_at,
            execution_quote=args["execution_quote"].model_copy(
                update={"timestamp": quote_at, "received_at": quote_at}
            ),
        )
    else:
        args["expires_at"] = at + timedelta(seconds=1)
    original = event_orders.demo_authorized
    calls = 0

    def slow_authority(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            runtime.broker.now += timedelta(seconds=2)
        return original(*args, **kwargs)

    monkeypatch.setattr(event_orders, "demo_authorized", slow_authority)
    result = runtime.oms.submit_entry(**args)
    assert calls == 2
    assert result["state"] == "expired"
    assert result["reasons"] == ["SUBMISSION_REVALIDATION_FAILED"]
    assert runtime.broker.submissions == 0
    row = runtime.store.linked_orders(runtime.settings.cohort_id)[0]
    assert row["status"] == "expired"
    assert row["link"]["definitively_unsent_at"]
    assert row["link"].get("submission_attempted_at") is None
    budget = runtime.oms.session_budget()
    assert budget["total_entries_reserved"] == 0
    assert budget["equipment_client_order_id"] == row["client_order_id"]


@pytest.mark.parametrize("confirmed", [True, False])
def test_demo_cli_requires_separate_post_acceptance_confirmation(
    make_runtime, monkeypatch, capsys, confirmed
):
    runtime = prepare(make_runtime, authorized=False)
    at = DEMO_AT - timedelta(minutes=5)
    tick(runtime, at)
    monkeypatch.setattr(event_cli, "datetime", Clock)
    monkeypatch.setenv("TRADEAGENT_DATABASE_URL", str(runtime.store.database.engine.url))
    monkeypatch.setenv("ALPACA_KEY_ID", "synthetic-key")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "synthetic-secret")
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
    args = argparse.Namespace(
        command="paper-preflight",
        output=None,
        cohort_id=runtime.settings.cohort_id,
        purpose="iex-practice",
        practice_start_date=NOW.date(),
        **POLICY,
        confirm_experimental_paper=True,
        confirm_equipment_only_demo=confirmed,
        demo_acceptance_sha256="a" * 64,
        demo_reviewed_code_sha=runtime.code_sha,
    )
    assert event_cli.handle_event_command(args)
    result = json.loads(capsys.readouterr().out)
    assert result["operational_certificate_issued"] is confirmed, result["blockers"]
    assert runtime.repo.get_control("kill_switch") == ("inactive" if confirmed else "active")
    assert (
        bool(runtime.repo.get_control(f"{runtime.settings.cohort_id}:demo-authorization"))
        is confirmed
    )
    assert runtime.broker.submissions == 0
