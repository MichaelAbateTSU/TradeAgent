from __future__ import annotations

import argparse
import json
from contextlib import ExitStack, nullcontext
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import ClassVar

import httpx
import pytest
import test_event_orders as order_fixtures
from pydantic import ValidationError
from sqlalchemy import select

from tradeagent import event_cli
from tradeagent.event_context import HaltStatus, OfficialContextSnapshot
from tradeagent.event_market import EventMarketState
from tradeagent.event_research import DEFAULT_EVENT_POLICY, EventQuote
from tradeagent.event_runtime import EventRuntime, cohort_manifest, policy_for
from tradeagent.event_store import EventStore, event_order_links
from tradeagent.experimental_policy import ExperimentalSettings, certificate
from tradeagent.persistence import Database, ProductionRepository

NOW = datetime(2026, 9, 8, 13, 35, tzinfo=UTC)
D = Decimal


class Clock(datetime):
    current = NOW

    @classmethod
    def now(cls, tz=None):
        return cls.current


class Source:
    capabilities: ClassVar = {"sec_enabled": True, "primary_urls_configured": 0}
    last_errors = ()

    def poll(self, **kwargs):
        return ()


class Market:
    def __init__(self):
        self.changes = {}
        self.fail = False
        self.fail_symbols = set()

    def state(self, symbol, now):
        if self.fail or symbol in self.fail_symbols:
            raise httpx.ReadTimeout("synthetic quote outage")
        return EventMarketState(
            **{
                "symbol": symbol,
                "observed_at": now,
                "feed": "iex",
                "bid": D("99.99"),
                "ask": D("100"),
                "bid_size": D(100),
                "ask_size": D(100),
                "quote_at": now,
                "raw_quote": {},
                "completed_bar": None,
                "previous_close": D(100),
                "median_daily_dollar_volume": D("100000000"),
                "pre_event_volatility_bps": D(100),
                **self.changes,
            }
        )

    def refresh_quote(self, state):
        return self.state(state.symbol, Clock.current)


class Context:
    capabilities: ClassVar = {"synthetic": True}

    def __init__(self):
        self.halted = False
        self.scheduled = ()

    def poll(self, **kwargs):
        now = Clock.current
        return OfficialContextSnapshot(
            observed_at=now,
            evidence=(),
            macro_calendar_available_at=now,
            macro_calendar_covers_until=now + timedelta(days=1),
            scheduled_macro_events=self.scheduled,
            halts=(
                HaltStatus(
                    symbol="AAPL",
                    halted=self.halted,
                    available_at=now,
                    valid_until=now + timedelta(minutes=5),
                    reason="synthetic",
                ),
            ),
        )


@pytest.fixture
def make_runtime(tmp_path, monkeypatch):
    def deny_network(*args, **kwargs):
        raise AssertionError("practice tests must never contact a real broker")

    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", deny_network)
    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", deny_network)
    monkeypatch.setattr("tradeagent.event_runtime.datetime", Clock)
    Clock.current = NOW
    for key in ("ALPACA_LIVE_KEY", "ALPACA_LIVE_KEY_ID", "ALPACA_LIVE_SECRET_KEY"):
        monkeypatch.delenv(key, raising=False)
    with ExitStack() as stack:
        count = 0

        def make(*, confirmed=True, **changes):
            nonlocal count
            count += 1
            database = stack.enter_context(Database(f"sqlite:///{tmp_path / f'p{count}.db'}"))
            database.initialize()
            store = EventStore(database)
            settings = ExperimentalSettings(
                **{
                    "mode": "experimental-paper",
                    "purpose": "iex-practice",
                    "practice_start_date": date(2026, 9, 8),
                    "cohort_id": f"practice-{count}",
                    "symbols": "AAPL",
                    **changes,
                },
                _env_file=None,
            )
            repo = ProductionRepository(database)
            repo.acquire_worker_lock("tradeagent-event-worker", "fixture", observed_at=NOW)
            broker = order_fixtures.Broker()
            broker.now = NOW
            runtime = EventRuntime(
                store,
                settings,
                Source(),
                Market(),
                broker,
                instance_id="fixture",
                code_sha="practice-code",
            )
            runtime.context_client.close()
            runtime.context_client = Context()
            repo.set_control("v20:mechanics_attestation", "practice-code")
            if confirmed:
                proof = certificate(
                    settings,
                    config_hash=runtime.config_hash,
                    code_sha=runtime.code_sha,
                    account_id=broker.account().id,
                    checks={"operator_confirmation": True, "paper_host": True},
                    now=NOW - timedelta(days=2),
                )
                repo.set_control(f"{settings.cohort_id}:certificate", proof.model_dump_json())
            return runtime

        yield make


def tick(runtime, at=NOW):
    Clock.current = at
    runtime.broker.now = at
    return runtime.tick(at)


def test_practice_opt_in_does_not_change_frozen_research_policy():
    research = ExperimentalSettings(_env_file=None)
    practice = ExperimentalSettings(
        purpose="iex-practice", practice_start_date=date(2026, 9, 8), _env_file=None
    )
    assert not DEFAULT_EVENT_POLICY.allow_iex_experimental_paper
    assert not policy_for(research).allow_iex_experimental_paper
    assert policy_for(practice).allow_iex_experimental_paper
    old_digest, old = cohort_manifest(research, "same-code")
    new_digest, new = cohort_manifest(practice, "same-code")
    assert old_digest != new_digest
    assert old["execution_feed"] == "sip"
    assert new["execution_feed"] == "iex"
    assert new["qualification_eligible"] is False
    assert new["evidence_use"] == "operational_practice_only"
    assert new["calibration"]["maximum_entry_attempts"] == 1


@pytest.mark.parametrize(
    "changes",
    [
        {"purpose": "iex-practice"},
        {"practice_start_date": date(2026, 9, 8)},
        {
            "purpose": "iex-practice",
            "practice_start_date": date(2026, 9, 8),
            "symbols": "MSFT",
        },
        {"purpose": "delayed-sip"},
    ],
)
def test_invalid_practice_configuration_fails_closed(changes):
    with pytest.raises(ValidationError):
        ExperimentalSettings(**changes, _env_file=None)


def test_opening_calibration_is_durable_small_and_exits_after_one_minute(make_runtime):
    runtime = make_runtime()
    first = tick(runtime)
    assert first["purpose"] == "iex-practice"
    assert first["qualification_eligible"] is False
    assert first["calibration"]["state"] == "filled"
    assert runtime.broker.submissions == 1
    bought = runtime.store.linked_orders(runtime.settings.cohort_id)[0]
    assert bought["link"]["entry_kind"] == "calibration"
    assert D(bought["quantity"]) * D(bought["link"]["limit_price"]) <= 25
    assert datetime.fromisoformat(bought["link"]["exit_at"]) == NOW + timedelta(seconds=60)
    tick(runtime, NOW + timedelta(seconds=30))
    assert runtime.broker.submissions == 1
    last = tick(runtime, NOW + timedelta(seconds=61))
    assert runtime.broker.submissions == 2
    assert runtime.broker.positions() == ()
    assert last["calibration"]["state"] == "completed_round_trip"
    assert (
        runtime.oms.valuation({}, NOW + timedelta(seconds=61))["qualifying_closed_round_trips"] == 0
    )
    tick(runtime, NOW + timedelta(minutes=2))
    assert runtime.broker.submissions == 2


def test_restart_does_not_repeat_calibration(make_runtime):
    runtime = make_runtime()
    tick(runtime)
    tick(runtime, NOW + timedelta(seconds=61))
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
    assert tick(restarted, NOW + timedelta(minutes=2))["calibration"]["state"] == (
        "completed_round_trip"
    )
    assert runtime.broker.submissions == 2


def test_exit_identifiers_fit_production_postgres_columns(make_runtime):
    runtime = make_runtime(cohort_id="v20-iex-practice-20260908")
    tick(runtime)
    tick(runtime, NOW + timedelta(seconds=61))
    rows = runtime.store.linked_orders(runtime.settings.cohort_id)
    assert len(rows) == 2
    with runtime.store.database.begin() as connection:
        keys = list(connection.scalars(select(event_order_links.c.cluster_key)))
    assert len(keys) == 2
    assert all(len(key) <= event_order_links.c.cluster_key.type.length for key in keys)
    assert all(len(row["client_order_id"]) <= 64 for row in rows)
    assert runtime.broker.positions() == ()


def test_denied_preflight_rechecks_after_transient_other_symbol_failure(make_runtime):
    runtime = make_runtime(symbols="AAPL,MSFT")
    runtime.market.fail_symbols.add("MSFT")
    first = tick(runtime)
    assert first["calibration"]["state"] == "blocked"
    assert first["operational_certificate"]["checks"]["frozen_policy_market_feed"] is False
    assert runtime.broker.submissions == 0
    runtime.market.fail_symbols.clear()
    recovered = tick(runtime, NOW + timedelta(seconds=30))
    assert recovered["calibration"]["state"] == "filled"
    assert recovered["operational_certificate"]["permits_paper"] is True
    assert runtime.broker.submissions == 1


@pytest.mark.parametrize("boundary", ["calibration_window", "quote_age"])
def test_final_dispatch_rechecks_window_and_quote_after_lookup(make_runtime, monkeypatch, boundary):
    runtime = make_runtime()
    at = datetime(2026, 9, 8, 13, 59, 59, tzinfo=UTC) if boundary == "calibration_window" else NOW
    if boundary == "quote_age":
        runtime.market.changes["quote_at"] = at - timedelta(seconds=4)

    def slow_lookup(client_id):
        runtime.broker.now = at + timedelta(seconds=2)
        return None

    monkeypatch.setattr(runtime.broker, "find_order_by_client_id", slow_lookup)
    result = tick(runtime, at)
    assert result["calibration"]["state"] == "expired"
    assert runtime.broker.submissions == 0
    assert runtime.store.linked_orders(runtime.settings.cohort_id)[0]["status"] == "expired"


@pytest.mark.parametrize(
    "at,state",
    [
        (datetime(2026, 9, 7, 13, 35, tzinfo=UTC), "scheduled"),
        (datetime(2026, 9, 8, 13, 29, tzinfo=UTC), "waiting_for_regular_entry_window"),
        (datetime(2026, 9, 8, 13, 30, tzinfo=UTC), "waiting_for_regular_entry_window"),
        (datetime(2026, 9, 8, 14, 0, tzinfo=UTC), "missed_window_no_trade"),
        (datetime(2026, 9, 9, 13, 35, tzinfo=UTC), "missed_window_no_trade"),
    ],
)
def test_schedule_preserves_holiday_warmup_and_never_catches_up_late(make_runtime, at, state):
    runtime = make_runtime()
    assert tick(runtime, at)["calibration"]["state"] == state
    assert runtime.broker.submissions == 0


@pytest.mark.parametrize(
    "changes",
    [
        {"quote_at": NOW - timedelta(minutes=15)},
        {"quote_at": NOW - timedelta(seconds=6)},
        {"quote_at": NOW + timedelta(seconds=1)},
        {"observed_at": NOW + timedelta(seconds=1)},
        {"bid_size": D(0)},
        {"ask_size": D(0)},
        {"bid": D(101)},
        {"ask": D(102)},
        {"feed": "sip"},
        {"median_daily_dollar_volume": D(1)},
    ],
)
def test_practice_never_uses_delayed_invalid_or_wrong_feed_quotes(make_runtime, changes):
    runtime = make_runtime()
    runtime.market.changes.update(changes)
    tick(runtime)
    assert runtime.broker.submissions == 0
    assert not runtime.store.linked_orders(runtime.settings.cohort_id)


@pytest.mark.parametrize("control", ["kill_switch", "practice-1:pause"])
def test_kill_and_pause_still_block_practice(make_runtime, control):
    runtime = make_runtime()
    runtime.repo.set_control(control, "active")
    result = tick(runtime)
    assert result["calibration"]["state"] == "risk_rejected"
    assert runtime.broker.submissions == 0


def test_no_implicit_operator_permission_from_global_replay_attestation(make_runtime):
    runtime = make_runtime(confirmed=False)
    result = tick(runtime)
    assert result["calibration"]["state"] == "blocked"
    assert not result["operational_certificate"]["checks"]["operator_confirmation"]
    assert runtime.broker.submissions == 0


def test_shadow_never_calibrates(make_runtime):
    runtime = make_runtime(mode="shadow")
    assert tick(runtime)["calibration"]["state"] == "shadow_no_orders"
    assert runtime.broker.submissions == 0


@pytest.mark.parametrize("problem", ["halt", "macro", "source", "market"])
def test_context_and_provider_problems_still_block_calibration(make_runtime, problem):
    runtime = make_runtime()
    if problem == "halt":
        runtime.context_client.halted = True
    elif problem == "macro":
        runtime.context_client.scheduled = (NOW + timedelta(minutes=5),)
    elif problem == "source":
        runtime.source.last_errors = ("synthetic outage",)
    else:
        runtime.market.fail = True
    assert tick(runtime)["calibration"]["state"] == "blocked"
    assert runtime.broker.submissions == 0


def test_partial_calibration_cancels_and_exits_only_owned_quantity(make_runtime):
    runtime = make_runtime()
    runtime.broker.partial = True
    tick(runtime)
    held = runtime.broker.positions()[0].quantity
    tick(runtime, NOW + timedelta(seconds=61))
    sold = next(order for order in runtime.broker.values.values() if order.side == "sell")
    assert sold.quantity == held
    assert runtime.broker.positions() == ()
    assert runtime.broker.submissions == 2


def test_calibration_timeout_is_reconciled_never_blindly_resubmitted(make_runtime):
    runtime = make_runtime()
    runtime.broker.timeout = True
    assert tick(runtime)["calibration"]["state"] == "submission_outcome_unknown"
    tick(runtime, NOW + timedelta(seconds=30))
    assert runtime.broker.positions() == ()
    assert runtime.broker.submissions == 2  # one entry plus the paused-risk exit
    tick(runtime, NOW + timedelta(minutes=2))
    assert runtime.broker.submissions == 2


def test_calibration_keyword_cannot_authorize_an_existing_research_manager(tmp_path):
    db, store, broker, manager, args = order_fixtures.setup(tmp_path)
    try:
        result = manager.submit_entry(**args, entry_kind="calibration")
        assert "CALIBRATION_NOT_AUTHORIZED" in result["reasons"]
        assert broker.submissions == 0
        assert not store.linked_orders("fixture")
    finally:
        db.dispose()


def test_practice_orders_cannot_enter_before_declared_start(make_runtime):
    runtime = make_runtime(practice_start_date=date(2026, 9, 9))
    proof = certificate(
        runtime.settings,
        config_hash=runtime.config_hash,
        code_sha=runtime.code_sha,
        account_id=runtime.broker.account().id,
        checks={"operator_confirmation": True},
        now=NOW,
    )
    result = runtime.oms.submit_entry(
        symbol="AAPL",
        cluster_key="early-strategy",
        decision_id="early",
        eligible_at=NOW,
        expires_at=NOW + timedelta(seconds=30),
        bid=D("99.99"),
        ask=D(100),
        quote_at=NOW,
        median_dollar_volume=D("100000000"),
        source_valid=True,
        certificate=proof,
        now=NOW,
        execution_quote=EventQuote(
            symbol="AAPL",
            bid=D("99.99"),
            ask=D(100),
            bid_size=D(100),
            ask_size=D(100),
            timestamp=NOW,
            received_at=NOW,
            feed="iex",
            size_unit="shares",
        ),
    )
    assert "PRACTICE_NOT_STARTED" in result["reasons"]
    assert runtime.broker.submissions == 0


def test_cli_exposes_explicit_practice_choice_without_changing_default():
    parser = argparse.ArgumentParser()
    event_cli.register_event_commands(parser.add_subparsers(dest="command"))
    plain = parser.parse_args(["run", "--cohort-id", "existing"])
    assert plain.purpose is None
    assert plain.mode == "shadow"
    practice = parser.parse_args(
        [
            "run",
            "--mode",
            "experimental-paper",
            "--cohort-id",
            "separate-practice",
            "--purpose",
            "iex-practice",
            "--practice-start-date",
            "2026-09-08",
        ]
    )
    assert practice.purpose == "iex-practice"
    assert practice.practice_start_date == date(2026, 9, 8)


@pytest.mark.parametrize(
    "purpose,confirmed,start,permitted",
    [
        ("research", True, None, False),
        ("iex-practice", False, date(2026, 9, 8), False),
        ("iex-practice", True, date(2026, 9, 8), True),
        ("iex-practice", True, date(2026, 9, 7), False),
    ],
)
def test_preflight_iex_permission_is_explicit_and_never_weakens_research(
    tmp_path, monkeypatch, capsys, purpose, confirmed, start, permitted
):
    monkeypatch.setattr("tradeagent.event_cli.datetime", Clock)
    Clock.current = NOW
    url = f"sqlite:///{tmp_path / 'preflight.db'}"
    monkeypatch.setenv("TRADEAGENT_DATABASE_URL", url)
    monkeypatch.setenv("ALPACA_KEY_ID", "synthetic-key")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "synthetic-secret")
    monkeypatch.setenv("EVENT_SYMBOLS", "AAPL")
    with Database(url) as database:
        database.initialize()
        ProductionRepository(database).set_control("kill_switch", "active")
    diagnostics = {
        "broker_healthy": True,
        "live_credential_environment_present": False,
        "broker_positions": 0,
        "broker_open_orders": 0,
        "market_data": {
            "sip_latest_quote": {"accessible": False},
            "iex_latest_quote": {"accessible": True},
        },
        "active_execution_feed": "iex",
        "assets": [{"fractionable": True, "tradable": True}],
    }
    broker = order_fixtures.Broker()
    monkeypatch.setattr(event_cli, "source_capabilities", lambda: diagnostics)
    monkeypatch.setattr(event_cli, "code_identity", lambda: "cli-code")
    monkeypatch.setattr(event_cli, "replay_event_pipeline", lambda: {"reconciled": True})
    monkeypatch.setattr(event_cli, "OfficialContextClient", lambda: nullcontext(Context()))
    monkeypatch.setattr(
        "tradeagent.alpaca_paper.AlpacaPaperClient", lambda settings: nullcontext(broker)
    )
    args = argparse.Namespace(
        command="paper-preflight",
        cohort_id="cli-cohort",
        purpose=purpose,
        practice_start_date=start,
        confirm_experimental_paper=confirmed,
        output=None,
    )
    assert event_cli.handle_event_command(args)
    result = json.loads(capsys.readouterr().out)
    assert result["operational_certificate_issued"] is permitted
    assert result["edge_established"] is False
    if purpose == "iex-practice":
        assert result["qualification_eligible"] is False
    with Database(url) as database:
        repo = ProductionRepository(database)
        assert bool(repo.get_control("cli-cohort:certificate")) is permitted
        assert repo.get_control("kill_switch") == ("inactive" if permitted else "active")
    assert broker.submissions == 0


def test_execution_quote_refresh_uses_only_latest_configured_feed(monkeypatch):
    from pydantic import SecretStr

    from tradeagent.alpaca import AlpacaDataSettings
    from tradeagent.event_market import EventMarketClient

    monkeypatch.setattr("tradeagent.event_market.datetime", Clock)
    Clock.current = NOW
    state = Market().state("AAPL", NOW - timedelta(seconds=30))
    requests = []

    def handler(request):
        requests.append(request)
        assert request.url.path == "/v2/stocks/quotes/latest"
        assert dict(request.url.params) == {"symbols": "AAPL", "feed": "iex"}
        return httpx.Response(
            200,
            json={
                "quotes": {
                    "AAPL": {
                        "bp": 100,
                        "ap": 100.01,
                        "bs": 100,
                        "as": 200,
                        "t": NOW.isoformat(),
                    }
                }
            },
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as http:
        client = EventMarketClient(
            AlpacaDataSettings(
                key_id=SecretStr("synthetic-key"),
                secret_key=SecretStr("synthetic-secret"),
                feed="iex",
                _env_file=None,
            ),
            client=http,
        )
        refreshed = client.refresh_quote(state)
        assert refreshed.quote_at == refreshed.observed_at == NOW
        assert refreshed.feed == "iex"
        assert refreshed.ask == D("100.01")
        assert state.quote_at < refreshed.quote_at
        with pytest.raises(ValueError, match="feed changed"):
            client.refresh_quote(state.model_copy(update={"feed": "sip"}))
    assert len(requests) == 1
