from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock

import httpx
import pytest
import test_event_orders as order_fixtures
import test_event_surfaces as surface_fixtures
from pydantic import SecretStr, ValidationError

from tradeagent import event_doctor, event_runtime
from tradeagent.alpaca import AlpacaDataClient
from tradeagent.alpaca_paper import AlpacaOrderStatus, AlpacaPaperClient, AlpacaPaperSettings
from tradeagent.domain import MarketBar, OrderRequest, OrderType, Side
from tradeagent.event_market import EventMarketClient
from tradeagent.event_orders import EventLeaseLostError
from tradeagent.event_research import SourceEvent, extract_event, text_hash
from tradeagent.event_runtime import EventRuntime
from tradeagent.event_store import EventStore
from tradeagent.experimental_policy import ExperimentalSettings
from tradeagent.persistence import Database

NOW = surface_fixtures.NOW
D = Decimal


@pytest.fixture(autouse=True)
def synthetic_only(monkeypatch: pytest.MonkeyPatch) -> None:
    def deny_network(*_: object, **__: object) -> None:
        raise AssertionError("Risk tests may only use explicitly mocked HTTP")

    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", deny_network)
    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", deny_network)
    monkeypatch.setenv("ALPACA_KEY_ID", "synthetic-key")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "synthetic-secret")
    monkeypatch.setenv("EVENT_SYMBOLS", "AAPL")
    monkeypatch.setenv("EVENT_MODE", "shadow")
    monkeypatch.setenv("TRADEAGENT_DATABASE_URL", "sqlite:///:memory:")
    for key in ("ALPACA_LIVE_KEY", "ALPACA_LIVE_KEY_ID", "ALPACA_LIVE_SECRET_KEY"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr(event_runtime, "datetime", surface_fixtures.FrozenClock)
    monkeypatch.setattr(event_doctor, "datetime", surface_fixtures.FrozenClock)
    monkeypatch.setattr(event_doctor, "code_identity", lambda: "synthetic-risk-code")


@pytest.fixture
def synthetic_orders(monkeypatch: pytest.MonkeyPatch) -> Iterator[tuple[Any, ...]]:
    monkeypatch.setattr(order_fixtures, "Database", lambda _: Database("sqlite:///:memory:"))
    result = order_fixtures.setup(Path("."))
    try:
        yield result
    finally:
        result[0].dispose()


def synthetic_request(order_type: OrderType = OrderType.LIMIT) -> OrderRequest:
    return OrderRequest(
        client_order_id="synthetic-http-limit",
        decision_id="synthetic-decision",
        strategy_id="synthetic-risk-boundary",
        symbol="AAPL",
        side=Side.BUY,
        quantity=D("0.123456"),
        order_type=order_type,
        submitted_at=NOW,
    )


def paper_settings() -> AlpacaPaperSettings:
    return AlpacaPaperSettings(
        key_id=SecretStr("synthetic-key"),
        secret_key=SecretStr("synthetic-secret"),
        _env_file=None,
    )


def test_limit_http_payload_cannot_become_notional_market_or_extended_hours() -> None:
    requests: list[httpx.Request] = []
    intent = synthetic_request()

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        payload = json.loads(request.content)
        assert payload == {
            "symbol": "AAPL",
            "qty": "0.123456",
            "side": "buy",
            "type": "limit",
            "time_in_force": "day",
            "limit_price": "100.12",
            "extended_hours": False,
            "client_order_id": intent.client_order_id,
        }
        return httpx.Response(
            200,
            json={
                "id": "synthetic-broker-id",
                **payload,
                "status": "new",
                "filled_qty": "0",
                "filled_avg_price": None,
                "created_at": NOW.isoformat(),
            },
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as http:
        broker = AlpacaPaperClient(paper_settings(), client=http)
        order = broker.submit_limit_order(intent, D("100.12"))
    assert len(requests) == 1
    assert requests[0].method == "POST"
    assert str(requests[0].url) == "https://paper-api.alpaca.markets/v2/orders"
    assert order.quantity == intent.quantity
    assert order.filled_quantity == 0


@pytest.mark.parametrize(
    "intent_type,method",
    [
        (OrderType.MARKET, "limit"),
        (OrderType.LIMIT, "market"),
    ],
)
def test_broker_rejects_intent_endpoint_mismatch_before_http(
    intent_type: OrderType,
    method: str,
) -> None:
    handler = Mock(side_effect=AssertionError("No HTTP permitted for incompatible intent"))
    with httpx.Client(transport=httpx.MockTransport(handler)) as http:
        broker = AlpacaPaperClient(paper_settings(), client=http)
        with pytest.raises(ValueError, match=f"{method} intent"):
            if method == "limit":
                broker.submit_limit_order(synthetic_request(intent_type), D(100))
            else:
                broker.submit_market_order(synthetic_request(intent_type))
    handler.assert_not_called()


def test_live_host_configuration_is_rejected_before_client_creation() -> None:
    with pytest.raises(ValidationError):
        AlpacaPaperSettings(
            key_id=SecretStr("synthetic-key"),
            secret_key=SecretStr("synthetic-secret"),
            paper_url="https://api.alpaca.markets",
            _env_file=None,
        )


def test_absent_unknown_order_never_resubmits_even_after_negative_lookup(
    synthetic_orders: tuple[Any, ...],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, store, broker, manager, args = synthetic_orders
    submit = Mock(side_effect=httpx.ReadTimeout("synthetic ambiguous submission"))
    monkeypatch.setattr(broker, "submit_limit_order", submit)
    result = manager.submit_entry(**args)
    assert result["state"] == "submission_outcome_unknown"
    assert broker.find_order_by_client_id(result["client_order_id"]) is None
    for _ in range(2):
        reconciliation = manager.reconcile(order_fixtures.NOW)
        assert not reconciliation["healthy"]
        assert any(
            reason.startswith("SUBMISSION_OUTCOME_UNKNOWN:")
            for reason in reconciliation["mismatches"]
        )
    row = store.linked_orders("fixture")[0]
    request = OrderRequest.model_validate(row["link"]["request"])
    manager.repo.set_control("fixture:pause", "")
    assert manager._dispatch(request, D(100), order_fixtures.NOW)["state"] == (
        "submission_outcome_unknown"
    )
    assert manager.submit_entry(**args)["state"] == "duplicate_event"
    assert submit.call_count == 1
    assert store.linked_orders("fixture")[0]["status"] == "reconciliation_required"


@pytest.mark.parametrize(
    "changes",
    [
        {"expires_at": order_fixtures.NOW},
        {"issued_at": order_fixtures.NOW + timedelta(seconds=1)},
        {"config_hash": "synthetic-changed-config"},
        {"code_sha": "synthetic-changed-code"},
    ],
)
def test_expired_future_or_stale_certificate_never_reserves_exposure(
    synthetic_orders: tuple[Any, ...],
    changes: dict[str, Any],
) -> None:
    _, store, broker, manager, args = synthetic_orders
    cert = args["certificate"].model_copy(update=changes)
    result = manager.submit_entry(**{**args, "certificate": cert})
    assert "OPERATIONAL_CERTIFICATE_REQUIRED" in result["reasons"]
    assert not store.linked_orders("fixture")
    assert broker.submissions == 0


def test_paper_account_reset_invalidates_certificate(
    synthetic_orders: tuple[Any, ...],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, store, broker, manager, args = synthetic_orders
    reset = broker.account().model_copy(update={"id": "synthetic-reset-account"})
    monkeypatch.setattr(broker, "account", lambda: reset)
    result = manager.submit_entry(**args)
    assert "OPERATIONAL_CERTIFICATE_REQUIRED" in result["reasons"]
    assert broker.submissions == 0
    assert not store.linked_orders("fixture")


@pytest.mark.parametrize("control", ["daily", "drawdown"])
def test_economic_daily_loss_and_drawdown_each_pause_without_statistical_gate(
    synthetic_orders: tuple[Any, ...],
    control: str,
) -> None:
    _, _, broker, manager, _ = synthetic_orders
    manager.repo.set_control(
        f"fixture:day:{order_fixtures.NOW.date()}",
        "10100" if control == "daily" else "10000",
    )
    manager.repo.set_control(
        "fixture:high-watermark", "10200" if control == "drawdown" else "10000"
    )
    result = manager.valuation({}, order_fixtures.NOW)
    assert result["state"] == "valued"
    assert D(result["economic_paper_equity"]) == 10000
    assert manager.repo.get_control("fixture:pause") == "ECONOMIC_LOSS_LIMIT"
    assert result["qualification"] == "unproven"
    assert broker.submissions == 0


def test_open_position_without_mark_pauses_not_zero_marks_or_false_profit(
    synthetic_orders: tuple[Any, ...],
) -> None:
    _, _, broker, manager, args = synthetic_orders
    assert manager.submit_entry(**args)["state"] == "filled"
    result = manager.valuation({}, order_fixtures.NOW)
    assert result["state"] == "UNVALUED_POSITION"
    assert result["economic_paper_pnl"] is None
    assert result["broker_paper_pnl"] is None
    assert manager.repo.get_control("fixture:pause") == "VALUATION_REQUIRED"
    assert len(broker.positions()) == 1


def test_approved_never_dispatched_entry_expires_without_order_retry(
    synthetic_orders: tuple[Any, ...],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, store, broker, manager, args = synthetic_orders
    with monkeypatch.context() as context:
        context.setattr(manager, "_dispatch", Mock(return_value={"state": "approved"}))
        assert manager.submit_entry(**args)["state"] == "approved"
    broker.now = args["expires_at"] + timedelta(seconds=1)
    manager.supervise(broker.now, feed_healthy=True)
    assert store.linked_orders("fixture")[0]["status"] == "expired"
    assert broker.submissions == 0


def test_quote_age_rechecked_after_slow_lookup_before_dispatch(
    synthetic_orders: tuple[Any, ...],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, store, broker, manager, args = synthetic_orders

    def slow_lookup(_: str) -> None:
        broker.now = order_fixtures.NOW + timedelta(seconds=6)

    monkeypatch.setattr(broker, "find_order_by_client_id", slow_lookup)
    assert manager.submit_entry(**args)["state"] == "expired"
    assert broker.submissions == 0
    assert store.linked_orders("fixture")[0]["status"] == "expired"


def test_partial_exit_waits_for_cancel_then_exits_only_remaining_inventory(
    synthetic_orders: tuple[Any, ...],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, _, broker, manager, args = synthetic_orders
    manager.submit_entry(**args)
    initial_quantity = broker.positions()[0].quantity

    def partially_fill_first_exit(request: OrderRequest) -> Any:
        broker.partial = broker.submissions == 1
        return broker.submit_limit_order(request, D(101))

    monkeypatch.setattr(broker, "submit_market_order", partially_fill_first_exit)
    broker.now = order_fixtures.NOW + timedelta(minutes=61)
    manager.supervise(broker.now, feed_healthy=True)
    first_exit = next(order for order in broker.values.values() if order.side == "sell")
    assert first_exit.status is AlpacaOrderStatus.PARTIALLY_FILLED
    assert broker.positions()[0].quantity == initial_quantity / 2
    manager.supervise(broker.now, feed_healthy=True)
    assert broker.submissions == 2
    broker.cancel_order(first_exit.id)
    manager.supervise(broker.now, feed_healthy=True)
    exits = [order for order in broker.values.values() if order.side == "sell"]
    assert len(exits) == 2
    assert exits[1].quantity == initial_quantity / 2
    assert sum((order.filled_quantity for order in exits), D(0)) == initial_quantity
    assert broker.positions() == ()
    assert manager.reconcile(broker.now)["healthy"]


@pytest.fixture
def synthetic_runtime(monkeypatch: pytest.MonkeyPatch) -> Iterator[tuple[Any, ...]]:
    database = Database("sqlite:///:memory:")
    database.initialize()
    settings = ExperimentalSettings(
        mode="experimental-paper",
        cohort_id="synthetic-risk-runtime",
        symbols="AAPL",
        _env_file=None,
    )
    received = NOW - timedelta(minutes=5, seconds=3)
    content = (
        "For fiscal 2027, GAAP revenue guidance increased from USD 100 million to USD 110 million."
    )
    event = SourceEvent(
        source_event_id="synthetic-risk-guidance",
        source="synthetic-primary",
        source_url="https://www.apple.com/newsroom/synthetic-risk-test",
        source_version=text_hash(content),
        content=content,
        content_sha256=text_hash(content),
        published_at=received - timedelta(seconds=1),
        first_received_at=received,
        content_available_at=received,
        event_cluster_id="synthetic-risk-guidance",
        issuer_id="sec:0000320193",
        cik="0000320193",
        is_primary_source=True,
        mapping_available_at=received - timedelta(days=1),
        rights_profile="synthetic-test",
        availability_basis="observed_receipt",
    )
    source = SimpleNamespace(
        poll=Mock(return_value=(event,)),
        last_errors=(),
        capabilities={"sec_enabled": True, "primary_urls_configured": 0},
    )
    current = surface_fixtures.market_state(
        feed="sip",
        bid_size=D(100),
        ask_size=D(100),
        completed_bar=MarketBar(
            symbol="AAPL",
            timestamp=NOW - timedelta(seconds=3),
            open=D(100),
            high=D("100.11"),
            low=D(100),
            close=D("100.10"),
            volume=D(10000),
        ),
    )
    market = SimpleNamespace(state=Mock(return_value=current), close=Mock())
    context = SimpleNamespace(
        poll=Mock(return_value=surface_fixtures.clear_context()),
        close=Mock(),
        capabilities={"synthetic": True},
    )
    monkeypatch.setattr(event_runtime, "OfficialContextClient", lambda: context)
    broker = order_fixtures.Broker()
    broker.now = NOW
    runtime = EventRuntime(
        EventStore(database),
        settings,
        source,
        market,
        broker,
        instance_id="synthetic-risk-owner",
        code_sha="synthetic-risk-code",
    )
    runtime.repo.acquire_worker_lock(
        "tradeagent-event-worker",
        runtime.instance_id,
        observed_at=NOW,
    )
    runtime.market_states["AAPL"] = current
    runtime.first_bar_receipts["AAPL", current.completed_bar.timestamp] = NOW - timedelta(seconds=3)
    runtime.last_source_success = NOW
    runtime.context = surface_fixtures.clear_context()
    pre = surface_fixtures.market_state(
        observed_at=received - timedelta(minutes=1),
        quote_at=received - timedelta(minutes=1),
        bid="99.99",
        ask="100.01",
        feed="sip",
    )
    runtime.store.evidence(event.evidence_id, event.model_dump(mode="json"), received)
    runtime.store.audit(
        "pre_context",
        {"AAPL": pre.model_dump(mode="json")},
        received,
        f"{settings.cohort_id}:{event.evidence_id}:pre_context",
    )
    extraction = extract_event(event, now=received + timedelta(seconds=1))
    runtime.extractions[event.evidence_id] = extraction
    runtime.repo.set_control("v20:mechanics_attestation", runtime.code_sha)
    runtime.operational_preflight(NOW)
    try:
        yield runtime, event, extraction, source, market, broker
    finally:
        database.dispose()


def test_runtime_eligible_decision_uses_persisted_pre_event_not_latest_tick(
    synthetic_runtime: tuple[Any, ...],
) -> None:
    runtime, event, extraction, _, _, broker = synthetic_runtime
    future = surface_fixtures.market_state(bid="200", ask="200.01")
    result = runtime._decision(event, extraction, NOW, {"AAPL": future})
    assert result.action == "eligible", result.reasons
    assert result.market_snapshot.pre_event_price == D(100)
    assert result.market_snapshot.pre_event_available_at < event.published_at
    assert runtime.cert.permits_paper
    assert not runtime.cert.establishes_edge
    assert broker.submissions == 0


def test_fully_valid_synthetic_runtime_records_decision_before_one_entry(
    synthetic_runtime: tuple[Any, ...],
) -> None:
    runtime, _, _, _, _, broker = synthetic_runtime
    runtime.tick(NOW)
    report = runtime.store.report(runtime.settings.cohort_id)
    assert report["decision_count"] == 1
    assert report["decisions"][0]["payload"]["action"] == "eligible"
    assert broker.submissions == 1
    assert len(report["orders"]) == 1
    assert report["orders"][0]["status"] == "filled"
    runtime.tick(NOW)
    assert broker.submissions == 1


def test_future_context_cannot_rescue_invalid_evidence_and_new_code_needs_new_cohort(
    synthetic_runtime: tuple[Any, ...],
) -> None:
    runtime, event, extraction, _, _, broker = synthetic_runtime
    old = runtime._decision(event, extraction, NOW, {})
    changed = event.model_copy(update={"published_at": NOW + timedelta(days=1)})
    result = runtime._decision(changed, extraction, NOW, {})
    assert old.action == "eligible"
    assert result.action == "abstain"
    assert "evidence_packet_mismatch" in result.reasons
    assert broker.submissions == 0
    with pytest.raises(ValueError, match="cohort immutable"):
        runtime.store.freeze(runtime.settings.cohort_id, "changed-code-hash", {}, "shadow", NOW)


def test_restart_restores_original_extraction_completion_not_replay_time(
    synthetic_runtime: tuple[Any, ...],
) -> None:
    runtime, event, extraction, _, _, _ = synthetic_runtime
    trace = f"{runtime.settings.cohort_id}:{event.evidence_id}:extraction"
    runtime.store.audit("extraction", extraction.model_dump(mode="json"), NOW, trace)
    runtime.extractions.clear()
    restored = runtime._extraction(event, NOW + timedelta(minutes=1))
    assert restored == extraction
    assert restored.completed_at < NOW


@pytest.mark.parametrize(
    "failure,reason",
    [
        ("closed", "MARKET_CLOSED_OR_ENTRY_CUTOFF"),
        ("clock", "BROKER_CLOCK_STALE"),
        ("blocked", "BROKER_BLOCKED"),
        ("cash", "SIZE_OR_CASH_LIMIT"),
        ("asset", "UNSUPPORTED_ASSET"),
        ("spread", "INVALID_PRICE_OR_SPREAD"),
        ("minimum", "SIZE_OR_CASH_LIMIT"),
        ("entries", "SESSION_ENTRY_LIMIT"),
    ],
)
def test_independent_operational_limits_reject_even_with_valid_certificate(
    synthetic_orders: tuple[Any, ...],
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
    reason: str,
) -> None:
    _, _, broker, manager, args = synthetic_orders
    if failure in {"closed", "clock"}:
        clock = broker.clock()
        updates = (
            {"is_open": False}
            if failure == "closed"
            else {"timestamp": order_fixtures.NOW - timedelta(minutes=2)}
        )
        monkeypatch.setattr(broker, "clock", lambda: clock.model_copy(update=updates))
    elif failure in {"blocked", "cash"}:
        account = broker.account()
        updates = {"trading_blocked": True} if failure == "blocked" else {"cash": D(1)}
        monkeypatch.setattr(broker, "account", lambda: account.model_copy(update=updates))
    elif failure == "asset":
        asset = broker.asset("AAPL").model_copy(update={"fractionable": False})
        monkeypatch.setattr(broker, "asset", lambda _: asset)
    elif failure == "spread":
        args.update(bid=D(50))
    elif failure == "minimum":
        manager.settings = manager.settings.model_copy(update={"max_entry_notional": D(1)})
    else:
        manager.settings = manager.settings.model_copy(update={"max_entries_per_session": 1})
        manager.submit_entry(**args)
        broker.now = order_fixtures.NOW + timedelta(minutes=61)
        manager.supervise(broker.now, feed_healthy=True)
        args.update(
            now=broker.now,
            quote_at=broker.now,
            eligible_at=broker.now,
            expires_at=broker.now + timedelta(minutes=1),
            cluster_key="new-cluster",
        )
    before = broker.submissions
    outcome = manager.submit_entry(**args)
    assert reason in outcome["reasons"]
    assert broker.submissions == before


def test_new_live_credential_cannot_cross_broker_dispatch_boundary(
    synthetic_orders: tuple[Any, ...],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, _, broker, manager, args = synthetic_orders
    monkeypatch.setenv("ALPACA_LIVE_KEY_ID", "synthetic-forbidden-live-key")
    with pytest.raises(ValueError, match="live broker configuration forbidden"):
        manager.submit_entry(**args)
    assert broker.submissions == 0


@pytest.mark.parametrize("source_status", ["never_received", "stale", "acquisition_error"])
def test_runtime_source_failure_blocks_pending_event_entry(
    synthetic_runtime: tuple[Any, ...],
    source_status: str,
) -> None:
    runtime, _, _, source, _, broker = synthetic_runtime
    source.last_errors = ("synthetic-primary-acquisition-error",)
    if source_status == "never_received":
        runtime.last_source_success = None
    elif source_status == "stale":
        runtime.last_source_success = NOW - timedelta(minutes=3)
    heartbeat = runtime.tick(NOW)
    assert heartbeat["events_received"] == 1
    assert broker.submissions == 0
    assert not runtime.store.linked_orders(runtime.settings.cohort_id)


def test_runtime_acquisition_exception_supervises_first_and_never_submits(
    synthetic_runtime: tuple[Any, ...],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, _, _, source, _, broker = synthetic_runtime
    supervise = Mock(wraps=runtime.oms.supervise)
    monkeypatch.setattr(runtime.oms, "supervise", supervise)
    source.poll.side_effect = httpx.ReadTimeout("synthetic ingestion timeout")
    with pytest.raises(httpx.ReadTimeout):
        runtime.tick(NOW)
    supervise.assert_called_once_with(NOW, feed_healthy=True)
    assert broker.submissions == 0


def test_runtime_preflight_does_not_certify_stale_source(
    synthetic_runtime: tuple[Any, ...],
) -> None:
    runtime, _, _, _, _, broker = synthetic_runtime
    runtime.last_source_success = NOW - timedelta(minutes=3)
    cert = runtime.operational_preflight(NOW)
    assert cert.checks["source_connected"] is False
    assert cert.permits_paper is False
    assert broker.submissions == 0


def test_runtime_lost_lease_stops_before_any_ingestion_or_broker_action(
    synthetic_runtime: tuple[Any, ...],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, _, _, source, market, broker = synthetic_runtime
    assert runtime.repo.release_worker_lock("tradeagent-event-worker", runtime.instance_id)
    assert runtime.repo.acquire_worker_lock(
        "tradeagent-event-worker",
        "synthetic-new-owner",
        observed_at=NOW,
    )
    account = Mock(side_effect=AssertionError("stale owner must not call broker"))
    monkeypatch.setattr(broker, "account", account)
    with pytest.raises(EventLeaseLostError):
        runtime.tick(NOW)
    account.assert_not_called()
    source.poll.assert_not_called()
    market.state.assert_not_called()
    assert broker.submissions == 0


@pytest.mark.parametrize("latest_status,historical_status", [(403, 200), (200, 403)])
def test_doctor_http_probes_live_and_historical_entitlements_independently(
    monkeypatch: pytest.MonkeyPatch,
    latest_status: int,
    historical_status: int,
) -> None:
    requests: list[httpx.Request] = []
    broker = order_fixtures.Broker()
    broker.now = NOW

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.method == "GET"
        path = request.url.path
        if path == "/v2/account":
            payload = broker.account().model_dump(mode="json")
        elif path == "/v2/clock":
            payload = broker.clock().model_dump(mode="json")
        elif path == "/v2/assets/AAPL":
            payload = broker.asset("AAPL").model_dump(mode="json", by_alias=True)
        elif path in {"/v2/positions", "/v2/orders"}:
            payload = []
        elif path.endswith("/quotes/latest"):
            if request.url.params["feed"] == "sip" and latest_status == 403:
                return httpx.Response(403, json={"message": "synthetic live SIP denied"})
            payload = {"quote": {"t": NOW.isoformat(), "bp": "100", "ap": "100.01"}}
        elif path.endswith("/quotes"):
            assert request.url.params["feed"] == "sip"
            assert datetime.fromisoformat(request.url.params["end"]) < NOW
            if historical_status == 403:
                return httpx.Response(403, json={"message": "synthetic historical SIP denied"})
            payload = {"quotes": [], "next_page_token": None}
        else:
            raise AssertionError(f"Unexpected HTTP request {request.url}")
        return httpx.Response(200, json=payload)

    with httpx.Client(transport=httpx.MockTransport(handler)) as http:
        monkeypatch.setattr(
            event_doctor,
            "AlpacaPaperClient",
            lambda settings: AlpacaPaperClient(settings, client=http),
        )
        monkeypatch.setattr(
            event_doctor,
            "AlpacaDataClient",
            lambda settings: AlpacaDataClient(settings, client=http),
        )
        monkeypatch.setattr(
            event_doctor,
            "EventMarketClient",
            lambda settings: EventMarketClient(settings, client=http),
        )
        result = event_doctor.source_capabilities()
    assert result["market_data"]["historical_sip"]["historical_quotes"] is (
        historical_status == 200
    )
    assert result["market_data"]["sip_latest_quote"]["accessible"] is (latest_status == 200)
    if latest_status == 403:
        assert result["market_data"]["sip_latest_quote"]["http_status"] == 403
    assert result["market_data"]["iex_latest_quote"]["nbbo"] is False
    assert result["live_execution_available"] is False
    assert result["statistical_qualification"] == "not established"
    assert "paper-fixture" not in json.dumps(result)
    assert not any(request.method != "GET" for request in requests)
