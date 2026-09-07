from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import httpx
import pytest
from pydantic import SecretStr
from sqlalchemy import select
from test_iex_practice import NOW, Clock, Context, tick
from test_iex_practice import make_runtime as make_runtime

from tradeagent.alpaca_paper import AlpacaOrderStatus, AlpacaPaperClient, AlpacaPaperSettings
from tradeagent.config import AppConfig
from tradeagent.event_orders import ExperimentalOrderManager
from tradeagent.event_research import EventQuote, SourceEvent, extract_event, text_hash
from tradeagent.event_runtime import EventRuntime
from tradeagent.event_session import verify_session_plan
from tradeagent.event_store import event_candidate_states
from tradeagent.experimental_policy import certificate
from tradeagent.persistence import events

D = Decimal


def add_guidance(runtime, *, symbol="AAPL", received=None, identity="guidance"):
    received = received or NOW - timedelta(minutes=10)
    cik, url, company = {
        "AAPL": ("0000320193", "https://www.apple.com/newsroom/synthetic-guidance", "Apple"),
        "NVDA": ("0001045810", "https://nvidianews.nvidia.com/news/synthetic-guidance", "NVIDIA"),
        "MSFT": ("0000789019", "https://www.microsoft.com/en-us/synthetic-guidance", "Microsoft"),
    }[symbol]
    content = (
        "For fiscal 2027, GAAP revenue guidance increased from USD 100 million to USD 110 million."
    )
    event = SourceEvent(
        source_event_id=identity,
        source="synthetic-primary",
        source_url=url,
        source_version=text_hash(content + identity),
        content=content,
        content_sha256=text_hash(content),
        headline=f"{company} guidance update",
        published_at=received - timedelta(seconds=1),
        first_received_at=received,
        content_available_at=received,
        event_cluster_id=f"{symbol}:{identity}",
        issuer_id=f"sec:{cik}",
        cik=cik,
        provider_symbols=(symbol,),
        related_instruments=(symbol,),
        is_primary_source=True,
        mapping_available_at=received - timedelta(days=1),
        rights_profile="synthetic-test",
        availability_basis="observed_receipt",
    )
    runtime.store.evidence(event.evidence_id, event.model_dump(mode="json"), received)
    extraction = extract_event(event, now=received + timedelta(seconds=1))
    runtime.extractions[event.evidence_id] = extraction
    runtime.store.audit(
        "extraction",
        extraction.model_dump(mode="json"),
        extraction.completed_at,
        f"{runtime.settings.cohort_id}:{event.evidence_id}:extraction",
    )
    pre = runtime.market.state(symbol, received - timedelta(seconds=2)).model_copy(
        update={"bid": D("99.80"), "ask": D("99.82")}
    )
    runtime.store.audit(
        "pre_context",
        {symbol: pre.model_dump(mode="json")},
        received,
        f"{runtime.settings.cohort_id}:{event.evidence_id}:pre_context",
    )
    return event


def recorded(runtime, kind):
    with runtime.store.database.begin() as connection:
        return list(
            connection.scalars(
                select(events.c.payload)
                .where(events.c.event_type == f"event_{kind}")
                .order_by(events.c.occurred_at, events.c.recorded_at)
            )
        )


def test_broker_calendar_confirms_holiday_weekend_and_previous_close():
    requests = []

    def handler(request):
        requests.append(request)
        assert str(request.url).startswith("https://paper-api.alpaca.markets/v2/calendar?")
        assert request.method == "GET"
        return httpx.Response(
            200,
            json=[
                {"date": "2026-09-04", "open": "09:30", "close": "16:00"},
                {"date": "2026-09-08", "open": "09:30", "close": "16:00"},
            ],
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as http:
        broker = AlpacaPaperClient(
            AlpacaPaperSettings(
                key_id=SecretStr("synthetic"),
                secret_key=SecretStr("synthetic"),
                _env_file=None,
            ),
            client=http,
        )
        plan = verify_session_plan(broker, date(2026, 9, 8), AppConfig().intraday, NOW)
    assert plan.previous_session_close == datetime(2026, 9, 4, 20, tzinfo=UTC)
    assert plan.session_open == datetime(2026, 9, 8, 13, 30, tzinfo=UTC)
    assert plan.session_close == datetime(2026, 9, 8, 20, tzinfo=UTC)
    assert len(requests) == 1


def test_clock_alone_does_not_skip_completed_bar_and_next_frame(make_runtime):
    runtime = make_runtime()
    runtime.first_bar_receipts.clear()
    first = tick(runtime)
    assert first["calibration"]["state"] == "blocked"
    assert "NEXT_FRAME_PROCESSING_LATENCY" in first["calibration"]["reasons"]
    assert runtime.broker.submissions == 0
    assert tick(runtime, NOW + timedelta(seconds=30))["calibration"]["state"] == "filled"


@pytest.mark.parametrize("completed_sessions", [None, 0, 19])
def test_equipment_requires_actual_sufficient_daily_history(make_runtime, completed_sessions):
    runtime = make_runtime()
    runtime.market.changes["completed_daily_sessions"] = completed_sessions
    result = tick(runtime)
    assert result["calibration"]["state"] == "blocked"
    assert "COMPLETED_OPENING_OBSERVATION_REQUIRED" in result["calibration"]["reasons"]
    assert result["calibration"]["observed"]["completed_daily_sessions"] == completed_sessions
    assert runtime.broker.submissions == 0


def test_verified_official_feeds_are_a_configured_primary_source(make_runtime):
    runtime = make_runtime()
    runtime.source.capabilities = {
        "sec_enabled": False,
        "primary_urls_configured": 0,
        "issuer_feeds_enabled": True,
    }
    result = tick(runtime)
    assert result["operational_certificate"]["checks"]["primary_source_configured"] is True
    assert result["calibration"]["state"] == "filled"


def test_premarket_brief_starts_at_previous_regular_close_not_one_hour(make_runtime):
    runtime = make_runtime()
    tick(runtime, datetime(2026, 9, 7, 13, tzinfo=UTC))
    assert runtime.session_plan.previous_session_close == datetime(2026, 9, 4, 20, tzinfo=UTC)
    assert datetime.fromisoformat(runtime.source.last_poll_stats["requested_start"]) <= (
        runtime.session_plan.previous_session_close
    )
    assert runtime.premarket_brief["prepared_before_open"] is True
    assert runtime.broker.submissions == 0


def test_eligible_news_waits_for_equipment_then_gets_fresh_ticket(make_runtime):
    runtime = make_runtime()
    event = add_guidance(runtime)
    tick(runtime)
    assert runtime.broker.submissions == 1
    with runtime.store.database.begin() as connection:
        assert connection.scalar(select(event_candidate_states.c.status)) == "waiting"
    later = NOW + timedelta(seconds=61)
    tick(runtime, later)
    rows = runtime.store.linked_orders(runtime.settings.cohort_id)
    news = [row for row in rows if row["side"] == "buy" and row["link"]["entry_kind"] == "strategy"]
    assert len(news) == 1
    assert runtime.broker.submissions == 3
    ticket = news[0]["link"]["decision_ticket"]
    assert ticket["trade_classification"] == "NEWS_STRATEGY"
    assert event.evidence_id in ticket["evidence_ids"]
    assert datetime.fromisoformat(ticket["decision"]["decided_at"]) == later
    assert ticket["expected_net_return_bps"] is None
    assert datetime.fromisoformat(news[0]["link"]["exit_at"]) == later + timedelta(minutes=60)
    assert runtime.oms.session_budget()["news_entries_reserved"] == 1
    assert runtime.oms.session_budget()["total_entries_reserved"] == 2
    approvals = recorded(runtime, "decision_ticket")
    assert len(approvals) == 2
    assert approvals[1]["risk_approved"] is True


def test_equipment_target_miss_remains_visible_after_late_flatten(make_runtime):
    runtime = make_runtime()
    tick(runtime)
    result = tick(runtime, NOW + timedelta(seconds=65))
    targets = result["calibration"]["operational_targets"]
    assert targets["last_exit_fill_lateness_seconds"] == 5
    assert targets["completion_confirmed"] is True
    assert targets["overdue_owned_quantity"] == {}
    assert targets["cancel_request_lateness_seconds"] is None


def test_waiting_news_is_not_executed_after_price_already_chased(make_runtime):
    runtime = make_runtime()
    add_guidance(runtime)
    tick(runtime)
    runtime.market.changes.update(bid=D("102.99"), ask=D("103"))
    tick(runtime, NOW + timedelta(seconds=61))
    assert runtime.broker.submissions == 2
    assert runtime.broker.positions() == ()
    states = recorded(runtime, "candidate_state")
    assert any("post_event_chase_exceeds_policy" in row.get("reasons", []) for row in states)
    assert runtime.oms.session_budget()["news_entries_reserved"] == 0


def test_revalidation_considers_newly_known_revisions_without_rewriting_packet(make_runtime):
    runtime = make_runtime()
    original = add_guidance(runtime)
    tick(runtime)
    revised_at = NOW + timedelta(seconds=30)
    revised = SourceEvent.model_validate(
        {
            **original.model_dump(),
            "source_version": text_hash("later-revision"),
            "revision_of": original.evidence_id,
            "first_received_at": revised_at,
            "content_available_at": revised_at,
            "provider_updated_at": revised_at,
            "is_correction": True,
        }
    )
    runtime.store.evidence(revised.evidence_id, revised.model_dump(mode="json"), revised_at)
    extraction = runtime.extractions[original.evidence_id]
    frozen_hash = extraction.input_sha256
    runtime.broker.now = revised_at
    Clock.current = revised_at
    result = runtime._decision(original, extraction, revised_at, {})
    assert "known_correction_or_retraction" in result.reasons
    assert result.position_review_required is True
    assert extraction.input_sha256 == frozen_hash
    assert revised.evidence_id not in extraction.evidence_ids


def test_simultaneous_candidates_use_declared_ranking_not_later_profit(make_runtime):
    runtime = make_runtime(symbols="AAPL,NVDA")
    apple = add_guidance(runtime, symbol="AAPL", identity="apple")
    nvidia = add_guidance(
        runtime, symbol="NVDA", received=NOW - timedelta(minutes=9), identity="nvidia"
    )
    tick(runtime)
    tick(runtime, NOW + timedelta(seconds=61))
    rows = runtime.store.linked_orders(runtime.settings.cohort_id)
    news = [row for row in rows if row["side"] == "buy" and row["link"]["entry_kind"] == "strategy"]
    assert len(news) == 1
    assert news[0]["symbol"] == "NVDA"
    selection = news[0]["link"]["decision_ticket"]["selection"]
    assert selection["alternatives"][0]["evidence_id"] == nvidia.evidence_id
    assert selection["alternatives"][1]["evidence_id"] == apple.evidence_id
    assert "latest-primary-receipt" in selection["rule"]


def news_order_args(runtime, at, identity="news"):
    proof = certificate(
        runtime.settings,
        config_hash=runtime.config_hash,
        code_sha=runtime.code_sha,
        account_id=runtime.broker.account().id,
        checks={"operator_confirmation": True},
        now=at,
    )
    return {
        "symbol": "AAPL",
        "cluster_key": identity,
        "decision_id": identity,
        "eligible_at": at,
        "expires_at": at + timedelta(seconds=30),
        "bid": D("99.99"),
        "ask": D(100),
        "quote_at": at,
        "median_dollar_volume": D("100000000"),
        "source_valid": True,
        "certificate": proof,
        "now": at,
        "execution_quote": EventQuote(
            symbol="AAPL",
            bid=D("99.99"),
            ask=D(100),
            bid_size=D(100),
            ask_size=D(100),
            timestamp=at,
            received_at=at,
            feed="iex",
            size_unit="shares",
        ),
        "decision_ticket": {
            "evidence_ids": ["synthetic-unit-budget-evidence"],
            "decision": {"action": "eligible", "symbol": "AAPL"},
        },
    }


def test_final_dispatch_preserves_full_news_horizon_before_flatten(make_runtime, monkeypatch):
    runtime = make_runtime()
    at = NOW.replace(hour=18, minute=49, second=59)
    runtime.broker.now = at
    lookup = runtime.broker.find_order_by_client_id

    def lookup_crosses_horizon(client_id):
        runtime.broker.now = at + timedelta(seconds=2)
        return lookup(client_id)

    monkeypatch.setattr(runtime.broker, "find_order_by_client_id", lookup_crosses_horizon)
    result = runtime.oms.submit_entry(**news_order_args(runtime, at))
    assert result["state"] == "expired"
    assert runtime.broker.submissions == 0
    assert runtime.oms.session_budget()["total_entries_reserved"] == 1
    assert not recorded(runtime, "submission_attempt")


def test_one_news_limit_applies_without_equipment_and_across_deployments(make_runtime):
    runtime = make_runtime()
    at = NOW + timedelta(minutes=31)
    runtime.broker.now = at
    assert runtime.oms.submit_entry(**news_order_args(runtime, at))["state"] == "filled"
    runtime.broker.now = at + timedelta(minutes=61)
    runtime.oms.supervise(runtime.broker.now, feed_healthy=True)
    assert not runtime.broker.positions()
    later = runtime.broker.now + timedelta(seconds=1)
    runtime.broker.now = later
    second = runtime.oms.submit_entry(**news_order_args(runtime, later, "second-news"))
    assert second["reasons"] == ["NEWS_ENTRY_LIMIT"]
    assert runtime.oms.session_budget()["total_entries_reserved"] == 1
    new_settings = runtime.settings.model_copy(update={"cohort_id": "new-deployment-budget"})
    runtime.store.freeze(new_settings.cohort_id, "new-hash", {}, "experimental-paper", later)
    manager = ExperimentalOrderManager(
        runtime.store, runtime.broker, new_settings, runtime.app, "new-hash", "new-code"
    )
    args = news_order_args(runtime, later, "new-deployment-news")
    args["certificate"] = certificate(
        new_settings,
        config_hash="new-hash",
        code_sha="new-code",
        account_id=runtime.broker.account().id,
        checks={"operator_confirmation": True},
        now=later,
    )
    assert manager.submit_entry(**args)["reasons"] == ["NEWS_ENTRY_LIMIT"]
    assert runtime.broker.submissions == 2


def test_news_entry_cannot_roll_into_another_session(make_runtime):
    runtime = make_runtime()
    at = NOW + timedelta(days=1)
    runtime.broker.now = at
    result = runtime.oms.submit_entry(**news_order_args(runtime, at))
    assert "PRACTICE_SESSION_ENDED" in result["reasons"]
    assert runtime.broker.submissions == 0


def test_definitive_broker_rejection_consumes_news_attempt(make_runtime, monkeypatch):
    runtime = make_runtime()
    at = NOW + timedelta(minutes=31)
    runtime.broker.now = at

    def reject(request, limit):
        runtime.broker.submissions += 1
        response = httpx.Response(
            422,
            json={"code": 42210000},
            request=httpx.Request("POST", "https://paper-api.alpaca.markets/v2/orders"),
        )
        response.raise_for_status()

    monkeypatch.setattr(runtime.broker, "submit_limit_order", reject)
    result = runtime.oms.submit_entry(**news_order_args(runtime, at))
    assert result["state"] == "rejected"
    row = runtime.store.linked_orders(runtime.settings.cohort_id)[0]
    assert row["status"] == "rejected"
    assert row["link"]["submission_attempted_at"]
    assert row["link"]["rejection"]["provider_code"] == 42210000
    assert runtime.oms.submit_entry(**news_order_args(runtime, at, "another"))["reasons"] == [
        "NEWS_ENTRY_LIMIT"
    ]
    assert runtime.broker.submissions == 1


def test_equipment_identity_survives_new_cohort_and_code_deployment(make_runtime):
    runtime = make_runtime()
    tick(runtime)
    tick(runtime, NOW + timedelta(seconds=61))
    original = runtime.oms.session_budget()
    settings = runtime.settings.model_copy(update={"cohort_id": "same-session-new-deployment"})
    new = EventRuntime(
        runtime.store,
        settings,
        runtime.source,
        runtime.market,
        runtime.broker,
        instance_id="fixture",
        code_sha="repaired-code",
    )
    new.context_client.close()
    new.context_client = Context()
    proof = certificate(
        settings,
        config_hash=new.config_hash,
        code_sha=new.code_sha,
        account_id=new.broker.account().id,
        checks={"operator_confirmation": True},
        now=NOW,
    )
    new.repo.set_control(f"{settings.cohort_id}:certificate", proof.model_dump_json())
    new.repo.set_control("v20:mechanics_attestation", new.code_sha)
    result = tick(new, NOW + timedelta(minutes=2))
    assert result["calibration"]["state"] == "already_consumed_by_prior_cohort"
    assert result["calibration"]["equipment_test_id"] == original["equipment_test_id"]
    assert runtime.broker.submissions == 2


def test_missed_equipment_has_blocking_evidence_and_is_not_daily(make_runtime):
    runtime = make_runtime()
    runtime.market.changes["bid_size"] = D(0)
    tick(runtime)
    missed = tick(runtime, datetime(2026, 9, 8, 14, tzinfo=UTC))["calibration"]
    assert missed["outcome"] == "MISSED"
    assert missed["blocking_evidence"]["state"] == "risk_rejected"
    runtime.market.changes.clear()
    next_day = tick(runtime, datetime(2026, 9, 9, 13, 35, 5, tzinfo=UTC))
    assert next_day["calibration"]["outcome"] == "MISSED"
    assert runtime.broker.submissions == 0


def test_late_fill_while_cancel_pending_is_closed_without_overselling(make_runtime, monkeypatch):
    runtime = make_runtime()
    runtime.broker.partial = True
    tick(runtime)
    entry = next(iter(runtime.broker.values.values()))
    late_quantity = entry.filled_quantity + D("0.01")

    def pending_cancel(order_id):
        old = runtime.broker.values[order_id]
        runtime.broker.values[order_id] = old.model_copy(
            update={"status": AlpacaOrderStatus.PENDING_CANCEL, "filled_quantity": late_quantity}
        )

    monkeypatch.setattr(runtime.broker, "cancel_order", pending_cancel)
    tick(runtime, NOW + timedelta(seconds=31))
    assert runtime.broker.submissions == 1
    assert runtime.broker.positions()[0].quantity == late_quantity
    runtime.broker.values[entry.id] = runtime.broker.values[entry.id].model_copy(
        update={"status": AlpacaOrderStatus.CANCELED}
    )
    tick(runtime, NOW + timedelta(seconds=61))
    exits = [order for order in runtime.broker.values.values() if order.side == "sell"]
    assert len(exits) == 1
    assert exits[0].quantity == late_quantity
    assert runtime.broker.positions() == ()
    assert recorded(runtime, "cancel_requested")[0]["cancellation_confirmed"] is False


def test_out_of_order_update_cannot_reduce_confirmed_fills(make_runtime):
    runtime = make_runtime()
    tick(runtime)
    order = next(iter(runtime.broker.values.values()))
    latest = order.model_copy(update={"updated_at": NOW + timedelta(seconds=2)})
    runtime.oms._observe(order.client_order_id, latest, NOW + timedelta(seconds=2))
    older = order.model_copy(
        update={
            "updated_at": NOW + timedelta(seconds=1),
            "status": AlpacaOrderStatus.PARTIALLY_FILLED,
            "filled_quantity": order.filled_quantity / 2,
        }
    )
    runtime.oms._observe(order.client_order_id, older, NOW + timedelta(seconds=3))
    row = runtime.store.linked_orders(runtime.settings.cohort_id)[0]
    assert D(row["filled_quantity"]) == order.filled_quantity
    assert row["status"] == "filled"
    assert recorded(runtime, "broker_update_ignored")[-1]["reason"] == "out_of_order_update"


def test_paper_stream_hint_is_reconciled_from_rest_not_trusted_as_fill(make_runtime):
    runtime = make_runtime()
    tick(runtime)
    order = next(iter(runtime.broker.values.values()))
    runtime.oms.reconcile_stream_update(
        {
            "event": "fill",
            "client_order_id": order.client_order_id,
            "filled_quantity": "999999",
            "received_at": NOW.isoformat(),
        },
        NOW,
    )
    row = runtime.store.linked_orders(runtime.settings.cohort_id)[0]
    assert D(row["filled_quantity"]) == order.filled_quantity
    assert recorded(runtime, "broker_stream_update")[-1]["rest_confirmed"] is True


def test_partial_exit_recovers_after_confirmed_cancellation_without_manual_help(
    make_runtime, monkeypatch
):
    runtime = make_runtime()
    tick(runtime)

    def partial_first_exit(request):
        runtime.broker.partial = runtime.broker.submissions == 1
        return runtime.broker.submit_limit_order(request, D(101))

    monkeypatch.setattr(runtime.broker, "submit_market_order", partial_first_exit)
    tick(runtime, NOW + timedelta(seconds=61))
    remaining = runtime.broker.positions()[0].quantity
    tick(runtime, NOW + timedelta(seconds=92))
    exits = [order for order in runtime.broker.values.values() if order.side == "sell"]
    assert len(exits) == 2
    assert exits[1].quantity == remaining
    assert runtime.broker.positions() == ()
    assert any(item["side"] == "sell" for item in recorded(runtime, "cancel_requested"))


def test_session_completion_never_hides_an_uncertain_order(make_runtime, monkeypatch):
    runtime = make_runtime()

    def lost_before_ack(request, limit):
        raise httpx.ReadTimeout("synthetic uncertainty")

    monkeypatch.setattr(runtime.broker, "submit_limit_order", lost_before_ack)
    tick(runtime)
    closed = datetime(2026, 9, 8, 20, 1, tzinfo=UTC)
    tick(runtime, closed)
    status = recorded(runtime, "session_completion")[-1]
    assert status["state"] == "incident_unresolved_exposure"
    assert status["unconfirmed_local_orders"]
