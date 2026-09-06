from __future__ import annotations

import argparse
import json
from collections.abc import Iterator
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock

import httpx
import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import create_engine, func, select
from sqlalchemy.pool import StaticPool

from tradeagent import api, event_cli, event_runtime
from tradeagent.alpaca import AlpacaDataSettings
from tradeagent.event_context import HaltStatus, OfficialContextSnapshot
from tradeagent.event_market import EventMarketClient, EventMarketState
from tradeagent.event_outcomes import event_outcomes, outcome_summary, record_quote_paths
from tradeagent.event_replay import ReplayBroker, replay_event_pipeline
from tradeagent.event_research import SourceEvent
from tradeagent.event_store import EventStore, event_cohorts, event_decisions, event_evidence
from tradeagent.persistence import Database, ProductionRepository

NOW = datetime(2026, 9, 8, 14, 5, 3, tzinfo=UTC)
COHORT = "test-event-surfaces"


class FrozenClock(datetime):
    @classmethod
    def now(cls, tz: Any = None) -> FrozenClock:
        return cls.fromtimestamp(NOW.timestamp(), tz=tz or UTC)


@pytest.fixture(autouse=True)
def isolated_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    def deny_network(*_: object, **__: object) -> None:
        raise AssertionError("Surface tests must not make real network requests")

    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", deny_network)
    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", deny_network)
    monkeypatch.setenv("ALPACA_KEY_ID", "synthetic-key")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "synthetic-secret")
    monkeypatch.setenv("NEWS_CONTACT_EMAIL", "fixture@example.test")
    monkeypatch.setenv("EVENT_MODE", "shadow")
    monkeypatch.setenv("EVENT_SYMBOLS", "AAPL")
    monkeypatch.setenv("EVENT_COHORT_ID", COHORT)
    monkeypatch.setenv("TRADEAGENT_DATABASE_URL", "sqlite:///:memory:")
    monkeypatch.setattr(event_cli, "code_identity", lambda: "synthetic-code-sha")
    monkeypatch.setattr(event_runtime, "code_identity", lambda: "synthetic-code-sha")
    monkeypatch.setattr(event_cli, "datetime", FrozenClock)
    monkeypatch.setattr(event_runtime, "datetime", FrozenClock)
    monkeypatch.setattr(api, "datetime", FrozenClock)


@pytest.fixture
def memory_database(monkeypatch: pytest.MonkeyPatch) -> Iterator[Database]:
    database = Database("sqlite:///:memory:")
    database.dispose()
    database.engine = create_engine(
        "sqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    database.initialize()
    for module in (event_cli, event_runtime, api):
        monkeypatch.setattr(module, "Database", lambda _: nullcontext(database))
    try:
        yield database
    finally:
        database.dispose()


@pytest.fixture(scope="module")
def replay() -> dict[str, Any]:
    return replay_event_pipeline()


def arguments(*values: str) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    event_cli.register_event_commands(parser.add_subparsers(dest="command", required=True))
    return parser.parse_args(values)


def invoke(capsys: pytest.CaptureFixture[str], *values: str) -> dict[str, Any]:
    assert event_cli.handle_event_command(arguments(*values))
    result: dict[str, Any] = json.loads(capsys.readouterr().out)
    return result


def market_state(**updates: Any) -> EventMarketState:
    values: dict[str, Any] = {
        "symbol": "AAPL",
        "observed_at": NOW,
        "feed": "iex",
        "bid": "100.09",
        "ask": "100.11",
        "bid_size": "7",
        "ask_size": "9",
        "quote_at": NOW,
        "raw_quote": {"bx": "V", "ax": "V"},
        "raw_trade": None,
        "completed_bar": None,
        "previous_close": "100",
        "median_daily_dollar_volume": "100000000",
        "pre_event_volatility_bps": "100",
    }
    values.update(updates)
    return EventMarketState.model_validate(values)


def clear_context() -> OfficialContextSnapshot:
    return OfficialContextSnapshot(
        observed_at=NOW,
        evidence=(),
        macro_calendar_available_at=NOW - timedelta(minutes=1),
        macro_calendar_covers_until=NOW + timedelta(days=1),
        halts=(
            HaltStatus(
                symbol="AAPL",
                halted=False,
                available_at=NOW - timedelta(seconds=1),
                valid_until=NOW + timedelta(seconds=89),
                reason="synthetic_clear_context",
            ),
        ),
    )


def seed_decision(database: Database, replay: dict[str, Any]) -> tuple[EventStore, str]:
    store = EventStore(database)
    store.freeze(COHORT, "synthetic", {"evidence_kind": "synthetic"}, "shadow", NOW)
    evidence = SourceEvent.model_validate(replay["event"])
    store.evidence(
        evidence.evidence_id, evidence.model_dump(mode="json"), evidence.first_received_at
    )
    decision = {
        **replay["decision"],
        "mode": "shadow",
        "decided_at": NOW.isoformat(),
        "quote_snapshot": {
            **replay["decision"]["quote_snapshot"],
            "timestamp": NOW.isoformat(),
            "received_at": NOW.isoformat(),
        },
    }
    return store, store.decision(COHORT, evidence.evidence_id, decision, NOW)


def test_cli_registration_rejects_live_mode_and_unknown_commands() -> None:
    for command in event_cli.COMMANDS:
        parsed = arguments(command, "--cohort-id", COHORT)
        assert parsed.command == command
        assert parsed.cohort_id == COHORT
        assert parsed.output is None
    assert arguments("run").mode == "shadow"
    assert arguments("run", "--once").once
    with pytest.raises(SystemExit):
        arguments("run", "--mode", "live")
    assert event_cli.handle_event_command(argparse.Namespace(command="unrelated")) is False


def test_doctor_cli_preserves_missing_entitlements_and_unknown_inference(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    diagnostic = {
        "market_data": {
            "sip_latest_quote": {"accessible": False, "http_status": 403},
            "historical_sip": {"accessible": True},
        },
        "inference": {"provider": "none configured", "calls_daily_cap": 0},
        "live_execution_available": False,
    }
    probe = Mock(return_value=diagnostic)
    monkeypatch.setattr(event_cli, "source_capabilities", probe)
    assert invoke(capsys, "doctor") == diagnostic
    probe.assert_called_once_with()


def test_audit_execution_cli_preserves_report_evidence_boundaries(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    report = {
        "status": "historical_evidence_insufficient",
        "historical_results_valid": False,
        "untouched_holdouts_opened": False,
        "historical_reruns": 0,
        "findings": [{"reason": "raw_execution_evidence_missing"}],
    }
    audit = Mock(return_value=SimpleNamespace(model_dump=lambda **_: report))
    monkeypatch.setattr(event_cli, "run_execution_accounting_audit", audit)
    assert invoke(capsys, "audit-execution") == report
    audit.assert_called_once_with(Path.cwd())


def test_replay_and_extraction_cli_keep_synthetic_labels(
    replay: dict[str, Any], monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(event_cli, "replay_event_pipeline", lambda: replay)
    result = invoke(capsys, "event-replay")
    assert result["decision"]["action"] == "eligible"
    assert result["reconciled"]
    assert result["live_or_real_paper_orders_submitted"] == 0
    assert result["excluded_from_strategy_performance"]
    gold = invoke(capsys, "evaluate-extraction")
    assert gold["cases_count"] >= 18
    assert gold["type_and_field_count_accuracy"] == 1
    assert gold["production_accuracy"] is None
    assert gold["alpha_evidence"] is False
    assert any(row["id"] == "prompt_injection" and row["abstention"] for row in gold["cases"])


def test_risk_pause_is_persistent_and_keeps_reconciliation_available(
    memory_database: Database, capsys: pytest.CaptureFixture[str]
) -> None:
    result = invoke(capsys, "risk-pause", "--cohort-id", COHORT)
    repository = ProductionRepository(memory_database)
    assert repository.get_control(f"{COHORT}:pause") == "OPERATOR_PAUSE"
    assert repository.get_control("kill_switch") == "active"
    assert result == {"state": "paused", "risk_exits_and_reconciliation": "remain active"}


def test_experiment_freeze_idempotent_report_unqualified_and_changed_code_rejected(
    memory_database: Database,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    first = invoke(capsys, "experiment-freeze", "--cohort-id", COHORT)
    second = invoke(capsys, "experiment-freeze", "--cohort-id", COHORT)
    assert first == second
    with memory_database.begin() as connection:
        assert connection.scalar(select(func.count()).select_from(event_cohorts)) == 1
    report = invoke(capsys, "experiment-report", "--cohort-id", COHORT)
    assert report["cohort"]["manifest"]["config_hash"] == first["config_hash"]
    assert report["decision_count"] == 0
    assert report["orders"] == []
    assert report["prospective_diagnostics"]["dsr"] is None
    assert report["prospective_diagnostics"]["pbo"] is None
    monkeypatch.setattr(event_cli, "code_identity", lambda: "changed-code-sha")
    with pytest.raises(ValueError, match="cohort immutable"):
        event_cli.handle_event_command(arguments("experiment-freeze", "--cohort-id", COHORT))


def test_denied_latest_sip_cannot_issue_certificate_or_clear_kill_switch(
    memory_database: Database,
    replay: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    repository = ProductionRepository(memory_database)
    repository.set_control("kill_switch", "active")
    diagnostic = {
        "broker_healthy": True,
        "live_credential_environment_present": False,
        "broker_positions": 0,
        "broker_open_orders": 0,
        "market_data": {
            "sip_latest_quote": {"accessible": False, "http_status": 403},
            "historical_sip": {"accessible": True},
            "iex_latest_quote": {"accessible": True},
        },
        "assets": [{"fractionable": True, "tradable": True}],
    }
    broker = ReplayBroker(NOW)
    monkeypatch.setattr(event_cli, "source_capabilities", lambda: diagnostic)
    monkeypatch.setattr(event_cli, "replay_event_pipeline", lambda: replay)
    monkeypatch.setattr(
        event_cli,
        "OfficialContextClient",
        lambda: nullcontext(SimpleNamespace(poll=lambda **_: clear_context())),
    )
    monkeypatch.setattr("tradeagent.alpaca_paper.AlpacaPaperClient", lambda _: nullcontext(broker))
    result = invoke(
        capsys, "paper-preflight", "--cohort-id", COHORT, "--confirm-experimental-paper"
    )
    assert result["operational_certificate_issued"] is False
    assert result["mode"] == "shadow"
    assert result["blockers"] == ["frozen_policy_feed_entitlement"]
    assert result["certificate"]["establishes_edge"] is False
    assert repository.get_control("kill_switch") == "active"
    assert repository.get_control(f"{COHORT}:certificate") is None
    assert repository.get_control("v20:mechanics_attestation") is None
    assert broker.orders == {}
    assert EventStore(memory_database).report(COHORT)["cohort"] is not None


def test_run_shadow_once_runs_real_service_persists_abstention_and_releases_lease(
    memory_database: Database,
    replay: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    unverified = SourceEvent.model_validate(
        {
            **replay["event"],
            "source_event_id": "unverified-surface-event",
            "source": "synthetic_news",
            "is_primary_source": False,
            "issuer_id": None,
            "cik": None,
            "mapping_available_at": None,
            "content": None,
            "provider_symbols": ("AAPL",),
            "rights_profile": "metadata_only_synthetic",
        }
    )
    source_poll = Mock(return_value=(unverified,))
    source = SimpleNamespace(
        poll=source_poll,
        last_errors=(),
        capabilities={"sec_enabled": False, "primary_urls_configured": 0},
    )
    broker = ReplayBroker(NOW)
    market = SimpleNamespace(state=lambda symbol, now: market_state(observed_at=now), close=Mock())
    context = SimpleNamespace(
        poll=lambda **_: clear_context(), close=Mock(), capabilities={"synthetic": True}
    )
    monkeypatch.setattr(event_runtime, "EventSourceClient", lambda *_, **__: nullcontext(source))
    monkeypatch.setattr(event_runtime, "AlpacaPaperClient", lambda _: nullcontext(broker))
    monkeypatch.setattr(event_runtime, "EventMarketClient", lambda _: market)
    monkeypatch.setattr(event_runtime, "OfficialContextClient", lambda: context)
    result = invoke(capsys, "run", "--mode", "shadow", "--once", "--cohort-id", COHORT)
    assert result["mode"] == "shadow"
    assert result["events_received"] == 1
    assert source_poll.call_count == 1
    report = EventStore(memory_database).report(COHORT)
    assert report["decision_count"] == 1
    decision = report["decisions"][0]["payload"]
    assert decision["action"] == "abstain"
    assert "verified_primary_issuer" in decision["reasons"]
    assert report["orders"] == []
    assert broker.orders == {}
    market.close.assert_called_once_with()
    context.close.assert_called_once_with()
    repository = ProductionRepository(memory_database)
    assert repository.latest_heartbeat("tradeagent-event-worker") is not None
    assert not repository.refresh_worker_lock("tradeagent-event-worker", "nobody", observed_at=NOW)
    with memory_database.begin() as connection:
        assert connection.scalar(select(func.count()).select_from(event_evidence)) == 1


def test_event_product_api_reports_idle_then_persisted_state_and_stale_worker(
    memory_database: Database, replay: dict[str, Any]
) -> None:
    app = api.create_app(production_database_url="sqlite:///:memory:")
    with TestClient(app) as client:
        idle = client.get("/api/event-product").json()
        assert idle["state"] == "not_running"
        assert idle["live_execution_available"] is False
        assert idle["qualified"] is False
        assert client.post("/api/event-product").status_code == 405
        store, _ = seed_decision(memory_database, replay)
        repository = ProductionRepository(memory_database)
        repository.heartbeat(
            "tradeagent-event-worker",
            "surface-instance",
            {
                "state": "collecting",
                "mode": "shadow",
                "cohort_id": COHORT,
                "code_sha": "synthetic-code-sha",
                "blockers": ["EDGE_UNPROVEN"],
            },
            observed_at=NOW,
        )
        store.audit("performance", {"economic_paper_equity": "10000"}, NOW, COHORT)
        response = client.get("/api/event-product")
        assert response.status_code == 200
        active = response.json()
        assert active["state"] == "collecting"
        assert active["code_sha"] == "synthetic-code-sha"
        assert active["decision_count"] == 1
        assert active["ledgers"]["economic_paper_equity"] == "10000"
        assert active["blockers"] == ["EDGE_UNPROVEN"]
        assert active["qualified"] is False
        repository.heartbeat(
            "tradeagent-event-worker",
            "surface-instance",
            {"state": "collecting", "cohort_id": COHORT},
            observed_at=NOW - timedelta(seconds=121),
        )
        assert client.get("/api/event-product").json()["state"] == "worker_stale"


def test_market_client_preserves_raw_prices_sizes_and_skips_unfinished_bars() -> None:
    requests: list[httpx.Request] = []
    daily = [
        {
            "t": (NOW - timedelta(days=index + 1)).isoformat(),
            "o": "99",
            "h": "102",
            "l": "98",
            "c": "100",
            "v": "1000000",
        }
        for index in range(20)
    ]
    unfinished = {
        "t": (NOW - timedelta(minutes=1)).isoformat(),
        "o": "499",
        "h": "501",
        "l": "498",
        "c": "500",
        "v": "999999",
    }
    completed = {
        "t": (NOW - timedelta(minutes=5, seconds=3)).isoformat(),
        "o": "99",
        "h": "102",
        "l": "98",
        "c": "101",
        "v": "1234",
    }

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path.endswith("quotes/latest"):
            return httpx.Response(
                200,
                json={
                    "quotes": {
                        "AAPL": {
                            "t": NOW.isoformat(),
                            "bp": "100.09",
                            "ap": "100.11",
                            "bs": 7,
                            "as": 9,
                        }
                    }
                },
            )
        if request.url.path.endswith("trades/latest"):
            return httpx.Response(200, json={"trades": {}})
        rows = daily if request.url.params["timeframe"] == "1Day" else [unfinished, completed]
        return httpx.Response(200, json={"bars": rows})

    client = EventMarketClient(
        AlpacaDataSettings(
            key_id=SecretStr("synthetic-key"), secret_key=SecretStr("synthetic-secret"), feed="iex"
        ),
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    state = client.state("AAPL", NOW)
    assert state.feed == "iex"
    assert state.bid == Decimal("100.09")
    assert state.bid_size == 7 and state.ask_size == 9
    assert state.completed_bar is not None
    assert state.completed_bar.timestamp == NOW - timedelta(seconds=3)
    assert state.completed_bar.close == 101
    assert state.previous_close == 100
    assert state.median_daily_dollar_volume == 100_000_000
    assert state.pre_event_volatility_bps == 400
    bars = [request for request in requests if request.url.path.endswith("/bars")]
    assert all(request.url.params["adjustment"] == "raw" for request in bars)
    assert bars[0].url.params["feed"] == "sip"
    assert bars[1].url.params["feed"] == "iex"
    client.state("AAPL", NOW + timedelta(seconds=1))
    assert sum(request.url.params.get("timeframe") == "1Day" for request in requests) == 1


def test_future_quote_paths_are_causal_idempotent_and_never_broker_returns(
    memory_database: Database, replay: dict[str, Any]
) -> None:
    store, _ = seed_decision(memory_database, replay)
    target = NOW + timedelta(minutes=1)
    future = market_state(observed_at=target, quote_at=target, bid="101", ask="101.02")
    unavailable = future.model_copy(update={"observed_at": target + timedelta(minutes=1)})
    assert record_quote_paths(store, COHORT, {"AAPL": unavailable}, target) == 0
    assert record_quote_paths(store, COHORT, {"AAPL": future}, target - timedelta(seconds=1)) == 0
    assert record_quote_paths(store, COHORT, {"AAPL": future}, target + timedelta(seconds=6)) == 0
    assert record_quote_paths(store, COHORT, {"AAPL": future}, target) == 1
    assert record_quote_paths(store, COHORT, {"AAPL": future}, target) == 0
    five = NOW + timedelta(minutes=5)
    later = market_state(observed_at=five, quote_at=five, bid="99", ask="99.02")
    assert record_quote_paths(store, COHORT, {"AAPL": later}, five) == 1
    summary = outcome_summary(store, COHORT)
    assert summary["available_quote_paths"] == 2
    assert summary["hypothetical_not_broker_performance"]
    assert summary["dsr"] is None and summary["pbo"] is None
    first = next(row for row in summary["latest"] if row["horizon_minutes"] == 1)
    expected = (Decimal("101") / Decimal(replay["decision"]["quote_snapshot"]["ask"]) - 1) * 10000
    assert Decimal(first["spread_crossed_return_bps"]) == expected
    assert Decimal(first["omitted_cost_stress_bps"]["3x"]) == expected - 3
    assert first["expected_net_return_bps"] is None
    assert first["not_independent_trade_sample"]
    with memory_database.begin() as connection:
        assert connection.scalar(select(func.count()).select_from(event_decisions)) == 1
        assert connection.scalar(select(func.count()).select_from(event_outcomes)) == 2
