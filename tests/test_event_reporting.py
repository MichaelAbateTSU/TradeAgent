from __future__ import annotations

from collections.abc import Iterator
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, func, insert, select
from sqlalchemy.pool import StaticPool

from tradeagent import api
from tradeagent.daily_status import build_daily_status
from tradeagent.event_market import EventMarketState
from tradeagent.event_outcomes import event_outcomes, outcome_summary, record_quote_paths
from tradeagent.event_performance import allocation_ledgers
from tradeagent.event_reporting import (
    is_calibration,
    observation_labels,
    reporting_limitations,
    reporting_purpose,
)
from tradeagent.event_store import (
    EventStore,
    event_cohorts,
    event_decisions,
    event_evidence,
    event_order_links,
)
from tradeagent.persistence import Database, ProductionRepository, orders

NOW = datetime(2026, 9, 8, 14, 5, tzinfo=UTC)
COHORT = "isolated-practice"
MANIFEST = {
    "purpose": "iex-practice",
    "qualification_eligible": False,
    "evidence_use": "operational_practice_only",
    "settings": {"purpose": "iex-practice", "max_entry_notional": "25"},
}


@pytest.fixture
def database(monkeypatch: pytest.MonkeyPatch) -> Iterator[Database]:
    database = Database("sqlite:///:memory:")
    database.dispose()
    database.engine = create_engine(
        "sqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    database.initialize()
    monkeypatch.setattr(api, "Database", lambda _: nullcontext(database))
    monkeypatch.setattr(
        api,
        "ExperimentalSettings",
        lambda: SimpleNamespace(
            mode="experimental-paper",
            cohort_id=COHORT,
            model_dump=lambda: {"purpose": "research"},
        ),
    )

    class Clock(datetime):
        @classmethod
        def now(cls, tz: Any = None) -> datetime:
            return NOW.astimezone(tz or UTC)

    monkeypatch.setattr(api, "datetime", Clock)
    yield database
    database.dispose()


def decision(store: EventStore, cohort_id: str, key: str, **updates: Any) -> None:
    at = updates.pop("at", NOW)
    payload = {
        "action": "eligible",
        "mode": "experimental-paper",
        "symbol": "AAPL",
        "hypothesis": "H1",
        "issuer_id": "issuer",
        "event_cluster_id": key,
        "decided_at": at.isoformat(),
        "quote_snapshot": {"timestamp": at.isoformat(), "ask": "100"},
        **updates,
    }
    store.evidence(key, {}, at)
    store.decision(cohort_id, key, payload, at)


def filled_orders(*, calibration: bool = False) -> list[dict[str, Any]]:
    return [
        {
            "symbol": "AAPL",
            "side": side,
            "link": {
                "entry_kind": "calibration" if calibration else "strategy",
                "broker": {"filled_quantity": "0.25", "filled_average_price": price},
            },
        }
        for side, price in (("buy", "100"), ("sell", "102"))
    ]


@pytest.mark.parametrize(
    "sources, expected",
    [
        ((None, {}), "research"),
        (({"purpose": "research"},), "research"),
        (({"settings": {"purpose": "iex-practice"}},), "iex-practice"),
        (({"purpose": "iex-practice"}, {"purpose": "research"}), "iex-practice"),
    ],
)
def test_purpose_defaults_legacy_to_research(sources, expected) -> None:
    assert reporting_purpose(*sources) == expected


@pytest.mark.parametrize(
    "payload",
    [
        {"entry_kind": "calibration"},
        {"trade_classification": "calibration"},
        {"calibration": True},
    ],
)
def test_calibration_entry_kind_and_legacy_aliases_are_not_qualification_evidence(payload) -> None:
    assert is_calibration(payload)
    labels = observation_labels(payload, "research")
    assert labels["qualification_eligible"] is False
    assert labels["trade_classification"] == "calibration"
    assert not is_calibration({"entry_kind": "strategy"})
    assert not is_calibration({"calibration": {"state": "completed"}})


@pytest.mark.parametrize("purpose", ["research", "iex-practice"])
@pytest.mark.parametrize("field", ["blockers", "limitations", "capability_limitations"])
def test_current_no_inference_code_is_a_capability_limitation(purpose: str, field: str) -> None:
    code = "NO_CONFIGURED_INFERENCE_PROVIDER_DETERMINISTIC_ONLY"
    report = reporting_limitations(
        {
            "blockers": ["QUOTE_STALE"],
            field: [code, *(["QUOTE_STALE"] if field == "blockers" else [])],
        },
        purpose,
    )
    assert report["blockers"] == ["QUOTE_STALE"]
    assert report["capability_limitations"] == [code]
    assert code not in report["source_limitations"]


@pytest.mark.parametrize("calibration", [False, True])
def test_practice_preserves_dollars_but_cannot_fill_round_trip_floor(calibration: bool) -> None:
    rows = filled_orders(calibration=calibration) * 60
    research = allocation_ledgers(rows, {}, Decimal(10000), session_date=NOW.date())
    practice = allocation_ledgers(
        rows, {}, Decimal(10000), session_date=NOW.date(), purpose="iex-practice"
    )
    for key in ("broker_paper_pnl", "economic_paper_pnl", "broker_paper_equity"):
        assert practice[key] == research[key]
    assert Decimal(practice["broker_paper_pnl"]) == 30
    assert practice["closed_round_trips"] == 60
    assert practice["qualifying_closed_round_trips"] == 0
    assert research["qualifying_closed_round_trips"] == (0 if calibration else 60)
    assert practice["calibration_round_trips"] == (60 if calibration else 0)
    assert practice["qualification"] == "excluded_operational_practice"
    assert practice["qualification_eligible"] is False
    assert practice["validated_strategy_economics"] is False
    assert "single-venue" in practice["source_limitations"][0]
    assert "operational estimate only" in practice["economic_paper_pnl_label"]


def test_practice_is_labeled_without_fills_and_with_missing_marks() -> None:
    empty = allocation_ledgers(
        [], {}, Decimal(10000), session_date=NOW.date(), purpose="iex-practice"
    )
    assert empty["purpose"] == "iex-practice" and empty["qualification_eligible"] is False
    rows = filled_orders()[:1]
    rows[0]["link"]["purpose"] = "iex-practice"
    unvalued = allocation_ledgers(rows, {}, Decimal(10000), session_date=NOW.date())
    assert unvalued["state"] == "UNVALUED_POSITION"
    assert unvalued["broker_paper_pnl"] is None
    assert unvalued["economic_paper_pnl"] is None
    assert unvalued["qualifying_closed_round_trips"] == 0
    assert unvalued["qualification_eligible"] is False


def test_reports_exclude_practice_sessions_and_leave_frozen_research_unchanged(
    database: Database,
) -> None:
    store = EventStore(database)
    store.freeze("frozen", "research-hash", {"settings": {}}, "experimental-paper", NOW)
    store.freeze("frozen-practice", "practice-hash", MANIFEST, "experimental-paper", NOW)
    for index in range(100):
        at = NOW + timedelta(days=index, hours=1)
        for cohort_id in ("frozen", "frozen-practice"):
            decision(store, cohort_id, str(index), at=at)
    store.audit("performance", {"marker": "research"}, NOW, "frozen")
    store.audit("status", {"marker": "child"}, NOW, "frozen:cycle")
    store.audit("performance", {"marker": "practice"}, NOW, "frozen-practice")
    research = store.report("frozen")
    practice = store.report("frozen-practice")
    assert research["purpose"] == "research"
    assert research["usable_forward_trading_sessions"] >= 60
    assert practice["usable_forward_trading_sessions"] == 0
    assert practice["operational_practice_sessions"] >= 60
    assert practice["independent_event_clusters"] == 0
    assert practice["decision_count"] == 100
    assert all(row["payload"]["qualification_eligible"] is False for row in practice["decisions"])
    assert {row["payload"]["marker"] for row in research["timeline"]} == {"research", "child"}
    with database.begin() as connection:
        assert connection.scalar(
            select(event_cohorts.c.manifest).where(event_cohorts.c.cohort_id == "frozen")
        ) == {"settings": {}}
        assert "purpose" not in connection.scalar(select(event_decisions.c.payload))


def test_calibration_and_mistagged_practice_never_contribute_research_sessions(
    database: Database,
) -> None:
    store = EventStore(database)
    store.freeze(COHORT, "hash", {}, "experimental-paper", NOW)
    decision(store, COHORT, "calibration", trade_classification="calibration")
    decision(store, COHORT, "practice", purpose="iex-practice")
    report = store.report(COHORT)
    assert report["usable_forward_trading_sessions"] == 0
    assert report["independent_event_clusters"] == 0
    assert report["calibration_decision_count"] == 1
    assert all(row["payload"]["qualification_eligible"] is False for row in report["decisions"])


def test_calibration_order_labels_do_not_rewrite_broker_fill(database: Database) -> None:
    store = EventStore(database)
    store.freeze(COHORT, "hash", MANIFEST, "experimental-paper", NOW)
    link = filled_orders(calibration=True)[0]["link"]
    with database.begin() as connection:
        connection.execute(
            insert(orders).values(
                order_id="order",
                client_order_id="client",
                strategy_version=COHORT,
                symbol="AAPL",
                side="buy",
                quantity=Decimal("0.25"),
                filled_quantity=Decimal("0.25"),
                status="filled",
                created_at=NOW,
                updated_at=NOW,
            )
        )
        connection.execute(
            insert(event_order_links).values(
                client_order_id="client", cohort_id=COHORT, cluster_key="calibration", payload=link
            )
        )
    reported = store.report(COHORT)["orders"][0]["link"]
    assert reported["broker"] == link["broker"]
    assert reported["trade_classification"] == "calibration"
    assert reported["entry_kind"] == "calibration"
    assert reported["qualification_eligible"] is False
    assert store.linked_orders(COHORT)[0]["link"] == link


def test_practice_quote_paths_keep_factual_return_and_calibration_labels(
    database: Database,
) -> None:
    store = EventStore(database)
    store.freeze(COHORT, "hash", MANIFEST, "experimental-paper", NOW)
    decision(store, COHORT, "calibration", trade_classification="calibration")
    target = NOW + timedelta(minutes=1)
    state = EventMarketState(
        symbol="AAPL",
        observed_at=target,
        feed="iex",
        bid=Decimal(101),
        ask=Decimal("101.02"),
        bid_size=Decimal(10),
        ask_size=Decimal(10),
        quote_at=target,
        raw_quote={},
        raw_trade=None,
        completed_bar=None,
        previous_close=Decimal(100),
        median_daily_dollar_volume=Decimal(100000000),
        pre_event_volatility_bps=Decimal(100),
    )
    assert record_quote_paths(store, COHORT, {"AAPL": state}, target) == 1
    assert record_quote_paths(store, COHORT, {"AAPL": state}, target) == 0
    report = outcome_summary(store, COHORT)
    assert report["qualification_eligible"] is False
    assert report["qualifying_round_trips"] == report["qualifying_sessions"] == 0
    assert "excluded" in report["statistical_status"]
    assert report["dsr"] is None and report["pbo"] is None
    with database.begin() as connection:
        row = connection.scalar(select(event_outcomes.c.payload))
    assert row["purpose"] == "iex-practice"
    assert row["trade_classification"] == "calibration"
    assert row["qualification_eligible"] is False
    assert Decimal(row["spread_crossed_return_bps"]) == 100
    assert row["not_independent_trade_sample"]


@pytest.mark.parametrize("manifest", [MANIFEST, {"settings": {"purpose": "iex-practice"}}, {}])
def test_event_api_is_cohort_scoped_and_relabels_legacy_practice_payload(
    database: Database, manifest: dict[str, Any]
) -> None:
    store = EventStore(database)
    store.freeze(COHORT, "hash", manifest, "experimental-paper", NOW)
    decision(store, COHORT, "eligible")
    ProductionRepository(database).heartbeat(
        "tradeagent-event-worker",
        "owner",
        {
            "cohort_id": COHORT,
            "state": "collecting",
            "purpose": "iex-practice",
            "calibration_status": {"state": "completed"},
            "blockers": ["NO_CONFIGURED_INFERENCE_PROVIDER", "OPERATOR_PAUSE"],
            "source_limitations": ["SIP_LATEST_UNAVAILABLE"],
        },
        observed_at=NOW,
    )
    store.audit(
        "performance",
        {"broker_paper_pnl": "0.50", "economic_paper_pnl": "0.49", "closed_round_trips": 1},
        NOW - timedelta(seconds=1),
        COHORT,
    )
    store.audit("performance", {"broker_paper_pnl": "999999"}, NOW, "other-research")
    app = api.create_app(production_database_url="sqlite:///:memory:")
    with TestClient(app) as client:
        result = client.get("/api/event-product").json()
        html = client.get("/").text
    assert result["purpose"] == "iex-practice"
    assert result["qualification_eligible"] is False
    assert result["usable_forward_trading_sessions"] == 0
    assert result["ledgers"]["broker_paper_pnl"] == "0.50"
    assert result["ledgers"]["economic_paper_pnl"] == "0.49"
    assert result["ledgers"]["qualifying_closed_round_trips"] == 0
    assert result["calibration_status"]["state"] == "completed"
    assert result["blockers"] == ["OPERATOR_PAUSE"]
    assert result["capability_limitations"] == ["NO_CONFIGURED_INFERENCE_PROVIDER"]
    assert "SIP_LATEST_UNAVAILABLE" in result["source_limitations"]
    assert "not validated strategy economics" in html
    assert "Source and capability limitations" in html
    assert "product.calibration_status" in html


def test_event_api_never_uses_other_cohort_pnl_when_current_has_none(database: Database) -> None:
    store = EventStore(database)
    store.freeze(COHORT, "hash", MANIFEST, "experimental-paper", NOW)
    ProductionRepository(database).heartbeat(
        "tradeagent-event-worker", "owner", {"cohort_id": COHORT}, observed_at=NOW
    )
    store.audit("performance", {"broker_paper_pnl": "999999"}, NOW, "other-research")
    with TestClient(api.create_app(production_database_url="sqlite:///:memory:")) as client:
        assert client.get("/api/event-product").json()["ledgers"] is None


def test_runtime_calibration_contract_reports_audit_facts_not_source_evidence(
    database: Database,
) -> None:
    store = EventStore(database)
    store.freeze(COHORT, "hash", MANIFEST, "experimental-paper", NOW)
    calibration = {"state": "completed", "entry_attempts": 1, "reasons": []}
    runtime_limitations = [
        "NO_CONFIGURED_INFERENCE_PROVIDER_DETERMINISTIC_ONLY",
        "IEX_PRACTICE_ONLY_NOT_QUALIFICATION_EVIDENCE",
    ]
    details = {
        "cohort_id": COHORT,
        "state": "collecting",
        "mode": "experimental-paper",
        "purpose": "iex-practice",
        "qualification_eligible": False,
        "execution_feed": "iex",
        "practice_start_date": "2026-09-08",
        "calibration": calibration,
        "limitations": runtime_limitations,
        "blockers": [],
    }
    ProductionRepository(database).heartbeat(
        "tradeagent-event-worker", "owner", details, observed_at=NOW
    )
    store.audit("calibration_status", calibration, NOW, COHORT)
    rows = filled_orders(calibration=True)
    for row in rows:
        row["link"].update(purpose="iex-practice", qualification_eligible=False)
    performance = allocation_ledgers(rows, {}, Decimal(10000), session_date=NOW.date())
    store.audit("performance", performance, NOW, COHORT)

    report = store.report(COHORT)
    assert report["decision_count"] == report["calibration_decision_count"] == 0
    assert report["usable_forward_trading_sessions"] == report["independent_event_clusters"] == 0
    audit = next(
        row for row in report["timeline"] if row["event_type"] == "event_calibration_status"
    )
    assert audit["payload"]["state"] == "completed"
    assert audit["payload"]["entry_attempts"] == 1
    assert audit["payload"]["not_source_event"] is True
    assert audit["payload"]["qualification_eligible"] is False
    assert outcome_summary(store, COHORT)["available_quote_paths"] == 0

    with TestClient(api.create_app(production_database_url="sqlite:///:memory:")) as client:
        result = client.get("/api/event-product").json()
    assert result["calibration"] == result["calibration_status"] == calibration
    assert result["execution_feed"] == "iex"
    assert result["practice_start_date"] == "2026-09-08"
    assert result["blockers"] == []
    assert result["capability_limitations"] == [runtime_limitations[0]]
    assert runtime_limitations[1] in result["source_limitations"]
    assert result["ledgers"]["closed_round_trips"] == 1
    assert result["ledgers"]["calibration_round_trips"] == 1
    assert result["ledgers"]["qualifying_closed_round_trips"] == 0
    assert Decimal(result["ledgers"]["broker_paper_pnl"]) == Decimal("0.50")

    summary = build_daily_status(database, NOW, "America/New_York")
    text = summary["text"]
    assert summary["calibration"] == calibration
    assert summary["blockers"] == []
    assert "Execution feed (last reported): iex" in text
    assert "Practice start date: 2026-09-08" in text
    assert "Decisions: 0; eligible: 0; abstained: 0" in text
    assert "Calibration status (last reported): completed" in text
    assert "Calibration entry attempts (last reported): 1" in text
    assert "Calibration reasons (last reported): none" in text
    assert "not source events or qualification evidence" in text
    assert "Calibration round trips (operational only): 1" in text
    assert "deterministic extractor" in text
    assert "Resolve latest-SIP access" not in text
    assert all(value in text for value in runtime_limitations)
    with database.begin() as connection:
        assert connection.scalar(select(func.count()).select_from(event_evidence)) == 0
        assert connection.scalar(select(func.count()).select_from(event_decisions)) == 0


@pytest.mark.parametrize("purpose_source", ["manifest", "settings", "heartbeat"])
def test_daily_practice_email_separates_limitations_and_excludes_qualification(
    database: Database, purpose_source: str
) -> None:
    store = EventStore(database)
    manifest = {
        "manifest": MANIFEST,
        "settings": {"settings": {"purpose": "iex-practice", "max_entry_notional": "25"}},
        "heartbeat": {"settings": {"max_entry_notional": "25"}},
    }[purpose_source]
    store.freeze(COHORT, "hash", manifest, "experimental-paper", NOW)
    decision(store, COHORT, "calibration", trade_classification="calibration")
    details = {
        "cohort_id": COHORT,
        "mode": "experimental-paper",
        "state": "collecting",
        "blockers": ["NO_CONFIGURED_INFERENCE_PROVIDER", "IEX_QUOTE_STALE"],
        "source_limitations": ["SIP_LATEST_UNAVAILABLE"],
        "calibration_status": "completed",
        **({"purpose": "iex-practice"} if purpose_source == "heartbeat" else {}),
    }
    ProductionRepository(database).heartbeat(
        "tradeagent-event-worker", "owner", details, observed_at=NOW
    )
    store.audit(
        "performance",
        {
            "positions": {},
            "broker_paper_pnl": "0.50",
            "economic_paper_pnl": "0.49",
            "closed_round_trips": 1,
            "calibration_round_trips": 1,
        },
        NOW,
        COHORT,
    )
    summary = build_daily_status(database, NOW, "America/New_York")
    text = summary["text"]
    assert summary["purpose"] == "iex-practice"
    assert summary["qualification_eligible"] is False
    assert summary["blockers"] == ["IEX_QUOTE_STALE"]
    assert "Broker-paper cumulative P&L: 0.50 USD" in text
    assert "operational estimate only): 0.49 USD" in text
    assert "Economic-paper cumulative P&L:" not in text
    assert "SIP is not required for this isolated practice cohort" in text
    assert "Resolve latest-SIP access" not in text
    assert "single-venue" in text and "not consolidated SIP/NBBO" in text
    assert "Calibration status (last reported): completed" in text
    assert "Calibration round trips (operational only): 1" in text
    assert "research-qualifying round trips: 0" in text
    assert "excluded from research qualification and its 60-session/60-round-trip floor" in text
    blockers = text.split("BLOCKERS / WHY IT IS NOT TRADING")[1].split(
        "SOURCE / CAPABILITY LIMITATIONS"
    )[0]
    assert "NO_CONFIGURED_INFERENCE_PROVIDER" not in blockers
    assert "deterministic extractor" in text
