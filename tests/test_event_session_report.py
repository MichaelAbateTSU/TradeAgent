from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import insert, select, update

from tradeagent.daily_status import DailyStatusScheduler, DailyStatusSettings
from tradeagent.event_performance import (
    COST_MODEL_VERSION,
    allocation_ledgers,
    cost_assumptions,
    paper_cost_assumptions,
)
from tradeagent.event_reporting import observation_labels
from tradeagent.event_session import equipment_identity, session_control_key, session_identity
from tradeagent.event_session_report import render_session_report, report_delivery, session_report
from tradeagent.event_store import (
    EventStore,
    event_candidate_states,
    event_decisions,
    event_order_links,
)
from tradeagent.persistence import (
    Database,
    ProductionRepository,
    events,
    fills,
    notification_outbox,
    orders,
)

DAY = date(2026, 9, 8)
OPEN = datetime(2026, 9, 8, 13, 35, tzinfo=UTC)
CLOSE = datetime(2026, 9, 8, 22, tzinfo=UTC)
COHORT = "tuesday-paper-test-new"
MANIFEST = {
    "purpose": "iex-practice",
    "settings": {
        "purpose": "iex-practice",
        "planned_session_date": str(DAY),
        "practice_start_date": str(DAY),
        "virtual_equity": "10000",
        "max_entry_notional": "25",
    },
    "benchmarks": ["cash", "passive_AAPL"],
}


@pytest.fixture
def database() -> Iterator[Database]:
    with Database("sqlite:///:memory:") as database:
        database.initialize()
        EventStore(database).freeze(COHORT, "new-hash", MANIFEST, "experimental-paper", OPEN)
        yield database


def fill(
    identity: str,
    side: str,
    quantity: str,
    price: str,
    classification: str,
    *,
    status: str = "filled",
    requested: str | None = None,
) -> dict[str, Any]:
    return {
        "order_id": identity,
        "client_order_id": identity,
        "strategy_version": COHORT,
        "symbol": "AAPL",
        "side": side,
        "quantity": Decimal(requested or quantity),
        "filled_quantity": Decimal(quantity),
        "status": status,
        "created_at": OPEN,
        "updated_at": OPEN + timedelta(minutes=2),
        "link": {
            "purpose": "iex-practice",
            "trade_classification": classification,
            "entry_kind": "calibration" if classification == "EQUIPMENT_TEST" else "strategy",
            "qualification_eligible": False,
            "planned_session_date": str(DAY),
            "equipment_test_id": (
                "persistent-equipment-test" if classification == "EQUIPMENT_TEST" else None
            ),
            "broker": {
                "order_id": f"broker-{identity}",
                "filled_quantity": quantity,
                "filled_average_price": price,
                "status": status,
            },
        },
    }


def save_orders(
    database: Database,
    rows: list[dict[str, Any]],
    *,
    cohort_id: str = COHORT,
) -> None:
    with database.begin() as connection:
        for row in rows:
            connection.execute(
                insert(orders).values(**{k: v for k, v in row.items() if k != "link"})
            )
            connection.execute(
                insert(event_order_links).values(
                    client_order_id=row["client_order_id"],
                    cohort_id=cohort_id,
                    cluster_key=row["client_order_id"],
                    payload=row["link"],
                )
            )


def worker(database: Database, *, now: datetime = CLOSE) -> None:
    ProductionRepository(database).heartbeat(
        "tradeagent-event-worker",
        "owner",
        {
            "cohort_id": COHORT,
            "purpose": "iex-practice",
            "state": "collecting",
            "planned_session_date": str(DAY),
            "execution_feed": "iex",
        },
        observed_at=now,
    )


def poll(database: Database, *, healthy: bool = True, counts: dict[str, int] | None = None) -> None:
    EventStore(database).audit(
        "source_poll",
        {
            "planned_session_date": str(DAY),
            "count_semantics": "per_poll",
            "counts": counts
            or {
                "items_received": 0,
                "unique_events": 0,
                "supported_company_matches": 0,
                "valid_quantitative_events": 0,
            },
            "healthy": healthy,
            "errors": [] if healthy else ["provider_subscription_failed"],
            "coverage": {"AAPL": "checked", "MSFT": "checked", "NVDA": "checked"},
        },
        CLOSE,
        COHORT,
    )


def test_equipment_gains_never_offset_news_losses_or_delete_losing_trades() -> None:
    rows = [
        fill("1", "buy", ".25", "100", "EQUIPMENT_TEST"),
        fill("2", "sell", ".25", "104", "EQUIPMENT_TEST"),
        fill("3", "buy", ".2", "100", "NEWS_STRATEGY"),
        fill("4", "sell", ".2", "98", "NEWS_STRATEGY"),
        fill("5", "buy", ".1", "100", "NEWS_STRATEGY"),
        fill("6", "sell", ".1", "101", "NEWS_STRATEGY"),
    ]
    result = allocation_ledgers(rows, {}, Decimal(10000), session_date=DAY, purpose="iex-practice")
    assert Decimal(result["broker_paper_pnl"]) == Decimal(".70")
    assert Decimal(result["equipment_test"]["broker_paper_pnl"]) == 1
    news = result["news_strategy"]
    assert Decimal(news["broker_paper_pnl"]) == Decimal("-.30")
    assert [Decimal(trade["broker_paper_pnl"]) for trade in news["trades"]] == [
        Decimal("-.40"),
        Decimal(".10"),
    ]
    assert news["closed_round_trips"] == 2
    assert result["calibration_round_trips"] == 1
    assert news["qualifying_closed_round_trips"] == result["qualifying_closed_round_trips"] == 0
    assert news["sharpe"] is None and news["qualification_eligible"] is False
    assert all(trade["thesis_invalidated"] is None for trade in news["trades"])
    assert (
        observation_labels({"trade_classification": "EQUIPMENT_TEST"}, "research")[
            "trade_classification"
        ]
        == "EQUIPMENT_TEST"
    )


def test_cost_version_partial_fills_and_denominators_preserve_original_base() -> None:
    rows = [
        fill("entry", "buy", ".1", "100", "NEWS_STRATEGY", status="canceled", requested=".25"),
        fill("partial-exit", "sell", ".04", "102", "NEWS_STRATEGY"),
        fill("final-exit", "sell", ".06", "101", "NEWS_STRATEGY"),
    ]
    result = allocation_ledgers(
        [*rows, rows[0]], {}, Decimal(10000), session_date=DAY, purpose="iex-practice"
    )
    news = result["news_strategy"]
    assert Decimal(news["broker_paper_pnl"]) == Decimal(".14")
    residual = Decimal("20.14") * Decimal(".00005")
    assert Decimal(news["omitted_slippage_assumption"]) == residual
    assert Decimal(news["regulatory_fee_reserve"]) == Decimal(".02")
    base = Decimal(".14") - residual - Decimal(".02")
    assert Decimal(news["economic_paper_pnl"]) == base
    assert len(news["execution_costs"]) == 3
    assert news["execution_costs"][0]["partially_filled"] is True
    assert news["actual_deployed_notional"] == "10.0"
    assert Decimal(news["economic_return_on_deployed_notional"]) == base / 10
    assert Decimal(news["economic_return_on_strategy_capital"]) == base / 10000
    assert Decimal(news["stress_scenarios"]["3x"]["net_pnl"]) == (
        Decimal(".14") - residual * 3 - Decimal(".02")
    )
    assert cost_assumptions()["version"] == COST_MODEL_VERSION
    assert cost_assumptions()["additional_spread_deduction"] == "0"
    assert news["passive_benchmark_pnl"] is None
    assert news["cash_baseline_pnl"] == "0"


def test_open_position_does_not_hide_completed_losing_trade_or_invent_mark() -> None:
    rows = [
        fill("1", "buy", ".1", "100", "NEWS_STRATEGY"),
        fill("2", "sell", ".1", "99", "NEWS_STRATEGY"),
        fill("3", "buy", ".1", "100", "NEWS_STRATEGY"),
    ]
    result = allocation_ledgers(rows, {}, None, session_date=DAY, purpose="iex-practice")
    assert result["broker_paper_pnl"] is None
    assert result["news_strategy"]["trades"][0]["broker_paper_pnl"] == "-0.1"
    assert result["news_strategy"]["trades"][1]["broker_paper_pnl"] is None
    assert result["news_strategy"]["positions"] == {"AAPL": "0.1"}
    assert result["news_strategy"]["strategy_capital_denominator"] is None


@pytest.mark.parametrize("price", [None, "", "None", "invalid", "NaN", "Infinity", "0", "-1"])
@pytest.mark.parametrize("marked", [False, True])
def test_unpriced_entry_preserves_quantity_without_substituting_a_mark(
    price: str | None,
    marked: bool,
) -> None:
    row = fill("unpriced", "buy", ".1", "100", "NEWS_STRATEGY")
    row["link"]["broker"]["filled_average_price"] = price
    result = allocation_ledgers(
        [row],
        {"AAPL": Decimal(100)} if marked else {},
        Decimal(10000),
        session_date=DAY,
        purpose="iex-practice",
    )
    assert result["state"] == "UNVALUED_POSITION"
    assert result["positions"] == {"AAPL": "0.1"}
    assert result["fill_prices_complete"] is False
    assert result["unvalued_fills"][0]["filled_quantity"] == "0.1"
    assert result["execution_costs"][0]["filled_average_price"] is None
    assert result["execution_costs"][0]["filled_notional"] is None
    assert result["execution_costs"][0]["total_additional_cost"] is None
    for field in (
        "broker_paper_pnl",
        "economic_paper_pnl",
        "broker_paper_equity",
        "economic_paper_equity",
        "actual_deployed_notional",
        "turnover",
        "broker_return_on_deployed_notional",
        "economic_return_on_strategy_capital",
        "omitted_slippage_assumption",
    ):
        assert result[field] is None, field
    assert all(
        stress["net_pnl"] is None and stress["additional_execution_cost"] is None
        for stress in result["stress_scenarios"].values()
    )
    assert result["news_strategy"]["trades"][0]["positions"] == {"AAPL": "0.1"}
    assert result["qualifying_closed_round_trips"] == 0


def test_unpriced_exit_is_closed_by_quantity_but_not_valued_or_qualification_credit() -> None:
    rows = [
        fill("entry", "buy", ".1", "100", "NEWS_STRATEGY"),
        fill("exit", "sell", ".1", "99", "NEWS_STRATEGY"),
    ]
    for row in rows:
        row["link"]["purpose"] = "research"
    del rows[1]["link"]["broker"]["filled_average_price"]
    result = allocation_ledgers(rows, {}, Decimal(10000), session_date=DAY)
    assert result["positions"] == {}
    assert result["closed_round_trips"] == 1
    assert result["qualifying_closed_round_trips"] == 0
    assert result["actual_deployed_notional"] == "10.0"
    assert result["exit_filled_notional"] is None
    assert result["regulatory_fee_reserve"] is None
    assert result["broker_paper_pnl"] is None
    assert result["economic_paper_pnl"] is None
    trade = result["news_strategy"]["trades"][0]
    assert trade["state"] == "closed"
    assert trade["valuation_state"] == "UNVALUED_POSITION"
    assert trade["execution_costs"][1]["filled_quantity"] == "0.1"
    assert trade["execution_costs"][1]["regulatory_fee_reserve"] is None
    rows[1]["link"]["broker"]["filled_average_price"] = "99"
    recovered = allocation_ledgers(rows, {}, Decimal(10000), session_date=DAY)
    assert recovered["state"] == "valued"
    assert recovered["broker_paper_pnl"] == "-0.1"
    assert recovered["fill_prices_complete"] is True
    assert recovered["unvalued_fills"] == []
    assert recovered["qualifying_closed_round_trips"] == 1


@pytest.mark.parametrize("classification", ["NEWS_STRATEGY", "EQUIPMENT_TEST"])
def test_daily_report_persists_unpriced_fills_and_known_losses_with_no_worker(
    database: Database,
    classification: str,
) -> None:
    rows = [
        fill("1", "buy", ".1", "100", "NEWS_STRATEGY"),
        fill("2", "sell", ".1", "99", "NEWS_STRATEGY"),
        fill("3", "buy", ".1", "100", classification),
    ]
    rows[2]["link"]["broker"]["filled_average_price"] = None
    save_orders(database, rows)
    assert DailyStatusScheduler(database, DailyStatusSettings(_env_file=None)).enqueue_due(
        observed_at=CLOSE
    )
    with database.begin() as connection:
        notification = connection.execute(select(notification_outbox)).mappings().one()
        report = connection.scalar(
            select(events.c.payload).where(
                events.c.event_id == notification["payload"]["session_report_id"]
            )
        )
    assert report is not None
    unvalued = report["all_execution_economics"]["separate_ledgers"][classification]
    assert unvalued["broker_paper_pnl"] is None
    assert unvalued["positions"] == {"AAPL": "0.1"}
    assert report["news_strategy"]["trades"][0]["broker_paper_pnl"] == "-0.1"
    assert unvalued["trades"][-1]["broker_paper_pnl"] is None
    summary = notification["payload"]
    assert summary["completed_round_trips"] == 1
    assert len(summary["text"].split("\n\n")) == 5
    assert "a loss of $0.11" in summary["text"]
    assert "exact final net result is not confirmed" in summary["text"]
    assert "No completed buy-and-sell trades" not in summary["text"]
    if classification == "EQUIPMENT_TEST":
        assert report["news_strategy"]["broker_paper_pnl"] == "-0.1"
    rendered = render_session_report(report)
    assert "Unvalued broker fills (known quantities)" in rendered
    assert "missing_or_invalid_broker_filled_average_price" in rendered
    assert "unknown / not recorded" in rendered
    assert any("Reconcile missing broker fill prices" in item for item in report["next_actions"])


def test_healthy_silence_requires_provider_and_worker_evidence(database: Database) -> None:
    unknown = session_report(database, COHORT, observed_at=CLOSE)
    assert unknown["health"]["state"] == "unknown_or_stale"
    assert unknown["funnel"]["items_received"]["count"] is None
    assert unknown["ending_exposure"]["positions"] is None
    assert unknown["ending_exposure"]["broker_open_orders"] is None
    assert unknown["ending_exposure"]["completion_confirmed"] is False
    worker(database)
    poll(database)
    healthy = session_report(database, COHORT, observed_at=CLOSE)
    assert healthy["no_news_trade_status"] == "healthy_no_qualifying_event"
    assert healthy["funnel"]["items_received"]["count"] == 0
    assert healthy["funnel"]["risk_approved_decisions"]["count"] is None
    poll(database, healthy=False)
    failed = session_report(database, COHORT, observed_at=CLOSE)
    assert failed["health"]["state"] == "error"
    assert failed["no_news_trade_status"] == "unknown_or_failed_processing_not_healthy_silence"


def test_persisted_report_keeps_equipment_news_tickets_and_unresolved_orders(
    database: Database,
) -> None:
    worker(database)
    poll(
        database,
        counts={
            "items_received": 4,
            "unique_events": 3,
            "supported_company_matches": 2,
            "valid_quantitative_events": 1,
        },
    )
    store = EventStore(database)
    source = {
        "source_url": "https://issuer.example.test/release",
        "publisher": "Issuer",
        "published_at": OPEN.isoformat(),
        "first_received_at": (OPEN + timedelta(seconds=1)).isoformat(),
        "facts": {"old_guidance": "100", "new_guidance": "110"},
    }
    store.evidence("event-a", source, OPEN)
    decision_id = store.decision(
        COHORT,
        "event-a",
        {
            "action": "eligible",
            "symbol": "AAPL",
            "facts": source["facts"],
            "rule_results": {"guidance": {"passed": True, "observed": "10%"}},
            "reasons": [],
            "quote_snapshot": None,
        },
        OPEN,
    )
    ticket = {
        "decision_id": decision_id,
        "evidence_id": "event-a",
        "thesis": "Guidance increased",
        "expected_net_edge": None,
        "cost_assumptions": cost_assumptions(),
        "stop_rule": "frozen stop",
        "session_exit_deadline": "2026-09-08T15:45:00-04:00",
    }
    rows = [
        fill("equipment-entry", "buy", ".1", "100", "EQUIPMENT_TEST"),
        fill("equipment-exit", "sell", ".1", "101", "EQUIPMENT_TEST"),
        fill("news-entry", "buy", ".1", "100", "NEWS_STRATEGY"),
        fill("news-exit", "sell", ".1", "99", "NEWS_STRATEGY"),
        fill("pending", "buy", "0", "0", "NEWS_STRATEGY", status="pending_cancel", requested=".1"),
    ]
    rows[2]["link"]["decision_ticket"] = ticket
    save_orders(database, rows)
    store.audit("decision_ticket", {**ticket, "risk_approved": True}, OPEN, COHORT)
    store.audit(
        "candidate_selection",
        {"alternatives": ["event-a", "event-b"], "selected": "event-a", "rule": "frozen ordering"},
        OPEN,
        COHORT,
    )
    store.audit(
        "calibration_status",
        {"state": "completed", "equipment_test_id": "persistent"},
        OPEN,
        COHORT,
    )
    store.audit(
        "reconciliation",
        {"positions": [], "open_orders": [{"client_order_id": "pending"}]},
        CLOSE,
        COHORT,
    )
    report = session_report(database, COHORT, observed_at=CLOSE, persist=True)
    assert report["equipment_test"]["economics"]["broker_paper_pnl"] == "0.1"
    assert report["news_strategy"]["broker_paper_pnl"] == "-0.1"
    assert report["news_decisions"][0]["source_url"] == source["source_url"]
    assert report["news_decisions"][0]["decision_ticket"] == {**ticket, "risk_approved": True}
    assert report["news_decisions"][0]["timestamps"]["provider_received_at"] is None
    assert report["candidate_selections"][0]["evidence"]["alternatives"] == ["event-a", "event-b"]
    assert report["funnel"]["items_received"]["count"] == 4
    assert report["funnel"]["risk_approved_decisions"]["count"] == 1
    assert report["ending_exposure"]["completion_confirmed"] is False
    pending = report["ending_exposure"]["pending_or_uncertain_local_orders"]
    assert pending[0]["status"] == "pending_cancel"
    assert report["prospective_diagnostics"]["missing_candidate_horizons"] == 4
    assert report["news_strategy"]["trades"][0]["thesis_invalidated"] is None
    assert "NEWS_STRATEGY ECONOMICS" in render_session_report(report)
    with database.begin() as connection:
        saved = connection.scalar(
            select(events.c.payload).where(events.c.event_id == report["report_id"])
        )
    assert saved == report
    assert session_report(database, COHORT, observed_at=CLOSE, persist=True) == report


def test_report_survives_email_failure_without_duplicate_mail(database: Database) -> None:
    scheduler = DailyStatusScheduler(database, DailyStatusSettings(_env_file=None))
    assert scheduler.enqueue_due(observed_at=CLOSE)
    with database.begin() as connection:
        row = connection.execute(select(notification_outbox)).mappings().one()
        report_id = row["payload"]["session_report_id"]
        saved = connection.scalar(select(events.c.payload).where(events.c.event_id == report_id))
        assert saved is not None and saved["health"]["state"] == "unknown_or_stale"
        connection.execute(update(notification_outbox).values(status="failed", attempts=1))
    assert not DailyStatusScheduler(database, DailyStatusSettings(_env_file=None)).enqueue_due(
        observed_at=CLOSE + timedelta(minutes=1)
    )
    assert report_delivery(database, COHORT, DAY)[0]["delivery_failure_or_uncertainty"] is True
    with database.begin() as connection:
        assert (
            connection.scalar(select(events.c.payload).where(events.c.event_id == report_id))
            == saved
        )
        assert len(connection.execute(select(notification_outbox)).all()) == 1


def test_other_sessions_and_synthetic_rows_do_not_become_tuesday_trades(database: Database) -> None:
    rows = [fill("synthetic", "buy", ".1", "100", "NEWS_STRATEGY")]
    rows[0]["link"]["synthetic"] = True
    save_orders(database, rows)
    store = EventStore(database)
    store.audit("source_poll", {"healthy": True, "synthetic": True}, CLOSE, COHORT)
    store.audit(
        "source_poll",
        {
            "healthy": True,
            "planned_session_date": "2026-09-09",
            "counts": {"items_received": 999},
            "count_semantics": "per_poll",
        },
        CLOSE,
        COHORT,
    )
    report = session_report(database, COHORT, observed_at=CLOSE)
    assert report["excluded_synthetic_or_replay"]["orders"] == ["synthetic"]
    assert report["all_execution_economics"]["closed_round_trips"] == 0
    assert report["news_strategy"]["trades"] == []
    assert report["funnel"]["items_received"]["count"] is None
    before = session_report(database, COHORT, observed_at=OPEN - timedelta(days=1))
    after = session_report(database, COHORT, observed_at=CLOSE + timedelta(days=2))
    assert before["session_state"] == "NOT_STARTED"
    assert after["planned_session_date"] == str(DAY)
    assert after["session_state"] == "MISSED"


def test_both_confirmed_positions_and_orders_required_for_completion(database: Database) -> None:
    store = EventStore(database)
    store.audit("session_completion", {"positions": []}, CLOSE, COHORT)
    report = session_report(database, COHORT, observed_at=CLOSE)
    assert not report["ending_exposure"]["completion_confirmed"]
    store.audit("reconciliation", {"positions": [], "open_orders": []}, CLOSE, COHORT)
    report = session_report(database, COHORT, observed_at=CLOSE)
    assert report["ending_exposure"]["completion_confirmed"] is True
    assert report["ending_exposure"]["last_broker_confirmation_at"] == CLOSE.isoformat()
    assert report["session_state"] == "COMPLETE_AT_LAST_BROKER_CONFIRMATION"


@pytest.mark.parametrize(
    "unresolved",
    [
        {"unconfirmed_local_orders": ["uncertain-client-id"]},
        {"mismatches": ["owned_quantity_mismatch"]},
        {"state": "unresolved"},
        {"state": "awaiting_confirmation"},
        {"state": "reconciliation_required"},
        {"broker_confirmed": False},
    ],
)
def test_flat_broker_snapshot_is_not_completed_recovery_with_unresolved_evidence(
    database: Database,
    unresolved: dict[str, Any],
) -> None:
    store = EventStore(database)
    store.audit(
        "reconciliation",
        {"positions": [], "open_orders": []},
        CLOSE - timedelta(seconds=1),
        COHORT,
    )
    payload = {
        "positions": [],
        "open_orders": [],
        "state": "complete",
        "unconfirmed_local_orders": [],
        "mismatches": [],
        **unresolved,
    }
    store.audit("session_completion", payload, CLOSE, COHORT)
    report = session_report(database, COHORT, observed_at=CLOSE)
    exposure = report["ending_exposure"]
    assert exposure["positions"] == []
    assert exposure["broker_open_orders"] == []
    assert exposure["completion_confirmed"] is False
    assert exposure["has_unresolved_reconciliation"] is True
    assert exposure["unconfirmed_local_orders"] == payload["unconfirmed_local_orders"]
    assert exposure["mismatches"] == payload["mismatches"]
    assert exposure["latest_reconciliation"]["evidence"] == payload
    assert report["session_state"] == "UNRESOLVED_OR_IN_PROGRESS"


def test_heartbeat_summaries_do_not_replace_durable_source_or_budget_evidence(
    database: Database,
) -> None:
    budget = {"total_entries_reserved": 2, "news_entries_reserved": 1}
    brief = {"funnel": {"raw_items_received": 999}}
    stream = {"state": "connected", "last_message_at": CLOSE.isoformat()}
    ProductionRepository(database).heartbeat(
        "tradeagent-event-worker",
        "owner",
        {
            "cohort_id": COHORT,
            "purpose": "iex-practice",
            "session_budget": budget,
            "premarket_brief": brief,
            "broker_stream": stream,
        },
        observed_at=CLOSE,
    )
    report = session_report(database, COHORT, observed_at=CLOSE)
    assert report["runtime_session_budget_summary"] == budget
    assert report["runtime_premarket_brief_summary"] == brief
    assert report["broker_stream_health"] == stream
    assert report["session_budget"]["total_entries_reserved"] is None
    assert report["funnel"]["items_received"]["count"] is None
    assert report["premarket_brief"] is None
    assert report["ending_exposure"]["completion_confirmed"] is False


def test_counter_semantics_missing_or_cumulative_remain_unknown(database: Database) -> None:
    store = EventStore(database)
    for values in ({"items_received": 8}, {"items_received": 10, "count_semantics": "cumulative"}):
        store.audit("source_poll", values, CLOSE, COHORT)
    report = session_report(database, COHORT, observed_at=CLOSE)
    assert report["funnel"]["items_received"]["count"] is None


def test_snapshot_does_not_use_later_mutated_order_state(database: Database) -> None:
    row = fill(str(uuid4()), "buy", ".1", "100", "NEWS_STRATEGY")
    row["updated_at"] = CLOSE + timedelta(minutes=1)
    save_orders(database, [row])
    report = session_report(database, COHORT, observed_at=CLOSE)
    assert report["order_states_unavailable_at_snapshot"] == [row["client_order_id"]]
    assert report["ending_exposure"]["completion_confirmed"] is False
    assert report["news_strategy"]["trades"] == []


def test_individual_fill_count_is_not_invented_from_cumulative_vwap(database: Database) -> None:
    rows = [
        fill("entry", "buy", ".1", "100", "NEWS_STRATEGY"),
        fill("exit", "sell", ".1", "99", "NEWS_STRATEGY"),
    ]
    save_orders(database, rows)
    missing = session_report(database, COHORT, observed_at=CLOSE)
    assert missing["funnel"]["filled_orders"]["count"] == 2
    assert missing["funnel"]["fills"]["count"] is None
    with database.begin() as connection:
        for index, (order_id, quantity, price) in enumerate(
            [("entry", ".04", "100"), ("entry", ".06", "100"), ("exit", ".1", "99")]
        ):
            connection.execute(
                insert(fills).values(
                    fill_id=str(uuid4()),
                    order_id=order_id,
                    broker_fill_id=f"fill-{index}",
                    quantity=Decimal(quantity),
                    price=Decimal(price),
                    fees=Decimal(0),
                    filled_at=OPEN,
                )
            )
    report = session_report(database, COHORT, observed_at=CLOSE)
    assert report["funnel"]["fills"]["count"] == 3
    assert len(report["news_execution_fills"]) == 3
    assert report["equipment_test"]["execution_fills"] == []


def test_thesis_invalidation_requires_timestamped_evidence_not_just_a_loss() -> None:
    rows = [
        fill("entry", "buy", ".1", "100", "NEWS_STRATEGY"),
        fill("exit", "sell", ".1", "99", "NEWS_STRATEGY"),
    ]
    rows[1]["link"]["thesis_outcome"] = {"invalidated": True}
    report = allocation_ledgers(rows, {}, Decimal(10000), session_date=DAY)
    assert report["news_strategy"]["trades"][0]["thesis_invalidated"] is None
    rows[1]["link"]["thesis_outcome"].update(
        observed_at=OPEN.isoformat(),
        evidence={"source_url": "https://issuer.example.test/revision"},
    )
    report = allocation_ledgers(rows, {}, Decimal(10000), session_date=DAY)
    assert report["news_strategy"]["trades"][0]["thesis_invalidated"] is True


def test_cost_helper_alias_returns_the_same_version_without_shared_mutable_state() -> None:
    expected = paper_cost_assumptions()
    assert expected == cost_assumptions()
    assert expected["version"] == COST_MODEL_VERSION
    assert expected["base"]["residual_execution_rate_each_side"] == "0.00005"
    assert expected["stress_scenarios"]["3x"]["residual_cost_multiplier"] == "3"
    assert expected["stress_scenarios"]["3x"]["regulatory_fee_multiplier"] == "1"
    assert expected["benchmarks"]["cash"]["pnl"] == "0"
    assert expected["benchmarks"]["passive"]["pnl"] is None
    assert expected["benchmarks"]["passive"]["retrospective_selection_permitted"] is False
    expected["limitations"].append("caller-local note")
    assert "caller-local note" not in cost_assumptions()["limitations"]


def test_report_reads_account_session_reservations_without_counting_them_as_orders(
    database: Database,
) -> None:
    repository = ProductionRepository(database)
    account = "account-digest-fixture"
    key = session_control_key(account, DAY)
    budget = {
        "session_id": session_identity(account, DAY),
        "planned_session_date": str(DAY),
        "account_digest": account,
        "total_entries_reserved": 2,
        "news_entries_reserved": 1,
        "equipment_test_id": equipment_identity(account, DAY),
        "equipment_client_order_id": "earlier-equipment-order",
        "equipment_cohort_id": "earlier-preserved-cohort",
    }
    encoded = json.dumps(budget)
    repository.set_control(f"{COHORT}:broker-account", account)
    repository.set_control(key, encoded)
    report = session_report(database, COHORT, observed_at=CLOSE)
    assert report["session_id"] == budget["session_id"]
    assert report["session_budget"]["state"] == "recorded"
    assert report["session_budget"]["total_entries_reserved"] == 2
    assert report["session_budget"]["news_entries_reserved"] == 1
    assert report["equipment_test"]["stable_test_id"] == budget["equipment_test_id"]
    assert report["equipment_test"]["recorded_equipment_cohort_id"] == "earlier-preserved-cohort"
    assert report["funnel"]["attempted_entries"]["recorded_count"] == 0
    assert report["equipment_test"]["orders"] == []
    assert repository.get_control(key) == encoded


def test_missing_or_mismatched_session_budget_remains_unknown(database: Database) -> None:
    repository = ProductionRepository(database)
    unknown = session_report(database, COHORT, observed_at=CLOSE)["session_budget"]
    assert unknown["state"] == "unknown_account" and unknown["total_entries_reserved"] is None
    account = "account-digest-fixture"
    repository.set_control(f"{COHORT}:broker-account", account)
    key = session_control_key(account, DAY)
    missing = session_report(database, COHORT, observed_at=CLOSE)["session_budget"]
    assert missing["state"] == "not_recorded" and missing["news_entries_reserved"] is None
    repository.set_control(
        key,
        json.dumps(
            {
                "session_id": "another-session",
                "planned_session_date": str(DAY),
                "account_digest": account,
                "total_entries_reserved": 99,
            }
        ),
    )
    mismatch = session_report(database, COHORT, observed_at=CLOSE)["session_budget"]
    assert mismatch["state"] == "budget_identity_mismatch"
    assert mismatch["total_entries_reserved"] is None


@pytest.mark.parametrize("status", ["UNKNOWN", "reconciliation_required"])
def test_attempt_counts_require_dispatch_not_reservations_or_local_status(
    database: Database,
    status: str,
) -> None:
    rows = [
        fill("reserved", "buy", "0", "0", "NEWS_STRATEGY", status="reserved", requested=".1"),
        fill("expired", "buy", "0", "0", "NEWS_STRATEGY", status="expired", requested=".1"),
        fill("rejected", "buy", "0", "0", "NEWS_STRATEGY", status="rejected", requested=".1"),
        fill("uncertain", "buy", "0", "0", "NEWS_STRATEGY", status=status, requested=".1"),
        fill("audit-only", "buy", "0", "0", "NEWS_STRATEGY", status="UNKNOWN", requested=".1"),
        fill("exit", "sell", "0", "0", "NEWS_STRATEGY", status="UNKNOWN", requested=".1"),
    ]
    for row in rows:
        del row["link"]["broker"]
        row["link"]["session_id"] = "session-fixture"
    rows[3]["link"]["submission_attempted_at"] = OPEN.isoformat()
    save_orders(database, rows)
    store = EventStore(database)
    for seconds in (1, 2):
        store.audit(
            "submission_attempt",
            {"client_order_id": "audit-only", "side": "buy"},
            OPEN + timedelta(seconds=seconds),
            COHORT,
        )
    store.audit(
        "submission_attempt",
        {"client_order_id": "exit", "entry_client_order_id": "reserved", "side": "sell"},
        OPEN,
        COHORT,
    )
    report = session_report(database, COHORT, observed_at=CLOSE)
    activity = report["submission_activity"]
    entries = {entry["client_order_id"]: entry for entry in activity["entries"]}
    assert activity["recorded_count"] == 2
    assert activity["count"] is None
    assert entries["reserved"]["attempted"] is False
    assert entries["expired"]["state"] == "pre_dispatch_expired"
    assert entries["rejected"]["attempted"] is None
    assert entries["uncertain"]["attempted"] is True
    assert entries["audit-only"]["attempted"] is True
    assert len(entries["audit-only"]["dispatch_audits"]) == 2
    assert "exit" not in entries
    assert activity["pre_dispatch_expirations"] == 1
    assert report["funnel"]["attempted_entries"]["recorded_count"] == 2
    assert report["funnel"]["attempted_entries"]["count"] is None


@pytest.mark.parametrize("provider_code", ["400", "401", "403", "404", "422"])
def test_definitive_http_rejections_still_count_as_dispatched_entries(
    database: Database,
    provider_code: str,
) -> None:
    row = fill(
        "rejected-equipment",
        "buy",
        "0",
        "0",
        "EQUIPMENT_TEST",
        status="rejected",
        requested=".1",
    )
    del row["link"]["broker"]
    row["link"].update(submission_attempted_at=OPEN.isoformat(), provider_code=provider_code)
    save_orders(database, [row])
    report = session_report(database, COHORT, observed_at=CLOSE)
    assert report["funnel"]["attempted_entries"]["count"] == 1
    assert report["submission_activity"]["entries"][0]["provider_code"] == provider_code
    assert report["submission_activity"]["recorded_by_classification"]["EQUIPMENT_TEST"] == 1
    assert report["equipment_test"]["economics"]["trades"] == []


def test_predispatch_expiration_is_zero_attempts_not_a_broker_rejection(database: Database) -> None:
    row = fill("expired", "buy", "0", "0", "NEWS_STRATEGY", status="expired", requested=".1")
    del row["link"]["broker"]
    row["link"]["session_id"] = "session-fixture"
    save_orders(database, [row])
    report = session_report(database, COHORT, observed_at=CLOSE)
    assert report["funnel"]["attempted_entries"]["count"] == 0
    assert report["submission_activity"]["pre_dispatch_expirations"] == 1
    assert report["submission_activity"]["entries"][0]["broker_order_id"] is None
    assert "pre_dispatch_expired" in render_session_report(report)


def test_broker_calendar_audit_supplies_session_plan_when_worker_is_missing(
    database: Database,
) -> None:
    plan = {
        "session_date": DAY.isoformat(),
        "session_open": "2026-09-08T13:30:00+00:00",
        "session_close": "2026-09-08T20:00:00+00:00",
        "previous_session_close": "2026-09-04T20:00:00+00:00",
        "verified_at": OPEN.isoformat(),
        "calendar_source": "broker",
    }
    EventStore(database).audit("broker_calendar", {"plan": plan}, OPEN, COHORT)
    report = session_report(database, COHORT, observed_at=CLOSE)
    assert report["protocol"]["entry_and_exit_times"] == plan


def test_dispatch_audit_without_order_snapshot_is_counted_but_not_completed(
    database: Database,
) -> None:
    store = EventStore(database)
    store.audit(
        "submission_attempt",
        {"client_order_id": "missing-order", "side": "buy"},
        OPEN,
        COHORT,
    )
    store.audit("reconciliation", {"positions": [], "open_orders": []}, CLOSE, COHORT)
    report = session_report(database, COHORT, observed_at=CLOSE)
    assert report["funnel"]["attempted_entries"]["count"] == 1
    assert report["ending_exposure"]["completion_confirmed"] is False
    assert (
        report["ending_exposure"]["dispatches_without_order_snapshot"][0]["client_order_id"]
        == "missing-order"
    )


def test_prior_cohort_session_activity_is_versioned_without_merging_current_performance(
    database: Database,
) -> None:
    prior = "preserved-earlier-cohort"
    account = "same-paper-account"
    identity = session_identity(account, DAY)
    original_manifest = {
        **MANIFEST,
        "settings": {**MANIFEST["settings"], "virtual_equity": "2000"},
    }
    store = EventStore(database)
    store.freeze(
        prior, "old-frozen-hash", original_manifest, "experimental-paper", OPEN - timedelta(days=1)
    )
    repository = ProductionRepository(database)
    repository.set_control(f"{COHORT}:broker-account", account)
    repository.set_control(
        session_control_key(account, DAY),
        json.dumps(
            {
                "session_id": identity,
                "account_digest": account,
                "planned_session_date": str(DAY),
                "total_entries_reserved": 2,
                "news_entries_reserved": 1,
                "equipment_test_id": equipment_identity(account, DAY),
                "equipment_client_order_id": "old-1",
                "equipment_cohort_id": prior,
            }
        ),
    )
    rows = [
        fill("old-1", "buy", ".1", "100", "EQUIPMENT_TEST"),
        fill("old-2", "sell", ".1", "101", "EQUIPMENT_TEST"),
        fill("old-3", "buy", ".1", "100", "NEWS_STRATEGY"),
        fill("old-4", "sell", ".1", "99", "NEWS_STRATEGY"),
        fill("wrong-account", "buy", ".1", "100", "NEWS_STRATEGY"),
        fill("wrong-session", "buy", ".1", "100", "NEWS_STRATEGY"),
        fill("wrong-date", "buy", ".1", "100", "NEWS_STRATEGY"),
    ]
    for row in rows:
        row["strategy_version"] = prior
        row["link"].update(session_id=identity, account_digest=account)
    rows[1]["link"]["entry_client_order_id"] = "old-1"
    rows[3]["link"].update(
        entry_client_order_id="old-3",
        exit_decision={"reason": "existing_flatten_rule"},
        decision_ticket={"source_url": "https://issuer.example.test/original", "version": "old"},
    )
    rows[4]["link"]["account_digest"] = "other-account"
    rows[5]["link"]["session_id"] = "other-session"
    rows[6]["link"]["planned_session_date"] = "2026-09-09"
    save_orders(database, rows, cohort_id=prior)
    store.audit(
        "session_completion",
        {
            "session_id": identity,
            "account_digest": account,
            "state": "complete",
            "positions": [],
            "open_orders": [],
            "unconfirmed_local_orders": [],
            "mismatches": [],
        },
        CLOSE,
        prior,
    )
    calibration = {
        "state": "already_consumed_by_prior_cohort",
        "equipment_test_id": equipment_identity(account, DAY),
        "client_order_id": "old-1",
        "owning_cohort": prior,
    }
    repository.heartbeat(
        "tradeagent-event-worker",
        "owner",
        {"cohort_id": COHORT, "purpose": "iex-practice", "calibration": calibration},
        observed_at=CLOSE,
    )
    report = session_report(database, COHORT, observed_at=CLOSE, persist=True)
    assert report["equipment_test"]["status"] == calibration
    assert report["equipment_test"]["orders"] == []
    assert report["news_strategy"]["trades"] == []
    assert report["funnel"]["attempted_entries"]["recorded_count"] == 0
    assert report["account_session_activity"]["submission_activity"]["count"] == 2
    assert report["account_session_activity"]["recorded_filled_orders"] == 4
    assert report["prior_cohort_activity"]["state"] == "recorded_prior_activity"
    version = report["prior_cohort_activity"]["versions"][0]
    assert version["cohort_id"] == prior
    assert version["cohort_metadata"]["manifest"] == original_manifest
    assert version["qualification_eligible"] is False
    assert version["included_in_current_cohort_performance"] is False
    assert len(version["orders"]) == 4
    assert version["equipment_test"]["broker_paper_pnl"] == "0.1"
    assert version["news_strategy"]["broker_paper_pnl"] == "-0.1"
    assert version["news_strategy"]["strategy_capital_denominator"] == "2000"
    assert report["ending_exposure"]["completion_confirmed"] is True
    assert report["no_news_trade_status"] == (
        "prior_cohort_news_trade_recorded_not_current_cohort_performance"
    )
    assert report["qualifying_round_trips"] == 0
    rendered = render_session_report(report)
    assert f"Preserved cohort: {prior}" in rendered
    assert "https://issuer.example.test/original" in rendered
    assert "existing_flatten_rule" in rendered


def test_prior_pending_order_blocks_session_completion_without_current_orders(
    database: Database,
) -> None:
    prior = "preserved-pending"
    account = "same-paper-account"
    identity = session_identity(account, DAY)
    store = EventStore(database)
    store.freeze(prior, "pending-hash", MANIFEST, "experimental-paper", OPEN - timedelta(days=1))
    ProductionRepository(database).set_control(f"{COHORT}:broker-account", account)
    row = fill(
        "uncertain-prior", "buy", "0", "0", "NEWS_STRATEGY", status="UNKNOWN", requested=".1"
    )
    del row["link"]["broker"]
    row["strategy_version"] = prior
    row["link"].update(
        session_id=identity,
        account_digest=account,
        submission_attempted_at=OPEN.isoformat(),
    )
    save_orders(database, [row], cohort_id=prior)
    store.audit("reconciliation", {"positions": [], "open_orders": []}, CLOSE, COHORT)
    report = session_report(database, COHORT, observed_at=CLOSE)
    assert report["account_session_activity"]["submission_activity"]["count"] == 1
    assert report["ending_exposure"]["completion_confirmed"] is False
    pending = report["ending_exposure"]["pending_or_uncertain_local_orders"]
    assert pending[0]["client_order_id"] == "uncertain-prior"
    assert report["prior_cohort_activity"]["versions"][0]["cohort_id"] == prior


def test_current_exit_preserves_prior_entry_loss_without_creating_current_strategy_trade(
    database: Database,
) -> None:
    prior = "preserved-entry"
    account = "same-paper-account"
    identity = session_identity(account, DAY)
    store = EventStore(database)
    store.freeze(prior, "entry-hash", MANIFEST, "experimental-paper", OPEN - timedelta(days=1))
    ProductionRepository(database).set_control(f"{COHORT}:broker-account", account)
    entry = fill("prior-entry", "buy", ".1", "100", "NEWS_STRATEGY")
    entry["strategy_version"] = prior
    entry["link"].update(session_id=identity, account_digest=account)
    save_orders(database, [entry], cohort_id=prior)
    exit_order = fill("new-cohort-recovery-exit", "sell", ".1", "99", "NEWS_STRATEGY")
    exit_order["created_at"] = OPEN + timedelta(minutes=10)
    exit_order["updated_at"] = OPEN + timedelta(minutes=11)
    exit_order["link"].update(
        session_id=identity,
        account_digest=account,
        entry_client_order_id="prior-entry",
        decision_ticket={"version": prior},
        exit_decision={"reason": "risk_recovery"},
    )
    save_orders(database, [exit_order])
    report = session_report(database, COHORT, observed_at=CLOSE)
    assert report["news_strategy"]["trades"] == []
    assert report["exit_orders_attributed_to_prior_entry_cohorts"] == ["new-cohort-recovery-exit"]
    version = report["prior_cohort_activity"]["versions"][0]
    assert version["news_strategy"]["broker_paper_pnl"] == "-0.1"
    assert version["news_strategy"]["closed_round_trips"] == 1
    assert version["news_strategy"]["qualifying_closed_round_trips"] == 0
    assert version["mixed_execution_versions"] is True
    assert version["related_exit_orders_from_current_cohort"][0]["owning_cohort_id"] == COHORT
    assert version["news_strategy"]["trades"][0]["exit_decisions"][0]["exit_decision"] == {
        "reason": "risk_recovery"
    }
    assert report["account_session_activity"]["submission_activity"]["count"] == 1
    assert report["account_session_activity"]["recorded_filled_orders"] == 2


def test_typed_premarket_snapshots_supply_source_funnel_and_untraded_news_table(
    database: Database,
) -> None:
    store = EventStore(database)
    worker(database)
    source_poll = {
        "poll_id": "provider-poll",
        "observed_at": CLOSE.isoformat(),
        "coverage_complete": True,
        "coverage_watermark": CLOSE.isoformat(),
        "raw_items_received": 4,
        "errors": [],
        "source_health": {
            "news": {
                "status": "ok",
                "http_attempts": 1,
                "http_successes": 1,
                "last_http_received_at": CLOSE.isoformat(),
            },
            "sec:AAPL": {"status": "disabled_contact_not_configured"},
        },
    }
    store.audit(
        "source_poll",
        source_poll,
        CLOSE,
        f"{COHORT}:{DAY}:premarket_brief:poll:provider-poll",
    )
    item = {
        "evidence_id": "weekend-news",
        "symbols": ["AAPL"],
        "publisher": "Issuer",
        "source_url": "https://issuer.example.test/guidance",
        "document_id": "release-v1",
        "immutable_reference": "event_evidence:weekend-news",
        "publication_time": "2026-09-06T18:00:00Z",
        "bot_first_receipt_time": "2026-09-07T12:00:00Z",
        "provider_receipt_time": None,
        "revision_time": None,
        "revision_observed_at": "2026-09-07T12:02:00Z",
        "decision_time": None,
        "decision_action": None,
        "decision_reasons": ["old_event"],
        "quantitative_facts": [{"value": "110", "unit": "USD million", "source_offsets": [20, 34]}],
        "consensus": None,
        "contrary_evidence": [],
        "missing_information": ["prior comparison"],
    }
    brief = {
        "schema_version": "premarket-news-brief-v1",
        "cohort_id": COHORT,
        "session_date": str(DAY),
        "session_open": "2026-09-08T13:30:00Z",
        "session_close": "2026-09-08T20:00:00Z",
        "previous_session_close": "2026-09-04T20:00:00Z",
        "prepared_at": CLOSE.isoformat(),
        "prepared_before_open": True,
        "initial_prepared_at": "2026-09-07T12:00:00Z",
        "preparation_status": "premarket_prepared_updated",
        "snapshot_id": "brief-1",
        "source_health_status": "healthy",
        "coverage": {"complete_through_observation": True, "scope": "configured interval sources"},
        "capability_gaps": ["sec contact disabled"],
        "funnel": {
            "raw_items_received": 10,
            "new_evidence_versions_received": 2,
            "source_health_checks": 3,
            "unique_events": 2,
            "supported_company_matches": 2,
            "valid_quantitative_events": 1,
            "strategy_candidates": 0,
            "risk_approved_decisions": None,
            "attempted_entries": None,
            "fills": None,
            "duplicate_versions": 8,
        },
        "news": [item],
    }
    store.audit("premarket_brief", brief, CLOSE, f"{COHORT}:{DAY}:premarket_brief")
    second = {**brief, "snapshot_id": "brief-2", "supersedes_snapshot_id": "brief-1"}
    store.audit("premarket_brief", second, CLOSE, f"{COHORT}:{DAY}:premarket_brief")
    report = session_report(database, COHORT, observed_at=CLOSE)
    assert report["funnel"]["items_received"]["count"] == 10
    assert report["funnel"]["source_health_checks"]["count"] == 3
    assert report["funnel"]["unique_events"]["count"] == 2
    assert report["funnel"]["risk_approved_decisions"]["count"] is None
    assert report["funnel"]["items_received"]["snapshot_id"] == "brief-2"
    assert "never summed" in report["funnel"]["items_received"]["basis"]
    assert report["source_funnel_snapshot"]["duplicate_versions"] == 8
    assert report["health"]["state"] == "healthy"
    assert report["health"]["capability_gaps"] == ["sec contact disabled"]
    row = report["news_decisions"][0]
    assert row["action"] == "not_evaluated"
    assert row["timestamps"]["provider_received_at"] is None
    assert row["timestamps"]["revision_at"] is None
    assert row["timestamps"]["revision_observed_at"] == "2026-09-07T12:02:00Z"
    assert row["facts"] == item["quantitative_facts"]
    assert row["supporting_excerpt_or_reference"] == "event_evidence:weekend-news"
    assert report["news_strategy"]["trades"] == []
    assert "event_evidence:weekend-news" in render_session_report(report)


def test_pre_session_poll_trace_is_scoped_to_planned_session_not_collection_date(
    database: Database,
) -> None:
    monday = OPEN - timedelta(days=1)
    store = EventStore(database)
    store.audit(
        "source_poll",
        {"poll_id": "weekend", "raw_items_received": 2, "errors": []},
        monday,
        f"{COHORT}:{DAY}:premarket_brief:poll:weekend",
    )
    report = session_report(database, COHORT, observed_at=CLOSE)
    assert report["health"]["poll_count"] == 1
    assert report["health"]["state"] == "unknown_or_stale"
    assert report["funnel"]["items_received"]["count"] == 2


def test_interval_coverage_is_not_invalidated_by_optional_comparison_cache(
    database: Database,
) -> None:
    worker(database)
    EventStore(database).audit(
        "source_poll",
        {
            "coverage_complete": True,
            "errors": [],
            "coverage_scope": "configured_interval_sources",
            "coverage_excludes": ["optional_prior_comparison"],
            "source_health": {
                "news": {
                    "status": "ok",
                    "http_successes": 1,
                    "last_http_received_at": CLOSE.isoformat(),
                },
                "sec:AAPL": {
                    "status": "healthy",
                    "cache_hits": 1,
                    "http_successes": 1,
                    "last_http_received_at": CLOSE.isoformat(),
                    "prior_comparison_status": "unavailable",
                    "prior_comparison_error": "optional older comparison missing",
                },
            },
        },
        CLOSE,
        f"{COHORT}:{DAY}:premarket_brief:poll:cached",
    )
    report = session_report(database, COHORT, observed_at=CLOSE)
    assert report["health"]["state"] == "healthy"
    assert report["health"]["provider_http_confirmations"]["sec:AAPL"] == CLOSE.isoformat()
    assert report["health"]["coverage_excludes"] == ["optional_prior_comparison"]
    assert (
        report["health"]["health_evidence"]["source_health"]["sec:AAPL"]["prior_comparison_error"]
        == "optional older comparison missing"
    )


def test_actual_poll_raw_receipts_deduplicate_ids_but_never_invent_session_unique_or_quant(
    database: Database,
) -> None:
    store = EventStore(database)
    for identity, raw in (("one", 3), ("one", 3), ("two", 2)):
        store.audit(
            "source_poll",
            {
                "poll_id": identity,
                "raw_items_received": raw,
                "unique_events": 1,
                "supported_company_matches": 1,
                "unique_evidence_versions": 1,
                "new_evidence_versions": 0,
                "valid_quantitative_events": 0,
            },
            CLOSE,
            f"{COHORT}:{DAY}:premarket_brief:poll:{identity}",
        )
    report = session_report(database, COHORT, observed_at=CLOSE)
    assert report["funnel"]["items_received"]["count"] == 5
    assert report["funnel"]["items_received"]["duplicate_poll_records"] == 1
    for key in ("unique_events", "supported_company_matches", "valid_quantitative_events"):
        assert report["funnel"][key]["count"] is None
    assert report["funnel"]["source_health_checks"]["count"] is None


def test_missing_interval_coverage_is_unknown_despite_cached_provider_status(
    database: Database,
) -> None:
    worker(database)
    EventStore(database).audit(
        "source_poll",
        {"errors": [], "source_health": {"news": {"status": "cached", "cache_hits": 1}}},
        CLOSE,
        f"{COHORT}:{DAY}:premarket_brief:poll:cached",
    )
    report = session_report(database, COHORT, observed_at=CLOSE)
    assert report["health"]["state"] == "unknown_or_stale"


def test_latest_execution_state_preserves_first_eligibility_and_original_markouts(
    database: Database,
) -> None:
    store = EventStore(database)
    store.evidence("candidate", {"source_url": "https://issuer.example.test/release"}, OPEN)
    original = {
        "action": "eligible",
        "symbol": "AAPL",
        "reasons": [],
        "decided_at": OPEN.isoformat(),
        "quote_snapshot": {"ask": "100"},
    }
    identity = store.decision(COHORT, "candidate", original, OPEN)
    recheck = {
        "decision_id": identity,
        "evidence_id": "candidate",
        "action": "abstain",
        "reasons": ["quote_not_causal_or_fresh"],
        "rule_observations": {"quote_age_seconds": 12},
    }
    store.audit("decision_observation", recheck, CLOSE - timedelta(seconds=1), COHORT)
    with database.begin() as connection:
        connection.execute(
            insert(event_candidate_states).values(
                decision_id=identity,
                cohort_id=COHORT,
                status="waiting",
                updated_at=CLOSE,
                payload={"reason": "awaiting_fresh_quote"},
            )
        )
    report = session_report(database, COHORT, observed_at=CLOSE)
    row = report["news_decisions"][0]
    assert row["original_decision"] == original
    assert row["first_eligibility_action"] == "eligible"
    assert row["timestamps"]["decided_at"] == OPEN.isoformat()
    assert row["candidate_state"]["status"] == "waiting"
    assert row["failed_rules"] == ["awaiting_fresh_quote"]
    assert row["rule_results_and_observed_values"] == {"quote_age_seconds": 12}
    assert row["latest_execution_evaluation"]["kind"] == "decision_observation"
    assert report["prospective_diagnostics"]["missing_candidate_horizons"] == 4
    assert report["funnel"]["strategy_candidates"]["recorded_count"] == 1
    with database.begin() as connection:
        assert (
            connection.scalar(
                select(event_decisions.c.payload).where(event_decisions.c.decision_id == identity)
            )
            == original
        )
    store.audit(
        "candidate_state",
        {"decision_id": identity, "status": "expired", "reason": "entry_cutoff_passed"},
        CLOSE + timedelta(seconds=1),
        COHORT,
    )
    later = session_report(database, COHORT, observed_at=CLOSE + timedelta(seconds=2))
    row = later["news_decisions"][0]
    assert row["candidate_state"]["status"] == "expired"
    assert row["action"] == "expired"
    assert row["failed_rules"] == ["entry_cutoff_passed"]
    assert later["prospective_diagnostics"]["missing_candidate_horizons"] == 4


def test_broker_stream_audit_does_not_substitute_stream_claims_for_rest_fills(
    database: Database,
) -> None:
    entry = fill("pending-entry", "buy", "0", "0", "NEWS_STRATEGY", status="new", requested=".1")
    save_orders(database, [entry])
    payload = {
        "client_order_id": "pending-entry",
        "stream_event": {
            "status": "filled",
            "filled_quantity": ".1",
            "filled_average_price": "999",
        },
        "rest_confirmation": {"status": "new", "filled_quantity": "0"},
    }
    EventStore(database).audit("broker_stream_update", payload, CLOSE, COHORT)
    report = session_report(database, COHORT, observed_at=CLOSE)
    assert report["broker_stream_updates"][0]["evidence"] == payload
    assert report["news_strategy"]["trades"] == []
    assert report["funnel"]["filled_orders"]["recorded_count"] == 0
    assert report["ending_exposure"]["completion_confirmed"] is False
    assert report["ending_exposure"]["pending_or_uncertain_local_orders"][0]["status"] == "new"


def test_retained_feed_verification_is_distinct_from_content_receipt_and_full_interval(
    database: Database,
) -> None:
    worker(database)
    store = EventStore(database)
    payload = {
        "poll_id": "feed-cache",
        "observed_at": CLOSE.isoformat(),
        "coverage_complete": True,
        "errors": [],
        "coverage_watermark": (CLOSE - timedelta(minutes=30)).isoformat(),
        "requested_end": CLOSE.isoformat(),
        "source_health": {
            "issuer_feed:NVDA": {
                "status": "healthy",
                "cache_hits": 1,
                "http_successes": 0,
                "feed_received_at": (CLOSE - timedelta(days=2)).isoformat(),
                "feed_verified_at": CLOSE.isoformat(),
            },
        },
    }
    store.audit("source_poll", payload, CLOSE, f"{COHORT}:{DAY}:premarket_brief:poll:feed-cache")
    store.audit(
        "premarket_brief",
        {
            "schema_version": "premarket-news-brief-v1",
            "cohort_id": COHORT,
            "session_date": str(DAY),
            "prepared_at": CLOSE.isoformat(),
            "source_health_status": "configured_sources_observed_items",
            "coverage": {
                "complete_through_observation": False,
                "coverage_watermark": payload["coverage_watermark"],
                "required_through": CLOSE.isoformat(),
            },
        },
        CLOSE,
        f"{COHORT}:{DAY}:premarket_brief",
    )
    report = session_report(database, COHORT, observed_at=CLOSE)
    assert report["health"]["provider_http_confirmations"]["issuer_feed:NVDA"] == CLOSE.isoformat()
    assert report["health"]["state"] == "healthy_bounded_coverage"
    assert report["health"]["complete_through_observation"] is False
    assert report["no_news_trade_status"] == "healthy_bounded_coverage_no_news_fill"
    assert "uncovered required-through interval" in " ".join(report["next_actions"])


def test_new_poll_receipt_does_not_refresh_old_feed_verification(database: Database) -> None:
    worker(database)
    old = (CLOSE - timedelta(days=1)).isoformat()
    EventStore(database).audit(
        "source_poll",
        {
            "observed_at": CLOSE.isoformat(),
            "coverage_complete": True,
            "errors": [],
            "source_health": {
                "issuer_feed:AAPL": {
                    "status": "healthy",
                    "http_successes": 0,
                    "cache_hits": 1,
                    "feed_received_at": old,
                    "feed_verified_at": old,
                },
            },
        },
        CLOSE,
        f"{COHORT}:{DAY}:premarket_brief:poll:stale-feed",
    )
    report = session_report(database, COHORT, observed_at=CLOSE)
    assert report["health"]["provider_check_fresh"] is True
    assert report["health"]["provider_http_confirmations"]["issuer_feed:AAPL"] is None
    assert report["health"]["state"] == "unknown_or_stale"
