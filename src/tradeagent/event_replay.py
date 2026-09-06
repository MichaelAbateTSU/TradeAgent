"""Clearly labelled deterministic engineering replay. No HTTP client or broker credential access."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from tradeagent.alpaca_paper import (
    AlpacaOrderStatus,
    AlpacaPaperAccount,
    AlpacaPaperOrder,
    AlpacaPaperPosition,
    PaperAsset,
    PaperClock,
)
from tradeagent.config import AppConfig
from tradeagent.domain import OrderRequest
from tradeagent.event_orders import ExperimentalOrderManager
from tradeagent.event_research import (
    EventQuote,
    IssuerEligibility,
    MarketContext,
    SourceEvent,
    decide_event,
    extract_event,
    text_hash,
)
from tradeagent.event_store import EventStore
from tradeagent.experimental_policy import ExperimentalSettings, certificate
from tradeagent.persistence import Database


class ReplayBroker:
    broker_host = "https://paper-api.alpaca.markets"

    def __init__(self, now: datetime):
        self.now = now
        self.orders: dict[str, AlpacaPaperOrder] = {}

    def account(self) -> AlpacaPaperAccount:
        return AlpacaPaperAccount(
            id="synthetic-no-network-account",
            status="ACTIVE",
            currency="USD",
            cash=Decimal("100000"),
            portfolio_value=Decimal("100000"),
            buying_power=Decimal("100000"),
            trading_blocked=False,
            account_blocked=False,
            transfers_blocked=False,
        )

    def clock(self) -> PaperClock:
        return PaperClock(
            timestamp=self.now,
            is_open=True,
            next_open=self.now + timedelta(days=1),
            next_close=self.now + timedelta(hours=2),
        )

    def asset(self, symbol: str) -> PaperAsset:
        return PaperAsset.model_validate(
            {
                "id": "synthetic",
                "symbol": symbol,
                "name": "synthetic common stock",
                "class": "us_equity",
                "exchange": "NASDAQ",
                "status": "active",
                "tradable": True,
                "fractionable": True,
            }
        )

    def positions(self) -> tuple[AlpacaPaperPosition, ...]:
        quantities: dict[str, Decimal] = {}
        for order in self.orders.values():
            quantities[order.symbol] = quantities.get(order.symbol, Decimal(0)) + (
                order.filled_quantity * (1 if order.side == "buy" else -1)
            )
        return tuple(
            AlpacaPaperPosition(
                symbol=s,
                qty=q,
                avg_entry_price=Decimal(100),
                market_value=q * 100,
                unrealized_pl=Decimal(0),
            )
            for s, q in quantities.items()
            if q
        )

    def open_orders(self) -> tuple[AlpacaPaperOrder, ...]:
        return ()

    def find_order_by_client_id(self, client_order_id: str) -> AlpacaPaperOrder | None:
        return self.orders.get(client_order_id)

    def submit_limit_order(self, order: OrderRequest, limit_price: Decimal) -> AlpacaPaperOrder:
        result = AlpacaPaperOrder(
            id=order.client_order_id,
            client_order_id=order.client_order_id,
            symbol=order.symbol,
            side=order.side.value,
            qty=order.quantity,
            filled_qty=order.quantity,
            filled_avg_price=limit_price,
            status=AlpacaOrderStatus.FILLED,
            created_at=self.now,
            filled_at=self.now,
        )
        self.orders[order.client_order_id] = result
        return result

    def submit_market_order(self, order: OrderRequest) -> AlpacaPaperOrder:
        return self.submit_limit_order(order, Decimal("100.20"))

    def cancel_order(self, order_id: str) -> None:
        raise ValueError("synthetic immediate fills cannot be canceled")


def replay_event_pipeline() -> dict[str, Any]:
    received = datetime(2026, 9, 8, 14, 0, tzinfo=UTC)
    decided = received + timedelta(minutes=5, seconds=3)
    content = (
        "For fiscal 2027, GAAP revenue guidance increased from USD 100 million to USD 110 million."
    )
    event = SourceEvent(
        source_event_id="synthetic-upward-guidance",
        source="synthetic",
        source_url="https://www.apple.com/newsroom/synthetic-example",
        source_version=text_hash(content),
        published_at=received - timedelta(seconds=1),
        first_received_at=received,
        content_available_at=received,
        content=content,
        content_sha256=text_hash(content),
        event_cluster_id="synthetic-guidance-cluster",
        issuer_id="sec:0000320193",
        cik="0000320193",
        mapping_available_at=received - timedelta(days=1),
        is_primary_source=True,
        rights_profile="synthetic",
        availability_basis="synthetic",
    )
    extraction = extract_event(event, now=received + timedelta(seconds=1))
    quote = EventQuote(
        symbol="AAPL",
        bid=Decimal("100.09"),
        ask=Decimal("100.11"),
        bid_size=Decimal(100),
        ask_size=Decimal(100),
        size_unit="shares",
        feed="sip",
        timestamp=decided,
        received_at=decided,
    )
    decision = decide_event(
        extraction,
        evidence=(event,),
        now=decided,
        quote=quote,
        market=MarketContext(
            symbol="AAPL",
            mode="offline_replay",
            session_open=received - timedelta(minutes=30),
            session_close=received + timedelta(hours=6),
            observation_start=received,
            observation_end=received + timedelta(minutes=5),
            observation_available_at=received + timedelta(minutes=5),
            pre_event_price=Decimal(100),
            pre_event_volatility_fraction=Decimal("0.01"),
            pre_event_available_at=received - timedelta(minutes=5),
            halted=False,
            feed_healthy=True,
            macro_calendar_available_at=received - timedelta(days=1),
            macro_calendar_covers_until=received + timedelta(days=1),
        ),
        eligibility=IssuerEligibility(
            symbol="AAPL",
            issuer_id="sec:0000320193",
            cik="0000320193",
            available_at=received - timedelta(days=1),
            valid_from=received - timedelta(days=1),
            security_type="common_stock",
            exchange_listed=True,
            tradable=True,
            fractionable=True,
            median_daily_dollar_volume=Decimal("100000000"),
            liquidity_available_at=received - timedelta(days=1),
            liquidity_completed_sessions=20,
        ),
    )
    if decision.action != "eligible" or decision.eligible_at is None or decision.expires_at is None:
        raise RuntimeError(f"synthetic pipeline fixture failed: {decision.reasons}")
    settings = ExperimentalSettings(mode="experimental-paper", cohort_id="synthetic-replay")
    broker = ReplayBroker(decided)
    with Database("sqlite:///:memory:") as database:
        database.initialize()
        store = EventStore(database)
        store.freeze(
            settings.cohort_id, "synthetic", {"synthetic": True}, "offline-replay", received
        )
        store.evidence(event.evidence_id, event.model_dump(mode="json"), received)
        store.decision(
            settings.cohort_id, event.evidence_id, decision.model_dump(mode="json"), decided
        )
        oms = ExperimentalOrderManager(
            store, broker, settings, AppConfig(), "synthetic", "synthetic"
        )
        cert = certificate(
            settings,
            config_hash="synthetic",
            code_sha="synthetic",
            account_id=broker.account().id,
            checks={"synthetic_only": True},
            now=decided,
        )
        entry = oms.submit_entry(
            symbol="AAPL",
            cluster_key=event.event_cluster_id,
            decision_id=decision.decision_id,
            eligible_at=decision.eligible_at,
            expires_at=decision.expires_at,
            bid=quote.bid,
            ask=quote.ask,
            quote_at=decided,
            median_dollar_volume=Decimal("100000000"),
            source_valid=True,
            certificate=cert,
            now=decided,
        )
        broker.now = decided + timedelta(minutes=61)
        oms.supervise(broker.now, feed_healthy=True)
        ledger = oms.valuation({}, broker.now)
        return {
            "evidence_kind": "synthetic_deterministic_replay_NOT_live",
            "broker_network_calls": 0,
            "live_or_real_paper_orders_submitted": 0,
            "event": event.model_dump(mode="json"),
            "extraction": extraction.model_dump(mode="json"),
            "decision": decision.model_dump(mode="json"),
            "synthetic_entry": entry,
            "synthetic_orders": [order.model_dump(mode="json") for order in broker.orders.values()],
            "reconciled": oms.reconcile(broker.now)["healthy"],
            "synthetic_ledgers": ledger,
            "excluded_from_strategy_performance": True,
            "edge_established": False,
        }


def evaluate_extraction_fixture(path: Path) -> dict[str, Any]:
    import json

    cases = json.loads(path.read_text(encoding="utf-8"))["cases"]
    fixture = replay_event_pipeline()["event"]
    received = datetime.fromisoformat(fixture["first_received_at"].replace("Z", "+00:00"))
    results = []
    for case in cases:
        text = case["text"]
        event = SourceEvent.model_validate(
            {
                **fixture,
                "content": text,
                "content_sha256": text_hash(text),
                "source_event_id": case["id"],
                "source_version": text_hash(text),
                "is_primary_source": case.get("primary", True),
                "is_correction": case.get("correction", False),
                "first_received_at": received + timedelta(hours=1)
                if case.get("future")
                else received,
                "content_available_at": received + timedelta(hours=1)
                if case.get("future")
                else received,
            }
        )
        output = extract_event(event, now=received + timedelta(seconds=1))
        correct = output.event_type == case["event_type"] and len(output.facts) == case["facts"]
        results.append(
            {
                "id": case["id"],
                "type_and_field_count_correct": correct,
                "abstention": output.reason_for_abstention,
                "contradictions": output.contradictions,
                "missing": output.missing_required_fields,
            }
        )
    return {
        "synthetic": True,
        "cases": results,
        "cases_count": len(results),
        "type_and_field_count_accuracy": sum(r["type_and_field_count_correct"] for r in results)
        / len(results),
        "numeric_basis_and_spans": "covered by field-level regression tests",
        "production_accuracy": None,
        "alpha_evidence": False,
    }
