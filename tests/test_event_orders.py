from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import httpx
import pytest

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
from tradeagent.event_store import EventStore
from tradeagent.experimental_policy import ExperimentalSettings, certificate
from tradeagent.persistence import Database, ProductionRepository

NOW = datetime(2026, 9, 8, 15, 0, tzinfo=UTC)


class Broker:
    broker_host = "https://paper-api.alpaca.markets"

    def __init__(self):
        self.values: dict[str, AlpacaPaperOrder] = {}
        self.submissions = 0
        self.now = NOW
        self.timeout = False
        self.partial = False

    def account(self):
        return AlpacaPaperAccount(
            id="paper-fixture",
            status="ACTIVE",
            currency="USD",
            cash=Decimal("100000"),
            portfolio_value=Decimal("100000"),
            buying_power=Decimal("100000"),
            trading_blocked=False,
            transfers_blocked=False,
            account_blocked=False,
        )

    def positions(self):
        held: dict[str, Decimal] = {}
        for order in self.values.values():
            held[order.symbol] = held.get(order.symbol, Decimal(0)) + order.filled_quantity * (
                1 if order.side == "buy" else -1
            )
        return tuple(
            AlpacaPaperPosition(
                symbol=s, qty=q, avg_entry_price=100, market_value=q * 100, unrealized_pl=0
            )
            for s, q in held.items()
            if q
        )

    def open_orders(self):
        return tuple(
            order
            for order in self.values.values()
            if order.status in {AlpacaOrderStatus.NEW, AlpacaOrderStatus.PARTIALLY_FILLED}
        )

    def clock(self):
        return PaperClock(
            timestamp=self.now,
            is_open=True,
            next_open=self.now + timedelta(days=1),
            next_close=self.now + timedelta(hours=4),
        )

    def asset(self, symbol):
        return PaperAsset.model_validate(
            {
                "id": "asset",
                "symbol": symbol,
                "name": f"{symbol} common stock",
                "class": "us_equity",
                "exchange": "NASDAQ",
                "status": "active",
                "tradable": True,
                "fractionable": True,
            }
        )

    def find_order_by_client_id(self, client_order_id):
        return self.values.get(client_order_id)

    def submit_limit_order(self, request: OrderRequest, limit: Decimal):
        self.submissions += 1
        qty = request.quantity / 2 if self.partial else request.quantity
        result = AlpacaPaperOrder(
            id=request.client_order_id,
            client_order_id=request.client_order_id,
            status=AlpacaOrderStatus.PARTIALLY_FILLED if self.partial else AlpacaOrderStatus.FILLED,
            symbol=request.symbol,
            side=request.side.value,
            qty=request.quantity,
            filled_qty=qty,
            filled_avg_price=limit,
            created_at=self.now,
            filled_at=self.now,
        )
        self.values[request.client_order_id] = result
        if self.timeout:
            raise httpx.ReadTimeout("lost acknowledgement")
        return result

    def submit_market_order(self, request):
        self.partial = False
        self.timeout = False
        return self.submit_limit_order(request, Decimal("101"))

    def cancel_order(self, order_id):
        current = self.values[order_id]
        self.values[order_id] = current.model_copy(update={"status": AlpacaOrderStatus.CANCELED})


def setup(tmp_path: Path, mode="experimental-paper"):
    database = Database(f"sqlite:///{tmp_path / 'events.db'}")
    database.initialize()
    store = EventStore(database)
    settings = ExperimentalSettings(mode=mode, cohort_id="fixture", _env_file=None)
    store.freeze("fixture", "hash", {"fixture": True}, mode, NOW)
    broker = Broker()
    manager = ExperimentalOrderManager(store, broker, settings, AppConfig(), "hash", "sha")
    cert = certificate(
        settings,
        config_hash="hash",
        code_sha="sha",
        account_id="paper-fixture",
        checks={"paper_host": True, "mechanics": True},
        now=NOW,
    )
    args = dict(
        symbol="AAPL",
        cluster_key="cluster",
        decision_id="decision",
        eligible_at=NOW - timedelta(seconds=1),
        expires_at=NOW + timedelta(minutes=10),
        bid=Decimal("99.99"),
        ask=Decimal("100"),
        quote_at=NOW,
        median_dollar_volume=Decimal("100000000"),
        source_valid=True,
        certificate=cert,
        now=NOW,
    )
    return database, store, broker, manager, args


def test_event_entry_reconcile_exit_and_duplicate_replay(tmp_path: Path):
    db, store, broker, manager, args = setup(tmp_path)
    try:
        assert manager.reconcile(NOW)["healthy"]
        first = manager.submit_entry(**args)
        assert first["state"] == "filled"
        assert manager.submit_entry(**args)["state"] == "duplicate_event"
        assert broker.submissions == 1
        assert broker.positions()[0].quantity * Decimal("100") <= 25
        broker.now = NOW + timedelta(minutes=61)
        manager.supervise(broker.now, feed_healthy=True)
        assert manager.reconcile(broker.now)["healthy"]
        assert broker.positions() == ()
        valuation = manager.valuation({}, broker.now)
        assert valuation["closed_round_trips"] == 1
        assert Decimal(valuation["broker_paper_pnl"]) > Decimal(valuation["economic_paper_pnl"])
        assert len(store.linked_orders("fixture")) == 2
    finally:
        db.dispose()


def test_timeout_recovers_without_second_exposure(tmp_path: Path):
    db, _, broker, manager, args = setup(tmp_path)
    try:
        broker.timeout = True
        assert manager.submit_entry(**args)["state"] == "submission_outcome_unknown"
        assert manager.reconcile(NOW)["healthy"]
        assert manager.submit_entry(**args)["state"] == "risk_rejected"
        assert broker.submissions == 1
        # Pause blocks new exposure, never risk-reducing exit supervision.
        manager.supervise(NOW, feed_healthy=False)
        assert not broker.positions()
    finally:
        db.dispose()


def test_partial_entry_cancel_exits_only_actual_owned_quantity(tmp_path: Path):
    db, _, broker, manager, args = setup(tmp_path)
    try:
        broker.partial = True
        assert manager.submit_entry(**args)["state"] == "partially_filled"
        owned = broker.positions()[0].quantity
        manager.supervise(NOW, feed_healthy=False)
        sells = [order for order in broker.values.values() if order.side == "sell"]
        assert len(sells) == 1
        assert sells[0].quantity == owned
        assert broker.positions() == ()
    finally:
        db.dispose()


@pytest.mark.parametrize(
    "change,reason",
    [
        ({"source_valid": False}, "UNVERIFIED_SOURCE_OR_ISSUER"),
        ({"quote_at": NOW - timedelta(minutes=2)}, "STALE_QUOTE"),
        ({"eligible_at": NOW + timedelta(seconds=1)}, "EVENT_NOT_EXECUTABLE_NOW"),
        ({"median_dollar_volume": Decimal("1")}, "LIQUIDITY_FLOOR"),
    ],
)
def test_pretrade_blocks_bad_evidence(tmp_path: Path, change, reason):
    db, _, broker, manager, args = setup(tmp_path)
    try:
        result = manager.submit_entry(**{**args, **change})
        assert reason in result["reasons"]
        assert not broker.submissions
    finally:
        db.dispose()


def test_shadow_and_changed_certificate_cannot_trade(tmp_path: Path):
    db, store, broker, manager, args = setup(tmp_path, "shadow")
    try:
        assert "SHADOW_NO_ORDERS" in manager.submit_entry(**args)["reasons"]
        with pytest.raises(ValueError, match="cohort immutable"):
            store.freeze("fixture", "changed", {}, "shadow", NOW)
        ProductionRepository(db).set_control("kill_switch", "active")
        assert "GLOBAL_KILL_SWITCH" in manager.submit_entry(**args)["reasons"]
        assert not broker.submissions
    finally:
        db.dispose()


def test_account_mismatch_pauses_and_live_host_never_called(tmp_path: Path):
    db, _, broker, manager, _ = setup(tmp_path)
    try:
        broker.broker_host = "https://api.alpaca.markets"
        with pytest.raises(ValueError, match="endpoint forbidden"):
            manager.reconcile(NOW)
        assert not broker.submissions
    finally:
        db.dispose()
