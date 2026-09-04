from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from conftest import make_account, make_order

from tradeagent.alpaca_paper import (
    AlpacaOrderStatus,
    AlpacaPaperAccount,
    AlpacaPaperOrder,
    AlpacaPaperPosition,
)
from tradeagent.config import RiskLimits
from tradeagent.domain import MarketBar, Side
from tradeagent.ledger import SQLiteLedger
from tradeagent.oms import PaperOrderManager
from tradeagent.risk import RiskEngine


def _account(*, blocked: bool = False) -> AlpacaPaperAccount:
    return AlpacaPaperAccount(
        id="account-1",
        status="ACTIVE",
        currency="USD",
        cash=Decimal("100000"),
        portfolio_value=Decimal("100000"),
        buying_power=Decimal("100000"),
        pattern_day_trader=False,
        trading_blocked=blocked,
        transfers_blocked=False,
        account_blocked=False,
    )


def _broker_order(
    *,
    client_order_id: str = "strategy:decision:1",
    status: AlpacaOrderStatus = AlpacaOrderStatus.NEW,
) -> AlpacaPaperOrder:
    return AlpacaPaperOrder(
        id=f"broker-{client_order_id}",
        client_order_id=client_order_id,
        status=status,
        symbol="SPY",
        side="buy",
        qty=Decimal("10"),
        filled_qty=Decimal("0"),
        filled_avg_price=None,
        created_at=datetime(2025, 1, 2, tzinfo=UTC),
    )


class FakeGateway:
    def __init__(
        self,
        *,
        existing: AlpacaPaperOrder | None = None,
        open_orders: tuple[AlpacaPaperOrder, ...] = (),
        blocked: bool = False,
    ) -> None:
        self.existing = existing
        self._open_orders = open_orders
        self._blocked = blocked
        self.submissions = 0

    def account(self) -> AlpacaPaperAccount:
        return _account(blocked=self._blocked)

    def positions(self) -> tuple[AlpacaPaperPosition, ...]:
        return ()

    def submit_market_order(self, order: object) -> AlpacaPaperOrder:
        self.submissions += 1
        return _broker_order()

    def find_order_by_client_id(self, client_order_id: str) -> AlpacaPaperOrder | None:
        if self.existing is not None and self.existing.client_order_id == client_order_id:
            return self.existing
        return None

    def open_orders(self) -> tuple[AlpacaPaperOrder, ...]:
        return self._open_orders


class FakeQualificationGate:
    def __init__(self, qualified: bool) -> None:
        self._qualified = qualified

    def is_strategy_qualified(self, strategy_id: str) -> bool:
        return self._qualified


def test_oms_recovers_existing_idempotent_order(bar: MarketBar, timestamp: datetime) -> None:
    existing = _broker_order()
    gateway = FakeGateway(existing=existing)
    with SQLiteLedger(":memory:") as ledger:
        result = PaperOrderManager(
            gateway,
            RiskEngine(RiskLimits()),
            ledger,
            FakeQualificationGate(True),
        ).submit(
            make_order(timestamp),
            bar,
            make_account(timestamp),
            observed_at=timestamp,
        )

        assert result.risk.approved
        assert result.recovered_existing_order
        assert result.broker_order == existing
        assert gateway.submissions == 0
        assert ledger.latest_event("broker_order_state") is not None


def test_oms_submits_only_after_risk_approval(bar: MarketBar, timestamp: datetime) -> None:
    gateway = FakeGateway()
    with SQLiteLedger(":memory:") as ledger:
        manager = PaperOrderManager(
            gateway,
            RiskEngine(RiskLimits()),
            ledger,
            FakeQualificationGate(True),
        )
        approved = manager.submit(
            make_order(timestamp),
            bar,
            make_account(timestamp),
            observed_at=timestamp,
        )
        ledger.set_control(
            "kill_switch",
            "active",
            occurred_at=timestamp,
            trace_id="operator:kill-switch:active",
        )
        rejected = manager.submit(
            make_order(timestamp, client_order_id="blocked:decision:1"),
            bar,
            make_account(timestamp),
            observed_at=timestamp,
        )

    assert approved.broker_order is not None
    assert gateway.submissions == 1
    assert not rejected.risk.approved
    assert rejected.broker_order is None


def test_oms_fails_closed_without_strategy_qualification(
    bar: MarketBar, timestamp: datetime
) -> None:
    gateway = FakeGateway()
    with SQLiteLedger(":memory:") as ledger:
        result = PaperOrderManager(
            gateway,
            RiskEngine(RiskLimits()),
            ledger,
        ).submit(
            make_order(timestamp),
            bar,
            make_account(timestamp),
            observed_at=timestamp,
        )

    assert not result.risk.approved
    assert result.risk.codes == ("STRATEGY_NOT_QUALIFIED",)
    assert result.broker_order is None
    assert gateway.submissions == 0


def test_unqualified_strategy_can_still_reduce_risk(bar: MarketBar, timestamp: datetime) -> None:
    gateway = FakeGateway()
    with SQLiteLedger(":memory:") as ledger:
        result = PaperOrderManager(
            gateway,
            RiskEngine(RiskLimits()),
            ledger,
        ).submit(
            make_order(timestamp, side=Side.SELL, quantity=Decimal("1")),
            bar,
            make_account(
                timestamp,
                cash=Decimal("99000"),
                position_quantity=Decimal("10"),
            ),
            observed_at=timestamp,
        )

    assert result.risk.approved
    assert result.broker_order is not None
    assert gateway.submissions == 1


def test_reconciliation_activates_kill_switch_for_missing_order(
    timestamp: datetime,
) -> None:
    missing = _broker_order(client_order_id="missing:decision:1")
    gateway = FakeGateway()
    with SQLiteLedger(":memory:") as ledger:
        ledger.append(
            "broker_order_state",
            missing,
            occurred_at=timestamp,
            trace_id="decision",
        )
        result = PaperOrderManager(
            gateway,
            RiskEngine(RiskLimits()),
            ledger,
        ).reconcile(observed_at=timestamp)

        assert not result.healthy
        assert result.mismatches == ("LOCAL_ORDER_MISSING_AT_BROKER:missing:decision:1",)
        assert ledger.get_control("kill_switch") == "active"
        assert ledger.latest_event("broker_reconciliation") is not None


def test_reconciliation_records_healthy_broker_state(timestamp: datetime) -> None:
    order = _broker_order()
    gateway = FakeGateway(existing=order, open_orders=(order,))
    with SQLiteLedger(":memory:") as ledger:
        result = PaperOrderManager(
            gateway,
            RiskEngine(RiskLimits()),
            ledger,
        ).reconcile(observed_at=timestamp)

        assert result.healthy
        assert result.mismatches == ()
        assert ledger.get_control("kill_switch", default="inactive") == "inactive"
