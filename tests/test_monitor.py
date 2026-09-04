from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from tradeagent.alpaca_paper import (
    AlpacaOrderStatus,
    AlpacaPaperAccount,
    AlpacaPaperOrder,
    AlpacaPaperPosition,
)
from tradeagent.domain import OrderRequest
from tradeagent.ledger import SQLiteLedger
from tradeagent.monitor import monitor_take_profit


def _position(profit: str) -> AlpacaPaperPosition:
    return AlpacaPaperPosition(
        symbol="BTCUSD",
        qty=Decimal("0.001"),
        avg_entry_price=Decimal("50000"),
        market_value=Decimal("50"),
        unrealized_pl=Decimal(profit),
    )


def _order(status: AlpacaOrderStatus) -> AlpacaPaperOrder:
    return AlpacaPaperOrder(
        id="order-1",
        client_order_id="take-profit-20250102T210000000000Z",
        status=status,
        symbol="BTC/USD",
        side="sell",
        qty=Decimal("0.001"),
        filled_qty=Decimal("0.001") if status is AlpacaOrderStatus.FILLED else Decimal(0),
        filled_avg_price=Decimal("50100") if status is AlpacaOrderStatus.FILLED else None,
        created_at=datetime(2025, 1, 2, 21, tzinfo=UTC),
    )


class FakeTakeProfitGateway:
    def __init__(self, positions: list[AlpacaPaperPosition]) -> None:
        self._positions = positions
        self.submitted: OrderRequest | None = None

    def account(self) -> AlpacaPaperAccount:
        return AlpacaPaperAccount(
            id="account-1",
            status="ACTIVE",
            currency="USD",
            cash=Decimal("100000"),
            portfolio_value=Decimal("100000"),
            buying_power=Decimal("100000"),
            trading_blocked=False,
            transfers_blocked=False,
            account_blocked=False,
        )

    def positions(self) -> tuple[AlpacaPaperPosition, ...]:
        if not self._positions:
            return ()
        return (self._positions.pop(0),)

    def submit_market_order(self, order: OrderRequest) -> AlpacaPaperOrder:
        self.submitted = order
        return _order(AlpacaOrderStatus.PENDING_NEW)

    def order_by_client_id(self, client_order_id: str) -> AlpacaPaperOrder:
        return _order(AlpacaOrderStatus.FILLED)


def test_monitor_waits_then_closes_profitable_position() -> None:
    gateway = FakeTakeProfitGateway([_position("-0.10"), _position("0.01")])
    samples: list[Decimal] = []
    fixed_time = datetime(2025, 1, 2, 21, tzinfo=UTC)
    with SQLiteLedger(":memory:") as ledger:
        result = monitor_take_profit(
            gateway,
            ledger,
            symbol="BTC/USD",
            minimum_profit=Decimal(0),
            poll_seconds=0,
            on_sample=lambda position: samples.append(position.unrealized_pnl),
            sleeper=lambda _: None,
            clock=lambda: fixed_time,
        )

        assert result.status == "filled"
        assert result.observed_profit == Decimal("0.01")
        assert gateway.submitted is not None
        assert gateway.submitted.quantity == Decimal("0.001")
        assert samples == [Decimal("-0.10"), Decimal("0.01")]
        assert ledger.latest_event("manual_take_profit_triggered") is not None


def test_monitor_handles_missing_position_and_bounded_polling() -> None:
    with SQLiteLedger(":memory:") as ledger:
        missing = monitor_take_profit(
            FakeTakeProfitGateway([]),
            ledger,
            symbol="BTC/USD",
            minimum_profit=Decimal(0),
            poll_seconds=0,
            sleeper=lambda _: None,
        )
        monitoring = monitor_take_profit(
            FakeTakeProfitGateway([_position("-0.01")]),
            ledger,
            symbol="BTC/USD",
            minimum_profit=Decimal(0),
            poll_seconds=0,
            sleeper=lambda _: None,
            max_polls=1,
        )

    assert missing.status == "no_position"
    assert monitoring.status == "monitoring"
