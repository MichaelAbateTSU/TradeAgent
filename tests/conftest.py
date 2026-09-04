from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from tradeagent.domain import AccountSnapshot, MarketBar, OrderRequest, Position, Side


@pytest.fixture
def timestamp() -> datetime:
    return datetime(2025, 1, 2, 21, tzinfo=UTC)


@pytest.fixture
def bar(timestamp: datetime) -> MarketBar:
    return MarketBar(
        symbol="spy",
        timestamp=timestamp,
        open=Decimal("99"),
        high=Decimal("101"),
        low=Decimal("98"),
        close=Decimal("100"),
        volume=Decimal("1000000"),
    )


def make_order(
    timestamp: datetime,
    *,
    side: Side = Side.BUY,
    quantity: Decimal = Decimal("10"),
    client_order_id: str = "strategy:decision:1",
) -> OrderRequest:
    return OrderRequest(
        client_order_id=client_order_id,
        decision_id="decision",
        strategy_id="strategy",
        symbol="SPY",
        side=side,
        quantity=quantity,
        submitted_at=timestamp,
    )


def make_account(
    timestamp: datetime,
    *,
    cash: Decimal = Decimal("100000"),
    equity: Decimal = Decimal("100000"),
    day_start_equity: Decimal = Decimal("100000"),
    high_watermark: Decimal = Decimal("100000"),
    position_quantity: Decimal = Decimal(0),
) -> AccountSnapshot:
    positions: tuple[Position, ...] = ()
    if position_quantity:
        positions = (
            Position(
                symbol="SPY",
                quantity=position_quantity,
                average_price=Decimal("100"),
                market_price=Decimal("100"),
                realized_pnl=Decimal(0),
            ),
        )
    return AccountSnapshot(
        as_of=timestamp,
        cash=cash,
        equity=equity,
        day_start_equity=day_start_equity,
        high_watermark=high_watermark,
        positions=positions,
    )
