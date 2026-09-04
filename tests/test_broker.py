from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import pytest
from conftest import make_order

from tradeagent.broker import PaperBroker
from tradeagent.config import BrokerConfig
from tradeagent.domain import MarketBar, Side


def test_paper_broker_applies_costs_and_is_idempotent(bar: MarketBar, timestamp: datetime) -> None:
    broker = PaperBroker(
        BrokerConfig(
            starting_cash=Decimal("100000"),
            slippage_bps=Decimal("2"),
            commission_bps=Decimal("1"),
        )
    )
    broker.mark(bar)
    order = make_order(timestamp)

    fill = broker.submit(order, bar)
    repeated_fill = broker.submit(order, bar)
    account = broker.account(timestamp)

    assert fill is repeated_fill
    assert fill.price == Decimal("100.020000")
    assert fill.commission == Decimal("0.1000")
    assert broker.fill_count == 1
    assert account.position_for("SPY").quantity == Decimal("10")  # type: ignore[union-attr]
    assert account.cash == Decimal("98999.7000")


def test_paper_broker_realizes_profit_on_sale(bar: MarketBar, timestamp: datetime) -> None:
    broker = PaperBroker(BrokerConfig(slippage_bps=Decimal(0), commission_bps=Decimal(0)))
    broker.mark(bar)
    broker.submit(make_order(timestamp), bar)
    higher_bar = bar.model_copy(update={"close": Decimal("110"), "high": Decimal("111")})

    fill = broker.submit(
        make_order(timestamp, side=Side.SELL, quantity=Decimal("10"), client_order_id="sell:1"),
        higher_bar,
    )
    account = broker.account(timestamp)

    assert fill.price == Decimal("110.000000")
    assert account.positions == ()
    assert account.equity == Decimal("100100.0000")


def test_paper_broker_rejects_short_sale(bar: MarketBar, timestamp: datetime) -> None:
    broker = PaperBroker(BrokerConfig())
    broker.mark(bar)

    with pytest.raises(ValueError, match="short"):
        broker.submit(make_order(timestamp, side=Side.SELL), bar)
