from __future__ import annotations

from datetime import datetime, timedelta
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
    assert fill.price == Decimal("100.025000")
    assert fill.commission == Decimal("0.1000")
    assert broker.fill_count == 1
    assert account.position_for("SPY").quantity == Decimal("10")  # type: ignore[union-attr]
    assert account.cash == Decimal("98999.6500")


def test_paper_broker_realizes_profit_on_sale(bar: MarketBar, timestamp: datetime) -> None:
    broker = PaperBroker(
        BrokerConfig(
            slippage_bps=Decimal(0),
            spread_bps=Decimal(0),
            commission_bps=Decimal(0),
        )
    )
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


def test_paper_broker_state_restores_idempotently(bar: MarketBar, timestamp: datetime) -> None:
    config = BrokerConfig()
    broker = PaperBroker(config)
    broker.mark(bar)
    order = make_order(timestamp)
    original_fill = broker.submit(order, bar)
    original_account = broker.account(timestamp)

    restored = PaperBroker.from_state(config, broker.export_state())
    repeated_fill = restored.submit(order, bar)

    assert repeated_fill == original_fill
    assert restored.fill_count == 1
    assert restored.account(timestamp) == original_account


def test_new_session_baseline_preserves_overnight_loss(bar: MarketBar, timestamp: datetime) -> None:
    broker = PaperBroker(
        BrokerConfig(
            slippage_bps=Decimal(0),
            spread_bps=Decimal(0),
            commission_bps=Decimal(0),
        )
    )
    broker.mark(bar)
    broker.submit(make_order(timestamp, quantity=Decimal("100")), bar)
    next_session = bar.model_copy(
        update={
            "timestamp": bar.timestamp + timedelta(days=1),
            "open": Decimal("80"),
            "high": Decimal("81"),
            "low": Decimal("79"),
            "close": Decimal("80"),
        }
    )

    broker.mark(next_session)
    account = broker.account(next_session.timestamp)

    assert account.day_start_equity == Decimal("100000.0000")
    assert account.equity == Decimal("98000.0000")
    assert account.daily_return == Decimal("-0.02")


def test_paper_broker_partially_fills_at_volume_cap(bar: MarketBar, timestamp: datetime) -> None:
    low_volume_bar = bar.model_copy(update={"volume": Decimal("100")})
    broker = PaperBroker(
        BrokerConfig(
            slippage_bps=Decimal(0),
            spread_bps=Decimal(0),
            commission_bps=Decimal(0),
            max_volume_participation=Decimal("0.05"),
        )
    )
    broker.mark(low_volume_bar)

    fill = broker.submit(
        make_order(timestamp, quantity=Decimal("10")),
        low_volume_bar,
    )

    assert fill.quantity == Decimal("5")
    assert broker.account(timestamp).position_for("SPY").quantity == Decimal("5")  # type: ignore[union-attr]


def test_realized_pnl_includes_both_sides_commission(bar: MarketBar, timestamp: datetime) -> None:
    broker = PaperBroker(
        BrokerConfig(
            slippage_bps=Decimal(0),
            spread_bps=Decimal(0),
            commission_bps=Decimal("1"),
        )
    )
    broker.mark(bar)
    broker.submit(make_order(timestamp, quantity=Decimal("1")), bar)
    broker.submit(
        make_order(
            timestamp,
            side=Side.SELL,
            quantity=Decimal("1"),
            client_order_id="sell-commission",
        ),
        bar,
    )
    account = broker.account(timestamp)

    assert account.equity == Decimal("99999.9800")
    state = broker.export_state()
    assert state.positions[0].realized_pnl == Decimal("-0.0200")
    assert state.positions[0].entry_commission == 0
