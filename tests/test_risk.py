from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal

from conftest import make_account, make_order

from tradeagent.config import RiskLimits
from tradeagent.domain import MarketBar, Side
from tradeagent.risk import RiskEngine


def test_risk_engine_approves_small_fresh_order(bar: MarketBar, timestamp: datetime) -> None:
    decision = RiskEngine(RiskLimits()).evaluate(
        make_order(timestamp),
        bar,
        make_account(timestamp),
        observed_at=timestamp,
        trading_enabled=True,
    )

    assert decision.approved
    assert decision.codes == ()


def test_risk_engine_rejects_stale_data(bar: MarketBar, timestamp: datetime) -> None:
    decision = RiskEngine(RiskLimits(max_data_age_seconds=120)).evaluate(
        make_order(timestamp),
        bar,
        make_account(timestamp),
        observed_at=timestamp + timedelta(seconds=121),
        trading_enabled=True,
    )

    assert not decision.approved
    assert "STALE_DATA" in decision.codes


def test_risk_engine_rejects_oversized_order(bar: MarketBar, timestamp: datetime) -> None:
    decision = RiskEngine(RiskLimits()).evaluate(
        make_order(timestamp, quantity=Decimal("21")),
        bar,
        make_account(timestamp),
        observed_at=timestamp,
        trading_enabled=True,
    )

    assert not decision.approved
    assert "MAX_ORDER_EXPOSURE" in decision.codes


def test_kill_switch_allows_only_risk_reduction(bar: MarketBar, timestamp: datetime) -> None:
    engine = RiskEngine(RiskLimits())
    engine.activate_kill_switch()
    account = make_account(
        timestamp,
        cash=Decimal("99000"),
        position_quantity=Decimal("10"),
    )

    buy = engine.evaluate(
        make_order(timestamp, side=Side.BUY, quantity=Decimal("1")),
        bar,
        account,
        observed_at=timestamp,
        trading_enabled=True,
    )
    sell = engine.evaluate(
        make_order(timestamp, side=Side.SELL, quantity=Decimal("1")),
        bar,
        account,
        observed_at=timestamp,
        trading_enabled=True,
    )

    assert "TRADING_DISABLED" in buy.codes
    assert sell.approved


def test_loss_limit_blocks_new_risk(bar: MarketBar, timestamp: datetime) -> None:
    account = make_account(
        timestamp,
        cash=Decimal("98000"),
        equity=Decimal("98000"),
        day_start_equity=Decimal("100000"),
        high_watermark=Decimal("100000"),
    )
    decision = RiskEngine(RiskLimits()).evaluate(
        make_order(timestamp),
        bar,
        account,
        observed_at=timestamp,
        trading_enabled=True,
    )

    assert "MAX_DAILY_LOSS" in decision.codes
    assert "MAX_DRAWDOWN" in decision.codes


def test_order_rate_limit_is_fail_closed(bar: MarketBar, timestamp: datetime) -> None:
    engine = RiskEngine(RiskLimits(max_orders_per_hour=1))
    account = make_account(timestamp)
    first = engine.evaluate(
        make_order(timestamp, client_order_id="first"),
        bar,
        account,
        observed_at=timestamp,
        trading_enabled=True,
    )
    second = engine.evaluate(
        make_order(timestamp, client_order_id="second"),
        bar,
        account,
        observed_at=timestamp + timedelta(minutes=1),
        trading_enabled=True,
    )

    assert first.approved
    assert "ORDER_RATE_LIMIT" in second.codes


def test_risk_engine_rejects_shorting(bar: MarketBar, timestamp: datetime) -> None:
    decision = RiskEngine(RiskLimits()).evaluate(
        make_order(timestamp, side=Side.SELL),
        bar,
        make_account(timestamp),
        observed_at=timestamp,
        trading_enabled=True,
    )

    assert "SHORTING_DISABLED" in decision.codes
