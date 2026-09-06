from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from tradeagent.alpaca import HistoricalQuote
from tradeagent.diagnostics import TradeDiagnostic
from tradeagent.execution_calibration import _calibrate_trade


def _quote(timestamp: datetime, bid: str, ask: str) -> HistoricalQuote:
    return HistoricalQuote(
        symbol="SPY",
        timestamp=timestamp,
        bid_exchange="P",
        bid_price=Decimal(bid),
        bid_size=Decimal("10"),
        ask_exchange="Q",
        ask_price=Decimal(ask),
        ask_size=Decimal("10"),
        feed_source="sip",
    )


def test_market_and_marketable_limit_use_quotes_without_touch_fill_assumption() -> None:
    entry = datetime(2024, 1, 2, 15, tzinfo=UTC)
    exit_at = datetime(2024, 1, 2, 16, tzinfo=UTC)
    entry_signal = entry - timedelta(minutes=30)
    exit_signal = exit_at - timedelta(minutes=30)
    trade = TradeDiagnostic(
        symbol="SPY",
        entry_at=entry,
        exit_at=exit_at,
        quantity=Decimal("1"),
        gross_pnl=Decimal("1"),
        net_pnl=Decimal("0.93"),
        execution_cost=Decimal("0.07"),
        spread_cost=Decimal("0.01"),
        slippage_cost=Decimal("0.04"),
        fees=Decimal("0.02"),
        flattening_cost=Decimal("0"),
        mfe=Decimal("1"),
        mae=Decimal("-0.2"),
        holding_frames=2,
    )
    quotes = {
        ("SPY", "30Min", "entry_signal", entry_signal): (_quote(entry_signal, "99.99", "100.01"),),
        ("SPY", "30Min", "entry_submission", entry): (_quote(entry, "100.01", "100.03"),),
        ("SPY", "30Min", "exit_signal", exit_signal): (_quote(exit_signal, "101.00", "101.02"),),
        ("SPY", "30Min", "exit_submission", exit_at): (_quote(exit_at, "100.98", "101.00"),),
    }

    result = _calibrate_trade(trade, "30Min", quotes)

    assert result.market_net_edge == Decimal("0.95")
    assert result.market_entry.status == "filled"
    assert result.marketable_limit_entry.status == "missed"
    assert result.marketable_limit_exit.status == "missed"
    assert result.marketable_limit_net_edge is None
    assert result.spread_cost == Decimal("0.02")
