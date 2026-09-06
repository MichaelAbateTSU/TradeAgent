from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from tradeagent.alpaca import HistoricalQuote
from tradeagent.diagnostics import TradeDiagnostic
from tradeagent.domain import Side
from tradeagent.execution_calibration import (
    _calibrate_trade,
    _decision_quote,
    _market_execution,
    _submission_quote,
)


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

    result = _calibrate_trade(trade, "30Min", quotes, quote_size_units="shares")

    assert result.market_net_edge == Decimal("0.95")
    assert result.market_entry.status == "filled"
    assert result.marketable_limit_entry.status == "missed"
    assert result.marketable_limit_exit.status == "missed"
    assert result.marketable_limit_net_edge is None
    assert result.spread_cost == Decimal("0.02")


def test_synthetic_causal_quote_selection_never_falls_back_across_arrival() -> None:
    at = datetime(2026, 9, 4, 15, tzinfo=UTC)
    prior = _quote(at - timedelta(seconds=1), "99", "101")
    future = _quote(at + timedelta(seconds=1), "199", "201")
    assert _submission_quote((prior,), at) is None
    assert _decision_quote((future,), at) is None
    assert _decision_quote((future, prior), at) == prior
    assert _submission_quote((future, prior), at) == future
    changed_future = future.model_copy(update={"bid_price": Decimal(999)})
    assert _decision_quote((changed_future, prior), at) == prior


@pytest.mark.parametrize(
    "at",
    [
        datetime(2026, 9, 4, 20, tzinfo=UTC),
        datetime(2026, 9, 4, 20, 1, tzinfo=UTC),
        datetime(2026, 9, 7, 15, tzinfo=UTC),  # Labor Day, not a regular session.
        datetime(2026, 11, 27, 18, tzinfo=UTC),  # Early close.
    ],
)
def test_synthetic_closed_session_never_executes(at: datetime) -> None:
    quote = _quote(at, "99", "101")
    assert _submission_quote((quote,), at) is None
    result = _market_execution(
        Side.BUY,
        Decimal("0.25"),
        at,
        quote,
        quote_size_units="shares",
    )
    assert result.filled_quantity == 0
    assert result.fill_price is None


def test_synthetic_unknown_units_and_stale_quote_are_unavailable() -> None:
    at = datetime(2026, 9, 4, 15, tzinfo=UTC)
    quote = _quote(at, "99", "101")
    assert _market_execution(Side.BUY, Decimal(1), at, quote).status == "unknown_quote_size_units"
    old = quote.model_copy(update={"timestamp": at - timedelta(seconds=61)})
    delayed = quote.model_copy(update={"timestamp": at + timedelta(seconds=61)})
    assert _decision_quote((old,), at) is None
    assert _submission_quote((delayed,), at) is None
