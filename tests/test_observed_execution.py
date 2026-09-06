from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from tradeagent.alpaca import HistoricalQuote, HistoricalTrade
from tradeagent.domain import MarketBar
from tradeagent.lower_execution_evidence import (
    LowerEvidenceAnchor,
    PointInTimeSnapshot,
)
from tradeagent.observed_execution import simulate_observed_execution
from tradeagent.portfolio import PortfolioIntent
from tradeagent.universe import UniverseFrame


class OneDayStrategy:
    strategy_id = "one-day"

    def __init__(self) -> None:
        self.frames = 0

    def on_frame(self, frame: UniverseFrame) -> PortfolioIntent:
        self.frames += 1
        return PortfolioIntent(
            strategy_id=self.strategy_id,
            timestamp=frame.timestamp,
            target_weights={"SPY": Decimal("0.01") if self.frames == 1 else Decimal(0)},
            rationale="one day test position",
        )


def _frame(index: int, price: str) -> UniverseFrame:
    timestamp = datetime(2024, 1, 2, 21, tzinfo=UTC) + timedelta(days=index)
    value = Decimal(price)
    return UniverseFrame(
        timestamp=timestamp,
        bars=(
            MarketBar(
                symbol="SPY",
                timestamp=timestamp,
                open=value,
                high=value,
                low=value,
                close=value,
                volume=Decimal("1000000"),
            ),
        ),
    )


def _snapshot(frame: UniverseFrame, bid: str, ask: str) -> PointInTimeSnapshot:
    timestamp = frame.timestamp
    anchor = LowerEvidenceAnchor(
        symbol="SPY",
        timestamp=timestamp,
        anchor_types=("test",),
        hypothesis_ids=("one-day",),
    )

    def quote(offset: int) -> HistoricalQuote:
        return HistoricalQuote(
            symbol="SPY",
            timestamp=timestamp + timedelta(milliseconds=offset),
            bid_exchange="P",
            bid_price=Decimal(bid),
            bid_size=Decimal("100"),
            ask_exchange="Q",
            ask_price=Decimal(ask),
            ask_size=Decimal("100"),
            feed_source="sip",
        )

    def trade(offset: int) -> HistoricalTrade:
        return HistoricalTrade(
            symbol="SPY",
            timestamp=timestamp + timedelta(milliseconds=offset),
            exchange="P",
            price=(Decimal(bid) + Decimal(ask)) / 2,
            size=Decimal("10"),
            trade_id=f"trade-{offset}",
            feed_source="sip",
        )

    return PointInTimeSnapshot(
        anchor=anchor,
        quote_before=quote(-1),
        quote_after=quote(1),
        trade_before=trade(-1),
        trade_after=trade(1),
    )


def test_observed_market_execution_includes_spread_slippage_fees_and_cash_days() -> None:
    frames = (_frame(0, "100"), _frame(1, "101"), _frame(2, "102"))
    snapshots = {
        ("SPY", frame.timestamp): _snapshot(
            frame,
            str(frame.bars[0].close - Decimal("0.01")),
            str(frame.bars[0].close + Decimal("0.01")),
        )
        for frame in frames
    }

    report = simulate_observed_execution(
        frames,
        OneDayStrategy(),
        snapshots,
        execution_style="market",
    )

    assert len(report.period_returns) == len(frames)
    assert report.full_fills == 2
    assert report.missed_fills == 0
    assert report.spread_cost > 0
    assert report.slippage_cost > 0
    assert report.regulatory_fees > 0
    assert report.open_positions == {}


def test_decision_price_marketable_limit_records_missed_fill() -> None:
    frames = (_frame(0, "100"), _frame(1, "101"), _frame(2, "102"))
    snapshots = {
        ("SPY", frames[0].timestamp): _snapshot(frames[0], "99.99", "100.01"),
        ("SPY", frames[1].timestamp): _snapshot(frames[1], "100.99", "101.01"),
        ("SPY", frames[2].timestamp): _snapshot(frames[2], "101.99", "102.01"),
    }

    report = simulate_observed_execution(
        frames,
        OneDayStrategy(),
        snapshots,
        execution_style="decision_marketable_limit",
    )

    assert report.missed_fills == 1
    assert report.full_fills == 0
    assert report.total_return == 0
