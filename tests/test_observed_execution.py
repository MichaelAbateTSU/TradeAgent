from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from tradeagent.alpaca import HistoricalQuote, HistoricalTrade
from tradeagent.domain import MarketBar
from tradeagent.lower_execution_evidence import (
    LowerEvidenceAnchor,
    PointInTimeSnapshot,
)
from tradeagent.observed_execution import (
    DEFAULT_FEE_SCHEDULE,
    ExecutionEvidenceUnavailableError,
    _fill,
    simulate_observed_execution,
)
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


def test_legacy_observed_execution_is_unavailable_without_accounting_provenance() -> None:
    frames = (_frame(0, "100"), _frame(1, "101"), _frame(2, "102"))
    snapshots = {
        ("SPY", frame.timestamp): _snapshot(
            frame,
            str(frame.bars[0].close - Decimal("0.01")),
            str(frame.bars[0].close + Decimal("0.01")),
        )
        for frame in frames
    }

    strategy = OneDayStrategy()
    with pytest.raises(ExecutionEvidenceUnavailableError, match="raw price/share basis"):
        simulate_observed_execution(frames, strategy, snapshots, execution_style="market")
    assert strategy.frames == 0


def test_decision_price_marketable_limit_records_missed_fill() -> None:
    frames = (_frame(0, "100"), _frame(1, "101"), _frame(2, "102"))
    snapshots = {
        ("SPY", frames[0].timestamp): _snapshot(frames[0], "99.99", "100.01"),
        ("SPY", frames[1].timestamp): _snapshot(frames[1], "100.99", "101.01"),
        ("SPY", frames[2].timestamp): _snapshot(frames[2], "101.99", "102.01"),
    }

    with pytest.raises(ExecutionEvidenceUnavailableError, match="effective-dated"):
        simulate_observed_execution(
            frames,
            OneDayStrategy(),
            snapshots,
            execution_style="decision_marketable_limit",
        )


@pytest.mark.parametrize("missing_after", [False, True])
def test_synthetic_close_snapshot_is_not_an_executable_arrival(missing_after: bool) -> None:
    frame = _frame(0, "100")
    snapshot = _snapshot(frame, "99", "101")
    if missing_after:
        snapshot = snapshot.model_copy(update={"quote_after": None})
    result = _fill(
        "SPY",
        frame.timestamp,
        frame.timestamp,
        Decimal("0.25"),
        buy=True,
        snapshots={("SPY", frame.timestamp): snapshot},
        execution_style="market",
        cost_multiplier=Decimal(1),
        fee_schedule=DEFAULT_FEE_SCHEDULE,
        quote_size_units="shares",
    )
    assert result.quantity == 0
    assert result.status == "unavailable"


def test_synthetic_arrival_move_is_not_charged_again_as_slippage() -> None:
    frame = _frame(0, "100").model_copy(
        update={
            "timestamp": datetime(2024, 1, 2, 15, tzinfo=UTC),
        }
    )
    snapshot = _snapshot(frame, "99", "101")
    assert snapshot.quote_after is not None
    snapshot = snapshot.model_copy(
        update={
            "quote_after": snapshot.quote_after.model_copy(
                update={
                    "bid_price": Decimal("100"),
                    "ask_price": Decimal("102"),
                }
            ),
        }
    )
    result = _fill(
        "SPY",
        frame.timestamp,
        frame.timestamp,
        Decimal("0.25"),
        buy=True,
        snapshots={("SPY", frame.timestamp): snapshot},
        execution_style="market",
        cost_multiplier=Decimal(1),
        fee_schedule=DEFAULT_FEE_SCHEDULE,
        quote_size_units="shares",
    )
    assert result.price == Decimal("102.00505")
    assert result.delay_cost == Decimal("0.25")
    assert result.slippage_cost == Decimal("0.0012625")
