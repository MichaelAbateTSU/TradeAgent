from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from tradeagent.config import IntradayConfig
from tradeagent.domain import MarketBar
from tradeagent.intraday import (
    IntradayDataGapError,
    NyseSessionCalendar,
    SessionPhase,
    aggregate_minute_bars,
    regular_session_frames,
)
from tradeagent.universe import UniverseFrame


def _minute_bars(start: datetime, count: int) -> list[MarketBar]:
    return [
        MarketBar(
            symbol="SPY",
            timestamp=start + timedelta(minutes=index),
            open=Decimal(100 + index),
            high=Decimal("100.5") + index,
            low=Decimal("99.5") + index,
            close=Decimal("100.25") + index,
            volume=Decimal(100 + index),
        )
        for index in range(count)
    ]


def test_nyse_calendar_handles_holiday_and_early_close() -> None:
    calendar = NyseSessionCalendar(IntradayConfig())

    assert calendar.session_bounds(date(2026, 9, 7)) is None
    regular = calendar.session_bounds(date(2026, 9, 4))
    early = calendar.session_bounds(date(2026, 11, 27))

    assert regular is not None
    assert regular[0] == datetime(2026, 9, 4, 13, 30, tzinfo=UTC)
    assert regular[1] == datetime(2026, 9, 4, 20, 0, tzinfo=UTC)
    assert early is not None
    assert early[1] == datetime(2026, 11, 27, 18, 0, tzinfo=UTC)


def test_session_gate_transitions_and_early_flatten() -> None:
    calendar = NyseSessionCalendar(IntradayConfig())

    assert calendar.gate(datetime(2026, 9, 4, 13, 34, tzinfo=UTC)).phase is SessionPhase.PRE_ENTRY
    assert calendar.gate(datetime(2026, 9, 4, 13, 35, tzinfo=UTC)).can_enter
    assert calendar.gate(datetime(2026, 9, 4, 19, 31, tzinfo=UTC)).phase is SessionPhase.MANAGE_ONLY
    assert calendar.gate(datetime(2026, 9, 4, 19, 51, tzinfo=UTC)).must_flatten
    assert calendar.gate(datetime(2026, 9, 4, 20, 1, tzinfo=UTC)).phase is SessionPhase.CLOSED
    assert calendar.gate(datetime(2026, 9, 7, 15, 0, tzinfo=UTC)).phase is SessionPhase.CLOSED
    assert calendar.gate(datetime(2026, 11, 27, 17, 51, tzinfo=UTC)).must_flatten


def test_aggregate_minute_bars_builds_complete_intervals() -> None:
    calendar = NyseSessionCalendar(IntradayConfig())
    bars = _minute_bars(datetime(2026, 9, 4, 13, 30, tzinfo=UTC), 11)

    result = aggregate_minute_bars(
        bars,
        interval_minutes=5,
        calendar=calendar,
    )

    assert len(result.bars) == 2
    assert result.dropped_incomplete_bars == 1
    assert result.bars[0].timestamp == datetime(2026, 9, 4, 13, 35, tzinfo=UTC)
    assert result.bars[0].open == Decimal("100")
    assert result.bars[0].close == Decimal("104.25")
    assert result.bars[0].volume == sum(Decimal(100 + index) for index in range(5))


def test_aggregate_minute_bars_rejects_gaps_and_mixed_symbols() -> None:
    calendar = NyseSessionCalendar(IntradayConfig())
    bars = _minute_bars(datetime(2026, 9, 4, 13, 30, tzinfo=UTC), 5)
    with pytest.raises(IntradayDataGapError, match="gap"):
        aggregate_minute_bars(
            [*bars[:2], *bars[3:]],
            interval_minutes=5,
            calendar=calendar,
        )
    with pytest.raises(ValueError, match="one symbol"):
        aggregate_minute_bars(
            [bars[0], bars[1].model_copy(update={"symbol": "QQQ"})],
            interval_minutes=5,
            calendar=calendar,
        )


def test_regular_session_frames_remove_after_hours() -> None:
    calendar = NyseSessionCalendar(IntradayConfig())
    regular_bar = _minute_bars(datetime(2026, 9, 4, 13, 30, tzinfo=UTC), 1)[0]
    after_hours = regular_bar.model_copy(
        update={"timestamp": datetime(2026, 9, 4, 21, 0, tzinfo=UTC)}
    )
    frames = (
        UniverseFrame(timestamp=regular_bar.timestamp, bars=(regular_bar,)),
        UniverseFrame(timestamp=after_hours.timestamp, bars=(after_hours,)),
    )

    filtered = regular_session_frames(frames, calendar)

    assert filtered == (frames[0],)
