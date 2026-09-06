from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from tradeagent.domain import MarketBar
from tradeagent.squeeze_external import _aggregate_hourly, _regular_bars


def _bar(timestamp: datetime, price: int) -> MarketBar:
    value = Decimal(price)
    return MarketBar(
        symbol="SPY",
        timestamp=timestamp,
        open=value,
        high=value + 1,
        low=value - 1,
        close=value,
        volume=Decimal("1000"),
    )


def test_external_matrix_excludes_premarket_and_aggregates_from_session_open() -> None:
    premarket_close = datetime(2024, 1, 2, 14, 30, tzinfo=UTC)
    bars = tuple(
        _bar(premarket_close + timedelta(minutes=30 * index), 100 + index) for index in range(14)
    )

    regular = _regular_bars(bars, minutes=30)
    hourly = _aggregate_hourly(regular)

    assert len(regular) == 13
    assert regular[0].timestamp == datetime(2024, 1, 2, 15, 0, tzinfo=UTC)
    assert len(hourly) == 7
    assert hourly[0].timestamp == datetime(2024, 1, 2, 15, 30, tzinfo=UTC)
    assert hourly[-1].timestamp == datetime(2024, 1, 2, 21, 0, tzinfo=UTC)
