from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from tradeagent.domain import MarketBar
from tradeagent.economic_ml import build_economic_events, evaluate_economic_ml
from tradeagent.universe import UniverseFrame


def _daily_frames(count: int = 1_300) -> tuple[UniverseFrame, ...]:
    output = []
    for index in range(count):
        timestamp = datetime(2020, 1, 1, tzinfo=UTC) + timedelta(days=index)
        bars = []
        for symbol, multiplier in (("SPY", 1), ("QQQ", 2)):
            price = Decimal("100") + Decimal(index * multiplier) / Decimal(10)
            bars.append(
                MarketBar(
                    symbol=symbol,
                    timestamp=timestamp,
                    open=price,
                    high=price,
                    low=price,
                    close=price,
                    volume=Decimal("1000000"),
                )
            )
        output.append(UniverseFrame(timestamp=timestamp, bars=tuple(bars)))
    return tuple(output)


def test_economic_ml_uses_large_net_return_event_set_and_temporal_folds() -> None:
    events = build_economic_events(_daily_frames())
    report = evaluate_economic_ml(events)

    assert len(events) >= 1_000
    assert report.eligible
    assert len(report.attempted_models) == 3
    assert report.baseline is not None
    assert all(len(attempt.folds) >= 3 for attempt in report.attempted_models)
    assert "net return" in report.target


def test_economic_ml_stays_disabled_below_event_minimums() -> None:
    events = build_economic_events(_daily_frames(100))
    report = evaluate_economic_ml(events)

    assert not report.eligible
    assert not report.qualified
    assert report.attempted_models == ()
    assert "INSUFFICIENT_CANDIDATE_EVENTS" in report.eligibility_reasons
