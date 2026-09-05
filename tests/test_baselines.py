from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from tradeagent.baselines import RandomTimingBaseline
from tradeagent.config import IntradayConfig
from tradeagent.domain import MarketBar
from tradeagent.universe import UniverseFrame


def _frames() -> list[UniverseFrame]:
    frames = []
    start = datetime(2026, 9, 4, 14, 0, tzinfo=UTC)
    for index in range(20):
        timestamp = start + timedelta(minutes=index * 5)
        bar = MarketBar(
            symbol="SPY",
            timestamp=timestamp,
            open=Decimal("100"),
            high=Decimal("101"),
            low=Decimal("99"),
            close=Decimal("100"),
            volume=Decimal("1000"),
        )
        frames.append(UniverseFrame(timestamp=timestamp, bars=(bar,)))
    return frames


def test_random_timing_baseline_is_seeded_and_bounded() -> None:
    first = RandomTimingBaseline(
        IntradayConfig(enabled=True),
        seed=7,
        entry_probability=0.25,
        hold_frames=3,
    )
    second = RandomTimingBaseline(
        IntradayConfig(enabled=True),
        seed=7,
        entry_probability=0.25,
        hold_frames=3,
    )

    first_targets = [strategy.target_weights for strategy in map(first.on_frame, _frames())]
    second_targets = [strategy.target_weights for strategy in map(second.on_frame, _frames())]

    assert first_targets == second_targets
    assert all(sum(targets.values()) <= Decimal("0.0025") for targets in first_targets)


def test_random_timing_baseline_validates_inputs() -> None:
    with pytest.raises(ValueError, match="between"):
        RandomTimingBaseline(
            IntradayConfig(),
            seed=1,
            entry_probability=2,
            hold_frames=3,
        )
    with pytest.raises(ValueError, match="positive"):
        RandomTimingBaseline(
            IntradayConfig(),
            seed=1,
            entry_probability=0.1,
            hold_frames=0,
        )
