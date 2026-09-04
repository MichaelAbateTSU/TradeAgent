from __future__ import annotations

from itertools import islice
from pathlib import Path

import pytest

from tradeagent.data import read_bars, synthetic_bars, write_bars


def test_synthetic_bars_are_deterministic_and_skip_weekends() -> None:
    first = list(islice(synthetic_bars(seed=42), 20))
    second = list(islice(synthetic_bars(seed=42), 20))

    assert first == second
    assert all(bar.timestamp.weekday() < 5 for bar in first)
    assert all(bar.low <= bar.open <= bar.high for bar in first)
    assert all(bar.low <= bar.close <= bar.high for bar in first)


def test_read_bars_filters_symbol(tmp_path: Path) -> None:
    path = tmp_path / "bars.csv"
    path.write_text(
        "timestamp,symbol,open,high,low,close,volume\n"
        "2025-01-02T21:00:00Z,SPY,100,102,99,101,1000\n"
        "2025-01-02T21:00:00Z,QQQ,200,202,199,201,2000\n",
        encoding="utf-8",
    )

    bars = list(read_bars(path, symbol="spy"))

    assert len(bars) == 1
    assert bars[0].symbol == "SPY"


def test_read_bars_requires_canonical_columns(tmp_path: Path) -> None:
    path = tmp_path / "bars.csv"
    path.write_text("timestamp,symbol\n2025-01-02T21:00:00Z,SPY\n", encoding="utf-8")

    with pytest.raises(ValueError, match="missing columns"):
        list(read_bars(path))


def test_write_bars_round_trips_and_refuses_overwrite(tmp_path: Path) -> None:
    path = tmp_path / "bars.csv"
    bars = list(islice(synthetic_bars(seed=3), 3))

    assert write_bars(path, bars) == 3
    assert list(read_bars(path)) == bars
    with pytest.raises(FileExistsError, match="already exists"):
        write_bars(path, bars)


def test_read_bars_rejects_duplicates(tmp_path: Path) -> None:
    path = tmp_path / "duplicates.csv"
    path.write_text(
        "timestamp,symbol,open,high,low,close,volume\n"
        "2025-01-02T21:00:00Z,SPY,100,102,99,101,1000\n"
        "2025-01-02T21:00:00Z,SPY,100,102,99,101,1000\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="strictly chronological"):
        list(read_bars(path))
