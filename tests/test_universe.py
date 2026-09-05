from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest

from tradeagent.data import synthetic_bars, write_bars
from tradeagent.universe import (
    UniverseFrame,
    align_universe,
    load_universe,
    symbol_filename,
)


def test_align_universe_keeps_shared_frames_and_tracks_drops() -> None:
    spy = list(synthetic_bars(symbol="SPY", count=5, seed=1))
    qqq = list(synthetic_bars(symbol="QQQ", count=4, seed=2, start=spy[1].timestamp))

    dataset = align_universe({"SPY": spy, "QQQ": qqq})

    assert dataset.manifest.symbols == ("QQQ", "SPY")
    assert dataset.manifest.frames == 4
    assert dataset.manifest.rows == 8
    assert dataset.manifest.dropped_rows == {"SPY": 1, "QQQ": 0}
    assert dataset.frames[0].bar_for("spy").symbol == "SPY"
    assert len(dataset.manifest.dataset_hash) == 64


def test_load_universe_round_trips_canonical_files(tmp_path: Path) -> None:
    spy = list(synthetic_bars(symbol="SPY", count=3, seed=1))
    qqq = list(synthetic_bars(symbol="QQQ", count=3, seed=2))
    write_bars(tmp_path / symbol_filename("SPY"), spy)
    write_bars(tmp_path / symbol_filename("QQQ"), qqq)

    dataset = load_universe(tmp_path, ["SPY", "QQQ"])

    assert dataset.manifest.frames == 3
    assert dataset.frames[-1].bar_for("QQQ") == qqq[-1]


def test_universe_validation_is_fail_closed() -> None:
    spy = list(synthetic_bars(symbol="SPY", count=3))
    qqq = list(
        synthetic_bars(
            symbol="QQQ",
            count=3,
            start=spy[0].timestamp + timedelta(hours=1),
        )
    )

    with pytest.raises(ValueError, match="at least one"):
        align_universe({})
    with pytest.raises(ValueError, match="no market bars"):
        align_universe({"SPY": []})
    with pytest.raises(ValueError, match="mismatched"):
        align_universe({"QQQ": spy})
    with pytest.raises(ValueError, match="no shared"):
        align_universe({"SPY": spy, "QQQ": qqq})


def test_universe_frame_rejects_duplicate_symbols() -> None:
    bar = next(synthetic_bars())

    with pytest.raises(ValueError, match="duplicate"):
        UniverseFrame(timestamp=bar.timestamp, bars=(bar, bar))
