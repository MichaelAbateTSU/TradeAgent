from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from tradeagent.domain import MarketBar
from tradeagent.external_dataset import (
    build_external_daily_dataset,
    load_staggered_universe,
)


class FakeDailyClient:
    def bars(self, symbol, *, start, end, timeframe, feed):
        assert timeframe == "1Day"
        assert feed == "sip"
        offset = 0 if symbol == "SPY" else 1
        for index in range(offset, 3):
            timestamp = start + timedelta(days=index)
            yield MarketBar(
                symbol=symbol,
                timestamp=timestamp,
                open=Decimal("100"),
                high=Decimal("100"),
                low=Decimal("100"),
                close=Decimal("100"),
                volume=Decimal("1000"),
            )


def test_external_dataset_preserves_inception_and_hashes_files(tmp_path: Path) -> None:
    start = datetime(2016, 1, 1, tzinfo=UTC)
    manifest = build_external_daily_dataset(
        FakeDailyClient(),
        tmp_path,
        era="pre-2020",
        start=start,
        end=datetime(2020, 1, 1, tzinfo=UTC),
        symbols=("SPY", "XLC"),
    )
    frames = load_staggered_universe(tmp_path, ("SPY", "XLC"))

    assert len(manifest.files) == 2
    assert manifest.files[0].rows == 3
    assert manifest.files[1].rows == 2
    assert len(manifest.manifest_hash) == 64
    assert [bar.symbol for bar in frames[0].bars] == ["SPY"]
    assert [bar.symbol for bar in frames[1].bars] == ["SPY", "XLC"]
