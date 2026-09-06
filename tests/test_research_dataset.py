from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from tradeagent.domain import MarketBar
from tradeagent.research_dataset import build_v09_bar_dataset, write_dataset_manifest


class FakeDataClient:
    def __init__(self) -> None:
        self.calls = 0

    def bars(self, symbol: str, *, start, end, timeframe, feed):
        self.calls += 1
        assert feed == "sip"
        yield MarketBar(
            symbol=symbol,
            timestamp=start + timedelta(days=1),
            open=Decimal("100"),
            high=Decimal("101"),
            low=Decimal("99"),
            close=Decimal("100"),
            volume=Decimal("1000000"),
        )


def test_research_dataset_is_versioned_hashed_and_resumable(tmp_path: Path) -> None:
    client = FakeDataClient()
    start = datetime(2020, 1, 1, tzinfo=UTC)
    end = datetime(2025, 1, 1, tzinfo=UTC)
    output = tmp_path / "bars"

    first = build_v09_bar_dataset(
        client,
        output,
        start=start,
        end=end,
        symbols=("SPY",),
        timeframes=("1Day", "30Min"),
    )
    second = build_v09_bar_dataset(
        client,
        output,
        start=start,
        end=end,
        symbols=("SPY",),
        timeframes=("1Day", "30Min"),
    )
    manifest_path = tmp_path / "manifest.json"
    write_dataset_manifest(manifest_path, second)

    assert client.calls == 2
    assert first.manifest_hash == second.manifest_hash
    assert len(first.files) == 2
    assert all(len(item.sha256) == 64 for item in first.files)
    assert not first.sealed_holdouts_used
    assert manifest_path.exists()
