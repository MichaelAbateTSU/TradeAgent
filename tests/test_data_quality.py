from __future__ import annotations

from tradeagent.data import synthetic_bars
from tradeagent.data_quality import analyze_dataset
from tradeagent.universe import align_universe


def test_data_quality_reports_known_limitations() -> None:
    dataset = align_universe(
        {
            "SPY": list(synthetic_bars(symbol="SPY", count=20, seed=1)),
            "QQQ": list(synthetic_bars(symbol="QQQ", count=20, seed=2)),
        }
    )

    report = analyze_dataset(dataset, expected_regular_bars=1)

    assert report.aligned_frames == 20
    assert report.symbols == ("QQQ", "SPY")
    assert not report.quote_coverage_available
    assert not report.independent_vendor_verified
    assert all(item.duplicate_timestamps == 0 for item in report.symbol_quality)
    assert all(item.out_of_order == 0 for item in report.symbol_quality)
    assert all(item.missing_bars == 0 for item in report.symbol_quality)
