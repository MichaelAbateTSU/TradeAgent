from __future__ import annotations

from collections import Counter
from collections.abc import Sequence
from datetime import date, datetime
from decimal import Decimal
from itertools import pairwise
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict

from tradeagent.config import IntradayConfig
from tradeagent.domain import MarketBar
from tradeagent.intraday import NyseSessionCalendar
from tradeagent.universe import UniverseDataset


class SymbolQuality(BaseModel):
    model_config = ConfigDict(frozen=True)

    symbol: str
    observations: int
    sessions: int
    started_at: datetime
    ended_at: datetime
    duplicate_timestamps: int
    out_of_order: int
    zero_volume: int
    suspicious_jumps: int
    incomplete_sessions: int
    median_session_bars: Decimal
    expected_observations: int
    missing_bars: int
    missing_rate: Decimal


class DataQualityReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    dataset_hash: str
    symbols: tuple[str, ...]
    aligned_frames: int
    dropped_rows: dict[str, int]
    symbol_quality: tuple[SymbolQuality, ...]
    quote_coverage_available: bool = False
    spread_coverage_available: bool = False
    corporate_actions_verified: bool = False
    independent_vendor_verified: bool = False
    limitations: tuple[str, ...]


def analyze_dataset(
    dataset: UniverseDataset,
    *,
    timezone: str = "America/New_York",
    expected_regular_bars: int | None = None,
) -> DataQualityReport:
    by_symbol = {
        symbol: [frame.bar_for(symbol) for frame in dataset.frames]
        for symbol in dataset.manifest.symbols
    }
    calendar = NyseSessionCalendar(IntradayConfig(timezone=timezone))
    quality = tuple(
        _analyze_symbol(
            symbol,
            bars,
            ZoneInfo(timezone),
            expected_regular_bars,
            calendar,
        )
        for symbol, bars in by_symbol.items()
    )
    return DataQualityReport(
        dataset_hash=dataset.manifest.dataset_hash,
        symbols=dataset.manifest.symbols,
        aligned_frames=dataset.manifest.frames,
        dropped_rows=dataset.manifest.dropped_rows,
        symbol_quality=quality,
        limitations=(
            "Historical quote and spread coverage is unavailable in bar files.",
            "Corporate actions are vendor-adjusted but not independently verified.",
            "The dataset uses one vendor and has no delisted-universe coverage.",
            "Market impact and queue position are unavailable.",
        ),
    )


def _analyze_symbol(
    symbol: str,
    bars: Sequence[MarketBar],
    timezone: ZoneInfo,
    expected_regular_bars: int | None,
    calendar: NyseSessionCalendar,
) -> SymbolQuality:
    timestamps = [bar.timestamp for bar in bars]
    duplicates = len(timestamps) - len(set(timestamps))
    out_of_order = sum(current <= prior for prior, current in pairwise(timestamps))
    zero_volume = sum(bar.volume == 0 for bar in bars)
    suspicious_jumps = sum(
        abs(current.close / prior.close - Decimal(1)) > Decimal("0.10")
        for prior, current in pairwise(bars)
    )
    session_counts: Counter[date] = Counter(
        bar.timestamp.astimezone(timezone).date() for bar in bars
    )
    ordered_counts = sorted(session_counts.values())
    median_count = Decimal(ordered_counts[len(ordered_counts) // 2])
    incomplete = 0
    expected_observations = 0
    for session_date, count in session_counts.items():
        bounds = calendar.session_bounds(session_date)
        expected = (
            expected_regular_bars
            if expected_regular_bars is not None
            else int((bounds[1] - bounds[0]).total_seconds() // 300)
            if bounds is not None
            else 0
        )
        expected_observations += expected
        incomplete += int(count != expected)
    missing_bars = max(0, expected_observations - len(bars))
    return SymbolQuality(
        symbol=symbol,
        observations=len(bars),
        sessions=len(session_counts),
        started_at=bars[0].timestamp,
        ended_at=bars[-1].timestamp,
        duplicate_timestamps=duplicates,
        out_of_order=out_of_order,
        zero_volume=zero_volume,
        suspicious_jumps=suspicious_jumps,
        incomplete_sessions=incomplete,
        median_session_bars=median_count,
        expected_observations=expected_observations,
        missing_bars=missing_bars,
        missing_rate=(
            Decimal(missing_bars) / Decimal(expected_observations)
            if expected_observations
            else Decimal(0)
        ),
    )
