from __future__ import annotations

import csv
import json
from collections.abc import Callable, Iterator, Sequence
from datetime import UTC, datetime
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from statistics import median
from typing import Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field

from tradeagent.alpaca import HistoricalTimeframe
from tradeagent.data import write_bars
from tradeagent.domain import MarketBar

V09_ETF_UNIVERSE = (
    "SPY",
    "QQQ",
    "IWM",
    "DIA",
    "XLB",
    "XLC",
    "XLE",
    "XLF",
    "XLI",
    "XLK",
    "XLP",
    "XLRE",
    "XLU",
    "XLV",
    "XLY",
    "SHY",
    "IEF",
    "TLT",
    "GLD",
    "EFA",
    "EEM",
)
V09_TIMEFRAMES: tuple[HistoricalTimeframe, ...] = ("1Day", "1Hour", "30Min", "5Min")
TIMEFRAME_DIRECTORIES = {
    "1Day": "1day",
    "1Hour": "1hour",
    "30Min": "30min",
    "5Min": "5min",
}


class ResearchDataFile(BaseModel):
    model_config = ConfigDict(frozen=True)

    symbol: str
    timeframe: str
    path: str
    rows: int = Field(gt=0)
    sha256: str = Field(min_length=64, max_length=64)
    started_at: datetime
    ended_at: datetime


class ResearchDatasetManifest(BaseModel):
    model_config = ConfigDict(frozen=True)

    version: str
    provider: str
    feed: str
    adjusted: bool
    requested_start: datetime
    requested_end: datetime
    created_at: datetime
    symbols: tuple[str, ...]
    timeframes: tuple[str, ...]
    liquidity_requirements: dict[str, str]
    liquidity_observations: dict[str, dict[str, str | bool]]
    sealed_holdouts_used: bool
    files: tuple[ResearchDataFile, ...]
    manifest_hash: str


class HistoricalBarSource(Protocol):
    def bars(
        self,
        symbol: str,
        *,
        start: datetime,
        end: datetime,
        timeframe: HistoricalTimeframe,
        feed: Literal["iex", "sip"],
    ) -> Iterator[MarketBar]: ...


def build_v09_bar_dataset(
    client: HistoricalBarSource,
    output_directory: Path,
    *,
    start: datetime,
    end: datetime,
    symbols: Sequence[str] = V09_ETF_UNIVERSE,
    timeframes: Sequence[HistoricalTimeframe] = V09_TIMEFRAMES,
    on_file: Callable[[ResearchDataFile], None] | None = None,
) -> ResearchDatasetManifest:
    normalized_symbols = tuple(dict.fromkeys(symbol.strip().upper() for symbol in symbols))
    normalized_timeframes = tuple(dict.fromkeys(timeframes))
    if not normalized_symbols or not normalized_timeframes:
        raise ValueError("research dataset requires symbols and timeframes")
    unknown_timeframes = set(normalized_timeframes) - set(V09_TIMEFRAMES)
    if unknown_timeframes:
        raise ValueError(f"unsupported research timeframes: {sorted(unknown_timeframes)}")

    files: list[ResearchDataFile] = []
    for timeframe in normalized_timeframes:
        for symbol in normalized_symbols:
            path = output_directory / TIMEFRAME_DIRECTORIES[timeframe] / f"{symbol}.csv"
            if path.exists():
                count, started_at, ended_at = _csv_stats(path)
                record = ResearchDataFile(
                    symbol=symbol,
                    timeframe=timeframe,
                    path=path.as_posix(),
                    rows=count,
                    sha256=_file_hash(path),
                    started_at=started_at,
                    ended_at=ended_at,
                )
                files.append(record)
                if on_file is not None:
                    on_file(record)
                continue
            temporary = path.with_suffix(".csv.partial")
            temporary.unlink(missing_ok=True)
            count = write_bars(
                temporary,
                client.bars(
                    symbol,
                    start=start,
                    end=end,
                    timeframe=timeframe,
                    feed="sip",
                ),
            )
            if count == 0:
                temporary.unlink(missing_ok=True)
                raise ValueError(f"Alpaca returned no {timeframe} bars for {symbol}")
            _, started_at, ended_at = _csv_stats(temporary)
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary.replace(path)
            record = ResearchDataFile(
                symbol=symbol,
                timeframe=timeframe,
                path=path.as_posix(),
                rows=count,
                sha256=_file_hash(path),
                started_at=started_at,
                ended_at=ended_at,
            )
            files.append(record)
            if on_file is not None:
                on_file(record)

    liquidity_observations = {
        symbol: _daily_liquidity(output_directory / TIMEFRAME_DIRECTORIES["1Day"] / f"{symbol}.csv")
        for symbol in normalized_symbols
    }
    payload = {
        "version": "v0.9.0-bars-1",
        "provider": "alpaca",
        "feed": "sip",
        "adjusted": True,
        "requested_start": start.astimezone(UTC).isoformat(),
        "requested_end": end.astimezone(UTC).isoformat(),
        "symbols": normalized_symbols,
        "timeframes": normalized_timeframes,
        "liquidity_requirements": {
            "instrument_type": "US-listed ETF",
            "minimum_median_daily_dollar_volume": "50000000 USD",
            "minimum_price": "5 USD",
            "maximum_decision_time_spread": "20 bps",
            "membership_policy": "predefined before download; no individual stocks",
        },
        "liquidity_observations": liquidity_observations,
        "sealed_holdouts_used": False,
        "files": [record.model_dump(mode="json") for record in files],
    }
    manifest_hash = sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return ResearchDatasetManifest(
        version="v0.9.0-bars-1",
        provider="alpaca",
        feed="sip",
        adjusted=True,
        requested_start=start,
        requested_end=end,
        created_at=datetime.now(UTC),
        symbols=normalized_symbols,
        timeframes=normalized_timeframes,
        liquidity_requirements={
            "instrument_type": "US-listed ETF",
            "minimum_median_daily_dollar_volume": "50000000 USD",
            "minimum_price": "5 USD",
            "maximum_decision_time_spread": "20 bps",
            "membership_policy": "predefined before download; no individual stocks",
        },
        liquidity_observations=liquidity_observations,
        sealed_holdouts_used=False,
        files=tuple(files),
        manifest_hash=manifest_hash,
    )


def write_dataset_manifest(path: Path, manifest: ResearchDatasetManifest) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".partial")
    temporary.write_text(
        manifest.model_dump_json(indent=2) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _csv_stats(path: Path) -> tuple[int, datetime, datetime]:
    first: datetime | None = None
    last: datetime | None = None
    count = 0
    with path.open(encoding="utf-8", newline="") as source:
        for row in csv.DictReader(source):
            timestamp = datetime.fromisoformat(row["timestamp"].replace("Z", "+00:00"))
            first = first or timestamp
            last = timestamp
            count += 1
    if first is None or last is None:
        raise ValueError(f"{path} contains no data rows")
    return count, first, last


def _file_hash(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _daily_liquidity(path: Path) -> dict[str, str | bool]:
    dollar_volumes: list[Decimal] = []
    closes: list[Decimal] = []
    with path.open(encoding="utf-8", newline="") as source:
        for row in csv.DictReader(source):
            close = Decimal(row["close"])
            closes.append(close)
            dollar_volumes.append(close * Decimal(row["volume"]))
    if not closes:
        raise ValueError(f"{path} contains no daily bars")
    median_dollar_volume = Decimal(str(median(dollar_volumes)))
    minimum_close = min(closes)
    return {
        "median_daily_dollar_volume": str(median_dollar_volume),
        "minimum_close": str(minimum_close),
        "passes": median_dollar_volume >= Decimal("50000000") and minimum_close >= Decimal("5"),
    }
