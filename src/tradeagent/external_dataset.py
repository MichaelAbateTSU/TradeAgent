from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

from tradeagent.alpaca import AlpacaDataClient
from tradeagent.data import read_bars, write_bars
from tradeagent.domain import MarketBar
from tradeagent.universe import UniverseFrame


class ExternalDataFile(BaseModel):
    model_config = ConfigDict(frozen=True)

    symbol: str
    path: str
    rows: int = Field(gt=0)
    started_at: datetime
    ended_at: datetime
    sha256: str = Field(min_length=64, max_length=64)


class ExternalEraManifest(BaseModel):
    model_config = ConfigDict(frozen=True)

    version: str
    era: str
    provider: str
    feed: str
    requested_start: datetime
    requested_end: datetime
    created_at: datetime
    universe_policy: str
    inception_policy: str
    development_era_used: bool
    sealed_holdouts_used: bool
    files: tuple[ExternalDataFile, ...]
    manifest_hash: str


def build_external_daily_dataset(
    client: AlpacaDataClient,
    output_directory: Path,
    *,
    era: str,
    start: datetime,
    end: datetime,
    symbols: Sequence[str],
    on_file: Callable[[ExternalDataFile], None] | None = None,
) -> ExternalEraManifest:
    files: list[ExternalDataFile] = []
    for symbol in tuple(dict.fromkeys(value.strip().upper() for value in symbols)):
        path = output_directory / f"{symbol}.csv"
        if not path.exists():
            partial = path.with_suffix(".csv.partial")
            partial.unlink(missing_ok=True)
            rows = write_bars(
                partial,
                client.bars(
                    symbol,
                    start=start,
                    end=end,
                    timeframe="1Day",
                    feed="sip",
                ),
            )
            if rows == 0:
                partial.unlink(missing_ok=True)
                raise ValueError(f"no external daily data returned for {symbol}")
            path.parent.mkdir(parents=True, exist_ok=True)
            partial.replace(path)
        bars = tuple(read_bars(path))
        record = ExternalDataFile(
            symbol=symbol,
            path=path.as_posix(),
            rows=len(bars),
            started_at=bars[0].timestamp,
            ended_at=bars[-1].timestamp,
            sha256=sha256(path.read_bytes()).hexdigest(),
        )
        files.append(record)
        if on_file is not None:
            on_file(record)
    identity = {
        "version": "v0.10.0-external-daily-1",
        "era": era,
        "provider": "alpaca",
        "feed": "sip",
        "requested_start": start.astimezone(UTC).isoformat(),
        "requested_end": end.astimezone(UTC).isoformat(),
        "universe_policy": "fixed 21-ETF v0.9 universe; no result-based inclusion",
        "inception_policy": "symbol enters only on its first observed session; no backfill",
        "development_era_used": False,
        "sealed_holdouts_used": False,
        "files": [record.model_dump(mode="json") for record in files],
    }
    manifest_hash = sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return ExternalEraManifest(
        version="v0.10.0-external-daily-1",
        era=era,
        provider="alpaca",
        feed="sip",
        requested_start=start,
        requested_end=end,
        created_at=datetime.now(UTC),
        universe_policy="fixed 21-ETF v0.9 universe; no result-based inclusion",
        inception_policy="symbol enters only on its first observed session; no backfill",
        development_era_used=False,
        sealed_holdouts_used=False,
        files=tuple(files),
        manifest_hash=manifest_hash,
    )


def load_staggered_universe(directory: Path, symbols: Sequence[str]) -> tuple[UniverseFrame, ...]:
    by_timestamp: defaultdict[datetime, list[MarketBar]] = defaultdict(list)
    for symbol in symbols:
        for bar in read_bars(directory / f"{symbol}.csv", symbol=symbol):
            by_timestamp[bar.timestamp].append(bar)
    if not by_timestamp:
        raise ValueError("external universe has no bars")
    return tuple(
        UniverseFrame(
            timestamp=timestamp,
            bars=tuple(sorted(bars, key=lambda bar: bar.symbol)),
        )
        for timestamp, bars in sorted(by_timestamp.items())
    )


def write_external_manifest(path: Path, manifest: ExternalEraManifest) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".partial")
    temporary.write_text(manifest.model_dump_json(indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)
