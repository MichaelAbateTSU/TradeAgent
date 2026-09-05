from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from hashlib import sha256
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, model_validator

from tradeagent.data import read_bars
from tradeagent.domain import MarketBar


class UniverseFrame(BaseModel):
    model_config = ConfigDict(frozen=True)

    timestamp: datetime
    bars: tuple[MarketBar, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_frame(self) -> UniverseFrame:
        symbols = [bar.symbol for bar in self.bars]
        if len(symbols) != len(set(symbols)):
            raise ValueError("universe frame cannot contain duplicate symbols")
        if any(bar.timestamp != self.timestamp for bar in self.bars):
            raise ValueError("all frame bars must share the frame timestamp")
        return self

    def bar_for(self, symbol: str) -> MarketBar:
        normalized = symbol.strip().upper()
        for bar in self.bars:
            if bar.symbol == normalized:
                return bar
        raise KeyError(f"{normalized} is not present in the frame")


class UniverseManifest(BaseModel):
    model_config = ConfigDict(frozen=True)

    dataset_hash: str = Field(min_length=64, max_length=64)
    symbols: tuple[str, ...]
    frames: int = Field(gt=0)
    rows: int = Field(gt=0)
    started_at: datetime
    ended_at: datetime
    dropped_rows: dict[str, int]


class UniverseDataset(BaseModel):
    model_config = ConfigDict(frozen=True)

    manifest: UniverseManifest
    frames: tuple[UniverseFrame, ...]


def symbol_filename(symbol: str) -> str:
    return f"{symbol.strip().upper().replace('/', '-')}.csv"


def align_universe(
    bars_by_symbol: Mapping[str, Sequence[MarketBar]],
) -> UniverseDataset:
    if not bars_by_symbol:
        raise ValueError("universe requires at least one symbol")

    indexed: dict[str, dict[datetime, MarketBar]] = {}
    for requested_symbol, bars in bars_by_symbol.items():
        symbol = requested_symbol.strip().upper()
        if not bars:
            raise ValueError(f"{symbol} has no market bars")
        if any(bar.symbol != symbol for bar in bars):
            raise ValueError(f"{symbol} input contains a mismatched bar symbol")
        timestamps = [bar.timestamp for bar in bars]
        if timestamps != sorted(timestamps) or len(timestamps) != len(set(timestamps)):
            raise ValueError(f"{symbol} bars must be unique and chronological")
        indexed[symbol] = {bar.timestamp: bar for bar in bars}

    shared_timestamps = set.intersection(*(set(symbol_bars) for symbol_bars in indexed.values()))
    if not shared_timestamps:
        raise ValueError("universe symbols have no shared timestamps")
    symbols = tuple(sorted(indexed))
    frames = tuple(
        UniverseFrame(
            timestamp=timestamp,
            bars=tuple(indexed[symbol][timestamp] for symbol in symbols),
        )
        for timestamp in sorted(shared_timestamps)
    )
    canonical = "\n".join(bar.model_dump_json() for frame in frames for bar in frame.bars)
    dropped_rows = {
        symbol: len(symbol_bars) - len(frames) for symbol, symbol_bars in indexed.items()
    }
    manifest = UniverseManifest(
        dataset_hash=sha256(canonical.encode()).hexdigest(),
        symbols=symbols,
        frames=len(frames),
        rows=sum(len(frame.bars) for frame in frames),
        started_at=frames[0].timestamp,
        ended_at=frames[-1].timestamp,
        dropped_rows=dropped_rows,
    )
    return UniverseDataset(manifest=manifest, frames=frames)


def load_universe(directory: Path, symbols: Sequence[str]) -> UniverseDataset:
    bars_by_symbol = {
        symbol.strip().upper(): list(
            read_bars(
                directory / symbol_filename(symbol),
                symbol=symbol,
            )
        )
        for symbol in symbols
    }
    return align_universe(bars_by_symbol)
