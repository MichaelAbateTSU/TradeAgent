from __future__ import annotations

import csv
import random
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from tradeagent.domain import MarketBar


def read_bars(path: Path, *, symbol: str | None = None) -> Iterator[MarketBar]:
    with path.open(encoding="utf-8", newline="") as source:
        reader = csv.DictReader(source)
        required = {"timestamp", "symbol", "open", "high", "low", "close", "volume"}
        if reader.fieldnames is None or not required.issubset(reader.fieldnames):
            missing = required - set(reader.fieldnames or ())
            raise ValueError(f"CSV is missing columns: {', '.join(sorted(missing))}")
        for row in reader:
            if symbol is not None and row["symbol"].strip().upper() != symbol.strip().upper():
                continue
            yield MarketBar(
                symbol=row["symbol"],
                timestamp=datetime.fromisoformat(row["timestamp"].replace("Z", "+00:00")),
                open=Decimal(row["open"]),
                high=Decimal(row["high"]),
                low=Decimal(row["low"]),
                close=Decimal(row["close"]),
                volume=Decimal(row["volume"]),
            )


def synthetic_bars(
    *,
    symbol: str = "SPY",
    count: int = 500,
    seed: int = 7,
    start: datetime = datetime(2024, 1, 2, 21, tzinfo=UTC),
) -> Iterator[MarketBar]:
    """Generate deterministic regime-changing daily bars for offline system tests."""
    generator = random.Random(seed)
    price = 100.0
    timestamp = start
    generated = 0
    while generated < count:
        if timestamp.weekday() >= 5:
            timestamp += timedelta(days=1)
            continue
        regime = (generated // 80) % 4
        drift = (0.0012, -0.0008, 0.0003, 0.0018)[regime]
        daily_return = drift + generator.gauss(0, 0.009)
        open_price = price
        close_price = max(1.0, price * (1 + daily_return))
        intraday_range = abs(generator.gauss(0.004, 0.002))
        high_price = max(open_price, close_price) * (1 + intraday_range)
        low_price = min(open_price, close_price) * max(0.01, 1 - intraday_range)
        yield MarketBar(
            symbol=symbol,
            timestamp=timestamp,
            open=Decimal(f"{open_price:.6f}"),
            high=Decimal(f"{high_price:.6f}"),
            low=Decimal(f"{low_price:.6f}"),
            close=Decimal(f"{close_price:.6f}"),
            volume=Decimal(generator.randint(5_000_000, 20_000_000)),
        )
        price = close_price
        generated += 1
        timestamp += timedelta(days=1)
