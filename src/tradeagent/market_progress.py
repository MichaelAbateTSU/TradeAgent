"""Shared bounded durable-market evidence for release acceptance and scheduled workers."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import func, select

from tradeagent.persistence import Database, market_bars, market_quotes, market_trades

RECORDER_SYMBOLS = ("SPY", "QQQ", "IWM", "TLT", "GLD")


def market_window_counts(
    database: Database,
    *,
    since: datetime,
    until: datetime,
    symbols: tuple[str, ...] = RECORDER_SYMBOLS,
) -> dict[str, Any]:
    if since.tzinfo is None or until.tzinfo is None or since > until or not symbols:
        raise ValueError("An ordered aware window and explicit recorder symbols are required")
    result = {}
    with database.begin() as connection:
        for table in (market_quotes, market_trades, market_bars):
            result[table.name] = [
                dict(row)
                for row in connection.execute(
                    select(
                        table.c.symbol,
                        func.count().label("count"),
                        func.max(table.c.event_at).label("latest_exchange_at"),
                        func.max(table.c.received_at).label("latest_received_at"),
                        func.max(table.c.processed_at).label("latest_committed_at"),
                    )
                    .where(
                        table.c.symbol.in_(symbols),
                        table.c.event_at >= since,
                        table.c.event_at <= until,
                    )
                    .group_by(table.c.symbol)
                ).mappings()
            ]
    return result


def physical_progress_failures(first: dict[str, Any], last: dict[str, Any]) -> list[str]:
    """Validate indexed physical endpoints; aggregate counters cannot prove each symbol."""

    def timestamp(value: Any) -> datetime:
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(value)
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError("Physical evidence timestamps must be timezone-aware")
        return parsed

    try:
        scopes = (first["market_count_scope"], last["market_count_scope"])
        starts = [timestamp(scope["since_exchange_at"]) for scope in scopes]
        ends = [timestamp(scope["until_exchange_at"]) for scope in scopes]
        if (
            starts[0] != starts[1]
            or not timedelta(0) < ends[1] - ends[0] < timedelta(minutes=25)
            or any(
                not timedelta(0) <= end - start <= timedelta(minutes=30)
                for start, end in zip(starts, ends, strict=True)
            )
            or any(
                len(scope["symbols"]) != len(RECORDER_SYMBOLS)
                or set(scope["symbols"]) != set(RECORDER_SYMBOLS)
                for scope in scopes
            )
        ):
            return ["physical:invalid_or_changed_fixed_window"]
    except (KeyError, TypeError, ValueError):
        return ["physical:missing_or_invalid_window_metadata"]

    failures = []
    for table in ("market_quotes", "market_trades", "market_bars"):
        try:
            series = []
            for sample, end in zip((first, last), ends, strict=True):
                rows = sample["market"][table]
                by_symbol = {row["symbol"]: row for row in rows}
                if len(by_symbol) != len(rows) or set(by_symbol) - set(RECORDER_SYMBOLS):
                    raise ValueError("Duplicate or unexpected physical symbol")
                for row in rows:
                    if type(row["count"]) is not int or row["count"] <= 0:
                        raise ValueError("Physical row counts must be positive integers")
                    exchange = timestamp(row["latest_exchange_at"])
                    if not starts[0] <= exchange <= end:
                        raise ValueError("Exchange proof outside the fixed window")
                    received = timestamp(row["latest_received_at"])
                    committed = timestamp(row["latest_committed_at"])
                    if not exchange <= received <= committed:
                        raise ValueError("Invalid exchange/receipt/processing chronology")
                series.append(by_symbol)
            for symbol in RECORDER_SYMBOLS:
                before, after = (rows.get(symbol) for rows in series)
                if (
                    after is None
                    or after["count"] <= (before["count"] if before else 0)
                    or (
                        before is not None
                        and timestamp(after["latest_exchange_at"])
                        <= timestamp(before["latest_exchange_at"])
                    )
                ):
                    failures.append(f"physical:{table}:{symbol}:not_advancing")
        except (KeyError, TypeError, ValueError):
            failures.append(f"physical:{table}:invalid_rows_or_timestamps")
    return failures
