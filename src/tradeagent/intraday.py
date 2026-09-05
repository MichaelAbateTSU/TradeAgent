from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from enum import StrEnum
from itertools import pairwise
from zoneinfo import ZoneInfo

import exchange_calendars
from pydantic import BaseModel, ConfigDict

from tradeagent.config import IntradayConfig
from tradeagent.domain import MarketBar
from tradeagent.universe import UniverseFrame


class SessionPhase(StrEnum):
    CLOSED = "closed"
    PRE_ENTRY = "pre_entry"
    ENTRY = "entry"
    MANAGE_ONLY = "manage_only"
    FLATTEN = "flatten"


class SessionGate(BaseModel):
    model_config = ConfigDict(frozen=True)

    phase: SessionPhase
    session_open: datetime | None
    session_close: datetime | None
    can_enter: bool
    must_flatten: bool
    reason: str


class AggregationResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    bars: tuple[MarketBar, ...]
    dropped_incomplete_bars: int


class IntradayDataGapError(ValueError):
    pass


class NyseSessionCalendar:
    def __init__(self, config: IntradayConfig) -> None:
        self._config = config
        self._calendar = exchange_calendars.get_calendar("XNYS")
        self._timezone = ZoneInfo(config.timezone)

    def session_bounds(self, session_date: date) -> tuple[datetime, datetime] | None:
        if not self._calendar.is_session(session_date.isoformat()):
            return None
        session = self._calendar.date_to_session(session_date.isoformat())
        session_open = self._calendar.session_open(session).to_pydatetime()
        session_close = self._calendar.session_close(session).to_pydatetime()
        return session_open.astimezone(UTC), session_close.astimezone(UTC)

    def gate(self, observed_at: datetime) -> SessionGate:
        if observed_at.tzinfo is None or observed_at.utcoffset() is None:
            raise ValueError("observed_at must be timezone-aware")
        local = observed_at.astimezone(self._timezone)
        bounds = self.session_bounds(local.date())
        if bounds is None:
            return SessionGate(
                phase=SessionPhase.CLOSED,
                session_open=None,
                session_close=None,
                can_enter=False,
                must_flatten=False,
                reason="exchange is closed for this date",
            )
        session_open, session_close = bounds
        entry_start = self._at_local(local.date(), self._config.entry_start)
        configured_no_entry = self._at_local(local.date(), self._config.no_new_entries_after)
        configured_flatten = self._at_local(local.date(), self._config.flatten_start)
        configured_hard_flatten = self._at_local(local.date(), self._config.hard_flatten_deadline)
        no_entry = min(configured_no_entry, session_close - timedelta(minutes=30))
        flatten = min(configured_flatten, session_close - timedelta(minutes=10))
        hard_flatten = min(
            configured_hard_flatten,
            session_close - timedelta(minutes=5),
        )

        if observed_at < session_open or observed_at > session_close:
            phase = SessionPhase.CLOSED
            reason = "outside regular session"
        elif observed_at < entry_start:
            phase = SessionPhase.PRE_ENTRY
            reason = "entry warm-up window"
        elif observed_at < no_entry:
            phase = SessionPhase.ENTRY
            reason = "new entries permitted"
        elif observed_at < flatten:
            phase = SessionPhase.MANAGE_ONLY
            reason = "new entries disabled"
        else:
            phase = SessionPhase.FLATTEN
            reason = (
                "hard flatten deadline reached" if observed_at >= hard_flatten else "flatten window"
            )
        return SessionGate(
            phase=phase,
            session_open=session_open,
            session_close=session_close,
            can_enter=phase is SessionPhase.ENTRY,
            must_flatten=phase is SessionPhase.FLATTEN,
            reason=reason,
        )

    def _at_local(self, session_date: date, value: time) -> datetime:
        return datetime.combine(session_date, value, self._timezone).astimezone(UTC)


def aggregate_minute_bars(
    bars: Sequence[MarketBar],
    *,
    interval_minutes: int,
    calendar: NyseSessionCalendar,
) -> AggregationResult:
    if interval_minutes <= 0:
        raise ValueError("interval_minutes must be positive")
    if not bars:
        raise ValueError("at least one minute bar is required")
    symbols = {bar.symbol for bar in bars}
    if len(symbols) != 1:
        raise ValueError("aggregate one symbol at a time")
    for prior, current in pairwise(bars):
        if current.timestamp <= prior.timestamp:
            raise IntradayDataGapError("minute bars must be unique and chronological")
        if current.timestamp - prior.timestamp != timedelta(minutes=1):
            raise IntradayDataGapError(
                f"minute data gap between {prior.timestamp} and {current.timestamp}"
            )

    grouped: dict[datetime, list[MarketBar]] = {}
    for bar in bars:
        bounds = calendar.session_bounds(
            bar.timestamp.astimezone(ZoneInfo("America/New_York")).date()
        )
        if bounds is None or not bounds[0] <= bar.timestamp < bounds[1]:
            raise ValueError("minute bar is outside an exchange session")
        minutes_from_open = int((bar.timestamp - bounds[0]).total_seconds() // 60)
        bucket_start = bounds[0] + timedelta(
            minutes=(minutes_from_open // interval_minutes) * interval_minutes
        )
        grouped.setdefault(bucket_start, []).append(bar)

    output: list[MarketBar] = []
    dropped = 0
    for bucket_start, bucket in sorted(grouped.items()):
        if len(bucket) != interval_minutes:
            dropped += 1
            continue
        output.append(
            MarketBar(
                symbol=bucket[0].symbol,
                timestamp=bucket_start + timedelta(minutes=interval_minutes),
                open=bucket[0].open,
                high=max(bar.high for bar in bucket),
                low=min(bar.low for bar in bucket),
                close=bucket[-1].close,
                volume=sum((bar.volume for bar in bucket), Decimal(0)),
            )
        )
    return AggregationResult(
        bars=tuple(output),
        dropped_incomplete_bars=dropped,
    )


def regular_session_frames(
    frames: Sequence[UniverseFrame],
    calendar: NyseSessionCalendar,
) -> tuple[UniverseFrame, ...]:
    return tuple(
        frame for frame in frames if calendar.gate(frame.timestamp).phase is not SessionPhase.CLOSED
    )
