from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import date, time
from decimal import Decimal
from statistics import mean, stdev
from zoneinfo import ZoneInfo

from tradeagent.config import IntradayConfig
from tradeagent.domain import MarketBar
from tradeagent.intraday import NyseSessionCalendar, SessionPhase
from tradeagent.portfolio import PortfolioIntent
from tradeagent.universe import UniverseFrame


@dataclass
class _SessionState:
    session_date: date
    opening_price: Decimal
    price_volume: Decimal = Decimal(0)
    volume: Decimal = Decimal(0)


class NoiseAreaMomentumStrategy:
    """Long-only intraday momentum outside a point-in-time historical noise boundary."""

    def __init__(self, intraday: IntradayConfig, *, target_weight: Decimal = Decimal("0.0025")):
        self._calendar = NyseSessionCalendar(intraday)
        self._timezone = ZoneInfo(intraday.timezone)
        self._target = target_weight
        self._sessions: dict[str, _SessionState] = {}
        self._moves: defaultdict[tuple[str, time], deque[Decimal]] = defaultdict(
            lambda: deque(maxlen=14)
        )
        self._active: set[str] = set()

    @property
    def strategy_id(self) -> str:
        return "noise-area-momentum-v1"

    def on_frame(self, frame: UniverseFrame) -> PortfolioIntent:
        gate = self._calendar.gate(frame.timestamp)
        local = frame.timestamp.astimezone(self._timezone)
        strengths: dict[str, Decimal] = {}
        for bar in frame.bars:
            state = self._sessions.get(bar.symbol)
            if state is None or state.session_date != local.date():
                state = _SessionState(local.date(), bar.open)
                self._sessions[bar.symbol] = state
                self._active.discard(bar.symbol)
            state.price_volume += ((bar.high + bar.low + bar.close) / 3) * bar.volume
            state.volume += bar.volume
            vwap = state.price_volume / state.volume if state.volume else bar.close
            slot = (bar.symbol, local.time())
            history = self._moves[slot]
            move = abs(bar.close / state.opening_price - 1)
            noise = sum(history, Decimal(0)) / len(history) if history else None
            if (
                gate.phase is SessionPhase.ENTRY
                and noise is not None
                and bar.close > state.opening_price * (1 + noise)
                and bar.close > vwap
            ):
                strengths[bar.symbol] = bar.close / vwap - 1
            elif bar.symbol in self._active and bar.close < vwap:
                self._active.discard(bar.symbol)
            history.append(move)
        if gate.phase is SessionPhase.FLATTEN:
            self._active.clear()
        elif strengths and not self._active:
            self._active.add(max(strengths, key=lambda symbol: strengths[symbol]))
        return _intent(self.strategy_id, frame, self._active, self._target, "noise boundary")


class DonchianAtrBreakoutStrategy:
    def __init__(self, intraday: IntradayConfig, *, target_weight: Decimal = Decimal("0.0025")):
        self._calendar = NyseSessionCalendar(intraday)
        self._target = target_weight
        self._bars: defaultdict[str, deque[MarketBar]] = defaultdict(lambda: deque(maxlen=21))
        self._active: set[str] = set()

    @property
    def strategy_id(self) -> str:
        return "donchian-atr-breakout-v1"

    def on_frame(self, frame: UniverseFrame) -> PortfolioIntent:
        gate = self._calendar.gate(frame.timestamp)
        for bar in frame.bars:
            history = self._bars[bar.symbol]
            prior = list(history)
            history.append(bar)
            if len(prior) < 20:
                continue
            channel_high = max(item.high for item in prior[-20:])
            channel_mid = (channel_high + min(item.low for item in prior[-20:])) / Decimal(2)
            true_ranges = [item.high - item.low for item in prior[-14:]]
            atr = sum(true_ranges, Decimal(0)) / len(true_ranges)
            if (
                gate.phase is SessionPhase.ENTRY
                and bar.close > channel_high
                and atr / bar.close >= Decimal("0.0005")
                and not self._active
            ):
                self._active.add(bar.symbol)
            elif bar.symbol in self._active and bar.close < channel_mid:
                self._active.discard(bar.symbol)
        if gate.phase is SessionPhase.FLATTEN:
            self._active.clear()
        return _intent(self.strategy_id, frame, self._active, self._target, "Donchian ATR")


class VolatilitySqueezeBreakoutStrategy:
    def __init__(self, intraday: IntradayConfig, *, target_weight: Decimal = Decimal("0.0025")):
        self._calendar = NyseSessionCalendar(intraday)
        self._target = target_weight
        self._bars: defaultdict[str, deque[MarketBar]] = defaultdict(lambda: deque(maxlen=21))
        self._squeezed: set[str] = set()
        self._active: set[str] = set()

    @property
    def strategy_id(self) -> str:
        return "volatility-squeeze-breakout-v1"

    def on_frame(self, frame: UniverseFrame) -> PortfolioIntent:
        gate = self._calendar.gate(frame.timestamp)
        for bar in frame.bars:
            history = self._bars[bar.symbol]
            history.append(bar)
            if len(history) < 20:
                continue
            closes = [float(item.close) for item in history]
            center = Decimal(str(mean(closes)))
            deviation = Decimal(str(stdev(closes)))
            atr = sum((item.high - item.low for item in history), Decimal(0)) / len(history)
            bollinger_width = deviation * 4
            keltner_width = atr * 3
            if bollinger_width < keltner_width:
                self._squeezed.add(bar.symbol)
            elif (
                gate.phase is SessionPhase.ENTRY
                and bar.symbol in self._squeezed
                and bar.close > center + deviation * 2
                and bar.volume > Decimal(str(mean(float(item.volume) for item in history)))
                and not self._active
            ):
                self._active.add(bar.symbol)
                self._squeezed.discard(bar.symbol)
            elif bar.symbol in self._active and bar.close < center:
                self._active.discard(bar.symbol)
        if gate.phase is SessionPhase.FLATTEN:
            self._active.clear()
        return _intent(self.strategy_id, frame, self._active, self._target, "volatility squeeze")


def _intent(
    strategy_id: str,
    frame: UniverseFrame,
    active: set[str],
    target: Decimal,
    rationale: str,
) -> PortfolioIntent:
    return PortfolioIntent(
        strategy_id=strategy_id,
        timestamp=frame.timestamp,
        target_weights={
            bar.symbol: target if bar.symbol in active else Decimal(0) for bar in frame.bars
        },
        rationale=rationale,
    )
