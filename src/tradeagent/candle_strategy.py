from __future__ import annotations

from collections import defaultdict, deque
from decimal import Decimal
from enum import StrEnum

from tradeagent.config import IntradayConfig
from tradeagent.domain import MarketBar
from tradeagent.intraday import NyseSessionCalendar, SessionPhase
from tradeagent.portfolio import PortfolioIntent
from tradeagent.universe import UniverseFrame


class CandlePattern(StrEnum):
    HEIKIN_ASHI = "heikin-ashi"
    ENGULFING = "engulfing"
    INSIDE_BREAKOUT = "inside-breakout"


class CandleTrackingStrategy:
    def __init__(
        self,
        pattern: CandlePattern,
        intraday: IntradayConfig,
        *,
        target_weight: Decimal = Decimal("0.0025"),
    ) -> None:
        self._pattern = pattern
        self._calendar = NyseSessionCalendar(intraday)
        self._target_weight = target_weight
        self._bars: defaultdict[str, deque[MarketBar]] = defaultdict(lambda: deque(maxlen=4))
        self._ha_open: dict[str, Decimal] = {}

    @property
    def strategy_id(self) -> str:
        return f"candle-{self._pattern.value}-v1"

    def on_frame(self, frame: UniverseFrame) -> PortfolioIntent:
        gate = self._calendar.gate(frame.timestamp)
        signals: dict[str, Decimal] = {}
        for bar in frame.bars:
            history = self._bars[bar.symbol]
            history.append(bar)
            if gate.phase is SessionPhase.ENTRY and self._signal(bar.symbol):
                body = abs(bar.close - bar.open)
                signals[bar.symbol] = body / max(bar.high - bar.low, Decimal("0.000001"))
        selected = max(signals, key=lambda symbol: (signals[symbol], symbol)) if signals else None
        if gate.phase not in {SessionPhase.ENTRY, SessionPhase.MANAGE_ONLY}:
            selected = None
        return PortfolioIntent(
            strategy_id=self.strategy_id,
            timestamp=frame.timestamp,
            target_weights={
                bar.symbol: self._target_weight if bar.symbol == selected else Decimal(0)
                for bar in frame.bars
            },
            rationale=f"{self._pattern.value} candle confirmation",
        )

    def _signal(self, symbol: str) -> bool:
        bars = self._bars[symbol]
        if self._pattern is CandlePattern.ENGULFING:
            if len(bars) < 2:
                return False
            prior, current = bars[-2], bars[-1]
            return (
                prior.close < prior.open
                and current.close > current.open
                and current.open <= prior.close
                and current.close >= prior.open
            )
        if self._pattern is CandlePattern.INSIDE_BREAKOUT:
            if len(bars) < 3:
                return False
            mother, inside, current = bars[-3], bars[-2], bars[-1]
            return (
                inside.high < mother.high
                and inside.low > mother.low
                and current.close > mother.high
            )
        current = bars[-1]
        ha_close = (current.open + current.high + current.low + current.close) / Decimal(4)
        prior_ha_open = self._ha_open.get(symbol, (current.open + current.close) / Decimal(2))
        ha_open = (prior_ha_open + ha_close) / Decimal(2)
        self._ha_open[symbol] = ha_open
        return len(bars) >= 3 and ha_close > ha_open and current.close > current.open
