from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import date, datetime, time
from decimal import Decimal
from enum import StrEnum
from math import sqrt
from statistics import stdev
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict

from tradeagent.alpaca_stream import MarketQuote
from tradeagent.config import IntradayConfig
from tradeagent.domain import MarketBar


class MarketRegime(StrEnum):
    WARMUP = "warmup"
    CALM = "calm"
    TRENDING = "trending"
    HIGH_VOLATILITY = "high_volatility"


class IntradayFeatureVector(BaseModel):
    model_config = ConfigDict(frozen=True)

    symbol: str
    timestamp: datetime
    session_vwap_distance: Decimal
    relative_volume: Decimal | None
    spread_bps: Decimal | None
    realized_volatility: Decimal | None
    momentum_15m: Decimal | None
    momentum_30m: Decimal | None
    momentum_60m: Decimal | None
    regime: MarketRegime


@dataclass
class _FeatureState:
    session_date: date
    prices: deque[Decimal] = field(default_factory=lambda: deque(maxlen=13))
    returns: deque[float] = field(default_factory=lambda: deque(maxlen=12))
    price_volume: Decimal = Decimal(0)
    volume: Decimal = Decimal(0)


class IntradayFeatureEngine:
    def __init__(self, config: IntradayConfig) -> None:
        self._config = config
        self._timezone = ZoneInfo(config.timezone)
        self._states: dict[str, _FeatureState] = {}
        self._quotes: dict[str, MarketQuote] = {}
        self._volume_history: dict[tuple[str, time], deque[Decimal]] = defaultdict(
            lambda: deque(maxlen=20)
        )

    def on_quote(self, quote: MarketQuote) -> None:
        prior = self._quotes.get(quote.symbol)
        if prior is not None and quote.timestamp < prior.timestamp:
            raise ValueError("quotes must be chronological")
        self._quotes[quote.symbol] = quote

    def on_bar(self, bar: MarketBar) -> IntradayFeatureVector:
        local = bar.timestamp.astimezone(self._timezone)
        state = self._states.get(bar.symbol)
        if state is None or state.session_date != local.date():
            state = _FeatureState(session_date=local.date())
            self._states[bar.symbol] = state

        prior_price = state.prices[-1] if state.prices else None
        state.prices.append(bar.close)
        if prior_price is not None:
            state.returns.append(float(bar.close / prior_price - Decimal(1)))
        typical = (bar.high + bar.low + bar.close) / Decimal(3)
        state.price_volume += typical * bar.volume
        state.volume += bar.volume
        vwap = state.price_volume / state.volume if state.volume > 0 else bar.close
        vwap_distance = bar.close / vwap - Decimal(1)

        slot = (bar.symbol, local.time())
        historical_volumes = self._volume_history[slot]
        relative_volume = (
            bar.volume / (sum(historical_volumes, Decimal(0)) / len(historical_volumes))
            if historical_volumes and sum(historical_volumes, Decimal(0)) > 0
            else None
        )

        quote = self._quotes.get(bar.symbol)
        spread_bps: Decimal | None = None
        if quote is not None and quote.timestamp <= bar.timestamp:
            quote_age = (bar.timestamp - quote.timestamp).total_seconds()
            midpoint = (quote.bid_price + quote.ask_price) / Decimal(2)
            if quote_age <= self._config.quote_max_age_seconds and midpoint > 0:
                spread_bps = (quote.ask_price - quote.bid_price) / midpoint * Decimal(10_000)

        realized_volatility = (
            Decimal(str(stdev(state.returns) * sqrt(252 * 78))) if len(state.returns) >= 2 else None
        )
        momentum_15m = self._momentum(state.prices, 3)
        momentum_30m = self._momentum(state.prices, 6)
        momentum_60m = self._momentum(state.prices, 12)
        if momentum_60m is None:
            regime = MarketRegime.WARMUP
        elif realized_volatility is not None and realized_volatility >= Decimal("0.30"):
            regime = MarketRegime.HIGH_VOLATILITY
        elif abs(momentum_60m) >= Decimal("0.003"):
            regime = MarketRegime.TRENDING
        else:
            regime = MarketRegime.CALM

        vector = IntradayFeatureVector(
            symbol=bar.symbol,
            timestamp=bar.timestamp,
            session_vwap_distance=vwap_distance,
            relative_volume=relative_volume,
            spread_bps=spread_bps,
            realized_volatility=realized_volatility,
            momentum_15m=momentum_15m,
            momentum_30m=momentum_30m,
            momentum_60m=momentum_60m,
            regime=regime,
        )
        historical_volumes.append(bar.volume)
        return vector

    @staticmethod
    def _momentum(prices: deque[Decimal], periods: int) -> Decimal | None:
        if len(prices) <= periods:
            return None
        return prices[-1] / prices[-periods - 1] - Decimal(1)
