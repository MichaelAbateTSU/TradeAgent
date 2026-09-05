from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from statistics import mean, stdev
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, model_validator

from tradeagent.alpaca_stream import MarketQuote
from tradeagent.config import IntradayConfig
from tradeagent.features import IntradayFeatureEngine, IntradayFeatureVector, MarketRegime
from tradeagent.intraday import NyseSessionCalendar, SessionPhase
from tradeagent.portfolio import PortfolioIntent
from tradeagent.universe import UniverseFrame


class IntradayStrategyConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    opening_range_minutes: int = Field(default=30, ge=10, le=60)
    breakout_buffer_bps: Decimal = Field(default=Decimal("5"), ge=0, le=100)
    vwap_minimum_observations: int = Field(default=12, ge=5)
    vwap_entry_z: Decimal = Field(default=Decimal("-1.5"), lt=0)
    vwap_exit_z: Decimal = Field(default=Decimal("-0.25"), le=0)
    target_weight: Decimal = Field(default=Decimal("0.0025"), gt=0, le=0.005)
    top_n: int = Field(default=1, ge=1, le=2)
    minimum_momentum_15m: Decimal = Decimal("0.0005")
    minimum_momentum_30m: Decimal = Decimal("0.001")
    minimum_momentum_60m: Decimal = Decimal("0.002")
    minimum_relative_volume: Decimal = Field(default=Decimal("0.8"), gt=0)
    maximum_spread_bps: Decimal = Field(default=Decimal("10"), gt=0)
    maximum_realized_volatility: Decimal = Field(default=Decimal("0.40"), gt=0)

    @model_validator(mode="after")
    def validate_vwap_thresholds(self) -> IntradayStrategyConfig:
        if self.vwap_entry_z >= self.vwap_exit_z:
            raise ValueError("vwap_entry_z must be below vwap_exit_z")
        return self


@dataclass
class _OpeningRange:
    session_date: date
    high: Decimal
    low: Decimal


class OpeningRangeBreakoutStrategy:
    def __init__(
        self,
        strategy: IntradayStrategyConfig,
        intraday: IntradayConfig,
    ) -> None:
        self._strategy = strategy
        self._calendar = NyseSessionCalendar(intraday)
        self._timezone = ZoneInfo(intraday.timezone)
        self._ranges: dict[str, _OpeningRange] = {}
        self._active: set[str] = set()

    @property
    def strategy_id(self) -> str:
        return "opening-range-breakout-v1"

    def on_frame(self, frame: UniverseFrame) -> PortfolioIntent:
        gate = self._calendar.gate(frame.timestamp)
        local = frame.timestamp.astimezone(self._timezone)
        minutes_from_open = (
            int((frame.timestamp - gate.session_open).total_seconds() // 60)
            if gate.session_open is not None
            else -1
        )
        strengths: dict[str, Decimal] = {}
        for bar in frame.bars:
            current = self._ranges.get(bar.symbol)
            if current is None or current.session_date != local.date():
                current = _OpeningRange(local.date(), bar.high, bar.low)
                self._ranges[bar.symbol] = current
                self._active.discard(bar.symbol)
            if 0 <= minutes_from_open < self._strategy.opening_range_minutes:
                current.high = max(current.high, bar.high)
                current.low = min(current.low, bar.low)
                continue
            buffer = self._strategy.breakout_buffer_bps / Decimal(10_000)
            threshold = current.high * (Decimal(1) + buffer)
            if gate.phase is SessionPhase.ENTRY and bar.close > threshold:
                strengths[bar.symbol] = bar.close / current.high - Decimal(1)
            elif bar.symbol in self._active and bar.close <= current.high:
                self._active.discard(bar.symbol)

        if gate.phase is SessionPhase.FLATTEN:
            self._active.clear()
        elif gate.phase is SessionPhase.ENTRY:
            ranked = sorted(strengths, key=lambda symbol: (-strengths[symbol], symbol))
            self._active = set(ranked[: self._strategy.top_n])

        targets = {
            bar.symbol: (self._strategy.target_weight if bar.symbol in self._active else Decimal(0))
            for bar in frame.bars
        }
        return PortfolioIntent(
            strategy_id=self.strategy_id,
            timestamp=frame.timestamp,
            target_weights=targets,
            rationale=(
                f"{self._strategy.opening_range_minutes}-minute opening range; "
                f"active={','.join(sorted(self._active)) or 'cash'}"
            ),
        )


@dataclass
class _VwapState:
    session_date: date
    price_volume: Decimal = Decimal(0)
    volume: Decimal = Decimal(0)
    deviations: list[float] | None = None

    def __post_init__(self) -> None:
        if self.deviations is None:
            self.deviations = []


class SessionVwapMeanReversionStrategy:
    def __init__(
        self,
        strategy: IntradayStrategyConfig,
        intraday: IntradayConfig,
    ) -> None:
        self._strategy = strategy
        self._calendar = NyseSessionCalendar(intraday)
        self._timezone = ZoneInfo(intraday.timezone)
        self._states: dict[str, _VwapState] = {}
        self._active: set[str] = set()

    @property
    def strategy_id(self) -> str:
        return "session-vwap-mean-reversion-v1"

    def on_frame(self, frame: UniverseFrame) -> PortfolioIntent:
        gate = self._calendar.gate(frame.timestamp)
        local_date = frame.timestamp.astimezone(self._timezone).date()
        z_scores: dict[str, Decimal] = {}
        for bar in frame.bars:
            state = self._states.get(bar.symbol)
            if state is None or state.session_date != local_date:
                state = _VwapState(session_date=local_date)
                self._states[bar.symbol] = state
                self._active.discard(bar.symbol)
            typical = (bar.high + bar.low + bar.close) / Decimal(3)
            state.price_volume += typical * bar.volume
            state.volume += bar.volume
            if state.volume <= 0:
                continue
            vwap = state.price_volume / state.volume
            deviation = float(bar.close / vwap - Decimal(1))
            assert state.deviations is not None
            state.deviations.append(deviation)
            if len(state.deviations) < self._strategy.vwap_minimum_observations:
                continue
            sample = state.deviations[-self._strategy.vwap_minimum_observations :]
            dispersion = stdev(sample)
            z_score = (
                Decimal(str((deviation - mean(sample)) / dispersion))
                if dispersion > 0
                else Decimal(0)
            )
            z_scores[bar.symbol] = z_score
            if bar.symbol in self._active and z_score >= self._strategy.vwap_exit_z:
                self._active.discard(bar.symbol)

        if gate.phase is SessionPhase.FLATTEN:
            self._active.clear()
        elif gate.phase is SessionPhase.ENTRY:
            entries = [
                symbol
                for symbol, score in sorted(z_scores.items(), key=lambda item: item[1])
                if score <= self._strategy.vwap_entry_z
            ]
            available = self._strategy.top_n - len(self._active)
            self._active.update(entries[: max(0, available)])

        targets = {
            bar.symbol: (self._strategy.target_weight if bar.symbol in self._active else Decimal(0))
            for bar in frame.bars
        }
        return PortfolioIntent(
            strategy_id=self.strategy_id,
            timestamp=frame.timestamp,
            target_weights=targets,
            rationale=(f"session VWAP z-score; active={','.join(sorted(self._active)) or 'cash'}"),
        )


class IntradayEqualWeightBenchmark:
    def __init__(
        self,
        gross_target: Decimal,
        intraday: IntradayConfig,
    ) -> None:
        self._gross_target = gross_target
        self._calendar = NyseSessionCalendar(intraday)

    @property
    def strategy_id(self) -> str:
        return "intraday-equal-weight-v1"

    def on_frame(self, frame: UniverseFrame) -> PortfolioIntent:
        gate = self._calendar.gate(frame.timestamp)
        invested = gate.phase in {
            SessionPhase.ENTRY,
            SessionPhase.MANAGE_ONLY,
        }
        weight = self._gross_target / Decimal(len(frame.bars)) if invested else Decimal(0)
        return PortfolioIntent(
            strategy_id=self.strategy_id,
            timestamp=frame.timestamp,
            target_weights={bar.symbol: weight for bar in frame.bars},
            rationale="regular-session equal-weight benchmark",
        )


class RegimeFilteredMomentumStrategy:
    def __init__(
        self,
        strategy: IntradayStrategyConfig,
        intraday: IntradayConfig,
    ) -> None:
        self._strategy = strategy
        self._calendar = NyseSessionCalendar(intraday)
        self._features = IntradayFeatureEngine(intraday)
        self._active: set[str] = set()

    @property
    def strategy_id(self) -> str:
        return "regime-filtered-intraday-momentum-v1"

    def on_quote(self, quote: MarketQuote) -> None:
        self._features.on_quote(quote)

    def on_frame(self, frame: UniverseFrame) -> PortfolioIntent:
        gate = self._calendar.gate(frame.timestamp)
        vectors = {bar.symbol: self._features.on_bar(bar) for bar in frame.bars}
        for symbol in tuple(self._active):
            vector = vectors[symbol]
            if (
                vector.momentum_30m is None
                or vector.momentum_30m <= 0
                or vector.regime is MarketRegime.HIGH_VOLATILITY
            ):
                self._active.discard(symbol)

        if gate.phase is SessionPhase.FLATTEN:
            self._active.clear()
        elif gate.phase is SessionPhase.ENTRY and not self._active:
            eligible = [vector for vector in vectors.values() if self._eligible(vector)]
            ranked = sorted(
                eligible,
                key=lambda vector: (
                    -(vector.momentum_60m or Decimal(0)),
                    vector.symbol,
                ),
            )
            self._active.update(vector.symbol for vector in ranked[: self._strategy.top_n])

        return PortfolioIntent(
            strategy_id=self.strategy_id,
            timestamp=frame.timestamp,
            target_weights={
                bar.symbol: (
                    self._strategy.target_weight if bar.symbol in self._active else Decimal(0)
                )
                for bar in frame.bars
            },
            rationale=(
                f"regime-filtered multi-horizon momentum; active="
                f"{','.join(sorted(self._active)) or 'cash'}"
            ),
        )

    def _eligible(self, vector: IntradayFeatureVector) -> bool:
        return (
            vector.regime is MarketRegime.TRENDING
            and vector.momentum_15m is not None
            and vector.momentum_15m >= self._strategy.minimum_momentum_15m
            and vector.momentum_30m is not None
            and vector.momentum_30m >= self._strategy.minimum_momentum_30m
            and vector.momentum_60m is not None
            and vector.momentum_60m >= self._strategy.minimum_momentum_60m
            and (
                vector.relative_volume is None
                or vector.relative_volume >= self._strategy.minimum_relative_volume
            )
            and (
                vector.spread_bps is None or vector.spread_bps <= self._strategy.maximum_spread_bps
            )
            and (
                vector.realized_volatility is None
                or vector.realized_volatility <= self._strategy.maximum_realized_volatility
            )
        )
