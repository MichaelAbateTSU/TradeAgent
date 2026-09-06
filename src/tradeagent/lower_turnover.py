from __future__ import annotations

from collections import defaultdict, deque
from decimal import Decimal
from itertools import pairwise
from statistics import mean, stdev

from pydantic import BaseModel, ConfigDict, Field

from tradeagent.portfolio import PortfolioIntent
from tradeagent.universe import UniverseFrame


class TimeSeriesMomentumConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    lookback_days: int = Field(ge=20)
    skip_days: int = Field(ge=0)
    rebalance_days: int = Field(ge=1)
    maximum_positions: int = Field(default=10, ge=1)
    gross_target: Decimal = Field(default=Decimal("0.20"), gt=0, le=Decimal("0.50"))
    maximum_position_weight: Decimal = Field(default=Decimal("0.02"), gt=0)
    estimated_round_trip_cost_bps: Decimal = Field(default=Decimal("7"), ge=0)
    uncertainty_buffer_bps: Decimal = Field(default=Decimal("3"), ge=0)


class RelativeStrengthConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    lookback_days: int = Field(ge=20)
    skip_days: int = Field(ge=0)
    rebalance_days: int = Field(ge=1)
    top_n: int = Field(ge=1)
    maximum_position_weight: Decimal = Field(default=Decimal("0.02"), gt=0)
    estimated_round_trip_cost_bps: Decimal = Field(default=Decimal("7"), ge=0)
    uncertainty_buffer_bps: Decimal = Field(default=Decimal("3"), ge=0)


class SwingMeanReversionConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    zscore_window: int = Field(ge=10)
    entry_zscore: Decimal = Field(lt=0)
    exit_zscore: Decimal = Field(le=0)
    maximum_holding_days: int = Field(ge=2)
    regime_lookback_days: int = Field(default=200, ge=50)
    maximum_regime_volatility: Decimal = Field(default=Decimal("0.30"), gt=0)
    maximum_positions: int = Field(default=3, ge=1)
    position_weight: Decimal = Field(default=Decimal("0.02"), gt=0)
    estimated_round_trip_cost_bps: Decimal = Field(default=Decimal("7"), ge=0)
    uncertainty_buffer_bps: Decimal = Field(default=Decimal("3"), ge=0)


TIME_SERIES_MOMENTUM_CONFIGS = (
    TimeSeriesMomentumConfig(lookback_days=63, skip_days=0, rebalance_days=5),
    TimeSeriesMomentumConfig(lookback_days=63, skip_days=5, rebalance_days=5),
    TimeSeriesMomentumConfig(lookback_days=126, skip_days=0, rebalance_days=5),
    TimeSeriesMomentumConfig(lookback_days=126, skip_days=21, rebalance_days=5),
    TimeSeriesMomentumConfig(lookback_days=252, skip_days=21, rebalance_days=5),
    TimeSeriesMomentumConfig(lookback_days=63, skip_days=0, rebalance_days=21),
    TimeSeriesMomentumConfig(lookback_days=126, skip_days=0, rebalance_days=21),
    TimeSeriesMomentumConfig(lookback_days=126, skip_days=21, rebalance_days=21),
    TimeSeriesMomentumConfig(lookback_days=252, skip_days=0, rebalance_days=21),
    TimeSeriesMomentumConfig(lookback_days=252, skip_days=21, rebalance_days=21),
)

RELATIVE_STRENGTH_CONFIGS = (
    RelativeStrengthConfig(lookback_days=63, skip_days=0, rebalance_days=5, top_n=3),
    RelativeStrengthConfig(lookback_days=63, skip_days=5, rebalance_days=5, top_n=5),
    RelativeStrengthConfig(lookback_days=126, skip_days=0, rebalance_days=5, top_n=3),
    RelativeStrengthConfig(lookback_days=126, skip_days=21, rebalance_days=5, top_n=5),
    RelativeStrengthConfig(lookback_days=252, skip_days=21, rebalance_days=5, top_n=3),
    RelativeStrengthConfig(lookback_days=63, skip_days=0, rebalance_days=21, top_n=3),
    RelativeStrengthConfig(lookback_days=126, skip_days=0, rebalance_days=21, top_n=5),
    RelativeStrengthConfig(lookback_days=126, skip_days=21, rebalance_days=21, top_n=3),
    RelativeStrengthConfig(lookback_days=252, skip_days=0, rebalance_days=21, top_n=5),
    RelativeStrengthConfig(lookback_days=252, skip_days=21, rebalance_days=21, top_n=3),
)

SWING_MEAN_REVERSION_CONFIGS = (
    SwingMeanReversionConfig(
        zscore_window=20,
        entry_zscore=Decimal("-1.0"),
        exit_zscore=Decimal("-0.25"),
        maximum_holding_days=5,
    ),
    SwingMeanReversionConfig(
        zscore_window=20,
        entry_zscore=Decimal("-1.5"),
        exit_zscore=Decimal("-0.25"),
        maximum_holding_days=5,
    ),
    SwingMeanReversionConfig(
        zscore_window=20,
        entry_zscore=Decimal("-2.0"),
        exit_zscore=Decimal("-0.25"),
        maximum_holding_days=5,
    ),
    SwingMeanReversionConfig(
        zscore_window=40,
        entry_zscore=Decimal("-1.0"),
        exit_zscore=Decimal("-0.25"),
        maximum_holding_days=5,
    ),
    SwingMeanReversionConfig(
        zscore_window=40,
        entry_zscore=Decimal("-1.5"),
        exit_zscore=Decimal("-0.25"),
        maximum_holding_days=5,
    ),
    SwingMeanReversionConfig(
        zscore_window=20,
        entry_zscore=Decimal("-1.0"),
        exit_zscore=Decimal("0"),
        maximum_holding_days=10,
    ),
    SwingMeanReversionConfig(
        zscore_window=20,
        entry_zscore=Decimal("-1.5"),
        exit_zscore=Decimal("0"),
        maximum_holding_days=10,
    ),
    SwingMeanReversionConfig(
        zscore_window=20,
        entry_zscore=Decimal("-2.0"),
        exit_zscore=Decimal("0"),
        maximum_holding_days=10,
    ),
    SwingMeanReversionConfig(
        zscore_window=40,
        entry_zscore=Decimal("-1.0"),
        exit_zscore=Decimal("0"),
        maximum_holding_days=10,
    ),
    SwingMeanReversionConfig(
        zscore_window=40,
        entry_zscore=Decimal("-1.5"),
        exit_zscore=Decimal("0"),
        maximum_holding_days=10,
    ),
)


class _MomentumBase:
    def __init__(self, maximum_history: int, rebalance_days: int) -> None:
        self._closes: defaultdict[str, deque[Decimal]] = defaultdict(
            lambda: deque(maxlen=maximum_history)
        )
        self._rebalance_days = rebalance_days
        self._frames = 0
        self._targets: dict[str, Decimal] = {}

    def _observe(self, frame: UniverseFrame) -> None:
        for bar in frame.bars:
            self._closes[bar.symbol].append(bar.close)
        self._frames += 1

    def _should_rebalance(self) -> bool:
        return (self._frames - 1) % self._rebalance_days == 0

    def _momentum(self, symbol: str, lookback: int, skip: int) -> Decimal | None:
        prices = self._closes[symbol]
        required = lookback + skip + 1
        if len(prices) < required:
            return None
        return prices[-1 - skip] / prices[-1 - skip - lookback] - 1

    @staticmethod
    def _clears_cost_hurdle(
        momentum: Decimal,
        *,
        lookback_days: int,
        expected_holding_days: int,
        cost_bps: Decimal,
        uncertainty_bps: Decimal,
    ) -> bool:
        projected_move_bps = (
            momentum * Decimal(expected_holding_days) / Decimal(lookback_days) * Decimal(10_000)
        )
        return projected_move_bps > cost_bps + uncertainty_bps

    def _intent(self, strategy_id: str, frame: UniverseFrame, rationale: str) -> PortfolioIntent:
        return PortfolioIntent(
            strategy_id=strategy_id,
            timestamp=frame.timestamp,
            target_weights={
                bar.symbol: self._targets.get(bar.symbol, Decimal(0)) for bar in frame.bars
            },
            rationale=rationale,
        )


class TimeSeriesMomentumStrategy(_MomentumBase):
    def __init__(self, config: TimeSeriesMomentumConfig) -> None:
        super().__init__(
            config.lookback_days + config.skip_days + 1,
            config.rebalance_days,
        )
        self._config = config

    @property
    def strategy_id(self) -> str:
        return (
            f"time-series-momentum-{self._config.lookback_days}-"
            f"{self._config.skip_days}-{self._config.rebalance_days}"
        )

    def on_frame(self, frame: UniverseFrame) -> PortfolioIntent:
        self._observe(frame)
        if self._should_rebalance():
            positive = sorted(
                (
                    (symbol, momentum)
                    for symbol in self._closes
                    if (
                        momentum := self._momentum(
                            symbol,
                            self._config.lookback_days,
                            self._config.skip_days,
                        )
                    )
                    is not None
                    and momentum > 0
                    and self._clears_cost_hurdle(
                        momentum,
                        lookback_days=self._config.lookback_days,
                        expected_holding_days=self._config.rebalance_days,
                        cost_bps=self._config.estimated_round_trip_cost_bps,
                        uncertainty_bps=self._config.uncertainty_buffer_bps,
                    )
                ),
                key=lambda item: (-item[1], item[0]),
            )[: self._config.maximum_positions]
            allocation = (
                min(
                    self._config.maximum_position_weight,
                    self._config.gross_target / len(positive),
                )
                if positive
                else Decimal(0)
            )
            self._targets = {symbol: allocation for symbol, _ in positive}
        return self._intent(self.strategy_id, frame, "positive multi-day time-series momentum")


class RelativeStrengthRotationStrategy(_MomentumBase):
    def __init__(self, config: RelativeStrengthConfig) -> None:
        super().__init__(
            config.lookback_days + config.skip_days + 1,
            config.rebalance_days,
        )
        self._config = config

    @property
    def strategy_id(self) -> str:
        return (
            f"relative-strength-{self._config.lookback_days}-{self._config.skip_days}-"
            f"{self._config.rebalance_days}-{self._config.top_n}"
        )

    def on_frame(self, frame: UniverseFrame) -> PortfolioIntent:
        self._observe(frame)
        if self._should_rebalance():
            ranked = sorted(
                (
                    (symbol, momentum)
                    for symbol in self._closes
                    if (
                        momentum := self._momentum(
                            symbol,
                            self._config.lookback_days,
                            self._config.skip_days,
                        )
                    )
                    is not None
                    and momentum > 0
                    and self._clears_cost_hurdle(
                        momentum,
                        lookback_days=self._config.lookback_days,
                        expected_holding_days=self._config.rebalance_days,
                        cost_bps=self._config.estimated_round_trip_cost_bps,
                        uncertainty_bps=self._config.uncertainty_buffer_bps,
                    )
                ),
                key=lambda item: (-item[1], item[0]),
            )[: self._config.top_n]
            self._targets = {symbol: self._config.maximum_position_weight for symbol, _ in ranked}
        return self._intent(self.strategy_id, frame, "cross-sectional relative-strength rotation")


class RegimeConditionedSwingMeanReversionStrategy:
    def __init__(self, config: SwingMeanReversionConfig) -> None:
        self._config = config
        history = max(config.zscore_window, config.regime_lookback_days) + 1
        self._closes: defaultdict[str, deque[Decimal]] = defaultdict(lambda: deque(maxlen=history))
        self._active: dict[str, int] = {}

    @property
    def strategy_id(self) -> str:
        return (
            f"regime-swing-mean-reversion-{self._config.zscore_window}-"
            f"{self._config.entry_zscore}-{self._config.maximum_holding_days}"
        )

    def on_frame(self, frame: UniverseFrame) -> PortfolioIntent:
        for bar in frame.bars:
            self._closes[bar.symbol].append(bar.close)
        risk_on = self._risk_on()
        scores: dict[str, Decimal] = {}
        for bar in frame.bars:
            score = self._zscore(bar.symbol)
            if score is None:
                continue
            if bar.symbol in self._active:
                self._active[bar.symbol] += 1
                if (
                    not risk_on
                    or score >= self._config.exit_zscore
                    or self._active[bar.symbol] >= self._config.maximum_holding_days
                ):
                    del self._active[bar.symbol]
            elif risk_on and score <= self._config.entry_zscore:
                recent = list(self._closes[bar.symbol])[-self._config.zscore_window :]
                moving_average = sum(recent, Decimal(0)) / len(recent)
                expected_move_bps = (moving_average / bar.close - 1) * Decimal(10_000)
                if expected_move_bps > (
                    self._config.estimated_round_trip_cost_bps + self._config.uncertainty_buffer_bps
                ):
                    scores[bar.symbol] = score
        available = self._config.maximum_positions - len(self._active)
        if available > 0:
            for symbol, _ in sorted(scores.items(), key=lambda item: (item[1], item[0]))[
                :available
            ]:
                self._active[symbol] = 0
        return PortfolioIntent(
            strategy_id=self.strategy_id,
            timestamp=frame.timestamp,
            target_weights={
                bar.symbol: (
                    self._config.position_weight if bar.symbol in self._active else Decimal(0)
                )
                for bar in frame.bars
            },
            rationale="swing mean reversion gated by SPY trend and volatility",
        )

    def _risk_on(self) -> bool:
        spy = self._closes["SPY"]
        if len(spy) < self._config.regime_lookback_days:
            return False
        recent = list(spy)[-self._config.regime_lookback_days :]
        moving_average = sum(recent, Decimal(0)) / len(recent)
        returns = [float(current / previous - 1) for previous, current in pairwise(recent)]
        annualized_volatility = Decimal(str(stdev(returns) * (252**0.5)))
        return spy[-1] > moving_average and annualized_volatility <= (
            self._config.maximum_regime_volatility
        )

    def _zscore(self, symbol: str) -> Decimal | None:
        closes = self._closes[symbol]
        if len(closes) < self._config.zscore_window:
            return None
        values = [float(value) for value in list(closes)[-self._config.zscore_window :]]
        deviation = stdev(values)
        if deviation == 0:
            return Decimal(0)
        return Decimal(str((values[-1] - mean(values)) / deviation))
