from __future__ import annotations

from collections import defaultdict, deque
from decimal import Decimal
from math import sqrt
from statistics import mean, stdev
from typing import Protocol

from tradeagent.config import StrategyConfig
from tradeagent.domain import MarketBar, TradeIntent


class Strategy(Protocol):
    @property
    def strategy_id(self) -> str: ...

    def on_bar(self, bar: MarketBar) -> TradeIntent | None: ...


class SmaCrossoverStrategy:
    """Long-only deterministic baseline used to validate the trading pipeline."""

    def __init__(self, config: StrategyConfig) -> None:
        self._config = config
        self._prices: dict[str, deque[Decimal]] = defaultdict(
            lambda: deque(maxlen=config.slow_window)
        )

    @property
    def strategy_id(self) -> str:
        return self._config.strategy_id

    def on_bar(self, bar: MarketBar) -> TradeIntent | None:
        prices = self._prices[bar.symbol]
        prices.append(bar.close)
        if len(prices) < self._config.slow_window:
            return None

        values = list(prices)
        fast_average = sum(values[-self._config.fast_window :], Decimal(0)) / Decimal(
            self._config.fast_window
        )
        slow_average = sum(values, Decimal(0)) / Decimal(self._config.slow_window)
        target = self._config.target_weight if fast_average > slow_average else Decimal(0)
        relationship = "above" if target > 0 else "at-or-below"
        return TradeIntent(
            strategy_id=self.strategy_id,
            symbol=bar.symbol,
            target_weight=target,
            generated_at=bar.timestamp,
            rationale=f"fast SMA is {relationship} slow SMA",
        )


class ConstantWeightStrategy:
    """Cash/no-trade or buy-and-hold benchmark using the same execution path."""

    def __init__(self, strategy_id: str, target_weight: Decimal) -> None:
        if not Decimal(0) <= target_weight <= Decimal(1):
            raise ValueError("target_weight must be between zero and one")
        self._strategy_id = strategy_id
        self._target_weight = target_weight

    @property
    def strategy_id(self) -> str:
        return self._strategy_id

    def on_bar(self, bar: MarketBar) -> TradeIntent:
        return TradeIntent(
            strategy_id=self.strategy_id,
            symbol=bar.symbol,
            target_weight=self._target_weight,
            generated_at=bar.timestamp,
            rationale=f"constant target weight {self._target_weight}",
        )


class VolatilityTargetTrendStrategy:
    """Long-only trend challenger that reduces exposure as realized volatility rises."""

    def __init__(self, config: StrategyConfig) -> None:
        self._config = config
        history = max(config.slow_window, config.volatility_window + 1)
        self._prices: dict[str, deque[Decimal]] = defaultdict(lambda: deque(maxlen=history))

    @property
    def strategy_id(self) -> str:
        return "volatility-target-trend-v1"

    def on_bar(self, bar: MarketBar) -> TradeIntent | None:
        prices = self._prices[bar.symbol]
        prices.append(bar.close)
        minimum_history = max(self._config.slow_window, self._config.volatility_window + 1)
        if len(prices) < minimum_history:
            return None

        values = list(prices)
        fast_average = sum(values[-self._config.fast_window :], Decimal(0)) / Decimal(
            self._config.fast_window
        )
        slow_average = sum(values[-self._config.slow_window :], Decimal(0)) / Decimal(
            self._config.slow_window
        )
        if fast_average <= slow_average:
            target = Decimal(0)
            rationale = "trend is non-positive"
        else:
            volatility_prices = values[-(self._config.volatility_window + 1) :]
            returns = [
                float(volatility_prices[index] / volatility_prices[index - 1] - 1)
                for index in range(1, len(volatility_prices))
            ]
            annualized_volatility = Decimal(str(stdev(returns) * sqrt(252)))
            scale = min(
                Decimal(1),
                self._config.target_annual_volatility
                / max(annualized_volatility, Decimal("0.000001")),
            )
            target = self._config.target_weight * scale
            rationale = f"positive trend with {annualized_volatility:.4f} realized volatility"

        return TradeIntent(
            strategy_id=self.strategy_id,
            symbol=bar.symbol,
            target_weight=target,
            generated_at=bar.timestamp,
            rationale=rationale,
        )


class DelayedStrategy:
    """Delay target intents so a close-derived signal cannot fill on that close."""

    def __init__(self, strategy: Strategy, delay_bars: int) -> None:
        if delay_bars < 0:
            raise ValueError("delay_bars cannot be negative")
        self._strategy = strategy
        self._delay_bars = delay_bars
        self._pending: deque[TradeIntent | None] = deque()

    @property
    def strategy_id(self) -> str:
        return self._strategy.strategy_id

    def on_bar(self, bar: MarketBar) -> TradeIntent | None:
        current = self._strategy.on_bar(bar)
        if self._delay_bars == 0:
            return current
        self._pending.append(current)
        if len(self._pending) <= self._delay_bars:
            return None
        delayed = self._pending.popleft()
        if delayed is None:
            return None
        return delayed.model_copy(
            update={
                "generated_at": bar.timestamp,
                "rationale": (f"{delayed.rationale}; executed after {self._delay_bars}-bar delay"),
            }
        )


class MeanReversionStrategy:
    """Long-only z-score mean reversion with hysteresis between entry and exit."""

    def __init__(self, config: StrategyConfig) -> None:
        self._config = config
        self._prices: dict[str, deque[Decimal]] = defaultdict(
            lambda: deque(maxlen=config.mean_reversion_window)
        )
        self._targets: dict[str, Decimal] = defaultdict(Decimal)

    @property
    def strategy_id(self) -> str:
        return "zscore-mean-reversion-v1"

    def on_bar(self, bar: MarketBar) -> TradeIntent | None:
        prices = self._prices[bar.symbol]
        prices.append(bar.close)
        if len(prices) < self._config.mean_reversion_window:
            return None

        float_prices = [float(price) for price in prices]
        standard_deviation = stdev(float_prices)
        z_score = (
            Decimal(str((float(bar.close) - mean(float_prices)) / standard_deviation))
            if standard_deviation > 0
            else Decimal(0)
        )
        target = self._targets[bar.symbol]
        if z_score <= self._config.mean_reversion_entry_z:
            target = self._config.target_weight
        elif z_score >= self._config.mean_reversion_exit_z:
            target = Decimal(0)
        self._targets[bar.symbol] = target

        return TradeIntent(
            strategy_id=self.strategy_id,
            symbol=bar.symbol,
            target_weight=target,
            generated_at=bar.timestamp,
            rationale=f"close z-score is {z_score:.4f}",
        )
