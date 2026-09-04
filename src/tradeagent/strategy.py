from __future__ import annotations

from collections import defaultdict, deque
from decimal import Decimal
from math import sqrt
from statistics import stdev
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
