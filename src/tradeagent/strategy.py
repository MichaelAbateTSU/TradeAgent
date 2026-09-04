from __future__ import annotations

from collections import defaultdict, deque
from decimal import Decimal
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
