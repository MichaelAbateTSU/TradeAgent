from __future__ import annotations

from collections import defaultdict, deque
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from tradeagent.portfolio import PortfolioIntent, PortfolioStrategy
from tradeagent.universe import UniverseFrame


class PortfolioStrategyConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    lookback_frames: int = Field(default=63, ge=2)
    top_n: int = Field(default=2, ge=1)
    gross_target: Decimal = Field(default=Decimal("0.04"), gt=0, le=1)
    require_positive_momentum: bool = True

    @model_validator(mode="after")
    def validate_position_weight(self) -> PortfolioStrategyConfig:
        if self.gross_target / Decimal(self.top_n) > Decimal("0.02"):
            raise ValueError("per-position target cannot exceed two percent")
        return self


class CrossSectionalMomentumStrategy:
    """Rank trailing returns and hold the strongest positive assets."""

    def __init__(self, config: PortfolioStrategyConfig) -> None:
        self._config = config
        self._prices: dict[str, deque[Decimal]] = defaultdict(
            lambda: deque(maxlen=config.lookback_frames + 1)
        )

    @property
    def strategy_id(self) -> str:
        return "cross-sectional-momentum-v1"

    def on_frame(self, frame: UniverseFrame) -> PortfolioIntent | None:
        for bar in frame.bars:
            self._prices[bar.symbol].append(bar.close)
        if any(
            len(self._prices[bar.symbol]) < self._config.lookback_frames + 1 for bar in frame.bars
        ):
            return None

        momentum = {
            bar.symbol: (self._prices[bar.symbol][-1] / self._prices[bar.symbol][0]) - Decimal(1)
            for bar in frame.bars
        }
        ranked = sorted(momentum, key=lambda symbol: (-momentum[symbol], symbol))
        eligible = (
            [symbol for symbol in ranked if momentum[symbol] > 0]
            if self._config.require_positive_momentum
            else ranked
        )
        selected = set(eligible[: self._config.top_n])
        position_weight = self._config.gross_target / Decimal(self._config.top_n)
        targets = {
            bar.symbol: position_weight if bar.symbol in selected else Decimal(0)
            for bar in frame.bars
        }
        return PortfolioIntent(
            strategy_id=self.strategy_id,
            timestamp=frame.timestamp,
            target_weights=targets,
            rationale=(
                f"selected {','.join(sorted(selected)) or 'cash'} by "
                f"{self._config.lookback_frames}-frame momentum"
            ),
        )


class EqualWeightPortfolioStrategy:
    def __init__(self, gross_target: Decimal) -> None:
        if not Decimal(0) < gross_target <= Decimal(1):
            raise ValueError("gross_target must be between zero and one")
        self._gross_target = gross_target

    @property
    def strategy_id(self) -> str:
        return "equal-weight-portfolio-v1"

    def on_frame(self, frame: UniverseFrame) -> PortfolioIntent:
        weight = self._gross_target / Decimal(len(frame.bars))
        return PortfolioIntent(
            strategy_id=self.strategy_id,
            timestamp=frame.timestamp,
            target_weights={bar.symbol: weight for bar in frame.bars},
            rationale=f"equal-weight {self._gross_target} gross benchmark",
        )


class DelayedPortfolioStrategy:
    def __init__(self, strategy: PortfolioStrategy, delay_frames: int) -> None:
        if delay_frames < 0:
            raise ValueError("delay_frames cannot be negative")
        self._strategy = strategy
        self._delay_frames = delay_frames
        self._pending: deque[PortfolioIntent | None] = deque()

    @property
    def strategy_id(self) -> str:
        return self._strategy.strategy_id

    def on_frame(self, frame: UniverseFrame) -> PortfolioIntent | None:
        current = self._strategy.on_frame(frame)
        if self._delay_frames == 0:
            return current
        self._pending.append(current)
        if len(self._pending) <= self._delay_frames:
            return None
        delayed = self._pending.popleft()
        if delayed is None:
            return None
        return delayed.model_copy(
            update={
                "timestamp": frame.timestamp,
                "rationale": (
                    f"{delayed.rationale}; executed after {self._delay_frames}-frame delay"
                ),
            }
        )
